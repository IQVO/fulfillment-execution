//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
)

// cmd/sweep is the entrypoint the two sweep CronJobs run (ADR-0037,
// ADR-0048): it POSTs this release's own Service, with a per-attempt
// timeout, a bounded backed-off retry, a non-zero exit when it gives up,
// and the sidecar quit on every exit path. Its documented effects live in
// the endpoints it calls:
//
//   - POST /task-lease-expirations frees a task whose lease timed out back
//     to the pool (Pending, claimable again) and raises LeaseExpired on the
//     integration topic plus TaskAvailable (ADR-0003, ADR-0049);
//   - POST /cpt-miss-sweeps raises TaskCPTMissed for every still-open task
//     past its CPT, re-fired on every pass, and never mutates the task
//     (ADR-0025).
//
// This test builds the REAL cmd/execution binary and runs it as a
// subprocess on testcontainers Postgres + Kafka with the deployment's
// environment, seeds the stale state through the real REST use cases, then
// runs the real runMain() against it and asserts the documented
// transitions and events, ending with the API's own graceful stop.

const sweepPaths = `building: sweep-it
paths:
  - id: PICK
    matchPrefix: pick
    direct: true
    requiredCapabilities: [pick]
  - id: PACK
    matchPrefix: pack
    direct: true
    requiredCapabilities: [pack]
`

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func waitUntil(t *testing.T, ctx context.Context, what string, cond func() bool) {
	t.Helper()
	waitUntilDiag(t, ctx, what, nil, cond)
}

// waitUntilDiag is waitUntil with an optional diagnostics callback whose
// output is included in the timeout failure (the api subprocess's log).
func waitUntilDiag(t *testing.T, ctx context.Context, what string, diag func() string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(150 * time.Second); time.Now().Before(deadline) && ctx.Err() == nil; time.Sleep(200 * time.Millisecond) {
		if cond() {
			return
		}
	}
	if diag != nil {
		t.Fatalf("timed out waiting for %s; diagnostics:\n%s", what, diag())
	}
	t.Fatalf("timed out waiting for %s", what)
}

// apiProcess is the real cmd/execution binary as a subprocess.
type apiProcess struct {
	cmd  *exec.Cmd
	addr string
	out  *bytes.Buffer
	done chan struct{}
}

func (a *apiProcess) url(path string) string { return "http://" + a.addr + path }

// startAPI builds cmd/execution (one cached `go build`) and boots it with
// the deployment's environment against the test's containers: real
// migrations, real Kafka consumers, the transactional outbox relay and the
// REST surface the sweep POSTs.
func startAPI(t *testing.T, ctx context.Context, dsn string, brokers []string, dir string) *apiProcess {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "execution")
	build := exec.CommandContext(ctx, "go", "build", "-o", bin, "./cmd/execution")
	build.Dir = dir
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build cmd/execution: %v: %s", err, out)
	}

	paths := filepath.Join(t.TempDir(), "process-paths.yaml")
	if err := os.WriteFile(paths, []byte(sweepPaths), 0o600); err != nil {
		t.Fatal(err)
	}

	p := &apiProcess{cmd: exec.Command(bin), addr: freeAddr(t), out: &bytes.Buffer{}}
	p.cmd.Dir = dir
	p.cmd.Env = append(os.Environ(),
		"DATABASE_URL="+dsn,
		"MIGRATIONS_PATH=migrations",
		"KAFKA_BROKERS="+strings.Join(brokers, ","),
		"WORK_RELEASED_CONSUMER_GROUP=fe-sweep-it-work-released",
		"PRODUCT_CLASSIFICATION_CONSUMER_GROUP=fe-sweep-it-classification",
		"TASK_INTERLEAVING_CONSUMER_GROUP=fe-sweep-it-interleaving",
		"PACK_VIEW_CONSUMER_GROUP=fe-sweep-it-pack-view",
		"INVENTORY_STORAGE_BASE_URL=http://127.0.0.1:1",
		"EVENT_PUBLISHER=kafka",
		"HTTP_ADDR="+p.addr,
		"PATH_CATALOGUE_FILE="+paths,
		"OUTBOX_RELAY_INTERVAL=200ms",
		"LOG_LEVEL=error",
		"OTEL_SDK_DISABLED=true",
		"OTEL_TRACES_EXPORTER=none",
		"OTEL_METRICS_EXPORTER=none",
	)
	p.cmd.Stdout = p.out
	p.cmd.Stderr = p.out
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start api: %v", err)
	}
	p.done = make(chan struct{})
	go func() { _ = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		if p.cmd.ProcessState == nil {
			_ = p.cmd.Process.Kill()
			<-p.done
		}
	})
	waitUntil(t, ctx, "GET /readyz = 200 on the real api", func() bool {
		code, err := statusCode(p.url("/readyz"))
		return err == nil && code == http.StatusOK
	})
	return p
}

// stop sends the api its documented graceful-shutdown signal and waits for
// exit code 0.
func (a *apiProcess) stop(t *testing.T) {
	t.Helper()
	if a.cmd.ProcessState != nil {
		return
	}
	if err := a.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal api: %v", err)
	}
	select {
	case <-a.done:
	case <-time.After(40 * time.Second):
		t.Fatalf("api did not stop after SIGTERM; output:\n%s", a.out)
	}
	if a.cmd.ProcessState.ExitCode() != 0 {
		t.Fatalf("api exited %d after SIGTERM, want 0; output:\n%s", a.cmd.ProcessState.ExitCode(), a.out)
	}
}

func (a *apiProcess) post(t *testing.T, path, key, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, a.url(path), strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func statusCode(url string) (int, error) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

// runSweep runs the REAL entrypoint logic (runner.runMain, the function
// main() calls) against url, with the chart's configuration shape.
func runSweep(t *testing.T, ctx context.Context, sweepURL string) int {
	t.Helper()
	lookup := env(map[string]string{
		"SWEEP_URL":             sweepURL,
		"SIDECAR_QUIT_URL":      "",
		"SWEEP_REQUEST_TIMEOUT": "15s",
	})
	r := runner{client: &http.Client{}, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), sleep: sleepCtx}
	return r.runMain(ctx, lookup)
}

// consumeEvents reads topic from the beginning and returns how many times
// each wanted event type (matched on the type's last segment) was seen,
// plus the raw events of the FIRST occurrence per type. Bounded by a
// deadline; safe to call before the events exist yet (it polls).
func consumeEvents(t *testing.T, ctx context.Context, brokers []string, topic string, diag func() string, want ...string) (map[string]int, map[string]json.RawMessage) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	counts := map[string]int{}
	first := map[string]json.RawMessage{}
	for time.Now().Before(deadline) && ctx.Err() == nil {
		readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, StartOffset: kafkago.FirstOffset})
		counts, first = map[string]int{}, map[string]json.RawMessage{}
		for _, msg := range readAll(t, readCtx, r) {
			var e struct {
				Type    string          `json:"type"`
				Subject string          `json:"subject"`
				Data    json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal(msg.Value, &e); err != nil {
				continue // not a CloudEvent on this topic
			}
			for _, name := range want {
				if strings.HasSuffix(e.Type, "."+name) {
					counts[name]++
					if _, ok := first[name]; !ok {
						first[name] = e.Data
					}
				}
			}
		}
		_ = r.Close()
		cancel()
		if len(counts) == len(want) {
			return counts, first
		}
		time.Sleep(300 * time.Millisecond)
	}
	if diag != nil {
		t.Fatalf("timed out reading %v from %s, saw %v; diagnostics:\n%s", want, topic, counts, diag())
	}
	t.Fatalf("timed out reading %v from %s, saw %v", want, topic, counts)
	return nil, nil
}

// apiDiag renders the api subprocess's log and the outbox table for a
// timeout failure.
func apiDiag(t *testing.T, api *apiProcess, pool *pgxpool.Pool) func() string {
	return func() string {
		rows, err := pool.Query(context.Background(), "SELECT id, topic, event_type, published_at IS NULL AS pending FROM outbox_events ORDER BY id")
		if err != nil {
			return fmt.Sprintf("outbox query: %v\napi log:\n%s", err, api.out)
		}
		defer rows.Close()
		var b strings.Builder
		for rows.Next() {
			var id int64
			var topic, typ string
			var pending bool
			if err := rows.Scan(&id, &topic, &typ, &pending); err != nil {
				break
			}
			fmt.Fprintf(&b, "outbox %d %s %s pending=%v\n", id, topic, typ, pending)
		}
		return b.String() + "api log:\n" + api.out.String()
	}
}

func readAll(t *testing.T, ctx context.Context, r *kafkago.Reader) []kafkago.Message {
	t.Helper()
	var out []kafkago.Message
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return out
			}
			t.Fatalf("read kafka: %v", err)
		}
		out = append(out, msg)
	}
}

// dataString digs one string field out of a CloudEvent JSON data payload.
func dataString(t *testing.T, raw json.RawMessage, field string) string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode event data %s: %v", raw, err)
	}
	v, ok := m[field]
	if !ok {
		t.Fatalf("event data %s has no %s", raw, field)
	}
	s, _ := v.(string)
	return s
}

// TestSweepEntryPointFreesAnExpiredLeaseAndReportsACPTMiss boots the real
// api on real Postgres + Kafka, seeds the two stale states through the real
// REST use cases (a claimed PICK task whose lease has lapsed, and an open
// PACK task already past its CPT), then runs the real sweep entrypoint
// against each sweep endpoint and asserts the documented transition and
// event for both, plus the re-fire semantics of the CPT sweep (ADR-0025)
// and a graceful api stop.
func TestSweepEntryPointFreesAnExpiredLeaseAndReportsACPTMiss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	pg, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("fulfillment_execution"),
		tcpostgres.WithUsername("fulfillment"),
		tcpostgres.WithPassword("fulfillment"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(pg) })
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}

	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("fe-sweep-it"))
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(kc) })
	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}

	// Pre-create the topics the service consumes (a kafka-go group
	// consumer that starts before its topic exists does not pick it up
	// when it appears), like the cluster would have them.
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial kafka: %v", err)
	}
	for _, topic := range []string{"warehouse.work-planning.events", inboundkafka.ProductMasterTopic, inboundkafka.TaskInterleavingTopic} {
		if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
			t.Fatalf("create topic %s: %v", topic, err)
		}
	}
	_ = conn.Close()

	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	api := startAPI(t, ctx, dsn, brokers, repo)

	pool, err := postgres.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("test pool: %v", err)
	}
	t.Cleanup(pool.Close)

	count := func(query string, args ...any) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return n
	}

	// Seed through the REAL use cases: a station and a PACK task already
	// past its CPT over REST, and a sited PICK task (future CPT) through
	// the WorkReleased consumer — the documented way a pick task gets its
	// placement (ADR-0062), which is what makes its return to the pool
	// announceable as TaskAvailable (ADR-0049).
	if code, resp := api.post(t, "/stations", "sweep-it-station", `{"stationId":"ST-SWEEP","capabilities":["pick","pack"]}`); code != http.StatusCreated {
		t.Fatalf("POST /stations = %d %s, want 201", code, resp)
	}
	now := time.Now().UTC()
	released := kafkago.Message{
		Key: []byte("order-sweep-lease"),
		Value: cloudEventBytes(t, "sweep-it-wr-1", "/warehouse/wes-work-planning", inboundkafka.TypeWorkReleased,
			"order-sweep-lease", "urn:warehouse:wes-work-planning:events:WorkReleased:v1", map[string]any{
				"path_id": "PICK", "work_unit_id": "order-sweep-lease",
				"cpt": now.Add(time.Hour).Format(time.RFC3339), "ref": "order-sweep",
				"pick": map[string]any{"sku": "SKU-SWEEP", "quantity": 2, "unit": "EACH", "site_id": "WH1", "from_bin": "A-01-01"},
			}),
	}
	producer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: "warehouse.work-planning.events", RequiredAcks: kafkago.RequireAll, BatchTimeout: 10 * time.Millisecond}
	t.Cleanup(func() { _ = producer.Close() })
	publishUntil(t, ctx, producer, released, "WorkReleased creating the sited PICK task", func() bool {
		return count("SELECT count(*) FROM tasks WHERE order_ref = 'order-sweep-lease'") == 1
	})
	var leaseTaskId string
	if err := pool.QueryRow(ctx, `SELECT id FROM tasks WHERE order_ref = 'order-sweep-lease'`).Scan(&leaseTaskId); err != nil {
		t.Fatalf("load the created task: %v", err)
	}
	if code, resp := api.post(t, "/stations/ST-SWEEP/task-claims", "sweep-it-claim-1", `{"taskType":"PICK"}`); code != http.StatusOK {
		t.Fatalf("task-claims = %d %s, want 200", code, resp)
	}
	if n := count("SELECT count(*) FROM tasks WHERE id = $1 AND status = 'CLAIMED' AND lease_station_id = 'ST-SWEEP'", leaseTaskId); n != 1 {
		t.Fatalf("task %s not claimed, rows = %d", leaseTaskId, n)
	}
	cptBody := fmt.Sprintf(`{"type":"PACK","cpt":%q,"orderRef":"order-sweep-cpt","requiredCapabilities":["pack"]}`, now.Add(-time.Hour).Format(time.RFC3339))
	if code, resp := api.post(t, "/tasks", "sweep-it-task-cpt", cptBody); code != http.StatusCreated {
		t.Fatalf("POST /tasks (cpt) = %d %s, want 201 (a task may be created already past its CPT; the sweep exists for exactly that state)", code, resp)
	}

	// The lease's five minutes pass: backdate the stored expiry, the one
	// bit of the stale state the real use cases cannot conjure in test
	// time (the api's wall clock is real and uninjectable).
	if _, err := pool.Exec(ctx, `UPDATE tasks SET lease_expiry = now() - interval '2 minutes' WHERE id = $1`, leaseTaskId); err != nil {
		t.Fatalf("backdate lease: %v", err)
	}

	// The scheduled moment arrives: the sweep entrypoint runs as the
	// CronJob runs it, against this release's own Service.
	if code := runSweep(t, ctx, api.url("/task-lease-expirations")); code != exitOK {
		t.Fatalf("lease sweep exit code = %d, want %d", code, exitOK)
	}

	// The documented transition: the task is freed back to the pool
	// (ADR-0003) — Pending, no lease — and is claimable again.
	if n := count("SELECT count(*) FROM tasks WHERE id = $1 AND status = 'PENDING' AND lease_station_id IS NULL AND lease_expiry IS NULL", leaseTaskId); n != 1 {
		t.Fatalf("task %s not freed to Pending, rows = %d", leaseTaskId, n)
	}
	code, reclaimed := api.post(t, "/stations/ST-SWEEP/task-claims", "sweep-it-claim-2", `{"taskType":"PICK"}`)
	if code != http.StatusOK {
		t.Fatalf("re-claim after the sweep = %d %s, want 200", code, reclaimed)
	}
	var again struct {
		Id string `json:"id"`
	}
	if err := json.Unmarshal([]byte(reclaimed), &again); err != nil || again.Id != leaseTaskId {
		t.Fatalf("re-claim returned %s (%v), want the freed task %s", reclaimed, err, leaseTaskId)
	}

	// The documented events: TaskAvailable (the task re-enters the pool,
	// ADR-0049) on the integration topic, and LeaseExpired on the ANALYTICS
	// topic (the two occurrences the sweep raises flow on separate
	// contracts, ADR-0003/ADR-0012). First wait for the relay to drain the
	// outbox, exactly as the api's own e2e suite does.
	waitUntil(t, ctx, "the outbox drained by the relay", func() bool {
		return count("SELECT count(*) FROM outbox_events WHERE published_at IS NULL") == 0 &&
			count("SELECT count(*) FROM outbox_events WHERE published_at IS NOT NULL") > 0
	})
	counts, first := consumeEvents(t, ctx, brokers, outboundkafka.Topic, apiDiag(t, api, pool), "TaskAvailable", "TaskClaimed")
	// TaskAvailable twice: once when the WorkReleased consumer created the
	// sited pick (its pool announcement, ADR-0062) and once when the sweep
	// freed it back (ADR-0049) — both documented occurrences for this task.
	if counts["TaskAvailable"] != 2 || dataString(t, first["TaskAvailable"], "task_id") != leaseTaskId {
		t.Errorf("TaskAvailable count=%d task=%s, want 2 (creation + sweep re-entry) for %s", counts["TaskAvailable"], dataString(t, first["TaskAvailable"], "task_id"), leaseTaskId)
	}
	aCounts, aFirst := consumeEvents(t, ctx, brokers, outboundkafka.AnalyticsTopic, apiDiag(t, api, pool), "LeaseExpired")
	if aCounts["LeaseExpired"] != 1 || dataString(t, aFirst["LeaseExpired"], "task_id") != leaseTaskId {
		t.Errorf("LeaseExpired count=%d task=%s on the analytics topic, want 1 for %s", aCounts["LeaseExpired"], dataString(t, aFirst["LeaseExpired"], "task_id"), leaseTaskId)
	}

	// The CPT sweep: the entrypoint reports the open overdue task, which
	// never changes state (ADR-0025), and re-fires on every pass.
	cptURL := api.url("/cpt-miss-sweeps")
	if code := runSweep(t, ctx, cptURL); code != exitOK {
		t.Fatalf("cpt sweep exit code = %d, want %d", code, exitOK)
	}
	if code := runSweep(t, ctx, cptURL); code != exitOK {
		t.Fatalf("second cpt sweep exit code = %d, want %d", code, exitOK)
	}
	waitUntil(t, ctx, "the cpt sweep's outbox rows drained", func() bool {
		return count("SELECT count(*) FROM outbox_events WHERE published_at IS NULL") == 0
	})
	counts, first = consumeEvents(t, ctx, brokers, outboundkafka.Topic, apiDiag(t, api, pool), "TaskCPTMissed")
	if counts["TaskCPTMissed"] != 2 {
		t.Errorf("TaskCPTMissed count = %d, want 2 (re-fired on every pass)", counts["TaskCPTMissed"])
	}
	if dataString(t, first["TaskCPTMissed"], "task_id") == leaseTaskId {
		t.Errorf("TaskCPTMissed reported the completed-lifecycle PICK task; data=%s", first["TaskCPTMissed"])
	}
	if dataString(t, first["TaskCPTMissed"], "order_ref") != "order-sweep-cpt" {
		t.Errorf("TaskCPTMissed order_ref = %s, want order-sweep-cpt", dataString(t, first["TaskCPTMissed"], "order_ref"))
	}
	if n := count("SELECT count(*) FROM tasks WHERE order_ref = 'order-sweep-cpt' AND status = 'PENDING'"); n != 1 {
		t.Errorf("the CPT sweep must not change task state; PENDING rows for order-sweep-cpt = %d, want 1", n)
	}

	// Clean drain: the api stops gracefully on its shutdown signal.
	api.stop(t)
}

// cloudEventBytes builds one CloudEvents 1.0 structured-mode message, the
// envelope every consumed topic requires (ADR-0032).
func cloudEventBytes(t *testing.T, id, source, typ, subject, schema string, data any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource(source)
	e.SetType(typ)
	e.SetSubject(subject)
	e.SetTime(time.Now().UTC())
	e.SetDataSchema(schema)
	if err := e.SetData(ce.ApplicationJSON, data); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// publishUntil republishes msg (the consumer dedupes on the CloudEvents id,
// so a repeat is harmless) until cond holds: a consumer group that started
// at the latest offset legitimately skips what was published before it
// joined, so the release is repeated until the effect is visible.
func publishUntil(t *testing.T, ctx context.Context, w *kafkago.Writer, msg kafkago.Message, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if err := w.WriteMessages(ctx, msg); err != nil {
			t.Fatalf("publish %s: %v", what, err)
		}
		for range 10 {
			if cond() {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}
