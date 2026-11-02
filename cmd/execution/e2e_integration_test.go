//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
)

// e2eEnv is the real deployment configuration of the service under test.
type e2eEnv struct {
	dsn     string
	brokers []string
	addr    string
	dir     string
}

const processPaths = `building: e2e
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

// startContainers boots a real Postgres and a real Kafka (testcontainers) and
// pre-creates the three topics the service consumes: a kafka-go group
// consumer that starts before its topic exists does not pick it up when it
// appears (documented in the ADR), so the test creates them like the cluster
// does.
func startContainers(t *testing.T, ctx context.Context) e2eEnv {
	t.Helper()
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

	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("fe-e2e"))
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(kc) })
	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}
	for _, topic := range []string{workReleasedTopic, inboundkafka.ProductMasterTopic, inboundkafka.TaskInterleavingTopic} {
		createTopic(t, ctx, brokers[0], topic)
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "process-paths.yaml"), []byte(processPaths), 0o600); err != nil {
		t.Fatal(err)
	}
	return e2eEnv{dsn: dsn, brokers: brokers, addr: freeAddr(t), dir: dir}
}

func createTopic(t *testing.T, ctx context.Context, broker, topic string) {
	t.Helper()
	conn, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatalf("dial kafka: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	waitFor(t, ctx, 20*time.Second, "topic "+topic+" has a leader", func() bool {
		parts, err := conn.ReadPartitions(topic)
		return err == nil && len(parts) == 1
	})
}

// waitFor polls cond until it is true or the timeout passes.
func waitFor(t *testing.T, ctx context.Context, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, what)
}

// startService runs the REAL entry point, in-process, with only the
// deployment's environment, and returns a stop func that cancels it and
// returns run()'s result.
func startService(t *testing.T, ctx context.Context, e2e e2eEnv) func() error {
	t.Helper()
	t.Setenv("DATABASE_URL", e2e.dsn)
	t.Setenv("MIGRATIONS_PATH", "../../migrations")
	t.Setenv("KAFKA_BROKERS", strings.Join(e2e.brokers, ","))
	t.Setenv("WORK_RELEASED_CONSUMER_GROUP", "fe-e2e-work-released")
	t.Setenv("PRODUCT_CLASSIFICATION_CONSUMER_GROUP", "fe-e2e-product-classification")
	t.Setenv("TASK_INTERLEAVING_CONSUMER_GROUP", "fe-e2e-interleaving")
	t.Setenv("PACK_VIEW_CONSUMER_GROUP", "fe-e2e-pack-view")
	t.Setenv("INVENTORY_STORAGE_BASE_URL", "http://127.0.0.1:1")
	t.Setenv("EVENT_PUBLISHER", "kafka")
	t.Setenv("HTTP_ADDR", e2e.addr)
	t.Setenv("PATH_CATALOGUE_FILE", filepath.Join(e2e.dir, "process-paths.yaml"))
	t.Setenv("OUTBOX_RELAY_INTERVAL", "200ms")
	t.Setenv("LOG_LEVEL", "error")
	// No collector in this test: keep the OTel exporters from retrying a
	// dial to localhost:4317 at shutdown.
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- run(runCtx) }()
	stopped := false
	stop := func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(40 * time.Second):
			return errors.New("run() did not return within 40s of cancellation")
		}
	}
	t.Cleanup(func() { _ = stop() })

	waitFor(t, ctx, 120*time.Second, "GET /readyz = 200 on real Postgres + Kafka", func() bool {
		code, err := statusOf(t, "http://"+e2e.addr+"/readyz")
		return err == nil && code == http.StatusOK
	})
	return stop
}

func (e e2eEnv) url(path string) string { return "http://" + e.addr + path }

func (e e2eEnv) do(t *testing.T, method, path, key, body string) (int, string, http.Header) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, e.url(path), strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

// producer writes CloudEvents to a consumed topic.
func producer(t *testing.T, e2e e2eEnv, topic string) *kafkago.Writer {
	t.Helper()
	w := &kafkago.Writer{Addr: kafkago.TCP(e2e.brokers...), Topic: topic, RequiredAcks: kafkago.RequireAll, BatchTimeout: 10 * time.Millisecond}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

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

// publishUntil republishes msg (the consumers dedupe on the CloudEvents id,
// so a repeat is harmless) until cond holds. A consumer group that starts at
// the latest offset legitimately skips what was published before it joined.
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

func count(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// eventsOn reads topic from the beginning until every wanted event type
// (matched on the type's last segment) was seen, and returns them. The read
// is bounded by a deadline.
func eventsOn(t *testing.T, ctx context.Context, brokers []string, topic string, want ...string) map[string]ce.Event {
	t.Helper()
	readCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, StartOffset: kafkago.FirstOffset})
	defer func() { _ = r.Close() }()
	got := map[string]ce.Event{}
	for len(got) < len(want) {
		msg, err := r.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("reading %s, saw %v of %v: %v", topic, keys(got), want, err)
		}
		var e ce.Event
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			t.Fatalf("%s: not a CloudEvent: %v (%s)", topic, err, msg.Value)
		}
		if err := e.Validate(); err != nil || e.SpecVersion() != "1.0" {
			t.Fatalf("%s: invalid CloudEvents 1.0 message %s: %v", topic, msg.Value, err)
		}
		for _, name := range want {
			if strings.HasSuffix(e.Type(), "."+name) {
				got[name] = e
			}
		}
	}
	return got
}

func keys(m map[string]ce.Event) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestRunOnRealPostgresAndKafka boots the REAL entry point (run) against a
// real Postgres and a real Kafka with only the deployment's environment and
// drives it only through Kafka and REST: the three consumers feed Postgres,
// the REST edge writes through the transactional outbox, and the always-on
// relay puts CloudEvents 1.0 on the output topics. The process is restarted
// in the middle and nothing is lost or duplicated.
func TestRunOnRealPostgresAndKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 270*time.Second)
	defer cancel()
	e2e := startContainers(t, ctx)

	pool, err := postgres.NewPool(ctx, e2e.dsn)
	if err != nil {
		t.Fatalf("test pool: %v", err)
	}
	t.Cleanup(pool.Close)

	stop := startService(t, ctx, e2e)
	assertReadyWithoutOutputTopics(t, ctx, e2e)
	feedConsumers(t, ctx, e2e, pool)
	driveRest(t, e2e, pool)

	// Restart mid-way: the same environment, a fresh process-in-process.
	if err := stop(); err != nil {
		t.Fatalf("first run() returned %v, want nil after a graceful stop", err)
	}
	if code, err := statusOf(t, e2e.url("/readyz")); err == nil {
		t.Fatalf("GET /readyz after the stop = %d, want a refused connection", code)
	}
	stop = startService(t, ctx, e2e)
	assertNothingLostOrDuplicated(t, ctx, e2e, pool)
	claimAndComplete(t, e2e, pool)
	assertOutputEvents(t, ctx, e2e, pool)
	if err := stop(); err != nil {
		t.Fatalf("second run() returned %v, want nil", err)
	}
}

// assertReadyWithoutOutputTopics: /readyz is 200 although the service's own
// output topics do not exist yet (the broker auto-creates them on first
// write, so readiness never requires a topic) and the probe did not create
// them either.
func assertReadyWithoutOutputTopics(t *testing.T, ctx context.Context, e2e e2eEnv) {
	t.Helper()
	conn, err := kafkago.DialContext(ctx, "tcp", e2e.brokers[0])
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	parts, err := conn.ReadPartitions()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range parts {
		if p.Topic == outboundkafka.Topic || p.Topic == outboundkafka.AnalyticsTopic {
			t.Fatalf("output topic %s exists before the service published anything", p.Topic)
		}
	}
	if code, err := statusOf(t, e2e.url("/readyz")); err != nil || code != http.StatusOK {
		t.Fatalf("GET /readyz with missing output topics = %d, %v, want 200", code, err)
	}
	if code, err := statusOf(t, e2e.url("/healthz")); err != nil || code != http.StatusOK {
		t.Fatalf("GET /healthz = %d, %v, want 200", code, err)
	}
}

// feedConsumers publishes one event per consumed topic and asserts the
// Postgres effect of each.
func feedConsumers(t *testing.T, ctx context.Context, e2e e2eEnv, pool *pgxpool.Pool) {
	t.Helper()

	classified := kafkago.Message{Key: []byte("SKU-E2E"), Value: cloudEventBytes(t, "ce-e2e-class-1", "/warehouse/product-master",
		inboundkafka.TypeProductClassified, "SKU-E2E", "urn:warehouse:product-master:events:ProductClassified:v1",
		map[string]any{"sku": "SKU-E2E", "handling_tags": []string{"Hazmat"}, "dot_hazard_class": 3, "version": 1})}
	publishUntil(t, ctx, producer(t, e2e, inboundkafka.ProductMasterTopic), classified, "ProductClassified in product_classification_copy",
		func() bool {
			return count(t, ctx, pool, "SELECT count(*) FROM product_classification_copy WHERE sku = 'SKU-E2E' AND dot_hazard_class = 3") == 1
		})

	hint := kafkago.Message{Key: []byte("s-e2e"), Value: cloudEventBytes(t, "ce-e2e-hint-1", "/warehouse/task-interleaving",
		inboundkafka.TypeInterleavingHintIssued, "s-e2e", "urn:warehouse:task-interleaving:events:InterleavingHintIssued:v1",
		map[string]any{
			"hint_id": "hint-e2e", "station_id": "s-e2e", "site_id": "SITE-1", "after_task_id": "task-done",
			"origin_location": "WH1-FWD-A01-01-01-A", "expires_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano), "policy_version": 1,
			"candidates": []map[string]any{{"rank": 1, "task_id": "t-x", "task_type": "PICK", "location": "WH1-FWD-A01-02-01-A", "detour_m": 4.5}},
		})}
	publishUntil(t, ctx, producer(t, e2e, inboundkafka.TaskInterleavingTopic), hint, "InterleavingHintIssued in interleaving_hints",
		func() bool {
			return count(t, ctx, pool, "SELECT count(*) FROM interleaving_hints WHERE station_id = 's-e2e' AND hint_id = 'hint-e2e'") == 1
		})

	released := kafkago.Message{Key: []byte("order-e2e"), Value: cloudEventBytes(t, "ce-e2e-wr-1", "/warehouse/wes-work-planning",
		inboundkafka.TypeWorkReleased, "order-e2e", "urn:warehouse:wes-work-planning:events:WorkReleased:v1",
		map[string]any{"path_id": "PICK", "work_unit_id": "order-e2e", "cpt": "2031-01-01T12:00:00Z"})}
	publishUntil(t, ctx, producer(t, e2e, workReleasedTopic), released, "WorkReleased creating a PICK task",
		func() bool {
			return count(t, ctx, pool, "SELECT count(*) FROM tasks WHERE order_ref = 'order-e2e'") == 1
		})
}

// driveRest exercises the REST edge: the Idempotency-Key is always required,
// a NUL is a client error that does not consume the key, and a replay is
// answered from the stored outcome.
func driveRest(t *testing.T, e2e e2eEnv, pool *pgxpool.Pool) {
	t.Helper()
	const body = `{"type":"PICK","cpt":"2032-01-01T12:00:00Z","orderRef":"order-rest","requiredCapabilities":["pick"]}`

	if code, resp, _ := e2e.do(t, http.MethodPost, "/tasks", "", body); code != http.StatusBadRequest || !strings.Contains(resp, "idempotency-key-required") {
		t.Fatalf("POST /tasks without a key = %d %s, want 400 idempotency-key-required", code, resp)
	}
	nul := strings.Replace(body, "order-rest", `order-\u0000rest`, 1)
	if code, resp, _ := e2e.do(t, http.MethodPost, "/tasks", "key-e2e-1", nul); code != http.StatusBadRequest || !strings.Contains(resp, "malformed-request-body") {
		t.Fatalf("POST /tasks with a NUL = %d %s, want 400 malformed-request-body", code, resp)
	}
	code, first, hdr := e2e.do(t, http.MethodPost, "/tasks", "key-e2e-1", body)
	if code != http.StatusCreated || hdr.Get("Location") == "" {
		t.Fatalf("POST /tasks = %d %s, want 201 with a Location (the key survived the rejected NUL)", code, first)
	}
	if code, replay, replayHdr := e2e.do(t, http.MethodPost, "/tasks", "key-e2e-1", body); code != http.StatusCreated || replay != first || replayHdr.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay = %d %s (Idempotency-Replayed=%q), want the stored 201 %s with Idempotency-Replayed: true", code, replay, replayHdr.Get("Idempotency-Replayed"), first)
	}
	if n := count(t, t.Context(), pool, "SELECT count(*) FROM tasks WHERE order_ref = 'order-rest'"); n != 1 {
		t.Fatalf("tasks for order-rest = %d, want exactly 1", n)
	}
	if code, list, _ := e2e.do(t, http.MethodGet, "/tasks?orderRef=order-e2e", "", ""); code != http.StatusOK || !strings.Contains(list, "order-e2e") {
		t.Fatalf("GET /tasks?orderRef=order-e2e = %d %s, want the Kafka-created task", code, list)
	}
}

// assertNothingLostOrDuplicated: after the restart both tasks are still
// there, and redelivering the same WorkReleased CloudEvents id changes
// nothing (the consumer dedupes on it in the same transaction).
func assertNothingLostOrDuplicated(t *testing.T, ctx context.Context, e2e e2eEnv, pool *pgxpool.Pool) {
	t.Helper()
	if n := count(t, ctx, pool, "SELECT count(*) FROM tasks"); n != 2 {
		t.Fatalf("tasks after the restart = %d, want 2", n)
	}
	released := kafkago.Message{Key: []byte("order-e2e"), Value: cloudEventBytes(t, "ce-e2e-wr-1", "/warehouse/wes-work-planning",
		inboundkafka.TypeWorkReleased, "order-e2e", "urn:warehouse:wes-work-planning:events:WorkReleased:v1",
		map[string]any{"path_id": "PICK", "work_unit_id": "order-e2e", "cpt": "2031-01-01T12:00:00Z"})}
	w := producer(t, e2e, workReleasedTopic)
	for range 3 {
		if err := w.WriteMessages(ctx, released); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
	}
	if n := count(t, ctx, pool, "SELECT count(*) FROM tasks WHERE order_ref = 'order-e2e'"); n != 1 {
		t.Fatalf("tasks for order-e2e after redelivery = %d, want exactly 1", n)
	}
	if n := count(t, ctx, pool, "SELECT count(*) FROM processed_events WHERE event_id = 'ce-e2e-wr-1'"); n != 1 {
		t.Fatalf("processed claims for ce-e2e-wr-1 = %d, want exactly 1", n)
	}
}

// claimAndComplete drives a station through register, task-claims and completions.
func claimAndComplete(t *testing.T, e2e e2eEnv, pool *pgxpool.Pool) {
	t.Helper()
	if code, resp, _ := e2e.do(t, http.MethodPost, "/stations", "key-e2e-station", `{"stationId":"s-e2e","capabilities":["pick"]}`); code != http.StatusCreated {
		t.Fatalf("POST /stations = %d %s, want 201", code, resp)
	}
	code, claimed, _ := e2e.do(t, http.MethodPost, "/stations/s-e2e/task-claims", "key-e2e-claim", `{"taskType":"PICK"}`)
	if code != http.StatusOK {
		t.Fatalf("task-claims = %d %s, want 200", code, claimed)
	}
	var task struct {
		Id       string `json:"id"`
		OrderRef string `json:"orderRef"`
	}
	if err := json.Unmarshal([]byte(claimed), &task); err != nil || task.Id == "" {
		t.Fatalf("task-claims body %s: %v", claimed, err)
	}
	if task.OrderRef != "order-e2e" {
		t.Fatalf("claimed %q, want the earlier-CPT Kafka-created order-e2e", task.OrderRef)
	}
	if code, resp, _ := e2e.do(t, http.MethodPost, fmt.Sprintf("/tasks/%s/completions", task.Id), "key-e2e-complete", `{"stationId":"s-e2e"}`); code/100 != 2 {
		t.Fatalf("complete = %d %s, want 2xx", code, resp)
	}
	if n := count(t, t.Context(), pool, "SELECT count(*) FROM tasks WHERE id = $1 AND status = 'COMPLETED'", task.Id); n != 1 {
		t.Fatalf("task %s not COMPLETED in Postgres", task.Id)
	}
}

// assertOutputEvents: the relay drained the outbox, and the CloudEvents 1.0
// messages are on both output topics (which the writer auto-created).
func assertOutputEvents(t *testing.T, ctx context.Context, e2e e2eEnv, pool *pgxpool.Pool) {
	t.Helper()
	waitFor(t, ctx, 60*time.Second, "the outbox drained by the relay", func() bool {
		return count(t, ctx, pool, "SELECT count(*) FROM outbox_events WHERE published_at IS NULL") == 0 &&
			count(t, ctx, pool, "SELECT count(*) FROM outbox_events WHERE published_at IS NOT NULL") > 0
	})
	integration := eventsOn(t, ctx, e2e.brokers, outboundkafka.Topic, "TaskClaimed", "TaskCompleted")
	for name, e := range integration {
		if !strings.Contains(e.Source(), "fulfillment-execution") || e.ID() == "" || e.DataSchema() == "" || e.DataContentType() != "application/json" {
			t.Errorf("%s: source=%q id=%q dataschema=%q datacontenttype=%q", name, e.Source(), e.ID(), e.DataSchema(), e.DataContentType())
		}
	}
	if !bytes.Contains(integration["TaskCompleted"].Data(), []byte(`"task_id"`)) && !bytes.Contains(integration["TaskCompleted"].Data(), []byte(`"taskId"`)) {
		t.Errorf("TaskCompleted data lacks a task id: %s", integration["TaskCompleted"].Data())
	}
	analytics := eventsOn(t, ctx, e2e.brokers, outboundkafka.AnalyticsTopic, "TaskCreated", "TaskCompleted")
	if len(analytics) != 2 {
		t.Errorf("analytics events = %v", keys(analytics))
	}
}
