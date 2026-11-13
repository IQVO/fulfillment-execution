//go:build integration

package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/analyticsstore"
	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// The projector is the WRITER composition root of the throughput data
// product (ADR-0012): it owns the analytical schema, so this test boots it
// against an EMPTY testcontainers Postgres — no migration is run by the test
// — plus a real testcontainers Kafka, produces the repo's own encoder output
// onto the projector's one input topic (warehouse.fulfillment.analytics,
// CloudEvents 1.0, ADR-0032) and asserts the documented projection answers
// (docs/docs/analytics/throughput-report.md): one rollup row per
// (task type x station x hour bucket), completions and the claim-to-complete
// latency from TaskClaimed/TaskCompleted, leaseExpiries from LeaseExpired.

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(120 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// projectorTasks is a TaskRepo double for the ENCODER's task_type
// enrichment (the publisher looks the process path up per task-scoped
// event, ADR-0012): every task is a PICK task. Only FindById is meaningful;
// the rest satisfy the port. The in-memory adapter package is forbidden in
// cmd/** wiring, so the double lives here inline.
type projectorTasks struct{}

func (projectorTasks) FindById(_ context.Context, id shared.TaskId) (*task.Task, error) {
	return task.New(id, task.Pick, shared.NewCPT(time.Now().Add(time.Hour)), "order-projector-it", shared.NewCapabilitySet(), false, false), nil
}
func (projectorTasks) Save(context.Context, *task.Task) error { return nil }
func (projectorTasks) SaveClaim(context.Context, *task.Task, time.Time) (bool, error) {
	return true, nil
}
func (projectorTasks) FindClaimableByType(context.Context, task.Type, time.Time) ([]*task.Task, error) {
	return nil, nil
}
func (projectorTasks) FindAllClaimed(context.Context) ([]*task.Task, error) { return nil, nil }
func (projectorTasks) FindOpenPastCPT(context.Context, time.Time) ([]*task.Task, error) {
	return nil, nil
}
func (projectorTasks) CountByTypeAndStatus(context.Context, task.Type, task.Status) (int, error) {
	return 0, nil
}
func (projectorTasks) FindByOrderRef(context.Context, shared.OrderRef) ([]*task.Task, error) {
	return nil, nil
}
func (projectorTasks) FindPageByOrderRef(context.Context, shared.OrderRef, shared.TaskId, int) ([]*task.Task, error) {
	return nil, nil
}

// stopOnSignal delivers SIGTERM to this process until run() returns, so the
// test sees the entry point's real shutdown path (waitForShutdown drains on
// SIGINT/SIGTERM). The test registers its own signal.Notify BEFORE starting
// run(), so the signal is never left to the process's default disposition
// during the window before waitForShutdown's own Notify.
func stopOnSignal(t *testing.T, done <-chan error) error {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
		select {
		case err := <-done:
			return err
		case <-time.After(300 * time.Millisecond):
		}
	}
	t.Fatal("run() did not return after SIGTERM")
	return nil
}

// TestProjectorBootsMigratesProjectsAndDrainsCleanly runs the REAL run()
// over an EMPTY Postgres (the binary runs the analytics migrations itself,
// ADR-0012) and a real Kafka: events encoded by the repo's own
// AnalyticsPublisher.Encode are produced to the analytics topic before boot
// (a fresh consumer group starts at the earliest offset, so pre-produced
// history is seen), the documented projection answers in throughput_rollup,
// and a SIGTERM drains the process cleanly.
func TestProjectorBootsMigratesProjectsAndDrainsCleanly(t *testing.T) {
	ctx := context.Background()

	pg, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("fulfillment_analytics"),
		tcpostgres.WithUsername("fx_analytics"),
		tcpostgres.WithPassword("fx_analytics"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(pg) })
	url, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}

	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("fe-projector-it"))
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(kc) })
	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}

	// The projector's single input topic, pre-created like the cluster
	// would have it (a group consumer that starts before its topic exists
	// does not pick it up when it appears).
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial kafka: %v", err)
	}
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: outboundkafka.AnalyticsTopic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic %s: %v", outboundkafka.AnalyticsTopic, err)
	}
	_ = conn.Close()

	// The repo's OWN encoder: AnalyticsPublisher.Encode builds the
	// CloudEvents 1.0 wire form (ADR-0032) of the domain events, enriched
	// with task_type via the TaskRepo lookup, exactly as the outbox path
	// stores it. A claim, its completion 90s later, and one lease expiry:
	// the three throughput-moving occurrences of the documented contract.
	at := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	seq := 0
	encoder := outboundkafka.NewAnalyticsPublisherWithWriter(nil, projectorTasks{}, func() string {
		seq++
		return "projector-it-" + time.Now().Format("150405.000000000") + "-" + string(rune('a'+seq))
	})
	encoded, err := encoder.Encode(ctx,
		shared.NewTaskClaimed("T-PROJ-COMPLETE", "ST-PROJ-A", at),
		shared.NewTaskCompleted("T-PROJ-COMPLETE", "ST-PROJ-A", at.Add(90*time.Second)),
		shared.NewLeaseExpired("T-PROJ-EXPIRED", at.Add(5*time.Minute)),
	)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	w := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: outboundkafka.AnalyticsTopic, RequiredAcks: kafkago.RequireAll, BatchTimeout: 10 * time.Millisecond, Balancer: &kafkago.Hash{}}
	for _, e := range encoded {
		// Only the five throughput-moving types project; these three all
		// do (guard against silently proving nothing if the encoder ever
		// changes what it emits for them).
		switch e.EventType {
		case inboundkafka.TypeTaskClaimed, inboundkafka.TypeTaskCompleted, inboundkafka.TypeLeaseExpired:
		default:
			t.Fatalf("encoder emitted %s, expected a projecting type", e.EventType)
		}
		if err := w.WriteMessages(ctx, kafkago.Message{Key: e.Key, Value: e.Value, Headers: e.Headers}); err != nil {
			t.Fatalf("produce %s: %v", e.EventType, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close producer: %v", err)
	}

	adminAddr := freeAddr(t)
	t.Setenv("ANALYTICS_DATABASE_URL", url)
	t.Setenv("KAFKA_BROKERS", strings.Join(brokers, ","))
	t.Setenv("ADMIN_ADDR", adminAddr)
	t.Setenv("LOG_LEVEL", "error")
	t.Setenv("ANALYTICS_MIGRATIONS_PATH", "../../migrations/analytics")
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")

	// Own the SIGTERM disposition BEFORE run() starts (see stopOnSignal).
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	defer signal.Stop(sig)

	done := make(chan error, 1)
	go func() { done <- run() }()

	waitUntil(t, "GET /healthz = 200 on the empty, self-migrated database", func() bool {
		resp, err := http.Get("http://" + adminAddr + "/healthz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	pool, err := analyticsstore.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("analytics pool: %v", err)
	}
	defer pool.Close()

	row := func(station string) (completions, withClaim, expiries int, seconds float64, ok bool) {
		err := pool.QueryRow(ctx, `SELECT completions, completions_with_claim, lease_expiries, claim_to_complete_seconds
			FROM throughput_rollup WHERE task_type = 'PICK' AND station_id = $1 AND hour_bucket = $2`,
			station, at).Scan(&completions, &withClaim, &expiries, &seconds)
		return completions, withClaim, expiries, seconds, err == nil
	}

	// The documented projection answers (throughput-report.md): the
	// claim+complete pair rolls up to one completion whose claim-to-complete
	// latency is 90s on the claiming station's row; the LeaseExpired
	// occurrence carries no station on the wire, so its row is attributed
	// to the empty station id of the analytics payload.
	waitUntil(t, "the claim/complete rollup row", func() bool {
		c, wc, _, s, ok := row("ST-PROJ-A")
		return ok && c == 1 && wc == 1 && s == 90
	})
	waitUntil(t, "the lease-expiry rollup row", func() bool {
		_, _, le, _, ok := row("")
		return ok && le == 1
	})

	if err := stopOnSignal(t, done); err != nil {
		t.Fatalf("a SIGTERMed projector must drain cleanly, got %v", err)
	}
}
