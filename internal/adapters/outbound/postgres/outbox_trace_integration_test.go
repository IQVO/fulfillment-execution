//go:build integration

// D27 (trace continuity across the outbox): the W3C trace context of the
// transaction that wrote an outbox row crosses the outbox to Kafka. This repo
// already stores it (outbox_events.headers, migration 0009; both encoders
// Inject from the use case ctx; the relay forwards the stored headers), so
// these two tests pin the behaviour; they are copied from the fleet
// reference. Real Postgres (the package's shared testcontainer) and a real
// Kafka broker (testcontainers), never KAFKA_BROKERS or localhost.
package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
	"github.com/claudioed/fulfillment-execution/internal/observability"
)

// installW3CTracing installs a real SDK tracer provider and the W3C
// trace-context propagator globally (what observability.Setup does in
// production), restoring the previous globals when the test ends.
func installW3CTracing(t *testing.T) trace.Tracer {
	t.Helper()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return tp.Tracer("outbox-trace-itest")
}

// startTraceKafka boots a disposable broker and pre-creates topics so the
// relay's first write does not race topic auto-creation.
func startTraceKafka(t *testing.T, ctx context.Context, topics ...string) []string {
	t.Helper()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("fulfillment-execution-outbox-trace-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate Kafka container: %v", err)
		}
	})
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial Kafka: %v", err)
	}
	defer func() { _ = conn.Close() }()
	for _, topic := range topics {
		if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
			t.Fatalf("create topic %s: %v", topic, err)
		}
	}
	return brokers
}

func readOneMessage(t *testing.T, ctx context.Context, brokers []string, topic string) kafkago.Message {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: topic, StartOffset: kafkago.FirstOffset, MaxWait: 2 * time.Second,
	})
	defer func() { _ = reader.Close() }()
	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("read %s: %v", topic, err)
	}
	return msg
}

func headerValue(headers []kafkago.Header, key string) (string, bool) {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// storedTraceparent returns the traceparent stored in the
// outbox_events.headers of the only row for eventType.
func storedTraceparent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventType string) string {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT headers FROM outbox_events WHERE event_type = $1`, eventType).Scan(&raw); err != nil {
		t.Fatalf("read stored headers: %v", err)
	}
	var headers []map[string]string
	if err := json.Unmarshal(raw, &headers); err != nil {
		t.Fatalf("decode stored headers %s: %v", raw, err)
	}
	for _, h := range headers {
		if h["key"] == "traceparent" {
			return h["value"]
		}
	}
	return ""
}

// TestOutboxTrace_TraceparentCrossesTheOutboxToKafka: a use case runs inside
// a request span; the outbox row stores that span's traceparent; the relay
// (a later pass, no span of its own) sends it as a Kafka header; and a
// consumer extracting it the way this repo's consumers do
// (observability.ExtractKafkaTrace) lands on the request's trace and span.
func TestOutboxTrace_TraceparentCrossesTheOutboxToKafka(t *testing.T) {
	tracer := installW3CTracing(t)
	s := newOutboxStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	brokers := startTraceKafka(t, ctx, outboundkafka.AnalyticsTopic)

	create := &usecases.CreateTask{Tasks: s.tasks, Publisher: s.pub, Clock: s.clock, NewId: taskIds("trace"), UnitOfWork: s.uow}
	reqCtx, reqSpan := tracer.Start(ctx, "POST /tasks", trace.WithSpanKind(trace.SpanKindServer))
	if _, err := create.Execute(reqCtx, task.Pick, shared.NewCPT(s.clock.t.Add(time.Hour)), "wu-trace", shared.NewCapabilitySet("pick"), false, false); err != nil {
		t.Fatalf("create: %v", err)
	}
	reqSpan.End()
	want := reqSpan.SpanContext()
	wantTraceparent := fmt.Sprintf("00-%s-%s-01", want.TraceID(), want.SpanID())

	const created = "com.warehouse.wes.fulfillment-execution.task.TaskCreated"
	if stored := storedTraceparent(t, ctx, s.pool, created); stored != wantTraceparent {
		t.Fatalf("outbox row traceparent = %q, want the request span's %q", stored, wantTraceparent)
	}

	sink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = sink.Close() })
	relay := postgres.NewOutboxRelay(s.pool, sink, slog.Default(), postgres.WithMaxAttempts(postgres.DefaultOutboxMaxAttempts))
	// The relay runs without the request's context: continuity must come
	// from the stored row, not from the caller.
	if n, err := relay.RelayOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("relay: n=%d err=%v", n, err)
	}

	msg := readOneMessage(t, ctx, brokers, outboundkafka.AnalyticsTopic)
	got, ok := headerValue(msg.Headers, "traceparent")
	if !ok || got != wantTraceparent {
		t.Fatalf("Kafka traceparent = %q (present=%v), want %q", got, ok, wantTraceparent)
	}
	if ct, _ := headerValue(msg.Headers, "content-type"); ct == "" {
		t.Fatalf("content-type header lost on the outbox path: %+v", msg.Headers)
	}

	parent := trace.SpanContextFromContext(observability.ExtractKafkaTrace(context.Background(), msg.Headers))
	if parent.TraceID() != want.TraceID() || parent.SpanID() != want.SpanID() || !parent.IsRemote() {
		t.Fatalf("consumer would parent on %s/%s (remote=%v), want the request span %s/%s",
			parent.TraceID(), parent.SpanID(), parent.IsRemote(), want.TraceID(), want.SpanID())
	}
}

// TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged: a row written with
// no trace context (no active span, or a row that predates trace capture)
// relays with exactly its stored key, value and headers, and carries no
// traceparent a consumer could mis-parent onto.
func TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged(t *testing.T) {
	installW3CTracing(t)
	pool := outboxDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const topic = "warehouse.fulfillment.itest-no-trace"
	brokers := startTraceKafka(t, ctx, topic)

	if _, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (topic, event_type, key, value)
		VALUES ($1, 'com.example.Untraced', $2, '{}')
	`, topic, []byte("task-untraced")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	sink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = sink.Close() })
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default(), postgres.WithMaxAttempts(postgres.DefaultOutboxMaxAttempts))
	if n, err := relay.RelayOnce(ctx); err != nil || n != 1 {
		t.Fatalf("relay: n=%d err=%v", n, err)
	}

	msg := readOneMessage(t, ctx, brokers, topic)
	if string(msg.Key) != "task-untraced" || string(msg.Value) != `{}` {
		t.Fatalf("row relayed changed: key=%q value=%q", msg.Key, msg.Value)
	}
	if len(msg.Headers) != 0 {
		t.Fatalf("a row without stored headers must relay with none, got %+v", msg.Headers)
	}
}
