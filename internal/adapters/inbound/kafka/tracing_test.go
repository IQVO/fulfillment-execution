package kafka_test

import (
	"context"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/observability"
)

// ctxCapturingProcessedEvents records the context the use case chain runs
// under, so a test can inspect the span active deep inside message handling.
type ctxCapturingProcessedEvents struct {
	inner ports.ProcessedEvents
	ctx   context.Context
}

func (p *ctxCapturingProcessedEvents) MarkProcessed(ctx context.Context, eventId string) (bool, error) {
	p.ctx = ctx
	return p.inner.MarkProcessed(ctx, eventId)
}

func installRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	return recorder
}

func workReleasedMessage(t *testing.T, eventId string) kafkago.Message {
	t.Helper()

	payload := workReleasedCE(eventId, kafka.TypeWorkReleased, "wu-1", map[string]any{
		"path_id":      "PICK",
		"work_unit_id": "wu-1",
		"cpt":          epoch,
		"ref":          "order-1",
	})
	return kafkago.Message{Topic: "warehouse.work-planning.events", Value: payload}
}

// TestHandle_JoinsTheProducersTrace is the consumer half of the end-to-end
// trace across the wes-work-planning -> fulfillment-execution boundary: the
// span started around handling the message must be a child of the span that
// published it, carried purely over the message headers.
func TestHandle_JoinsTheProducersTrace(t *testing.T) {
	recorder := installRecorder(t)

	c, _ := newConsumer(t)
	captured := &ctxCapturingProcessedEvents{inner: memory.NewProcessedEventsRepo()}
	c.Processed = captured

	// Stand in for wes-work-planning's producer span.
	producerCtx, producerSpan := otel.Tracer("producer").Start(context.Background(), "kafka.publish warehouse.work-planning.events")
	msg := workReleasedMessage(t, "evt-1")
	observability.InjectKafkaTrace(producerCtx, &msg.Headers)
	producerSpan.End()

	if err := c.Handle(context.Background(), msg); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if captured.ctx == nil {
		t.Fatal("the use case chain never ran")
	}
	sc := trace.SpanContextFromContext(captured.ctx)
	if !sc.IsValid() {
		t.Fatal("no span active while handling the message")
	}
	if sc.TraceID() != producerSpan.SpanContext().TraceID() {
		t.Errorf("handling ran in trace %s, want the producer's %s", sc.TraceID(), producerSpan.SpanContext().TraceID())
	}

	var consumeSpan bool
	for _, s := range recorder.Ended() {
		if s.Name() == "kafka.consume warehouse.work-planning.events" {
			consumeSpan = true
			if s.SpanKind() != trace.SpanKindConsumer {
				t.Errorf("span kind = %v, want consumer", s.SpanKind())
			}
			if s.Parent().SpanID() != producerSpan.SpanContext().SpanID() {
				t.Errorf("parent = %s, want the producer span %s", s.Parent().SpanID(), producerSpan.SpanContext().SpanID())
			}
		}
	}
	if !consumeSpan {
		t.Errorf("no kafka.consume span recorded; got %v", spanNames(recorder))
	}
}

// TestHandle_StartsARootSpanWhenNoTraceContextIsPresent covers a producer
// that is not yet instrumented: handling still gets a span, just a new
// trace rather than a continued one.
func TestHandle_StartsARootSpanWhenNoTraceContextIsPresent(t *testing.T) {
	recorder := installRecorder(t)

	c, tasks := newConsumer(t)

	if err := c.Handle(context.Background(), workReleasedMessage(t, "evt-2")); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if got := totalPending(t, tasks); got != 1 {
		t.Fatalf("pending tasks = %d, want 1", got)
	}

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("recorded spans = %v, want exactly the consume span", spanNames(recorder))
	}
	if ended[0].Parent().IsValid() {
		t.Errorf("consume span has parent %s, want a root span", ended[0].Parent().SpanID())
	}
}

func TestHandle_MarksTheSpanFailedOnAHandlingError(t *testing.T) {
	recorder := installRecorder(t)

	c, _ := newConsumer(t)

	err := c.Handle(context.Background(), kafkago.Message{
		Topic: "warehouse.work-planning.events",
		Value: []byte("not json"),
	})
	if err == nil {
		t.Fatal("expected a decode error")
	}

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("recorded spans = %v, want exactly the consume span", spanNames(recorder))
	}
	if len(ended[0].Events()) == 0 {
		t.Error("the error was not recorded on the span")
	}
}

func spanNames(recorder *tracetest.SpanRecorder) []string {
	names := make([]string, 0, len(recorder.Ended()))
	for _, s := range recorder.Ended() {
		names = append(names, s.Name())
	}
	return names
}
