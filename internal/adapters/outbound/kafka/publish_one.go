package kafka

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/observability"
)

// publishOneTo encodes and writes one event inside a PRODUCER span on topic,
// so the injected traceparent names the publish span itself (the consumer
// hangs off the publish, not its caller) and a broker error is recorded on
// it. what labels the event kind in the broker error ("" for the domain
// contract, " analytics event" for the analytics topic).
func publishOneTo(ctx context.Context, topic, what string, writer Writer,
	encode func(context.Context, ...shared.DomainEvent) ([]Encoded, error), e shared.DomainEvent) error {
	ctx, span := otel.Tracer(observability.InstrumentationName).Start(ctx,
		"kafka.publish "+topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(topic),
			semconv.MessagingOperationName("publish"),
		),
	)
	defer span.End()

	encoded, err := encode(ctx, e)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	for _, enc := range encoded {
		if err := writer.WriteMessages(ctx, enc.message()); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return fmt.Errorf("kafka: publish %s%s: %w", enc.EventType, what, err)
		}
	}
	return nil
}
