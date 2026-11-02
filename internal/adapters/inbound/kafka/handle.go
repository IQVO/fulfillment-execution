package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/fulfillment-execution/internal/observability"
)

// handleConsumedEvent is the shared body of the integration consumers'
// Handle (InterleavingHint, PackLevel, ProductClassified): it opens a
// "kafka.consume <topic>" CONSUMER span on the trace context extracted from
// the message headers, decodes the payload, WARN-logs and skips a message
// that is not a CloudEvent or whose payload is invalid (invalidErr, logged
// as invalidWarn), and applies the decoded request, recording an apply
// failure on the span. idOf names the event in that failure; applyWhat is
// the human label ("InterleavingHintIssued", "pack level", ...). Consumers
// whose settle loop differs keep a hand-written Handle.
func handleConsumedEvent[T any](ctx context.Context, msg kafkago.Message, logger *slog.Logger,
	decode func([]byte) (T, bool, error), invalidErr error, invalidWarn, applyWhat string,
	idOf func(T) string, apply func(context.Context, T) error) error {
	ctx = observability.ExtractKafkaTrace(ctx, msg.Headers)
	ctx, span := otel.Tracer(observability.InstrumentationName).Start(ctx,
		"kafka.consume "+msg.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(msg.Topic),
			semconv.MessagingOperationName("process"),
		),
	)
	defer span.End()

	req, ok, err := decode(msg.Value)
	switch {
	case errors.Is(err, cloudevents.ErrNotCloudEvent):
		logger.WarnContext(ctx, "skipping kafka message that is not a valid CloudEvent",
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	case errors.Is(err, invalidErr):
		logger.WarnContext(ctx, invalidWarn,
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	case err != nil:
		return err
	case !ok:
		return nil
	}

	if err := apply(ctx, req); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("kafka: apply %s %s: %w", applyWhat, idOf(req), err)
	}
	return nil
}
