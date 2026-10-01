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
	"github.com/claudioed/fulfillment-execution/internal/analytics/report"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/observability"
)

// AnalyticsConsumerGroup is the Kafka consumer group the analytics projector
// reads under. It is distinct from the OLTP consumer group so the two
// pipelines track their offsets independently.
const AnalyticsConsumerGroup = "fulfillment-analytics"

// Full CloudEvents `type` strings (ADR-0032) of the analytics occurrences
// this projector applies. The same `type` names the occurrence on both the
// integration and analytics topics; the analytics payload shape is named
// by `dataschema` (urn:warehouse:fulfillment-execution:analytics:...).
// Declared here (rather than imported from the outbound publisher) so this
// inbound adapter does not depend on an outbound adapter.
const (
	TypeTaskClaimed               = "com.warehouse.wes.fulfillment-execution.task.TaskClaimed"
	TypeTaskCompleted             = "com.warehouse.wes.fulfillment-execution.task.TaskCompleted"
	TypeLeaseExpired              = "com.warehouse.wes.fulfillment-execution.task.LeaseExpired"
	TypeWeightDiscrepancyDetected = "com.warehouse.wes.fulfillment-execution.package.WeightDiscrepancyDetected"
	TypePackageManifested         = "com.warehouse.wes.fulfillment-execution.package.PackageManifested"
)

// analyticsData is the union of fields the projecting event payloads carry.
// Each event type populates the subset it needs. TaskType is enriched onto
// task-scoped events by the publisher (via a TaskRepo lookup) since the domain
// events do not carry it. OnTime/Resolved are PackageManifested-only
// (ADR-0026): Resolved reports whether the publisher could correlate the
// package back to an originating SLAM task at all (see
// AnalyticsPublisher.onTimeToCPTFields) — when false, TaskType/StationId/
// OnTime are meaningless and the projection is skipped rather than recorded
// with a wrong dimension.
type analyticsData struct {
	TaskId    string `json:"task_id"`
	TaskType  string `json:"task_type"`
	StationId string `json:"station_id"`
	PackageId string `json:"package_id"`
	OnTime    bool   `json:"on_time"`
	Resolved  bool   `json:"resolved"`
}

// AnalyticsConsumer reads analytics events off the analytics topic and
// applies each to the throughput ProjectionStore, exactly once per
// CloudEvents id despite Kafka's at-least-once delivery.
type AnalyticsConsumer struct {
	Reader     *kafkago.Reader
	Projection report.ProjectionStore
	Processed  ports.ProcessedEvents
	Logger     *slog.Logger
}

// NewAnalyticsConsumer constructs an AnalyticsConsumer reading topic from
// brokers under AnalyticsConsumerGroup.
func NewAnalyticsConsumer(brokers []string, topic string, projection report.ProjectionStore, processed ports.ProcessedEvents, logger *slog.Logger) *AnalyticsConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: AnalyticsConsumerGroup,
		// Start a brand-new consumer group at the EARLIEST offset. The
		// analytics projection must see the full history of the topic (it is a
		// replayable read model, not a live integration reaction), so a fresh
		// projector — or a backfill into a new group — reads from the
		// beginning rather than kafka-go's default of the latest offset, which
		// would silently drop every event produced before the group first
		// committed an offset. Once the group has committed offsets, those
		// take precedence and this only affects the first join.
		StartOffset: kafkago.FirstOffset,
	})
	return &AnalyticsConsumer{Reader: reader, Projection: projection, Processed: processed, Logger: logger}
}

// Run reads and handles messages until ctx is cancelled or the reader
// returns a fatal error. A handling error is logged and the loop continues
// so one bad message cannot wedge the projector. A message that is not a
// valid CloudEvents 1.0 event (including the retired flat envelope) is a
// deterministic poison message: it is logged at WARN with its
// topic/partition/offset and committed past, never parsed any other way.
func (c *AnalyticsConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if err := c.Handle(ctx, msg); err != nil {
			if errors.Is(err, cloudevents.ErrNotCloudEvent) {
				c.Logger.WarnContext(ctx, "skipping analytics message that is not a valid CloudEvent",
					"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
				continue
			}
			c.Logger.ErrorContext(ctx, "analytics message handling failed", "error", err)
		}
	}
}

// Close releases the underlying Kafka reader.
func (c *AnalyticsConsumer) Close() error {
	return c.Reader.Close()
}

// Handle processes one consumed message inside a "kafka.consume <topic>"
// span whose parent is the producer's span, read from the message headers.
// It is exported separately from Run so the propagation can be tested without
// a live broker.
func (c *AnalyticsConsumer) Handle(ctx context.Context, msg kafkago.Message) error {
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

	if err := c.HandleMessage(ctx, msg.Value); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// HandleMessage decodes raw as a CloudEvents 1.0 event and applies the
// matching projection method for its full `type`. Types outside the
// projection contract are ignored (and not marked processed). For a
// projecting event it dedupes on the CloudEvents id via ProcessedEvents
// before applying, so a redelivery is a no-op; the occurrence instant is
// the `time` attribute and the payload is read via DataAs. A message that
// fails CloudEvents validation returns an error wrapping
// cloudevents.ErrNotCloudEvent. It is exported separately from Run so tests
// can feed raw events without a live broker.
func (c *AnalyticsConsumer) HandleMessage(ctx context.Context, raw []byte) error {
	e, err := cloudevents.Decode(raw)
	if err != nil {
		return fmt.Errorf("analytics: %w", err)
	}

	// Only the five throughput-moving events project; everything else
	// (TaskCreated, ItemPicked, PackageSealed, LabelApplied, PackageDiverted)
	// is acknowledged without touching the read model or the processed set.
	switch e.Type() {
	case TypeTaskClaimed, TypeTaskCompleted, TypeLeaseExpired, TypeWeightDiscrepancyDetected, TypePackageManifested:
	default:
		return nil
	}

	eventId, occurredAt := e.ID(), e.Time()
	isNew, err := c.Processed.MarkProcessed(ctx, eventId)
	if err != nil {
		return fmt.Errorf("analytics: mark processed: %w", err)
	}
	if !isNew {
		return nil
	}

	var data analyticsData
	if err := e.DataAs(&data); err != nil {
		return fmt.Errorf("analytics: decode data: %w", err)
	}

	switch e.Type() {
	case TypeTaskClaimed:
		return c.Projection.ApplyTaskClaimed(ctx, eventId, data.TaskId, data.TaskType, data.StationId, occurredAt)
	case TypeTaskCompleted:
		return c.Projection.ApplyTaskCompleted(ctx, eventId, data.TaskId, data.TaskType, data.StationId, occurredAt)
	case TypeLeaseExpired:
		return c.Projection.ApplyLeaseExpired(ctx, eventId, data.TaskId, data.TaskType, data.StationId, occurredAt)
	case TypeWeightDiscrepancyDetected:
		return c.Projection.ApplyWeightDiscrepancy(ctx, eventId, taskTypeSlam, data.StationId, occurredAt)
	case TypePackageManifested:
		// The publisher's enrichment lookup (onTimeToCPTFields) could not
		// correlate this package to an originating SLAM task — an edge
		// case that should not happen in practice (see ADR-0026). Skip
		// recording rather than projecting a wrong/empty dimension; the
		// event is still marked processed above so a redelivery is a
		// no-op, matching this consumer's idempotency contract.
		if !data.Resolved {
			return nil
		}
		return c.Projection.ApplyPackageManifested(ctx, eventId, data.TaskType, data.StationId, occurredAt, data.OnTime)
	default:
		return nil
	}
}

// taskTypeSlam is the process path a weigh-check divert belongs to: SLAM.
const taskTypeSlam = "SLAM"
