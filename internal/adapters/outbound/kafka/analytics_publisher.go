package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
	"github.com/claudioed/fulfillment-execution/internal/observability"
)

// AnalyticsTopic is the dedicated topic the analytics data product consumes.
// It is separate from the integration topic (Topic) so the OLTP integration
// contract and the analytical read-model stream evolve independently
// (ADR-0012).
const AnalyticsTopic = "warehouse.fulfillment.analytics"

// analyticsSchemaVersion is the dataschema version
// (urn:warehouse:fulfillment-execution:analytics:<Event>:v1) stamped onto
// every analytics CloudEvent this publisher emits. It replaces the retired
// flat analytics envelope's schema-version field (ADR-0032).
const analyticsSchemaVersion = 1

// AnalyticsPublisher publishes every fulfillment-execution domain event onto
// AnalyticsTopic as a CloudEvents 1.0 structured-mode message (ADR-0032),
// with the same `type` as the occurrence on the integration topic and an
// analytics `dataschema`. It satisfies ports.EventPublisher and is a
// SEPARATE adapter from Publisher.
//
// Task-scoped events are enriched with the owning task's process path
// (task_type) via a TaskRepo lookup — the same repo-lookup-enrichment pattern
// Publisher uses for OrderRef — because the domain events themselves stay thin
// and do not carry the task type (see ADR-0012). The report is keyed by
// task_type, so this enrichment is what populates that dimension.
type AnalyticsPublisher struct {
	Writer Writer
	Tasks  ports.TaskRepo
	NewId  func() string
}

// NewAnalyticsPublisher constructs an AnalyticsPublisher writing to
// AnalyticsTopic on brokers. newId mints the CloudEvents id; tasks is used
// to enrich task-scoped events with their process path.
func NewAnalyticsPublisher(brokers []string, tasks ports.TaskRepo, newId func() string) *AnalyticsPublisher {
	return NewAnalyticsPublisherWithWriter(&kafkago.Writer{
		BatchTimeout: syncWriterBatchTimeout,
		RequiredAcks: syncWriterRequiredAcks,
		Addr:         kafkago.TCP(brokers...),
		Topic:        AnalyticsTopic,
		// Balancer is kafkago.Hash, matching Publisher's
		// NewPublisher choice: marshalData already keys
		// every message by its aggregate id (TaskId/PackageId), but
		// LeastBytes would silently discard that key for partition
		// routing — Hash is what actually turns the key into a
		// partition decision.
		Balancer:               &kafkago.Hash{},
		AllowAutoTopicCreation: true,
	}, tasks, newId)
}

// NewAnalyticsPublisherWithWriter constructs an AnalyticsPublisher over an
// explicit Writer. When used only as an Encoder (the outbox configuration,
// ADR 0020) the writer may be nil — Encode never touches it.
func NewAnalyticsPublisherWithWriter(w Writer, tasks ports.TaskRepo, newId func() string) *AnalyticsPublisher {
	return &AnalyticsPublisher{Writer: w, Tasks: tasks, NewId: newId}
}

// Encode builds the CloudEvents wire form of every event in evts WITHOUT
// sending it. Events with no analytics payload (an unrecognised type) are
// skipped rather than erroring, so the caller can hand it the full event
// stream indiscriminately. The task_type enrichment lookup happens here, so
// inside a use case's transaction (the outbox path) it sees the just-saved
// task. Every message carries the CloudEvents content-type header and the
// active span on ctx injected as W3C trace context.
func (p *AnalyticsPublisher) Encode(ctx context.Context, evts ...shared.DomainEvent) ([]Encoded, error) {
	var out []Encoded
	for _, e := range evts {
		entity, eventName, key, data, ok := p.marshalData(ctx, e)
		if !ok {
			continue
		}
		payload, err := cloudevents.New(cloudevents.Spec{
			ID:        p.NewId(),
			Entity:    entity,
			EventName: eventName,
			Subject:   key,
			Time:      e.OccurredAt(),
			Stream:    cloudevents.StreamAnalytics,
			Version:   analyticsSchemaVersion,
			Data:      data,
		})
		if err != nil {
			return nil, fmt.Errorf("kafka: encode analytics %s: %w", eventName, err)
		}
		enc := Encoded{
			Topic:     AnalyticsTopic,
			EventType: cloudevents.Type(entity, eventName),
			Key:       []byte(key),
			Value:     payload,
			Headers:   []kafkago.Header{cloudevents.ContentTypeHeader()},
		}
		observability.InjectKafkaTrace(ctx, &enc.Headers)
		out = append(out, enc)
	}
	return out, nil
}

// Publish emits every event in evts onto AnalyticsTopic, each encoded and
// written inside its own "kafka.publish <topic>" producer span. Events
// outside the analytics contract are skipped.
func (p *AnalyticsPublisher) Publish(ctx context.Context, evts ...shared.DomainEvent) error {
	for _, e := range evts {
		if !inAnalyticsContract(e) {
			continue
		}
		if err := p.publishOne(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// inAnalyticsContract reports whether marshalData knows e, without the
// repo lookup marshalData performs — so Publish can skip foreign events
// before opening a span, and Encode's lookup runs exactly once per event.
func inAnalyticsContract(e shared.DomainEvent) bool {
	switch e.(type) {
	case shared.TaskCreated, shared.TaskClaimed, shared.LeaseExpired, shared.TaskCompleted, shared.ItemPicked,
		shared.PackageSealed, shared.WeightDiscrepancyDetected, shared.LabelApplied, shared.PackageDiverted,
		shared.PackageManifested:
		return true
	default:
		return false
	}
}

// publishOne encodes and writes one event inside a producer span, so the
// injected traceparent names the publish span itself and a broker error
// is recorded on it.
func (p *AnalyticsPublisher) publishOne(ctx context.Context, e shared.DomainEvent) error {
	ctx, span := otel.Tracer(observability.InstrumentationName).Start(ctx,
		"kafka.publish "+AnalyticsTopic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(AnalyticsTopic),
			semconv.MessagingOperationName("publish"),
		),
	)
	defer span.End()

	encoded, err := p.Encode(ctx, e)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	for _, enc := range encoded {
		if err := p.Writer.WriteMessages(ctx, enc.message()); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return fmt.Errorf("kafka: publish %s analytics event: %w", enc.EventType, err)
		}
	}
	return nil
}

// taskType looks up the process path of the task with id, returning "" when
// the task cannot be found (a best-effort enrichment: a missing task_type
// leaves the report's process-path dimension unspecified rather than failing
// the publish).
func (p *AnalyticsPublisher) taskType(ctx context.Context, id shared.TaskId) string {
	if p.Tasks == nil {
		return ""
	}
	t, err := p.Tasks.FindById(ctx, id)
	if err != nil || t == nil {
		return ""
	}
	return string(t.Type())
}

// onTimeToCPTFields resolves a PackageManifested event's on-time-to-CPT
// enrichment: the originating SLAM task's process path/station and whether
// manifestedAt was on time against that task's CPT. It correlates via
// TaskRepo.FindByOrderRef — a package carries no TaskId of its own — and
// picks the SLAM-type task among the order's tasks (a Pick/Pack/Rebin leg
// for the same order is not the one a manifest event is measured against).
// found is false (and every other return zero) when no SLAM task can be
// resolved for orderRef — an edge case that should not happen in practice
// (every Package descends from a SLAM task by construction) but is handled
// defensively: the caller skips recording rather than failing the whole
// publish, mirroring this fleet's fail-soft convention for a best-effort
// enrichment lookup (see taskType above).
//
// The on-time boundary is manifestedAt <= cpt counts as on-time — the
// deliberate mirror of task.Task.IsCPTMissed's own boundary (now >= cpt
// counts as missed), so the two predicates are consistent at the instant: a
// package manifested exactly at CPT is a promise kept (see ADR-0026).
func (p *AnalyticsPublisher) onTimeToCPTFields(ctx context.Context, orderRef shared.OrderRef, manifestedAt time.Time) (taskType, stationId string, onTime, found bool) {
	if p.Tasks == nil {
		return "", "", false, false
	}
	tasks, err := p.Tasks.FindByOrderRef(ctx, orderRef)
	if err != nil {
		return "", "", false, false
	}
	for _, t := range tasks {
		if t.Type() != task.Slam {
			continue
		}
		station := ""
		if lease := t.Lease(); lease != nil {
			station = string(lease.StationId)
		}
		return string(t.Type()), station, !manifestedAt.After(t.CPT().Time()), true
	}
	return "", "", false, false
}

// marshalData maps a domain event to its CloudEvents entity segment and
// event name, aggregate-id message key/subject, and snake_case JSON payload. The bool return is false for an
// event type outside the analytics contract, so Publish can skip it.
// Task-scoped events are enriched with task_type via a TaskRepo lookup.
func (p *AnalyticsPublisher) marshalData(ctx context.Context, e shared.DomainEvent) (entity, eventName, key string, data json.RawMessage, ok bool) {
	switch ev := e.(type) {
	case shared.TaskCreated:
		return entityTask, "TaskCreated", string(ev.TaskId), mustMarshal(map[string]any{
			"task_id":   string(ev.TaskId),
			"task_type": p.taskType(ctx, ev.TaskId),
		}), true
	case shared.TaskClaimed:
		return entityTask, "TaskClaimed", string(ev.TaskId), mustMarshal(map[string]any{
			"task_id":    string(ev.TaskId),
			"task_type":  p.taskType(ctx, ev.TaskId),
			"station_id": string(ev.StationId),
		}), true
	case shared.LeaseExpired:
		return entityTask, "LeaseExpired", string(ev.TaskId), mustMarshal(map[string]any{
			"task_id":   string(ev.TaskId),
			"task_type": p.taskType(ctx, ev.TaskId),
		}), true
	case shared.TaskCompleted:
		return entityTask, "TaskCompleted", string(ev.TaskId), mustMarshal(map[string]any{
			"task_id":    string(ev.TaskId),
			"task_type":  p.taskType(ctx, ev.TaskId),
			"station_id": string(ev.StationId),
		}), true
	case shared.ItemPicked:
		return entityTask, "ItemPicked", string(ev.TaskId), mustMarshal(map[string]any{
			"task_id":   string(ev.TaskId),
			"task_type": p.taskType(ctx, ev.TaskId),
		}), true
	case shared.PackageSealed:
		return entityPackage, "PackageSealed", string(ev.PackageId), mustMarshal(map[string]any{
			"package_id": string(ev.PackageId),
		}), true
	case shared.WeightDiscrepancyDetected:
		return entityPackage, "WeightDiscrepancyDetected", string(ev.PackageId), mustMarshal(map[string]any{
			"package_id": string(ev.PackageId),
			"expected_g": ev.ExpectedWeight,
			"actual_g":   ev.ActualWeight,
		}), true
	case shared.LabelApplied:
		return entityPackage, "LabelApplied", string(ev.PackageId), mustMarshal(map[string]any{
			"package_id": string(ev.PackageId),
		}), true
	case shared.PackageDiverted:
		return entityPackage, "PackageDiverted", string(ev.PackageId), mustMarshal(map[string]any{
			"package_id": string(ev.PackageId),
		}), true
	case shared.PackageManifested:
		taskType, stationId, onTime, resolved := p.onTimeToCPTFields(ctx, ev.OrderRef, ev.OccurredAt())
		return entityPackage, "PackageManifested", string(ev.PackageId), mustMarshal(map[string]any{
			"package_id": string(ev.PackageId),
			"order_ref":  string(ev.OrderRef),
			"task_type":  taskType,
			"station_id": stationId,
			"on_time":    onTime,
			"resolved":   resolved,
		}), true
	default:
		return "", "", "", nil, false
	}
}

// mustMarshal marshals a map whose shape is fully controlled by marshalData,
// so an error here is a programming mistake rather than a runtime condition.
func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("kafka: marshal analytics data: %v", err))
	}
	return b
}

// Close releases the underlying Kafka writer.
func (p *AnalyticsPublisher) Close() error {
	if w, ok := p.Writer.(*kafkago.Writer); ok {
		return w.Close()
	}
	return nil
}

// Compile-time assertion that AnalyticsPublisher satisfies the outbound
// event-publishing port.
var _ ports.EventPublisher = (*AnalyticsPublisher)(nil)
