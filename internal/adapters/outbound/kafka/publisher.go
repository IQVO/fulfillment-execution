// Package kafka provides the outbound adapter that publishes
// fulfillment-execution domain events onto Kafka, satisfying
// ports.EventPublisher. It forwards TaskCompleted, TaskCPTMissed, and
// PackageManifested — every other event type is not yet part of the
// published integration contract and is skipped. TaskCompleted is
// enriched with the completed Task's OrderRef (looked up via TaskRepo,
// since the domain event itself carries only TaskId/StationId) — the same
// repo-lookup-enrichment pattern inventory-storage's Kafka publisher uses
// for ReservationRevoked. It also enriches TaskCompleted with the
// completing associate's identity, the task's duration, and the task's own
// type (PICK/PACK/SLAM/REBIN), resolved via a StationRepo lookup, the
// Task's ClaimedAt timestamp, and the already-loaded Task's Type()
// respectively (see ADR-0014, ADR-0023) — inputs the labor-performance
// bounded context needs and that this service is the sole owner of.
// TaskCPTMissed and PackageManifested need no repo enrichment: both carry
// every field their wire payload needs directly on the domain event
// itself (see ADR-0025).
//
// Every message is a CloudEvents 1.0 event in structured content mode
// built by internal/adapters/kafka/cloudevents (ADR-0032); there is no
// other envelope.
package kafka

import (
	"context"
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
	"github.com/claudioed/fulfillment-execution/internal/observability"
)

// Topic is the topic fulfillment-execution publishes its outbound
// integration events to.
const Topic = "warehouse.fulfillment.events"

// Entity segments of the CloudEvents `type` attribute
// (com.warehouse.wes.fulfillment-execution.<entity>.<EventName>): the
// aggregate that raised the event, as already catalogued in
// apis/asyncapi.yaml.
const (
	entityTask     = "task"
	entityPackage  = "package"
	entityTransfer = "transfer"
)

// TaskCompletedData is the payload of a published TaskCompleted event,
// enriched with the completed Task's OrderRef as work_unit_id so Work
// Planning can call RecordCompletion(WorkUnitId), plus three
// labor-performance-relevant facts resolved at publish time (ADR-0014,
// ADR-0023): AssociateId (the occupant of the claiming station, if any —
// empty for a station with no checked-in occupant, e.g. a robot),
// DurationSeconds (elapsed time between the task's claim and its
// completion), and TaskType (this service's own task.Type — PICK, PACK,
// SLAM, or REBIN — read directly off the already-loaded Task, no new
// lookup needed).
type TaskCompletedData struct {
	TaskId          string `json:"task_id"`
	StationId       string `json:"station_id"`
	WorkUnitId      string `json:"work_unit_id"`
	AssociateId     string `json:"associate_id,omitempty"`
	DurationSeconds int64  `json:"duration_seconds,omitempty"`
	TaskType        string `json:"task_type,omitempty"`
}

// TaskCPTMissedData is the payload of a published TaskCPTMissed event: a
// task still open (Pending or Claimed) past its CPT deadline. OrderRef is
// what order-management's RepromiseOrder consumer (ADR 0014 §5) keys its
// recompute on; TaskType and Cpt let it reason about which leg missed and
// how late without a repo lookup back into this service. Every field
// comes straight off the domain event — no repo enrichment needed (unlike
// TaskCompleted).
type TaskCPTMissedData struct {
	TaskId   string    `json:"task_id"`
	OrderRef string    `json:"order_ref"`
	TaskType string    `json:"task_type,omitempty"`
	Cpt      time.Time `json:"cpt"`
}

// PackageManifestedData is the payload of a published PackageManifested
// event: a package that passed its SLAM weigh-check. OrderRef is what
// order-management's RepromiseOrder consumer keys its recompute on.
type PackageManifestedData struct {
	PackageId string `json:"package_id"`
	OrderRef  string `json:"order_ref"`
}

// TransferFactData is the payload of a published TransferPicked,
// TransferDispatched or TransferArrived event: the inter-warehouse-
// transfer correlation the completed Task carried, stamped at release
// time. Every field comes straight off the domain event — no repo
// enrichment needed. task_id is the message key and the CloudEvents
// subject, so per-task ordering holds; transfer_ref/work_unit_id are the
// correlation keys downstream sagas dispatch on.
type TransferFactData struct {
	TransferRef string `json:"transfer_ref"`
	DemandId    string `json:"demand_id,omitempty"`
	WorkUnitId  string `json:"work_unit_id"`
	TaskId      string `json:"task_id"`
	WorkKind    string `json:"work_kind"`
	SiteId      string `json:"site_id,omitempty"`
	SKU         string `json:"sku,omitempty"`
	Quantity    int    `json:"quantity,omitempty"`
}

// Writer is the subset of *kafkago.Writer the Publisher needs, so tests can
// substitute a fake without a live broker.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// Publisher publishes fulfillment-execution domain events onto Kafka as
// CloudEvents 1.0 structured-mode messages (ADR-0032). It satisfies
// ports.EventPublisher. Event types other than TaskCompleted,
// TaskCPTMissed, and PackageManifested are not part of the published
// integration contract and are skipped.
type Publisher struct {
	Writer   Writer
	Tasks    ports.TaskRepo
	Stations ports.StationRepo
	// NewId mints the CloudEvents `id`, exactly once per domain event, in
	// Encode — so the outbox persists, and the relay republishes, the
	// same id on every redelivery.
	NewId func() string
}

// NewPublisher constructs a Publisher writing to Topic on brokers.
func NewPublisher(brokers []string, tasks ports.TaskRepo, stations ports.StationRepo, newId func() string) *Publisher {
	return NewPublisherWithWriter(&kafkago.Writer{
		BatchTimeout: syncWriterBatchTimeout,
		RequiredAcks: syncWriterRequiredAcks,
		Addr:         kafkago.TCP(brokers...),
		Topic:        Topic,
		// Balancer is kafkago.Hash (FNV-1a over Message.Key), not
		// LeastBytes: this Publisher sets a per-aggregate Key
		// (TaskId/PackageId) on every message, but LeastBytes routes
		// purely by cumulative byte volume and ignores Key. Hash is the
		// balancer that actually gives "same Key always maps to the same
		// partition", which per-aggregate event ordering depends on.
		Balancer:               &kafkago.Hash{},
		AllowAutoTopicCreation: true,
	}, tasks, stations, newId)
}

// NewPublisherWithWriter constructs a Publisher over an explicit Writer.
// When used only as an Encoder (the outbox configuration, ADR 0020) the
// writer may be nil — Encode never touches it.
func NewPublisherWithWriter(w Writer, tasks ports.TaskRepo, stations ports.StationRepo, newId func() string) *Publisher {
	return &Publisher{Writer: w, Tasks: tasks, Stations: stations, NewId: newId}
}

// Encode builds the wire form of every TaskCompleted, TaskCPTMissed, and
// PackageManifested event in evts — WITHOUT sending it. TaskCompleted's
// envelope is enriched with the completed Task's OrderRef, the completing
// associate's identity and the task's duration; TaskCPTMissed and
// PackageManifested need no enrichment, every field they carry on the wire
// comes straight off the domain event (see ADR-0025). Other event types
// are not part of the integration contract and yield nothing. The repo
// lookups (TaskCompleted only) happen here, so when Encode runs inside a
// use case's transaction (the outbox path) they see the just-saved rows.
//
// The W3C trace context of whatever span is active on ctx is injected
// into each message's headers, so the consumer's span becomes a child of
// the caller's — the publish span when called through Publish, the use
// case's span when called by the outbox publisher.
func (p *Publisher) Encode(ctx context.Context, evts ...shared.DomainEvent) ([]Encoded, error) {
	var out []Encoded
	for _, e := range evts {
		var encs []Encoded
		var err error
		switch ev := e.(type) {
		case shared.TaskCompleted:
			encs, err = p.encodeTaskCompleted(ctx, ev)
		case shared.TaskCPTMissed:
			encs, err = p.encodeTaskCPTMissed(ev)
		case shared.PackageManifested:
			encs, err = p.encodePackageManifested(ev)
		case shared.TransferPicked:
			encs, err = p.encodeTransferFact("TransferPicked", ev.TaskId, ev.OccurredAt(), ev.TaskTransferDetails)
		case shared.TransferDispatched:
			encs, err = p.encodeTransferFact("TransferDispatched", ev.TaskId, ev.OccurredAt(), ev.TaskTransferDetails)
		case shared.TransferArrived:
			encs, err = p.encodeTransferFact("TransferArrived", ev.TaskId, ev.OccurredAt(), ev.TaskTransferDetails)
		default:
			continue
		}
		if err != nil {
			return nil, err
		}
		for i := range encs {
			encs[i].Headers = append(encs[i].Headers, cloudevents.ContentTypeHeader())
			observability.InjectKafkaTrace(ctx, &encs[i].Headers)
		}
		out = append(out, encs...)
	}
	return out, nil
}

func (p *Publisher) encodeTaskCompleted(ctx context.Context, tc shared.TaskCompleted) ([]Encoded, error) {
	t, err := p.Tasks.FindById(ctx, tc.TaskId)
	if err != nil {
		return nil, fmt.Errorf("kafka: lookup task %s for enrichment: %w", tc.TaskId, err)
	}
	var workUnitId string
	var durationSeconds int64
	var taskType string
	if t != nil {
		workUnitId = string(t.OrderRef())
		if claimedAt := t.ClaimedAt(); claimedAt != nil {
			durationSeconds = int64(tc.OccurredAt().Sub(*claimedAt).Seconds())
		}
		taskType = string(t.Type())
	}

	associateId, err := p.associateId(ctx, tc.StationId)
	if err != nil {
		return nil, fmt.Errorf("kafka: lookup station %s for enrichment: %w", tc.StationId, err)
	}

	data := TaskCompletedData{
		TaskId:          string(tc.TaskId),
		StationId:       string(tc.StationId),
		WorkUnitId:      workUnitId,
		AssociateId:     associateId,
		DurationSeconds: durationSeconds,
		TaskType:        taskType,
	}
	enc, err := p.encodeIntegration(entityTask, "TaskCompleted", string(tc.TaskId), tc.OccurredAt(), data)
	if err != nil {
		return nil, err
	}
	return []Encoded{enc}, nil
}

func (p *Publisher) encodeTaskCPTMissed(ev shared.TaskCPTMissed) ([]Encoded, error) {
	data := TaskCPTMissedData{
		TaskId:   string(ev.TaskId),
		OrderRef: string(ev.OrderRef),
		TaskType: ev.TaskType,
		Cpt:      ev.CPT,
	}
	enc, err := p.encodeIntegration(entityTask, "TaskCPTMissed", string(ev.TaskId), ev.OccurredAt(), data)
	if err != nil {
		return nil, err
	}
	return []Encoded{enc}, nil
}

func (p *Publisher) encodePackageManifested(ev shared.PackageManifested) ([]Encoded, error) {
	data := PackageManifestedData{
		PackageId: string(ev.PackageId),
		OrderRef:  string(ev.OrderRef),
	}
	enc, err := p.encodeIntegration(entityPackage, "PackageManifested", string(ev.PackageId), ev.OccurredAt(), data)
	if err != nil {
		return nil, err
	}
	return []Encoded{enc}, nil
}

// encodeTransferFact builds the wire form of one transfer fact. The
// subject/key is the completing task's id; the CloudEvents time is the
// completion time (the domain event's occurred-at). Every payload field
// rides on the domain event itself — no repo enrichment, exactly like
// TaskCPTMissed/PackageManifested.
func (p *Publisher) encodeTransferFact(eventName string, taskId shared.TaskId, occurredAt time.Time, d shared.TaskTransferDetails) ([]Encoded, error) {
	data := TransferFactData{
		TransferRef: d.TransferRef,
		DemandId:    d.DemandId,
		WorkUnitId:  d.WorkUnitId,
		TaskId:      string(taskId),
		WorkKind:    d.WorkKind,
		SiteId:      d.SiteId,
		SKU:         d.SKU,
		Quantity:    d.Quantity,
	}
	payload, err := cloudevents.New(cloudevents.Spec{
		ID:        p.NewId(),
		Entity:    entityTransfer,
		EventName: eventName,
		Subject:   string(taskId),
		Time:      occurredAt,
		Stream:    cloudevents.StreamEvents,
		Version:   1,
		Data:      data,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka: encode %s: %w", eventName, err)
	}
	return []Encoded{{
		Topic:     Topic,
		EventType: cloudevents.Type(entityTransfer, eventName),
		Key:       []byte(string(taskId)),
		Value:     payload,
	}}, nil
}

// encodeIntegration wraps data in the CloudEvents 1.0 envelope for the
// integration stream (dataschema urn:warehouse:fulfillment-execution:
// events:<EventName>:v1). subject is the raising aggregate's id, which is
// also the Kafka message key — so partition affinity is unchanged.
func (p *Publisher) encodeIntegration(entity, eventName, subject string, occurredAt time.Time, data any) (Encoded, error) {
	payload, err := cloudevents.New(cloudevents.Spec{
		ID:        p.NewId(),
		Entity:    entity,
		EventName: eventName,
		Subject:   subject,
		Time:      occurredAt,
		Stream:    cloudevents.StreamEvents,
		Version:   1,
		Data:      data,
	})
	if err != nil {
		return Encoded{}, fmt.Errorf("kafka: encode %s: %w", eventName, err)
	}
	return Encoded{
		Topic:     Topic,
		EventType: cloudevents.Type(entity, eventName),
		Key:       []byte(subject),
		Value:     payload,
	}, nil
}

// Publish forwards every TaskCompleted, TaskCPTMissed, and
// PackageManifested event in evts onto Kafka. Each message is encoded and
// written inside its own "kafka.publish <topic>" producer span. Other
// event types are skipped.
func (p *Publisher) Publish(ctx context.Context, evts ...shared.DomainEvent) error {
	for _, e := range evts {
		if !inIntegrationContract(e) {
			continue
		}
		if err := p.publishOne(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// inIntegrationContract reports whether Encode knows how to turn e into a
// wire message, so Publish can skip a foreign event before opening a span.
func inIntegrationContract(e shared.DomainEvent) bool {
	switch e.(type) {
	case shared.TaskCompleted, shared.TaskCPTMissed, shared.PackageManifested,
		shared.TransferPicked, shared.TransferDispatched, shared.TransferArrived:
		return true
	default:
		return false
	}
}

// publishOne encodes and writes one event inside a producer span, so the
// injected traceparent names the publish span itself (the consumer hangs
// off the publish, not its caller) and a broker error is recorded on it.
func (p *Publisher) publishOne(ctx context.Context, e shared.DomainEvent) error {
	ctx, span := otel.Tracer(observability.InstrumentationName).Start(ctx,
		"kafka.publish "+Topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(Topic),
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
			return fmt.Errorf("kafka: publish %s: %w", enc.EventType, err)
		}
	}
	return nil
}

// associateId resolves the occupant checked into stationId at publish
// time, returning "" (not an error) when Stations is unset, the station is
// unknown, or the station has no occupant — associate identity is an
// intentionally soft, best-effort fact (see ADR-0014), not every station
// has a checked-in occupant (e.g. a robot station never checks anyone in).
func (p *Publisher) associateId(ctx context.Context, stationId shared.StationId) (string, error) {
	if p.Stations == nil {
		return "", nil
	}
	s, err := p.Stations.FindById(ctx, stationId)
	if err != nil {
		return "", err
	}
	if s == nil || s.Occupant() == nil {
		return "", nil
	}
	return string(*s.Occupant()), nil
}

// Close releases the underlying Kafka writer.
func (p *Publisher) Close() error {
	if w, ok := p.Writer.(*kafkago.Writer); ok {
		return w.Close()
	}
	return nil
}
