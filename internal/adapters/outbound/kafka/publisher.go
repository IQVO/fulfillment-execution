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

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/observability"
)

// Topic is the topic fulfillment-execution publishes its outbound
// integration events to.
const Topic = "warehouse.fulfillment.events"

// Envelope is the CloudEvents-like wrapper shared across all four
// warehouse-systems services.
type Envelope struct {
	EventId    string            `json:"event_id"`
	EventType  string            `json:"event_type"`
	OccurredAt time.Time         `json:"occurred_at"`
	Source     string            `json:"source"`
	Data       TaskCompletedData `json:"data"`
}

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

// TaskCPTMissedEnvelope is the CloudEvents-like wrapper for a published
// TaskCPTMissed event (ADR-0025) — the fulfillment-execution half of
// order-management ADR 0014 §5's promise feedback loop.
type TaskCPTMissedEnvelope struct {
	EventId    string            `json:"event_id"`
	EventType  string            `json:"event_type"`
	OccurredAt time.Time         `json:"occurred_at"`
	Source     string            `json:"source"`
	Data       TaskCPTMissedData `json:"data"`
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

// PackageManifestedEnvelope is the CloudEvents-like wrapper for a
// published PackageManifested event (ADR-0025) — the SLAM-pass half of
// order-management ADR 0014 §5's promise feedback loop.
type PackageManifestedEnvelope struct {
	EventId    string                `json:"event_id"`
	EventType  string                `json:"event_type"`
	OccurredAt time.Time             `json:"occurred_at"`
	Source     string                `json:"source"`
	Data       PackageManifestedData `json:"data"`
}

// PackageManifestedData is the payload of a published PackageManifested
// event: a package that passed its SLAM weigh-check. OrderRef is what
// order-management's RepromiseOrder consumer keys its recompute on.
type PackageManifestedData struct {
	PackageId string `json:"package_id"`
	OrderRef  string `json:"order_ref"`
}

// Writer is the subset of *kafkago.Writer the Publisher needs, so tests can
// substitute a fake without a live broker.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// Publisher publishes fulfillment-execution domain events onto Kafka. It
// satisfies ports.EventPublisher. Event types other than TaskCompleted are
// not yet part of the published integration contract and are skipped.
type Publisher struct {
	Writer   Writer
	Tasks    ports.TaskRepo
	Stations ports.StationRepo
	NewId    func() string
	// Mode selects the wire envelope shape(s) Encode produces
	// (ADR-0027 Phase 4). The zero value behaves as EnvelopeModeFlat, so
	// every existing test/caller building a Publisher via a bare struct
	// literal is unaffected.
	Mode EnvelopeMode
}

// NewPublisher constructs a Publisher writing to Topic on brokers, in
// EnvelopeModeFlat. Use NewPublisherWithMode to select a different mode.
func NewPublisher(brokers []string, tasks ports.TaskRepo, stations ports.StationRepo, newId func() string) *Publisher {
	return NewPublisherWithMode(brokers, tasks, stations, newId, EnvelopeModeFlat)
}

// NewPublisherWithMode constructs a Publisher writing to Topic on brokers
// in the given EnvelopeMode (ADR-0027 Phase 4).
func NewPublisherWithMode(brokers []string, tasks ports.TaskRepo, stations ports.StationRepo, newId func() string, mode EnvelopeMode) *Publisher {
	p := NewPublisherWithWriter(&kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  Topic,
		Balancer:               &kafkago.LeastBytes{},
		AllowAutoTopicCreation: true,
	}, tasks, stations, newId)
	p.Mode = mode
	return p
}

// NewPublisherWithWriter constructs a Publisher over an explicit Writer, in
// EnvelopeModeFlat. When used only as an Encoder (the outbox configuration,
// ADR 0020) the writer may be nil — Encode never touches it. Use
// NewPublisherWithWriterAndMode to select a different mode.
func NewPublisherWithWriter(w Writer, tasks ports.TaskRepo, stations ports.StationRepo, newId func() string) *Publisher {
	return &Publisher{Writer: w, Tasks: tasks, Stations: stations, NewId: newId}
}

// NewPublisherWithWriterAndMode constructs a Publisher over an explicit
// Writer in the given EnvelopeMode (ADR-0027 Phase 4).
func NewPublisherWithWriterAndMode(w Writer, tasks ports.TaskRepo, stations ports.StationRepo, newId func() string, mode EnvelopeMode) *Publisher {
	p := NewPublisherWithWriter(w, tasks, stations, newId)
	p.Mode = mode
	return p
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
		default:
			continue
		}
		if err != nil {
			return nil, err
		}
		for i := range encs {
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

	eventId := p.NewId()
	data := TaskCompletedData{
		TaskId:          string(tc.TaskId),
		StationId:       string(tc.StationId),
		WorkUnitId:      workUnitId,
		AssociateId:     associateId,
		DurationSeconds: durationSeconds,
		TaskType:        taskType,
	}

	var out []Encoded
	if mode := p.effectiveMode(); mode == EnvelopeModeFlat || mode == EnvelopeModeDual {
		env := Envelope{
			EventId:    eventId,
			EventType:  "TaskCompleted",
			OccurredAt: tc.OccurredAt(),
			Source:     "fulfillment-execution",
			Data:       data,
		}
		payload, err := json.Marshal(env)
		if err != nil {
			return nil, fmt.Errorf("kafka: marshal envelope: %w", err)
		}
		out = append(out, Encoded{Topic: Topic, EventType: env.EventType, Key: []byte(tc.TaskId), Value: payload})
	}
	if mode := p.effectiveMode(); mode == EnvelopeModeCloudEvents || mode == EnvelopeModeDual {
		ce := newCloudEvent(eventId, "task", "TaskCompleted", string(tc.TaskId), tc.OccurredAt(), data)
		payload, err := json.Marshal(ce)
		if err != nil {
			return nil, fmt.Errorf("kafka: marshal cloudevent: %w", err)
		}
		out = append(out, Encoded{Topic: Topic, EventType: "TaskCompleted", Key: []byte(tc.TaskId), Value: payload})
	}
	return out, nil
}

func (p *Publisher) encodeTaskCPTMissed(ev shared.TaskCPTMissed) ([]Encoded, error) {
	eventId := p.NewId()
	data := TaskCPTMissedData{
		TaskId:   string(ev.TaskId),
		OrderRef: string(ev.OrderRef),
		TaskType: ev.TaskType,
		Cpt:      ev.CPT,
	}

	var out []Encoded
	if mode := p.effectiveMode(); mode == EnvelopeModeFlat || mode == EnvelopeModeDual {
		env := TaskCPTMissedEnvelope{
			EventId:    eventId,
			EventType:  "TaskCPTMissed",
			OccurredAt: ev.OccurredAt(),
			Source:     "fulfillment-execution",
			Data:       data,
		}
		payload, err := json.Marshal(env)
		if err != nil {
			return nil, fmt.Errorf("kafka: marshal TaskCPTMissed envelope: %w", err)
		}
		out = append(out, Encoded{Topic: Topic, EventType: env.EventType, Key: []byte(ev.TaskId), Value: payload})
	}
	if mode := p.effectiveMode(); mode == EnvelopeModeCloudEvents || mode == EnvelopeModeDual {
		ce := newCloudEvent(eventId, "task", "TaskCPTMissed", string(ev.TaskId), ev.OccurredAt(), data)
		payload, err := json.Marshal(ce)
		if err != nil {
			return nil, fmt.Errorf("kafka: marshal TaskCPTMissed cloudevent: %w", err)
		}
		out = append(out, Encoded{Topic: Topic, EventType: "TaskCPTMissed", Key: []byte(ev.TaskId), Value: payload})
	}
	return out, nil
}

func (p *Publisher) encodePackageManifested(ev shared.PackageManifested) ([]Encoded, error) {
	eventId := p.NewId()
	data := PackageManifestedData{
		PackageId: string(ev.PackageId),
		OrderRef:  string(ev.OrderRef),
	}

	var out []Encoded
	if mode := p.effectiveMode(); mode == EnvelopeModeFlat || mode == EnvelopeModeDual {
		env := PackageManifestedEnvelope{
			EventId:    eventId,
			EventType:  "PackageManifested",
			OccurredAt: ev.OccurredAt(),
			Source:     "fulfillment-execution",
			Data:       data,
		}
		payload, err := json.Marshal(env)
		if err != nil {
			return nil, fmt.Errorf("kafka: marshal PackageManifested envelope: %w", err)
		}
		out = append(out, Encoded{Topic: Topic, EventType: env.EventType, Key: []byte(ev.PackageId), Value: payload})
	}
	if mode := p.effectiveMode(); mode == EnvelopeModeCloudEvents || mode == EnvelopeModeDual {
		ce := newCloudEvent(eventId, "package", "PackageManifested", string(ev.PackageId), ev.OccurredAt(), data)
		payload, err := json.Marshal(ce)
		if err != nil {
			return nil, fmt.Errorf("kafka: marshal PackageManifested cloudevent: %w", err)
		}
		out = append(out, Encoded{Topic: Topic, EventType: "PackageManifested", Key: []byte(ev.PackageId), Value: payload})
	}
	return out, nil
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
	case shared.TaskCompleted, shared.TaskCPTMissed, shared.PackageManifested:
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
			return fmt.Errorf("kafka: publish TaskCompleted: %w", err)
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
