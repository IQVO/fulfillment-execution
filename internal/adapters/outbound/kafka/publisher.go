// Package kafka provides the outbound adapter that publishes
// fulfillment-execution domain events onto Kafka, satisfying
// ports.EventPublisher. It forwards TaskCompleted, TaskCPTMissed,
// PackageManifested, PackageDiverted and WeightDiscrepancyDetected (plus the
// task-pool, transfer and work-batch facts) — every other event type is not part of the
// published integration contract and is skipped. TaskCompleted is
// enriched with the completed Task's OrderRef (looked up via TaskRepo,
// since the domain event itself carries only TaskId/StationId) — the same
// repo-lookup-enrichment pattern inventory-storage's Kafka publisher uses
// for ReservationRevoked. It also enriches TaskCompleted with the
// completing associate's identity, the task's duration, and the task's own
// type (PICK/PACK/SLAM/REBIN), resolved via a StationRepo lookup, the
// Task's DurationBase timestamp (the per-stop work start inside a work
// batch, else the claim — ADR 0064), and the already-loaded Task's Type()
// respectively (see ADR-0014, ADR-0023) — inputs the labor-performance
// bounded context needs and that this service is the sole owner of.
// Since ADR 0061, PackageManifested is enriched the same best-effort way
// with the manifest's ship correlation (source_order_id, cpt), resolved
// from the order's tasks; its actual weight rides the domain event.
// TaskCPTMissed still needs no repo enrichment: it carries every field
// its wire payload needs directly on the domain event itself (see
// ADR-0025); so do PackageDiverted and WeightDiscrepancyDetected,
// promoted from the analytics topic for exception-management (ADR 0053).
//
// Every message is a CloudEvents 1.0 event in structured content mode
// built by internal/adapters/kafka/cloudevents (ADR-0032); there is no
// other envelope.
package kafka

import (
	"context"
	"time"

	kafkago "github.com/segmentio/kafka-go"

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
	// entityWorkBatch is the WorkBatch aggregate's entity segment (ADR
	// 0063): com.warehouse.wes.fulfillment-execution.workbatch.<Event>.
	entityWorkBatch = "workbatch"
)

// TaskCompletedData is the payload of a published TaskCompleted event,
// enriched with the completed Task's OrderRef as work_unit_id so Work
// Planning can call RecordCompletion(WorkUnitId), plus three
// labor-performance-relevant facts resolved at publish time (ADR-0014,
// ADR-0023): AssociateId (the occupant of the claiming station, if any —
// empty for a station with no checked-in occupant, e.g. a robot),
// DurationSeconds (elapsed work time between the task's DurationBase —
// the per-stop work start inside a work batch, else its claim (ADR 0064) —
// and its completion), and TaskType (this service's own task.Type — PICK,
// PACK, SLAM, or REBIN — read directly off the already-loaded Task, no new
// lookup needed). OrderRef (optional, omitted when empty) is the upstream
// ORDER the task's work was released for (Task.SourceOrderId, from
// WorkReleased.ref) — NOT work_unit_id, which for order work is the
// per-line work unit id "<order>-line-<n>". inventory-storage confirms
// picks by order_ref (audit decision 17, ADR 0040).
type TaskCompletedData struct {
	TaskId          string `json:"task_id"`
	StationId       string `json:"station_id"`
	WorkUnitId      string `json:"work_unit_id"`
	AssociateId     string `json:"associate_id,omitempty"`
	DurationSeconds int64  `json:"duration_seconds,omitempty"`
	TaskType        string `json:"task_type,omitempty"`
	OrderRef        string `json:"order_ref,omitempty"`
	// LineNo is the optional 1-based order line the task's work was
	// released for (Task.SourceLineNo, from WorkReleased.line_no), omitted
	// when unknown. inventory-storage uses order_ref + line_no to confirm
	// the reservation of that line (decision 18, ADR 0041).
	LineNo int `json:"line_no,omitempty"`
	// SKU, FromBin, ToBin, Quantity and RelocationRef are the additive
	// RELOCATE fields (ADR 0047): the executed slot-plan move, present only
	// on a RELOCATE task's TaskCompleted and omitted on every other one.
	// inventory-storage applies them to its ledger as a RelocateStock
	// (its ADR 0039); relocation_ref is "<plan_id>:<move_seq>".
	SKU           string `json:"sku,omitempty"`
	FromBin       string `json:"from_bin,omitempty"`
	ToBin         string `json:"to_bin,omitempty"`
	Quantity      int    `json:"quantity,omitempty"`
	RelocationRef string `json:"relocation_ref,omitempty"`
	// RequestRef is the additive REPLENISH field (ADR 0050): the
	// replenishment request id, present only on a REPLENISH task's
	// TaskCompleted (which also carries sku, from_bin, to_bin and quantity
	// above) and omitted on every other one. It mirrors relocation_ref;
	// inventory-storage applies the move as a RelocateStock with reason
	// REPLENISHMENT and source_ref = request_ref (its ADR 0041).
	RequestRef string `json:"request_ref,omitempty"`
	// BatchId and BatchSize are the additive pick-batching fields (ADR
	// 0063): the work batch the completed PICK's task was a member of, and
	// how many tasks that walk held. Omitted on every non-batched
	// completion — nil batch repo, task in no batch, or any other task
	// type — so the pre-batching payload bytes are unchanged.
	BatchId   string `json:"batch_id,omitempty"`
	BatchSize int    `json:"batch_size,omitempty"`
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
//
// The ship-correlation fields (ADR 0061) are additive and optional —
// omitted entirely when unknown, never empty strings or a zero weight on
// the wire, so the pre-ADR-0061 payload bytes are unchanged:
//
//   - source_order_id: the order-management OrderId the package ships for.
//     Resolved from the correlated tasks' Task.SourceOrderId (stamped from
//     WorkReleased.ref on the release path, and on the rebin-created PACK
//     task by ArriveAtRebin); when no correlated task carries one, the
//     package's orderRef is the bare order id by rebin caller convention
//     and a trailing "-line-N" work-unit suffix is stripped defensively.
//   - cpt: the manifest's SLAM task's CPT, RFC3339, the same SLAM-leg
//     correlation as the analytics on-time-to-CPT KPI.
//   - weight_g: the actual SLAM weigh-check measurement in grams (the
//     domain event carries it from RunSlam).
//   - site_id: reserved; no source exists in the order-work chain yet, so
//     it is omitted until a later wave lands one (see ADR 0061).
type PackageManifestedData struct {
	PackageId string `json:"package_id"`
	OrderRef  string `json:"order_ref"`
	// Additive ship-correlation fields (ADR 0061). Omitted entirely when
	// unknown — never empty strings or zero values on the wire.
	SiteId        string  `json:"site_id,omitempty"`
	SourceOrderId string  `json:"source_order_id,omitempty"`
	Cpt           string  `json:"cpt,omitempty"`
	WeightG       float64 `json:"weight_g,omitempty"`
}

// PackageDivertedData is the payload of a published PackageDiverted event:
// a package that failed its SLAM weigh-check and was routed to the
// problem-solve area instead of being labelled (ADR 0053). It mirrors the
// analytics publisher's payload field for field, so the `data` bytes on
// the integration topic are identical to the analytics occurrence; the
// exception-management context opens its problem-solve case on it.
type PackageDivertedData struct {
	PackageId string `json:"package_id"`
}

// WeightDiscrepancyDetectedData is the payload of a published
// WeightDiscrepancyDetected event: the SLAM measurement that caused the
// divert, both weights in grams (ADR 0053). Fields are declared in
// alphabetical order ON PURPOSE: the analytics publisher marshals this
// payload from a map (encoding/json sorts map keys), and the integration
// `data` is asserted byte-identical to it.
type WeightDiscrepancyDetectedData struct {
	ActualG   float64 `json:"actual_g"`
	ExpectedG float64 `json:"expected_g"`
	PackageId string  `json:"package_id"`
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

// WorkBatchEventData structs (ADR 0063) mirror the analytics publisher's
// payload maps field for field, with fields declared in alphabetical order
// ON PURPOSE (encoding/json sorts map keys, so the integration `data` bytes
// and the analytics occurrence are byte-identical — same convention as
// WeightDiscrepancyDetectedData).

// WorkBatchFormData is the payload of a published WorkBatchFormed event:
// the batch that just formed, its members in stop order.
type WorkBatchFormData struct {
	BatchId   string                `json:"batch_id"`
	Cpt       time.Time             `json:"cpt"`
	FormedAt  time.Time             `json:"formed_at"`
	Members   []WorkBatchMemberWire `json:"members"`
	StationId string                `json:"station_id"`
	Version   int64                 `json:"version"`
}

// WorkBatchMemberWire is one member of a WorkBatchFormed payload.
type WorkBatchMemberWire struct {
	Compartment int    `json:"compartment"`
	StopSeq     int    `json:"stop_seq"`
	TaskId      string `json:"task_id"`
}

// WorkBatchCompletedData is the payload of a published WorkBatchCompleted
// event: the last open member completed, so the walk is over and
// walk_seconds is measurable from formation.
type WorkBatchCompletedData struct {
	BatchId     string    `json:"batch_id"`
	CompletedAt time.Time `json:"completed_at"`
	MemberCount int       `json:"member_count"`
	StationId   string    `json:"station_id"`
	Version     int64     `json:"version"`
	WalkSeconds int64     `json:"walk_seconds"`
}

// WorkBatchAbandonedData is the payload of a published WorkBatchAbandoned
// event: the last open member left the batch (lease expired) without
// completing.
type WorkBatchAbandonedData struct {
	AbandonedAt time.Time `json:"abandoned_at"`
	BatchId     string    `json:"batch_id"`
	MemberCount int       `json:"member_count"`
	StationId   string    `json:"station_id"`
	Version     int64     `json:"version"`
}

// Writer is the subset of *kafkago.Writer the Publisher needs, so tests can
// substitute a fake without a live broker.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// Publisher publishes fulfillment-execution domain events onto Kafka as
// CloudEvents 1.0 structured-mode messages (ADR-0032). It satisfies
// ports.EventPublisher. Event types outside inIntegrationContract are not
// part of the published integration contract and are skipped.
type Publisher struct {
	Writer   Writer
	Tasks    ports.TaskRepo
	Stations ports.StationRepo
	// Batches, when non-nil, enriches a PICK's TaskCompleted with the
	// batch_id and batch_size of the work batch the task completed into
	// (ADR 0063) — the fields labor-performance needs to report batched and
	// single picks apart. Nil (or no batch found) omits both, so the
	// pre-batching payload bytes are unchanged.
	Batches ports.WorkBatchRepo
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
// associate's identity and the task's duration; since ADR 0061
// PackageManifested's is enriched with the manifest's ship correlation
// (source_order_id, cpt) from the order's tasks, while its actual weight
// rides the domain event; TaskCPTMissed needs no enrichment, every field
// it carries on the wire comes straight off the domain event (see
// ADR-0025). Other event types
// are not part of the integration contract and yield nothing. The repo
// lookups (TaskCompleted and PackageManifested) happen here, so when
// Encode runs inside a use case's transaction (the outbox path) they see
// the just-saved rows.
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
		case shared.TaskAvailable:
			encs, err = p.encodeTaskAvailable(ctx, ev)
		case shared.TaskClaimed:
			encs, err = p.encodeTaskClaimed(ctx, ev)
		case shared.TaskCPTMissed:
			encs, err = p.encodeTaskCPTMissed(ev)
		case shared.PackageManifested:
			encs, err = p.encodePackageManifested(ctx, ev)
		case shared.PackageDiverted:
			encs, err = p.encodePackageDiverted(ev)
		case shared.WeightDiscrepancyDetected:
			encs, err = p.encodeWeightDiscrepancyDetected(ev)
		case shared.TransferPicked, shared.TransferDispatched, shared.TransferArrived:
			id, d := transferFactOf(e)
			encs, err = p.encodeTransferFact(e.EventName(), id, e.OccurredAt(), d)
		case shared.WorkBatchFormed, shared.WorkBatchCompleted, shared.WorkBatchAbandoned:
			encs, err = p.encodeWorkBatch(e)
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

// transferFactOf unpacks any Transfer* fact into the arguments
// encodeTransferFact takes, so Encode's switch stays one arm per fact
// family.
func transferFactOf(e shared.DomainEvent) (id shared.TaskId, d shared.TaskTransferDetails) {
	switch ev := e.(type) {
	case shared.TransferPicked:
		return ev.TaskId, ev.TaskTransferDetails
	case shared.TransferDispatched:
		return ev.TaskId, ev.TaskTransferDetails
	case shared.TransferArrived:
		return ev.TaskId, ev.TaskTransferDetails
	}
	return "", shared.TaskTransferDetails{}
}

// inIntegrationContract reports whether Encode knows how to turn e into a
// wire message, so Publish can skip a foreign event before opening a span.
func inIntegrationContract(e shared.DomainEvent) bool {
	switch e.(type) {
	case shared.TaskCompleted, shared.TaskCPTMissed, shared.PackageManifested,
		shared.PackageDiverted, shared.WeightDiscrepancyDetected,
		shared.TaskAvailable, shared.TaskClaimed,
		shared.TransferPicked, shared.TransferDispatched, shared.TransferArrived,
		shared.WorkBatchFormed, shared.WorkBatchCompleted, shared.WorkBatchAbandoned:
		return true
	default:
		return false
	}
}

// publishOne encodes and writes one event inside a producer span, so the
// injected traceparent names the publish span itself (the consumer hangs
// off the publish, not its caller) and a broker error is recorded on it.
func (p *Publisher) publishOne(ctx context.Context, e shared.DomainEvent) error {
	return publishOneTo(ctx, Topic, "", p.Writer, p.Encode, e)
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
