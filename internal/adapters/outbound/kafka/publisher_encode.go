package kafka

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

func (p *Publisher) encodeTaskCompleted(ctx context.Context, tc shared.TaskCompleted) ([]Encoded, error) {
	t, err := p.Tasks.FindById(ctx, tc.TaskId)
	if err != nil {
		return nil, fmt.Errorf("kafka: lookup task %s for enrichment: %w", tc.TaskId, err)
	}
	var workUnitId, orderRef string
	var lineNo int
	var durationSeconds int64
	var taskType string
	if t != nil {
		workUnitId = string(t.OrderRef())
		// order_ref is the upstream ORDER id, not the work unit id above
		// (audit decision 17): empty (omitted) when the task has none.
		orderRef = t.SourceOrderId()
		lineNo = t.SourceLineNo()
		// duration_seconds measures from the task's DurationBase — the
		// per-stop work start inside a batch, else the claim (ADR 0064) —
		// so a non-batched task produces exactly the bytes it did before.
		if base := t.DurationBase(); base != nil {
			durationSeconds = int64(tc.OccurredAt().Sub(*base).Seconds())
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
		OrderRef:        orderRef,
		LineNo:          lineNo,
	}
	withRelocation(&data, tc.Relocation)
	withReplenishment(&data, tc.Replenishment)
	enc, err := p.encodeIntegration(entityTask, "TaskCompleted", string(tc.TaskId), tc.OccurredAt(), data)
	if err != nil {
		return nil, err
	}
	return []Encoded{enc}, nil
}

// withRelocation copies a RELOCATE completion's executed move onto the
// TaskCompleted payload (ADR 0047); a nil move leaves the fields empty, so
// they are omitted from the wire.
func withRelocation(data *TaskCompletedData, r *shared.TaskRelocation) {
	if r == nil {
		return
	}
	data.SKU, data.FromBin, data.ToBin = r.SKU, r.FromBin, r.ToBin
	data.Quantity, data.RelocationRef = r.Quantity, r.Ref
}

// withReplenishment copies a REPLENISH completion's executed move onto the
// TaskCompleted payload (ADR 0050); a nil move leaves the fields empty, so
// they are omitted from the wire.
func withReplenishment(data *TaskCompletedData, r *shared.TaskReplenishment) {
	if r == nil {
		return
	}
	data.SKU, data.FromBin, data.ToBin = r.SKU, r.FromBin, r.ToBin
	data.Quantity, data.RequestRef = r.Quantity, r.RequestRef
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

// encodePackageManifested builds the wire form of a PackageManifested.
// Since ADR 0061 the payload carries the ship-correlation fields,
// resolved best-effort from the order's tasks (fail-soft, exactly like
// the analytics publisher's on-time-to-CPT enrichment: a missing repo,
// lookup error or unresolvable task leaves the optional fields omitted
// rather than failing the publish — the pre-ADR-0061 payload ships). The
// actual SLAM weight rides the domain event itself (RunSlam measured it).
func (p *Publisher) encodePackageManifested(ctx context.Context, ev shared.PackageManifested) ([]Encoded, error) {
	sourceOrderId, cpt := p.manifestCorrelation(ctx, ev.OrderRef)
	data := PackageManifestedData{
		PackageId:     string(ev.PackageId),
		OrderRef:      string(ev.OrderRef),
		SourceOrderId: sourceOrderId,
		Cpt:           cpt,
		WeightG:       ev.WeightG,
	}
	enc, err := p.encodeIntegration(entityPackage, "PackageManifested", string(ev.PackageId), ev.OccurredAt(), data)
	if err != nil {
		return nil, err
	}
	return []Encoded{enc}, nil
}

// manifestCorrelation resolves a manifest's source_order_id and cpt from
// the tasks sharing its orderRef (ADR 0061). The SLAM task is the leg a
// manifest is measured against — the same correlation the analytics
// on-time-to-CPT KPI uses (pickSlamTask, ADR-0026) — so its CPT becomes
// the payload's cpt. source_order_id prefers a correlated task's
// Task.SourceOrderId (WorkReleased.ref on the release path; the rebin
// PACK task's stamp on the consolidation path), checking the SLAM leg
// first, then the PACK task the package was sealed for. When the order's
// tasks are known but none carries one (a PACK task from before the
// ArriveAtRebin stamp), the rebin caller convention applies: the
// package's orderRef IS the bare order id, and a trailing "-line-N"
// work-unit suffix is stripped defensively (stripWorkUnitSuffix). When
// nothing correlates at all (no repo, lookup error, no tasks) both
// returns are "" and the payload keeps its exact pre-ADR-0061 bytes
// (omit-when-unknown).
func (p *Publisher) manifestCorrelation(ctx context.Context, orderRef shared.OrderRef) (sourceOrderId, cpt string) {
	if p.Tasks == nil {
		return "", ""
	}
	tasks, err := p.Tasks.FindByOrderRef(ctx, orderRef)
	if err != nil {
		return "", ""
	}
	if slam := pickSlamTask(tasks); slam != nil {
		if id := slam.SourceOrderId(); id != "" {
			sourceOrderId = id
		}
		cpt = slam.CPT().Time().UTC().Format(time.RFC3339)
	}
	if sourceOrderId == "" {
		for _, t := range tasks {
			if t.Type() == task.Pack {
				if id := t.SourceOrderId(); id != "" {
					sourceOrderId = id
					break
				}
			}
		}
	}
	if sourceOrderId == "" && len(tasks) > 0 {
		sourceOrderId = stripWorkUnitSuffix(string(orderRef))
	}
	return sourceOrderId, cpt
}

// workUnitSuffixRx matches the trailing "-line-N" of a per-line work
// unit id ("<order>-line-<n>", as wes-work-planning mints them).
var workUnitSuffixRx = regexp.MustCompile(`^(.*)-line-[0-9]+$`)

// stripWorkUnitSuffix returns ref without a trailing "-line-N" work-unit
// suffix, or ref unchanged when it has none. ADR 0061: on the rebin path
// the package's orderRef is the bare order-management OrderId by caller
// convention; this guard only defends against a caller that posts the
// work-unit shape instead.
func stripWorkUnitSuffix(ref string) string {
	if m := workUnitSuffixRx.FindStringSubmatch(ref); m != nil {
		return m[1]
	}
	return ref
}

// encodePackageDiverted builds the wire form of a PackageDiverted on the
// integration stream (ADR 0053): same type, subject (package id) and data
// as the analytics occurrence, no repo enrichment.
func (p *Publisher) encodePackageDiverted(ev shared.PackageDiverted) ([]Encoded, error) {
	data := PackageDivertedData{PackageId: string(ev.PackageId)}
	enc, err := p.encodeIntegration(entityPackage, "PackageDiverted", string(ev.PackageId), ev.OccurredAt(), data)
	if err != nil {
		return nil, err
	}
	return []Encoded{enc}, nil
}

// encodeWeightDiscrepancyDetected builds the wire form of a
// WeightDiscrepancyDetected on the integration stream (ADR 0053): same
// type, subject (package id) and data as the analytics occurrence, no
// repo enrichment.
func (p *Publisher) encodeWeightDiscrepancyDetected(ev shared.WeightDiscrepancyDetected) ([]Encoded, error) {
	data := WeightDiscrepancyDetectedData{
		ActualG:   ev.ActualWeight,
		ExpectedG: ev.ExpectedWeight,
		PackageId: string(ev.PackageId),
	}
	enc, err := p.encodeIntegration(entityPackage, "WeightDiscrepancyDetected", string(ev.PackageId), ev.OccurredAt(), data)
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
