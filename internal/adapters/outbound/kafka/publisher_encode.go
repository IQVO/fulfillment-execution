package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
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
