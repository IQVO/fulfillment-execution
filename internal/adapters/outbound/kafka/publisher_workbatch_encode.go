package kafka

import (
	"context"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// encodeWorkBatchFormed builds the wire form of WorkBatchFormed (ADR 0063).
// Every field comes straight off the domain event — no repo enrichment
// needed; the message key is the batch id, so per-batch ordering holds.
func (p *Publisher) encodeWorkBatchFormed(ev shared.WorkBatchFormed) ([]Encoded, error) {
	members := make([]WorkBatchMemberWire, len(ev.Members))
	for i, m := range ev.Members {
		members[i] = WorkBatchMemberWire{Compartment: m.Compartment, StopSeq: m.StopSeq, TaskId: string(m.TaskId)}
	}
	data := WorkBatchFormData{
		BatchId:   string(ev.BatchId),
		StationId: string(ev.StationId),
		Cpt:       ev.Cpt,
		Members:   members,
		FormedAt:  ev.OccurredAt(),
		Version:   ev.Version,
	}
	enc, err := p.encodeIntegration(entityWorkBatch, "WorkBatchFormed", string(ev.BatchId), ev.OccurredAt(), data)
	if err != nil {
		return nil, err
	}
	return []Encoded{enc}, nil
}

// encodeWorkBatchCompleted builds the wire form of WorkBatchCompleted.
func (p *Publisher) encodeWorkBatchCompleted(ev shared.WorkBatchCompleted) ([]Encoded, error) {
	data := WorkBatchCompletedData{
		BatchId:     string(ev.BatchId),
		StationId:   string(ev.StationId),
		MemberCount: ev.MemberCount,
		WalkSeconds: ev.WalkSeconds,
		CompletedAt: ev.OccurredAt(),
		Version:     ev.Version,
	}
	enc, err := p.encodeIntegration(entityWorkBatch, "WorkBatchCompleted", string(ev.BatchId), ev.OccurredAt(), data)
	if err != nil {
		return nil, err
	}
	return []Encoded{enc}, nil
}

// encodeWorkBatchAbandoned builds the wire form of WorkBatchAbandoned.
func (p *Publisher) encodeWorkBatchAbandoned(ev shared.WorkBatchAbandoned) ([]Encoded, error) {
	data := WorkBatchAbandonedData{
		BatchId:     string(ev.BatchId),
		StationId:   string(ev.StationId),
		MemberCount: ev.MemberCount,
		AbandonedAt: ev.OccurredAt(),
		Version:     ev.Version,
	}
	enc, err := p.encodeIntegration(entityWorkBatch, "WorkBatchAbandoned", string(ev.BatchId), ev.OccurredAt(), data)
	if err != nil {
		return nil, err
	}
	return []Encoded{enc}, nil
}

// encodeWorkBatch dispatches the WorkBatch* events (ADR 0063) to their
// wire encoders; kept out of Encode's switch so that arm stays readable.
func (p *Publisher) encodeWorkBatch(e shared.DomainEvent) ([]Encoded, error) {
	switch ev := e.(type) {
	case shared.WorkBatchFormed:
		return p.encodeWorkBatchFormed(ev)
	case shared.WorkBatchCompleted:
		return p.encodeWorkBatchCompleted(ev)
	case shared.WorkBatchAbandoned:
		return p.encodeWorkBatchAbandoned(ev)
	default:
		return nil, nil
	}
}

// batchFieldsOf resolves a completed task's additive pick-batching fields
// (ADR 0063): the batch the completion belongs to and how many tasks that
// walk held. ok is false when the publisher carries no batch repo or the
// task completed into no batch — the fields are then omitted, exactly the
// pre-batching payload.
func (p *Publisher) batchFieldsOf(ctx context.Context, taskId shared.TaskId) (batchId string, batchSize int, ok bool) {
	if p.Batches == nil {
		return "", 0, false
	}
	b, err := p.Batches.FindByTaskId(ctx, taskId)
	if err != nil || b == nil {
		return "", 0, false
	}
	return string(b.Id()), len(b.Members()), true
}
