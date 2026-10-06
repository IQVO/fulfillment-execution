package usecases

import (
	"context"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// CompleteTask finishes a task, validating that the calling station owns the
// active claim.
//
// For a task carrying an inter-warehouse-transfer correlation block
// (see task.TransferDetails), completion additionally raises exactly ONE
// transfer fact selected by the block's WorkKind — TransferPicked,
// TransferDispatched or TransferArrived — alongside the unchanged
// TaskCompleted. Both facts commit with the task's Completed state in the
// same UnitOfWork, so a completed transfer task and its transfer fact are
// never observably apart. Non-transfer tasks raise no transfer fact.
type CompleteTask struct {
	Tasks     ports.TaskRepo
	Publisher ports.EventPublisher
	Clock     ports.Clock
	// Metrics is optional: nil means this use case runs uninstrumented.
	Metrics ports.Metrics
	// UnitOfWork brackets Save + Publish atomically (ADR 0020); nil runs
	// them back to back.
	UnitOfWork ports.UnitOfWork
}

// Execute completes taskId on behalf of stationId.
func (uc *CompleteTask) Execute(ctx context.Context, taskId shared.TaskId, stationId shared.StationId) error {
	t, err := uc.Tasks.FindById(ctx, taskId)
	if err != nil {
		return err
	}
	if t == nil {
		return ErrTaskNotFound
	}

	now := uc.Clock.Now()
	if err := t.Complete(stationId, now); err != nil {
		return err
	}
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Tasks.Save(ctx, t); err != nil {
			return err
		}
		events := []shared.DomainEvent{shared.NewTaskCompleted(taskId, stationId, now)}
		events = append(events, transferFactFor(t, now)...)
		return uc.Publisher.Publish(ctx, events...)
	})
	if err != nil {
		return err
	}
	if uc.Metrics != nil {
		uc.Metrics.TaskCompleted(ctx, t.Type())
	}
	return nil
}

// transferFactFor maps a just-completed task's transfer correlation block
// to the single transfer fact its WorkKind selects, or nil for a
// non-transfer task. Pure function of the aggregate — no I/O — so the
// fact-selection table is unit-testable on its own (see the
// transfer-task-facts tests).
func transferFactFor(t *task.Task, now time.Time) []shared.DomainEvent {
	tr := t.Transfer()
	if tr == nil {
		return nil
	}
	details := shared.TaskTransferDetails{
		TransferRef: tr.TransferRef,
		DemandId:    tr.DemandId,
		WorkUnitId:  string(t.OrderRef()),
		WorkKind:    string(tr.WorkKind),
		SiteId:      tr.SiteId,
		SKU:         tr.SKU,
		Quantity:    tr.Quantity,
	}
	switch tr.WorkKind {
	case task.WorkKindTransferPick:
		return []shared.DomainEvent{shared.NewTransferPicked(t.Id(), details, now)}
	case task.WorkKindTransferDispatch:
		return []shared.DomainEvent{shared.NewTransferDispatched(t.Id(), details, now)}
	case task.WorkKindTransferArrival:
		return []shared.DomainEvent{shared.NewTransferArrived(t.Id(), details, now)}
	default:
		// An unparsable work kind cannot reach here: the consumer
		// validates it with task.ParseWorkKind before the task is ever
		// created, and rehydrate fails the read on an unknown value.
		// Still, an unknown kind must not silently swallow the
		// completion's transfer fact — returning no fact would lose the
		// custody signal, so this is a programming-error panic rather
		// than a swallowed branch.
		panic("usecases: task carries unknown transfer work kind " + string(tr.WorkKind))
	}
}
