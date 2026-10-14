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

// Execute completes taskId on behalf of stationId, with no relocation
// inputs. A RELOCATE task is rejected (task.ErrRelocationConfirmationRequired):
// it completes only through ExecuteWith.
func (uc *CompleteTask) Execute(ctx context.Context, taskId shared.TaskId, stationId shared.StationId) error {
	return uc.ExecuteWith(ctx, taskId, stationId, task.RelocationConfirmation{})
}

// ExecuteWith completes taskId on behalf of stationId with the operator's
// relocation inputs (ADR 0047). For a RELOCATE task they are required: the
// confirmed quantity (>= 1) and the bins the move left open; the completion
// publishes the executed move on TaskCompleted. For any other task they
// must be empty (task.ErrNotRelocationTask otherwise).
func (uc *CompleteTask) ExecuteWith(ctx context.Context, taskId shared.TaskId, stationId shared.StationId, in task.RelocationConfirmation) error {
	t, err := uc.Tasks.FindById(ctx, taskId)
	if err != nil {
		return err
	}
	if t == nil {
		return ErrTaskNotFound
	}

	now := uc.Clock.Now()
	if err := completeAggregate(t, stationId, now, in); err != nil {
		return err
	}
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Tasks.Save(ctx, t); err != nil {
			return err
		}
		events := []shared.DomainEvent{taskCompletedFor(t, stationId, now)}
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

// completeAggregate routes the completion: a RELOCATE task takes the
// confirmation, any other task takes none.
func completeAggregate(t *task.Task, stationId shared.StationId, now time.Time, in task.RelocationConfirmation) error {
	if t.Type() == task.Relocate && in.HasInput() {
		return t.CompleteRelocation(stationId, now, in)
	}
	if t.Type() != task.Relocate && in.HasInput() {
		return task.ErrNotRelocationTask
	}
	// No inputs: a RELOCATE task is rejected by Complete itself
	// (ErrRelocationConfirmationRequired, after the claim rules).
	return t.Complete(stationId, now)
}

// taskCompletedFor builds the TaskCompleted of a just-completed task: a
// RELOCATE task's carries the executed move (SKU from the plan, the bins
// and quantity from the confirmation, ref "<plan_id>:<move_seq>").
func taskCompletedFor(t *task.Task, stationId shared.StationId, now time.Time) shared.TaskCompleted {
	move, result := t.Relocation(), t.RelocationResult()
	if move == nil || result == nil {
		return shared.NewTaskCompleted(t.Id(), stationId, now)
	}
	return shared.NewRelocationTaskCompleted(t.Id(), stationId, now, shared.TaskRelocation{
		SKU:      move.SKU,
		FromBin:  result.FromBin,
		ToBin:    result.ToBin,
		Quantity: result.Quantity,
		Ref:      move.Ref(),
	})
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
