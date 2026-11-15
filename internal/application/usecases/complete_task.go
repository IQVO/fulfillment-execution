package usecases

import (
	"context"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
	"github.com/claudioed/fulfillment-execution/internal/domain/workbatch"
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
	// Batches, when non-nil, keeps a completed task's work batch in step
	// (ADR 0063): the member completes, the next open member's work clock
	// starts (the NextStopClock policy, ADR 0064 — duration_seconds stays
	// per stop), and the last completion completes the batch and raises
	// WorkBatchCompleted. Nil means this completion runs exactly as
	// before batching existed — every pre-existing caller and test.
	Batches ports.WorkBatchRepo
}

// Execute completes taskId on behalf of stationId, with no confirmation
// inputs. A RELOCATE or REPLENISH task is rejected
// (task.ErrRelocationConfirmationRequired,
// task.ErrReplenishmentConfirmationRequired): it completes only through
// ExecuteWith.
func (uc *CompleteTask) Execute(ctx context.Context, taskId shared.TaskId, stationId shared.StationId) error {
	return uc.ExecuteWith(ctx, taskId, stationId, task.RelocationConfirmation{})
}

// ExecuteWith completes taskId on behalf of stationId with the operator's
// confirmation inputs (ADR 0047 for RELOCATE, ADR 0050 for REPLENISH). For a
// RELOCATE task they are required: the confirmed quantity (>= 1) and the
// bins the move left open. For a REPLENISH task the confirmed quantity is
// required and the bins are optional scans that must equal the planned
// ones. The completion publishes the executed move on TaskCompleted. For any
// other task the inputs must be empty (task.ErrNotRelocationTask otherwise).
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
	var extraEvents []shared.DomainEvent
	if uc.Batches != nil {
		evts, err := uc.completeBatchMember(ctx, t, now)
		if err != nil {
			return err
		}
		extraEvents = evts
	}
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Tasks.Save(ctx, t); err != nil {
			return err
		}
		events := []shared.DomainEvent{taskCompletedFor(t, stationId, now)}
		events = append(events, transferFactFor(t, now)...)
		events = append(events, extraEvents...)
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

// completeBatchMember applies a completion to the task's open work batch
// (ADR 0063) and returns the events it raises. A task in no batch, a member
// that already completed, and a member that left the batch (its lease
// expired and it returned to the pool — it then completed as an ordinary
// task) are all no-ops: the task's own completion is unaffected. Completing
// the last open member completes the batch and raises WorkBatchCompleted;
// the next open member's work clock starts now (NextStopClock, ADR 0064),
// so its duration_seconds measures its own stop, not the whole walk.
func (uc *CompleteTask) completeBatchMember(ctx context.Context, t *task.Task, now time.Time) ([]shared.DomainEvent, error) {
	b, err := uc.Batches.FindOpenByTaskId(ctx, t.Id())
	if err != nil || b == nil {
		return nil, err
	}
	if err := b.CompleteMember(t.Id(), now); err != nil {
		// AlreadyCompleted cannot happen (completion is at-most-once
		// upstream); Detached and NotInBatch are the batch having moved
		// on without this member — nothing to record.
		return nil, nil
	}
	if next := b.NextOpenMember(); next != nil {
		if nt, err := uc.Tasks.FindById(ctx, next.TaskId); err != nil {
			return nil, err
		} else if nt != nil {
			nt.StartWork(now)
			if err := uc.Tasks.Save(ctx, nt); err != nil {
				return nil, err
			}
		}
	}
	if err := uc.Batches.Save(ctx, b); err != nil {
		return nil, err
	}
	if b.Status() == workbatch.StatusCompleted {
		return []shared.DomainEvent{shared.NewWorkBatchCompleted(b.Id(), b.StationId(), len(b.Members()), b.WalkSeconds(), b.Version(), now)}, nil
	}
	return nil, nil
}

// completeAggregate routes the completion: a RELOCATE or REPLENISH task
// takes the confirmation, any other task takes none.
func completeAggregate(t *task.Task, stationId shared.StationId, now time.Time, in task.RelocationConfirmation) error {
	if t.Type() == task.Relocate && in.HasInput() {
		return t.CompleteRelocation(stationId, now, in)
	}
	if t.Type() == task.Replenish && in.HasInput() {
		return t.CompleteReplenishment(stationId, now, in)
	}
	if t.Type() != task.Relocate && t.Type() != task.Replenish && in.HasInput() {
		return task.ErrNotRelocationTask
	}
	// No inputs: a RELOCATE or REPLENISH task is rejected by Complete itself
	// (ErrRelocationConfirmationRequired / ErrReplenishmentConfirmationRequired,
	// after the claim rules).
	return t.Complete(stationId, now)
}

// taskCompletedFor builds the TaskCompleted of a just-completed task: a
// RELOCATE task's carries the executed move (SKU from the plan, the bins
// and quantity from the confirmation, ref "<plan_id>:<move_seq>"), a
// REPLENISH task's the executed replenishment (SKU from the plan, the bins
// and quantity from the confirmation, request_ref = the request id).
func taskCompletedFor(t *task.Task, stationId shared.StationId, now time.Time) shared.TaskCompleted {
	if ev, ok := replenishmentCompletedFor(t, stationId, now); ok {
		return ev
	}
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

// replenishmentCompletedFor builds the TaskCompleted of a just-completed
// REPLENISH task; ok is false for any other task.
func replenishmentCompletedFor(t *task.Task, stationId shared.StationId, now time.Time) (shared.TaskCompleted, bool) {
	move, result := t.Replenishment(), t.ReplenishmentResult()
	if move == nil || result == nil {
		return shared.TaskCompleted{}, false
	}
	return shared.NewReplenishmentTaskCompleted(t.Id(), stationId, now, shared.TaskReplenishment{
		SKU:        move.SKU,
		FromBin:    result.FromBin,
		ToBin:      result.ToBin,
		Quantity:   result.Quantity,
		RequestRef: move.Ref(),
	}), true
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
