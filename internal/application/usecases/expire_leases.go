package usecases

import (
	"context"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
	"github.com/claudioed/fulfillment-execution/internal/domain/workbatch"
)

// ExpireLeases is the Clock-driven sweep that frees tasks whose lease has
// timed out without confirmation or completion, returning them to Pending
// so they are never silently lost.
type ExpireLeases struct {
	Tasks     ports.TaskRepo
	Publisher ports.EventPublisher
	Clock     ports.Clock
	// UnitOfWork brackets each freed task's Save + Publish atomically
	// (ADR 0020); nil runs them back to back. One scope per freed task, so
	// a failure part-way through the sweep leaves the already-freed tasks
	// committed (the sweep is re-run on the next tick anyway).
	UnitOfWork ports.UnitOfWork
	// Batches, when non-nil, keeps an expired member's work batch in step
	// (ADR 0063): the freed task leaves the batch (DetachMember), and when
	// no open member remains the batch is abandoned and raises
	// WorkBatchAbandoned — in the same scope as the task's own return to
	// the pool. Nil means the sweep runs exactly as before batching.
	Batches ports.WorkBatchRepo
}

// Execute scans every Claimed task and frees the ones whose lease has expired.
// Returns the count of tasks freed.
func (uc *ExpireLeases) Execute(ctx context.Context) (int, error) {
	now := uc.Clock.Now()
	claimed, err := uc.Tasks.FindAllClaimed(ctx)
	if err != nil {
		return 0, err
	}

	freed := 0
	for _, t := range claimed {
		if !t.ExpireLeaseIfDue(now) {
			continue
		}
		var batchEvents []shared.DomainEvent
		if uc.Batches != nil {
			evts, err := uc.detachBatchMember(ctx, t, now)
			if err != nil {
				return freed, err
			}
			batchEvents = evts
		}
		err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
			if err := uc.Tasks.Save(ctx, t); err != nil {
				return err
			}
			// LeaseExpired (analytics) and TaskAvailable (integration, ADR
			// 0049): the task re-enters the pool at `now`, in the same
			// transaction as the Save.
			events := []shared.DomainEvent{shared.NewLeaseExpired(t.Id(), now), shared.NewTaskAvailable(t.Id(), now)}
			events = append(events, batchEvents...)
			return uc.Publisher.Publish(ctx, events...)
		})
		if err != nil {
			return freed, err
		}
		freed++
	}
	return freed, nil
}

// detachBatchMember records that the freed task left its open work batch
// (ADR 0063) and returns the events that raised: nothing when the task is
// in no open batch, WorkBatchAbandoned when it was the last open member.
// DetachMember is idempotent, so a re-fired sweep tick changes nothing.
func (uc *ExpireLeases) detachBatchMember(ctx context.Context, t *task.Task, now time.Time) ([]shared.DomainEvent, error) {
	b, err := uc.Batches.FindOpenByTaskId(ctx, t.Id())
	if err != nil || b == nil {
		return nil, err
	}
	if err := b.DetachMember(t.Id(), now); err != nil {
		// ErrBatchNotOpen cannot happen (FindOpenByTaskId returned OPEN);
		// ErrMemberNotInBatch is the batch having moved on. Both quiet.
		return nil, nil
	}
	if err := uc.Batches.Save(ctx, b); err != nil {
		return nil, err
	}
	if b.Status() == workbatch.StatusAbandoned {
		return []shared.DomainEvent{shared.NewWorkBatchAbandoned(b.Id(), b.StationId(), len(b.Members()), b.Version(), now)}, nil
	}
	return nil, nil
}
