package usecases

import (
	"context"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
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
		err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
			if err := uc.Tasks.Save(ctx, t); err != nil {
				return err
			}
			// LeaseExpired (analytics) and TaskAvailable (integration, ADR
			// 0049): the task re-enters the pool at `now`, in the same
			// transaction as the Save.
			return uc.Publisher.Publish(ctx, shared.NewLeaseExpired(t.Id(), now), shared.NewTaskAvailable(t.Id(), now))
		})
		if err != nil {
			return freed, err
		}
		freed++
	}
	return freed, nil
}
