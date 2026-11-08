package usecases

import (
	"context"
	"errors"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
	"github.com/claudioed/fulfillment-execution/internal/domain/workbatch"
)

// GetWorkBatch is the read side of the WorkBatch aggregate (ADR 0063): one
// batch by id, for GET /work-batches/{batchId} and the MCP read tool.
type GetWorkBatch struct {
	Batches ports.WorkBatchRepo
}

// Execute returns the batch with id, or ErrBatchNotFound.
func (uc *GetWorkBatch) Execute(ctx context.Context, id shared.BatchId) (*workbatch.WorkBatch, error) {
	b, err := uc.Batches.FindById(ctx, id)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, ErrBatchNotFound
	}
	return b, nil
}

// RenewWorkBatchLease implements the LeaseTogether policy (ADR 0063,
// model.yaml): renewing a batch's lease renews EVERY open member's task
// lease, so later stops of a long walk cannot expire mid-walk. Completed
// members are not renewed (their tasks are Completed); detached members are
// gone (the sweep already returned them to the pool).
type RenewWorkBatchLease struct {
	Tasks   ports.TaskRepo
	Batches ports.WorkBatchRepo
	Clock   ports.Clock
	// LeaseDuration mirrors RenewLease's; <= 0 means DefaultLeaseDuration.
	LeaseDuration time.Duration
}

// Execute renews every open member's lease on behalf of the batch's
// station. Every open member is attempted even when one fails (a partial
// renewal beats none — the floor client retries, and the sweep detaches
// whatever actually expired); the first failure is returned afterwards.
func (uc *RenewWorkBatchLease) Execute(ctx context.Context, batchId shared.BatchId) error {
	b, err := uc.Batches.FindById(ctx, batchId)
	if err != nil {
		return err
	}
	if b == nil {
		return ErrBatchNotFound
	}
	if b.Status() != workbatch.StatusOpen {
		return workbatch.ErrBatchNotOpen
	}
	now := uc.Clock.Now()
	var firstErr error
	for _, m := range b.OpenMembers() {
		if err := uc.renewMember(ctx, b.StationId(), m.TaskId, now); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// renewMember renews one open member's task lease. A member whose lease
// already expired frees to Pending and answers ErrNotClaimed (the sweep
// will detach it from the batch — not the renewal's failure to report), and
// a member whose task already completed (bookkeeping not yet in step) is
// equally not a failure; every other problem is returned.
func (uc *RenewWorkBatchLease) renewMember(ctx context.Context, stationId shared.StationId, taskId shared.TaskId, now time.Time) error {
	t, err := uc.Tasks.FindById(ctx, taskId)
	if err != nil {
		return err
	}
	if t == nil {
		return nil
	}
	duration := uc.LeaseDuration
	if duration <= 0 {
		duration = DefaultLeaseDuration
	}
	if err := t.RenewLease(stationId, now, duration); err != nil {
		if errors.Is(err, task.ErrNotClaimed) || errors.Is(err, task.ErrAlreadyCompleted) {
			return nil
		}
		return err
	}
	return uc.Tasks.Save(ctx, t)
}
