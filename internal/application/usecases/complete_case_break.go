package usecases

import (
	"context"
	"fmt"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// CaseBreakIdempotencyKey is the Idempotency-Key sent to inventory-storage
// for the break of one BREAK_CASE task: stable per task, so every retry of
// the completion replays the first answer instead of breaking twice.
func CaseBreakIdempotencyKey(id shared.TaskId) string { return "break-case:" + string(id) }

// CompleteCaseBreak completes a BREAK_CASE task (ADR 0054): after the claim
// rules hold it asks inventory-storage to break the sealed case into the
// loose bin (POST /containers/{lpn}:break, Idempotency-Key = the task), and
// only when that succeeds completes the task and publishes TaskCompleted. A
// failed break leaves the task Claimed and unchanged; a retry reuses the
// same idempotency key, so a break that succeeded but whose answer or local
// save was lost is not repeated.
type CompleteCaseBreak struct {
	Tasks      ports.TaskRepo
	Breaker    ports.CaseBreaker
	Publisher  ports.EventPublisher
	Clock      ports.Clock
	Metrics    ports.Metrics
	UnitOfWork ports.UnitOfWork
}

// Execute completes taskId on behalf of stationId.
func (uc *CompleteCaseBreak) Execute(ctx context.Context, taskId shared.TaskId, stationId shared.StationId) error {
	t, err := uc.Tasks.FindById(ctx, taskId)
	if err != nil {
		return err
	}
	if t == nil {
		return ErrTaskNotFound
	}
	brk, err := t.AuthorizeCaseBreak(stationId, uc.Clock.Now())
	if err != nil {
		return err
	}
	if err := uc.Breaker.BreakCase(ctx, ports.CaseBreakRequest{
		LPN: brk.LPN, LooseBin: brk.LooseBin, IdempotencyKey: CaseBreakIdempotencyKey(t.Id()),
	}); err != nil {
		return fmt.Errorf("break case %s of task %s: %w", brk.LPN, t.Id(), err)
	}
	now := uc.Clock.Now()
	if err := t.CompleteCaseBreak(stationId, now); err != nil {
		return err
	}
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Tasks.Save(ctx, t); err != nil {
			return err
		}
		return uc.Publisher.Publish(ctx, shared.NewTaskCompleted(t.Id(), stationId, now))
	})
	if err != nil {
		return err
	}
	if uc.Metrics != nil {
		uc.Metrics.TaskCompleted(ctx, task.BreakCase)
	}
	return nil
}
