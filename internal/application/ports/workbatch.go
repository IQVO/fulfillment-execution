package ports

import (
	"context"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/workbatch"
)

// WorkBatchRepo persists and retrieves WorkBatch aggregates (ADR 0063).
//
// FindOpenByStation and FindOpenByTaskId return nil (not an error) when no
// match exists, mirroring TaskRepo.FindById's convention.
type WorkBatchRepo interface {
	Save(ctx context.Context, b *workbatch.WorkBatch) error
	FindById(ctx context.Context, id shared.BatchId) (*workbatch.WorkBatch, error)
	// FindOpenByStation returns the station's OPEN batch, or nil when it
	// holds none — the ErrStationHasOpenBatch formation guard.
	FindOpenByStation(ctx context.Context, stationId shared.StationId) (*workbatch.WorkBatch, error)
	// FindOpenByTaskId returns the OPEN batch in which taskId is a still-
	// OPEN member, or nil — the completion and lease-expiry hooks use it
	// to keep the batch in step with the task lifecycle. A detached member
	// (already returned to the pool) and a completed member are both "no
	// match": neither accepts a further lifecycle transition.
	FindOpenByTaskId(ctx context.Context, taskId shared.TaskId) (*workbatch.WorkBatch, error)
	// FindByTaskId returns the batch this task's completion belongs to:
	// the batch in which taskId is a non-detached member of a
	// non-abandoned batch, most recently formed first, or nil. Unlike
	// FindOpenByTaskId it also matches the batch that JUST completed (its
	// status flipped to COMPLETED in the same transaction as the
	// completion), so the last member's TaskCompleted still carries
	// batch_id and batch_size. A member that detached (lease expired) and
	// completed as an ordinary task matches nothing.
	FindByTaskId(ctx context.Context, taskId shared.TaskId) (*workbatch.WorkBatch, error)
}
