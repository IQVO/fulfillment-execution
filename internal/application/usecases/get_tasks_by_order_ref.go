package usecases

import (
	"context"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// GetTasksByOrderRef is a read-only query that traces every task this
// service created for a given order-management order back from its
// orderRef — a Pick, Pack, and SLAM leg is typically one each, but a leg
// may appear more than once if it was retried. It exists so a cross-service
// console can answer "what tasks has fulfillment-execution created/
// completed for order X" without a database backdoor into this service.
type GetTasksByOrderRef struct {
	Tasks ports.TaskRepo
}

// Execute returns every task recorded for orderRef, in whatever order the
// repository returns them. An unknown orderRef returns an empty slice, not
// an error — this mirrors GetQueueDepth treating an unrecognized taskType
// as zero rather than a failure.
// ExecutePage returns one keyset page of the order's tasks: at most limit
// tasks with id > afterId ("" = first page), ordered by id.
func (uc *GetTasksByOrderRef) ExecutePage(ctx context.Context, orderRef shared.OrderRef, afterId shared.TaskId, limit int) ([]*task.Task, error) {
	return uc.Tasks.FindPageByOrderRef(ctx, orderRef, afterId, limit)
}

func (uc *GetTasksByOrderRef) Execute(ctx context.Context, orderRef shared.OrderRef) ([]*task.Task, error) {
	return uc.Tasks.FindByOrderRef(ctx, orderRef)
}
