package usecases

import (
	"context"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// orderSite returns the fulfillment site of the order's PICK tasks (the
// first non-empty one, tasks ordered by id), or "" when none of them carries
// one — a PACK task created without a site, exactly as before ADR 0065. The
// PICK tasks are found by the order id they were released for
// (Task.SourceOrderId), which on this rebin path is the posted orderRef. Only
// PICK tasks speak for the order's site: they are the order work released
// with the order's WorkReleased.site_id. A repo failure is returned so the
// caller's atomic scope rolls back.
func (uc *ArriveAtRebin) orderSite(ctx context.Context, orderRef shared.OrderRef) (string, error) {
	tasks, err := uc.CreateTask.Tasks.FindBySourceOrderId(ctx, string(orderRef))
	if err != nil {
		return "", err
	}
	for _, t := range tasks {
		if t.Type() == task.Pick && t.OrderSiteId() != "" {
			return t.OrderSiteId(), nil
		}
	}
	return "", nil
}
