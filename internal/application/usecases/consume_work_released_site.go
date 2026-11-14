package usecases

import (
	"context"

	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// orderSite returns the order site to stamp on the task a release creates:
// req.SiteId when it is a valid site code (task.ParseOrderSite), else ""
// (unknown). Transfer work never takes one (its site is the transfer
// block's). A malformed value is a corrupt optional producer field, not a
// reason to fail or dead-letter the release: it is ignored with a WARN naming
// the event, so the task is still created, just without a site (ADR 0065).
func (uc *ApplyWorkReleased) orderSite(ctx context.Context, req WorkReleasedRequest) string {
	if req.Transfer != nil || req.SiteId == "" {
		return ""
	}
	site, err := task.ParseOrderSite(req.SiteId)
	if err != nil {
		uc.logger().WarnContext(ctx, "WorkReleased site_id is not a valid site code; order site left unknown",
			"event_id", req.EventId, "work_unit_id", req.WorkUnitId, "site_id", req.SiteId)
		return ""
	}
	return site
}
