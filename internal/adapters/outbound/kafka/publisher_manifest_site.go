package kafka

import "github.com/claudioed/fulfillment-execution/internal/domain/task"

// pickPackTask selects the PACK task a manifest's package was sealed from
// among an order's tasks, independent of their order in the slice: a PACK
// task that was actually worked (Claimed or Completed) beats one that is
// back to Pending (a retried leg), among equals the most recently claimed
// wins, and the lowest id breaks a final tie — the same deterministic rule
// pickSlamTask applies to the SLAM leg (slamPreferred is task-type-agnostic).
// Returns nil when the order has no PACK task (ADR 0065).
func pickPackTask(tasks []*task.Task) *task.Task {
	var best *task.Task
	for _, t := range tasks {
		if t.Type() != task.Pack {
			continue
		}
		if best == nil || slamPreferred(t, best) {
			best = t
		}
	}
	return best
}

// firstOrderSite returns the first non-empty order site among legs, in the
// given order (nil legs are skipped), or "" when none carries one (ADR 0065).
func firstOrderSite(legs ...*task.Task) string {
	for _, t := range legs {
		if t != nil && t.OrderSiteId() != "" {
			return t.OrderSiteId()
		}
	}
	return ""
}
