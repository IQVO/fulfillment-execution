package usecases

import (
	"context"
	"errors"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// ErrPathIdRequired is returned by RequestNextWork when the path id is
// empty: never defaulted, never sent to the planning agent.
var ErrPathIdRequired = errors.New("usecases: pathId is required to request work")

// ErrWorkPlanningNotConfigured is returned by RequestNextWork when this
// deployment has no WORK_PLANNING_A2A_URL: the delegation surface is
// explicitly off, and the endpoint answers 503 rather than pretending.
var ErrWorkPlanningNotConfigured = errors.New("usecases: work-planning delegation not configured (WORK_PLANNING_A2A_URL unset)")

// RequestNextWork is the PULL side of the execution/planning seam over A2A
// (ADR-0058): execution, whose claim queue for a path is draining, asks the
// planning agent to admit the next unit. Planning keeps every release
// decision (release policy, WIP invariant); execution keeps the judgement of
// WHEN to ask. The released work's authoritative fact still arrives as the
// WorkReleased CloudEvent through the existing consumer — this use case only
// delegates and reports the acknowledgement.
type RequestNextWork struct {
	// Planning is the delegation port. Nil means the deployment has the
	// surface off: Execute answers ErrWorkPlanningNotConfigured, never a
	// nil-dereference — a half-wired process must fail closed.
	Planning ports.WorkPlanning
}

// RequestNextWorkRequest names the process path more work is requested for.
type RequestNextWorkRequest struct {
	PathId string
}

// Execute validates the path id and delegates. Rejections from planning are
// returned unchanged (typed ports errors carry the peer's reason); an empty
// path id is refused here — never defaulted, never sent.
func (uc *RequestNextWork) Execute(ctx context.Context, req RequestNextWorkRequest) (ports.ReleasedWork, error) {
	if req.PathId == "" {
		return ports.ReleasedWork{}, ErrPathIdRequired
	}
	if uc.Planning == nil {
		return ports.ReleasedWork{}, ErrWorkPlanningNotConfigured
	}
	return uc.Planning.ReleaseNextWork(ctx, req.PathId)
}
