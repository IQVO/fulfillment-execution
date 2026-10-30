package ports

import (
	"context"
	"errors"
)

// WorkPlanning is the outbound DELEGATION port to the Work Planning &
// Release context (wes-work-planning), carried over the Agent2Agent protocol
// (ADR-0058). It is deliberately goal-shaped — "admit the next unit for this
// path" — not a data query: facts about released work still arrive as
// CloudEvents on Kafka through the existing WorkReleased consumer; this port
// only asks the planning agent to ACT.
//
// Implementations must be safe for concurrent use.
type WorkPlanning interface {
	// ReleaseNextWork asks the planning agent to admit the next
	// priority-ordered pending work unit into path's pool. The planning side
	// owns every release decision (release policy, WIP invariant); a
	// rejection is a normal business answer, returned as a typed error.
	ReleaseNextWork(ctx context.Context, pathId string) (ReleasedWork, error)
}

// ReleasedWork is the narrow result of an accepted delegation: the work unit
// planning admitted. The WorkReleased event that follows on Kafka remains the
// authoritative fact — this value is the immediate acknowledgement only.
type ReleasedWork struct {
	WorkUnitId string
	CPT        string
	Reference  string
}

// Typed delegation outcomes. The outbound A2A adapter maps the planning
// agent's problem answers onto these; the HTTP adapter maps them to RFC 7807
// statuses. Wrap them (%w) to carry the agent's detail text.
var (
	// ErrNoPendingWork: planning rejected the request with a business rule —
	// the pool is empty or at its WIP limit. Nothing was released.
	ErrNoPendingWork = errors.New("work planning has no pending work for this path")
	// ErrUnknownWorkPlanningPath: planning knows no such process path.
	ErrUnknownWorkPlanningPath = errors.New("work planning does not know this process path")
	// ErrWorkPlanningUnreachable: the planning agent could not be reached or
	// answered something other than its contract.
	ErrWorkPlanningUnreachable = errors.New("work planning agent unreachable or answered off-contract")
)
