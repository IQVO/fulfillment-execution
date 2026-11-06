package usecases

import (
	"context"
	"log/slog"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// DefaultLeaseDuration is how long a claim is held before it expires and the
// task returns to the pool.
const DefaultLeaseDuration = 5 * time.Minute

// ClaimNext implements PULL dispatch: a station calls claimNext and the
// system selects the highest-priority (earliest CPT) pending task the
// station is certified/equipped for. The station is never named in advance.
//
// Since ADR 0063 the selection core lives in claimer (claim_core.go) and is
// shared with FormWorkBatch, whose anchor is exactly the task this use case
// returns — same code, not a copy.
type ClaimNext struct {
	Tasks         ports.TaskRepo
	Stations      ports.StationRepo
	Publisher     ports.EventPublisher
	Clock         ports.Clock
	LeaseDuration time.Duration
	// Metrics is optional: nil means this use case runs uninstrumented.
	Metrics ports.Metrics
	// UnitOfWork brackets Save + Publish atomically (ADR 0020); nil runs
	// them back to back.
	UnitOfWork ports.UnitOfWork
	// Hints, when non-nil, is the local copy of task-interleaving's
	// advisory hints (ADR 0051). It only re-orders peers INSIDE one
	// priority band (same task type, same CPT instant): the band order —
	// earliest CPT first — is never crossed. Nil means no hints at all
	// (exactly the pre-ADR-0051 behavior).
	Hints ports.InterleavingHintLookup
	// Logger receives the WARN when a hint read fails (claiming then
	// proceeds without the hint — fail-open). Nil means slog.Default().
	Logger *slog.Logger
}

// core returns the claimer with this use case's wiring.
func (uc *ClaimNext) core() *claimer {
	return &claimer{
		Tasks:         uc.Tasks,
		Stations:      uc.Stations,
		Publisher:     uc.Publisher,
		Metrics:       uc.Metrics,
		UnitOfWork:    uc.UnitOfWork,
		Hints:         uc.Hints,
		Logger:        uc.Logger,
		LeaseDuration: uc.LeaseDuration,
	}
}

// Execute finds the best-fit pending task for stationId and leases it.
func (uc *ClaimNext) Execute(ctx context.Context, stationId shared.StationId, taskType task.Type) (*task.Task, error) {
	return uc.core().claimFirstClaimable(ctx, stationId, taskType, uc.Clock.Now())
}
