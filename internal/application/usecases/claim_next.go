package usecases

import (
	"context"
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
}

// Execute finds the best-fit pending task for stationId and leases it.
func (uc *ClaimNext) Execute(ctx context.Context, stationId shared.StationId, taskType task.Type) (*task.Task, error) {
	st, err := uc.Stations.FindById(ctx, stationId)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, ErrStationNotFound
	}

	now := uc.Clock.Now()
	candidates, err := uc.Tasks.FindClaimableByType(ctx, taskType, now)
	if err != nil {
		return nil, err
	}

	leaseDuration := uc.LeaseDuration
	if leaseDuration <= 0 {
		leaseDuration = DefaultLeaseDuration
	}

	// candidates are ordered earliest-CPT-first by the repository; take the
	// first one this station's capabilities satisfy.
	for _, t := range candidates {
		if t.Claim(stationId, st.Capabilities(), now, leaseDuration) != nil {
			continue
		}
		won, err := uc.persistClaim(ctx, t, stationId, now)
		if err != nil {
			return nil, err
		}
		if !won {
			continue
		}
		if uc.Metrics != nil {
			uc.Metrics.TaskClaimed(ctx, t.Type())
		}
		return t, nil
	}

	return nil, ErrNoClaimableTask
}

// persistClaim saves t's claim and publishes TaskClaimed atomically. It is
// a compare-and-set, not a blind upsert: ClaimNext's candidates are a plain
// read, so concurrent calls load the SAME Pending task. Only the first
// SaveClaim matches the still-claimable row; it reports won=false (and
// publishes nothing) for every station that lost the race, which then
// moves on to the next candidate -- at-most-once assignment, ADR-0003.
func (uc *ClaimNext) persistClaim(ctx context.Context, t *task.Task, stationId shared.StationId, now time.Time) (bool, error) {
	won := false
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		ok, err := uc.Tasks.SaveClaim(ctx, t, now)
		if err != nil || !ok {
			return err
		}
		won = true
		return uc.Publisher.Publish(ctx, shared.NewTaskClaimed(t.Id(), stationId, now))
	})
	return won, err
}
