package usecases

import (
	"context"
	"log/slog"
	"sort"
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
	candidates = uc.orderWithHint(ctx, stationId, now, candidates)

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

// orderWithHint applies the station's active interleaving hint (ADR 0051)
// as an advisory tie-break. candidates arrive ordered earliest-CPT-first;
// the priority band of a task is (its type — uniform here —, its CPT
// instant), so a band is a maximal run of candidates sharing one CPT
// instant. Within each band the hinted candidates still in the pool come
// first, lowest rank (the producer's preference) before higher, followed by
// the unhinted peers in the repository's own order. Bands themselves are
// NEVER reordered: a hinted candidate with a later CPT stays behind every
// candidate of an earlier band.
//
// Fail-open everywhere: no Hints lookup wired, no hint stored, an expired
// hint, a hint whose candidates are all gone from the pool, or a hint read
// that fails — every one of these returns candidates unchanged, so an
// ignored hint costs nothing and can never stop dispatch.
func (uc *ClaimNext) orderWithHint(ctx context.Context, stationId shared.StationId, now time.Time, candidates []*task.Task) []*task.Task {
	if uc.Hints == nil || len(candidates) < 2 {
		return candidates
	}
	hint, err := uc.Hints.ActiveForStation(ctx, stationId, now)
	if err != nil {
		uc.logger().WarnContext(ctx, "interleaving hint lookup failed; claiming without it (fail-open)",
			"station_id", logSafe(string(stationId)), "error", logSafe(err.Error()))
		return candidates
	}
	if hint == nil || !hint.ExpiresAt.After(now) || len(hint.Candidates) == 0 {
		return candidates
	}
	rank := make(map[shared.TaskId]int, len(hint.Candidates))
	for _, c := range hint.Candidates {
		id := shared.TaskId(c.TaskId)
		if r, seen := rank[id]; !seen || c.Rank < r {
			rank[id] = c.Rank
		}
	}
	if !anyHinted(candidates, rank) {
		return candidates
	}
	out := make([]*task.Task, 0, len(candidates))
	for i := 0; i < len(candidates); {
		j := i + 1
		for j < len(candidates) && candidates[j].CPT().Time().Equal(candidates[i].CPT().Time()) {
			j++
		}
		out = append(out, hintFirst(candidates[i:j], rank)...)
		i = j
	}
	return out
}

// anyHinted reports whether any candidate is a hinted task.
func anyHinted(candidates []*task.Task, rank map[shared.TaskId]int) bool {
	for _, t := range candidates {
		if _, ok := rank[t.Id()]; ok {
			return true
		}
	}
	return false
}

// hintFirst orders one priority band: hinted candidates by ascending rank
// first, then the unhinted peers in their incoming order.
func hintFirst(band []*task.Task, rank map[shared.TaskId]int) []*task.Task {
	hinted := make([]*task.Task, 0, len(band))
	rest := make([]*task.Task, 0, len(band))
	for _, t := range band {
		if _, ok := rank[t.Id()]; ok {
			hinted = append(hinted, t)
		} else {
			rest = append(rest, t)
		}
	}
	sort.SliceStable(hinted, func(i, j int) bool { return rank[hinted[i].Id()] < rank[hinted[j].Id()] })
	return append(hinted, rest...)
}

func (uc *ClaimNext) logger() *slog.Logger {
	if uc.Logger != nil {
		return uc.Logger
	}
	return slog.Default()
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
