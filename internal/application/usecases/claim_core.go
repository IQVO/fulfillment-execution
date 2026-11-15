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

// claimer is the PULL-dispatch selection core ClaimNext has always used,
// extracted so FormWorkBatch (ADR 0063) can select its anchor through the
// exact same code. Anchor equivalence ("the anchor is exactly the task
// ClaimNext would return", model.yaml's AnchorIsTodaysClaim policy) then
// holds by construction, not by parallel maintenance of two scans — and the
// FormWorkBatch-vs-ClaimNext comparison test guards it from drifting apart.
//
// Every field mirrors the same-named field of ClaimNext; ClaimNext delegates
// here with its own wiring, so its behavior is unchanged bit for bit.
type claimer struct {
	Tasks         ports.TaskRepo
	Stations      ports.StationRepo
	Publisher     ports.EventPublisher
	Metrics       ports.Metrics
	UnitOfWork    ports.UnitOfWork
	Hints         ports.InterleavingHintLookup
	Logger        *slog.Logger
	LeaseDuration time.Duration
}

func (c *claimer) leaseDuration() time.Duration {
	if c.LeaseDuration <= 0 {
		return DefaultLeaseDuration
	}
	return c.LeaseDuration
}

func (c *claimer) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

// station returns the registered station for stationId, or
// ErrStationNotFound.
func (c *claimer) station(ctx context.Context, stationId shared.StationId) (*stationTaskView, error) {
	st, err := c.Stations.FindById(ctx, stationId)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, ErrStationNotFound
	}
	return &stationTaskView{id: stationId, capabilities: st.Capabilities()}, nil
}

// stationTaskView is the slice of the Station aggregate claiming needs: its
// id and its capability set. It exists so claimer need not import the
// station package for one accessor (and so the capability gate has exactly
// one input).
type stationTaskView struct {
	id           shared.StationId
	capabilities shared.CapabilitySet
}

// claimFirstClaimable finds the best-fit pending task of taskType for the
// station and leases it: candidates arrive earliest-CPT-first from the
// repository, the station's active interleaving hint re-orders peers inside
// one priority band (never across bands), and the first candidate the
// station's capabilities satisfy is claimed through the per-task
// compare-and-set. ErrNoClaimableTask when nothing fits.
func (c *claimer) claimFirstClaimable(ctx context.Context, stationId shared.StationId, taskType task.Type, now time.Time) (*task.Task, error) {
	st, err := c.station(ctx, stationId)
	if err != nil {
		return nil, err
	}
	candidates, err := c.Tasks.FindClaimableByType(ctx, taskType, now)
	if err != nil {
		return nil, err
	}
	candidates = c.orderWithHint(ctx, stationId, now, candidates)

	for _, t := range candidates {
		if t.Claim(stationId, st.capabilities, now, c.leaseDuration()) != nil {
			continue
		}
		won, err := c.persistClaim(ctx, t, stationId, now)
		if err != nil {
			return nil, err
		}
		if !won {
			continue
		}
		if c.Metrics != nil {
			c.Metrics.TaskClaimed(ctx, t.Type())
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
func (c *claimer) orderWithHint(ctx context.Context, stationId shared.StationId, now time.Time, candidates []*task.Task) []*task.Task {
	if c.Hints == nil || len(candidates) < 2 {
		return candidates
	}
	hint, err := c.Hints.ActiveForStation(ctx, stationId, now)
	if err != nil {
		c.logger().WarnContext(ctx, "interleaving hint lookup failed; claiming without it (fail-open)",
			"station_id", logSafe(string(stationId)), "error", logSafe(err.Error()))
		return candidates
	}
	if hint == nil || !hint.ExpiresAt.After(now) || len(hint.Candidates) == 0 {
		return candidates
	}
	rank := make(map[shared.TaskId]int, len(hint.Candidates))
	for _, cand := range hint.Candidates {
		id := shared.TaskId(cand.TaskId)
		if r, seen := rank[id]; !seen || cand.Rank < r {
			rank[id] = cand.Rank
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

// persistClaim saves t's claim and publishes TaskClaimed atomically. It is
// a compare-and-set, not a blind upsert: the candidate list is a plain
// read, so concurrent callers load the SAME Pending task. Only the first
// SaveClaim matches the still-claimable row; it reports won=false (and
// publishes nothing) for every caller that lost the race, which then moves
// on to the next candidate — at-most-once assignment, ADR-0003.
func (c *claimer) persistClaim(ctx context.Context, t *task.Task, stationId shared.StationId, now time.Time) (bool, error) {
	won := false
	err := atomically(ctx, c.UnitOfWork, func(ctx context.Context) error {
		ok, err := c.Tasks.SaveClaim(ctx, t, now)
		if err != nil || !ok {
			return err
		}
		won = true
		return c.Publisher.Publish(ctx, shared.NewTaskClaimed(t.Id(), stationId, now))
	})
	return won, err
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
