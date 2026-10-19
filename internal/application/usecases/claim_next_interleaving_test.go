package usecases_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// fakeHintLookup is a programmable ports.InterleavingHintLookup.
type fakeHintLookup struct {
	hint *ports.InterleavingHint
	err  error
}

func (f fakeHintLookup) ActiveForStation(_ context.Context, _ shared.StationId, _ time.Time) (*ports.InterleavingHint, error) {
	return f.hint, f.err
}

// hintFor builds an active hint for stationId expiring after now, candidates
// in the given rank order (first = rank 1).
func hintFor(stationId string, now time.Time, taskIds ...string) *ports.InterleavingHint {
	candidates := make([]ports.InterleavingHintCandidate, len(taskIds))
	for i, id := range taskIds {
		candidates[i] = ports.InterleavingHintCandidate{Rank: i + 1, TaskId: id}
	}
	return &ports.InterleavingHint{
		HintId:     fmt.Sprintf("hint-%s", stationId),
		StationId:  stationId,
		SiteId:     "SITE-1",
		ExpiresAt:  now.Add(90 * time.Second),
		Candidates: candidates,
		ReceivedAt: now,
	}
}

// seedClaimNext seeds count PICK tasks with the given CPTs and one station,
// returning the wired ClaimNext (hint lookup attached) and its context.
func seedClaimNext(t *testing.T, now time.Time, cpts []time.Time, stationCaps []string, lookup ports.InterleavingHintLookup) (*usecases.ClaimNext, context.Context, shared.StationId) {
	t.Helper()
	ctx := context.Background()
	tasks := memory.NewTaskRepo()
	stations := memory.NewStationRepo()
	for i, cpt := range cpts {
		tk := task.New(shared.TaskId(fmt.Sprintf("t-%d", i)), task.Pick, shared.NewCPT(cpt), shared.OrderRef(fmt.Sprintf("o-%d", i)), shared.NewCapabilitySet("pick"), false, false)
		if err := tasks.Save(ctx, tk); err != nil {
			t.Fatalf("seed task: %v", err)
		}
	}
	stationId := shared.StationId("s-1")
	caps := make([]shared.Capability, len(stationCaps))
	for i, c := range stationCaps {
		caps[i] = shared.Capability(c)
	}
	if err := stations.Save(ctx, station.New(stationId, shared.NewCapabilitySet(caps...))); err != nil {
		t.Fatalf("seed station: %v", err)
	}
	uc := &usecases.ClaimNext{
		Tasks: tasks, Stations: stations, Publisher: events.NewLogPublisher(nil),
		Clock: memory.NewFixedClock(now), Hints: lookup,
	}
	return uc, ctx, stationId
}

// The hint is a tie-break INSIDE one priority band: of the same-CPT peers,
// the highest-ranked hinted candidate still in the pool is claimed first.
func TestClaimNext_PrefersHintedCandidateInsideOnePriorityBand(t *testing.T) {
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	band := now.Add(time.Hour)
	uc, ctx, stationId := seedClaimNext(t, now, []time.Time{band, band}, []string{"pick"},
		fakeHintLookup{hint: hintFor("s-1", now, "t-1", "t-0")})

	claimed, err := uc.Execute(ctx, stationId, task.Pick)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Id() != "t-1" {
		t.Fatalf("claimed %s, want the rank-1 hinted candidate t-1", claimed.Id())
	}
	// Once the hinted candidate is gone, the remaining peer claims as before.
	claimed, err = uc.Execute(ctx, stationId, task.Pick)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if claimed.Id() != "t-0" {
		t.Fatalf("second claim = %s, want t-0", claimed.Id())
	}
}

// The hint NEVER lifts a candidate across a priority/CPT boundary: a hinted
// task with a later CPT loses to an unhinted task with an earlier CPT.
func TestClaimNext_HintNeverCrossesAPriorityBandBoundary(t *testing.T) {
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	earlier := now.Add(time.Hour)
	later := now.Add(2 * time.Hour)
	uc, ctx, stationId := seedClaimNext(t, now, []time.Time{earlier, later}, []string{"pick"},
		fakeHintLookup{hint: hintFor("s-1", now, "t-1")}) // t-1 is the LATER-CPT task

	claimed, err := uc.Execute(ctx, stationId, task.Pick)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Id() != "t-0" {
		t.Fatalf("claimed %s, want t-0 (earlier CPT beats the hinted later-CPT candidate)", claimed.Id())
	}
}

// An expired hint is ignored: claiming behaves exactly as without a hint.
func TestClaimNext_ExpiredHintIsIgnored(t *testing.T) {
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	expired := hintFor("s-1", now, "t-1")
	expired.ExpiresAt = now.Add(-time.Second)
	uc, ctx, stationId := seedClaimNext(t, now,
		[]time.Time{now.Add(time.Hour), now.Add(2 * time.Hour)}, []string{"pick"},
		fakeHintLookup{hint: expired})

	claimed, err := uc.Execute(ctx, stationId, task.Pick)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Id() != "t-0" {
		t.Fatalf("claimed %s, want t-0 (the expired hint must be ignored)", claimed.Id())
	}
}

// A hint whose candidates are all gone from the pool costs nothing.
func TestClaimNext_HintWithNoCandidateInPoolIsIgnored(t *testing.T) {
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	uc, ctx, stationId := seedClaimNext(t, now,
		[]time.Time{now.Add(time.Hour), now.Add(2 * time.Hour)}, []string{"pick"},
		fakeHintLookup{hint: hintFor("s-1", now, "t-gone", "t-alsogone")})

	claimed, err := uc.Execute(ctx, stationId, task.Pick)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Id() != "t-0" {
		t.Fatalf("claimed %s, want t-0 (no hinted candidate in the pool)", claimed.Id())
	}
}

// The capability check still guards every claim: a hinted peer the station
// cannot work is skipped for the next peer inside the same band.
func TestClaimNext_HintedPeerFailingTheCapabilityCheckIsSkipped(t *testing.T) {
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	band := now.Add(time.Hour)
	ctx := context.Background()
	tasks := memory.NewTaskRepo()
	stations := memory.NewStationRepo()
	// t-0 requires "pack" (the station lacks it); t-1 requires "pick".
	if err := tasks.Save(ctx, task.New("t-0", task.Pick, shared.NewCPT(band), "o-0", shared.NewCapabilitySet("pack"), false, false)); err != nil {
		t.Fatalf("seed t-0: %v", err)
	}
	if err := tasks.Save(ctx, task.New("t-1", task.Pick, shared.NewCPT(band), "o-1", shared.NewCapabilitySet("pick"), false, false)); err != nil {
		t.Fatalf("seed t-1: %v", err)
	}
	if err := stations.Save(ctx, station.New("s-1", shared.NewCapabilitySet("pick"))); err != nil {
		t.Fatalf("seed station: %v", err)
	}
	uc := &usecases.ClaimNext{
		Tasks: tasks, Stations: stations, Publisher: events.NewLogPublisher(nil),
		Clock: memory.NewFixedClock(now),
		Hints: fakeHintLookup{hint: hintFor("s-1", now, "t-0", "t-1")},
	}
	claimed, err := uc.Execute(ctx, "s-1", task.Pick)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Id() != "t-1" {
		t.Fatalf("claimed %s, want t-1 (the hinted t-0 fails the capability check)", claimed.Id())
	}
}

// A failing hint lookup fails open: claimNext behaves exactly as without a
// hint and returns no error.
func TestClaimNext_HintLookupErrorFailsOpen(t *testing.T) {
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	uc, ctx, stationId := seedClaimNext(t, now,
		[]time.Time{now.Add(time.Hour), now.Add(2 * time.Hour)}, []string{"pick"},
		fakeHintLookup{err: errFake})

	claimed, err := uc.Execute(ctx, stationId, task.Pick)
	if err != nil {
		t.Fatalf("claim with a failing hint lookup: %v", err)
	}
	if claimed.Id() != "t-0" {
		t.Fatalf("claimed %s, want t-0 (a hint read error must fail open)", claimed.Id())
	}
}
