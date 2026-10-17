package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/interleavinghintcopy"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// countingHintStore records every ReplaceForStation call and can be forced
// to fail.
type countingHintStore struct {
	calls  int
	stored ports.InterleavingHint
	fail   bool
}

func (s *countingHintStore) ReplaceForStation(_ context.Context, hint ports.InterleavingHint) error {
	if s.fail {
		return errFake
	}
	s.calls++
	s.stored = hint
	return nil
}

func hintRequest(eventId, stationId, hintId string, now time.Time, taskIds ...string) usecases.InterleavingHintRequest {
	candidates := make([]ports.InterleavingHintCandidate, len(taskIds))
	for i, id := range taskIds {
		candidates[i] = ports.InterleavingHintCandidate{Rank: i + 1, TaskId: id}
	}
	return usecases.InterleavingHintRequest{
		EventId: eventId,
		Hint: ports.InterleavingHint{
			HintId:     hintId,
			StationId:  stationId,
			SiteId:     "SITE-1",
			ExpiresAt:  now.Add(90 * time.Second),
			Candidates: candidates,
			ReceivedAt: now,
		},
	}
}

// A hint is stored, and a newer hint for the same station supersedes the
// previous one (one ACTIVE hint per station).
func TestApplyInterleavingHint_StoresAndSupersedesPerStation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	store := interleavinghintcopy.NewMemoryStore()
	uc := &usecases.ApplyInterleavingHint{Processed: memory.NewProcessedEventsRepo(), Hints: store}

	if err := uc.Execute(ctx, hintRequest("ce-1", "STN-07", "hint-1", now, "t-a", "t-b")); err != nil {
		t.Fatalf("apply first hint: %v", err)
	}
	active, err := store.ActiveForStation(ctx, "STN-07", now)
	if err != nil || active == nil {
		t.Fatalf("active hint = %+v, %v; want hint-1 stored", active, err)
	}
	if active.HintId != "hint-1" || len(active.Candidates) != 2 {
		t.Fatalf("active hint = %+v, want hint-1 with two candidates", active)
	}

	if err := uc.Execute(ctx, hintRequest("ce-2", "STN-07", "hint-2", now, "t-c")); err != nil {
		t.Fatalf("apply superseding hint: %v", err)
	}
	active, err = store.ActiveForStation(ctx, "STN-07", now)
	if err != nil || active == nil {
		t.Fatalf("active hint after supersede = %+v, %v", active, err)
	}
	if active.HintId != "hint-2" || len(active.Candidates) != 1 || active.Candidates[0].TaskId != "t-c" {
		t.Fatalf("active hint = %+v, want hint-2 [t-c] (the newer hint supersedes)", active)
	}

	// A hint for ANOTHER station does not disturb STN-07's.
	if err := uc.Execute(ctx, hintRequest("ce-3", "STN-08", "hint-3", now, "t-z")); err != nil {
		t.Fatalf("apply other station hint: %v", err)
	}
	active, _ = store.ActiveForStation(ctx, "STN-07", now)
	if active == nil || active.HintId != "hint-2" {
		t.Fatalf("STN-07 hint = %+v, want hint-2 untouched by STN-08's hint", active)
	}
}

// At-least-once delivery: a redelivery of the same CloudEvents id has
// exactly one effect.
func TestApplyInterleavingHint_DuplicateCloudEventIdHasOneEffect(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	store := &countingHintStore{}
	uc := &usecases.ApplyInterleavingHint{Processed: memory.NewProcessedEventsRepo(), Hints: store}

	req := hintRequest("ce-dup", "STN-07", "hint-1", now, "t-a")
	if err := uc.Execute(ctx, req); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	if err := uc.Execute(ctx, req); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if store.calls != 1 {
		t.Fatalf("ReplaceForStation called %d times, want exactly 1 (the duplicate id is deduped)", store.calls)
	}
}

// Without a UnitOfWork (the in-memory configuration) a failed upsert undoes
// the processed-event claim, so the retry re-applies instead of the event
// being lost.
func TestApplyInterleavingHint_FailedUpsertReleasesTheClaim(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	processed := memory.NewProcessedEventsRepo()
	store := &countingHintStore{fail: true}
	uc := &usecases.ApplyInterleavingHint{Processed: processed, Hints: store}

	req := hintRequest("ce-fail", "STN-07", "hint-1", now, "t-a")
	if err := uc.Execute(ctx, req); err == nil {
		t.Fatal("apply with a failing store must return an error")
	}
	isNew, err := processed.MarkProcessed(ctx, "ce-fail")
	if err != nil {
		t.Fatalf("probe processed: %v", err)
	}
	if !isNew {
		t.Fatal("the failed apply must release the processed-event claim (no UnitOfWork)")
	}
}

// An expired hint is stored but never returned as active (fail-open read
// side of the copy itself).
func TestMemoryHintStore_ActiveForStationFiltersExpiry(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	store := interleavinghintcopy.NewMemoryStore()
	uc := &usecases.ApplyInterleavingHint{Processed: memory.NewProcessedEventsRepo(), Hints: store}

	req := hintRequest("ce-exp", "STN-07", "hint-exp", now, "t-a")
	req.Hint.ExpiresAt = now.Add(-time.Minute)
	if err := uc.Execute(ctx, req); err != nil {
		t.Fatalf("apply expired hint: %v", err)
	}
	active, err := store.ActiveForStation(ctx, shared.StationId("STN-07"), now)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if active != nil {
		t.Fatalf("active hint = %+v, want nil for an expired hint", active)
	}
	if none, _ := store.ActiveForStation(ctx, "STN-NONE", now); none != nil {
		t.Fatalf("unknown station yielded %+v, want nil", none)
	}
}
