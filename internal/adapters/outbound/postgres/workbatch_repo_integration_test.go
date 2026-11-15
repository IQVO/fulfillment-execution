//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/workbatch"
)

// formedBatch builds a two-member batch: the unhinted anchor (rank 0, so it
// visits first) and a hinted class-3 companion.
func formedBatch(t *testing.T, id string, now time.Time) *workbatch.WorkBatch {
	t.Helper()
	cpt := now.Add(6 * time.Hour)
	b, err := workbatch.Form(
		shared.BatchId(id), shared.StationId("stn-1"), 4, cpt,
		[]workbatch.Member{
			{TaskId: "wb-t1", Cpt: cpt, FromBin: "A-01", HazardClass: 0, Rank: 0},
			{TaskId: "wb-t2", Cpt: cpt, FromBin: "A-02", HazardClass: 3, Rank: 1},
		},
		now,
	)
	if err != nil {
		t.Fatalf("Form: %v", err)
	}
	return b
}

// TestWorkBatchRepo_SaveAndReadRoundTrip pins the persistence contract:
// the aggregate rehydrates field for field (members in stop order), and a
// lifecycle transition (complete one member, complete the last) survives
// the same Save path the use cases use.
func TestWorkBatchRepo_SaveAndReadRoundTrip(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewWorkBatchRepo(pool)
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	b := formedBatch(t, "wb-r1", now)
	if err := repo.Save(ctx, b); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := repo.FindById(ctx, "wb-r1")
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if got == nil || got.Id() != "wb-r1" || got.StationId() != "stn-1" || got.Status() != workbatch.StatusOpen {
		t.Fatalf("batch = %+v", got)
	}
	if ms := got.Members(); len(ms) != 2 || ms[0].TaskId != "wb-t1" || ms[1].TaskId != "wb-t2" || ms[1].Compartment != 2 || ms[1].HazardClass != 3 {
		t.Fatalf("members = %+v", ms)
	}

	// Complete both members; the second completes the batch.
	if err := got.CompleteMember("wb-t1", now.Add(2*time.Minute)); err != nil {
		t.Fatalf("CompleteMember t1: %v", err)
	}
	if err := repo.Save(ctx, got); err != nil {
		t.Fatalf("Save after t1: %v", err)
	}
	if err := got.CompleteMember("wb-t2", now.Add(4*time.Minute)); err != nil {
		t.Fatalf("CompleteMember t2: %v", err)
	}
	if got.Status() != workbatch.StatusCompleted {
		t.Fatalf("status = %s, want COMPLETED", got.Status())
	}
	if err := repo.Save(ctx, got); err != nil {
		t.Fatalf("Save after t2: %v", err)
	}

	done, err := repo.FindById(ctx, "wb-r1")
	if err != nil {
		t.Fatalf("re-FindById: %v", err)
	}
	if done.Status() != workbatch.StatusCompleted || done.CompletedAt() == nil || done.WalkSeconds() != 240 {
		t.Fatalf("completed batch = status %s completedAt %v walk %d", done.Status(), done.CompletedAt(), done.WalkSeconds())
	}
	// A COMPLETED batch is neither open-by-station nor open-by-task.
	if open, err := repo.FindOpenByStation(ctx, "stn-1"); err != nil || open != nil {
		t.Fatalf("FindOpenByStation after completion = %v, %v", open, err)
	}
	if open, err := repo.FindOpenByTaskId(ctx, "wb-t1"); err != nil || open != nil {
		t.Fatalf("FindOpenByTaskId after completion = %v, %v", open, err)
	}
	// FindByTaskId still matches the just-completed batch (the TaskCompleted
	// enrichment path, ADR 0063).
	if byTask, err := repo.FindByTaskId(ctx, "wb-t1"); err != nil || byTask == nil || byTask.Id() != "wb-r1" {
		t.Fatalf("FindByTaskId = %v, %v", byTask, err)
	}
}

// TestWorkBatchRepo_OpenLookups pins the two guards the use cases lean on:
// the station's OPEN batch, and the OPEN batch a claimed task belongs to —
// plus the write-side one-open-per-station partial unique index.
func TestWorkBatchRepo_OpenLookups(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewWorkBatchRepo(pool)
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	if err := repo.Save(ctx, formedBatch(t, "wb-o1", now)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if open, err := repo.FindOpenByStation(ctx, "stn-1"); err != nil || open == nil || open.Id() != "wb-o1" {
		t.Fatalf("FindOpenByStation = %v, %v", open, err)
	}
	if open, err := repo.FindOpenByStation(ctx, "stn-2"); err != nil || open != nil {
		t.Fatalf("FindOpenByStation other station = %v, %v", open, err)
	}
	if open, err := repo.FindOpenByTaskId(ctx, "wb-t2"); err != nil || open == nil || open.Id() != "wb-o1" {
		t.Fatalf("FindOpenByTaskId = %v, %v", open, err)
	}

	// A second OPEN batch for the same station must fail on the write
	// (migration 0027's partial unique index), exactly like a lost claim
	// race fails the compare-and-set.
	if err := repo.Save(ctx, formedBatch(t, "wb-o2", now)); err == nil {
		t.Fatal("second OPEN batch for one station must fail the partial unique index")
	}

	// A DETACHED member no longer matches FindOpenByTaskId, and abandoning
	// every member clears FindOpenByStation.
	open, err := repo.FindOpenByStation(ctx, "stn-1")
	if err != nil || open == nil {
		t.Fatalf("reload: %v, %v", open, err)
	}
	if err := open.DetachMember("wb-t1", now.Add(time.Minute)); err != nil {
		t.Fatalf("DetachMember t1: %v", err)
	}
	if err := open.DetachMember("wb-t2", now.Add(time.Minute)); err != nil {
		t.Fatalf("DetachMember t2: %v", err)
	}
	if open.Status() != workbatch.StatusAbandoned {
		t.Fatalf("status = %s, want ABANDONED", open.Status())
	}
	if err := repo.Save(ctx, open); err != nil {
		t.Fatalf("Save abandoned: %v", err)
	}
	if still, err := repo.FindOpenByStation(ctx, "stn-1"); err != nil || still != nil {
		t.Fatalf("FindOpenByStation after abandon = %v, %v", still, err)
	}
	if byTask, err := repo.FindByTaskId(ctx, "wb-t1"); err != nil || byTask != nil {
		t.Fatalf("FindByTaskId detached member = %v, %v", byTask, err)
	}
}
