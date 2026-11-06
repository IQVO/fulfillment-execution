package workbatch_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/workbatch"
)

// cpt parses "HH:MM" on the fixture date the workbatch tests use.
func cpt(clock string) time.Time {
	parsed, err := time.Parse("15:04", clock)
	if err != nil {
		panic("workbatch test fixture clock must be HH:MM: " + clock)
	}
	return time.Date(2026, 1, 1, parsed.Hour(), parsed.Minute(), 0, 0, time.UTC)
}

// member builds a batch member fixture: a pickable task at the given band
// instant and pick face, unhinted (rank 0), unclassified (hazard 0).
func member(id string, band time.Time, fromBin string) workbatch.Member {
	return workbatch.Member{TaskId: shared.TaskId(id), Cpt: band, FromBin: fromBin}
}

// hinted is member with a hint rank (the producer's preference order).
func hinted(id string, band time.Time, fromBin string, rank int) workbatch.Member {
	m := member(id, band, fromBin)
	m.Rank = rank
	return m
}

// hazarded is member carrying a DOT hazard class on its SKU.
func hazarded(id string, band time.Time, fromBin string, class int) workbatch.Member {
	m := member(id, band, fromBin)
	m.HazardClass = class
	return m
}

// formed builds a valid batch-of-two fixture.
func formed(t *testing.T) *workbatch.WorkBatch {
	t.Helper()
	b, err := workbatch.Form("b1", "stn-1", 4, cpt("10:00"), []workbatch.Member{
		member("t1", cpt("10:00"), "A-01"),
		member("t2", cpt("10:00"), "A-02"),
	})
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	return b
}

// --- Form: the invariant gate (model.yaml, ADR 0063) -------------------------

// TestForm_RejectsAcrossPriorityBand: a member with a different CPT instant
// crosses the anchor's priority band — never allowed (D3).
func TestForm_RejectsAcrossPriorityBand(t *testing.T) {
	_, err := workbatch.Form("b1", "stn-1", 4, cpt("10:00"), []workbatch.Member{
		member("t1", cpt("10:00"), "A-01"),
		member("t2", cpt("11:00"), "A-02"),
	})
	if !errors.Is(err, workbatch.ErrCrossesPriorityBand) {
		t.Fatalf("err = %v", err)
	}
}

// TestForm_BatchOfOneIsValid: nothing waits for a batch to fill — a batch of
// one (the anchor alone) is valid and returned at once.
func TestForm_BatchOfOneIsValid(t *testing.T) {
	b, err := workbatch.Form("b1", "stn-1", 4, cpt("10:00"), []workbatch.Member{member("t1", cpt("10:00"), "A-01")})
	if err != nil || len(b.Members()) != 1 {
		t.Fatalf("b = %+v, err = %v", b, err)
	}
}

// TestForm_RejectsMoreThanMaximum: more members than the configured maximum
// (D4) is ErrBatchSize.
func TestForm_RejectsMoreThanMaximum(t *testing.T) {
	_, err := workbatch.Form("b1", "stn-1", 1, cpt("10:00"), []workbatch.Member{
		member("t1", cpt("10:00"), "A-01"),
		member("t2", cpt("10:00"), "A-02"),
	})
	if !errors.Is(err, workbatch.ErrBatchSize) {
		t.Fatalf("err = %v", err)
	}
}

// TestForm_RejectsEmptyBatch: zero members is ErrBatchSize too — a batch
// exists only around an anchor.
func TestForm_RejectsEmptyBatch(t *testing.T) {
	if _, err := workbatch.Form("b1", "stn-1", 4, cpt("10:00"), nil); !errors.Is(err, workbatch.ErrBatchSize) {
		t.Fatalf("err = %v", err)
	}
}

// TestForm_RejectsMemberWithoutBin: every member must be a pickable PICK with
// a planned from_bin — an empty pick face is ErrMemberNotPickable.
func TestForm_RejectsMemberWithoutBin(t *testing.T) {
	_, err := workbatch.Form("b1", "stn-1", 4, cpt("10:00"), []workbatch.Member{
		member("t1", cpt("10:00"), "A-01"),
		member("t2", cpt("10:00"), ""),
	})
	if !errors.Is(err, workbatch.ErrMemberNotPickable) {
		t.Fatalf("err = %v", err)
	}
}

// TestForm_RejectsHazardConflict: hazard classes that may not share one cart
// (the segregation matrix, class 2 vs class 3) are ErrSegregationConflict.
func TestForm_RejectsHazardConflict(t *testing.T) {
	_, err := workbatch.Form("b1", "stn-1", 4, cpt("10:00"), []workbatch.Member{
		hazarded("t1", cpt("10:00"), "A-01", 2),
		hazarded("t2", cpt("10:00"), "A-02", 3),
	})
	if !errors.Is(err, workbatch.ErrSegregationConflict) {
		t.Fatalf("err = %v", err)
	}
}

// TestForm_AllowsUnclassifiedAndCompatible: an unclassified SKU (hazard 0)
// never triggers or blocks segregation (fail-open), and matrix-compatible
// classes share the cart.
func TestForm_AllowsUnclassifiedAndCompatible(t *testing.T) {
	if _, err := workbatch.Form("b1", "stn-1", 4, cpt("10:00"), []workbatch.Member{
		hazarded("t1", cpt("10:00"), "A-01", 0),
		hazarded("t2", cpt("10:00"), "A-02", 2),
		hazarded("t3", cpt("10:00"), "A-03", 2),
	}); err != nil {
		t.Fatalf("err = %v", err)
	}
}

// TestForm_AssignsStopSeqInRankOrder: v1 visits stops in hint rank order
// (D6); stop_seq and the cart compartment follow that order, not the caller's
// slice order.
func TestForm_AssignsStopSeqInRankOrder(t *testing.T) {
	b, err := workbatch.Form("b1", "stn-1", 4, cpt("10:00"), []workbatch.Member{
		hinted("t3", cpt("10:00"), "A-03", 3),
		hinted("t1", cpt("10:00"), "A-01", 1),
		hinted("t2", cpt("10:00"), "A-02", 2),
	})
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	for i, m := range b.Members() {
		if m.StopSeq != i+1 || m.Compartment != i+1 {
			t.Fatalf("member %d: stop_seq=%d compartment=%d, want %d/%d", i, m.StopSeq, m.Compartment, i+1, i+1)
		}
	}
	if got := b.Members()[0].TaskId; got != "t1" {
		t.Fatalf("first stop = %s, want t1 (rank order)", got)
	}
}

// TestForm_OpenWithVersionOne: a formed batch is OPEN, carries the band's CPT
// and the station, and starts at version 1.
func TestForm_OpenWithVersionOne(t *testing.T) {
	b := formed(t)
	if b.Status() != workbatch.StatusOpen || b.StationId() != "stn-1" {
		t.Fatalf("status = %s station = %s", b.Status(), b.StationId())
	}
	if !b.CPT().Equal(cpt("10:00")) {
		t.Fatalf("cpt = %v", b.CPT())
	}
	if b.Version() != 1 {
		t.Fatalf("version = %d, want 1", b.Version())
	}
}

// --- CompleteMember: [OPEN, COMPLETED, CompleteMember] -----------------------

// TestCompleteMember_LastCompletionCompletesBatch: completing the last open
// member completes the batch, records walk_seconds from formation and bumps
// the version.
func TestCompleteMember_LastCompletionCompletesBatch(t *testing.T) {
	b := formed(t)
	at := cpt("10:12")
	if err := b.CompleteMember("t1", at); err != nil {
		t.Fatalf("complete t1: %v", err)
	}
	if b.Status() != workbatch.StatusOpen {
		t.Fatalf("status = %s, want OPEN with one member left", b.Status())
	}
	if err := b.CompleteMember("t2", cpt("10:20")); err != nil {
		t.Fatalf("complete t2: %v", err)
	}
	if b.Status() != workbatch.StatusCompleted {
		t.Fatalf("status = %s, want COMPLETED", b.Status())
	}
	if got := b.WalkSeconds(); got != int64(time.Duration(20*time.Minute).Seconds()) {
		t.Fatalf("walk_seconds = %d, want 1200", got)
	}
	if b.Version() != 3 {
		t.Fatalf("version = %d, want 3", b.Version())
	}
}

// TestCompleteMember_UnknownTask: a task that is not a member is
// ErrMemberNotInBatch.
func TestCompleteMember_UnknownTask(t *testing.T) {
	b := formed(t)
	if err := b.CompleteMember("t9", cpt("10:05")); !errors.Is(err, workbatch.ErrMemberNotInBatch) {
		t.Fatalf("err = %v", err)
	}
}

// TestCompleteMember_AlreadyCompleted: completing the same member twice is
// ErrMemberAlreadyCompleted — the aggregate never double-counts a completion.
func TestCompleteMember_AlreadyCompleted(t *testing.T) {
	b := formed(t)
	if err := b.CompleteMember("t1", cpt("10:05")); err != nil {
		t.Fatalf("first completion: %v", err)
	}
	if err := b.CompleteMember("t1", cpt("10:09")); !errors.Is(err, workbatch.ErrMemberAlreadyCompleted) {
		t.Fatalf("err = %v", err)
	}
}

// TestCompleteMember_OnClosedBatch: a completed batch accepts no further
// member completion (ErrBatchNotOpen).
func TestCompleteMember_OnClosedBatch(t *testing.T) {
	b, err := workbatch.Form("b1", "stn-1", 4, cpt("10:00"), []workbatch.Member{member("t1", cpt("10:00"), "A-01")})
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	if err := b.CompleteMember("t1", cpt("10:05")); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := b.CompleteMember("t1", cpt("10:06")); !errors.Is(err, workbatch.ErrBatchNotOpen) {
		t.Fatalf("err = %v", err)
	}
}

// TestCompleteMember_NextOpenMember: the next member to work is the lowest
// stop_seq still open — completed and detached members are skipped.
func TestCompleteMember_NextOpenMember(t *testing.T) {
	b, err := workbatch.Form("b1", "stn-1", 4, cpt("10:00"), []workbatch.Member{
		hinted("t1", cpt("10:00"), "A-01", 1),
		hinted("t2", cpt("10:00"), "A-02", 2),
		hinted("t3", cpt("10:00"), "A-03", 3),
	})
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	if err := b.CompleteMember("t1", cpt("10:04")); err != nil {
		t.Fatalf("complete t1: %v", err)
	}
	if err := b.DetachMember("t2", cpt("10:05")); err != nil {
		t.Fatalf("detach t2: %v", err)
	}
	next := b.NextOpenMember()
	if next == nil || next.TaskId != "t3" {
		t.Fatalf("next open member = %+v, want t3", next)
	}
}

// --- DetachMember: [OPEN, ABANDONED, DetachMember] ---------------------------

// TestDetachMember_AllDetachedAbandonsBatch: when the last open member leaves
// (its lease expired and it returned to the pool), the batch is ABANDONED.
func TestDetachMember_AllDetachedAbandonsBatch(t *testing.T) {
	b := formed(t)
	if err := b.DetachMember("t1", cpt("10:06")); err != nil {
		t.Fatalf("detach t1: %v", err)
	}
	if b.Status() != workbatch.StatusOpen {
		t.Fatalf("status = %s, want OPEN with one member left", b.Status())
	}
	if err := b.DetachMember("t2", cpt("10:07")); err != nil {
		t.Fatalf("detach t2: %v", err)
	}
	if b.Status() != workbatch.StatusAbandoned {
		t.Fatalf("status = %s, want ABANDONED", b.Status())
	}
}

// TestDetachMember_Idempotent: detaching an already-detached member again is
// a no-op (the sweep may re-fire), never an error and never a version bump.
func TestDetachMember_Idempotent(t *testing.T) {
	b := formed(t)
	if err := b.DetachMember("t1", cpt("10:06")); err != nil {
		t.Fatalf("detach t1: %v", err)
	}
	before := b.Version()
	if err := b.DetachMember("t1", cpt("10:07")); err != nil {
		t.Fatalf("re-detach t1: %v", err)
	}
	if b.Version() != before {
		t.Fatalf("version moved %d -> %d on an idempotent detach", before, b.Version())
	}
}

// TestDetachMember_OnClosedBatch: a completed batch accepts no detach
// (ErrBatchNotOpen).
func TestDetachMember_OnClosedBatch(t *testing.T) {
	b := formed(t)
	if err := b.CompleteMember("t1", cpt("10:05")); err != nil || b.CompleteMember("t2", cpt("10:08")) != nil {
		t.Fatalf("completions failed: %v", err)
	}
	if err := b.DetachMember("t1", cpt("10:09")); !errors.Is(err, workbatch.ErrBatchNotOpen) {
		t.Fatalf("err = %v", err)
	}
}

// TestDetachMember_UnknownTask: detaching a task that is not a member is
// ErrMemberNotInBatch.
func TestDetachMember_UnknownTask(t *testing.T) {
	b := formed(t)
	if err := b.DetachMember("t9", cpt("10:05")); !errors.Is(err, workbatch.ErrMemberNotInBatch) {
		t.Fatalf("err = %v", err)
	}
}

// TestDetachMember_DetachedMemberCannotComplete: a member that left the batch
// (lease expired, back in the pool) cannot complete INTO the batch — its
// completion, if the floor client still sends one, is ErrMemberDetached.
func TestDetachMember_DetachedMemberCannotComplete(t *testing.T) {
	b := formed(t)
	if err := b.DetachMember("t1", cpt("10:06")); err != nil {
		t.Fatalf("detach t1: %v", err)
	}
	if err := b.CompleteMember("t1", cpt("10:07")); !errors.Is(err, workbatch.ErrMemberDetached) {
		t.Fatalf("err = %v", err)
	}
}

// TestOpenMembers_ListsOnlyOpen: the open-member view used by lease renewal
// and the MCP read tool lists exactly the still-open members, in stop order.
func TestOpenMembers_ListsOnlyOpen(t *testing.T) {
	b := formed(t)
	if err := b.CompleteMember("t1", cpt("10:04")); err != nil {
		t.Fatalf("complete t1: %v", err)
	}
	open := b.OpenMembers()
	if len(open) != 1 || open[0].TaskId != "t2" {
		t.Fatalf("open members = %+v, want [t2]", open)
	}
}
