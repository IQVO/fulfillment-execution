package task_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

var now = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func newPickTask() *task.Task {
	return task.New(
		shared.TaskId("t1"),
		task.Pick,
		shared.NewCPT(now.Add(time.Hour)),
		shared.OrderRef("order-1"),
		shared.NewCapabilitySet("pick"),
		false,
		false,
	)
}

func TestNew_StartsPending(t *testing.T) {
	tk := newPickTask()
	if tk.Status() != task.Pending {
		t.Fatalf("expected Pending, got %s", tk.Status())
	}
	if tk.Lease() != nil {
		t.Fatalf("expected no lease on a new task")
	}
}

func TestClaim_Success(t *testing.T) {
	tk := newPickTask()
	err := tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now, time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tk.Status() != task.Claimed {
		t.Fatalf("expected Claimed, got %s", tk.Status())
	}
	if tk.Lease() == nil || tk.Lease().StationId != shared.StationId("s1") {
		t.Fatalf("expected active lease owned by s1, got %+v", tk.Lease())
	}
}

// Invariant: at most one active claim (at-most-once).
func TestClaim_RejectsSecondClaimWhileLeaseActive(t *testing.T) {
	tk := newPickTask()
	if err := tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now, time.Minute); err != nil {
		t.Fatalf("first claim should succeed: %v", err)
	}
	err := tk.Claim(shared.StationId("s2"), shared.NewCapabilitySet("pick"), now.Add(time.Second), time.Minute)
	if !errors.Is(err, task.ErrAlreadyClaimed) {
		t.Fatalf("expected ErrAlreadyClaimed, got %v", err)
	}
	if tk.Lease().StationId != shared.StationId("s1") {
		t.Fatalf("original claim must remain intact, got owner %s", tk.Lease().StationId)
	}
}

// Invariant: a claim requires matching capabilities.
func TestClaim_RejectsCapabilityMismatch(t *testing.T) {
	tk := newPickTask()
	err := tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pack"), now, time.Minute)
	if !errors.Is(err, task.ErrCapabilityMismatch) {
		t.Fatalf("expected ErrCapabilityMismatch, got %v", err)
	}
	if tk.Status() != task.Pending {
		t.Fatalf("rejected claim must leave task Pending, got %s", tk.Status())
	}
}

// Invariant: an expired lease frees the task, allowing a new claim.
func TestClaim_ExpiredLeaseFreesTaskForNewClaim(t *testing.T) {
	tk := newPickTask()
	if err := tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now, time.Minute); err != nil {
		t.Fatalf("first claim should succeed: %v", err)
	}
	afterExpiry := now.Add(2 * time.Minute)
	err := tk.Claim(shared.StationId("s2"), shared.NewCapabilitySet("pick"), afterExpiry, time.Minute)
	if err != nil {
		t.Fatalf("claim after lease expiry should succeed, got %v", err)
	}
	if tk.Lease().StationId != shared.StationId("s2") {
		t.Fatalf("expected s2 to hold the new claim, got %s", tk.Lease().StationId)
	}
}

func TestExpireLeaseIfDue_FreesClaimedTask(t *testing.T) {
	tk := newPickTask()
	_ = tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now, time.Minute)
	freed := tk.ExpireLeaseIfDue(now.Add(2 * time.Minute))
	if !freed {
		t.Fatalf("expected lease to be expired and task freed")
	}
	if tk.Status() != task.Pending {
		t.Fatalf("expected Pending after lease expiry, got %s", tk.Status())
	}
}

func TestExpireLeaseIfDue_NoOpWhenLeaseStillActive(t *testing.T) {
	tk := newPickTask()
	_ = tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now, time.Minute)
	freed := tk.ExpireLeaseIfDue(now.Add(30 * time.Second))
	if freed {
		t.Fatalf("lease should still be active, should not free the task")
	}
	if tk.Status() != task.Claimed {
		t.Fatalf("expected Claimed, got %s", tk.Status())
	}
}

func TestRenewLease_ExtendsExpiry(t *testing.T) {
	tk := newPickTask()
	_ = tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now, time.Minute)
	original := tk.Lease().Expiry
	if err := tk.RenewLease(shared.StationId("s1"), now.Add(30*time.Second), time.Minute); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !tk.Lease().Expiry.After(original) {
		t.Fatalf("expected lease expiry to be extended")
	}
}

func TestRenewLease_RejectsNonOwner(t *testing.T) {
	tk := newPickTask()
	_ = tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now, time.Minute)
	err := tk.RenewLease(shared.StationId("s2"), now.Add(30*time.Second), time.Minute)
	if !errors.Is(err, task.ErrNotOwner) {
		t.Fatalf("expected ErrNotOwner, got %v", err)
	}
}

func TestRenewLease_RejectsWhenNotClaimed(t *testing.T) {
	tk := newPickTask()
	err := tk.RenewLease(shared.StationId("s1"), now, time.Minute)
	if !errors.Is(err, task.ErrNotClaimed) {
		t.Fatalf("expected ErrNotClaimed, got %v", err)
	}
}

func TestComplete_Success(t *testing.T) {
	tk := newPickTask()
	_ = tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now, time.Minute)
	if err := tk.Complete(shared.StationId("s1"), now.Add(10*time.Second)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tk.Status() != task.Completed {
		t.Fatalf("expected Completed, got %s", tk.Status())
	}
}

// Invariant: no double-complete.
func TestComplete_RejectsDoubleComplete(t *testing.T) {
	tk := newPickTask()
	_ = tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now, time.Minute)
	_ = tk.Complete(shared.StationId("s1"), now.Add(10*time.Second))
	err := tk.Complete(shared.StationId("s1"), now.Add(20*time.Second))
	if !errors.Is(err, task.ErrAlreadyCompleted) {
		t.Fatalf("expected ErrAlreadyCompleted, got %v", err)
	}
}

func TestComplete_RejectsNonOwner(t *testing.T) {
	tk := newPickTask()
	_ = tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now, time.Minute)
	err := tk.Complete(shared.StationId("s2"), now.Add(10*time.Second))
	if !errors.Is(err, task.ErrNotOwner) {
		t.Fatalf("expected ErrNotOwner, got %v", err)
	}
}

func TestComplete_RejectsWhenLeaseExpired(t *testing.T) {
	tk := newPickTask()
	_ = tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now, time.Minute)
	err := tk.Complete(shared.StationId("s1"), now.Add(2*time.Minute))
	if !errors.Is(err, task.ErrNotClaimed) {
		t.Fatalf("expected ErrNotClaimed after lease expiry, got %v", err)
	}
	if tk.Status() != task.Pending {
		t.Fatalf("expected task freed back to Pending, got %s", tk.Status())
	}
}

// A Pending task has no active claim to complete from.
func TestComplete_RejectsWhenNotClaimed(t *testing.T) {
	tk := newPickTask()
	err := tk.Complete(shared.StationId("s1"), now.Add(10*time.Second))
	if !errors.Is(err, task.ErrNotClaimed) {
		t.Fatalf("expected ErrNotClaimed on a never-claimed task, got %v", err)
	}
	if tk.Status() != task.Pending {
		t.Fatalf("expected task to remain Pending, got %s", tk.Status())
	}
	if tk.Lease() != nil {
		t.Fatalf("expected no lease to appear on a rejected completion, got %+v", tk.Lease())
	}
}

// A rehydrated Claimed task with no lease (e.g. a row persisted by an
// older writer before leases existed) must not be completable: the claim
// itself is unverifiable, so it is treated as not claimed.
func TestComplete_RejectsClaimedWithoutLease(t *testing.T) {
	tk := task.Rehydrate(
		shared.TaskId("t1"), task.Pick, task.Claimed,
		shared.NewCPT(now.Add(time.Hour)), shared.OrderRef("order-1"),
		shared.NewCapabilitySet("pick"), nil, false, false, &now,
	)
	err := tk.Complete(shared.StationId("s1"), now.Add(10*time.Second))
	if !errors.Is(err, task.ErrNotClaimed) {
		t.Fatalf("expected ErrNotClaimed when the claim has no lease, got %v", err)
	}
	if tk.Status() != task.Claimed {
		t.Fatalf("expected status untouched (no lease to expire), got %s", tk.Status())
	}
}

func TestRenewLease_RejectsAfterLeaseExpiredAndFreesTask(t *testing.T) {
	tk := newPickTask()
	_ = tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now, time.Minute)
	err := tk.RenewLease(shared.StationId("s1"), now.Add(2*time.Minute), time.Minute)
	if !errors.Is(err, task.ErrNotClaimed) {
		t.Fatalf("expected ErrNotClaimed after lease expiry, got %v", err)
	}
	if tk.Status() != task.Pending {
		t.Fatalf("expected task freed back to Pending, got %s", tk.Status())
	}
}

func TestRenewLease_RejectsOnCompletedTask(t *testing.T) {
	tk := newPickTask()
	_ = tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now, time.Minute)
	_ = tk.Complete(shared.StationId("s1"), now.Add(10*time.Second))
	err := tk.RenewLease(shared.StationId("s1"), now.Add(20*time.Second), time.Minute)
	if !errors.Is(err, task.ErrAlreadyCompleted) {
		t.Fatalf("expected ErrAlreadyCompleted, got %v", err)
	}
}

func TestClaim_RejectsOnCompletedTask(t *testing.T) {
	tk := newPickTask()
	_ = tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now, time.Minute)
	_ = tk.Complete(shared.StationId("s1"), now.Add(10*time.Second))
	err := tk.Claim(shared.StationId("s2"), shared.NewCapabilitySet("pick"), now.Add(20*time.Second), time.Minute)
	if !errors.Is(err, task.ErrAlreadyCompleted) {
		t.Fatalf("expected ErrAlreadyCompleted, got %v", err)
	}
}

func TestNew_Getters(t *testing.T) {
	tk := newPickTask()
	if tk.Id() != shared.TaskId("t1") {
		t.Fatalf("expected Id t1, got %s", tk.Id())
	}
	if tk.Type() != task.Pick {
		t.Fatalf("expected Type Pick, got %s", tk.Type())
	}
	if !tk.CPT().Time().Equal(now.Add(time.Hour)) {
		t.Fatalf("expected CPT %v, got %v", now.Add(time.Hour), tk.CPT().Time())
	}
	if tk.OrderRef() != shared.OrderRef("order-1") {
		t.Fatalf("expected OrderRef order-1, got %s", tk.OrderRef())
	}
	if !tk.RequiredCapabilities().Contains("pick") {
		t.Fatalf("expected required capabilities to contain pick")
	}
}

func TestRehydrate_ReconstructsPersistedState(t *testing.T) {
	lease := &task.Lease{StationId: shared.StationId("s1"), Expiry: now.Add(time.Minute)}
	tk := task.Rehydrate(shared.TaskId("t2"), task.Pack, task.Claimed, shared.NewCPT(now), shared.OrderRef("order-2"), shared.NewCapabilitySet("pack"), lease, false, false, nil)
	if tk.Id() != shared.TaskId("t2") {
		t.Fatalf("expected Id t2, got %s", tk.Id())
	}
	if tk.Status() != task.Claimed {
		t.Fatalf("expected Claimed, got %s", tk.Status())
	}
	if tk.Lease() == nil || tk.Lease().StationId != shared.StationId("s1") {
		t.Fatalf("expected rehydrated lease owned by s1, got %+v", tk.Lease())
	}
}

func TestIsAvailable(t *testing.T) {
	tk := newPickTask()
	if !tk.IsAvailable(now) {
		t.Fatalf("pending task should be available")
	}
	_ = tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now, time.Minute)
	if tk.IsAvailable(now.Add(30 * time.Second)) {
		t.Fatalf("actively claimed task should not be available")
	}
	if !tk.IsAvailable(now.Add(2 * time.Minute)) {
		t.Fatalf("task with expired lease should be available")
	}
}

// Fragile is a packing hint stamped by wes-work-planning at task-creation
// time; it must round-trip through both New and Rehydrate and must not
// affect claim/capability behavior.
func TestNew_FragileFlag_RoundTrips(t *testing.T) {
	tk := task.New(shared.TaskId("t1"), task.Pack, shared.NewCPT(now.Add(time.Hour)), shared.OrderRef("order-1"), shared.NewCapabilitySet("pack"), true, false)
	if !tk.Fragile() {
		t.Fatalf("expected Fragile() to be true")
	}
}

func TestNew_NotFragileByDefault(t *testing.T) {
	tk := newPickTask()
	if tk.Fragile() {
		t.Fatalf("expected Fragile() to be false when not requested")
	}
}

func TestRehydrate_FragileFlag_RoundTrips(t *testing.T) {
	tk := task.Rehydrate(shared.TaskId("t2"), task.Pack, task.Pending, shared.NewCPT(now), shared.OrderRef("order-2"), shared.NewCapabilitySet("pack"), nil, true, false, nil)
	if !tk.Fragile() {
		t.Fatalf("expected rehydrated Fragile() to be true")
	}
}

// Fragile must not influence Claim's capability-matching outcome.
func TestClaim_FragileTaskStillMatchesOnCapabilitiesOnly(t *testing.T) {
	tk := task.New(shared.TaskId("t1"), task.Pack, shared.NewCPT(now.Add(time.Hour)), shared.OrderRef("order-1"), shared.NewCapabilitySet("pack"), true, false)
	if err := tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pack"), now, time.Minute); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tk.Status() != task.Claimed {
		t.Fatalf("expected Claimed, got %s", tk.Status())
	}
}

// GiftWrap is a packing hint stamped by wes-work-planning at task-creation
// time from a caller-stated WorkReleased.data.gift_wrap request; it must
// round-trip through both New and Rehydrate and must not affect
// claim/capability behavior (see ADR-0011).
func TestNew_GiftWrapFlag_RoundTrips(t *testing.T) {
	tk := task.New(shared.TaskId("t1"), task.Pack, shared.NewCPT(now.Add(time.Hour)), shared.OrderRef("order-1"), shared.NewCapabilitySet("pack"), false, true)
	if !tk.GiftWrap() {
		t.Fatalf("expected GiftWrap() to be true")
	}
}

func TestNew_NotGiftWrapByDefault(t *testing.T) {
	tk := newPickTask()
	if tk.GiftWrap() {
		t.Fatalf("expected GiftWrap() to be false when not requested")
	}
}

func TestRehydrate_GiftWrapFlag_RoundTrips(t *testing.T) {
	tk := task.Rehydrate(shared.TaskId("t2"), task.Pack, task.Pending, shared.NewCPT(now), shared.OrderRef("order-2"), shared.NewCapabilitySet("pack"), nil, false, true, nil)
	if !tk.GiftWrap() {
		t.Fatalf("expected rehydrated GiftWrap() to be true")
	}
}

// GiftWrap must not influence Claim's capability-matching outcome —
// deliberately unlike hazmat, no station-eligibility gating exists for it.
func TestClaim_GiftWrapTaskStillMatchesOnCapabilitiesOnly(t *testing.T) {
	tk := task.New(shared.TaskId("t1"), task.Pack, shared.NewCPT(now.Add(time.Hour)), shared.OrderRef("order-1"), shared.NewCapabilitySet("pack"), false, true)
	if err := tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pack"), now, time.Minute); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tk.Status() != task.Claimed {
		t.Fatalf("expected Claimed, got %s", tk.Status())
	}
}

// ClaimedAt is nil until the task is claimed, set by Claim to the claim's
// start time, and — unlike the lease — is NOT cleared by Complete, so a
// completion-time consumer (see the Kafka publisher's duration enrichment)
// can still compute elapsed time.
func TestClaimedAt_NilUntilClaimed(t *testing.T) {
	tk := newPickTask()
	if tk.ClaimedAt() != nil {
		t.Fatalf("expected ClaimedAt() nil before any claim, got %v", tk.ClaimedAt())
	}
}

func TestClaim_SetsClaimedAt(t *testing.T) {
	tk := newPickTask()
	if err := tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now, time.Minute); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tk.ClaimedAt() == nil || !tk.ClaimedAt().Equal(now) {
		t.Fatalf("expected ClaimedAt() == %v, got %v", now, tk.ClaimedAt())
	}
}

func TestComplete_DoesNotClearClaimedAt(t *testing.T) {
	tk := newPickTask()
	_ = tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now, time.Minute)
	if err := tk.Complete(shared.StationId("s1"), now.Add(10*time.Second)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tk.ClaimedAt() == nil || !tk.ClaimedAt().Equal(now) {
		t.Fatalf("expected ClaimedAt() to remain %v after Complete, got %v", now, tk.ClaimedAt())
	}
}

func TestRehydrate_ClaimedAtRoundTrips(t *testing.T) {
	lease := &task.Lease{StationId: shared.StationId("s1"), Expiry: now.Add(time.Minute)}
	claimedAt := now
	tk := task.Rehydrate(shared.TaskId("t2"), task.Pack, task.Claimed, shared.NewCPT(now), shared.OrderRef("order-2"), shared.NewCapabilitySet("pack"), lease, false, false, &claimedAt)
	if tk.ClaimedAt() == nil || !tk.ClaimedAt().Equal(claimedAt) {
		t.Fatalf("expected rehydrated ClaimedAt() == %v, got %v", claimedAt, tk.ClaimedAt())
	}
}

func TestRehydrate_ClaimedAtNilForPreMigrationRows(t *testing.T) {
	tk := task.Rehydrate(shared.TaskId("t2"), task.Pack, task.Pending, shared.NewCPT(now), shared.OrderRef("order-2"), shared.NewCapabilitySet("pack"), nil, false, false, nil)
	if tk.ClaimedAt() != nil {
		t.Fatalf("expected nil ClaimedAt() for a pre-migration row, got %v", tk.ClaimedAt())
	}
}

// REBIN is a fourth process-path task type (see the consolidation package
// and its ADR) and deliberately gets NO special-cased behavior anywhere in
// Task's claim/complete lifecycle — a REBIN task is claimed, leased, and
// completed by the exact same rules as PICK/PACK/SLAM. This test proves
// that at the Task level. It intentionally does not exercise order-level
// fan-in/consolidation at all, because Task has no opinion about that —
// see the consolidation package for that concern.
func TestClaim_RebinTaskFollowsIdenticalRulesToOtherTypes(t *testing.T) {
	tk := task.New(
		shared.TaskId("t-rebin"),
		task.Rebin,
		shared.NewCPT(now.Add(time.Hour)),
		shared.OrderRef("order-1"),
		shared.NewCapabilitySet("rebin"),
		false,
		false,
	)
	if err := tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("rebin"), now, time.Minute); err != nil {
		t.Fatalf("unexpected error claiming a REBIN task: %v", err)
	}
	if tk.Status() != task.Claimed || tk.Type() != task.Rebin {
		t.Fatalf("expected Claimed REBIN task, got status=%s type=%s", tk.Status(), tk.Type())
	}
	if err := tk.Complete(shared.StationId("s1"), now.Add(30*time.Second)); err != nil {
		t.Fatalf("unexpected error completing a REBIN task: %v", err)
	}
	if tk.Status() != task.Completed {
		t.Fatalf("expected Completed, got %s", tk.Status())
	}
}

// IsCPTMissed is the domain-logic core of the CPT-missed sweep (see
// usecases.SweepCPTMisses / ADR-0025). Boundary is explicit and pinned by
// this test: a task due EXACTLY at now counts as missed (mirrors
// Lease.expired's own not-before boundary), so a sweep tick landing
// precisely on the CPT catches it immediately rather than waiting a tick.
func TestIsCPTMissed_PendingTaskPastCPT(t *testing.T) {
	tk := task.New(shared.TaskId("t1"), task.Pick, shared.NewCPT(now), shared.OrderRef("order-1"), shared.NewCapabilitySet("pick"), false, false)
	if !tk.IsCPTMissed(now.Add(time.Second)) {
		t.Fatalf("expected a Pending task past its CPT to be missed")
	}
}

// The boundary itself: due EXACTLY at now is missed, not merely "about to
// be missed". This is the precise off-by-one a mutant on the comparison
// operator would flip.
func TestIsCPTMissed_ExactlyAtCPT_CountsAsMissed(t *testing.T) {
	tk := task.New(shared.TaskId("t1"), task.Pick, shared.NewCPT(now), shared.OrderRef("order-1"), shared.NewCapabilitySet("pick"), false, false)
	if !tk.IsCPTMissed(now) {
		t.Fatalf("expected a task due exactly at now to count as CPT-missed")
	}
}

// One instant before CPT: not yet missed.
func TestIsCPTMissed_BeforeCPT_NotMissed(t *testing.T) {
	tk := task.New(shared.TaskId("t1"), task.Pick, shared.NewCPT(now), shared.OrderRef("order-1"), shared.NewCapabilitySet("pick"), false, false)
	if tk.IsCPTMissed(now.Add(-time.Nanosecond)) {
		t.Fatalf("expected a task one nanosecond before its CPT to not be missed yet")
	}
}

// A Claimed (still-open, unconfirmed) task past its CPT is also missed —
// IsCPTMissed treats Pending and Claimed identically as "still open".
func TestIsCPTMissed_ClaimedTaskPastCPT(t *testing.T) {
	tk := task.New(shared.TaskId("t1"), task.Pick, shared.NewCPT(now), shared.OrderRef("order-1"), shared.NewCapabilitySet("pick"), false, false)
	if err := tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now.Add(-time.Hour), time.Hour); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !tk.IsCPTMissed(now) {
		t.Fatalf("expected a Claimed task past its CPT to be missed")
	}
}

// A Completed task is NEVER reported as CPT-missed, no matter how far past
// its CPT "now" is — completion, not the deadline alone, is what ends the
// sweep's interest in a task (mirrors ExpireLeaseIfDue's Completed guard).
func TestIsCPTMissed_CompletedTaskNeverMissed(t *testing.T) {
	tk := task.New(shared.TaskId("t1"), task.Pick, shared.NewCPT(now), shared.OrderRef("order-1"), shared.NewCapabilitySet("pick"), false, false)
	if err := tk.Claim(shared.StationId("s1"), shared.NewCapabilitySet("pick"), now.Add(-time.Hour), 2*time.Hour); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := tk.Complete(shared.StationId("s1"), now); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if tk.IsCPTMissed(now.Add(24 * time.Hour)) {
		t.Fatalf("expected a Completed task to never be reported as CPT-missed")
	}
}
