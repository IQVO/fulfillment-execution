package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

var epoch = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func idSeq(prefix string) func() shared.TaskId {
	n := 0
	return func() shared.TaskId {
		n++
		return shared.TaskId(prefix + string(rune('0'+n)))
	}
}

type harness struct {
	tasks     *memory.TaskRepo
	stations  *memory.StationRepo
	packages  *memory.PackageRepo
	publisher *events.BufferedPublisher
	clock     *memory.FixedClock
}

func newHarness() *harness {
	return &harness{
		tasks:     memory.NewTaskRepo(),
		stations:  memory.NewStationRepo(),
		packages:  memory.NewPackageRepo(),
		publisher: events.NewBufferedPublisher(),
		clock:     memory.NewFixedClock(epoch),
	}
}

func TestCreateTask_PutsTaskInPool(t *testing.T) {
	h := newHarness()
	uc := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}

	tk, err := uc.Execute(context.Background(), task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tk.Status() != task.Pending {
		t.Fatalf("expected Pending, got %s", tk.Status())
	}

	got, _ := h.tasks.FindById(context.Background(), tk.Id())
	if got == nil {
		t.Fatalf("expected task to be persisted")
	}
	if len(h.publisher.Events()) != 1 || h.publisher.Events()[0].EventName() != "TaskCreated" {
		t.Fatalf("expected a single TaskCreated event, got %+v", h.publisher.Events())
	}
}

// CreateTask threads the fragile flag straight through to task.New — it is
// a packing hint sourced from wes-work-planning, not derived here.
func TestCreateTask_ThreadsFragileFlagThrough(t *testing.T) {
	h := newHarness()
	uc := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}

	tk, err := uc.Execute(context.Background(), task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), true, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !tk.Fragile() {
		t.Fatalf("expected the created task to carry Fragile() == true")
	}

	got, _ := h.tasks.FindById(context.Background(), tk.Id())
	if got == nil || !got.Fragile() {
		t.Fatalf("expected the persisted task to carry Fragile() == true, got %+v", got)
	}
}

func TestCreateTask_FragileDefaultsFalse(t *testing.T) {
	h := newHarness()
	uc := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}

	tk, err := uc.Execute(context.Background(), task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tk.Fragile() {
		t.Fatalf("expected Fragile() == false when not requested")
	}
}

// CreateTask threads the gift-wrap flag straight through to task.New — it
// is a caller-stated request sourced from wes-work-planning, not derived
// here (see ADR-0011).
func TestCreateTask_ThreadsGiftWrapFlagThrough(t *testing.T) {
	h := newHarness()
	uc := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}

	tk, err := uc.Execute(context.Background(), task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !tk.GiftWrap() {
		t.Fatalf("expected the created task to carry GiftWrap() == true")
	}

	got, _ := h.tasks.FindById(context.Background(), tk.Id())
	if got == nil || !got.GiftWrap() {
		t.Fatalf("expected the persisted task to carry GiftWrap() == true, got %+v", got)
	}
}

func TestCreateTask_GiftWrapDefaultsFalse(t *testing.T) {
	h := newHarness()
	uc := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}

	tk, err := uc.Execute(context.Background(), task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tk.GiftWrap() {
		t.Fatalf("expected GiftWrap() == false when not requested")
	}
}

func TestClaimNext_SelectsEarliestCPTMatchingCapabilities(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}

	// A later-CPT task that matches, and an earlier-CPT task requiring a
	// capability the station lacks — ClaimNext must skip it and take the
	// best-fit match, not merely the earliest CPT overall.
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(30*time.Minute)), "order-hazmat", shared.NewCapabilitySet("pick", "hazmat"), false, false)
	wantTask, _ := create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)

	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	got, err := claim.Execute(ctx, "s1", task.Pick)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Id() != wantTask.Id() {
		t.Fatalf("expected task %s (capability-matching), got %s", wantTask.Id(), got.Id())
	}
	if got.Status() != task.Claimed {
		t.Fatalf("expected Claimed, got %s", got.Status())
	}
}

func TestClaimNext_ReturnsErrNoClaimableTaskWhenPoolEmpty(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	_, err := claim.Execute(ctx, "s1", task.Pick)
	if !errors.Is(err, usecases.ErrNoClaimableTask) {
		t.Fatalf("expected ErrNoClaimableTask, got %v", err)
	}
}

// Invariant (through the use case): at-most-once claim — a second station
// cannot claim a task another station holds an active lease on.
func TestClaimNext_AtMostOnce_SecondStationCannotClaimSameTask(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)

	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	_ = h.stations.Save(ctx, station.New("s2", shared.NewCapabilitySet("pick")))

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	first, err := claim.Execute(ctx, "s1", task.Pick)
	if err != nil {
		t.Fatalf("first claim should succeed: %v", err)
	}

	_, err = claim.Execute(ctx, "s2", task.Pick)
	if !errors.Is(err, usecases.ErrNoClaimableTask) {
		t.Fatalf("expected ErrNoClaimableTask (task already leased), got %v", err)
	}
	if first.Lease().StationId != shared.StationId("s1") {
		t.Fatalf("lease should remain owned by s1")
	}
}

// Lease-expiry via a FixedClock: after the lease window passes, the pool
// makes the task claimable again.
func TestClaimNext_ExpiredLeaseReturnsTaskToPool(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)

	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	_ = h.stations.Save(ctx, station.New("s2", shared.NewCapabilitySet("pick")))

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock, LeaseDuration: time.Minute}
	_, err := claim.Execute(ctx, "s1", task.Pick)
	if err != nil {
		t.Fatalf("first claim should succeed: %v", err)
	}

	h.clock.Advance(2 * time.Minute)

	got, err := claim.Execute(ctx, "s2", task.Pick)
	if err != nil {
		t.Fatalf("claim after lease expiry should succeed: %v", err)
	}
	if got.Lease().StationId != shared.StationId("s2") {
		t.Fatalf("expected s2 to hold the new claim, got %s", got.Lease().StationId)
	}
}

func TestExpireLeases_SweepsExpiredClaimsBackToPending(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	tk, _ := create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock, LeaseDuration: time.Minute}
	_, _ = claim.Execute(ctx, "s1", task.Pick)

	h.clock.Advance(2 * time.Minute)

	sweep := &usecases.ExpireLeases{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock}
	freed, err := sweep.Execute(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if freed != 1 {
		t.Fatalf("expected 1 freed task, got %d", freed)
	}

	got, _ := h.tasks.FindById(ctx, tk.Id())
	if got.Status() != task.Pending {
		t.Fatalf("expected Pending after sweep, got %s", got.Status())
	}
}

// --- SweepCPTMisses ---

// A still-Pending task whose CPT has passed raises TaskCPTMissed, carrying
// the OrderRef/TaskType/CPT order-management's RepromiseOrder (ADR 0014 §5)
// needs, and never mutates the task's own lifecycle state (unlike
// ExpireLeases, a CPT miss is a fact reported upstream, not a state
// transition here).
func TestSweepCPTMisses_ReportsOverdueOpenTasks(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	tk, _ := create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Minute)), "order-1", shared.NewCapabilitySet("pick"), false, false)

	h.clock.Advance(2 * time.Minute)

	sweep := &usecases.SweepCPTMisses{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock}
	reported, err := sweep.Execute(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reported != 1 {
		t.Fatalf("expected 1 task reported, got %d", reported)
	}

	got, _ := h.tasks.FindById(ctx, tk.Id())
	if got.Status() != task.Pending {
		t.Fatalf("SweepCPTMisses must not mutate task state, expected Pending, got %s", got.Status())
	}

	var found *shared.TaskCPTMissed
	for _, e := range h.publisher.Events() {
		if m, ok := e.(shared.TaskCPTMissed); ok {
			found = &m
		}
	}
	if found == nil {
		t.Fatalf("expected a TaskCPTMissed event, got %v", h.publisher.Events())
	}
	if found.TaskId != tk.Id() {
		t.Fatalf("expected TaskId %s, got %s", tk.Id(), found.TaskId)
	}
	if found.OrderRef != "order-1" {
		t.Fatalf("expected OrderRef order-1, got %s", found.OrderRef)
	}
	if found.TaskType != string(task.Pick) {
		t.Fatalf("expected TaskType PICK, got %s", found.TaskType)
	}
	if !found.CPT.Equal(epoch.Add(time.Minute)) {
		t.Fatalf("expected CPT %v, got %v", epoch.Add(time.Minute), found.CPT)
	}
}

// A task not yet past its CPT is not reported.
func TestSweepCPTMisses_IgnoresTasksNotYetDue(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)

	sweep := &usecases.SweepCPTMisses{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock}
	reported, err := sweep.Execute(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reported != 0 {
		t.Fatalf("expected 0 tasks reported, got %d", reported)
	}
}

// A Completed task is never reported, regardless of how far past its CPT.
func TestSweepCPTMisses_IgnoresCompletedTasks(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	tk, _ := create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Minute)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pick)
	complete := &usecases.CompleteTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock}
	if err := complete.Execute(ctx, claimed.Id(), "s1"); err != nil {
		t.Fatalf("complete: %v", err)
	}

	h.clock.Advance(2 * time.Hour)
	sweep := &usecases.SweepCPTMisses{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock}
	reported, err := sweep.Execute(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reported != 0 {
		t.Fatalf("expected 0 tasks reported for a Completed task, got %d", reported)
	}
	_ = tk
}

// Re-fire semantics (ADR-0025, citing order-management ADR 0014 §5's
// idempotency note): the same still-overdue task is reported on every
// sweep tick, not just once.
func TestSweepCPTMisses_RefiresOnEveryTickWhileStillOverdue(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Minute)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	h.clock.Advance(2 * time.Minute)

	sweep := &usecases.SweepCPTMisses{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock}
	first, err := sweep.Execute(ctx)
	if err != nil || first != 1 {
		t.Fatalf("first pass: reported=%d err=%v", first, err)
	}
	second, err := sweep.Execute(ctx)
	if err != nil || second != 1 {
		t.Fatalf("second pass: expected the still-overdue task to be reported again, reported=%d err=%v", second, err)
	}
}

func TestSweepCPTMisses_PropagatesFindOpenPastCPTError(t *testing.T) {
	h := newHarness()
	tasks := newErrTaskRepo()
	tasks.failFindOpenPastCPT = true

	sweep := &usecases.SweepCPTMisses{Tasks: tasks, Publisher: h.publisher, Clock: h.clock}
	_, err := sweep.Execute(context.Background())
	if !errors.Is(err, errFake) {
		t.Fatalf("expected FindOpenPastCPT error to propagate, got %v", err)
	}
}

func TestSweepCPTMisses_PropagatesPublishError(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Minute)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	h.clock.Advance(2 * time.Minute)

	sweep := &usecases.SweepCPTMisses{Tasks: h.tasks, Publisher: &errPublisher{fail: true}, Clock: h.clock}
	_, err := sweep.Execute(ctx)
	if !errors.Is(err, errFake) {
		t.Fatalf("expected publish error to propagate, got %v", err)
	}
}

func TestRenewLease_ExtendsClaim(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock, LeaseDuration: time.Minute}
	claimed, _ := claim.Execute(ctx, "s1", task.Pick)

	h.clock.Advance(30 * time.Second)
	renew := &usecases.RenewLease{Tasks: h.tasks, Clock: h.clock, LeaseDuration: time.Minute}
	if err := renew.Execute(ctx, claimed.Id(), "s1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, _ := h.tasks.FindById(ctx, claimed.Id())
	if !got.Lease().Expiry.After(epoch.Add(time.Minute)) {
		t.Fatalf("expected extended lease expiry, got %v", got.Lease().Expiry)
	}
}

func TestCompleteTask_ValidatesOwnershipAndCompletes(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pick)

	complete := &usecases.CompleteTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock}

	if err := complete.Execute(ctx, claimed.Id(), "wrong-station"); err == nil {
		t.Fatalf("expected ownership validation to reject a non-owning station")
	}

	if err := complete.Execute(ctx, claimed.Id(), "s1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, _ := h.tasks.FindById(ctx, claimed.Id())
	if got.Status() != task.Completed {
		t.Fatalf("expected Completed, got %s", got.Status())
	}
}

func TestSealPackageAndRunSlam_LabelsWithinTolerance(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pack)

	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }}
	p, err := seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	slam := &usecases.RunSlam{Packages: h.packages, Publisher: h.publisher, Clock: h.clock}
	if err := slam.Execute(ctx, p.Id(), 2.0, 2.0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, _ := h.packages.FindById(ctx, p.Id())
	if got.Status() != pack.Labeled {
		t.Fatalf("expected LABELED, got %s", got.Status())
	}
}

// The idempotency guard REST_AUDIT.md flags as a known gap: a retried
// POST /tasks/{id}/seal-package (same taskId, same stationId) must return
// the ALREADY-sealed Package, not create a second one.
func TestSealPackage_RetryReturnsExistingPackage_DoesNotCreateDuplicate(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pack)

	ids := idSeq("p")
	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return shared.PackageId(ids()) }}

	first, err := seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-1"})
	if err != nil {
		t.Fatalf("first call: unexpected error: %v", err)
	}

	second, err := seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-1"})
	if err != nil {
		t.Fatalf("retry: unexpected error: %v", err)
	}
	if second.Id() != first.Id() {
		t.Fatalf("expected retry to return the SAME package id %q, got a different one %q", first.Id(), second.Id())
	}

	// Only one Package was ever actually saved — the memory repo is keyed
	// by id, so a duplicate Save under a different id would leave both
	// discoverable; assert there is exactly one via a fresh FindByTaskId,
	// and that its own scanned contents were not re-appended on retry.
	stored, err := h.packages.FindByTaskId(ctx, claimed.Id())
	if err != nil {
		t.Fatalf("FindByTaskId: %v", err)
	}
	if stored == nil || stored.Id() != first.Id() {
		t.Fatalf("expected FindByTaskId to resolve to the original package, got %+v", stored)
	}
	if len(stored.ScannedContents()) != 1 {
		t.Fatalf("expected scanned contents to still have exactly 1 item after retry, got %d", len(stored.ScannedContents()))
	}
}

// A different task must never see another task's already-sealed package —
// the guard is scoped per-taskId, not global.
func TestSealPackage_IdempotencyIsScopedPerTask(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-2", shared.NewCapabilitySet("pack"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))
	_ = h.stations.Save(ctx, station.New("s2", shared.NewCapabilitySet("pack")))

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimedA, _ := claim.Execute(ctx, "s1", task.Pack)
	claimedB, _ := claim.Execute(ctx, "s2", task.Pack)

	ids := idSeq("p")
	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return shared.PackageId(ids()) }}

	pA, err := seal.Execute(ctx, claimedA.Id(), "s1", []string{"sku-a"})
	if err != nil {
		t.Fatalf("seal A: unexpected error: %v", err)
	}
	pB, err := seal.Execute(ctx, claimedB.Id(), "s2", []string{"sku-b"})
	if err != nil {
		t.Fatalf("seal B: unexpected error: %v", err)
	}
	if pA.Id() == pB.Id() {
		t.Fatalf("expected two distinct tasks to produce two distinct packages, got the same id %q for both", pA.Id())
	}
}

// SealPackage derives Package.FragileHandling from the owning task's
// Fragile flag rather than accepting it as a separate caller-supplied
// argument — the flag rides in on the Task (stamped by wes-work-planning at
// release time), not the seal-package request.
func TestSealPackage_DerivesFragileHandlingFromTask(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), true, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pack)
	if !claimed.Fragile() {
		t.Fatalf("expected the claimed task to carry Fragile() == true")
	}

	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }}
	p, err := seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.FragileHandling() {
		t.Fatalf("expected the sealed package to carry FragileHandling() == true, derived from the task")
	}

	got, _ := h.packages.FindById(ctx, p.Id())
	if got == nil || !got.FragileHandling() {
		t.Fatalf("expected the persisted package to carry FragileHandling() == true, got %+v", got)
	}
}

func TestSealPackage_NonFragileTaskProducesNonFragilePackage(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pack)

	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }}
	p, err := seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.FragileHandling() {
		t.Fatalf("expected FragileHandling() == false when the task was not fragile")
	}
}

// SealPackage derives Package.GiftWrapRequested from the owning task's
// GiftWrap flag rather than accepting it as a separate caller-supplied
// argument — the flag rides in on the Task (stamped by wes-work-planning
// from a caller-stated WorkReleased.data.gift_wrap request), not the
// seal-package request (see ADR-0011).
func TestSealPackage_DerivesGiftWrapRequestedFromTask(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, true)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pack)
	if !claimed.GiftWrap() {
		t.Fatalf("expected the claimed task to carry GiftWrap() == true")
	}

	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }}
	p, err := seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.GiftWrapRequested() {
		t.Fatalf("expected the sealed package to carry GiftWrapRequested() == true, derived from the task")
	}

	got, _ := h.packages.FindById(ctx, p.Id())
	if got == nil || !got.GiftWrapRequested() {
		t.Fatalf("expected the persisted package to carry GiftWrapRequested() == true, got %+v", got)
	}
}

func TestSealPackage_NonGiftWrapTaskProducesNonGiftWrapPackage(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pack)

	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }}
	p, err := seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.GiftWrapRequested() {
		t.Fatalf("expected GiftWrapRequested() == false when the task was not gift-wrap requested")
	}
}

// Fragile and GiftWrap are independently derived onto Package at seal
// time — one being true must not affect the other (no merged flag).
func TestSealPackage_FragileAndGiftWrapAreIndependentlyDerived(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), true, true)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pack)

	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }}
	p, err := seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.FragileHandling() || !p.GiftWrapRequested() {
		t.Fatalf("expected both FragileHandling() and GiftWrapRequested() to be true, got %v/%v", p.FragileHandling(), p.GiftWrapRequested())
	}
}

func TestGetQueueDepth_CountsPendingTasksOfType(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(2*time.Hour)), "order-2", shared.NewCapabilitySet("pick"), false, false)
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-3", shared.NewCapabilitySet("pack"), false, false)

	depth := &usecases.GetQueueDepth{Tasks: h.tasks}
	got, err := depth.Execute(ctx, task.Pick)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 2 {
		t.Fatalf("expected 2, got %d", got)
	}
}

func TestGetInstalledCapacity_CountsStationsHoldingCapability(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	_ = h.stations.Save(ctx, station.New("s2", shared.NewCapabilitySet("pick", "pack")))
	_ = h.stations.Save(ctx, station.New("s3", shared.NewCapabilitySet("pack")))

	uc := &usecases.GetInstalledCapacity{Stations: h.stations}
	got, err := uc.Execute(ctx, "pick")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 2 {
		t.Fatalf("expected 2 stations with pick capability, got %d", got)
	}
}

func TestGetInstalledCapacity_UnknownCapability_ReturnsZero(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))

	uc := &usecases.GetInstalledCapacity{Stations: h.stations}
	got, err := uc.Execute(ctx, "rebin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestGetTasksByOrderRef_ReturnsEveryTaskForTheOrder(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(2*time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	_, _ = create.Execute(ctx, task.Slam, shared.NewCPT(epoch.Add(3*time.Hour)), "order-2", shared.NewCapabilitySet("slam"), false, false)

	uc := &usecases.GetTasksByOrderRef{Tasks: h.tasks}
	got, err := uc.Execute(ctx, "order-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 tasks for order-1, got %d", len(got))
	}
	for _, tk := range got {
		if tk.OrderRef() != "order-1" {
			t.Fatalf("expected only order-1 tasks, got orderRef %q", tk.OrderRef())
		}
	}
}

// A retried leg (a second task created for the same order after the first
// was e.g. abandoned) must show up as a second entry, not be deduplicated —
// callers need to see every task, including retries.
func TestGetTasksByOrderRef_IncludesRetriedLegs(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(2*time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)

	uc := &usecases.GetTasksByOrderRef{Tasks: h.tasks}
	got, err := uc.Execute(ctx, "order-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 retried PICK legs for order-1, got %d", len(got))
	}
}

func TestGetTasksByOrderRef_UnknownOrderRefReturnsEmptyNotError(t *testing.T) {
	h := newHarness()
	uc := &usecases.GetTasksByOrderRef{Tasks: h.tasks}

	got, err := uc.Execute(context.Background(), "does-not-exist")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty result, got %d", len(got))
	}
}

func TestRegisterStation_AddsStationToPool(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	register := &usecases.RegisterStation{Stations: h.stations, Publisher: h.publisher}

	got, err := register.Execute(ctx, "s1", []string{"pick"}, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Id() != shared.StationId("s1") {
		t.Fatalf("expected station s1, got %s", got.Id())
	}

	found, _ := h.stations.FindById(ctx, "s1")
	if found == nil {
		t.Fatalf("expected station to be persisted")
	}
	if !found.Capabilities().Contains("pick") {
		t.Fatalf("expected persisted station to have pick capability")
	}
}

func TestRegisterStation_ReRegisteringUpdatesCapabilities(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	register := &usecases.RegisterStation{Stations: h.stations, Publisher: h.publisher}

	_, err := register.Execute(ctx, "s1", []string{"pick"}, "")
	if err != nil {
		t.Fatalf("first registration: %v", err)
	}
	_, err = register.Execute(ctx, "s1", []string{"pick", "hazmat"}, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	found, _ := h.stations.FindById(ctx, "s1")
	if !found.Capabilities().Contains("hazmat") {
		t.Fatalf("expected re-registration to update capabilities to include hazmat")
	}
}

func TestCreateTask_PropagatesSaveError(t *testing.T) {
	h := newHarness()
	tasks := newErrTaskRepo()
	tasks.failSave = true
	uc := &usecases.CreateTask{Tasks: tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}

	_, err := uc.Execute(context.Background(), task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	if !errors.Is(err, errFake) {
		t.Fatalf("expected save error to propagate, got %v", err)
	}
}

func TestCreateTask_PropagatesPublishError(t *testing.T) {
	h := newHarness()
	uc := &usecases.CreateTask{Tasks: h.tasks, Publisher: &errPublisher{fail: true}, Clock: h.clock, NewId: idSeq("t")}

	_, err := uc.Execute(context.Background(), task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	if !errors.Is(err, errFake) {
		t.Fatalf("expected publish error to propagate, got %v", err)
	}
}

func TestClaimNext_PropagatesStationLookupError(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	stations := newErrStationRepo()
	stations.failFindById = true

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: stations, Publisher: h.publisher, Clock: h.clock}
	_, err := claim.Execute(ctx, "s1", task.Pick)
	if !errors.Is(err, errFake) {
		t.Fatalf("expected station lookup error to propagate, got %v", err)
	}
}

func TestClaimNext_PropagatesFindClaimableError(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	tasks := newErrTaskRepo()
	tasks.failFindClaimableByType = true
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))

	claim := &usecases.ClaimNext{Tasks: tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	_, err := claim.Execute(ctx, "s1", task.Pick)
	if !errors.Is(err, errFake) {
		t.Fatalf("expected FindClaimableByType error to propagate, got %v", err)
	}
}

func TestClaimNext_PropagatesSaveError(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	tasks := newErrTaskRepo()
	create := &usecases.CreateTask{Tasks: tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))

	tasks.failSave = true
	claim := &usecases.ClaimNext{Tasks: tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	_, err := claim.Execute(ctx, "s1", task.Pick)
	if !errors.Is(err, errFake) {
		t.Fatalf("expected save error to propagate, got %v", err)
	}
}

func TestClaimNext_PropagatesPublishError(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))

	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: &errPublisher{fail: true}, Clock: h.clock}
	_, err := claim.Execute(ctx, "s1", task.Pick)
	if !errors.Is(err, errFake) {
		t.Fatalf("expected publish error to propagate, got %v", err)
	}
}

func TestCompleteTask_PropagatesFindError(t *testing.T) {
	h := newHarness()
	tasks := newErrTaskRepo()
	tasks.failFindById = true

	complete := &usecases.CompleteTask{Tasks: tasks, Publisher: h.publisher, Clock: h.clock}
	err := complete.Execute(context.Background(), "t1", "s1")
	if !errors.Is(err, errFake) {
		t.Fatalf("expected find error to propagate, got %v", err)
	}
}

func TestCompleteTask_ReturnsErrTaskNotFound(t *testing.T) {
	h := newHarness()
	complete := &usecases.CompleteTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock}
	err := complete.Execute(context.Background(), "missing", "s1")
	if !errors.Is(err, usecases.ErrTaskNotFound) {
		t.Fatalf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestCompleteTask_PropagatesSaveError(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	tasks := newErrTaskRepo()
	create := &usecases.CreateTask{Tasks: tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	claim := &usecases.ClaimNext{Tasks: tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pick)

	tasks.failSave = true
	complete := &usecases.CompleteTask{Tasks: tasks, Publisher: h.publisher, Clock: h.clock}
	err := complete.Execute(ctx, claimed.Id(), "s1")
	if !errors.Is(err, errFake) {
		t.Fatalf("expected save error to propagate, got %v", err)
	}
}

func TestCompleteTask_PropagatesPublishError(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pick)

	complete := &usecases.CompleteTask{Tasks: h.tasks, Publisher: &errPublisher{fail: true}, Clock: h.clock}
	err := complete.Execute(ctx, claimed.Id(), "s1")
	if !errors.Is(err, errFake) {
		t.Fatalf("expected publish error to propagate, got %v", err)
	}
}

func TestExpireLeases_PropagatesFindAllClaimedError(t *testing.T) {
	h := newHarness()
	tasks := newErrTaskRepo()
	tasks.failFindAllClaimed = true

	sweep := &usecases.ExpireLeases{Tasks: tasks, Publisher: h.publisher, Clock: h.clock}
	_, err := sweep.Execute(context.Background())
	if !errors.Is(err, errFake) {
		t.Fatalf("expected FindAllClaimed error to propagate, got %v", err)
	}
}

func TestExpireLeases_PropagatesSaveError(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	tasks := newErrTaskRepo()
	create := &usecases.CreateTask{Tasks: tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	claim := &usecases.ClaimNext{Tasks: tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock, LeaseDuration: time.Minute}
	_, _ = claim.Execute(ctx, "s1", task.Pick)
	h.clock.Advance(2 * time.Minute)

	tasks.failSave = true
	sweep := &usecases.ExpireLeases{Tasks: tasks, Publisher: h.publisher, Clock: h.clock}
	_, err := sweep.Execute(ctx)
	if !errors.Is(err, errFake) {
		t.Fatalf("expected save error to propagate, got %v", err)
	}
}

func TestExpireLeases_PropagatesPublishError(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock, LeaseDuration: time.Minute}
	_, _ = claim.Execute(ctx, "s1", task.Pick)
	h.clock.Advance(2 * time.Minute)

	sweep := &usecases.ExpireLeases{Tasks: h.tasks, Publisher: &errPublisher{fail: true}, Clock: h.clock}
	_, err := sweep.Execute(ctx)
	if !errors.Is(err, errFake) {
		t.Fatalf("expected publish error to propagate, got %v", err)
	}
}

func TestRegisterStation_PropagatesSaveError(t *testing.T) {
	stations := newErrStationRepo()
	stations.failSave = true
	register := &usecases.RegisterStation{Stations: stations, Publisher: events.NewBufferedPublisher()}

	_, err := register.Execute(context.Background(), "s1", []string{"pick"}, "")
	if !errors.Is(err, errFake) {
		t.Fatalf("expected save error to propagate, got %v", err)
	}
}

func TestRenewLease_PropagatesFindError(t *testing.T) {
	tasks := newErrTaskRepo()
	tasks.failFindById = true
	renew := &usecases.RenewLease{Tasks: tasks, Clock: memory.NewFixedClock(epoch)}
	err := renew.Execute(context.Background(), "t1", "s1")
	if !errors.Is(err, errFake) {
		t.Fatalf("expected find error to propagate, got %v", err)
	}
}

func TestRenewLease_ReturnsErrTaskNotFound(t *testing.T) {
	h := newHarness()
	renew := &usecases.RenewLease{Tasks: h.tasks, Clock: h.clock}
	err := renew.Execute(context.Background(), "missing", "s1")
	if !errors.Is(err, usecases.ErrTaskNotFound) {
		t.Fatalf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestRenewLease_PropagatesSaveError(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	tasks := newErrTaskRepo()
	create := &usecases.CreateTask{Tasks: tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	claim := &usecases.ClaimNext{Tasks: tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock, LeaseDuration: time.Minute}
	claimed, _ := claim.Execute(ctx, "s1", task.Pick)

	tasks.failSave = true
	renew := &usecases.RenewLease{Tasks: tasks, Clock: h.clock, LeaseDuration: time.Minute}
	err := renew.Execute(ctx, claimed.Id(), "s1")
	if !errors.Is(err, errFake) {
		t.Fatalf("expected save error to propagate, got %v", err)
	}
}

func TestRenewLease_PropagatesDomainError(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pick)

	renew := &usecases.RenewLease{Tasks: h.tasks, Clock: h.clock}
	err := renew.Execute(ctx, claimed.Id(), "wrong-station")
	if !errors.Is(err, task.ErrNotOwner) {
		t.Fatalf("expected ErrNotOwner, got %v", err)
	}
}

func TestRunSlam_PropagatesFindError(t *testing.T) {
	packages := newErrPackageRepo()
	packages.failFindById = true
	slam := &usecases.RunSlam{Packages: packages, Publisher: events.NewBufferedPublisher(), Clock: memory.NewFixedClock(epoch)}
	err := slam.Execute(context.Background(), "p1", 2.0, 2.0)
	if !errors.Is(err, errFake) {
		t.Fatalf("expected find error to propagate, got %v", err)
	}
}

func TestRunSlam_ReturnsErrPackageNotFound(t *testing.T) {
	h := newHarness()
	slam := &usecases.RunSlam{Packages: h.packages, Publisher: h.publisher, Clock: h.clock}
	err := slam.Execute(context.Background(), "missing", 2.0, 2.0)
	if !errors.Is(err, usecases.ErrPackageNotFound) {
		t.Fatalf("expected ErrPackageNotFound, got %v", err)
	}
}

func TestRunSlam_PropagatesWeighError(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	p := pack.New("p1", "order-1", "task-1", false, false)
	_ = h.packages.Save(ctx, p)

	slam := &usecases.RunSlam{Packages: h.packages, Publisher: h.publisher, Clock: h.clock}
	err := slam.Execute(ctx, p.Id(), 2.0, 2.0)
	if !errors.Is(err, pack.ErrNotSealed) {
		t.Fatalf("expected ErrNotSealed, got %v", err)
	}
}

func TestRunSlam_PropagatesSaveError(t *testing.T) {
	ctx := context.Background()
	packages := newErrPackageRepo()
	p := pack.New("p1", "order-1", "task-1", false, false)
	_ = p.ScanItem("sku-1")
	_ = p.Seal()
	_ = packages.Save(ctx, p)

	packages.failSave = true
	slam := &usecases.RunSlam{Packages: packages, Publisher: events.NewBufferedPublisher(), Clock: memory.NewFixedClock(epoch)}
	err := slam.Execute(ctx, p.Id(), 2.0, 2.0)
	if !errors.Is(err, errFake) {
		t.Fatalf("expected save error to propagate, got %v", err)
	}
}

func TestRunSlam_PropagatesPublishError_LabelApplied(t *testing.T) {
	ctx := context.Background()
	packages := memory.NewPackageRepo()
	p := pack.New("p1", "order-1", "task-1", false, false)
	_ = p.ScanItem("sku-1")
	_ = p.Seal()
	_ = packages.Save(ctx, p)

	slam := &usecases.RunSlam{Packages: packages, Publisher: &errPublisher{fail: true}, Clock: memory.NewFixedClock(epoch)}
	err := slam.Execute(ctx, p.Id(), 2.0, 2.0)
	if !errors.Is(err, errFake) {
		t.Fatalf("expected publish error to propagate, got %v", err)
	}
}

func TestRunSlam_PropagatesPublishError_Diverted(t *testing.T) {
	ctx := context.Background()
	packages := memory.NewPackageRepo()
	p := pack.New("p1", "order-1", "task-1", false, false)
	_ = p.ScanItem("sku-1")
	_ = p.Seal()
	_ = packages.Save(ctx, p)

	slam := &usecases.RunSlam{Packages: packages, Publisher: &errPublisher{fail: true}, Clock: memory.NewFixedClock(epoch)}
	err := slam.Execute(ctx, p.Id(), 2.0, 2.5)
	if !errors.Is(err, errFake) {
		t.Fatalf("expected publish error to propagate, got %v", err)
	}
}

func TestSealPackage_PropagatesFindError(t *testing.T) {
	tasks := newErrTaskRepo()
	tasks.failFindById = true
	seal := &usecases.SealPackage{Tasks: tasks, Packages: memory.NewPackageRepo(), Publisher: events.NewBufferedPublisher(), Clock: memory.NewFixedClock(epoch), NewId: func() shared.PackageId { return "p1" }}
	_, err := seal.Execute(context.Background(), "t1", "s1", []string{"sku-1"})
	if !errors.Is(err, errFake) {
		t.Fatalf("expected find error to propagate, got %v", err)
	}
}

func TestSealPackage_ReturnsErrTaskNotFound(t *testing.T) {
	h := newHarness()
	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }}
	_, err := seal.Execute(context.Background(), "missing", "s1", []string{"sku-1"})
	if !errors.Is(err, usecases.ErrTaskNotFound) {
		t.Fatalf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestSealPackage_ReturnsErrWrongTaskType(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	tk, _ := create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false)

	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }}
	_, err := seal.Execute(ctx, tk.Id(), "s1", []string{"sku-1"})
	if !errors.Is(err, usecases.ErrWrongTaskType) {
		t.Fatalf("expected ErrWrongTaskType, got %v", err)
	}
}

func TestSealPackage_ReturnsErrNotClaimedWhenUnclaimed(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	tk, _ := create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)

	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }}
	_, err := seal.Execute(ctx, tk.Id(), "s1", []string{"sku-1"})
	if !errors.Is(err, task.ErrNotClaimed) {
		t.Fatalf("expected ErrNotClaimed for a missing lease, got %v", err)
	}
}

func TestSealPackage_ReturnsErrNotOwnerForNonOwningStation(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pack)

	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }}
	_, err := seal.Execute(ctx, claimed.Id(), "wrong-station", []string{"sku-1"})
	if !errors.Is(err, task.ErrNotOwner) {
		t.Fatalf("expected ErrNotOwner, got %v", err)
	}
}

// An expired lease must not authorise a seal: the lease no longer holds the
// task, even though the task row is still CLAIMED (no sweep has run) and the
// lease's station id still matches. Decision 2026-10-06 (ADR-0038): the error
// is ErrNotClaimed, the same one Complete and RenewLease return for the same
// condition. Nothing may be persisted or published.
func TestSealPackage_RejectsExpiredLease(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock, LeaseDuration: time.Minute}
	claimed, err := claim.Execute(ctx, "s1", task.Pack)
	if err != nil || claimed == nil {
		t.Fatalf("setup claim failed: %v", err)
	}

	h.clock.Advance(2 * time.Minute)

	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }}
	_, err = seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-1"})
	if !errors.Is(err, task.ErrNotClaimed) {
		t.Fatalf("expected ErrNotClaimed for an expired lease, got %v", err)
	}
	if errors.Is(err, task.ErrNotOwner) {
		t.Fatalf("an expired lease must not be reported as ErrNotOwner")
	}
	if got, _ := h.packages.FindByTaskId(ctx, claimed.Id()); got != nil {
		t.Fatalf("no package may be saved for an expired lease, found %v", got.Id())
	}
}

func TestSealPackage_PropagatesSealError(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pack)

	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }}
	_, err := seal.Execute(ctx, claimed.Id(), "s1", nil)
	if !errors.Is(err, pack.ErrNoScannedContents) {
		t.Fatalf("expected ErrNoScannedContents, got %v", err)
	}
}

func TestSealPackage_PropagatesSaveError(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pack)

	packages := newErrPackageRepo()
	packages.failSave = true
	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }}
	_, err := seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-1"})
	if !errors.Is(err, errFake) {
		t.Fatalf("expected save error to propagate, got %v", err)
	}
}

func TestSealPackage_PropagatesPublishError(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pack)

	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: &errPublisher{fail: true}, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }}
	_, err := seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-1"})
	if !errors.Is(err, errFake) {
		t.Fatalf("expected publish error to propagate, got %v", err)
	}
}

// --- ClassificationLookup: live per-scanned-item DOT hazard segregation (ADR-0010) ---

// A SealPackage built with no ClassificationLookup (as every test above
// does) must behave exactly as before this feature — permissive by
// construction, no segregation check ever runs.
func TestSealPackage_NilClassificationLookup_BehavesLikeBeforeThisFeature(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pack)

	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }}
	p, err := seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-1", "sku-2"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := p.SortLane(); got != pack.SortLaneStandard {
		t.Fatalf("expected STANDARD sort lane with no classification lookup wired, got %s", got)
	}
}

// Two hazmat-classified items whose DOT hazard classes ARE compatible per
// the segregation matrix must both seal successfully into one package and
// drive SortLane() to HAZMAT_LANE.
func TestSealPackage_HazmatClassifiedItemsCompatible_Pass(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pack)

	lookup := newFakeClassificationLookup()
	lookup.bySKU["sku-class-3"] = ports.ClassificationInfo{Known: true, Hazmat: true, DOTHazardClass: 3}
	lookup.bySKU["sku-class-9"] = ports.ClassificationInfo{Known: true, Hazmat: true, DOTHazardClass: 9}

	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }, ClassificationLookup: lookup}
	p, err := seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-class-3", "sku-class-9"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := p.SortLane(); got != pack.SortLaneHazmat {
		t.Fatalf("expected HAZMAT_LANE, got %s", got)
	}
	if len(p.ScannedHazardClasses()) != 2 {
		t.Fatalf("expected both hazard classes recorded, got %v", p.ScannedHazardClasses())
	}
}

// Two hazmat-classified items whose DOT hazard classes are INCOMPATIBLE
// must reject the whole seal with ErrPackageSegregationViolation, and the
// package must not be persisted.
func TestSealPackage_HazmatClassifiedItemsIncompatible_Reject(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pack)

	lookup := newFakeClassificationLookup()
	lookup.bySKU["sku-class-1"] = ports.ClassificationInfo{Known: true, Hazmat: true, DOTHazardClass: 1}
	lookup.bySKU["sku-class-3"] = ports.ClassificationInfo{Known: true, Hazmat: true, DOTHazardClass: 3}

	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }, ClassificationLookup: lookup}
	_, err := seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-class-1", "sku-class-3"})
	if !errors.Is(err, pack.ErrPackageSegregationViolation) {
		t.Fatalf("expected ErrPackageSegregationViolation, got %v", err)
	}

	got, _ := h.packages.FindById(ctx, "p1")
	if got != nil {
		t.Fatalf("expected the package to NOT be persisted after a segregation violation, got %+v", got)
	}
}

// A SKU the lookup reports Known=false (or plain unregistered) never
// triggers or blocks segregation — fail-open, and the item seals normally.
func TestSealPackage_UnclassifiedItems_FailOpen(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pack)

	lookup := newFakeClassificationLookup()
	lookup.bySKU["sku-class-1"] = ports.ClassificationInfo{Known: true, Hazmat: true, DOTHazardClass: 1}
	// sku-unclassified is never registered in lookup.bySKU -> Known: false.

	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }, ClassificationLookup: lookup}
	p, err := seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-class-1", "sku-unclassified"})
	if err != nil {
		t.Fatalf("expected the unclassified item to fail open (no rejection), got %v", err)
	}
	if len(p.ScannedContents()) != 2 {
		t.Fatalf("expected both items scanned, got %v", p.ScannedContents())
	}
	if len(p.ScannedHazardClasses()) != 1 {
		t.Fatalf("expected only the classified item's hazard class recorded, got %v", p.ScannedHazardClasses())
	}
}

// A single SKU's lookup transport error fails open for that SKU only — the
// rest of the seal proceeds normally rather than aborting the whole call.
func TestSealPackage_SingleSKULookupTransportError_FailsOpenForThatItemOnly(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, _ := claim.Execute(ctx, "s1", task.Pack)

	lookup := newFakeClassificationLookup()
	lookup.bySKU["sku-class-3"] = ports.ClassificationInfo{Known: true, Hazmat: true, DOTHazardClass: 3}
	lookup.failFor["sku-flaky"] = true

	seal := &usecases.SealPackage{Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.PackageId { return "p1" }, ClassificationLookup: lookup}
	p, err := seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-class-3", "sku-flaky"})
	if err != nil {
		t.Fatalf("expected the whole seal to succeed despite one SKU's lookup error, got %v", err)
	}
	if len(p.ScannedContents()) != 2 {
		t.Fatalf("expected both items scanned despite the lookup error, got %v", p.ScannedContents())
	}
	if len(p.ScannedHazardClasses()) != 1 {
		t.Fatalf("expected only the successfully-classified item's hazard class recorded, got %v", p.ScannedHazardClasses())
	}
	if len(lookup.calls) != 2 {
		t.Fatalf("expected both SKUs to be looked up, got %v", lookup.calls)
	}
}

// Proves RegisterStation and ClaimNext interoperate: a station that did not
// exist before is registered over the use case, then immediately claims
// against it successfully instead of failing with ErrStationNotFound.
func TestRegisterStation_ThenClaimNextSucceeds(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	register := &usecases.RegisterStation{Stations: h.stations, Publisher: h.publisher}
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}

	if _, err := claim.Execute(ctx, "s1", task.Pick); !errors.Is(err, usecases.ErrStationNotFound) {
		t.Fatalf("expected ErrStationNotFound before registration, got %v", err)
	}

	if _, err := register.Execute(ctx, "s1", []string{"pick"}, ""); err != nil {
		t.Fatalf("unexpected error registering station: %v", err)
	}
	if _, err := create.Execute(ctx, task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pick"), false, false); err != nil {
		t.Fatalf("unexpected error creating task: %v", err)
	}

	got, err := claim.Execute(ctx, "s1", task.Pick)
	if err != nil {
		t.Fatalf("expected claim to succeed after registration, got %v", err)
	}
	if got.Status() != task.Claimed {
		t.Fatalf("expected Claimed, got %s", got.Status())
	}
}

// TestClaimNext_CountsTheClaimedTaskByType and its CompleteTask counterpart
// pin the business metric to the real domain event -- the moment the task is
// actually leased/completed -- rather than to an HTTP request that may not
// have led to either.
func TestClaimNext_CountsTheClaimedTaskByType(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))

	metrics := &recordingMetrics{}
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock, Metrics: metrics}

	if _, err := claim.Execute(ctx, "s1", task.Pack); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(metrics.claimed) != 1 || metrics.claimed[0] != task.Pack {
		t.Fatalf("claimed counter recorded %v, want one Pack", metrics.claimed)
	}

	// A claim that finds nothing must not count.
	if _, err := claim.Execute(ctx, "s1", task.Pack); err == nil {
		t.Fatal("expected ErrNoClaimableTask on an empty pool")
	}
	if len(metrics.claimed) != 1 {
		t.Errorf("a failed claim was counted: %v", metrics.claimed)
	}
}

func TestCompleteTask_CountsTheCompletedTaskByType(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	_, _ = create.Execute(ctx, task.Slam, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("slam"), false, false)
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("slam")))

	metrics := &recordingMetrics{}
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock, Metrics: metrics}
	claimed, err := claim.Execute(ctx, "s1", task.Slam)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	complete := &usecases.CompleteTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, Metrics: metrics}

	// A rejected completion must not count.
	if err := complete.Execute(ctx, claimed.Id(), "wrong-station"); err == nil {
		t.Fatal("expected ownership validation to reject a non-owning station")
	}
	if len(metrics.completed) != 0 {
		t.Fatalf("a rejected completion was counted: %v", metrics.completed)
	}

	if err := complete.Execute(ctx, claimed.Id(), "s1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(metrics.completed) != 1 || metrics.completed[0] != task.Slam {
		t.Fatalf("completed counter recorded %v, want one Slam", metrics.completed)
	}
}

// CheckInStation wires station.CheckIn to the application layer: success
// assigns the occupant and persists it.
func TestCheckInStation_AssignsOccupant(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))

	uc := &usecases.CheckInStation{Stations: h.stations}
	got, err := uc.Execute(ctx, "s1", "worker-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got.IsOccupied() || *got.Occupant() != station.OccupantId("worker-1") {
		t.Fatalf("expected station occupied by worker-1, got %+v", got.Occupant())
	}

	persisted, _ := h.stations.FindById(ctx, "s1")
	if persisted == nil || !persisted.IsOccupied() {
		t.Fatalf("expected the check-in to be persisted")
	}
}

func TestCheckInStation_ReturnsErrStationNotFound(t *testing.T) {
	h := newHarness()
	uc := &usecases.CheckInStation{Stations: h.stations}
	_, err := uc.Execute(context.Background(), "does-not-exist", "worker-1")
	if !errors.Is(err, usecases.ErrStationNotFound) {
		t.Fatalf("expected ErrStationNotFound, got %v", err)
	}
}

// Invariant (through the use case): one occupant at a time.
func TestCheckInStation_RejectsSecondOccupant(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))

	uc := &usecases.CheckInStation{Stations: h.stations}
	if _, err := uc.Execute(ctx, "s1", "worker-1"); err != nil {
		t.Fatalf("first check-in should succeed: %v", err)
	}
	_, err := uc.Execute(ctx, "s1", "worker-2")
	if !errors.Is(err, station.ErrOccupied) {
		t.Fatalf("expected station.ErrOccupied, got %v", err)
	}
}

// CheckOutStation wires station.CheckOut to the application layer:
// success clears the occupant and persists it.
func TestCheckOutStation_ClearsOccupant(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	_, _ = (&usecases.CheckInStation{Stations: h.stations}).Execute(ctx, "s1", "worker-1")

	uc := &usecases.CheckOutStation{Stations: h.stations}
	got, err := uc.Execute(ctx, "s1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.IsOccupied() {
		t.Fatalf("expected station to be vacant after check-out")
	}

	persisted, _ := h.stations.FindById(ctx, "s1")
	if persisted == nil || persisted.IsOccupied() {
		t.Fatalf("expected the check-out to be persisted")
	}
}

func TestCheckOutStation_ReturnsErrStationNotFound(t *testing.T) {
	h := newHarness()
	uc := &usecases.CheckOutStation{Stations: h.stations}
	_, err := uc.Execute(context.Background(), "does-not-exist")
	if !errors.Is(err, usecases.ErrStationNotFound) {
		t.Fatalf("expected ErrStationNotFound, got %v", err)
	}
}

func TestCheckOutStation_RejectsWhenNotOccupied(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))

	uc := &usecases.CheckOutStation{Stations: h.stations}
	_, err := uc.Execute(ctx, "s1")
	if !errors.Is(err, station.ErrNotOccupied) {
		t.Fatalf("expected station.ErrNotOccupied, got %v", err)
	}
}

func TestCheckInStation_PropagatesStationLookupError(t *testing.T) {
	stations := newErrStationRepo()
	stations.failFindById = true

	uc := &usecases.CheckInStation{Stations: stations}
	_, err := uc.Execute(context.Background(), "s1", "worker-1")
	if !errors.Is(err, errFake) {
		t.Fatalf("expected station lookup error to propagate, got %v", err)
	}
}

// The station must be registered BEFORE Save is forced to fail, so the
// use case reaches the persistence step rather than the lookup.
//
// No persisted-state assertion is possible here: the in-memory repo
// stores and hands back the same *Station, so CheckIn's in-memory
// mutation is visible through the repo even when Save fails — an
// aliasing artifact of the test double, not of the use case (the
// Postgres repo round-trips through serialization, so a failed Save
// leaves persisted state untouched).
func TestCheckInStation_PropagatesSaveError(t *testing.T) {
	ctx := context.Background()
	stations := newErrStationRepo()
	_ = stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	stations.failSave = true

	uc := &usecases.CheckInStation{Stations: stations}
	_, err := uc.Execute(ctx, "s1", "worker-1")
	if !errors.Is(err, errFake) {
		t.Fatalf("expected save error to propagate, got %v", err)
	}
}

func TestCheckOutStation_PropagatesStationLookupError(t *testing.T) {
	stations := newErrStationRepo()
	stations.failFindById = true

	uc := &usecases.CheckOutStation{Stations: stations}
	_, err := uc.Execute(context.Background(), "s1")
	if !errors.Is(err, errFake) {
		t.Fatalf("expected station lookup error to propagate, got %v", err)
	}
}

// Check the occupant in BEFORE forcing Save to fail, so the use case
// reaches the persistence step with a legitimately occupied station.
// (See TestCheckInStation_PropagatesSaveError for why no persisted-state
// assertion is possible against the aliasing in-memory repo.)
func TestCheckOutStation_PropagatesSaveError(t *testing.T) {
	ctx := context.Background()
	stations := newErrStationRepo()
	_ = stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	if _, err := (&usecases.CheckInStation{Stations: stations}).Execute(ctx, "s1", "worker-1"); err != nil {
		t.Fatalf("check-in should succeed: %v", err)
	}
	stations.failSave = true

	uc := &usecases.CheckOutStation{Stations: stations}
	_, err := uc.Execute(ctx, "s1")
	if !errors.Is(err, errFake) {
		t.Fatalf("expected save error to propagate, got %v", err)
	}
}
