//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/domain/consolidation"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// newPool returns a pool over the package's shared, fully migrated
// Postgres with every table emptied. It delegates to outboxDB
// (testcontainers — never an external DATABASE_URL, never t.Skip).
func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return outboxDB(t)
}

func TestTaskRepo_SaveAndFindById(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewTaskRepo(pool)

	id := shared.TaskId("integration-task-1")
	cpt := shared.NewCPT(time.Now().Add(time.Hour).Truncate(time.Microsecond))
	tk := task.New(id, task.Pick, cpt, "order-1", shared.NewCapabilitySet("pick"), true, true)

	if err := repo.Save(context.Background(), tk); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.FindById(context.Background(), id)
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if got == nil {
		t.Fatalf("expected task to be found")
	}
	if got.Status() != task.Pending {
		t.Fatalf("expected Pending, got %s", got.Status())
	}
	if !got.Fragile() {
		t.Fatalf("expected round-tripped Fragile() to be true")
	}
	if !got.GiftWrap() {
		t.Fatalf("expected round-tripped GiftWrap() to be true")
	}
}

// TaskRepo's query specific to this repo: FindClaimableByType, which backs
// PULL dispatch (ClaimNext) and must return only Pending/lease-expired tasks
// of the requested type, earliest CPT first.
func TestTaskRepo_FindClaimableByType_OrdersByEarliestCPT(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewTaskRepo(pool)
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)

	later := task.New("integration-task-claimable-later", task.Pick, shared.NewCPT(now.Add(2*time.Hour)), "order-later", shared.NewCapabilitySet("pick"), false, false)
	earlier := task.New("integration-task-claimable-earlier", task.Pick, shared.NewCPT(now.Add(time.Hour)), "order-earlier", shared.NewCapabilitySet("pick"), false, false)
	wrongType := task.New("integration-task-claimable-pack", task.Pack, shared.NewCPT(now.Add(30*time.Minute)), "order-pack", shared.NewCapabilitySet("pack"), false, false)
	if err := repo.Save(ctx, later); err != nil {
		t.Fatalf("Save(later): %v", err)
	}
	if err := repo.Save(ctx, earlier); err != nil {
		t.Fatalf("Save(earlier): %v", err)
	}
	if err := repo.Save(ctx, wrongType); err != nil {
		t.Fatalf("Save(wrongType): %v", err)
	}

	candidates, err := repo.FindClaimableByType(ctx, task.Pick, now)
	if err != nil {
		t.Fatalf("FindClaimableByType: %v", err)
	}
	var earlierIdx, laterIdx = -1, -1
	for i, c := range candidates {
		if c.Type() != task.Pick {
			t.Fatalf("expected only PICK tasks, got %s", c.Type())
		}
		switch c.Id() {
		case earlier.Id():
			earlierIdx = i
		case later.Id():
			laterIdx = i
		}
	}
	if earlierIdx == -1 || laterIdx == -1 {
		t.Fatalf("expected both seeded tasks to be claimable, got %d candidates", len(candidates))
	}
	if earlierIdx >= laterIdx {
		t.Fatalf("expected earlier-CPT task (idx %d) to sort before later-CPT task (idx %d)", earlierIdx, laterIdx)
	}
}

// FindOpenPastCPT (ADR-0025) must return Pending/Claimed tasks whose CPT is
// at or before now, but never a Completed one regardless of its CPT — real
// Postgres round-trip of the same status+CPT filter task.Task.IsCPTMissed
// applies in-process.
func TestTaskRepo_FindOpenPastCPT_ReturnsOnlyOpenTasksPastCPT(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewTaskRepo(pool)
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)

	overdue := task.New("integration-task-cpt-overdue", task.Pick, shared.NewCPT(now.Add(-time.Hour)), "order-overdue", shared.NewCapabilitySet("pick"), false, false)
	notYetDue := task.New("integration-task-cpt-not-due", task.Pick, shared.NewCPT(now.Add(time.Hour)), "order-not-due", shared.NewCapabilitySet("pick"), false, false)
	completedOverdue := task.New("integration-task-cpt-completed", task.Pick, shared.NewCPT(now.Add(-time.Hour)), "order-completed", shared.NewCapabilitySet("pick"), false, false)
	if err := completedOverdue.Claim("s1", shared.NewCapabilitySet("pick"), now.Add(-2*time.Hour), time.Hour); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := completedOverdue.Complete("s1", now.Add(-90*time.Minute)); err != nil {
		t.Fatalf("complete: %v", err)
	}
	for _, tk := range []*task.Task{overdue, notYetDue, completedOverdue} {
		if err := repo.Save(ctx, tk); err != nil {
			t.Fatalf("Save(%s): %v", tk.Id(), err)
		}
	}

	got, err := repo.FindOpenPastCPT(ctx, now)
	if err != nil {
		t.Fatalf("FindOpenPastCPT: %v", err)
	}
	var sawOverdue, sawNotYetDue, sawCompleted bool
	for _, tk := range got {
		switch tk.Id() {
		case overdue.Id():
			sawOverdue = true
		case notYetDue.Id():
			sawNotYetDue = true
		case completedOverdue.Id():
			sawCompleted = true
		}
	}
	if !sawOverdue {
		t.Fatalf("expected the overdue Pending task to be returned, got %d results", len(got))
	}
	if sawNotYetDue {
		t.Fatalf("expected the not-yet-due task to be excluded")
	}
	if sawCompleted {
		t.Fatalf("expected the Completed (but overdue) task to be excluded")
	}
}

func TestStationRepo_SaveAndFindById(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewStationRepo(pool)

	id := shared.StationId("integration-station-1")
	s := station.New(id, shared.NewCapabilitySet("pick"))

	if err := repo.Save(context.Background(), s); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.FindById(context.Background(), id)
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if got == nil {
		t.Fatalf("expected station to be found")
	}
	if !got.CanAccept(shared.NewCapabilitySet("pick")) {
		t.Fatalf("expected round-tripped station to keep its capabilities")
	}
}

// TestStationRepo_SaveAndFindById_WithLocationCode proves the new
// location_code column (ADR-0024) round-trips against real Postgres, both
// when set and when left empty.
func TestStationRepo_SaveAndFindById_WithLocationCode(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewStationRepo(pool)

	id := shared.StationId("integration-station-location")
	s := station.New(id, shared.NewCapabilitySet("pack"))
	s.SetLocationCode("WH1-STOR-AMB-A07-01-01-A")

	if err := repo.Save(context.Background(), s); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.FindById(context.Background(), id)
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if got == nil {
		t.Fatalf("expected station to be found")
	}
	if got.LocationCode() != "WH1-STOR-AMB-A07-01-01-A" {
		t.Fatalf("expected round-tripped LocationCode, got %q", got.LocationCode())
	}
}

func TestStationRepo_SaveAndFindById_EmptyLocationCode(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewStationRepo(pool)

	id := shared.StationId("integration-station-no-location")
	s := station.New(id, shared.NewCapabilitySet("pick"))

	if err := repo.Save(context.Background(), s); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.FindById(context.Background(), id)
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if got.LocationCode() != "" {
		t.Fatalf("expected empty LocationCode, got %q", got.LocationCode())
	}
}

// TestStationRepo_CountByCapability proves the real Postgres
// ANY(capabilities) containment query against actual rows, not just the
// in-memory adapter's Go-side filter — the two must agree, since
// GetInstalledCapacity's behavior depends on whichever one is wired in
// production.
func TestStationRepo_CountByCapability(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewStationRepo(pool)
	ctx := context.Background()

	capability := shared.Capability(fmt.Sprintf("integration-cap-%d", time.Now().UnixNano()))
	otherCapability := shared.Capability(fmt.Sprintf("integration-other-cap-%d", time.Now().UnixNano()))

	if err := repo.Save(ctx, station.New(shared.StationId("cap-station-1"), shared.NewCapabilitySet(capability))); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := repo.Save(ctx, station.New(shared.StationId("cap-station-2"), shared.NewCapabilitySet(capability, otherCapability))); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := repo.Save(ctx, station.New(shared.StationId("cap-station-3"), shared.NewCapabilitySet(otherCapability))); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := repo.CountByCapability(ctx, capability)
	if err != nil {
		t.Fatalf("CountByCapability: %v", err)
	}
	if got != 2 {
		t.Fatalf("expected 2 stations with capability %q, got %d", capability, got)
	}

	got, err = repo.CountByCapability(ctx, shared.Capability("no-such-capability"))
	if err != nil {
		t.Fatalf("CountByCapability: %v", err)
	}
	if got != 0 {
		t.Fatalf("expected 0 stations for an unregistered capability, got %d", got)
	}
}

func TestPackageRepo_SaveAndFindById(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewPackageRepo(pool)
	ctx := context.Background()

	id := shared.PackageId("integration-package-1")
	p := pack.New(id, "order-1", "task-1", true, true)
	if err := p.ScanItemWithClass("sku-1", 3); err != nil {
		t.Fatalf("ScanItemWithClass: %v", err)
	}
	if err := p.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	if err := repo.Save(ctx, p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.FindById(ctx, id)
	if err != nil {
		t.Fatalf("FindById: %v", err)
	}
	if got == nil {
		t.Fatalf("expected package to be found")
	}
	if got.Status() != pack.Sealed {
		t.Fatalf("expected Sealed, got %s", got.Status())
	}
	if len(got.ScannedContents()) != 1 || got.ScannedContents()[0] != "sku-1" {
		t.Fatalf("expected scanned contents [sku-1], got %v", got.ScannedContents())
	}
	if !got.FragileHandling() {
		t.Fatalf("expected round-tripped FragileHandling() to be true")
	}
	if !got.GiftWrapRequested() {
		t.Fatalf("expected round-tripped GiftWrapRequested() to be true")
	}
	if len(got.ScannedHazardClasses()) != 1 || got.ScannedHazardClasses()[0] != 3 {
		t.Fatalf("expected round-tripped ScannedHazardClasses() [3], got %v", got.ScannedHazardClasses())
	}
	if got.SortLane() != pack.SortLaneHazmat {
		t.Fatalf("expected SortLane() HAZMAT_LANE, got %s", got.SortLane())
	}

	// Package-specific: the SLAM weigh-check outcome (label vs. divert) must
	// round-trip through an update to the same row.
	if _, err := got.Weigh(2.0, 2.5); err != nil {
		t.Fatalf("Weigh: %v", err)
	}
	if err := repo.Save(ctx, got); err != nil {
		t.Fatalf("Save (post-weigh): %v", err)
	}
	reloaded, err := repo.FindById(ctx, id)
	if err != nil {
		t.Fatalf("FindById (post-weigh): %v", err)
	}
	if reloaded.Status() != pack.Diverted {
		t.Fatalf("expected Diverted after weigh-check discrepancy, got %s", reloaded.Status())
	}
}

// TaskRepo's other query specific to this repo: FindByOrderRef, which backs
// GET /tasks?orderRef= — must return every task recorded for an order,
// including retried legs, and an unknown orderRef must return an empty
// slice rather than an error.
func TestTaskRepo_FindByOrderRef_ReturnsEveryMatchingTask(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewTaskRepo(pool)
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)

	pick := task.New("integration-task-orderref-pick", task.Pick, shared.NewCPT(now.Add(time.Hour)), "integration-order-ref-1", shared.NewCapabilitySet("pick"), false, false)
	retriedPick := task.New("integration-task-orderref-pick-retry", task.Pick, shared.NewCPT(now.Add(90*time.Minute)), "integration-order-ref-1", shared.NewCapabilitySet("pick"), false, false)
	pack := task.New("integration-task-orderref-pack", task.Pack, shared.NewCPT(now.Add(2*time.Hour)), "integration-order-ref-1", shared.NewCapabilitySet("pack"), false, false)
	otherOrder := task.New("integration-task-orderref-other", task.Pick, shared.NewCPT(now.Add(time.Hour)), "integration-order-ref-2", shared.NewCapabilitySet("pick"), false, false)
	for _, tk := range []*task.Task{pick, retriedPick, pack, otherOrder} {
		if err := repo.Save(ctx, tk); err != nil {
			t.Fatalf("Save(%s): %v", tk.Id(), err)
		}
	}

	got, err := repo.FindByOrderRef(ctx, "integration-order-ref-1")
	if err != nil {
		t.Fatalf("FindByOrderRef: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 tasks for integration-order-ref-1 (including retried leg), got %d", len(got))
	}
	for _, tk := range got {
		if tk.OrderRef() != "integration-order-ref-1" {
			t.Fatalf("expected only integration-order-ref-1 tasks, got orderRef %q", tk.OrderRef())
		}
	}

	empty, err := repo.FindByOrderRef(ctx, "integration-order-ref-does-not-exist")
	if err != nil {
		t.Fatalf("FindByOrderRef (unknown): %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected empty result for unknown orderRef, got %d", len(empty))
	}
}

// ProcessedEventsRepo's query specific to this repo: MarkProcessed must be
// idempotent so an at-least-once source (Kafka) is consumed exactly once.
func TestProcessedEventsRepo_MarkProcessed_IsIdempotent(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewProcessedEventsRepo(pool)
	ctx := context.Background()
	eventId := "integration-event-1"
	// Clean up any leftover row from a prior run of this test so the
	// idempotency check below starts from a known "not yet processed" state.
	if _, err := pool.Exec(ctx, `DELETE FROM processed_events WHERE event_id = $1`, eventId); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	first, err := repo.MarkProcessed(ctx, eventId)
	if err != nil {
		t.Fatalf("MarkProcessed (first): %v", err)
	}
	if !first {
		t.Fatalf("expected first MarkProcessed call to return true")
	}

	second, err := repo.MarkProcessed(ctx, eventId)
	if err != nil {
		t.Fatalf("MarkProcessed (second): %v", err)
	}
	if second {
		t.Fatalf("expected second MarkProcessed call for the same event id to return false")
	}
}

// OrderConsolidationRepo's round-trip: Save then FindByOrderRef must
// preserve both the required and arrived line sets, and an unknown
// orderRef must return (nil, nil) rather than an error.
func TestOrderConsolidationRepo_SaveAndFindByOrderRef(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewOrderConsolidationRepo(pool)
	ctx := context.Background()

	oc := consolidation.New("integration-order-consolidation-1", []string{"line-1", "line-2"})
	if err := oc.RecordArrival("line-1"); err != nil {
		t.Fatalf("RecordArrival: %v", err)
	}
	if err := repo.Save(ctx, oc); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := repo.FindByOrderRef(ctx, "integration-order-consolidation-1")
	if err != nil {
		t.Fatalf("FindByOrderRef: %v", err)
	}
	if got == nil {
		t.Fatalf("expected to find the saved consolidation")
	}
	if got.IsComplete() {
		t.Fatalf("expected incomplete: only one of two required lines arrived")
	}
	if len(got.RequiredLineIds()) != 2 {
		t.Fatalf("expected 2 required lines to round-trip, got %v", got.RequiredLineIds())
	}
	if len(got.ArrivedLineIds()) != 1 || got.ArrivedLineIds()[0] != "line-1" {
		t.Fatalf("expected arrived lines [line-1] to round-trip, got %v", got.ArrivedLineIds())
	}

	// Recording the second arrival and re-saving must update the same
	// row (ON CONFLICT DO UPDATE), not create a duplicate.
	if err := got.RecordArrival("line-2"); err != nil {
		t.Fatalf("RecordArrival (line-2): %v", err)
	}
	if err := repo.Save(ctx, got); err != nil {
		t.Fatalf("Save (post-completion): %v", err)
	}
	reloaded, err := repo.FindByOrderRef(ctx, "integration-order-consolidation-1")
	if err != nil {
		t.Fatalf("FindByOrderRef (post-completion): %v", err)
	}
	if !reloaded.IsComplete() {
		t.Fatalf("expected complete after both lines arrived and were re-saved")
	}

	unknown, err := repo.FindByOrderRef(ctx, "integration-order-consolidation-does-not-exist")
	if err != nil {
		t.Fatalf("FindByOrderRef (unknown): %v", err)
	}
	if unknown != nil {
		t.Fatalf("expected nil for an unknown orderRef, got %+v", unknown)
	}
}
