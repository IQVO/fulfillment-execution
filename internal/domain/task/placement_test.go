package task_test

import (
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

const (
	binFwd = "WH1-FWD-AMB-A01-01-01-A"
	binRsv = "WH1-RSV-AMB-B09-02-03-B"
)

func relocationWith(t *testing.T, kind, from, to string) *task.Task {
	t.Helper()
	return task.NewRelocationTask("r1", shared.NewCPT(now.Add(time.Hour)), "wu", shared.NewCapabilitySet("relocate"), mustMove(t, kind, from, to))
}

func TestPlacement_RelocateUsesBothPlannedBins(t *testing.T) {
	p, ok := relocationWith(t, "Relocate", binRsv, binFwd).Placement()
	want := task.Placement{SiteId: "WH1", FromLocation: binRsv, ToLocation: binFwd}
	if !ok || p != want {
		t.Fatalf("Placement() = (%+v, %v), want (%+v, true)", p, ok, want)
	}
}

// An Assign leaves the source open and a Vacate the destination (scanned at
// completion): the known bin stands for both ends of the trip.
func TestPlacement_OpenSideFallsBackToTheKnownBin(t *testing.T) {
	assign, ok := relocationWith(t, "Assign", "", binFwd).Placement()
	if !ok || assign.FromLocation != binFwd || assign.ToLocation != binFwd || assign.SiteId != "WH1" {
		t.Fatalf("Assign placement = (%+v, %v)", assign, ok)
	}
	vacate, ok := relocationWith(t, "Vacate", binFwd, "").Placement()
	if !ok || vacate.FromLocation != binFwd || vacate.ToLocation != binFwd || vacate.SiteId != "WH1" {
		t.Fatalf("Vacate placement = (%+v, %v)", vacate, ok)
	}
}

// The site comes from the first bin that is a well-formed LocationCode.
func TestPlacement_SiteFallsBackToTheDestinationBin(t *testing.T) {
	p, ok := relocationWith(t, "Relocate", "BIN-X", binFwd).Placement()
	if !ok || p.SiteId != "WH1" || p.FromLocation != "BIN-X" || p.ToLocation != binFwd {
		t.Fatalf("Placement() = (%+v, %v)", p, ok)
	}
}

// No determinable site: no placement (fail closed, nothing announced).
func TestPlacement_NoSiteMeansNoPlacement(t *testing.T) {
	if p, ok := relocationWith(t, "Relocate", "BIN-A", "BIN-B").Placement(); ok {
		t.Fatalf("Placement() = (%+v, true), want none for bins that are not location codes", p)
	}
}

// Only a RELOCATE or REPLENISH task carries locations through its move, and
// a PICK only through a sited plan (ADR 0062); a bare task of any other type
// has none.
func TestPlacement_NonRelocateTasksHaveNone(t *testing.T) {
	for _, typ := range []task.Type{task.Pick, task.Pack, task.Slam, task.Rebin, task.Dispatch, task.Arrival} {
		tk := task.New("t1", typ, shared.NewCPT(now), "wu", shared.NewCapabilitySet(), false, false)
		if p, ok := tk.Placement(); ok {
			t.Errorf("%s: Placement() = (%+v, true), want none", typ, p)
		}
	}
}

// pickWith is a PICK task carrying a plan with the given site and pick face
// (either may be empty, the omit-when-unknown release forms).
func pickWith(t *testing.T, site, fromBin string) *task.Task {
	t.Helper()
	plan, err := task.NewPickPlan("SKU-1", 2, "EACH", site, fromBin)
	if err != nil {
		t.Fatalf("NewPickPlan: %v", err)
	}
	return task.New("p1", task.Pick, shared.NewCPT(now), "wu-p1", shared.NewCapabilitySet("pick"), false, false).WithPickPlan(&plan, 0)
}

// A PICK whose plan carries both the site and the pick face is announced
// (ADR 0062): the site is the plan's own site, the pick face is where the
// operator goes. The station they end at is not known at announce time, so
// the pick face stands for both ends of the trip, exactly like the one
// known bin of a RELOCATE Assign or Vacate.
func TestPlacement_PickWithSiteAndBin(t *testing.T) {
	p, ok := pickWith(t, "WH1", binFwd).Placement()
	if !ok || p.SiteId != "WH1" || p.FromLocation != binFwd || p.ToLocation != binFwd {
		t.Fatalf("Placement() = (%+v, %v), want site WH1 and both ends %s", p, ok, binFwd)
	}
}

// Omit-when-unknown, exactly like a RELOCATE whose bins carry no site: a
// pick without a pick face has no from_location, and the pool consumer
// rejects such an event — so it is not announced. It can only ever be a
// batch anchor, never a companion.
func TestPlacement_PickWithoutBinIsNotAnnounced(t *testing.T) {
	if p, ok := pickWith(t, "WH1", "").Placement(); ok {
		t.Fatalf("Placement() = (%+v, true), want none for a pick with no pick face", p)
	}
}

// Same rule for the site: a pick face without a site cannot be placed
// (the bin of a pick plan is not required to be a facility-layout
// LocationCode, so the site is never derived from it).
func TestPlacement_PickWithoutSiteIsNotAnnounced(t *testing.T) {
	if p, ok := pickWith(t, "", binFwd).Placement(); ok {
		t.Fatalf("Placement() = (%+v, true), want none for a pick with no site", p)
	}
}

func TestPriorityBand_PeersShareABandAndNothingElseDoes(t *testing.T) {
	at := time.Date(2026, 10, 17, 14, 0, 0, 0, time.UTC)
	mk := func(typ task.Type, cpt time.Time) *task.Task {
		return task.New("t", typ, shared.NewCPT(cpt), "wu", shared.NewCapabilitySet(), false, false)
	}
	a := mk(task.Pick, at)
	sameInstantOtherZone := mk(task.Pick, at.In(time.FixedZone("BRT", -3*3600)))
	if a.PriorityBand() != sameInstantOtherZone.PriorityBand() {
		t.Fatalf("one instant, two zones: bands %q != %q", a.PriorityBand(), sameInstantOtherZone.PriorityBand())
	}
	if later := mk(task.Pick, at.Add(time.Nanosecond)); later.PriorityBand() == a.PriorityBand() {
		t.Fatalf("strictly ordered tasks share band %q", a.PriorityBand())
	}
	if otherType := mk(task.Pack, at); otherType.PriorityBand() == a.PriorityBand() {
		t.Fatalf("tasks of different types (never compared by claimNext) share band %q", a.PriorityBand())
	}
	if want := "PICK:2026-10-17T14:00:00Z"; a.PriorityBand() != want {
		t.Fatalf("PriorityBand() = %q, want %q", a.PriorityBand(), want)
	}
}

func TestRequiredCapabilityNames_SortedAndNeverNil(t *testing.T) {
	tk := task.New("t", task.Pick, shared.NewCPT(now), "wu", shared.NewCapabilitySet("pick", "hazmat"), false, false)
	got := tk.RequiredCapabilityNames()
	if len(got) != 2 || got[0] != "hazmat" || got[1] != "pick" {
		t.Fatalf("RequiredCapabilityNames() = %v, want [hazmat pick]", got)
	}
	none := task.New("t", task.Pick, shared.NewCPT(now), "wu", shared.NewCapabilitySet(), false, false)
	if got := none.RequiredCapabilityNames(); got == nil || len(got) != 0 {
		t.Fatalf("RequiredCapabilityNames() = %#v, want an empty non-nil slice", got)
	}
}
