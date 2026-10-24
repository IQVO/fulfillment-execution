package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/packview"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

const siocCaseGTIN = "24006381333935"

// siocFixture is a claimed PACK task for order-1 and a seal use case wired to
// an in-memory PackView holding (optionally) a CASE level of "sku-1".
type siocFixture struct {
	*harness
	view   *packview.MemoryStore
	seal   *usecases.SealPackage
	taskID shared.TaskId
}

func newSIOCFixture(t *testing.T) *siocFixture {
	t.Helper()
	h := newHarness()
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: idSeq("t")}
	if _, err := create.Execute(ctx, task.Pack, shared.NewCPT(epoch.Add(time.Hour)), "order-1", shared.NewCapabilitySet("pack"), false, false); err != nil {
		t.Fatal(err)
	}
	_ = h.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack")))
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: h.stations, Publisher: h.publisher, Clock: h.clock}
	claimed, err := claim.Execute(ctx, "s1", task.Pack)
	if err != nil {
		t.Fatal(err)
	}
	view := packview.NewMemoryStore()
	return &siocFixture{
		harness: h,
		view:    view,
		taskID:  claimed.Id(),
		seal: &usecases.SealPackage{
			Tasks: h.tasks, Packages: h.packages, Publisher: h.publisher, Clock: h.clock,
			NewId: func() shared.PackageId { return "p1" }, CaseLevels: view,
		},
	}
}

func (f *siocFixture) declareCase(t *testing.T, rec ports.PackRecord) {
	t.Helper()
	rec.SKU, rec.Level = "sku-1", task.PackLevelCase
	if rec.GTIN == "" {
		rec.GTIN = siocCaseGTIN
	}
	if rec.Version == 0 {
		rec.Version = 1
	}
	if rec.EffectiveFrom.IsZero() {
		rec.EffectiveFrom = epoch.Add(-time.Hour)
	}
	// An upsert clears effective_to (a re-declaration reopens the level), so
	// an ended level is declared and then retired, as the events arrive.
	to := rec.EffectiveTo
	rec.EffectiveTo = nil
	if _, err := f.view.UpsertIfNewer(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if to != nil {
		if _, err := f.view.RetireIfNewer(context.Background(), rec.GTIN, *to, rec.Version+1); err != nil {
			t.Fatal(err)
		}
	}
}

var caseDims = &ports.PackDims{LengthMM: 400, WidthMM: 300, HeightMM: 200, WeightG: 6500}

func (f *siocFixture) sealedEvents() []shared.PackageSealed {
	var out []shared.PackageSealed
	for _, e := range f.publisher.Events() {
		if s, ok := e.(shared.PackageSealed); ok {
			out = append(out, s)
		}
	}
	return out
}

// The line is exactly one active case: the package ships in the case (its
// GTIN, dimensions and weight), nothing is cartonised, and the sealed event
// says so.
func TestSealPackage_OwnContainer_ExactlyOneCaseShipsInTheCase(t *testing.T) {
	f := newSIOCFixture(t)
	f.declareCase(t, ports.PackRecord{UnitsPerPack: 12, Dims: caseDims})

	p, err := f.seal.ExecuteInOwnContainer(context.Background(), f.taskID, "s1", []string{"sku-1"}, 12)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	oc := p.OwnContainer()
	want := pack.OwnContainer{Source: pack.ContainerSourceCase, GTIN: siocCaseGTIN, Dims: pack.Dimensions{LengthMM: 400, WidthMM: 300, HeightMM: 200, WeightG: 6500}}
	if oc == nil || *oc != want {
		t.Fatalf("own container = %+v, want %+v", oc, want)
	}
	if p.Status() != pack.Sealed {
		t.Fatalf("status = %s, want SEALED", p.Status())
	}
	evs := f.sealedEvents()
	if len(evs) != 1 || evs[0].OwnContainer == nil {
		t.Fatalf("PackageSealed events = %+v, want one carrying the own container", evs)
	}
	got := *evs[0].OwnContainer
	if got != (shared.OwnContainerFact{Source: "CASE", GTIN: siocCaseGTIN, LengthMM: 400, WidthMM: 300, HeightMM: 200, WeightG: 6500}) {
		t.Fatalf("event own container = %+v", got)
	}
	if !evs[0].OccurredAt().Equal(epoch) {
		t.Fatalf("event time = %v, want the clock's %v", evs[0].OccurredAt(), epoch)
	}
}

// Boundary quantities around one full case are refused and seal nothing.
func TestSealPackage_OwnContainer_QuantityBoundaries(t *testing.T) {
	for _, qty := range []int{0, 1, 11, 13, 24} {
		f := newSIOCFixture(t)
		f.declareCase(t, ports.PackRecord{UnitsPerPack: 12, Dims: caseDims})
		_, err := f.seal.ExecuteInOwnContainer(context.Background(), f.taskID, "s1", []string{"sku-1"}, qty)
		if !errors.Is(err, pack.ErrOwnContainerQuantityMismatch) {
			t.Fatalf("qty %d: err = %v, want ErrOwnContainerQuantityMismatch", qty, err)
		}
		if got, _ := f.packages.FindByTaskId(context.Background(), f.taskID); got != nil {
			t.Fatalf("qty %d: a package was saved for a refused seal", qty)
		}
		if n := len(f.sealedEvents()); n != 0 {
			t.Fatalf("qty %d: %d PackageSealed events for a refused seal", qty, n)
		}
	}
}

func TestSealPackage_OwnContainer_Refusals(t *testing.T) {
	retiredAt := epoch.Add(-time.Minute)
	tests := []struct {
		name     string
		record   *ports.PackRecord
		contents []string
		nilView  bool
		want     error
	}{
		{"unknown SKU: no case level", nil, []string{"sku-1"}, false, pack.ErrNoCaseLevel},
		{"no PackView wired: no case level", nil, []string{"sku-1"}, true, pack.ErrNoCaseLevel},
		{"retired case", &ports.PackRecord{UnitsPerPack: 12, Dims: caseDims, EffectiveTo: &retiredAt}, []string{"sku-1"}, false, pack.ErrCaseLevelRetired},
		{"case without dims", &ports.PackRecord{UnitsPerPack: 12}, []string{"sku-1"}, false, pack.ErrCaseDimensionsUnknown},
		{"two lines", &ports.PackRecord{UnitsPerPack: 12, Dims: caseDims}, []string{"sku-1", "sku-2"}, false, pack.ErrNotSingleLine},
		{"two scans of one SKU", &ports.PackRecord{UnitsPerPack: 12, Dims: caseDims}, []string{"sku-1", "sku-1"}, false, pack.ErrNotSingleLine},
		{"no contents", &ports.PackRecord{UnitsPerPack: 12, Dims: caseDims}, nil, false, pack.ErrNoScannedContents},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSIOCFixture(t)
			if tt.record != nil {
				f.declareCase(t, *tt.record)
			}
			if tt.nilView {
				f.seal.CaseLevels = nil
			}
			_, err := f.seal.ExecuteInOwnContainer(context.Background(), f.taskID, "s1", tt.contents, 12)
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if got, _ := f.packages.FindByTaskId(context.Background(), f.taskID); got != nil {
				t.Fatal("a package was saved for a refused seal")
			}
			if n := len(f.sealedEvents()); n != 0 {
				t.Fatalf("%d PackageSealed events for a refused seal", n)
			}
		})
	}
}

// A level that ended exactly now is retired; one that ends a moment later is
// still the active case.
func TestSealPackage_OwnContainer_RetirementBoundary(t *testing.T) {
	endsNow, endsLater := epoch, epoch.Add(time.Nanosecond)

	f := newSIOCFixture(t)
	f.declareCase(t, ports.PackRecord{UnitsPerPack: 12, Dims: caseDims, EffectiveTo: &endsNow})
	if _, err := f.seal.ExecuteInOwnContainer(context.Background(), f.taskID, "s1", []string{"sku-1"}, 12); !errors.Is(err, pack.ErrCaseLevelRetired) {
		t.Fatalf("level ended now: err = %v, want ErrCaseLevelRetired", err)
	}

	f = newSIOCFixture(t)
	f.declareCase(t, ports.PackRecord{UnitsPerPack: 12, Dims: caseDims, EffectiveTo: &endsLater})
	if _, err := f.seal.ExecuteInOwnContainer(context.Background(), f.taskID, "s1", []string{"sku-1"}, 12); err != nil {
		t.Fatalf("level ending a nanosecond later: %v", err)
	}
}

// A superseded case: the old level (retired) and the new one (other
// unitsPerPack) coexist in the view; only the new one counts.
func TestSealPackage_OwnContainer_UsesTheSupersedingCase(t *testing.T) {
	f := newSIOCFixture(t)
	oldEnd := epoch.Add(-time.Minute)
	f.declareCase(t, ports.PackRecord{GTIN: "00000000000017", UnitsPerPack: 6, Dims: caseDims, Version: 1, EffectiveTo: &oldEnd})
	f.declareCase(t, ports.PackRecord{GTIN: siocCaseGTIN, UnitsPerPack: 12, Dims: caseDims, Version: 3})

	if _, err := f.seal.ExecuteInOwnContainer(context.Background(), f.taskID, "s1", []string{"sku-1"}, 6); !errors.Is(err, pack.ErrOwnContainerQuantityMismatch) {
		t.Fatalf("a full OLD case: err = %v, want ErrOwnContainerQuantityMismatch", err)
	}
	p, err := f.seal.ExecuteInOwnContainer(context.Background(), f.taskID, "s1", []string{"sku-1"}, 12)
	if err != nil {
		t.Fatalf("a full new case: %v", err)
	}
	if p.OwnContainer().GTIN != siocCaseGTIN {
		t.Fatalf("container GTIN = %s, want the new case %s", p.OwnContainer().GTIN, siocCaseGTIN)
	}
}

// The guards that already protect the ordinary seal also protect the own
// container: a wrong station and a non-Pack task are refused before the
// PackView is read.
func TestSealPackage_OwnContainer_KeepsTheClaimGuards(t *testing.T) {
	f := newSIOCFixture(t)
	f.declareCase(t, ports.PackRecord{UnitsPerPack: 12, Dims: caseDims})
	if _, err := f.seal.ExecuteInOwnContainer(context.Background(), f.taskID, "other", []string{"sku-1"}, 12); !errors.Is(err, task.ErrNotOwner) {
		t.Fatalf("other station: err = %v, want ErrNotOwner", err)
	}
	if _, err := f.seal.ExecuteInOwnContainer(context.Background(), "ghost", "s1", []string{"sku-1"}, 12); !errors.Is(err, usecases.ErrTaskNotFound) {
		t.Fatalf("unknown task: err = %v, want ErrTaskNotFound", err)
	}
	f.clock.Advance(6 * time.Minute)
	if _, err := f.seal.ExecuteInOwnContainer(context.Background(), f.taskID, "s1", []string{"sku-1"}, 12); !errors.Is(err, task.ErrNotClaimed) {
		t.Fatalf("expired lease: err = %v, want ErrNotClaimed", err)
	}
}

// A retried own-container seal returns the package already sealed for the
// task (idempotent on taskId) and publishes no second PackageSealed.
func TestSealPackage_OwnContainer_RetryReturnsExistingPackage(t *testing.T) {
	f := newSIOCFixture(t)
	f.declareCase(t, ports.PackRecord{UnitsPerPack: 12, Dims: caseDims})
	first, err := f.seal.ExecuteInOwnContainer(context.Background(), f.taskID, "s1", []string{"sku-1"}, 12)
	if err != nil {
		t.Fatal(err)
	}
	again, err := f.seal.ExecuteInOwnContainer(context.Background(), f.taskID, "s1", []string{"sku-1"}, 12)
	if err != nil || again.Id() != first.Id() || again.OwnContainer() == nil {
		t.Fatalf("retry = %+v, %v; want the first package %s", again, err, first.Id())
	}
	if n := len(f.sealedEvents()); n != 1 {
		t.Fatalf("%d PackageSealed events after a retry, want 1", n)
	}
}

// The ordinary seal is untouched by the feature: no own container, the plain
// PackageSealed event, and no PackView read even when one is wired.
func TestSealPackage_OrdinaryPathIsUnchangedByOwnContainer(t *testing.T) {
	f := newSIOCFixture(t)
	f.declareCase(t, ports.PackRecord{UnitsPerPack: 12, Dims: caseDims})
	f.seal.CaseLevels = failingCaseLevels{}

	p, err := f.seal.Execute(context.Background(), f.taskID, "s1", []string{"sku-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.OwnContainer() != nil {
		t.Fatalf("ordinary seal has an own container: %+v", p.OwnContainer())
	}
	evs := f.sealedEvents()
	if len(evs) != 1 || evs[0].OwnContainer != nil {
		t.Fatalf("events = %+v, want one plain PackageSealed", evs)
	}
}

// A PackView read failure on the own-container path is surfaced (500), not
// mistaken for "no case".
func TestSealPackage_OwnContainer_LookupFailureIsSurfaced(t *testing.T) {
	f := newSIOCFixture(t)
	f.seal.CaseLevels = failingCaseLevels{}
	_, err := f.seal.ExecuteInOwnContainer(context.Background(), f.taskID, "s1", []string{"sku-1"}, 12)
	if !errors.Is(err, errCaseLevels) {
		t.Fatalf("err = %v, want the lookup error", err)
	}
}

var errCaseLevels = errors.New("pack view unavailable")

type failingCaseLevels struct{}

func (failingCaseLevels) FindCaseLevels(context.Context, string) ([]ports.PackRecord, error) {
	return nil, errCaseLevels
}
