package pack_test

import (
	"errors"
	"testing"
	"time"

	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
)

var (
	t0      = time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	caseDim = pack.Dimensions{LengthMM: 400, WidthMM: 300, HeightMM: 200, WeightG: 6500}
)

func caseOf(units int) pack.CaseLevel {
	d := caseDim
	return pack.CaseLevel{GTIN: "24006381333935", UnitsPerPack: units, Dims: &d, Version: 3, EffectiveFrom: t0.Add(-time.Hour)}
}

// The line must be EXACTLY one case: one each short, one each over, zero and
// a negative quantity are all refused; only unitsPerPack ships in the case.
func TestNewOwnContainerFromCase_QuantityBoundaries(t *testing.T) {
	const upp = 12
	tests := []struct {
		name string
		qty  int
		ok   bool
	}{
		{"zero", 0, false},
		{"negative", -upp, false},
		{"one each", 1, false},
		{"unitsPerPack minus one", upp - 1, false},
		{"unitsPerPack", upp, true},
		{"unitsPerPack plus one", upp + 1, false},
		{"two cases", 2 * upp, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oc, err := pack.NewOwnContainerFromCase(tt.qty, caseOf(upp))
			if tt.ok {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if oc.Source != pack.ContainerSourceCase || oc.GTIN != "24006381333935" || oc.Dims != caseDim {
					t.Fatalf("own container = %+v, want the case's source, GTIN and dims", oc)
				}
				return
			}
			if !errors.Is(err, pack.ErrOwnContainerQuantityMismatch) {
				t.Fatalf("err = %v, want ErrOwnContainerQuantityMismatch", err)
			}
		})
	}
}

// A single-unit case (unitsPerPack 1) is still a case: quantity 1 ships in it.
func TestNewOwnContainerFromCase_UnitsPerPackOne(t *testing.T) {
	if _, err := pack.NewOwnContainerFromCase(1, caseOf(1)); err != nil {
		t.Fatalf("qty 1 on a case of 1: %v", err)
	}
	if _, err := pack.NewOwnContainerFromCase(0, caseOf(1)); !errors.Is(err, pack.ErrOwnContainerQuantityMismatch) {
		t.Fatalf("qty 0 on a case of 1: %v, want ErrOwnContainerQuantityMismatch", err)
	}
	if _, err := pack.NewOwnContainerFromCase(2, caseOf(1)); !errors.Is(err, pack.ErrOwnContainerQuantityMismatch) {
		t.Fatalf("qty 2 on a case of 1: %v, want ErrOwnContainerQuantityMismatch", err)
	}
}

// Each of the four measures must be strictly positive; product-master
// declares dims optionally, so a case without them cannot ship by itself.
func TestNewOwnContainerFromCase_DimensionsMustAllBePositive(t *testing.T) {
	zero := func(f func(d *pack.Dimensions)) pack.CaseLevel {
		d := caseDim
		f(&d)
		c := caseOf(12)
		c.Dims = &d
		return c
	}
	tests := []struct {
		name  string
		level pack.CaseLevel
	}{
		{"no dims", func() pack.CaseLevel { c := caseOf(12); c.Dims = nil; return c }()},
		{"zero length", zero(func(d *pack.Dimensions) { d.LengthMM = 0 })},
		{"zero width", zero(func(d *pack.Dimensions) { d.WidthMM = 0 })},
		{"zero height", zero(func(d *pack.Dimensions) { d.HeightMM = 0 })},
		{"zero weight", zero(func(d *pack.Dimensions) { d.WeightG = 0 })},
		{"negative weight", zero(func(d *pack.Dimensions) { d.WeightG = -1 })},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := pack.NewOwnContainerFromCase(12, tt.level); !errors.Is(err, pack.ErrCaseDimensionsUnknown) {
				t.Fatalf("err = %v, want ErrCaseDimensionsUnknown", err)
			}
		})
	}
	// The minimal valid dims (1 everywhere) are accepted.
	c := caseOf(12)
	c.Dims = &pack.Dimensions{LengthMM: 1, WidthMM: 1, HeightMM: 1, WeightG: 1}
	if _, err := pack.NewOwnContainerFromCase(12, c); err != nil {
		t.Fatalf("minimal dims: %v", err)
	}
}

// The quantity is judged before the dimensions: a wrong quantity on a case
// with no dims is a quantity mismatch.
func TestNewOwnContainerFromCase_QuantityIsCheckedBeforeDimensions(t *testing.T) {
	c := caseOf(12)
	c.Dims = nil
	if _, err := pack.NewOwnContainerFromCase(11, c); !errors.Is(err, pack.ErrOwnContainerQuantityMismatch) {
		t.Fatalf("err = %v, want ErrOwnContainerQuantityMismatch", err)
	}
}

func TestResolveActiveCase(t *testing.T) {
	at := t0
	end := func(d time.Duration) *time.Time { e := at.Add(d); return &e }
	lvl := func(gtin string, version int64, from time.Duration, to *time.Time) pack.CaseLevel {
		c := caseOf(12)
		c.GTIN, c.Version, c.EffectiveFrom, c.EffectiveTo = gtin, version, at.Add(from), to
		return c
	}
	tests := []struct {
		name     string
		levels   []pack.CaseLevel
		wantGTIN string
		wantErr  error
	}{
		{"no levels", nil, "", pack.ErrNoCaseLevel},
		{"open-ended", []pack.CaseLevel{lvl("A", 1, -time.Hour, nil)}, "A", nil},
		{"starts exactly now is active", []pack.CaseLevel{lvl("A", 1, 0, nil)}, "A", nil},
		{"starts one nanosecond ahead is not active yet", []pack.CaseLevel{lvl("A", 1, time.Nanosecond, nil)}, "", pack.ErrNoCaseLevel},
		{"ends one nanosecond ahead is still active", []pack.CaseLevel{lvl("A", 1, -time.Hour, end(time.Nanosecond))}, "A", nil},
		{"ends exactly now is retired", []pack.CaseLevel{lvl("A", 1, -time.Hour, end(0))}, "", pack.ErrCaseLevelRetired},
		{"ended an hour ago is retired", []pack.CaseLevel{lvl("A", 1, -2*time.Hour, end(-time.Hour))}, "", pack.ErrCaseLevelRetired},
		{
			"superseded: the new level is active, the old one is retired",
			[]pack.CaseLevel{lvl("OLD", 2, -2*time.Hour, end(-time.Minute)), lvl("NEW", 3, -time.Minute, nil)},
			"NEW", nil,
		},
		{
			"superseded and the new level is not in effect yet: retired",
			[]pack.CaseLevel{lvl("OLD", 2, -2*time.Hour, end(-time.Minute)), lvl("NEW", 3, time.Hour, nil)},
			"", pack.ErrCaseLevelRetired,
		},
		{
			"overlap: the greater version wins whatever the order",
			[]pack.CaseLevel{lvl("NEW", 3, -time.Hour, nil), lvl("OLD", 2, -2*time.Hour, nil)},
			"NEW", nil,
		},
		{
			"overlap, the newer version listed last",
			[]pack.CaseLevel{lvl("OLD", 2, -2*time.Hour, nil), lvl("NEW", 3, -time.Hour, nil)},
			"NEW", nil,
		},
		{
			"only future-dated levels are not a retired case",
			[]pack.CaseLevel{lvl("A", 1, time.Hour, nil), lvl("B", 2, 2*time.Hour, nil)},
			"", pack.ErrNoCaseLevel,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := pack.ResolveActiveCase(tt.levels, at)
			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil && err != nil) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if got.GTIN != tt.wantGTIN {
				t.Fatalf("GTIN = %q, want %q", got.GTIN, tt.wantGTIN)
			}
		})
	}
}

func TestPackage_OwnContainer(t *testing.T) {
	p := newPackage()
	if p.OwnContainer() != nil {
		t.Fatal("a new package ships in no own container")
	}
	oc := pack.OwnContainer{Source: pack.ContainerSourceCase, GTIN: "24006381333935", Dims: caseDim}
	_ = p.ScanItem("sku-1")
	if err := p.WithOwnContainer(oc); err != nil {
		t.Fatalf("WithOwnContainer on an open package: %v", err)
	}
	if got := p.OwnContainer(); got == nil || *got != oc {
		t.Fatalf("OwnContainer() = %+v, want %+v", got, oc)
	}
	if err := p.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	if err := p.WithOwnContainer(oc); !errors.Is(err, pack.ErrAlreadySealed) {
		t.Fatalf("WithOwnContainer on a sealed package = %v, want ErrAlreadySealed", err)
	}
}

func TestRestoreOwnContainer(t *testing.T) {
	oc := pack.OwnContainer{Source: pack.ContainerSourceCase, GTIN: "24006381333935", Dims: caseDim}
	rehydrated := pack.Rehydrate("p1", "order-1", "task-1", pack.Labeled, []string{"sku-1"}, false, nil, false)
	if rehydrated.OwnContainer() != nil {
		t.Fatal("a rehydrated package has no own container unless restored")
	}
	if got := rehydrated.RestoreOwnContainer(&oc).OwnContainer(); got == nil || *got != oc {
		t.Fatalf("restored own container = %+v, want %+v", got, oc)
	}
}
