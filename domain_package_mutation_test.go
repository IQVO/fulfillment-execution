package main_test

// Mutation-anchor tests for internal/domain/package (Go package name `pack`).
//
// WHY THESE LIVE IN THE MODULE ROOT, NOT NEXT TO package.go
//
// gremlins v0.6.0 decides which `go test` target to run for a mutant by
// walking up from the mutated file's directory until a path segment ends
// with the Go package NAME declared in that file. This package's directory
// is `package` but its declared name is `pack` (`package` is a Go keyword),
// so the walk never matches and gremlins falls back to the module root
// package, i.e. it runs `go test github.com/claudioed/fulfillment-execution`
// for every mutant in internal/domain/package/*.go. The unit tests in
// internal/domain/package/package_test.go are therefore never executed
// against those mutants, and only this root package's tests (the BDD suite
// plus this file) can kill them. That is the cause of the scheduled
// `mutation` job's LIVED mutants in package.go and segregation.go.
//
// Every test here drives the exported domain API with exact boundary values
// and both polarities of each condition, so that flipping any comparison
// (>, >=, <, <=, !=, ==) in ScanItemWithClass, SortLane, Weigh or
// IsSegregationIncompatible fails at least one case.

import (
	"errors"
	"fmt"
	"math"
	"testing"

	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

func newMutPackage(fragile bool) *pack.Package {
	return pack.New(shared.PackageId("mut-p1"), shared.OrderRef("mut-order-1"), shared.TaskId("mut-task-1"), fragile, false)
}

// wantIncompatible is an independently hand-written copy of the expected
// class-level matrix (rows/cols 1..9, "X" = incompatible). It deliberately
// does not read the production matrix, so a mutant cannot make expectation
// and implementation drift together.
var wantIncompatible = [9]string{
	"XXXXXXXXX", // class 1
	"X.XXXXXX.", // class 2
	"XX..XX...", // class 3
	"XX...X.X.", // class 4
	"XXX..X.X.", // class 5
	"XXXXX..X.", // class 6
	"XX.......", // class 7
	"XX.XXX...", // class 8
	"X........", // class 9
}

func TestMutAnchor_IsSegregationIncompatible_RangeBoundaries(t *testing.T) {
	// 1 and 9 are the first/last valid classes; 0 and 10 the first
	// out-of-range values. Each argument is exercised at each boundary on
	// its own, with the other argument held at a valid class that is
	// incompatible with it (class 1 pairs with everything), so that a
	// mis-bounded guard returns the wrong answer.
	cases := []struct {
		name string
		a, b int
		want bool
	}{
		{"a=1 lower bound valid", 1, 2, true},
		{"b=1 lower bound valid", 2, 1, true},
		{"a=9 upper bound valid", 9, 1, true},
		{"b=9 upper bound valid", 1, 9, true},
		{"a=1 b=1", 1, 1, true},
		{"a=0 below range", 0, 1, false},
		{"b=0 below range", 1, 0, false},
		{"a=10 above range", 10, 1, false},
		{"b=10 above range", 1, 10, false},
		{"a=-1", -1, 1, false},
		{"b=-1", 1, -1, false},
		{"both out of range", 0, 10, false},
		{"mid-range incompatible", 3, 5, true},
		{"mid-range compatible", 3, 8, false},
		{"9 with 9 compatible", 9, 9, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pack.IsSegregationIncompatible(tc.a, tc.b); got != tc.want {
				t.Fatalf("IsSegregationIncompatible(%d, %d) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestMutAnchor_IsSegregationIncompatible_FullMatrix(t *testing.T) {
	// Exact value of every one of the 81 cells, in both argument orders.
	for a := 1; a <= 9; a++ {
		for b := 1; b <= 9; b++ {
			want := matrixCell(a, b)
			if got := pack.IsSegregationIncompatible(a, b); got != want {
				t.Fatalf("IsSegregationIncompatible(%d, %d) = %v, want %v", a, b, got, want)
			}
		}
	}
}

// matrixCell reads wantIncompatible (1-indexed class numbers). The table is
// cross-checked for symmetry in TestMutAnchor_ExpectedMatrixIsSymmetric.
func matrixCell(a, b int) bool {
	return wantIncompatible[a-1][b-1] == 'X'
}

func TestMutAnchor_ExpectedMatrixIsSymmetric(t *testing.T) {
	// Guards the test's own table against a transcription slip.
	for a := 1; a <= 9; a++ {
		for b := 1; b <= 9; b++ {
			if matrixCell(a, b) != matrixCell(b, a) {
				t.Fatalf("test table asymmetric at (%d, %d)", a, b)
			}
		}
	}
}

func TestMutAnchor_ScanItemWithClass_ZeroClass(t *testing.T) {
	// hazardClass == 0 must (a) never be blocked, even when class 1 (which
	// is incompatible with every real class) is already in the package,
	// and (b) never be recorded in ScannedHazardClasses; the SKU itself is
	// still appended.
	p := newMutPackage(false)
	if err := p.ScanItemWithClass("sku-c1", 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := p.ScanItemWithClass("sku-plain", 0); err != nil {
		t.Fatalf("class 0 must never be blocked, got %v", err)
	}
	if got := p.ScannedContents(); len(got) != 2 || got[0] != "sku-c1" || got[1] != "sku-plain" {
		t.Fatalf("scanned contents = %v, want [sku-c1 sku-plain]", got)
	}
	if got := p.ScannedHazardClasses(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("hazard classes = %v, want exactly [1] (class 0 must not be recorded)", got)
	}
}

func TestMutAnchor_ScanItemWithClass_NonZeroClassIsRecordedInOrder(t *testing.T) {
	p := newMutPackage(false)
	for i, class := range []int{3, 9, 7} {
		if err := p.ScanItemWithClass(fmt.Sprintf("sku-%d", i), class); err != nil {
			t.Fatalf("scan class %d: unexpected error %v", class, err)
		}
	}
	got := p.ScannedHazardClasses()
	if len(got) != 3 || got[0] != 3 || got[1] != 9 || got[2] != 7 {
		t.Fatalf("hazard classes = %v, want [3 9 7]", got)
	}
	if len(p.ScannedContents()) != 3 {
		t.Fatalf("scanned contents = %v, want 3 items", p.ScannedContents())
	}
}

func TestMutAnchor_ScanItemWithClass_FullMatrixViaAggregate(t *testing.T) {
	// For every ordered pair (existing, incoming): scanning `existing`
	// then `incoming` must be rejected iff the matrix marks them
	// incompatible, and a rejection must leave contents/classes unchanged.
	for existing := 1; existing <= 9; existing++ {
		for incoming := 1; incoming <= 9; incoming++ {
			t.Run(fmt.Sprintf("%d_then_%d", existing, incoming), func(t *testing.T) {
				assertScanPair(t, existing, incoming)
			})
		}
	}
}

func assertScanPair(t *testing.T, existing, incoming int) {
	t.Helper()
	p := newMutPackage(false)
	if err := p.ScanItemWithClass("sku-existing", existing); err != nil {
		t.Fatalf("seed class %d: %v", existing, err)
	}
	err := p.ScanItemWithClass("sku-incoming", incoming)

	wantViolation := matrixCell(existing, incoming)
	if gotViolation := errors.Is(err, pack.ErrPackageSegregationViolation); gotViolation != wantViolation {
		t.Fatalf("violation = %v (err %v), want %v", gotViolation, err, wantViolation)
	}
	if !wantViolation && err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantLen := 2
	if wantViolation {
		wantLen = 1 // a rejected item must not be recorded anywhere
	}
	if len(p.ScannedContents()) != wantLen || len(p.ScannedHazardClasses()) != wantLen {
		t.Fatalf("contents=%v classes=%v, want %d entries each",
			p.ScannedContents(), p.ScannedHazardClasses(), wantLen)
	}
}

func TestMutAnchor_ScanItemWithClass_ChecksEveryExistingClass(t *testing.T) {
	// 9 is compatible with 3; 3 is incompatible with 5. The conflicting
	// class is the FIRST of the existing classes in one case and the LAST
	// in the other, so an early return / off-by-one over the loop fails.
	for _, existing := range [][]int{{3, 9}, {9, 3}} {
		p := newMutPackage(false)
		for _, c := range existing {
			if err := p.ScanItemWithClass("sku", c); err != nil {
				t.Fatalf("seed %v: %v", existing, err)
			}
		}
		if err := p.ScanItemWithClass("sku-5", 5); !errors.Is(err, pack.ErrPackageSegregationViolation) {
			t.Fatalf("existing %v then 5: want violation, got %v", existing, err)
		}
	}
}

func TestMutAnchor_ScanItemWithClass_StatusGuard(t *testing.T) {
	for _, status := range []pack.Status{pack.Sealed, pack.Labeled, pack.Diverted} {
		p := pack.Rehydrate("p", "o", "t", status, []string{"sku-1"}, false, nil, false)
		if err := p.ScanItemWithClass("sku-2", 0); !errors.Is(err, pack.ErrAlreadySealed) {
			t.Fatalf("status %s: want ErrAlreadySealed, got %v", status, err)
		}
	}
	p := pack.Rehydrate("p", "o", "t", pack.Open, []string{"sku-1"}, false, nil, false)
	if err := p.ScanItemWithClass("sku-2", 0); err != nil {
		t.Fatalf("open package must accept scans, got %v", err)
	}
}

func TestMutAnchor_SortLane(t *testing.T) {
	cases := []struct {
		name    string
		fragile bool
		classes []int
		want    string
	}{
		{"no classes, not fragile", false, nil, pack.SortLaneStandard},
		{"no classes, fragile", true, nil, pack.SortLaneFragileNoTilt},
		{"exactly one class, not fragile", false, []int{7}, pack.SortLaneHazmat},
		{"exactly one class, fragile (hazmat wins)", true, []int{7}, pack.SortLaneHazmat},
		{"two classes, not fragile", false, []int{3, 9}, pack.SortLaneHazmat},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := pack.Rehydrate("p", "o", "t", pack.Sealed, []string{"sku-1"}, tc.fragile, tc.classes, false)
			if got := p.SortLane(); got != tc.want {
				t.Fatalf("SortLane() = %q, want %q", got, tc.want)
			}
		})
	}
	// A fresh, empty package: zero hazard classes must be STANDARD.
	if got := newMutPackage(false).SortLane(); got != pack.SortLaneStandard {
		t.Fatalf("fresh package SortLane() = %q, want %q", got, pack.SortLaneStandard)
	}
}

func TestMutAnchor_Weigh_ToleranceBoundary(t *testing.T) {
	tol := pack.WeightTolerance
	// Powers-of-two multiples/neighbours of tol are exact under IEEE-754
	// subtraction here (Sterbenz lemma: y/2 <= x <= 2y), so these
	// deviations are EXACTLY tol, one ulp over, and one ulp under.
	overTol := math.Nextafter(tol, math.Inf(1))
	underTol := math.Nextafter(tol, 0)
	twoTol := 2 * tol

	cases := []struct {
		name             string
		expected, actual float64
		wantLabeled      bool
		wantStatus       pack.Status
	}{
		{"deviation exactly tolerance, actual lower", twoTol, tol, true, pack.Labeled},
		{"deviation exactly tolerance, actual higher", tol, twoTol, true, pack.Labeled},
		{"deviation one ulp over tolerance, actual lower", twoTol, twoTol - overTol, false, pack.Diverted},
		{"deviation one ulp over tolerance, actual higher", twoTol - overTol, twoTol, false, pack.Diverted},
		{"deviation one ulp under tolerance, actual lower", twoTol, twoTol - underTol, true, pack.Labeled},
		{"deviation one ulp under tolerance, actual higher", twoTol - underTol, twoTol, true, pack.Labeled},
		{"identical non-zero weights", 2.0, 2.0, true, pack.Labeled},
		{"within tolerance, actual higher", 2.0, 2.02, true, pack.Labeled},
		{"within tolerance, actual lower", 2.02, 2.0, true, pack.Labeled},
		{"far over tolerance, actual higher", 2.0, 2.5, false, pack.Diverted},
		{"far over tolerance, actual lower", 2.5, 2.0, false, pack.Diverted},
		{"zero expected within tolerance", 0, 0.04, true, pack.Labeled},
		{"zero expected over tolerance", 0, 0.06, false, pack.Diverted},
		{"zero actual over tolerance", 0.06, 0, false, pack.Diverted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newMutPackage(false)
			if err := p.ScanItem("sku-1"); err != nil {
				t.Fatalf("scan: %v", err)
			}
			if err := p.Seal(); err != nil {
				t.Fatalf("seal: %v", err)
			}
			labeled, err := p.Weigh(tc.expected, tc.actual)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if labeled != tc.wantLabeled {
				t.Fatalf("Weigh(%v, %v) labeled = %v, want %v", tc.expected, tc.actual, labeled, tc.wantLabeled)
			}
			if p.Status() != tc.wantStatus {
				t.Fatalf("status = %s, want %s", p.Status(), tc.wantStatus)
			}
		})
	}
}

func TestMutAnchor_Weigh_FixtureDeviationsAreExact(t *testing.T) {
	tol := pack.WeightTolerance
	overTol := math.Nextafter(tol, math.Inf(1))
	underTol := math.Nextafter(tol, 0)
	twoTol := 2 * tol
	if d := twoTol - tol; d != tol {
		t.Fatalf("2*tol - tol = %v, want exactly %v", d, tol)
	}
	if d := twoTol - (twoTol - overTol); !(d > tol) {
		t.Fatalf("over-tolerance fixture deviation %v is not > %v", d, tol)
	}
	if d := twoTol - (twoTol - underTol); !(d < tol) {
		t.Fatalf("under-tolerance fixture deviation %v is not < %v", d, tol)
	}
}

func TestMutAnchor_Weigh_StateGuards(t *testing.T) {
	cases := []struct {
		name    string
		status  pack.Status
		wantErr error
	}{
		{"open", pack.Open, pack.ErrNotSealed},
		{"labeled", pack.Labeled, pack.ErrAlreadyProcessed},
		{"diverted", pack.Diverted, pack.ErrAlreadyProcessed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := pack.Rehydrate("p", "o", "t", tc.status, []string{"sku-1"}, false, nil, false)
			labeled, err := p.Weigh(2.0, 2.0)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if labeled {
				t.Fatalf("must not report a label on error")
			}
			if p.Status() != tc.status {
				t.Fatalf("status changed to %s on a rejected Weigh", p.Status())
			}
		})
	}
}

func TestMutAnchor_CheckSegregationSymmetry(t *testing.T) {
	var m [10][10]bool
	if err := pack.CheckSegregationSymmetry(&m); err != nil {
		t.Fatalf("an all-false matrix is symmetric, got %v", err)
	}
	// One-sided entries must be reported: this fails if the check loops never run.
	m[2][5] = true
	if err := pack.CheckSegregationSymmetry(&m); err == nil {
		t.Fatalf("m[2][5] without m[5][2] must be reported as asymmetric")
	}
	m[5][2] = true
	if err := pack.CheckSegregationSymmetry(&m); err != nil {
		t.Fatalf("a symmetric pair must pass, got %v", err)
	}
	// Edge classes: the first and last rows/columns are part of the check.
	var edge [10][10]bool
	edge[1][9] = true
	if err := pack.CheckSegregationSymmetry(&edge); err == nil {
		t.Fatalf("edge-class asymmetry [1][9] must be reported")
	}
	edge[9][1] = true
	edge[9][2] = true
	if err := pack.CheckSegregationSymmetry(&edge); err == nil {
		t.Fatalf("edge-class asymmetry [9][2] must be reported")
	}
	// The shipped matrix is symmetric (package init would have panicked otherwise).
	for a := 1; a <= 9; a++ {
		for b := 1; b <= 9; b++ {
			if pack.IsSegregationIncompatible(a, b) != pack.IsSegregationIncompatible(b, a) {
				t.Fatalf("shipped matrix asymmetric at (%d,%d)", a, b)
			}
		}
	}
}

// KNOWN EQUIVALENT MUTANTS (cannot be killed; do not chase them or lower the threshold):
//   - package.go Weigh: `deviation < 0` -> `<= 0`: at deviation 0 negation is 0, same result.
//   - segregation.go CheckSegregationSymmetry: `i <= 9` -> `i < 9` and `j <= 9` -> `j < 9`: the pair
//     (9, k) is also visited as (k, 9), so skipping row/column 9 reports exactly the same asymmetries.
// With these three surviving the sensor reads Killed 60 / Lived 3 (95.24%) against the 90% gate.
