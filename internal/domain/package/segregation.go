package pack

import "fmt"

// DOT hazard-class segregation, collapsed to the nine parent hazard
// classes.
//
// This is a DELIBERATE, DOCUMENTED duplication of the same matrix
// inventory-storage derives for its own SKU-level segregation concern (see
// that repository's `internal/domain/product` package, branch
// feature/dot-hazard-class-segregation). Bounded contexts do not share Go
// types or tables across repository boundaries — this mirrors the existing,
// already-accepted duplication of TemperatureClass between facility-layout
// and inventory-storage (see fulfillment-execution ADR-0009 and
// inventory-storage ADR-0009's package doc comment on
// internal/domain/product/classification.go). Each side names, derives, and
// tests the concept in its own ubiquitous language; nothing here imports or
// generates from the other repository.
//
// Source: 49 CFR §177.848, "Segregation of hazardous materials" — the
// federal segregation table for hazardous materials in the same transport
// vehicle/storage. The regulation's table is keyed by hazard-class
// DIVISION (1.1, 1.2, 1.3 ... 8), not by the nine parent CLASSES this
// service (and inventory-storage) actually track. Reducing that table to a
// 9x9 class-level boolean incompatibility matrix uses four simplification
// rules, applied uniformly:
//
//  1. Collapse divisions to their parent class. Where two classes' member
//     divisions disagree (e.g. class 2's 2.2 is compatible with class 3 but
//     2.3 is not), the pair is marked incompatible if ANY division pairing
//     in the regulation's table is marked incompatible — the conservative,
//     worst-case collapse. A same-package pack station has no visibility
//     into sub-division detail, so it must not stow a division-level
//     incompatibility optimistically.
//  2. The regulation's table uses two distinct incompatibility markers, "X"
//     (may not be loaded/transported/stored together at all) and "O"
//     (allowed only with a documented ADDITIONAL separation method, e.g. a
//     solid intervening bulkhead). Both collapse to a single boolean
//     "incompatible" here: this service models same-PACKAGE (same
//     container) storage, where the extra separation method a "O" permits
//     in a full transport vehicle has no equivalent — there is no
//     intervening compartment inside one sealed carton.
//  3. Class 1 (Explosives) is treated as maximally restrictive: every pair
//     involving class 1, including class 1 with itself, is incompatible.
//     The regulation's real answer for two class-1 divisions together
//     depends on the separate Compatibility Table for Class 1 materials
//     (49 CFR §177.848(f), compatibility groups A-S) which this
//     class-level boolean matrix does not model at all — so the safe
//     simplification is to never permit two hazmat-classified items to
//     share a package if either carries class 1.
//  4. Class 9 (Miscellaneous dangerous goods) does not appear as a row/
//     column in 49 CFR §177.848's table at all — Class 9 materials are
//     generally exempt from the segregation requirements the table
//     enforces. This matrix therefore treats class 9 as broadly
//     compatible with every other class EXCEPT class 1, consistent with
//     rule 3's "class 1 is maximally restrictive" being the one exception
//     that overrides "class 9 is otherwise unconstrained."
//
// IMPORTANT: this matrix was derived independently in this repository, not
// copied from inventory-storage's parallel branch (which may not have
// merged yet at the time this was written). The two matrices are derived
// from the same regulation and the same four rules and are EXPECTED to be
// identical, but they have not been mechanically diffed against each
// other — see this feature's PR description for the reconciliation note
// and follow-up.
//
// segregationMatrix[a][b] is true when DOT hazard class a and DOT hazard
// class b (1-indexed, 1..9) may NOT share the same sealed package. The
// array is sized [10][10] so index i means "class i" directly — index 0
// (row and column) is unused zero-value padding, not a class. The matrix
// is symmetric by construction (enforced by the init-time check below);
// out-of-range/zero classes are never looked up — callers must not pass a
// zero/unset class into IsSegregationIncompatible.
var segregationMatrix = [10][10]bool{
	// class 1: maximally restrictive — incompatible with everything, including itself.
	1: {0: false, 1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: true, 8: true, 9: true},
	// class 2
	2: {0: false, 1: true, 2: false, 3: true, 4: true, 5: true, 6: true, 7: true, 8: true, 9: false},
	// class 3
	3: {0: false, 1: true, 2: true, 3: false, 4: false, 5: true, 6: true, 7: false, 8: false, 9: false},
	// class 4
	4: {0: false, 1: true, 2: true, 3: false, 4: false, 5: false, 6: true, 7: false, 8: true, 9: false},
	// class 5
	5: {0: false, 1: true, 2: true, 3: true, 4: false, 5: false, 6: true, 7: false, 8: true, 9: false},
	// class 6
	6: {0: false, 1: true, 2: true, 3: true, 4: true, 5: true, 6: false, 7: false, 8: true, 9: false},
	// class 7
	7: {0: false, 1: true, 2: true, 3: false, 4: false, 5: false, 6: false, 7: false, 8: false, 9: false},
	// class 8
	8: {0: false, 1: true, 2: true, 3: false, 4: true, 5: true, 6: true, 7: false, 8: false, 9: false},
	// class 9: broadly compatible except with class 1.
	9: {0: false, 1: true, 2: false, 3: false, 4: false, 5: false, 6: false, 7: false, 8: false, 9: false},
}

func init() {
	// Verify at package-init time that the matrix is symmetric, so a
	// transcription slip fails loudly (any go build/test/run of this
	// package panics immediately) rather than silently producing an
	// asymmetric rule.
	if err := CheckSegregationSymmetry(&segregationMatrix); err != nil {
		panic(err.Error())
	}
}

// CheckSegregationSymmetry returns an error naming the first asymmetric
// pair in m, or nil when m is symmetric over classes 1..9. It is the init-time
// self-check above, extracted so the check itself can be exercised (an
// init-only loop cannot be tested, so a broken loop would go unnoticed).
func CheckSegregationSymmetry(m *[10][10]bool) error {
	for i := 1; i <= 9; i++ {
		for j := 1; j <= 9; j++ {
			if m[i][j] != m[j][i] {
				return fmt.Errorf("pack: segregationMatrix is not symmetric at [%d][%d] — see segregation.go", i, j)
			}
		}
	}
	return nil
}

// IsSegregationIncompatible reports whether two scanned items' DOT hazard
// classes may not share the same sealed package, per the class-level
// simplification documented above. a and b must each be in 1..9; a
// zero/unset class must never be passed here — callers check for "no
// hazard class" before calling (see Package.scan), since an unclassified
// item never triggers or blocks segregation (fail-open).
func IsSegregationIncompatible(a, b int) bool {
	if a < 1 || a > 9 || b < 1 || b > 9 {
		return false
	}
	return segregationMatrix[a][b]
}
