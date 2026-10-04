package pathcatalog_test

import (
	"errors"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/domain/pathcatalog"
)

func fleetCatalogue() *pathcatalog.Catalogue {
	return pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", Direct: true, RequiredCapabilities: []string{"pick"}},
		{Id: "PACK", MatchPrefix: "pack", Direct: true, RequiredCapabilities: []string{"pack"}},
		{Id: "REBIN", MatchPrefix: "rebin", Direct: true, RequiredCapabilities: []string{"rebin"}},
		{Id: "SLAM", MatchPrefix: "slam", Direct: true, RequiredCapabilities: []string{"slam"}},
	})
}

func TestCatalogue_Lookup_UnknownPathId_ReturnsError(t *testing.T) {
	cat := pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", Direct: true, RequiredCapabilities: []string{"pick"}},
	})
	if _, err := cat.Lookup("rebin"); !errors.Is(err, pathcatalog.ErrUnknownPath) {
		t.Fatalf("want ErrUnknownPath for an id not in the catalogue, got %v", err)
	}
}

func TestCatalogue_Lookup_KnownPathId_ReturnsDefinition(t *testing.T) {
	cat := pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", Direct: true, RequiredCapabilities: []string{"pick"}},
	})
	def, err := cat.Lookup("PICK")
	if err != nil || def.Id != "PICK" || !def.Direct {
		t.Fatalf("unexpected lookup result: %+v, err=%v", def, err)
	}
	if len(def.RequiredCapabilities) != 1 || def.RequiredCapabilities[0] != "pick" {
		t.Fatalf("expected RequiredCapabilities [pick], got %v", def.RequiredCapabilities)
	}
}

// The catalogue must hold every one of this fleet's four declared paths
// simultaneously and answer each independently.
func TestCatalogue_Lookup_AllFourDeclaredPaths(t *testing.T) {
	cat := fleetCatalogue()
	for _, want := range []string{"PICK", "PACK", "REBIN", "SLAM"} {
		def, err := cat.Lookup(want)
		if err != nil {
			t.Fatalf("Lookup(%q): unexpected error %v", want, err)
		}
		if def.Id != want {
			t.Fatalf("Lookup(%q): got id %q", want, def.Id)
		}
	}
}

// The real regression this test suite exists to prevent: actual path_id
// values across this fleet are NOT the bare canonical id. order-
// management's shared.DefaultPathId is the plain "pick"; e2e fixtures
// and ops-agent's seeded targets use station/zone/scenario-qualified
// forms like "pick-zone-a", "pick-soak", "pick-t5-imbalance". Every one
// of these must resolve to the PICK definition, not fail as unknown.
func TestCatalogue_Lookup_RealFleetPathIdVariants(t *testing.T) {
	cat := fleetCatalogue()

	cases := []struct {
		pathId string
		wantId string
	}{
		{"pick", "PICK"},
		{"pick-zone-a", "PICK"},
		{"pick-soak", "PICK"},
		{"pick-t5-imbalance", "PICK"},
		{"pack-soak", "PACK"},
		{"PICK", "PICK"},        // fulfillment-execution's own canonical-form usage
		{"Pick-Zone-B", "PICK"}, // case-insensitive
	}
	for _, tc := range cases {
		def, err := cat.Lookup(tc.pathId)
		if err != nil {
			t.Fatalf("Lookup(%q): unexpected error %v", tc.pathId, err)
		}
		if def.Id != tc.wantId {
			t.Fatalf("Lookup(%q): got id %q, want %q", tc.pathId, def.Id, tc.wantId)
		}
	}
}

// A prefix must not match a path_id that merely starts with the same
// letters but is not actually in that family (no "-" separator and not
// an exact match) — e.g. a hypothetical "picking-station" path_id must
// NOT be treated as a PICK variant just because it starts with "pick".
func TestCatalogue_Lookup_DoesNotMatchBareSubstringPrefix(t *testing.T) {
	cat := fleetCatalogue()
	if _, err := cat.Lookup("picking-station"); !errors.Is(err, pathcatalog.ErrUnknownPath) {
		t.Fatalf("want ErrUnknownPath for a bare-substring near-miss, got %v", err)
	}
}

func TestCatalogue_Ids_ReturnsEveryDeclaredId(t *testing.T) {
	cat := pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", Direct: true, RequiredCapabilities: []string{"pick"}},
		{Id: "PACK", MatchPrefix: "pack", Direct: true, RequiredCapabilities: []string{"pack"}},
	})
	ids := cat.Ids()
	if len(ids) != 2 {
		t.Fatalf("expected 2 ids, got %v", ids)
	}
}

// An empty catalogue is a valid (if useless) Catalogue value at the
// domain level — New does not itself enforce non-emptiness. That
// invariant belongs to the loader (a boot-time concern), not to the
// in-memory type, per this package's own doc comment.
func TestCatalogue_EmptyCatalogue_EveryLookupFails(t *testing.T) {
	cat := pathcatalog.New(nil)
	if _, err := cat.Lookup("pick"); !errors.Is(err, pathcatalog.ErrUnknownPath) {
		t.Fatalf("want ErrUnknownPath, got %v", err)
	}
}

// Two definitions with the SAME prefix are the only way to tie on length. The first declared one wins
// (`>`, not `>=`), so a duplicate later in the list never silently replaces an earlier declaration.
func TestCatalogue_Lookup_DuplicatePrefix_FirstDeclaredWins(t *testing.T) {
	cat := pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "FIRST", MatchPrefix: "pick", Direct: true},
		{Id: "SECOND", MatchPrefix: "pick", Direct: false},
	})
	def, err := cat.Lookup("pick")
	if err != nil || def.Id != "FIRST" {
		t.Fatalf("want the first declared definition, got %+v err=%v", def, err)
	}
	// and a longer prefix still beats a shorter one regardless of order
	cat = pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "SHORT", MatchPrefix: "pick"},
		{Id: "LONG", MatchPrefix: "pick-wave"},
	})
	if def, _ := cat.Lookup("pick-wave-7"); def.Id != "LONG" {
		t.Fatalf("longest prefix must win, got %s", def.Id)
	}
}
