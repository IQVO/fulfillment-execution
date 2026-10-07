package productclassificationcopy_test

import (
	"context"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

func TestMemoryStore_FoundMapsLikeTheHTTPClient(t *testing.T) {
	s := productclassificationcopy.NewMemoryStore()
	ctx := context.Background()
	applied, err := s.UpsertIfNewer(ctx, ports.ProductClassificationRecord{
		SKU: "SKU-1", HandlingTags: []string{"Hazmat", "Fragile", "TemperatureSensitive"},
		TemperatureClass: "Frozen", DOTHazardClass: 3, Version: 3,
	})
	if err != nil || !applied {
		t.Fatalf("upsert = %v, %v; want applied", applied, err)
	}
	got, err := s.GetClassification(ctx, "SKU-1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	want := ports.ClassificationInfo{Known: true, Hazmat: true, Fragile: true, DOTHazardClass: 3}
	if got != want {
		t.Fatalf("info = %+v, want %+v", got, want)
	}
}

func TestMemoryStore_NotFoundIsUnknownWithoutError(t *testing.T) {
	got, err := productclassificationcopy.NewMemoryStore().GetClassification(context.Background(), "SKU-NONE")
	if err != nil {
		t.Fatalf("not found must not be an error (the HTTP client's 404 case), got %v", err)
	}
	if got != (ports.ClassificationInfo{Known: false}) {
		t.Fatalf("info = %+v, want Known=false", got)
	}
}

func TestMemoryStore_NonHazmatTagsAreKnownWithoutHazard(t *testing.T) {
	s := productclassificationcopy.NewMemoryStore()
	ctx := context.Background()
	_, _ = s.UpsertIfNewer(ctx, ports.ProductClassificationRecord{SKU: "SKU-2", HandlingTags: []string{"Oversized"}, Version: 1})
	got, _ := s.GetClassification(ctx, "SKU-2")
	if got != (ports.ClassificationInfo{Known: true}) {
		t.Fatalf("info = %+v, want Known with no flags", got)
	}
}

func TestMemoryStore_VersionGuard(t *testing.T) {
	s := productclassificationcopy.NewMemoryStore()
	ctx := context.Background()
	rec := func(v int64, dot int) ports.ProductClassificationRecord {
		return ports.ProductClassificationRecord{SKU: "SKU-3", HandlingTags: []string{"Hazmat"}, DOTHazardClass: dot, Version: v}
	}
	cases := []struct {
		name        string
		rec         ports.ProductClassificationRecord
		wantApplied bool
		wantDot     int
	}{
		{"insert when absent", rec(2, 3), true, 3},
		{"equal version ignored", rec(2, 8), false, 3},
		{"older version ignored", rec(1, 8), false, 3},
		{"newer version replaces", rec(3, 8), true, 8},
	}
	for _, tc := range cases {
		applied, err := s.UpsertIfNewer(ctx, tc.rec)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if applied != tc.wantApplied {
			t.Fatalf("%s: applied = %v, want %v", tc.name, applied, tc.wantApplied)
		}
		got, _ := s.GetClassification(ctx, "SKU-3")
		if got.DOTHazardClass != tc.wantDot {
			t.Fatalf("%s: dot = %d, want %d", tc.name, got.DOTHazardClass, tc.wantDot)
		}
	}
}

func TestMemoryStore_CopiesTagsDefensively(t *testing.T) {
	s := productclassificationcopy.NewMemoryStore()
	ctx := context.Background()
	tags := []string{"Hazmat"}
	_, _ = s.UpsertIfNewer(ctx, ports.ProductClassificationRecord{SKU: "SKU-4", HandlingTags: tags, DOTHazardClass: 2, Version: 1})
	tags[0] = "Fragile"
	got, _ := s.GetClassification(ctx, "SKU-4")
	if !got.Hazmat || got.Fragile {
		t.Fatalf("caller mutation leaked into the copy: %+v", got)
	}
}

func TestPermissiveLookup_NeverClassified(t *testing.T) {
	got, err := productclassificationcopy.NewPermissiveLookup().GetClassification(context.Background(), "SKU-1")
	if err != nil || got != (ports.ClassificationInfo{Known: false}) {
		t.Fatalf("permissive = %+v, %v; want Known=false, nil", got, err)
	}
}
