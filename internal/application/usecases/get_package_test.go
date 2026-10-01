package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// sealedPackage persists a sealed package for orderRef straight through the
// repo — the read use cases only need rows to exist, not the Pack flow.
func sealedPackage(t *testing.T, h *harness, id shared.PackageId, orderRef shared.OrderRef) *pack.Package {
	t.Helper()
	p := pack.New(id, orderRef, shared.TaskId("task-"+string(id)), false, false)
	if err := p.ScanItem("sku-1"); err != nil {
		t.Fatalf("ScanItem: %v", err)
	}
	if err := p.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := h.packages.Save(context.Background(), p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	return p
}

func TestGetPackage_ReturnsThePackage(t *testing.T) {
	h := newHarness()
	sealedPackage(t, h, "pkg-1", "order-1")

	uc := &usecases.GetPackage{Packages: h.packages}
	got, err := uc.Execute(context.Background(), "pkg-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Id() != "pkg-1" || got.OrderRef() != "order-1" || got.Status() != pack.Sealed {
		t.Fatalf("unexpected package: id=%s orderRef=%s status=%s", got.Id(), got.OrderRef(), got.Status())
	}
}

// The read side must surface the SLAM outcome the 204 hides: a diverted
// package reads back DIVERTED, a labeled one LABELED.
func TestGetPackage_SurfacesSlamOutcome(t *testing.T) {
	cases := []struct {
		name   string
		actual float64
		want   pack.Status
	}{
		{"within tolerance is labeled", 2.0, pack.Labeled},
		{"outside tolerance is diverted", 2.5, pack.Diverted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness()
			sealedPackage(t, h, "pkg-1", "order-1")
			slam := &usecases.RunSlam{Packages: h.packages, Publisher: h.publisher, Clock: h.clock}
			if err := slam.Execute(context.Background(), "pkg-1", tc.actual, 2.0); err != nil {
				t.Fatalf("RunSlam: %v", err)
			}

			got, err := (&usecases.GetPackage{Packages: h.packages}).Execute(context.Background(), "pkg-1")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Status() != tc.want {
				t.Fatalf("expected %s, got %s", tc.want, got.Status())
			}
		})
	}
}

func TestGetPackage_UnknownIdReturnsErrPackageNotFound(t *testing.T) {
	h := newHarness()
	uc := &usecases.GetPackage{Packages: h.packages}
	got, err := uc.Execute(context.Background(), "does-not-exist")
	if !errors.Is(err, usecases.ErrPackageNotFound) {
		t.Fatalf("expected ErrPackageNotFound, got %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil package, got %+v", got)
	}
}

func TestGetPackage_PropagatesRepoError(t *testing.T) {
	repo := newErrPackageRepo()
	repo.failFindById = true
	uc := &usecases.GetPackage{Packages: repo}
	if _, err := uc.Execute(context.Background(), "pkg-1"); !errors.Is(err, errFake) {
		t.Fatalf("expected the repo error, got %v", err)
	}
}

func TestGetPackagesByOrderRef_ReturnsEveryPackageForTheOrder(t *testing.T) {
	h := newHarness()
	sealedPackage(t, h, "pkg-2", "order-1")
	sealedPackage(t, h, "pkg-1", "order-1")
	sealedPackage(t, h, "pkg-3", "order-2")

	uc := &usecases.GetPackagesByOrderRef{Packages: h.packages}
	got, err := uc.Execute(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 packages for order-1, got %d", len(got))
	}
	if got[0].Id() != "pkg-1" || got[1].Id() != "pkg-2" {
		t.Fatalf("expected packages ordered by id [pkg-1 pkg-2], got [%s %s]", got[0].Id(), got[1].Id())
	}
	for _, p := range got {
		if p.OrderRef() != "order-1" {
			t.Fatalf("expected only order-1 packages, got orderRef %q", p.OrderRef())
		}
	}
}

func TestGetPackagesByOrderRef_UnknownOrderRefReturnsEmptyNotError(t *testing.T) {
	h := newHarness()
	sealedPackage(t, h, "pkg-1", "order-1")
	uc := &usecases.GetPackagesByOrderRef{Packages: h.packages}

	got, err := uc.Execute(context.Background(), "does-not-exist")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("expected an empty non-nil slice, got %v", got)
	}
}

func TestGetPackagesByOrderRef_PropagatesRepoError(t *testing.T) {
	repo := newErrPackageRepo()
	repo.failFindByOrderRef = true
	uc := &usecases.GetPackagesByOrderRef{Packages: repo}
	if _, err := uc.Execute(context.Background(), "order-1"); !errors.Is(err, errFake) {
		t.Fatalf("expected the repo error, got %v", err)
	}
}
