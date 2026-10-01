//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// PackageRepo.FindByOrderRef backs GET /packages?orderRef= (ADR-0033). Runs
// against a throwaway Postgres 16 (testcontainers, via outboxDB) with every
// migration applied, so it also proves migration 0013's index exists and
// the query uses the real column set.
func TestPackageRepo_FindByOrderRef_ReturnsEveryPackageForTheOrderOrderedById(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewPackageRepo(pool)
	ctx := context.Background()

	saveSealedPackage(t, repo, "pkg-b", "order-read-1", 0)
	diverted := saveSealedPackage(t, repo, "pkg-a", "order-read-1", 3)
	saveSealedPackage(t, repo, "pkg-c", "order-read-2", 0)

	if _, err := diverted.Weigh(2.0, 2.5); err != nil {
		t.Fatalf("Weigh: %v", err)
	}
	if err := repo.Save(ctx, diverted); err != nil {
		t.Fatalf("Save(post-weigh): %v", err)
	}

	got, err := repo.FindByOrderRef(ctx, "order-read-1")
	if err != nil {
		t.Fatalf("FindByOrderRef: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 packages for order-read-1, got %d", len(got))
	}
	if got[0].Id() != "pkg-a" || got[1].Id() != "pkg-b" {
		t.Fatalf("expected [pkg-a pkg-b] ordered by id, got [%s %s]", got[0].Id(), got[1].Id())
	}
	if got[0].Status() != pack.Diverted {
		t.Fatalf("expected pkg-a to read back DIVERTED, got %s", got[0].Status())
	}
	if got[0].SortLane() != pack.SortLaneHazmat || !got[0].FragileHandling() || got[0].TaskId() != "task-pkg-a" {
		t.Fatalf("expected full rehydration of pkg-a, got lane=%s fragile=%v task=%s", got[0].SortLane(), got[0].FragileHandling(), got[0].TaskId())
	}
	if got[1].Status() != pack.Sealed {
		t.Fatalf("expected pkg-b to read back SEALED, got %s", got[1].Status())
	}

	assertUnknownOrderRefIsEmpty(t, repo)
	assertSingleRowLookups(t, repo)
	assertOrderRefIndexExists(t, pool)
}

// saveSealedPackage seals and persists a one-item package for orderRef.
func saveSealedPackage(t *testing.T, repo *postgres.PackageRepo, id shared.PackageId, orderRef shared.OrderRef, hazardClass int) *pack.Package {
	t.Helper()
	p := pack.New(id, orderRef, shared.TaskId("task-"+string(id)), true, false)
	if err := p.ScanItemWithClass("sku-"+string(id), hazardClass); err != nil {
		t.Fatalf("ScanItemWithClass: %v", err)
	}
	if err := p.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := repo.Save(context.Background(), p); err != nil {
		t.Fatalf("Save(%s): %v", id, err)
	}
	return p
}

func assertUnknownOrderRefIsEmpty(t *testing.T, repo *postgres.PackageRepo) {
	t.Helper()
	none, err := repo.FindByOrderRef(context.Background(), "order-does-not-exist")
	if err != nil {
		t.Fatalf("FindByOrderRef(unknown): %v", err)
	}
	if none == nil || len(none) != 0 {
		t.Fatalf("expected an empty non-nil slice for an unknown orderRef, got %v", none)
	}

}

// assertSingleRowLookups checks FindById / FindByTaskId, which share the
// refactored scan path with FindByOrderRef.
func assertSingleRowLookups(t *testing.T, repo *postgres.PackageRepo) {
	t.Helper()
	ctx := context.Background()
	byId, err := repo.FindById(ctx, "pkg-a")
	if err != nil || byId == nil || byId.Status() != pack.Diverted {
		t.Fatalf("FindById(pkg-a): got %v, err %v", byId, err)
	}
	missing, err := repo.FindById(ctx, "pkg-missing")
	if err != nil || missing != nil {
		t.Fatalf("FindById(missing): expected (nil, nil), got %v, %v", missing, err)
	}
	byTask, err := repo.FindByTaskId(ctx, "task-pkg-b")
	if err != nil || byTask == nil || byTask.Id() != "pkg-b" {
		t.Fatalf("FindByTaskId(task-pkg-b): got %v, err %v", byTask, err)
	}

}

func assertOrderRefIndexExists(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	var indexed bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'packages' AND indexname = 'idx_packages_order_ref')`).Scan(&indexed); err != nil {
		t.Fatalf("query pg_indexes: %v", err)
	}
	if !indexed {
		t.Fatalf("expected idx_packages_order_ref (migration 0013) to exist")
	}
}
