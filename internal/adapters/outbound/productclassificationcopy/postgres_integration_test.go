//go:build integration

package productclassificationcopy_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

// copyDB boots a throwaway Postgres (testcontainers, never an external
// DATABASE_URL), runs every migration and returns a pool.
func copyDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("fulfillment_execution"),
		tcpostgres.WithUsername("fulfillment"),
		tcpostgres.WithPassword("fulfillment"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	if err := postgres.Migrate(url, "../../../../migrations"); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestPostgresStore_FoundNotFoundAndVersionGuard(t *testing.T) {
	pool := copyDB(t)
	s := productclassificationcopy.NewPostgresStore(pool)
	ctx := context.Background()

	// Not found: Known=false, nil error (the HTTP client's 404 case).
	info, err := s.GetClassification(ctx, "SKU-NONE")
	if err != nil || info != (ports.ClassificationInfo{Known: false}) {
		t.Fatalf("not found = %+v, %v", info, err)
	}

	// Insert when absent.
	applied, err := s.UpsertIfNewer(ctx, ports.ProductClassificationRecord{
		SKU: "SKU-1", HandlingTags: []string{"Hazmat", "TemperatureSensitive"}, TemperatureClass: "Frozen", DOTHazardClass: 3, Version: 3,
	})
	if err != nil || !applied {
		t.Fatalf("insert = %v, %v", applied, err)
	}
	info, err = s.GetClassification(ctx, "SKU-1")
	if err != nil || info != (ports.ClassificationInfo{Known: true, Hazmat: true, DOTHazardClass: 3}) {
		t.Fatalf("found = %+v, %v", info, err)
	}

	// Equal and older versions are ignored.
	for _, v := range []int64{3, 2} {
		applied, err = s.UpsertIfNewer(ctx, ports.ProductClassificationRecord{SKU: "SKU-1", HandlingTags: []string{"Fragile"}, Version: v})
		if err != nil || applied {
			t.Fatalf("version %d: applied = %v, err = %v; want ignored", v, applied, err)
		}
	}

	// A newer version replaces the full state (omitted fields become NULL).
	applied, err = s.UpsertIfNewer(ctx, ports.ProductClassificationRecord{SKU: "SKU-1", HandlingTags: []string{"Fragile"}, Version: 4})
	if err != nil || !applied {
		t.Fatalf("newer = %v, %v", applied, err)
	}
	rec, ok, err := s.Find(ctx, "SKU-1")
	if err != nil || !ok {
		t.Fatalf("find = %v, %v", ok, err)
	}
	if rec.Version != 4 || rec.DOTHazardClass != 0 || rec.TemperatureClass != "" || len(rec.HandlingTags) != 1 || rec.HandlingTags[0] != "Fragile" {
		t.Fatalf("row after v4 = %+v", rec)
	}
	info, _ = s.GetClassification(ctx, "SKU-1")
	if info != (ports.ClassificationInfo{Known: true, Fragile: true}) {
		t.Fatalf("lookup after v4 = %+v", info)
	}

	if _, ok, err := s.Find(ctx, "SKU-NONE"); ok || err != nil {
		t.Fatalf("find absent = %v, %v", ok, err)
	}
}

// failingCopy delegates to the real store, then fails, so the surrounding
// UnitOfWork must roll back both the claim and the upsert.
type failingCopy struct {
	inner *productclassificationcopy.PostgresStore
}

func (f failingCopy) UpsertIfNewer(ctx context.Context, rec ports.ProductClassificationRecord) (bool, error) {
	if _, err := f.inner.UpsertIfNewer(ctx, rec); err != nil {
		return false, err
	}
	return false, errors.New("fake: failure after the upsert")
}

// The claim (processed_events) and the upsert commit or roll back together:
// a failure after the upsert leaves neither the row nor the claim, so the
// redelivery applies the classification.
func TestPostgresStore_ClaimAndUpsertShareOneTransaction(t *testing.T) {
	pool := copyDB(t)
	store := productclassificationcopy.NewPostgresStore(pool)
	processed := postgres.NewProcessedEventsRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	ctx := context.Background()
	req := usecases.ProductClassifiedRequest{
		EventId: "ce-tx-1",
		Record:  ports.ProductClassificationRecord{SKU: "SKU-TX", HandlingTags: []string{"Hazmat"}, DOTHazardClass: 2, Version: 1},
	}

	broken := &usecases.ApplyProductClassified{Processed: processed, Copy: failingCopy{inner: store}, UnitOfWork: uow}
	if err := broken.Execute(ctx, req); err == nil {
		t.Fatal("expected the failure to surface")
	}
	if _, ok, _ := store.Find(ctx, "SKU-TX"); ok {
		t.Fatal("the upsert survived a rolled-back transaction")
	}
	var claims int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE event_id = 'ce-tx-1'`).Scan(&claims); err != nil {
		t.Fatalf("count claims: %v", err)
	}
	if claims != 0 {
		t.Fatal("the processed-event claim survived a rolled-back transaction: the redelivery would be skipped")
	}

	ok := &usecases.ApplyProductClassified{Processed: processed, Copy: store, UnitOfWork: uow}
	if err := ok.Execute(ctx, req); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	info, err := store.GetClassification(ctx, "SKU-TX")
	if err != nil || !info.Known || info.DOTHazardClass != 2 {
		t.Fatalf("lookup after redelivery = %+v, %v", info, err)
	}
}

// A read failure surfaces as an error (the HTTP client's transport-error
// case), which SealPackage fails open on.
func TestPostgresStore_ReadFailureIsAnError(t *testing.T) {
	pool := copyDB(t)
	s := productclassificationcopy.NewPostgresStore(pool)
	pool.Close()
	if _, err := s.GetClassification(context.Background(), "SKU-1"); err == nil {
		t.Fatal("a failed read must return an error")
	}
	if _, err := s.UpsertIfNewer(context.Background(), ports.ProductClassificationRecord{SKU: "SKU-1", Version: 1}); err == nil {
		t.Fatal("a failed write must return an error")
	}
	if _, _, err := s.Find(context.Background(), "SKU-1"); err == nil {
		t.Fatal("a failed find must return an error")
	}
}
