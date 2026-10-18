//go:build integration

package interleavinghintcopy_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/interleavinghintcopy"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

// hintDB boots a throwaway Postgres (testcontainers, never an external
// DATABASE_URL), runs every migration and returns a pool.
func hintDB(t *testing.T) *pgxpool.Pool {
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

func hint(stationId, hintId string, expiresAt time.Time, taskIds ...string) ports.InterleavingHint {
	candidates := make([]ports.InterleavingHintCandidate, len(taskIds))
	for i, id := range taskIds {
		candidates[i] = ports.InterleavingHintCandidate{Rank: i + 1, TaskId: id}
	}
	return ports.InterleavingHint{
		HintId: hintId, StationId: stationId, SiteId: "SITE-1",
		ExpiresAt: expiresAt, Candidates: candidates, ReceivedAt: time.Now().UTC(),
	}
}

func TestPostgresHintStore_StoreSupersedeAndExpiry(t *testing.T) {
	pool := hintDB(t)
	s := interleavinghintcopy.NewPostgresStore(pool)
	ctx := context.Background()
	now := time.Now().UTC()

	// Unknown station: nil, no error.
	if h, err := s.ActiveForStation(ctx, "STN-NONE", now); err != nil || h != nil {
		t.Fatalf("unknown station = %+v, %v", h, err)
	}

	if err := s.ReplaceForStation(ctx, hint("STN-07", "hint-1", now.Add(time.Minute), "t-a", "t-b")); err != nil {
		t.Fatalf("store: %v", err)
	}
	h, err := s.ActiveForStation(ctx, "STN-07", now)
	if err != nil || h == nil {
		t.Fatalf("active = %+v, %v", h, err)
	}
	if h.HintId != "hint-1" || h.SiteId != "SITE-1" || len(h.Candidates) != 2 ||
		h.Candidates[0] != (ports.InterleavingHintCandidate{Rank: 1, TaskId: "t-a"}) ||
		h.Candidates[1] != (ports.InterleavingHintCandidate{Rank: 2, TaskId: "t-b"}) {
		t.Fatalf("active = %+v, want hint-1 with [t-a, t-b]", h)
	}

	// A newer hint for the same station supersedes; other stations untouched.
	if err := s.ReplaceForStation(ctx, hint("STN-07", "hint-2", now.Add(2*time.Minute), "t-c")); err != nil {
		t.Fatalf("supersede: %v", err)
	}
	if err := s.ReplaceForStation(ctx, hint("STN-08", "hint-3", now.Add(time.Minute), "t-z")); err != nil {
		t.Fatalf("other station: %v", err)
	}
	h, _ = s.ActiveForStation(ctx, "STN-07", now)
	if h == nil || h.HintId != "hint-2" || len(h.Candidates) != 1 || h.Candidates[0].TaskId != "t-c" {
		t.Fatalf("STN-07 = %+v, want hint-2 [t-c]", h)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM interleaving_hints`).Scan(&rows); err != nil || rows != 2 {
		t.Fatalf("rows = %d, %v; want 2 (one per station)", rows, err)
	}

	// Expired: invisible right at/after expires_at.
	h, _ = s.ActiveForStation(ctx, "STN-08", now.Add(2*time.Minute))
	if h != nil {
		t.Fatalf("STN-08 after expiry = %+v, want nil", h)
	}
}

// The write joins the UnitOfWork transaction: ApplyInterleavingHint's
// processed-event claim and the upsert commit or roll back together, so a
// duplicate delivery has exactly one effect.
func TestPostgresHintStore_ClaimAndUpsertAreAtomic(t *testing.T) {
	pool := hintDB(t)
	s := interleavinghintcopy.NewPostgresStore(pool)
	uow := postgres.NewUnitOfWork(pool)
	uc := &usecases.ApplyInterleavingHint{Processed: postgres.NewProcessedEventsRepo(pool), Hints: s, UnitOfWork: uow}
	ctx := context.Background()
	now := time.Now().UTC()

	req := usecases.InterleavingHintRequest{EventId: "ce-hint-1", Hint: hint("STN-07", "hint-1", now.Add(time.Minute), "t-a")}
	for i := 0; i < 3; i++ {
		if err := uc.Execute(ctx, req); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}
	var claims int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE event_id = 'ce-hint-1'`).Scan(&claims); err != nil {
		t.Fatalf("count claims: %v", err)
	}
	if claims != 1 {
		t.Fatalf("claims = %d, want 1 (three deliveries, one effect)", claims)
	}
	h, _ := s.ActiveForStation(ctx, "STN-07", now)
	if h == nil || h.HintId != "hint-1" {
		t.Fatalf("active = %+v, want hint-1", h)
	}
}
