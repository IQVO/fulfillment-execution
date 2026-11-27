//go:build integration

// The replay-class rule (the fleet rule of 2026-11-18,
// .claude/rules/fleet/idempotency-and-outbox.md) through the REAL chi router
// over a real Postgres 16: a refusal whose cause is state that can change
// (RETRYABLE) is NOT stored — the key row rolls back with the request — so
// the retry under the same Idempotency-Key executes again and only its final
// answer is stored and replayed.
package http_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/packview"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// gatePublisher wraps a ports.EventPublisher and lets a test hold the first
// published event — which happens INSIDE the request transaction, before the
// claim commits — so a second request provably races the winner.
type gatePublisher struct {
	delegate ports.EventPublisher
	gate     func()
}

func (p *gatePublisher) Publish(ctx context.Context, evs ...shared.DomainEvent) error {
	if p.gate != nil {
		p.gate()
	}
	return p.delegate.Publish(ctx, evs...)
}

func keyRows(t *testing.T, pool *pgxpool.Pool, key string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM idempotency_keys WHERE key = $1`, key).Scan(&n); err != nil {
		t.Fatalf("count key rows: %v", err)
	}
	return n
}

// waitForLockWaiter blocks until some session of the test database waits on
// a row lock: the loser of the race is then provably queued behind the
// winner's uncommitted write.
func waitForLockWaiter(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&n); err != nil {
			t.Fatalf("probe lock waiters: %v", err)
		}
		if n > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no session ever waited on a row lock: the race did not happen")
}

// claimFixture wires the real router over Postgres with one registered
// station and a gate on the publisher.
type claimFixture struct {
	pool      *pgxpool.Pool
	router    http.Handler
	tasks     *postgres.TaskRepo
	publisher *gatePublisher
	now       time.Time
}

func newClaimFixture(t *testing.T) *claimFixture {
	t.Helper()
	pool := idempotencyDB(t)
	ctx := context.Background()
	tasks := postgres.NewTaskRepo(pool)
	stations := postgres.NewStationRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	now := time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)

	st := station.New(shared.StationId("station-race"), shared.NewCapabilitySet("pick"))
	if err := stations.Save(ctx, st); err != nil {
		t.Fatalf("save station: %v", err)
	}

	publisher := &gatePublisher{delegate: events.NewLogPublisher(slog.New(slog.NewTextHandler(io.Discard, nil)))}
	handlers := &inboundhttp.Handlers{
		ClaimNext: &usecases.ClaimNext{
			Tasks: tasks, Stations: stations, Publisher: publisher,
			Clock: fixedClockAt(now), LeaseDuration: time.Hour, UnitOfWork: uow,
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &claimFixture{
		pool:      pool,
		tasks:     tasks,
		publisher: publisher,
		now:       now,
		router:    inboundhttp.NewRouter(handlers, logger, inboundhttp.WithIdempotencyPool(pool)),
	}
}

func (f *claimFixture) seedTask(t *testing.T, id string) {
	t.Helper()
	tk := task.New(shared.TaskId(id), task.Pick, shared.NewCPT(f.now.Add(time.Hour)), "order-race",
		shared.NewCapabilitySet("pick"), false, false)
	if err := f.tasks.Save(context.Background(), tk); err != nil {
		t.Fatalf("save task %s: %v", id, err)
	}
}

func (f *claimFixture) claim(t *testing.T, key string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/stations/station-race/task-claims",
		strings.NewReader(`{"taskType":"PICK"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)
	return rec
}

// RETRYABLE (the concurrent-update race of this service): two claims race
// for the SAME single pending task; the loser's compare-and-set matches no
// still-claimable row and it answers 409 no-claimable-task. That refusal
// depends on state that changed under the loser, so it is NOT stored: the
// loser's key row is absent, its retry under the same key executes once a
// new task exists, succeeds, and only then is stored and replayed.
func TestReplayClass_ClaimRaceLoserIsReleasedAndItsRetryExecutes(t *testing.T) {
	f := newClaimFixture(t)
	f.seedTask(t, "race-task-1")

	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.publisher.gate = func() { once.Do(func() { close(reached); <-release }) }

	var winner, loser *httptest.ResponseRecorder
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); winner = f.claim(t, "race-winner") }()
	<-reached // the winner holds the claimed row and has not committed
	wg.Add(1)
	go func() { defer wg.Done(); loser = f.claim(t, "race-loser") }()
	waitForLockWaiter(t, f.pool)
	close(release)
	wg.Wait()

	if winner.Code != http.StatusOK {
		t.Fatalf("winner = %d %s, want 200", winner.Code, winner.Body.String())
	}
	if loser.Code != http.StatusConflict || problemType(t, loser) != "no-claimable-task" {
		t.Fatalf("loser = %d %s, want 409 no-claimable-task", loser.Code, loser.Body.String())
	}
	if n := keyRows(t, f.pool, "race-loser"); n != 0 {
		t.Fatalf("key rows of the loser = %d, want 0: a RETRYABLE refusal is not stored", n)
	}
	if n := keyRows(t, f.pool, "race-winner"); n != 1 {
		t.Fatalf("key rows of the winner = %d, want 1", n)
	}

	// The pool offers work again: the retry under the SAME key executes and
	// its 200 is stored and replayed.
	f.seedTask(t, "race-task-2")
	retry := f.claim(t, "race-loser")
	if retry.Code != http.StatusOK {
		t.Fatalf("retry under the same key = %d %s, want 200: the retry must execute", retry.Code, retry.Body.String())
	}
	if got := retry.Header().Get(inboundhttp.IdempotencyReplayedHeader); got != "" {
		t.Fatalf("the retry carries %s: %q — it executed, it is not a replay", inboundhttp.IdempotencyReplayedHeader, got)
	}
	if !strings.Contains(retry.Body.String(), "race-task-2") {
		t.Fatalf("retry claimed %s, want race-task-2 (the work that appeared after the refusal)", retry.Body.String())
	}
	if n := keyRows(t, f.pool, "race-loser"); n != 1 {
		t.Fatalf("key rows after the successful retry = %d, want 1", n)
	}
	replay := f.claim(t, "race-loser")
	if replay.Code != http.StatusOK || replay.Body.String() != retry.Body.String() ||
		replay.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" {
		t.Fatalf("replay = %d replayed=%q %s, want the stored 200 replayed",
			replay.Code, replay.Header().Get(inboundhttp.IdempotencyReplayedHeader), replay.Body.String())
	}
}

// RETRYABLE: a referenced fact not known yet. The scanned GTIN is valid but
// the local PackView does not know it (product-master may declare it later);
// the refusal is NOT stored, so the retry under the same key finds the pack
// level once it is known and succeeds.
func TestReplayClass_UnknownGTINIsReleasedSoTheRetryFindsThePackOnceItIsKnown(t *testing.T) {
	f := newScanFixture(t, 10)
	const lateGTIN = "00012345678912" // a valid GTIN-14 the PackView does not know yet
	body := `{"stationId":"` + scanStation + `","gtin":"` + lateGTIN + `","count":1}`

	first := f.scan("pick-1", "scan-late-gtin", body)
	if first.Code != http.StatusConflict || problemType(t, first) != "unknown-gtin" {
		t.Fatalf("first = %d %s, want 409 unknown-gtin", first.Code, first.Body.String())
	}
	if n := keyRows(t, f.pool, "scan-late-gtin"); n != 0 {
		t.Fatalf("key rows after a RETRYABLE refusal = %d, want 0: the key must be released", n)
	}

	packs := packview.NewPostgresStore(f.pool)
	if _, err := packs.UpsertIfNewer(context.Background(), ports.PackRecord{
		GTIN: lateGTIN, SKU: "SKU-1", Level: task.PackLevelEach, UnitsPerPack: 1, Version: 1,
		EffectiveFrom: time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("declare the GTIN: %v", err)
	}

	retry := f.scan("pick-1", "scan-late-gtin", body)
	if retry.Code != http.StatusOK {
		t.Fatalf("retry = %d %s, want 200 now the GTIN is known", retry.Code, retry.Body.String())
	}
	if got := retry.Header().Get(inboundhttp.IdempotencyReplayedHeader); got != "" {
		t.Fatalf("the retry carries %s: %q — it executed, it is not a replay", inboundhttp.IdempotencyReplayedHeader, got)
	}
	if got := f.picked(t); got != 1 {
		t.Fatalf("pickedEaches = %d, want 1 (the retried scan counted once)", got)
	}
	if n := keyRows(t, f.pool, "scan-late-gtin"); n != 1 {
		t.Fatalf("key rows after the successful retry = %d, want 1", n)
	}
	replay := f.scan("pick-1", "scan-late-gtin", body)
	if replay.Code != http.StatusOK || replay.Body.String() != retry.Body.String() ||
		replay.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" {
		t.Fatalf("replay = %d replayed=%q %s, want the stored 200 replayed",
			replay.Code, replay.Header().Get(inboundhttp.IdempotencyReplayedHeader), replay.Body.String())
	}
	if got := f.picked(t); got != 1 {
		t.Fatalf("pickedEaches after the replay = %d, want still 1", got)
	}
}
