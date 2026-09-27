//go:build integration

// Integration tests for the transactional Idempotency-Key middleware
// (docs/docs/adr/0028-idempotency-key-middleware.md) against a real
// Postgres 16, through the REAL chi router (inboundhttp.NewRouter) over
// real net/http requests — not the middleware's internals in isolation.
// Testcontainers-only: the test boots and owns its own disposable
// Postgres, never reads DATABASE_URL or hardcodes localhost, so CI cannot
// silently skip this contract. Mirrors order-management's own
// idempotency_integration_test.go (the fleet reference implementation)
// scenario-for-scenario, applied here to POST /tasks.
package http_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundhttp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// idempotencyDB boots a throwaway Postgres (testcontainers — the test
// owns its own database, never an external DATABASE_URL) and runs every
// migration in this repo, including the idempotency_keys one.
func idempotencyDB(t *testing.T) *pgxpool.Pool {
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

type fixedIdGenerator struct {
	mu  sync.Mutex
	n   int
	pfx string
}

func (g *fixedIdGenerator) next() shared.TaskId {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.n++
	return shared.TaskId(g.pfx + "-" + itoa(g.n))
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

// newIdempotencyRouter wires the REAL chi router (inboundhttp.NewRouter)
// with a Postgres-backed TaskRepo + UnitOfWork, so POST /tasks' full
// request cycle — idempotency bookkeeping, the task Save, the outbox
// insert — runs through the exact same transaction-join mechanism
// production uses (internal/pgtx via postgres.UnitOfWork.Execute).
func newIdempotencyRouter(t *testing.T, pool *pgxpool.Pool) http.Handler {
	t.Helper()
	tasks := postgres.NewTaskRepo(pool)
	// The log publisher (never Kafka) keeps this suite hermetic; the
	// point under test is the Postgres transaction boundary, not event
	// delivery. It is still wrapped in the SAME UnitOfWork scope as the
	// task Save via CreateTask's own atomically() call, so the
	// commit-together claim is exercised exactly as in production.
	publisher := events.NewLogPublisher(slog.New(slog.NewTextHandler(io.Discard, nil)))
	uow := postgres.NewUnitOfWork(pool)
	clock := fixedClockAt(time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC))

	handlers := &inboundhttp.Handlers{
		CreateTask: &usecases.CreateTask{
			Tasks:      tasks,
			Publisher:  publisher,
			Clock:      clock,
			NewId:      (&fixedIdGenerator{pfx: "task"}).next,
			UnitOfWork: uow,
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return inboundhttp.NewRouter(handlers, logger, inboundhttp.WithIdempotencyPool(pool))
}

type fixedClockAt time.Time

func (c fixedClockAt) Now() time.Time { return time.Time(c) }

func countTaskRows(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM tasks").Scan(&n); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	return n
}

const validTaskBody = `{"type":"PICK","cpt":"2026-09-27T18:00:00Z","orderRef":"order-1","requiredCapabilities":["pick"]}`

// TestIdempotency_FreshKey_CreatesTaskAndRecordsOutcome is scenario (a):
// a fresh key + valid body creates the task and the idempotency_keys row
// records the exact 201 outcome.
func TestIdempotency_FreshKey_CreatesTaskAndRecordsOutcome(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	req := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(validTaskBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(inboundhttp.IdempotencyKeyHeader, "key-fresh-1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
	if got := countTaskRows(t, pool); got != 1 {
		t.Fatalf("task rows = %d, want 1", got)
	}

	var statusCode int
	var responseBody []byte
	if err := pool.QueryRow(context.Background(),
		"SELECT status_code, response_body FROM idempotency_keys WHERE key = $1", "key-fresh-1",
	).Scan(&statusCode, &responseBody); err != nil {
		t.Fatalf("read idempotency row: %v", err)
	}
	if statusCode != http.StatusCreated {
		t.Fatalf("stored status_code = %d, want 201", statusCode)
	}
	if string(responseBody) != rec.Body.String() {
		t.Fatalf("stored response_body does not match what was returned to the caller")
	}
}

// TestIdempotency_Replay_SameKeySameBody_ReturnsIdenticalResponseNoDuplicate
// is scenario (b): replaying the same key + identical body returns the
// exact same 201 response and creates no second task row.
func TestIdempotency_Replay_SameKeySameBody_ReturnsIdenticalResponseNoDuplicate(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	doPost := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(validTaskBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, "key-replay-1")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	first := doPost()
	if first.Code != http.StatusCreated {
		t.Fatalf("first call status = %d, want 201 (body: %s)", first.Code, first.Body.String())
	}
	second := doPost()
	if second.Code != first.Code {
		t.Fatalf("replay status = %d, want %d", second.Code, first.Code)
	}
	if second.Body.String() != first.Body.String() {
		t.Fatalf("replay body differs:\nfirst:  %s\nsecond: %s", first.Body.String(), second.Body.String())
	}
	if second.Header().Get("Location") != first.Header().Get("Location") {
		t.Fatalf("replay Location = %q, want %q", second.Header().Get("Location"), first.Header().Get("Location"))
	}
	if got := countTaskRows(t, pool); got != 1 {
		t.Fatalf("task rows after replay = %d, want exactly 1 (no duplicate task created)", got)
	}
}

// TestIdempotency_SameKeyDifferentBody_Returns422 is scenario (c).
func TestIdempotency_SameKeyDifferentBody_Returns422(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	req1 := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(validTaskBody))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set(inboundhttp.IdempotencyKeyHeader, "key-mismatch-1")
	rec1 := httptest.NewRecorder()
	router.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusCreated {
		t.Fatalf("first call status = %d, want 201 (body: %s)", rec1.Code, rec1.Body.String())
	}

	differentBody := `{"type":"PACK","cpt":"2026-09-27T18:00:00Z","orderRef":"order-2","requiredCapabilities":["pack"]}`
	req2 := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(differentBody))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set(inboundhttp.IdempotencyKeyHeader, "key-mismatch-1")
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (body: %s)", rec2.Code, rec2.Body.String())
	}
	var problem struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode problem: %v (body: %s)", err, rec2.Body.String())
	}
	if !strings.HasSuffix(problem.Type, "idempotency-key-reused") {
		t.Fatalf("problem.type = %q, want suffix idempotency-key-reused", problem.Type)
	}
	if got := countTaskRows(t, pool); got != 1 {
		t.Fatalf("task rows = %d, want 1 (the mismatched retry must not create a second task)", got)
	}
}

// TestIdempotency_NoHeader_Returns400 is scenario (d).
func TestIdempotency_NoHeader_Returns400(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	req := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(validTaskBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body: %s)", rec.Code, rec.Body.String())
	}
	var problem struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode problem: %v (body: %s)", err, rec.Body.String())
	}
	if !strings.HasSuffix(problem.Type, "idempotency-key-required") {
		t.Fatalf("problem.type = %q, want suffix idempotency-key-required", problem.Type)
	}
	if got := countTaskRows(t, pool); got != 0 {
		t.Fatalf("task rows = %d, want 0 (no task should be created without the header)", got)
	}
}

// TestIdempotency_Concurrent_SameKeySameBody_ExactlyOneTaskCreated is
// scenario (e): the real concurrency proof — two real goroutines racing
// the exact same key + body through the real HTTP router and the real
// Postgres unique-index lock, not a sequential simulation.
func TestIdempotency_Concurrent_SameKeySameBody_ExactlyOneTaskCreated(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	const n = 5
	var wg sync.WaitGroup
	results := make([]*httptest.ResponseRecorder, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(validTaskBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(inboundhttp.IdempotencyKeyHeader, "key-concurrent-1")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			results[i] = rec
		}(i)
	}
	wg.Wait()

	for i, rec := range results {
		if rec.Code != http.StatusCreated {
			t.Fatalf("goroutine %d status = %d, want 201 (body: %s)", i, rec.Code, rec.Body.String())
		}
		if rec.Body.String() != results[0].Body.String() {
			t.Fatalf("goroutine %d body differs from goroutine 0:\n%d: %s\n0: %s", i, i, rec.Body.String(), results[0].Body.String())
		}
	}
	if got := countTaskRows(t, pool); got != 1 {
		t.Fatalf("task rows after %d concurrent identical requests = %d, want exactly 1", n, got)
	}
}

// TestIdempotency_BusinessErrorResponse_IsCachedAndReplayed is scenario
// (f): the request has an invalid task type (rejected by createTaskRequest.validate
// AFTER the idempotency row would have been inserted), and the error
// response itself is cached — a retry with the same key+body replays the
// SAME error rather than re-validating and re-deciding.
func TestIdempotency_BusinessErrorResponse_IsCachedAndReplayed(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	invalidBody := `{"type":"BOGUS","cpt":"2026-09-27T18:00:00Z","orderRef":"order-3","requiredCapabilities":["pick"]}`

	doPost := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(invalidBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, "key-business-error-1")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	first := doPost()
	if first.Code != http.StatusBadRequest {
		t.Fatalf("first call status = %d, want 400 (body: %s)", first.Code, first.Body.String())
	}

	var statusCode int
	if err := pool.QueryRow(context.Background(),
		"SELECT status_code FROM idempotency_keys WHERE key = $1", "key-business-error-1",
	).Scan(&statusCode); err != nil {
		t.Fatalf("read idempotency row: %v", err)
	}
	if statusCode != http.StatusBadRequest {
		t.Fatalf("stored status_code = %d, want 400 — the validation error response must be cached", statusCode)
	}

	second := doPost()
	if second.Code != first.Code {
		t.Fatalf("replay status = %d, want %d (cached error response)", second.Code, first.Code)
	}
	if second.Body.String() != first.Body.String() {
		t.Fatalf("replay body differs from the first (cached) error response:\nfirst:  %s\nsecond: %s", first.Body.String(), second.Body.String())
	}
	if got := countTaskRows(t, pool); got != 0 {
		t.Fatalf("task rows = %d, want 0 (the invalid task was never persisted, either time)", got)
	}
}
