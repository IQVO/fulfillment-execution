//go:build integration

package http_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
	"github.com/claudioed/fulfillment-execution/internal/pgtx"
)

// poisonTx simulates what a mapped domain conflict does to the middleware's
// transaction: a statement fails on the server (a unique violation, like a
// lost race on a unique index), the repository translates it to a typed
// error, and the handler answers 4xx — but the transaction is ABORTED from
// then on. The middleware must still record and return that truthful 4xx.
func poisonTx(t *testing.T, r *http.Request) {
	t.Helper()
	tx, ok := pgtx.TxFrom(r.Context())
	if !ok {
		t.Error("the middleware did not bind a transaction into the request context")
		return
	}
	for i := 0; i < 2; i++ {
		if _, err := tx.Exec(r.Context(), `INSERT INTO idempotency_keys (key, method, path, request_hash) VALUES ('dup', 'm', 'p', 'h')`); err != nil {
			return // the second insert aborts the transaction (23505)
		}
	}
	t.Error("the duplicate insert did not abort the transaction")
}

func poisonRequest(key string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(`{}`))
	req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	return req
}

func poisonCount(t *testing.T, pool *pgxpool.Pool, key string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM idempotency_keys WHERE key = $1`, key).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestIdempotency_A409FromAnAbortedTransactionCommitsTheOutcome: a handler
// that loses a race on a unique index answers 409, not 500, and that 409 is
// cached and replayed without re-running the handler.
func TestIdempotency_A409FromAnAbortedTransactionCommitsTheOutcome(t *testing.T) {
	pool := idempotencyDB(t)
	var calls atomic.Int64
	mw := inboundhttp.RequireIdempotencyKey(pool)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		poisonTx(t, r)
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"title":"Conflict","status":409}`))
	}))

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, poisonRequest("k-poison"))
	if first.Code != http.StatusConflict {
		t.Fatalf("first = %d %s, want the truthful 409", first.Code, first.Body.String())
	}

	var statusCode int
	if err := pool.QueryRow(t.Context(), `SELECT status_code FROM idempotency_keys WHERE key = 'k-poison'`).Scan(&statusCode); err != nil {
		t.Fatalf("the 409 outcome was not committed: %v", err)
	}
	if statusCode != http.StatusConflict {
		t.Fatalf("cached status = %d", statusCode)
	}
	if n := poisonCount(t, pool, "dup"); n != 0 {
		t.Fatalf("the handler's partial write survived (%d rows)", n)
	}

	replay := httptest.NewRecorder()
	mw.ServeHTTP(replay, poisonRequest("k-poison"))
	if replay.Code != http.StatusConflict || replay.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" {
		t.Fatalf("replay = %d replayed=%q, want the cached 409", replay.Code, replay.Header().Get(inboundhttp.IdempotencyReplayedHeader))
	}
	if replay.Body.String() != first.Body.String() {
		t.Fatalf("replay body = %s, want %s", replay.Body.String(), first.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1 (replay must not re-run it)", calls.Load())
	}
}

// TestIdempotency_A2xxFromAnAbortedTransactionIsNeverCached: a success answer
// whose writes died with the transaction is a lie if returned or cached.
func TestIdempotency_A2xxFromAnAbortedTransactionIsNeverCached(t *testing.T) {
	pool := idempotencyDB(t)
	var calls atomic.Int64
	mw := inboundhttp.RequireIdempotencyKey(pool)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			poisonTx(t, r) // only the first attempt dies inside the transaction
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"t1"}`))
	}))

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, poisonRequest("k-lie"))
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("first = %d %s, want 500 (the writes never happened)", first.Code, first.Body.String())
	}
	if n := poisonCount(t, pool, "k-lie"); n != 0 {
		t.Fatalf("a failed success left %d idempotency rows: the key could not retry", n)
	}

	second := httptest.NewRecorder()
	mw.ServeHTTP(second, poisonRequest("k-lie"))
	if second.Code != http.StatusCreated {
		t.Fatalf("retry = %d %s, want the real work to succeed", second.Code, second.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2", calls.Load())
	}
}
