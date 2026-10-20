//go:build integration

package http_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	inboundhttp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
)

// TestNUL_IsAClientErrorAndDoesNotConsumeTheKey pins, on a real Postgres, the
// defect the in-memory boot hid: a fuzzed U+0000 in a text field was a 500
// (SQLSTATE 22021, then 25P02) and the Idempotency-Key it carried was left
// recorded. Now the NUL is a 400 (body) or a 404 (path), no task row and no
// idempotency_keys row is written, and the same key is still free for the
// clean retry.
func TestNUL_IsAClientErrorAndDoesNotConsumeTheKey(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	post := func(path, key, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	const key = "key-nul-1"
	nulBody := "{\"type\":\"PICK\",\"cpt\":\"2026-09-27T18:00:00Z\",\"orderRef\":\"order\x00-1\",\"requiredCapabilities\":[\"pick\"]}"
	if rec := post("/tasks", key, nulBody); rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "malformed-request-body") {
		t.Fatalf("raw NUL in the body = %d %s, want 400 malformed-request-body", rec.Code, rec.Body.String())
	}
	escaped := strings.ReplaceAll(nulBody, "\x00", `\u0000`)
	if rec := post("/tasks", key, escaped); rec.Code != http.StatusBadRequest {
		t.Fatalf("escaped NUL in the body = %d %s, want 400", rec.Code, rec.Body.String())
	}
	if rec := post("/tasks/a%00b/complete", key, `{}`); rec.Code != http.StatusNotFound {
		t.Fatalf("NUL in the path = %d %s, want 404", rec.Code, rec.Body.String())
	}

	var keys int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM idempotency_keys WHERE key = $1", key).Scan(&keys); err != nil {
		t.Fatal(err)
	}
	if keys != 0 || countTaskRows(t, pool) != 0 {
		t.Fatalf("a rejected NUL left %d idempotency key(s) and %d task(s), want none", keys, countTaskRows(t, pool))
	}

	// The key is still free: the clean request with it succeeds.
	if rec := post("/tasks", key, validTaskBody); rec.Code != http.StatusCreated {
		t.Fatalf("clean retry with the same key = %d %s, want 201", rec.Code, rec.Body.String())
	}
}
