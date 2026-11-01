//go:build integration

// REST guideline section 2 / 3 against a real Postgres (testcontainers, see
// main_integration_test.go) through the REAL chi router: the replay header,
// the global key, key validation, every POST requiring a key, a 5xx never
// being stored, N concurrent same-key requests and keyset paging.
package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	inboundhttp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
)

func postKeyed(router http.Handler, path, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func slugOf(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var p struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode problem %q: %v", rec.Body.String(), err)
	}
	return p.Type[strings.LastIndex(p.Type, "/")+1:]
}

func TestIdempotency_ReplayCarriesIdempotencyReplayedOnlyOnReplay(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	first := postKeyed(router, "/tasks", "key-replayed-1", validTaskBody)
	if first.Code != http.StatusCreated {
		t.Fatalf("first = %d (%s)", first.Code, first.Body.String())
	}
	if got := first.Header().Get(inboundhttp.IdempotencyReplayedHeader); got != "" {
		t.Fatalf("first execution carries Idempotency-Replayed = %q, want none", got)
	}
	second := postKeyed(router, "/tasks", "key-replayed-1", validTaskBody)
	if got := second.Header().Get("Idempotency-Replayed"); got != "true" {
		t.Fatalf("replay Idempotency-Replayed = %q, want true", got)
	}
	if second.Body.String() != first.Body.String() || second.Header().Get("Location") != first.Header().Get("Location") {
		t.Fatalf("replay is not byte-identical:\n%s\n%s", first.Body.String(), second.Body.String())
	}
	if second.Header().Get("Idempotent-Replayed") != "" {
		t.Fatal("the retired Idempotent-Replayed spelling must not be sent")
	}
}

// The key is global to the service: the same key on another route (or with
// another method) is a different request, never a fresh key.
func TestIdempotency_KeyIsGlobalAcrossRoutes(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	if rec := postKeyed(router, "/tasks", "key-global-1", validTaskBody); rec.Code != http.StatusCreated {
		t.Fatalf("first = %d (%s)", rec.Code, rec.Body.String())
	}
	for _, path := range []string{"/stations", "/task-lease-expirations", "/cpt-miss-sweeps", "/tasks/task-1/scans"} {
		rec := postKeyed(router, path, "key-global-1", validTaskBody)
		if rec.Code != http.StatusConflict || slugOf(t, rec) != "idempotency-key-reused" {
			t.Fatalf("same key on %s = %d %s, want 409 idempotency-key-reused", path, rec.Code, rec.Body.String())
		}
	}
	if got := countTaskRows(t, pool); got != 1 {
		t.Fatalf("task rows = %d, want 1", got)
	}
}

func TestIdempotency_MalformedKeyIs400Invalid(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	for name, key := range map[string]string{
		"129 chars":  strings.Repeat("k", 129),
		"DEL char":   "key\x7fend",
		"non-ASCII":  "kéy",
		"tab inside": "key\tend",
	} {
		rec := postKeyed(router, "/tasks", key, validTaskBody)
		if rec.Code != http.StatusBadRequest || slugOf(t, rec) != "idempotency-key-invalid" {
			t.Fatalf("%s: = %d %s, want 400 idempotency-key-invalid", name, rec.Code, rec.Body.String())
		}
	}
	if rec := postKeyed(router, "/tasks", strings.Repeat("k", 128), validTaskBody); rec.Code != http.StatusCreated {
		t.Fatalf("a 128-character key = %d (%s), want 201", rec.Code, rec.Body.String())
	}
	if got := countTaskRows(t, pool); got != 1 {
		t.Fatalf("task rows = %d, want 1 (rejected keys create nothing)", got)
	}
}

// Every POST route sits behind the middleware: with no key it is 400 before
// any handler (nil use cases here would panic) runs.
func TestIdempotency_EveryPostRouteRequiresAKey(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	for _, path := range []string{
		"/stations", "/tasks", "/stations/s1/task-claims", "/stations/s1/check-ins", "/stations/s1/check-outs",
		"/tasks/t1/lease-renewals", "/tasks/t1/completions", "/tasks/t1/scans", "/tasks/t1/case-breaks",
		"/tasks/t1/package-seals", "/packages/p1/slam-runs", "/task-lease-expirations", "/cpt-miss-sweeps",
		"/rebin/arrivals", "/paths/path-1/work-requests",
	} {
		rec := postKeyed(router, path, "", `{}`)
		if rec.Code != http.StatusBadRequest || slugOf(t, rec) != "idempotency-key-required" {
			t.Fatalf("POST %s without a key = %d %s, want 400 idempotency-key-required", path, rec.Code, rec.Body.String())
		}
	}
}

// A 5xx is never stored: the unit rolls back and the retry re-executes.
func TestIdempotency_5xxIsNotStoredAndTheRetryReExecutes(t *testing.T) {
	pool := idempotencyDB(t)
	calls := 0
	h := inboundhttp.RequireIdempotencyKey(pool)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))

	if rec := postKeyed(h, "/x", "key-5xx-1", `{}`); rec.Code != http.StatusInternalServerError {
		t.Fatalf("first = %d, want 500", rec.Code)
	}
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM idempotency_keys WHERE key = $1", "key-5xx-1").Scan(&n); err != nil || n != 0 {
		t.Fatalf("stored rows after a 5xx = %d (%v), want 0", n, err)
	}
	rec := postKeyed(h, "/x", "key-5xx-1", `{}`)
	if rec.Code != http.StatusCreated || calls != 2 || rec.Header().Get("Idempotency-Replayed") != "" {
		t.Fatalf("retry = %d calls=%d replayed=%q, want a fresh 201 execution", rec.Code, calls, rec.Header().Get("Idempotency-Replayed"))
	}
}

// N concurrent requests with one key: exactly one aggregate row, every
// caller sees the same 201, and all but the winner are replays.
func TestIdempotency_ConcurrentSameKey_OneRowManyReplays(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	const n = 12
	recs := make([]*httptest.ResponseRecorder, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			recs[i] = postKeyed(router, "/tasks", "key-concurrent-n", validTaskBody)
		}()
	}
	wg.Wait()

	replays := 0
	for _, rec := range recs {
		if rec.Code != http.StatusCreated || rec.Body.String() != recs[0].Body.String() {
			t.Fatalf("a concurrent caller got %d %s", rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Idempotency-Replayed") == "true" {
			replays++
		}
	}
	if replays != n-1 {
		t.Fatalf("replays = %d, want %d", replays, n-1)
	}
	if got := countTaskRows(t, pool); got != 1 {
		t.Fatalf("task rows = %d, want exactly 1", got)
	}
}

// GET /tasks over Postgres: more rows than limit, every page walked, no
// duplicate and no skipped row.
func TestGetTasks_PostgresKeysetWalksEveryRowOnce(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	const rows = 9
	for i := 0; i < rows; i++ {
		body := `{"type":"PICK","cpt":"2026-09-27T18:00:00Z","orderRef":"order-walk","requiredCapabilities":["pick"]}`
		if rec := postKeyed(router, "/tasks", "key-walk-"+strconv.Itoa(i), body); rec.Code != http.StatusCreated {
			t.Fatalf("seed %d = %d (%s)", i, rec.Code, rec.Body.String())
		}
	}
	postKeyed(router, "/tasks", "key-walk-other", `{"type":"PICK","cpt":"2026-09-27T18:00:00Z","orderRef":"someone-else","requiredCapabilities":["pick"]}`)

	for _, limit := range []int{1, 2, 4, rows, rows + 1} {
		var ids []string
		target := "/tasks?orderRef=order-walk&limit=" + strconv.Itoa(limit)
		for pages := 0; ; pages++ {
			if pages > rows+1 {
				t.Fatal("pagination did not terminate")
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("GET %s = %d (%s)", target, rec.Code, rec.Body.String())
			}
			var p struct {
				Items []struct {
					Id string `json:"id"`
				} `json:"items"`
				NextCursor string `json:"nextCursor"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
				t.Fatalf("decode: %v", err)
			}
			for _, it := range p.Items {
				ids = append(ids, it.Id)
			}
			if p.NextCursor == "" {
				break
			}
			target = "/tasks?orderRef=order-walk&limit=" + strconv.Itoa(limit) + "&cursor=" + url.QueryEscape(p.NextCursor)
		}
		assertWalkIsComplete(t, ids, rows)
	}
}
