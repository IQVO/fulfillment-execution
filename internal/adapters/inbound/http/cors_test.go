package http_test

import (
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// preflight issues a CORS preflight request for origin against path and
// returns the recorded response.
func preflight(t *testing.T, srv stdhttp.Handler, origin, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(stdhttp.MethodOptions, path, nil)
	req.Header.Set("Origin", origin)
	req.Header.Set("Access-Control-Request-Method", method)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

// CORS is required for the browser SPAs that call this API directly (the
// warehouse-console shell and this service's own fulfillment-mfe remote).
// The default allowed origins cover local dev; CORS_ALLOWED_ORIGINS
// overrides them for other environments. No credentials are needed
// (REST is unauthenticated by decision, ADR-0022; no cookies either).
func TestCORS_Preflight_AllowsDefaultOrigin(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := preflight(t, srv, "http://localhost:5173", stdhttp.MethodGet, "/queues/PICK/depth")

	if rec.Code != stdhttp.StatusNoContent && rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected a successful preflight response, got %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:5173" {
		t.Fatalf("expected Access-Control-Allow-Origin=http://localhost:5173, got %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "" {
		t.Fatalf("expected no Access-Control-Allow-Credentials header (no cookie auth), got %q", got)
	}
}

// POST /tasks requires an Idempotency-Key header (ADR-0028); a browser can
// only send it cross-origin if the preflight allows it.
func TestCORS_Preflight_AllowsIdempotencyKeyHeader(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	req := httptest.NewRequest(stdhttp.MethodOptions, "/tasks", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	req.Header.Set("Access-Control-Request-Method", stdhttp.MethodPost)
	req.Header.Set("Access-Control-Request-Headers", "idempotency-key,content-type")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	got := strings.ToLower(rec.Header().Get("Access-Control-Allow-Headers"))
	if !strings.Contains(got, "idempotency-key") {
		t.Fatalf("Access-Control-Allow-Headers = %q, want it to include Idempotency-Key", got)
	}
}

// A second default origin (this service's own fulfillment-mfe remote dev
// origin, :5184) is allowed too, alongside the console shell.
func TestCORS_Preflight_AllowsSecondDefaultOrigin(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := preflight(t, srv, "http://localhost:5184", stdhttp.MethodPost, "/tasks")

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:5184" {
		t.Fatalf("expected Access-Control-Allow-Origin=http://localhost:5184, got %q", got)
	}
}

// An origin outside the allowed set gets no CORS headers, so the browser
// still blocks it — this proves the allowlist is enforced, not wide open.
func TestCORS_Preflight_RejectsUnknownOrigin(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := preflight(t, srv, "http://evil.example.com", stdhttp.MethodGet, "/queues/PICK/depth")

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("expected no Access-Control-Allow-Origin for an unknown origin, got %q", got)
	}
}

// The preflight is only half the exchange: the actual GET the console's
// OverviewScreen makes must carry the header too, or the browser discards
// the response body it just fetched.
func TestCORS_ActualRequest_CarriesAllowOriginHeader(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	req := httptest.NewRequest(stdhttp.MethodGet, "/queues/PICK/depth", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:5173" {
		t.Fatalf("expected Access-Control-Allow-Origin=http://localhost:5173 on the actual response, got %q", got)
	}
}

// CORS_ALLOWED_ORIGINS replaces the local-dev defaults wholesale, which is
// how staging/prod deployments point the allowlist at their real console
// origin.
func TestCORS_AllowedOriginsEnvOverridesDefaults(t *testing.T) {
	t.Setenv("CORS_ALLOWED_ORIGINS", "https://console.example.com")
	srv, _, _, _, _ := newTestServer()

	rec := preflight(t, srv, "https://console.example.com", stdhttp.MethodGet, "/queues/PICK/depth")
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://console.example.com" {
		t.Fatalf("expected the env-configured origin to be allowed, got %q", got)
	}

	rec = preflight(t, srv, "http://localhost:5173", stdhttp.MethodGet, "/queues/PICK/depth")
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("expected the default origin to be replaced, not merged, got %q", got)
	}
}
