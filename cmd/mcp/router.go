package main

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"
)

// newRouter wraps the MCP handler in a small chi router so the binary is
// deployable behind Kubernetes probes and carries the same HTTP RED
// instrumentation as every other fleet service (ADR-0019 Tier 1):
//
//   - otelchi.Middleware starts the request span (named after the route
//     pattern, via WithChiRoutes) and otelchimetric records
//     http.server.request.duration, in that order.
//   - GET /healthz answers 200 {"status":"ok"}. MCP is unauthenticated by
//     decision (ADR-0022), so probes need no credentials. It exposes nothing
//     about the service beyond "the process is serving".
//   - The MCP Streamable HTTP endpoint is mounted at BOTH "/" and "/mcp":
//     "/" keeps the original root mount working, "/mcp" matches the
//     warehouse-ops-agent `*_MCP_ENDPOINT` convention and the docs' examples.
//
// Anything else is a 404 — the router is deliberately not a catch-all.
// serviceName is the OTEL_SERVICE_NAME resolved by the composition root.
func newRouter(mcpHandler http.Handler, serviceName string) http.Handler {
	r := chi.NewRouter()
	r.Use(otelchi.Middleware(
		serviceName,
		otelchi.WithChiRoutes(r),
		otelchi.WithRequestMethodInSpanName(true),
	))
	r.Use(otelchimetric.NewServerRequestDuration(otelchimetric.NewBaseConfig(serviceName)))
	r.Get("/healthz", healthz)
	r.Handle("/", mcpHandler)
	r.Handle("/mcp", mcpHandler)
	return r
}

// healthz is the liveness endpoint. It reports only that the HTTP server is
// up; the MCP tools' own dependencies (Postgres, the reports service) are
// checked lazily per call.
func healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
