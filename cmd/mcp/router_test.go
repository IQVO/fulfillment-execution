package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	inboundmcp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/mcp"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

// testRouter builds the real router over the real MCP handler (in-memory
// adapters), exactly as run() wires it.
func testRouter(t *testing.T) http.Handler {
	t.Helper()
	tasks := memory.NewTaskRepo()
	server := inboundmcp.NewServer(inboundmcp.Deps{
		GetQueueDepth: &usecases.GetQueueDepth{Tasks: tasks},
		Tasks:         tasks,
	})
	return newRouter(inboundmcp.Handler(server), "fulfillment-execution-mcp")
}

func TestHealthzNeedsNoCredentials(t *testing.T) {
	rec := httptest.NewRecorder()
	testRouter(t).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz: got %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if body := strings.TrimSpace(rec.Body.String()); body != `{"status":"ok"}` {
		t.Fatalf("body = %q, want {\"status\":\"ok\"}", body)
	}
}

func TestMCPMountsReachStreamableHandler(t *testing.T) {
	for _, path := range []string{"/", "/mcp"} {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json, text/event-stream")
			testRouter(t).ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("POST %s initialize: got %d, want 200 (body %q)", path, rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "fulfillment-execution-mcp") {
				t.Fatalf("initialize response does not name the server: %q", rec.Body.String())
			}
		})
	}
}

// The MCP router carries the same HTTP RED instrumentation as the REST
// routers (ADR-0019): otelchimetric records http.server.request.duration
// labelled with the route pattern.
func TestRouterRecordsRequestDuration(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	rec := httptest.NewRecorder()
	testRouter(t).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz: got %d, want 200", rec.Code)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "http.server.request.duration" {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok || len(hist.DataPoints) != 1 {
				t.Fatalf("unexpected histogram shape: %T %+v", m.Data, m.Data)
			}
			route, ok := hist.DataPoints[0].Attributes.Value(attribute.Key("http.route"))
			if !ok || route.AsString() != "/healthz" {
				t.Errorf("http.route = %v, want /healthz", route)
			}
			return
		}
	}
	t.Fatal("http.server.request.duration was not recorded by the MCP router")
}

// buildDeps must give CompleteTask the publisher and the UnitOfWork: a nil
// UnitOfWork is exactly the bug that made complete_task over MCP silent.
func TestBuildDepsWiresPublisherAndUnitOfWork(t *testing.T) {
	st := storage{tasks: memory.NewTaskRepo(), stations: memory.NewStationRepo()}
	pub, closeFn := buildPublisher(st, []string{"127.0.0.1:1"}, newLogger("error"))
	defer closeFn()

	deps := buildDeps(st, pub, nil)
	if deps.CompleteTask == nil || deps.CompleteTask.Publisher != pub {
		t.Fatal("CompleteTask must publish through the shared publisher")
	}
	if deps.GetQueueDepth == nil || deps.Tasks == nil || deps.Now == nil {
		t.Fatal("read-side deps must be wired")
	}
}

func TestMetricsPortNilSafe(t *testing.T) {
	if metricsPort(nil) != nil {
		t.Fatal("a nil *Metrics must become a nil interface, not a typed nil")
	}
}

func TestUnknownPathIsNotFound(t *testing.T) {
	rec := httptest.NewRecorder()
	testRouter(t).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /nope: got %d, want 404", rec.Code)
	}
}
