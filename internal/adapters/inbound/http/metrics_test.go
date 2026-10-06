package http_test

import (
	"context"
	"log/slog"
	stdhttp "net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

// installMeterProvider makes the global MeterProvider a manual-reader one.
// It must run BEFORE the router is built: otelchimetric resolves its Meter
// from the global provider when the middleware is constructed.
func installMeterProvider(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	return reader
}

// requestDuration returns the single data point of the
// http.server.request.duration histogram (ADR-0019 Tier 1, item 2).
func requestDuration(t *testing.T, reader *sdkmetric.ManualReader) metricdata.HistogramDataPoint[float64] {
	t.Helper()

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
			if !ok {
				t.Fatalf("http.server.request.duration is %T, want a float64 histogram", m.Data)
			}
			if m.Unit != "s" {
				t.Errorf("unit = %q, want %q", m.Unit, "s")
			}
			if len(hist.DataPoints) != 1 {
				t.Fatalf("data points = %d, want 1", len(hist.DataPoints))
			}
			return hist.DataPoints[0]
		}
	}
	t.Fatal("http.server.request.duration was never recorded; otelchimetric is not wired into the router")
	return metricdata.HistogramDataPoint[float64]{}
}

// The main router records the duration histogram through the sanctioned
// otelchimetric middleware, labelled with the chi route PATTERN (not the raw
// path) so the series stays low-cardinality.
func TestRouter_RecordsRequestDurationByRoutePattern(t *testing.T) {
	reader := installMeterProvider(t)

	router := http.NewRouter(&http.Handlers{
		GetQueueDepth: &usecases.GetQueueDepth{Tasks: memory.NewTaskRepo()},
	}, slog.Default())

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(stdhttp.MethodGet, "/queues/Pick/depth", nil))
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	attrs := requestDuration(t, reader).Attributes
	route, ok := attrs.Value(attribute.Key("http.route"))
	if !ok || route.AsString() != "/queues/{taskType}/depth" {
		t.Errorf("http.route = %v, want the route pattern %q", route, "/queues/{taskType}/depth")
	}
}

// The reports reader router carries the same instrumentation.
func TestReportsRouter_RecordsRequestDurationByRoutePattern(t *testing.T) {
	reader := installMeterProvider(t)

	router := http.NewReportsRouter(&http.ReportsHandlers{}, slog.Default())

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(stdhttp.MethodGet, "/healthz", nil))
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	attrs := requestDuration(t, reader).Attributes
	route, ok := attrs.Value(attribute.Key("http.route"))
	if !ok || route.AsString() != "/healthz" {
		t.Errorf("http.route = %v, want %q", route, "/healthz")
	}
}

// An unmatched path must never become a metric attribute.
func TestRouter_UnmatchedRouteOmitsRouteAttribute(t *testing.T) {
	reader := installMeterProvider(t)

	router := http.NewRouter(&http.Handlers{}, slog.Default())

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(stdhttp.MethodGet, "/no/such/path", nil))

	attrs := requestDuration(t, reader).Attributes
	if _, ok := attrs.Value(attribute.Key("http.route")); ok {
		t.Error("http.route recorded for an unmatched request; the raw path must never become an attribute")
	}
}
