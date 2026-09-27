package observability_test

import (
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/fulfillment-execution/internal/observability"
	"github.com/claudioed/fulfillment-execution/internal/resilience"
)

// TestCircuitBreakerMetrics_ImplementsStateRecorder is a compile-time
// contract check duplicated as a runtime assertion for readability.
func TestCircuitBreakerMetrics_ImplementsStateRecorder(t *testing.T) {
	var _ resilience.StateRecorder = (*observability.CircuitBreakerMetrics)(nil)
}

func TestCircuitBreakerMetrics_SetState_RecordsGaugeWithDependencyLabel(t *testing.T) {
	reader := installTestMeterProvider(t)

	m, err := observability.NewCircuitBreakerMetrics()
	if err != nil {
		t.Fatalf("NewCircuitBreakerMetrics: %v", err)
	}

	m.SetState("facility-layout", 2) // gobreaker.StateOpen

	got := findMetric(t, reader, "circuit_breaker.state")
	gauge, ok := got.Data.(metricdata.Gauge[int64])
	if !ok {
		t.Fatalf("circuit_breaker.state is not an int64 gauge: %T", got.Data)
	}
	if len(gauge.DataPoints) != 1 {
		t.Fatalf("expected exactly 1 data point, got %d", len(gauge.DataPoints))
	}
	dp := gauge.DataPoints[0]
	if dp.Value != 2 {
		t.Fatalf("gauge value = %d, want 2 (open)", dp.Value)
	}
	dep, ok := dp.Attributes.Value(("dependency"))
	if !ok || dep.AsString() != "facility-layout" {
		t.Fatalf("dependency attribute = %v, want facility-layout", dep)
	}
}

func TestCircuitBreakerMetrics_SetState_SeparateDependenciesGetSeparateSeries(t *testing.T) {
	reader := installTestMeterProvider(t)

	m, err := observability.NewCircuitBreakerMetrics()
	if err != nil {
		t.Fatalf("NewCircuitBreakerMetrics: %v", err)
	}

	m.SetState("facility-layout", 2)
	m.SetState("product-classification", 0)

	got := findMetric(t, reader, "circuit_breaker.state")
	gauge, ok := got.Data.(metricdata.Gauge[int64])
	if !ok {
		t.Fatalf("circuit_breaker.state is not an int64 gauge: %T", got.Data)
	}
	if len(gauge.DataPoints) != 2 {
		t.Fatalf("expected 2 independent data points (one per dependency), got %d", len(gauge.DataPoints))
	}
}

func TestRecordStateChange_NilRecorderIsNoop(t *testing.T) {
	fn := resilience.RecordStateChange("facility-layout", nil)
	// Must not panic.
	fn("facility-layout", 0, 2)
}
