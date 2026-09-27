package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/claudioed/fulfillment-execution/internal/resilience"
)

// circuitBreakerGaugeName follows this fleet's OTel-metric-name-becomes-
// Prometheus-metric-name convention: the OTel Collector's prometheus
// exporter turns the dot-separated instrument name into the
// underscore-separated series `circuit_breaker_state` (ADR-0029),
// labelled dependency="facility-layout"|"product-classification".
const circuitBreakerGaugeName = "circuit_breaker.state"

// dependencyKey is the one attribute this gauge carries: which
// downstream dependency's breaker changed state.
var dependencyKey = attribute.Key("dependency")

// CircuitBreakerMetrics implements resilience.StateRecorder against the
// SAME global MeterProvider Metrics already uses (installed by
// observability.Setup) — this fleet has exactly one Prometheus-facing
// registry per service (the OTel Collector's own re-exporter), and every
// instrument in this package reuses it rather than standing up a second,
// parallel prometheus.Registry.
type CircuitBreakerMetrics struct {
	gauge metric.Int64Gauge
}

var _ resilience.StateRecorder = (*CircuitBreakerMetrics)(nil)

// NewCircuitBreakerMetrics registers the circuit_breaker.state gauge.
// Mirrors NewMetrics' error contract: this only fails on an invalid
// instrument name (a programming error), so a caller that would rather
// run without the metric than not at all can pass a nil
// resilience.StateRecorder instead of propagating this error fatally.
func NewCircuitBreakerMetrics() (*CircuitBreakerMetrics, error) {
	gauge, err := otel.Meter(InstrumentationName).Int64Gauge(
		circuitBreakerGaugeName,
		metric.WithDescription("Circuit breaker state per downstream dependency (0=closed, 1=half-open, 2=open)."),
		metric.WithUnit("{state}"),
	)
	if err != nil {
		return nil, err
	}
	return &CircuitBreakerMetrics{gauge: gauge}, nil
}

// SetState implements resilience.StateRecorder. state is
// gobreaker.State's own int value (0/1/2), recorded verbatim — see
// resilience.RecordStateChange's doc comment for why no translation is
// needed here.
func (m *CircuitBreakerMetrics) SetState(dependency string, state int64) {
	m.gauge.Record(context.Background(), state, metric.WithAttributes(dependencyKey.String(dependency)))
}
