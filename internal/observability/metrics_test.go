package observability_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/fulfillment-execution/internal/domain/task"
	"github.com/claudioed/fulfillment-execution/internal/observability"
)

// installTestMeterProvider makes the global MeterProvider a manual-reader
// one, so a test can collect exactly what was recorded without a Collector.
func installTestMeterProvider(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	return reader
}

// findMetric returns the collected metric with the given name.
func findMetric(t *testing.T, reader *sdkmetric.ManualReader, name string) metricdata.Metrics {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == name {
				return m
			}
		}
	}
	t.Fatalf("metric %q was never recorded", name)
	return metricdata.Metrics{}
}

func TestMetrics_CountsClaimedAndCompletedByTaskType(t *testing.T) {
	reader := installTestMeterProvider(t)

	metrics, err := observability.NewMetrics()
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	ctx := context.Background()
	metrics.TaskClaimed(ctx, task.Pick)
	metrics.TaskClaimed(ctx, task.Pick)
	metrics.TaskClaimed(ctx, task.Pack)
	metrics.TaskCompleted(ctx, task.Slam)

	claimed := findMetric(t, reader, "fulfillment.tasks.claimed")
	sum, ok := claimed.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("fulfillment.tasks.claimed is %T, want an int64 sum", claimed.Data)
	}

	byType := map[string]int64{}
	for _, dp := range sum.DataPoints {
		v, present := dp.Attributes.Value(attribute.Key("task.type"))
		if !present {
			t.Fatalf("data point has no task.type attribute: %v", dp.Attributes)
		}
		byType[v.AsString()] = dp.Value
	}

	if byType[string(task.Pick)] != 2 {
		t.Errorf("claimed[Pick] = %d, want 2", byType[string(task.Pick)])
	}
	if byType[string(task.Pack)] != 1 {
		t.Errorf("claimed[Pack] = %d, want 1", byType[string(task.Pack)])
	}

	completed := findMetric(t, reader, "fulfillment.tasks.completed")
	completedSum, ok := completed.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("fulfillment.tasks.completed is %T, want an int64 sum", completed.Data)
	}
	if len(completedSum.DataPoints) != 1 || completedSum.DataPoints[0].Value != 1 {
		t.Errorf("completed data points = %v, want a single point of 1", completedSum.DataPoints)
	}
}
