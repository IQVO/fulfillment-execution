package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/observability"
)

// buildMetrics creates the task counters. A failure is logged, never
// returned: a missing counter must not stop the service from doing work.
func buildMetrics(logger *slog.Logger) *observability.Metrics {
	metrics, err := observability.NewMetrics()
	if err != nil {
		logger.Error("task metrics unavailable", "error", err)
		return nil
	}
	return metrics
}

// setupTelemetry wires OTel and returns the shutdown flush, which the
// caller defers. A setup error is logged, not returned: an unreachable
// Collector must not stop the service.
func setupTelemetry(rootCtx context.Context, logger *slog.Logger) func(context.Context) error {
	serviceName := observability.ServiceName()
	otelShutdown, err := observability.Setup(rootCtx, serviceName, observability.ServiceVersion(), observability.Endpoint())
	if err != nil {
		logger.Error("opentelemetry setup degraded", "error", err)
	}
	if otelShutdown == nil {
		logger.Warn("opentelemetry disabled; traces and metrics will not be exported")
	}
	logger.Info("telemetry configured",
		"service_name", serviceName,
		"service_version", observability.ServiceVersion(),
		"otlp_endpoint", observability.Endpoint(),
	)
	return otelShutdown
}

// telemetryCloser returns the func that flushes OTel on exit (a no-op when
// telemetry is disabled), bounded to 5s.
func telemetryCloser(otelShutdown func(context.Context) error, logger *slog.Logger) func() {
	if otelShutdown == nil {
		return func() {}
	}
	return func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := otelShutdown(flushCtx); err != nil {
			logger.Error("opentelemetry shutdown failed", "error", err)
		}
	}
}

// newLogger builds a JSON slog.Logger writing to stdout, with its minimum
// level set from a LOG_LEVEL value (debug|info|warn|warning|error,
// case-insensitive, defaulting to info for anything else). The JSON handler
// is wrapped so records logged with a context carrying an active span also
// carry that span's trace_id and span_id.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(observability.NewSlogHandler(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}),
	))
}
