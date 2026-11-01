package main

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/observability"
)

// intEnv parses key as a non-negative integer, falling back (with a WARN) on
// a malformed or negative value: like the relay interval, the cap is a
// tuning knob that never fails the boot.
func intEnv(logger *slog.Logger, key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n < 0 {
		logger.Warn("ignoring invalid integer env value", "key", key, "value", v, "default", fallback)
		return fallback
	}
	return n
}

// durationEnv parses key as a time.Duration, falling back on absence or a
// malformed/non-positive value: the relay interval is a tuning knob, not
// a contract, so it never fails the boot.
func durationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

// metricsPort converts a possibly-nil *observability.Metrics into a
// ports.Metrics, avoiding the typed-nil trap: assigning a nil *Metrics
// straight into the interface field would produce a non-nil interface whose
// method calls panic, defeating the use cases' nil check.
func metricsPort(m *observability.Metrics) ports.Metrics {
	if m == nil {
		return nil
	}
	return m
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
