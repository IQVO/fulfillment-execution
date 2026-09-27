// Package bootretry provides the fleet's standard boot-time dial retry.
//
// Every injected pod's FIRST outbound TCP dial (Postgres, Kafka) in this
// cluster fails with "read: connection reset by peer" ~10s after the app
// starts: Istio 1.30 runs native sidecars here (istio-proxy is an init
// container with restartPolicy=Always), so `holdApplicationUntilProxyStarts`
// is a no-op for them and there is no way to make the app wait for the
// proxy to finish warming up. A composition root that dials once and fails
// closed on that turns a transient ~10s window into a permanent
// CrashLoopBackOff.
//
// Retry (and RetryWithDelay, its base-delay-injectable form so tests do not
// have to sleep out the real budget) wraps exactly the boot-time dials that
// must succeed before a process can call itself ready: running migrations,
// pinging a pool, or opening a Kafka reader used synchronously at startup.
// It is NOT a general request-retry helper and must never be used on the
// request hot path — it exists only to survive the one known transient
// condition at boot, and it still fails closed (returning the last real
// error) once the budget is exhausted.
package bootretry

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// BootRetries and BootRetryDelay bound the startup retry budget.
//
// ~31s total (1+2+4+8+16), comfortably past the ~10s first-dial reset and
// still far inside the liveness probe's own tolerance, so a genuinely
// unreachable database or broker still fails the pod rather than hanging
// it.
const (
	BootRetries    = 5
	BootRetryDelay = time.Second
)

// Retry runs op with exponential backoff starting at BootRetryDelay,
// returning the LAST error so a permanent failure still reports its real
// cause rather than a generic timeout.
func Retry(ctx context.Context, logger *slog.Logger, what string, op func() error) error {
	return RetryWithDelay(ctx, logger, what, BootRetryDelay, op)
}

// RetryWithDelay is Retry with the base delay injected, so tests can
// exercise the give-up path without sleeping out the real ~31s budget.
func RetryWithDelay(ctx context.Context, logger *slog.Logger, what string, base time.Duration, op func() error) error {
	if logger == nil {
		logger = slog.Default()
	}
	delay := base
	var err error
	for attempt := 1; attempt <= BootRetries; attempt++ {
		if err = op(); err == nil {
			if attempt > 1 {
				logger.Info("succeeded after retry", "op", what, "attempt", attempt)
			}
			return nil
		}
		if attempt == BootRetries {
			break
		}
		logger.Warn("retrying", "op", what, "attempt", attempt, "in", delay, "err", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", what, ctx.Err())
		case <-time.After(delay):
		}
		delay *= 2
	}
	return fmt.Errorf("%s (after %d attempts): %w", what, BootRetries, err)
}
