// breaker.go wraps Client with a per-dependency circuit breaker
// (sony/gobreaker/v2, ADR-0029) AND jittered retry (cenkalti/backoff/v4)
// — GetRole is a pure read (GET /locations/{locationCode}), safe to
// retry. While the
// breaker is OPEN, this falls back to PermissiveLookup's existing
// fail-open behaviour (Known=false, nil error) — the SAME fallback
// RegisterStation already treats a lookup problem as (see
// RegisterStation.Execute: any non-nil error from LocationLookup is
// already ignored, registering the station unchecked), just now also
// reachable via the breaker short-circuiting a call it never attempts.
package facilitylayout

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
	gobreaker "github.com/sony/gobreaker/v2"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/resilience"
)

// DependencyName labels this breaker's Prometheus gauge series
// (circuit_breaker_state{dependency="facility-layout"}).
const DependencyName = "facility-layout"

// maxRetryAttempts caps the jittered retry at 3 total attempts (1
// original + 2 retries).
const maxRetryAttempts = 3

// retryInitialInterval/retryMaxInterval bound the exponential-backoff-
// with-jitter schedule between attempts — short, because this whole call
// is already bounded by DefaultTimeout end to end (see GetRole's
// resilience.CallTimeout use).
const (
	retryInitialInterval = 50 * time.Millisecond
	retryMaxInterval     = 500 * time.Millisecond
)

// BreakerClient wraps Client with retry-then-circuit-breaker for GetRole.
type BreakerClient struct {
	breaker  *gobreaker.CircuitBreaker[ports.LocationRoleInfo]
	inner    *Client
	fallback *PermissiveLookup
}

var _ ports.LocationRoleLookup = (*BreakerClient)(nil)

// NewBreakerClient builds a BreakerClient wrapping inner. recorder is
// resilience.StateRecorder (typically telemetry.CircuitBreakerMetrics)
// — nil is a valid, documented no-op (see resilience.RecordStateChange),
// so a test that does not care about the metric never needs to
// construct one.
func NewBreakerClient(inner *Client, recorder resilience.StateRecorder) *BreakerClient {
	return newBreakerClient(inner, recorder, resilience.DefaultTimeout)
}

// NewBreakerClientWithTimeout is NewBreakerClient with an explicit
// breaker cooldown (gobreaker.Settings.Timeout) instead of
// resilience.DefaultTimeout, so a half-open-recovery test does not have
// to sleep for the full production cooldown in real time. Production
// code should always use NewBreakerClient; this exists for tests.
func NewBreakerClientWithTimeout(inner *Client, recorder resilience.StateRecorder, cooldown time.Duration) *BreakerClient {
	return newBreakerClient(inner, recorder, cooldown)
}

func newBreakerClient(inner *Client, recorder resilience.StateRecorder, cooldown time.Duration) *BreakerClient {
	return &BreakerClient{
		breaker: gobreaker.NewCircuitBreaker[ports.LocationRoleInfo](gobreaker.Settings{
			Name:        DependencyName,
			MaxRequests: resilience.DefaultMaxRequests,
			Interval:    resilience.DefaultInterval,
			Timeout:     cooldown,
			ReadyToTrip: resilience.ReadyToTrip,
			// The caller giving up (request cancelled) is not this
			// dependency's fault; don't let it count as a failure
			// against the breaker either way.
			IsExcluded:    func(err error) bool { return errors.Is(err, context.Canceled) },
			OnStateChange: resilience.RecordStateChange(DependencyName, recorder),
		}),
		inner:    inner,
		fallback: NewPermissiveLookup(),
	}
}

// GetRole derives its timeout from the inbound request's remaining
// deadline (capped at DefaultTimeout), retries the underlying call up to
// maxRetryAttempts times with jittered backoff, and runs the whole
// retry loop through the breaker as ONE logical call — a retry storm
// against an already-degraded dependency still only ever counts as one
// success/failure toward the breaker's trip condition, not N. While the
// breaker is OPEN (or half-open and saturated), this falls back to
// PermissiveLookup — the SAME fail-open contract RegisterStation already
// gets for a real error, just reached via a different path.
func (c *BreakerClient) GetRole(ctx context.Context, locationCode string) (ports.LocationRoleInfo, error) {
	callCtx, cancel := resilience.CallTimeout(ctx, DefaultTimeout)
	defer cancel()

	result, err := c.breaker.Execute(func() (ports.LocationRoleInfo, error) {
		return c.retryingGetRole(callCtx, locationCode)
	})
	if isBreakerRejection(err) {
		return c.fallback.GetRole(ctx, locationCode)
	}
	return result, err
}

// retryingGetRole retries inner.GetRole with jittered exponential
// backoff, bounded to maxRetryAttempts total attempts and to callCtx's
// own deadline (whichever is tighter). A 404/400 (Known=false, nil
// error) is a legitimate answer, not a failure, so it returns on the
// first attempt like a 200 does — only a transport error or an
// unexpected status (both returned as a non-nil error by Client.GetRole)
// is retried.
func (c *BreakerClient) retryingGetRole(callCtx context.Context, locationCode string) (ports.LocationRoleInfo, error) {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxRetryAttempts-1), callCtx)

	return backoff.RetryNotifyWithData(func() (ports.LocationRoleInfo, error) {
		return c.inner.GetRole(callCtx, locationCode)
	}, bounded, nil)
}

// isBreakerRejection reports whether err is gobreaker refusing to even
// attempt the call (open, or half-open and already at its probe limit)
// — the ONLY case that means "fall back to the permissive behaviour"; a
// real error FROM a call gobreaker did let through must propagate
// unchanged, exactly as it did before this breaker existed.
func isBreakerRejection(err error) bool {
	return errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests)
}
