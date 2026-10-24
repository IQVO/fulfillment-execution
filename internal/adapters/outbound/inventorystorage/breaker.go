package inventorystorage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	gobreaker "github.com/sony/gobreaker/v2"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/resilience"
)

// DependencyName labels this breaker's circuit_breaker_state series.
const DependencyName = "inventory-storage"

// maxRetryAttempts caps the jittered retry at 3 attempts (1 + 2 retries). A
// retry is safe: every attempt carries the same Idempotency-Key.
const maxRetryAttempts = 3

const (
	retryInitialInterval = 50 * time.Millisecond
	retryMaxInterval     = 500 * time.Millisecond
)

// BreakerClient wraps Client with retry-then-circuit-breaker. Unlike the
// read-only facility-layout lookup there is no fail-open fallback: a break
// that cannot be confirmed leaves the BREAK_CASE task open, so an open
// breaker is ports.ErrCaseBreakUnavailable.
type BreakerClient struct {
	breaker *gobreaker.CircuitBreaker[struct{}]
	inner   *Client
}

var _ ports.CaseBreaker = (*BreakerClient)(nil)

// NewBreakerClient builds a BreakerClient around inner. recorder may be nil.
func NewBreakerClient(inner *Client, recorder resilience.StateRecorder) *BreakerClient {
	return NewBreakerClientWithTimeout(inner, recorder, resilience.DefaultTimeout)
}

// NewBreakerClientWithTimeout is NewBreakerClient with an explicit breaker
// cooldown, for tests.
func NewBreakerClientWithTimeout(inner *Client, recorder resilience.StateRecorder, cooldown time.Duration) *BreakerClient {
	return &BreakerClient{
		breaker: gobreaker.NewCircuitBreaker[struct{}](gobreaker.Settings{
			Name:        DependencyName,
			MaxRequests: resilience.DefaultMaxRequests,
			Interval:    resilience.DefaultInterval,
			Timeout:     cooldown,
			ReadyToTrip: resilience.ReadyToTrip,
			// The caller giving up, and a business rejection (the dependency
			// answered), are not the dependency failing.
			IsExcluded: func(err error) bool {
				return errors.Is(err, context.Canceled) || errors.Is(err, ports.ErrCaseBreakRejected)
			},
			OnStateChange: resilience.RecordStateChange(DependencyName, recorder),
		}),
		inner: inner,
	}
}

// BreakCase retries transient failures with jittered backoff inside one
// breaker call; a rejection is returned at once.
func (c *BreakerClient) BreakCase(ctx context.Context, r ports.CaseBreakRequest) error {
	callCtx, cancel := resilience.CallTimeout(ctx, DefaultTimeout)
	defer cancel()

	_, err := c.breaker.Execute(func() (struct{}, error) {
		policy := backoff.NewExponentialBackOff(
			backoff.WithInitialInterval(retryInitialInterval),
			backoff.WithMaxInterval(retryMaxInterval),
		)
		bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxRetryAttempts-1), callCtx)
		return struct{}{}, backoff.Retry(func() error {
			err := c.inner.BreakCase(callCtx, r)
			if err != nil && !errors.Is(err, ports.ErrCaseBreakUnavailable) {
				return backoff.Permanent(err)
			}
			return err
		}, bounded)
	})
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return fmt.Errorf("%w: circuit breaker %s: %w", ports.ErrCaseBreakUnavailable, DependencyName, err)
	}
	return err
}
