package kafka

import (
	"context"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"
	kafkago "github.com/segmentio/kafka-go"
)

// messageReader is the slice of *kafkago.Reader the consumers' read loop
// uses; an interface so broker-outage recovery is unit-testable.
type messageReader interface {
	ReadMessage(ctx context.Context) (kafkago.Message, error)
}

// Broker-outage recovery bounds for readRecovering.
const (
	recoverInitialInterval = 250 * time.Millisecond
	recoverMaxInterval     = 30 * time.Second
)

// newRecoverBackoff never gives up (MaxElapsedTime 0): a consumer is meant
// to outlive any broker outage.
func newRecoverBackoff() backoff.BackOff {
	return backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(recoverInitialInterval),
		backoff.WithMaxInterval(recoverMaxInterval),
		backoff.WithMaxElapsedTime(0),
	)
}

// readRecovering reads the next message, riding out broker-side failures.
//
// Observed live (warehouse-day simulation): the shared broker was
// OOM-killed and restarted; ReadMessage failed once with "fetching message:
// read tcp ...: i/o timeout", Run returned it, and main only logged "kafka
// consumer stopped" -- the process stayed up and healthy while no
// WorkReleased was ever turned into a task again (lag 1,063 and growing).
//
// Any read error other than ctx ending is now logged at WARN and retried
// with jittered exponential backoff (250ms doubling to 30s, never giving
// up); kafka-go re-dials the broker and rejoins the group on the next call.
// It returns ok=false only when ctx is done.
func readRecovering(ctx context.Context, r messageReader, policy backoff.BackOff, logger *slog.Logger, topic string) (kafkago.Message, bool) {
	for {
		msg, err := r.ReadMessage(ctx)
		if err == nil {
			policy.Reset()
			return msg, true
		}
		if ctx.Err() != nil {
			return kafkago.Message{}, false
		}
		wait := policy.NextBackOff()
		if logger != nil {
			logger.WarnContext(ctx, "kafka read failed; retrying",
				"topic", topic, "retry_in", wait.String(), "error", err)
		}
		select {
		case <-ctx.Done():
			return kafkago.Message{}, false
		case <-time.After(wait):
		}
	}
}
