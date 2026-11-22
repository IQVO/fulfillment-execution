// Package events provides outbound EventPublisher implementations: a log
// publisher for local/dev use and a buffered publisher for tests, both
// satisfying an interface shaped to drop in a Kafka producer later.
package events

import (
	"context"
	"log/slog"
	"sync"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// LogPublisher publishes domain events by logging them. It satisfies
// ports.EventPublisher and is a placeholder for a Kafka producer: swap it
// for an adapter that serializes DomainEvent onto a topic per EventName().
type LogPublisher struct {
	logger *slog.Logger
}

// NewLogPublisher constructs a LogPublisher writing through logger. A nil
// logger uses slog.Default().
func NewLogPublisher(logger *slog.Logger) *LogPublisher {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogPublisher{logger: logger}
}

// Publish logs each event through the context-carrying slog API so that,
// when the logger is trace-correlating, a published domain event can be tied
// back to the request or consumed message that raised it.
func (p *LogPublisher) Publish(ctx context.Context, evts ...shared.DomainEvent) error {
	for _, e := range evts {
		p.logger.InfoContext(ctx, "domain event published", "event_name", sanitizeLog(e.EventName()), "occurred_at", e.OccurredAt())
	}
	return nil
}

// BufferedPublisher accumulates published events in memory, for assertions
// in tests.
type BufferedPublisher struct {
	mu     sync.Mutex
	events []shared.DomainEvent
}

// NewBufferedPublisher constructs an empty BufferedPublisher.
func NewBufferedPublisher() *BufferedPublisher {
	return &BufferedPublisher{}
}

func (p *BufferedPublisher) Publish(_ context.Context, evts ...shared.DomainEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, evts...)
	return nil
}

// Events returns a copy of every event published so far, in order.
func (p *BufferedPublisher) Events() []shared.DomainEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]shared.DomainEvent, len(p.events))
	copy(out, p.events)
	return out
}

// MultiPublisher fans one Publish out to several EventPublishers in order.
// It is how the OLTP composition root sends the same domain events to both
// the integration topic (Publisher) and the analytics topic
// (AnalyticsPublisher) without either use case knowing there is more than one
// sink. The first publisher to error aborts the fan-out and returns that
// error, so a failed integration publish is not silently masked by a later
// success.
type MultiPublisher struct {
	publishers []publisher
}

// publisher is the shape each fan-out target satisfies; it is exactly
// ports.EventPublisher, restated here to avoid an import cycle
// (ports imports domain, this package imports domain).
type publisher interface {
	Publish(ctx context.Context, evts ...shared.DomainEvent) error
}

// NewMultiPublisher constructs a MultiPublisher fanning out to targets, in the
// order given. Nil targets are skipped so a caller can pass an optional sink
// without a nil-check at the call site.
func NewMultiPublisher(targets ...publisher) *MultiPublisher {
	kept := make([]publisher, 0, len(targets))
	for _, t := range targets {
		if t != nil {
			kept = append(kept, t)
		}
	}
	return &MultiPublisher{publishers: kept}
}

// Publish forwards evts to every configured publisher in order, stopping at
// the first error.
func (p *MultiPublisher) Publish(ctx context.Context, evts ...shared.DomainEvent) error {
	for _, pub := range p.publishers {
		if err := pub.Publish(ctx, evts...); err != nil {
			return err
		}
	}
	return nil
}
