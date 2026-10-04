package composition_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/composition"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// lazyPool returns a pgxpool that never dials (pgxpool connects lazily), so
// the wiring can be asserted without a database.
func lazyPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestBuildEventPublisher_DefaultsToLogPublisher(t *testing.T) {
	for _, kind := range []string{"", "log", "anything-else"} {
		pub, relay, closeFn := composition.BuildEventPublisher(
			composition.PublisherConfig{Kind: kind, RunRelay: true},
			nil, memory.NewTaskRepo(), memory.NewStationRepo(), quietLogger())
		if _, ok := pub.(*events.LogPublisher); !ok {
			t.Errorf("kind %q: publisher = %T, want *events.LogPublisher", kind, pub)
		}
		if relay != nil {
			t.Errorf("kind %q: relay must be nil for the log publisher", kind)
		}
		closeFn()
	}
}

func TestBuildEventPublisher_KafkaWithoutPoolPublishesDirect(t *testing.T) {
	pub, relay, closeFn := composition.BuildEventPublisher(
		composition.PublisherConfig{Kind: composition.KafkaPublisher, Brokers: []string{"127.0.0.1:1"}, RunRelay: true},
		nil, memory.NewTaskRepo(), memory.NewStationRepo(), quietLogger())
	defer closeFn()

	if _, ok := pub.(*events.MultiPublisher); !ok {
		t.Errorf("publisher = %T, want *events.MultiPublisher (direct mode)", pub)
	}
	if relay != nil {
		t.Error("direct mode has no outbox, so no relay")
	}
}

// With Postgres, the publisher is ALWAYS the transactional OutboxPublisher
// (ADR-0020). The relay exists only for the process that asked to run it:
// cmd/execution does, cmd/mcp must not.
func TestBuildEventPublisher_KafkaWithPoolUsesOutbox(t *testing.T) {
	pool := lazyPool(t)

	t.Run("relay requested (cmd/execution)", func(t *testing.T) {
		pub, relay, closeFn := composition.BuildEventPublisher(
			composition.PublisherConfig{Kind: composition.KafkaPublisher, Brokers: []string{"127.0.0.1:1"}, RunRelay: true, RelayInterval: time.Second},
			pool, postgres.NewTaskRepo(pool), postgres.NewStationRepo(pool), quietLogger())
		defer closeFn()

		if _, ok := pub.(*postgres.OutboxPublisher); !ok {
			t.Errorf("publisher = %T, want *postgres.OutboxPublisher", pub)
		}
		if relay == nil {
			t.Error("RunRelay=true must build the outbox relay")
		}
	})

	t.Run("writer only (cmd/mcp)", func(t *testing.T) {
		pub, relay, closeFn := composition.BuildEventPublisher(
			composition.PublisherConfig{Kind: composition.KafkaPublisher, Brokers: []string{"127.0.0.1:1"}, RunRelay: false},
			pool, postgres.NewTaskRepo(pool), postgres.NewStationRepo(pool), quietLogger())
		defer closeFn()

		if _, ok := pub.(*postgres.OutboxPublisher); !ok {
			t.Errorf("publisher = %T, want *postgres.OutboxPublisher", pub)
		}
		if relay != nil {
			t.Error("RunRelay=false must NOT build a relay: only cmd/execution drains the outbox")
		}
	})
}
