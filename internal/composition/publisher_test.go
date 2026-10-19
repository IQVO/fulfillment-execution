package composition_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/composition"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// lazyPool returns a pgxpool that never dials (pgxpool connects lazily), so
// the wiring can be asserted without a database.
func lazyPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://u:***@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// There is no log sink and no direct-to-Kafka path: without a pool or
// without brokers the wiring is an error, never a degraded publisher.
func TestBuildEventPublisher_RefusesAnythingButTheRealRuntime(t *testing.T) {
	pool := lazyPool(t)
	cases := []struct {
		name    string
		cfg     composition.PublisherConfig
		pool    bool
		wantErr error
	}{
		{"no pool", composition.PublisherConfig{Brokers: []string{"127.0.0.1:1"}, RunRelay: true}, false, composition.ErrNoPool},
		{"no brokers", composition.PublisherConfig{RunRelay: true}, true, composition.ErrNoBrokers},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := pool
			if !tc.pool {
				p = nil
			}
			pub, relay, closeFn, err := composition.BuildEventPublisher(tc.cfg, p, memory.NewTaskRepo(), memory.NewStationRepo(), quietLogger())
			defer closeFn()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if pub != nil || relay != nil {
				t.Fatalf("publisher = %v, relay = %v, want none", pub, relay)
			}
		})
	}
}

// The publisher is ALWAYS the transactional OutboxPublisher (ADR-0020). The
// relay exists only for the process that asked to run it: cmd/execution
// does, cmd/mcp must not.
func TestBuildEventPublisher_UsesTheOutbox(t *testing.T) {
	pool := lazyPool(t)

	t.Run("relay requested (cmd/execution)", func(t *testing.T) {
		pub, relay, closeFn, err := composition.BuildEventPublisher(
			composition.PublisherConfig{Brokers: []string{"127.0.0.1:1"}, RunRelay: true, RelayInterval: time.Second, RelayMaxAttempts: 7},
			pool, postgres.NewTaskRepo(pool), postgres.NewStationRepo(pool), quietLogger())
		if err != nil {
			t.Fatal(err)
		}
		defer closeFn()

		if _, ok := pub.(*postgres.OutboxPublisher); !ok {
			t.Errorf("publisher = %T, want *postgres.OutboxPublisher", pub)
		}
		if relay == nil {
			t.Fatal("RunRelay=true must build the outbox relay")
		}
		// Gap fix D12 (ADR 0042): the configured cap reaches the relay.
		if got := relay.MaxAttempts(); got != 7 {
			t.Errorf("relay MaxAttempts = %d, want 7 (PublisherConfig.RelayMaxAttempts)", got)
		}
	})

	t.Run("writer only (cmd/mcp)", func(t *testing.T) {
		pub, relay, closeFn, err := composition.BuildEventPublisher(
			composition.PublisherConfig{Brokers: []string{"127.0.0.1:1"}, RunRelay: false},
			pool, postgres.NewTaskRepo(pool), postgres.NewStationRepo(pool), quietLogger())
		if err != nil {
			t.Fatal(err)
		}
		defer closeFn()

		if _, ok := pub.(*postgres.OutboxPublisher); !ok {
			t.Errorf("publisher = %T, want *postgres.OutboxPublisher", pub)
		}
		if relay != nil {
			t.Error("RunRelay=false must NOT build a relay: only cmd/execution drains the outbox")
		}
	})
}
