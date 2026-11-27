//go:build integration

// The repo-specific half of the fleet replay-class test
// (idempotency_replay_classes_template_integration_test.go): newReplayOps
// wires the six operations over ONE clean store — the package's shared
// testcontainers Postgres (main_integration_test.go), truncated per call.
// Only this file knows the store; it still names no driver package: the
// pool comes from postgres.NewPool by type inference and the write goes
// through the transaction the middleware bound into the request context
// (internal/pgtx), exactly like a production use case.
package http_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	inboundhttp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/pgtx"
)

// newReplayOps returns the six replay-class operations over one clean
// Postgres store: a fresh pool over the shared container with every table
// truncated and the replay_probe scratch table (re)created.
func newReplayOps(t *testing.T) replayOps {
	t.Helper()
	pool, err := postgres.NewPool(context.Background(), sharedPostgresURL(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	truncateAll(t, pool)
	if _, err := pool.Exec(context.Background(),
		`CREATE TABLE IF NOT EXISTS replay_probe (tag TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("create replay_probe: %v", err)
	}

	count := func(t *testing.T, query string, args ...any) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	return replayOps{
		Catalogue: func() []problemEntry {
			var out []problemEntry
			for _, p := range inboundhttp.ProblemTypesForTest() {
				out = append(out, problemEntry{Slug: p.Slug, Status: p.Status, Class: p.Class})
			}
			return out
		},
		Middleware: func(_ *testing.T, next http.Handler) http.Handler {
			return inboundhttp.RequireIdempotencyKey(pool)(next)
		},
		Refuse: func(w http.ResponseWriter, r *http.Request, e problemEntry) {
			inboundhttp.RefuseForTest(w, r, e.Slug, e.Status)
		},
		Write: func(ctx context.Context, tag string) error {
			tx, ok := pgtx.TxFrom(ctx)
			if !ok {
				return errors.New("no transaction bound into the request context")
			}
			_, err := tx.Exec(ctx, `INSERT INTO replay_probe (tag) VALUES ($1) ON CONFLICT (tag) DO NOTHING`, tag)
			return err
		},
		Committed: func(t *testing.T, tag string) int {
			return count(t, `SELECT count(*) FROM replay_probe WHERE tag = $1`, tag)
		},
		KeyRows: func(t *testing.T, key string) int {
			return count(t, `SELECT count(*) FROM idempotency_keys WHERE key = $1`, key)
		},
	}
}
