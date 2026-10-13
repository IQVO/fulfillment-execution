//go:build integration

package postgres_test

import (
	"context"
	"testing"
)

// TestMigrations_DropLegacyDomainEvents proves migration 0018 (ADR 0045):
// after every migration has run, the legacy domain_events table created by
// 0001_init no longer exists, while the tables the code uses are intact.
func TestMigrations_DropLegacyDomainEvents(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	var legacy *string
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.domain_events')::text`).Scan(&legacy); err != nil {
		t.Fatalf("to_regclass(domain_events): %v", err)
	}
	if legacy != nil {
		t.Fatalf("domain_events still exists after migrations (%s); migration 0018 must drop it", *legacy)
	}

	for _, table := range []string{"tasks", "outbox_events", "processed_events", "idempotency_keys"} {
		var got *string
		if err := pool.QueryRow(ctx, `SELECT to_regclass('public.' || $1)::text`, table).Scan(&got); err != nil {
			t.Fatalf("to_regclass(%s): %v", table, err)
		}
		if got == nil {
			t.Fatalf("table %s is missing after migrations", table)
		}
	}
}
