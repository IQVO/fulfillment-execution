package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// migrationsDirForTest resolves the repo's migrations directory relative
// to cmd/execution.
func migrationsDirForTest(t *testing.T) string {
	t.Helper()
	dir := "../../migrations"
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("migrations directory not found at %s: %v", dir, err)
	}
	return dir
}

// TestMigrationsDatabaseURLFallback proves the fallback wiring
// cmd/execution/main.go's run() applies before calling openStorage:
// getenv("MIGRATIONS_DATABASE_URL", databaseURL) must return
// MIGRATIONS_DATABASE_URL's own value when it is set, and databaseURL
// itself (DATABASE_URL) when it is unset. This is the exact env-lookup
// line the fix for the PgBouncer/pg_advisory_lock incompatibility
// (ADR-0031, mirroring order-management's ADR-0029) depends on: any
// environment that doesn't provision the split (local dev, CI
// integration tests, a cluster whose Terraform predates this fix) must
// keep working exactly as before, using DATABASE_URL for everything
// including migrations.
func TestMigrationsDatabaseURLFallback(t *testing.T) {
	const databaseURL = "postgres://u:***@pgbouncer.example:6432/fulfillment_execution?sslmode=disable"

	t.Run("falls back to DATABASE_URL when MIGRATIONS_DATABASE_URL is unset", func(t *testing.T) {
		t.Setenv("MIGRATIONS_DATABASE_URL", "")
		os.Unsetenv("MIGRATIONS_DATABASE_URL")

		got := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
		if got != databaseURL {
			t.Fatalf("getenv fallback = %q, want the DATABASE_URL value %q", got, databaseURL)
		}
	})

	t.Run("uses MIGRATIONS_DATABASE_URL when set, not DATABASE_URL", func(t *testing.T) {
		const direct = "postgres://u:***@postgres-postgresql.example:5432/fulfillment_execution?sslmode=disable"
		t.Setenv("MIGRATIONS_DATABASE_URL", direct)

		got := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
		if got != direct {
			t.Fatalf("getenv = %q, want the direct MIGRATIONS_DATABASE_URL value %q (must NOT silently keep using DATABASE_URL/PgBouncer)", got, direct)
		}
		if got == databaseURL {
			t.Fatal("MIGRATIONS_DATABASE_URL and DATABASE_URL collapsed to the same value — the whole point of this env var is that it differs")
		}
	})
}

// TestOpenStorage_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations
// proves openStorage itself — not just the env-var read above — actually
// threads migrationsDatabaseURL into the migration step and databaseURL
// into the pgxpool, rather than the two ever being conflated. Gives
// DATABASE_URL an address nothing listens on (so opening the pgxpool,
// which happens AFTER migrations succeed, would hang/fail loudly if ever
// reached) and MIGRATIONS_DATABASE_URL a schemeless string that
// migrate.New rejects immediately with a distinctive parse error
// ("failed to parse scheme from database URL") — if openStorage ignored
// migrationsDatabaseURL and ran migrations against databaseURL instead,
// this test would see a dial/"connection refused" error, not the parse
// error.
func TestOpenStorage_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations(t *testing.T) {
	const (
		bogusMigrationsURL = "not-a-valid-connection-string"
		unreachableAppURL  = "postgres://u:***@127.0.0.1:1/fulfillment_execution?sslmode=disable&connect_timeout=1"
	)

	logger := newLogger("error")
	_, err := openStorage(context.Background(), logger, unreachableAppURL, bogusMigrationsURL, migrationsDirForTest(t))
	if err == nil {
		t.Fatal("a malformed MIGRATIONS_DATABASE_URL must fail boot")
	}
	if !strings.Contains(err.Error(), "parse scheme") {
		t.Fatalf("err = %v — expected the bogus-URL parse error from migrate.New; a \"connection refused\"/dial error here would mean migrations ran against databaseURL/unreachableAppURL instead of migrationsDatabaseURL", err)
	}
}

// TestOpenStorage_RetriesTheDatabaseNotJustOnce drives openStorage — the
// ACTUAL composition-root function cmd/execution/main.go calls at
// startup — against an unreachable address and asserts on the ELAPSED
// time: a single attempt returns fast (a refused connection is
// near-instant), whereas the retry budget cannot be paid in less than
// the sum of its backoffs.
func TestOpenStorage_RetriesTheDatabaseNotJustOnce(t *testing.T) {
	databaseURL := "postgres://u:***@127.0.0.1:1/fulfillment_execution?sslmode=disable&connect_timeout=1"

	logger := newLogger("error")
	start := time.Now()
	_, err := openStorage(context.Background(), logger, databaseURL, databaseURL, migrationsDirForTest(t))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("an unreachable database must fail boot, never fall back to the in-memory repo")
	}
	if elapsed < 1*time.Second {
		t.Fatalf("elapsed = %v, want at least the first backoff (~1s) — a single attempt would return near-instantly", elapsed)
	}
}

// TestOpenStorage_NoDatabaseURLUsesMemoryImmediately proves an unset
// DATABASE_URL takes the in-memory path with no dial, no backoff, no
// delay — MIGRATIONS_DATABASE_URL is irrelevant in that case.
func TestOpenStorage_NoDatabaseURLUsesMemoryImmediately(t *testing.T) {
	logger := newLogger("error")
	start := time.Now()
	storage, err := openStorage(context.Background(), logger, "", "", migrationsDirForTest(t))
	if err != nil {
		t.Fatalf("openStorage: %v", err)
	}
	if storage.pool != nil {
		t.Fatal("pool must be nil in the in-memory path")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("in-memory path took %v; it must not dial or sleep", elapsed)
	}
}
