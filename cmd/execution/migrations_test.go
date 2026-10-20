package main

import (
	"context"
	"errors"
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
// loadBootConfig applies before openStorage runs: MIGRATIONS_DATABASE_URL
// when it is set, DATABASE_URL itself when it is unset. This is the exact
// lookup the fix for the PgBouncer/pg_advisory_lock incompatibility
// (ADR-0031, mirroring order-management's ADR-0029) depends on: any
// environment that doesn't provision the split (local dev, CI integration
// tests, a cluster whose Terraform predates this fix) must keep working
// exactly as before, using DATABASE_URL for everything including
// migrations.
func TestMigrationsDatabaseURLFallback(t *testing.T) {
	const databaseURL = "postgres://u:***@pgbouncer.example:6432/fulfillment_execution?sslmode=disable"

	t.Run("falls back to DATABASE_URL when MIGRATIONS_DATABASE_URL is unset", func(t *testing.T) {
		cfg, err := loadBootConfig(validEnv(map[string]string{"DATABASE_URL": databaseURL}))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.migrationsDatabaseURL != databaseURL {
			t.Fatalf("migrationsDatabaseURL = %q, want the DATABASE_URL value %q", cfg.migrationsDatabaseURL, databaseURL)
		}
	})

	t.Run("uses MIGRATIONS_DATABASE_URL when set, not DATABASE_URL", func(t *testing.T) {
		const direct = "postgres://u:***@postgres-postgresql.example:5432/fulfillment_execution?sslmode=disable"
		cfg, err := loadBootConfig(validEnv(map[string]string{"DATABASE_URL": databaseURL, "MIGRATIONS_DATABASE_URL": direct}))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.migrationsDatabaseURL != direct {
			t.Fatalf("migrationsDatabaseURL = %q, want the direct MIGRATIONS_DATABASE_URL value %q (must NOT silently keep using DATABASE_URL/PgBouncer)", cfg.migrationsDatabaseURL, direct)
		}
		if cfg.databaseURL != databaseURL {
			t.Fatalf("databaseURL = %q, want %q: the runtime pool keeps using DATABASE_URL", cfg.databaseURL, databaseURL)
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
		t.Fatal("an unreachable database must fail boot")
	}
	if elapsed < 1*time.Second {
		t.Fatalf("elapsed = %v, want at least the first backoff (~1s) — a single attempt would return near-instantly", elapsed)
	}
}

// TestOpenStorage_NoDatabaseURLIsABootError proves an empty DATABASE_URL is
// refused at once (no dial, no backoff) with the error that names the
// variable: there is no in-memory path behind it.
func TestOpenStorage_NoDatabaseURLIsABootError(t *testing.T) {
	logger := newLogger("error")
	start := time.Now()
	storage, err := openStorage(context.Background(), logger, "", "", migrationsDirForTest(t))
	if !errors.Is(err, errNoDatabase) {
		t.Fatalf("err = %v, want errNoDatabase", err)
	}
	if storage.pool != nil {
		t.Fatal("no pool may be returned alongside the error")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("the refusal took %v; it must not dial or sleep", elapsed)
	}
}
