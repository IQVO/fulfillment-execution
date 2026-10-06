//go:build integration

package http_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
)

// One throwaway Postgres (testcontainers — never an external DATABASE_URL,
// never t.Skip) serves every idempotency integration test in this package.
// It is started lazily on first use (so the package's many non-DB tests pay
// nothing), migrated once, and terminated by TestMain after the run.
// Isolation: idempotencyDB truncates every application table before handing
// the test a pool, so no test depends on execution order.
var sharedPG struct {
	once      sync.Once
	container *tcpostgres.PostgresContainer
	url       string
	err       error
}

// TestMain owns the package-wide container lifecycle.
func TestMain(m *testing.M) {
	code := m.Run()
	if sharedPG.container != nil {
		if err := testcontainers.TerminateContainer(sharedPG.container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}
	os.Exit(code)
}

// sharedPostgresURL returns the DSN of the package's migrated container,
// starting it on first call.
func sharedPostgresURL(t *testing.T) string {
	t.Helper()
	sharedPG.once.Do(func() {
		ctx := context.Background()
		container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
			tcpostgres.WithDatabase("fulfillment_execution"),
			tcpostgres.WithUsername("fulfillment"),
			tcpostgres.WithPassword("fulfillment"),
			tcpostgres.BasicWaitStrategies(),
		)
		if err != nil {
			sharedPG.err = fmt.Errorf("start postgres container: %w", err)
			return
		}
		sharedPG.container = container
		url, err := container.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			sharedPG.err = fmt.Errorf("connection string: %w", err)
			return
		}
		if err := postgres.Migrate(url, "../../../../migrations"); err != nil {
			sharedPG.err = fmt.Errorf("run migrations: %w", err)
			return
		}
		sharedPG.url = url
	})
	if sharedPG.err != nil {
		t.Fatal(sharedPG.err)
	}
	return sharedPG.url
}

// truncateAll empties every application table (everything in the public
// schema except golang-migrate's own bookkeeping) and restarts identity
// columns, so each test starts from the freshly migrated, empty state a
// private container used to give it.
func truncateAll(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	rows, err := pool.Query(ctx, `SELECT quote_ident(tablename) FROM pg_tables WHERE schemaname = 'public' AND tablename <> 'schema_migrations'`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			t.Fatalf("scan table name: %v", err)
		}
		tables = append(tables, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("list tables: %v", err)
	}
	if len(tables) == 0 {
		t.Fatal("no application tables found: migrations did not run")
	}
	if _, err := pool.Exec(ctx, "TRUNCATE TABLE "+strings.Join(tables, ", ")+" RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}
