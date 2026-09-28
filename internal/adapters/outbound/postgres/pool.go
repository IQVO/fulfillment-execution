package postgres

import (
	"context"
	"fmt"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MaxConns is the OLTP pool's per-process connection ceiling, shared by
// both cmd/execution (the api Deployment, HPA-scalable up to
// charts/fulfillment-execution values.yaml's autoscaling.api.maxReplicas,
// 4) and cmd/mcp (the mcp Deployment, fixed at 1 replica -- it gets no HPA
// at all, see values.yaml's mcp comment for why).
//
// Sized against this shared Postgres instance's REAL max_connections (100,
// the unmodified Bitnami chart default -- warehouse-infra's
// terraform/postgres.tf does not override it): at the api Deployment's HPA
// ceiling of 4 replicas, 4 * 10 = 40 connections, ~40% of the instance-wide
// ceiling for this ONE of up to 10 fleet services' OLTP path alone,
// deliberately leaving headroom for the other services (and this service's
// own mcp/projector/reports processes) sharing the SAME Postgres instance.
// Matches order-management ADR-0026's OLTP number exactly -- same chart
// shape (api HPA max 4), same shared-instance ceiling. See ADR-0030 for the
// full connection-budget accounting.
//
// PgBouncer (warehouse-infra PR #43) now sits in front of the shared
// Postgres instance in transaction-pooling mode for every service's OLTP
// DATABASE_URL (pool_size=12 per service database) -- this MaxConns is the
// CLIENT-side pgxpool ceiling per process, which PgBouncer absorbs and
// multiplexes down to its own much smaller server-side pool. Both numbers
// matter: this one bounds how many concurrent requests one process can have
// in flight against Postgres at once; PgBouncer's pool_size bounds how many
// REAL server connections the shared instance sees across every replica of
// every service.
const MaxConns = 10

// StatementTimeout bounds how long a single query may hold a connection on
// the OLTP database before Postgres cancels it. This service's OLTP queries
// (claimNext, task/station/package reads and writes) are all
// single-aggregate operations keyed by id, normally low-single-digit
// milliseconds. 5s is generous headroom for lock contention or a slow disk
// without letting one runaway or blocked query hold a pool slot -- and
// therefore a bulkhead slot the HPA's replica math is sizing capacity
// around -- indefinitely. Matches order-management ADR-0026's OLTP value.
const StatementTimeout = "5s"

// NewPool builds the pgxpool every repository in this package is
// constructed from, with the OTel pgx tracer installed so each query,
// batch, copy and connection acquisition becomes a child span of whatever
// span is active on the calling context. MaxConns and StatementTimeout
// (see their doc comments) are applied to every connection.
//
// otelpgx puts the SQL statement on the span in its parameterised form
// (values stay as placeholders); query parameters are NOT recorded, which is
// the default and is left that way deliberately -- task ids and station ids
// are not worth leaking into a trace backend.
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return NewPoolWithLimits(ctx, databaseURL, MaxConns, StatementTimeout)
}

// NewPoolWithLimits is NewPool's shared implementation, taking maxConns and
// statementTimeout explicitly so an integration test can drive a much
// shorter timeout directly -- proving the AfterConnect hook really applies
// the setting to every new connection, by triggering an actual cancellation
// -- without waiting out the real production value. Production callers
// should use NewPool.
func NewPoolWithLimits(ctx context.Context, databaseURL string, maxConns int32, statementTimeout string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	// Trimmed SQL in span names (e.g. "query SELECT ...") is now otelpgx's
	// default behavior as of v0.12.0; no option needed.
	cfg.ConnConfig.Tracer = otelpgx.NewTracer()
	cfg.MaxConns = maxConns
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = '%s'", statementTimeout))
		return err
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

// RecordPoolStats registers observable gauges for pgxpool's connection
// counts (idle, in use, max) on the global MeterProvider. It is separate
// from NewPool so a caller that does not want pool metrics simply does not
// call it.
func RecordPoolStats(pool *pgxpool.Pool) error {
	return otelpgx.RecordStats(pool)
}
