// Command mcp is the composition root for the Fulfillment Execution MCP
// server: it wires env config to outbound adapters, adapters to the read use
// cases, and those to the inbound MCP adapter, then serves MCP over Streamable
// HTTP. It is a second, independent deployable alongside cmd/execution (the
// HTTP service), per ADR-0008.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundmcp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/mcp"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/bootretry"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/composition"
	"github.com/claudioed/fulfillment-execution/internal/observability"
)

func main() {
	if err := run(); err != nil {
		slog.Error("mcp server exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	// Same non-blocking telemetry setup as the HTTP service: an unreachable
	// Collector degrades to dropped telemetry, never a server that won't start.
	rootCtx := context.Background()
	serviceName := getenv("OTEL_SERVICE_NAME", "fulfillment-execution-mcp")
	otelShutdown, err := observability.Setup(rootCtx, serviceName, observability.ServiceVersion(), observability.Endpoint())
	if err != nil {
		logger.Error("opentelemetry setup degraded", "error", err)
	}
	if otelShutdown != nil {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := otelShutdown(ctx); err != nil {
				logger.Error("opentelemetry shutdown failed", "error", err)
			}
		}()
	} else {
		logger.Warn("opentelemetry disabled; traces and metrics will not be exported")
	}

	httpAddr := getenv("MCP_ADDR", ":8090")
	databaseURL := os.Getenv("DATABASE_URL")
	// See cmd/execution/main.go's identical fallback and openStorage's
	// doc comment for the full "why" (session-scoped pg_advisory_lock vs
	// PgBouncer transaction-pooling incompatibility, ADR-0031 and
	// order-management's ADR-0029, the reference implementation this
	// mirrors). This binary also runs migrations on start (buildStorage
	// below), so it needs the same direct-connection split.
	migrationsDatabaseURL := getenv("MIGRATIONS_DATABASE_URL", databaseURL)

	st, closeStorage, err := buildStorage(rootCtx, databaseURL, migrationsDatabaseURL, "migrations", logger)
	if err != nil {
		return err
	}
	defer closeStorage()

	// complete_task is a WRITE use case, so it must publish TaskCompleted
	// exactly like the REST path does (ADR-0008, ADR-0020): through the same
	// composition.BuildEventPublisher wiring cmd/execution uses, with the same
	// UnitOfWork. With EVENT_PUBLISHER=kafka and DATABASE_URL the events go
	// into outbox_events (integration + analytics topics) in the use case's
	// transaction; the outbox relay keeps running only in cmd/execution.
	publisher, closePublisher := buildPublisher(st, strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ","), logger)
	defer closePublisher()

	metrics, err := observability.NewMetrics()
	if err != nil {
		// A missing counter must not stop the server from doing work.
		logger.Error("task metrics unavailable", "error", err)
		metrics = nil
	}

	deps := buildDeps(st, publisher, metricsPort(metrics))

	// The curated throughput data-product tool talks to the reports REST
	// service (never the analytical DB directly). It is exposed only when
	// REPORTS_BASE_URL is set, so an MCP deployment without the reports
	// service simply omits the tool.
	if base := os.Getenv("REPORTS_BASE_URL"); base != "" {
		logger.Info("fulfillment reports tool enabled", "reports_base_url", base)
		deps.Reports = inboundmcp.NewReportsRESTClient(base, nil)
	}
	server := inboundmcp.NewServer(deps)

	// The MCP handler is mounted at "/" and "/mcp", unauthenticated by
	// decision (ADR-0022); GET /healthz is served for the Kubernetes probes
	// and every request is traced/metered (see router.go).
	handler := newRouter(inboundmcp.Handler(server), serviceName)

	srv := &http.Server{Addr: httpAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		logger.Info("mcp server listening (Streamable HTTP)", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("mcp server failed", "error", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(observability.NewSlogHandler(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}),
	))
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// storage groups the environment-selected persistence adapters this binary
// needs. pool and uow are nil when running in-memory: the use cases then run
// Save and Publish back to back (see ports.UnitOfWork).
type storage struct {
	tasks    ports.TaskRepo
	stations ports.StationRepo
	pool     *pgxpool.Pool
	uow      ports.UnitOfWork
}

// buildPublisher builds the event publisher through the wiring shared with
// cmd/execution (composition.BuildEventPublisher). RunRelay is false: only
// cmd/execution drains outbox_events onto Kafka; this process only inserts.
func buildPublisher(st storage, brokers []string, logger *slog.Logger) (ports.EventPublisher, func()) {
	publisher, _, closeFn := composition.BuildEventPublisher(composition.PublisherConfig{
		Kind:     getenv("EVENT_PUBLISHER", "log"),
		Brokers:  brokers,
		RunRelay: false,
	}, st.pool, st.tasks, st.stations, logger)
	return publisher, closeFn
}

// buildDeps wires the MCP adapter's use cases. CompleteTask gets the same
// Publisher, Clock, Metrics and UnitOfWork the HTTP service gives it.
func buildDeps(st storage, publisher ports.EventPublisher, metrics ports.Metrics) inboundmcp.Deps {
	return inboundmcp.Deps{
		GetQueueDepth: &usecases.GetQueueDepth{Tasks: st.tasks},
		CompleteTask: &usecases.CompleteTask{
			Tasks:      st.tasks,
			Publisher:  publisher,
			Clock:      memory.SystemClock{},
			Metrics:    metrics,
			UnitOfWork: st.uow,
		},
		Tasks: st.tasks,
		Now:   time.Now,
	}
}

// metricsPort converts a possibly-nil *observability.Metrics into a
// ports.Metrics, avoiding the typed-nil trap (see cmd/execution).
func metricsPort(m *observability.Metrics) ports.Metrics {
	if m == nil {
		return nil
	}
	return m
}

// buildStorage selects the in-memory or Postgres outbound adapters.
// The Postgres path runs the schema migrations, opens the pool, and
// verifies it with a ping, each under boot retry: this fleet's Istio
// native sidecars reset EVERY pod's first outbound TCP dial ~10s after
// the app starts (holdApplicationUntilProxyStarts is a no-op for native
// sidecars). A single attempt turns that transient condition into
// CrashLoopBackOff; the retry still fails closed once its budget is
// exhausted.
//
// migrationsDatabaseURL is used ONLY for the golang-migrate step below —
// the pgxpool opened just after it (and used for every subsequent
// request) always uses databaseURL. They are deliberately different
// connection strings in a PgBouncer-fronted environment: golang-migrate's
// postgres driver takes a session-scoped `SELECT pg_advisory_lock($1)` to
// serialize concurrent migration runs across replicas starting at the
// same time, and PgBouncer's transaction-pooling mode (this fleet's
// pool_mode for every OLTP DATABASE_URL, warehouse-infra PR #43) does not
// support session-scoped state — each statement in one logical client
// session can land on a different physical backend connection, so the
// advisory lock never behaves as a real mutex. See ADR-0031 (and
// order-management's ADR-0029, the reference implementation this
// mirrors) for the full incident and fix. Callers pass
// MIGRATIONS_DATABASE_URL when set (warehouse-infra provisions it as a
// direct, non-pooled DSN alongside DATABASE_URL for all 9 OLTP services,
// PR #44) or fall back to databaseURL itself for any environment that
// doesn't provision the split (local dev, CI integration tests) —
// byte-identical to this function's behavior before this parameter
// existed in that case.
func buildStorage(ctx context.Context, databaseURL, migrationsDatabaseURL, migrationsPath string, logger *slog.Logger) (storage, func(), error) {
	noop := func() {}

	if databaseURL == "" {
		logger.Info("database url not configured; using in-memory adapters")
		return storage{tasks: memory.NewTaskRepo(), stations: memory.NewStationRepo()}, noop, nil
	}

	if err := bootretry.Retry(ctx, logger, "run migrations", func() error {
		return postgres.Migrate(migrationsDatabaseURL, migrationsPath)
	}); err != nil {
		return storage{}, noop, err
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return storage{}, noop, err
	}
	// ParseConfig/NewWithConfig do not themselves establish a
	// connection, so without this the first-dial reset would surface
	// inside the first served request instead of at boot.
	if err := bootretry.Retry(ctx, logger, "ping database", func() error {
		return pool.Ping(ctx)
	}); err != nil {
		pool.Close()
		return storage{}, noop, err
	}
	return storage{
		tasks:    postgres.NewTaskRepo(pool),
		stations: postgres.NewStationRepo(pool),
		pool:     pool,
		uow:      postgres.NewUnitOfWork(pool),
	}, pool.Close, nil
}
