// Command a2a is the composition root for the Fulfillment Execution A2A
// agent: it wires env config to outbound adapters, adapters to the use cases,
// and those to the inbound A2A adapter, then serves Agent2Agent JSON-RPC over
// HTTP with the Agent Card at the well-known path. It is a third,
// independent deployable alongside cmd/execution (the HTTP service) and
// cmd/mcp, per ADR-0059. It is the INBOUND half of A2A; the outbound
// work-planning client (ADR-0058) lives in the OLTP binary and is not wired
// here.
//
// Like cmd/mcp it runs on real Postgres and real Kafka only (the real-wiring
// ADR): DATABASE_URL, KAFKA_BROKERS and A2A_AGENT_URL are mandatory, and
// EVENT_PUBLISHER may only be kafka (or unset); a missing one is a boot error
// that names the variable, raised before telemetry, any connection and the
// listener. The complete_task skill publishes TaskCompleted through the
// transactional outbox; this process never starts the relay (only
// cmd/execution drains outbox_events onto Kafka).
//
// Environment:
//
//	DATABASE_URL             REQUIRED. Postgres DSN
//	KAFKA_BROKERS            REQUIRED. Comma-separated brokers
//	A2A_AGENT_URL            REQUIRED. Absolute http(s) URL of this agent's
//	                         JSON-RPC endpoint, advertised on the Agent Card
//	MIGRATIONS_DATABASE_URL  direct DSN for the boot migrations (default DATABASE_URL)
//	EVENT_PUBLISHER          kafka (default). log is REJECTED at boot
//	A2A_ADDR                 listen address (default :8093)
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

	inbounda2a "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/a2a"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/bootretry"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/system"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/composition"
	"github.com/claudioed/fulfillment-execution/internal/observability"
)

func main() {
	if err := run(); err != nil {
		slog.Error("a2a agent exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	// Misconfiguration is refused before anything is opened or bound.
	cfg, err := loadBootConfig(os.Getenv)
	if err != nil {
		return err
	}

	// Same non-blocking telemetry setup as the HTTP service: an unreachable
	// Collector degrades to dropped telemetry, never a server that won't start.
	rootCtx := context.Background()
	serviceName := getenv("OTEL_SERVICE_NAME", "fulfillment-execution-a2a")
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

	httpAddr := getenv("A2A_ADDR", ":8093")
	// See cmd/execution/main.go's identical fallback and openStorage's
	// doc comment for the full "why" (session-scoped pg_advisory_lock vs
	// PgBouncer transaction-pooling incompatibility, ADR-0031 and
	// order-management's ADR-0029, the reference implementation this
	// mirrors). This binary also runs migrations on start (buildStorage
	// below), so it needs the same direct-connection split
	// (cfg.migrationsDatabaseURL, falling back to DATABASE_URL).
	st, closeStorage, err := buildStorage(rootCtx, cfg.databaseURL, cfg.migrationsDatabaseURL, "migrations", logger)
	if err != nil {
		return err
	}
	defer closeStorage()

	// complete_task is a WRITE use case, so it must publish TaskCompleted
	// exactly like the REST and MCP paths do (ADR-0059, ADR-0020): through the same
	// composition.BuildEventPublisher wiring cmd/execution uses, with the same
	// UnitOfWork. The events go into outbox_events (integration + analytics
	// topics) in the use case's transaction; the outbox relay keeps running
	// only in cmd/execution.
	publisher, closePublisher, err := buildPublisher(st, cfg.kafkaBrokers, logger)
	if err != nil {
		return err
	}
	defer closePublisher()

	metrics, err := observability.NewMetrics()
	if err != nil {
		// A missing counter must not stop the server from doing work.
		logger.Error("task metrics unavailable", "error", err)
		metrics = nil
	}

	deps := buildDeps(st, publisher, metricsPort(metrics))

	// The A2A handler is mounted at "/" and "/a2a" and the Agent Card at the
	// well-known path, all unauthenticated by decision (ADR-0022); GET
	// /healthz is served for the Kubernetes probes and every request is
	// traced/metered (see router.go).
	card := inbounda2a.Card(inbounda2a.Config{AgentURL: cfg.agentURL, Version: observability.ServiceVersion()})
	handler := newRouter(inbounda2a.Handler(inbounda2a.NewExecutor(deps)), card, serviceName)

	srv := &http.Server{Addr: httpAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		logger.Info("a2a agent listening (JSON-RPC)", "addr", httpAddr, "agent_url", cfg.agentURL)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("a2a agent failed", "error", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}

// newLogger builds the process-wide structured logger, mirroring
// cmd/execution: LOG_LEVEL debug|info|warn|warning|error (case-insensitive,
// default info), JSON to stdout (gap fix D22: it was case-sensitive here).
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
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

// storage groups the Postgres persistence adapters this binary needs. pool
// and uow are always set by buildStorage.
type storage struct {
	tasks    ports.TaskRepo
	stations ports.StationRepo
	pool     *pgxpool.Pool
	uow      ports.UnitOfWork
}

// buildPublisher builds the event publisher through the wiring shared with
// cmd/execution (composition.BuildEventPublisher). RunRelay is false: only
// cmd/execution drains outbox_events onto Kafka; this process only inserts.
func buildPublisher(st storage, brokers []string, logger *slog.Logger) (ports.EventPublisher, func(), error) {
	publisher, _, closeFn, err := composition.BuildEventPublisher(composition.PublisherConfig{
		Brokers:  brokers,
		RunRelay: false,
	}, st.pool, st.tasks, st.stations, logger)
	return publisher, closeFn, err
}

// buildDeps wires the A2A adapter's use cases. CompleteTask gets the same
// Publisher, Clock, Metrics and UnitOfWork the HTTP service gives it.
func buildDeps(st storage, publisher ports.EventPublisher, metrics ports.Metrics) inbounda2a.Deps {
	return inbounda2a.Deps{
		GetQueueDepth: &usecases.GetQueueDepth{Tasks: st.tasks},
		CompleteTask: &usecases.CompleteTask{
			Tasks:      st.tasks,
			Publisher:  publisher,
			Clock:      system.Clock{},
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

// buildStorage opens the Postgres outbound adapters; an empty databaseURL is
// the boot error errNoDatabase (there is no in-memory path).
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
		return storage{}, noop, errNoDatabase
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
