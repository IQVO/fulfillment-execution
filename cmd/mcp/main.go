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
	"syscall"
	"time"

	inboundmcp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/mcp"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/bootretry"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
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
	// mirrors). This binary also runs migrations on start (buildTaskRepo
	// below), so it needs the same direct-connection split.
	migrationsDatabaseURL := getenv("MIGRATIONS_DATABASE_URL", databaseURL)

	taskRepo, closeTaskRepo, err := buildTaskRepo(rootCtx, databaseURL, migrationsDatabaseURL, "migrations", logger)
	if err != nil {
		return err
	}
	defer closeTaskRepo()

	// The MCP adapter reuses the SAME use cases the HTTP adapter uses:
	// GetQueueDepth (read) and CompleteTask (write), plus the read-only query
	// port satisfied by the same TaskRepo. CompleteTask needs a publisher and
	// clock; the MCP server is not the platform's primary event publisher
	// (cmd/execution is), so it logs the TaskCompleted event rather than
	// publishing to Kafka.
	publisher := events.NewLogPublisher(logger)
	clock := memory.SystemClock{}
	deps := inboundmcp.Deps{
		GetQueueDepth: &usecases.GetQueueDepth{Tasks: taskRepo},
		CompleteTask:  &usecases.CompleteTask{Tasks: taskRepo, Publisher: publisher, Clock: clock},
		Tasks:         taskRepo,
		Now:           time.Now,
	}
	// The curated throughput data-product tool talks to the reports REST
	// service (never the analytical DB directly). It is exposed only when
	// REPORTS_BASE_URL is set, so an MCP deployment without the reports
	// service simply omits the tool.
	if base := os.Getenv("REPORTS_BASE_URL"); base != "" {
		logger.Info("fulfillment reports tool enabled", "reports_base_url", base)
		deps.Reports = inboundmcp.NewReportsRESTClient(base, nil)
	}
	server := inboundmcp.NewServer(deps)

	// The MCP handler is mounted at "/" and "/mcp", unauthenticated; GET
	// /healthz is served for the Kubernetes probes (see router.go).
	handler := newRouter(inboundmcp.Handler(server))

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

// buildTaskRepo selects the in-memory or Postgres outbound task repository.
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
func buildTaskRepo(ctx context.Context, databaseURL, migrationsDatabaseURL, migrationsPath string, logger *slog.Logger) (ports.TaskRepo, func(), error) {
	noop := func() {}

	if databaseURL == "" {
		logger.Info("database url not configured; using in-memory adapters")
		return memory.NewTaskRepo(), noop, nil
	}

	if err := bootretry.Retry(ctx, logger, "run migrations", func() error {
		return postgres.Migrate(migrationsDatabaseURL, migrationsPath)
	}); err != nil {
		return nil, noop, err
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return nil, noop, err
	}
	// ParseConfig/NewWithConfig do not themselves establish a
	// connection, so without this the first-dial reset would surface
	// inside the first served request instead of at boot.
	if err := bootretry.Retry(ctx, logger, "ping database", func() error {
		return pool.Ping(ctx)
	}); err != nil {
		pool.Close()
		return nil, noop, err
	}
	return postgres.NewTaskRepo(pool), pool.Close, nil
}
