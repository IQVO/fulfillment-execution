// Command fulfillment-projector is the WRITER composition root of the
// fulfillment throughput data product. It consumes the analytics Kafka topic,
// projects each event into the analytical Postgres database via the idempotent
// PostgresProjection, and serves only a health endpoint on an admin port. It
// is the single writer of the analytical database and serves no reports; the
// reader (cmd/fulfillment-reports) is a separate deployable (ADR-0012).
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/bootretry"
	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/observability"
)

// errMissingAnalyticsURL is returned when ANALYTICS_DATABASE_URL is unset:
// the projector is the writer of the analytical database and cannot start
// without it.
var errMissingAnalyticsURL = errors.New("ANALYTICS_DATABASE_URL is required")

func main() {
	if err := run(); err != nil {
		slog.Error("fulfillment-projector exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	rootCtx := context.Background()
	// Telemetry comes up right after the logger and before any adapter, so
	// everything built below is instrumented.
	defer setupTelemetry(rootCtx, logger)()

	adminAddr := getenv("ADMIN_ADDR", ":8091")
	analyticsURL := os.Getenv("ANALYTICS_DATABASE_URL")
	if analyticsURL == "" {
		return errMissingAnalyticsURL
	}
	kafkaBrokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	migrationsPath := getenv("ANALYTICS_MIGRATIONS_PATH", "migrations/analytics")

	pool, err := openAnalyticsPool(rootCtx, logger, analyticsURL, migrationsPath)
	if err != nil {
		return err
	}
	defer pool.Close()

	projection := analyticsstore.NewPostgresProjection(pool)
	consumed := analyticsstore.NewConsumedEventsRepo(pool)
	consumer := inboundkafka.NewAnalyticsConsumer(kafkaBrokers, outboundkafka.AnalyticsTopic, projection, consumed, logger)
	defer func() { _ = consumer.Close() }()

	srv := startAdminServer(logger, adminAddr)

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	go consumeAnalytics(consumer, consumerCtx, logger, kafkaBrokers)

	return waitForShutdown(srv, cancelConsumer)
}

// setupTelemetry configures OTel for this process. An unreachable
// Collector is not fatal: the OTLP exporters dial lazily, so the service
// starts and serves normally with telemetry dropped on the floor. The
// returned function flushes and shuts the exporters down and must be
// deferred by the caller.
func setupTelemetry(rootCtx context.Context, logger *slog.Logger) func() {
	serviceName := getenv("OTEL_SERVICE_NAME", "fulfillment-projector")
	otelShutdown, err := observability.Setup(rootCtx, serviceName, observability.ServiceVersion(), observability.Endpoint())
	if err != nil {
		logger.Error("opentelemetry setup degraded", "error", err)
	}
	if otelShutdown == nil {
		logger.Warn("opentelemetry disabled; traces and metrics will not be exported")
		return func() {}
	}
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := otelShutdown(ctx); err != nil {
			logger.Error("opentelemetry shutdown failed", "error", err)
		}
	}
}

// openAnalyticsPool owns the projector's analytical-database boot: it runs
// the analytics migrations (the projector is the single writer of the
// analytical schema and owns them, ADR-0012), opens the pool, and pings it
// so this fleet's known first-outbound-dial reset surfaces at boot instead
// of on the first consumed message. The pool is closed again on any
// failure so the caller only has to defer Close on success.
func openAnalyticsPool(rootCtx context.Context, logger *slog.Logger, analyticsURL, migrationsPath string) (*pgxpool.Pool, error) {
	// Retried: this fleet's Istio native sidecars reset EVERY pod's first
	// outbound TCP dial ~10s after the app starts
	// (holdApplicationUntilProxyStarts is a no-op for native sidecars). A
	// single attempt turns that transient condition into CrashLoopBackOff;
	// the retry still fails closed once its budget is exhausted.
	if err := bootretry.Retry(rootCtx, logger, "run analytics migrations", func() error {
		return postgres.Migrate(analyticsURL, migrationsPath)
	}); err != nil {
		return nil, err
	}

	pool, err := analyticsstore.NewPool(rootCtx, analyticsURL)
	if err != nil {
		return nil, err
	}
	// ParseConfig/NewWithConfig do not themselves establish a connection,
	// so without this the first-dial reset would surface inside the first
	// consumed message instead of at boot.
	if err := bootretry.Retry(rootCtx, logger, "ping analytics database", func() error {
		return pool.Ping(rootCtx)
	}); err != nil {
		pool.Close()
		return nil, err
	}
	if err := postgres.RecordPoolStats(pool); err != nil {
		logger.Error("analytics pgxpool metrics unavailable", "error", err)
	}
	return pool, nil
}

// startAdminServer serves the projector's only endpoint — health on
// /healthz — on the admin port in its own goroutine. The projector serves
// no reports; the reader is a separate deployable (ADR-0012).
func startAdminServer(logger *slog.Logger, adminAddr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	srv := &http.Server{Addr: adminAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		logger.Info("projector admin server listening", "addr", adminAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("projector admin server failed", "error", err)
		}
	}()
	return srv
}

// consumeAnalytics runs the analytics consumer loop, logging rather than
// failing the process when it stops with an error. It is meant to be
// started with `go` on its own goroutine.
func consumeAnalytics(consumer *inboundkafka.AnalyticsConsumer, ctx context.Context, logger *slog.Logger, brokers []string) {
	logger.Info("analytics consumer starting", "topic", outboundkafka.AnalyticsTopic, "group", inboundkafka.AnalyticsConsumerGroup, "brokers", brokers)
	if err := consumer.Run(ctx); err != nil {
		logger.Error("analytics consumer stopped", "error", err)
	}
}

// waitForShutdown blocks until SIGINT/SIGTERM, stops the analytics
// consumer, and drains the admin server, bounded by a 10s deadline.
func waitForShutdown(srv *http.Server, cancelConsumer context.CancelFunc) error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	cancelConsumer()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}

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
