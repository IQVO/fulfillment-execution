// Command execution is the composition root for the Fulfillment Execution
// service: it wires env config to adapters, adapters to use cases, and use
// cases to the HTTP router, the Kafka consumers and the outbox relay, all on
// real Postgres and real Kafka (the real-wiring ADR: there is no in-memory
// runtime and no log sink).
//
// Environment (every REQUIRED variable is a boot error that names it, before
// the listener binds):
//
//	DATABASE_URL                           Postgres DSN (REQUIRED)
//	MIGRATIONS_DATABASE_URL                direct DSN for the boot migrations (default DATABASE_URL)
//	MIGRATIONS_PATH                        golang-migrate source directory (default migrations)
//	KAFKA_BROKERS                          comma-separated brokers (REQUIRED: relay, consumers, /readyz)
//	WORK_RELEASED_CONSUMER_GROUP           stable group of the WorkReleased consumer (REQUIRED)
//	PRODUCT_CLASSIFICATION_CONSUMER_GROUP  stable group of the ProductClassified consumer (REQUIRED)
//	TASK_INTERLEAVING_CONSUMER_GROUP       stable group of the InterleavingHintIssued consumer (REQUIRED)
//	PRODUCT_CLASSIFICATION_MODE            kafka (default); permissive/off/http are boot errors
//	TASK_INTERLEAVING_MODE                 kafka (default); permissive/off are boot errors
//	EVENT_PUBLISHER                        kafka (default); log is a boot error
//	OUTBOX_RELAY_INTERVAL, OUTBOX_MAX_ATTEMPTS, PATH_CATALOGUE_SOURCE, PATH_CATALOGUE_FILE,
//	LOCATION_ROLE_MODE, FACILITY_LAYOUT_BASE_URL, HTTP_ADDR, LOG_LEVEL, OTEL_*
//
// GET /readyz is 200 only while Postgres answers a ping AND a Kafka broker
// answers a metadata request (verdict cached 3s, bounded 2s) and, with
// PATH_CATALOGUE_SOURCE=kafka, after the catalogue replay; /healthz is
// liveness only.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	inboundhttp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/bootretry"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/facilitylayout"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/filecatalog"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/inventorystorage"
	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafkacatalog"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/system"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/composition"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/observability"
	"github.com/claudioed/fulfillment-execution/internal/resilience"
)

// workReleasedTopic is the topic wes-work-planning publishes WorkReleased
// events to.
const workReleasedTopic = "warehouse.work-planning.events"

func main() {
	if err := run(context.Background()); err != nil {
		slog.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

// run boots the service and serves until SIGINT/SIGTERM or until ctx is
// cancelled (the integration test stops the real entry point that way).
func run(ctx context.Context) error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	// Misconfiguration is refused before anything is opened or bound.
	cfg, err := loadBootConfig(os.Getenv)
	if err != nil {
		return err
	}

	// Telemetry comes up right after the config check and before any
	// adapter, so everything built below is instrumented. An unreachable
	// Collector is not fatal: the OTLP exporters dial lazily, so the service
	// starts and serves normally with telemetry dropped on the floor.
	defer telemetryCloser(setupTelemetry(ctx, logger), logger)()

	metrics := buildMetrics(logger)

	// consumerCtx/cancelConsumer are declared here because the Kafka
	// catalogue source starts its consumer loop inside loadCatalogue: the
	// loop must be RUNNING before the replay wait below, which blocks on
	// messages actually being read.
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()

	// In Kafka mode the replay runs in the background from here on; the
	// wait for it (waitCatalogue) happens only after the HTTP listener is
	// up (gap fix D24, ADR-0044). File mode returns a nil waitCatalogue.
	catalogue, err := loadCatalogue(ctx, consumerCtx, cfg.kafkaBrokers, logger)
	if err != nil {
		return err
	}
	defer closeCatalogue(catalogue.kafka)

	storage, err := openStorage(ctx, logger, cfg.databaseURL, cfg.migrationsDatabaseURL, cfg.migrationsPath)
	if err != nil {
		return err
	}
	defer storage.pool.Close()

	publisher, relay, closePublisher, err := buildEventPublisher(storage, cfg.kafkaBrokers, logger)
	if err != nil {
		return err
	}
	defer closePublisher()
	clock := system.Clock{}

	// readiness gates GET /readyz (ADR-0029 §graceful shutdown): the drain
	// gate, the catalogue warm-up and the Postgres + Kafka dependency
	// checks. SetNotReady is called as the FIRST step of the shutdown
	// sequence below, before the HTTP server itself stops accepting
	// connections, so a Kubernetes readinessProbe has a chance to observe
	// the flip and stop routing new traffic during the drain window.
	readiness := newReadiness(storage.pool.Ping, cfg.kafkaBrokers)

	classification, err := buildClassification(cfg.classification, storage, cfg.kafkaBrokers, logger)
	if err != nil {
		return err
	}
	defer classification.close() // after its loop stopped (LIFO), before the pool
	packView, err := buildPackView(cfg.packViewGroup, storage, cfg.kafkaBrokers, logger)
	if err != nil {
		return err
	}
	defer packView.close() // after its loop stopped (LIFO), before the pool
	interleaving, err := buildInterleaving(cfg.interleaving, storage, cfg.kafkaBrokers, logger)
	if err != nil {
		return err
	}
	defer interleaving.close()
	lookups := buildOutboundLookups(classification.lookup, packView.lookup, cfg.inventoryStorageURL, logger)

	createTask := &usecases.CreateTask{Tasks: storage.taskRepo, Publisher: publisher, Clock: clock, NewId: newTaskId, UnitOfWork: storage.uow}
	srv := newHTTPServer(storage, publisher, clock, metrics, lookups, interleaving.lookup, createTask, readiness, logger, getenv("HTTP_ADDR", ":8080"))

	// ApplyWorkReleased wraps the same CreateTask the HTTP surface uses,
	// plus the processed-events claim, inside ONE UnitOfWork — the
	// atomicity fix for the lost-WorkReleased bug (its own doc comment
	// has the full story). CreateTask.UnitOfWork is the SAME instance, so
	// its nested Execute joins the outer scope instead of opening a
	// second transaction.
	consumer, closeConsumer := wireWorkReleasedConsumer(cfg.kafkaBrokers, cfg.workReleasedGroup, &usecases.ApplyWorkReleased{
		CreateTask: createTask,
		Processed:  storage.processedEvents,
		Catalogue:  catalogue.catalogue,
		UnitOfWork: storage.uow,
	}, logger)
	defer closeConsumer()

	// The listener starts BEFORE the catalogue replay has caught up, so the
	// startup/liveness probes (/healthz) see a live process while /readyz
	// stays 503 until the replay is done; the WorkReleased consumer, which
	// needs the complete catalogue, starts only afterwards (ADR-0044).
	if err := serveWhileWarmingUp(srv, readiness, catalogue.wait, kafkacatalog.WaitReadyTimeout, logger); err != nil {
		return err
	}
	workReleasedDone := startWorkReleasedConsumer(consumer, consumerCtx, logger, cfg.kafkaBrokers)
	lc := startLookupConsumers(consumerCtx, logger, classification, interleaving, packView)

	// The outbox relay (ADR 0020) runs alongside the HTTP server in the
	// same process, draining outbox_events onto both Kafka topics. It is
	// always on.
	stopRelay, relayDone := startRelay(relay, logger)
	defer stopRelay()

	return serveUntilSignal(ctx, logger, srv, readiness, stopRelay, relayDone, cancelConsumer,
		joinDone(workReleasedDone, lc.classification, lc.interleaving, lc.packView),
		namedDone{"outbox relay", relayDone},
		namedDone{"WorkReleased consumer", workReleasedDone},
		namedDone{"product classification consumer", lc.classification},
		namedDone{"interleaving hint consumer", lc.interleaving},
		namedDone{"pack level consumer", lc.packView},
		namedDone{"process-path catalogue consumer", catalogue.done},
	)
}

// newReadiness builds the /readyz gate: the drain gate plus a Postgres ping
// and a Kafka broker answering a topic-less metadata request (never topics:
// the broker auto-creates them on first write), cached 3s, bounded 2s. The
// ping is a parameter so the gate is testable without a pool.
func newReadiness(ping func(context.Context) error, brokers []string) *inboundhttp.Readiness {
	return inboundhttp.NewReadiness(inboundhttp.DefaultReadinessTTL, inboundhttp.DefaultReadinessTimeout,
		inboundhttp.Dependency{Name: "postgres", Check: ping},
		inboundhttp.Dependency{Name: "kafka", Check: outboundkafka.NewBrokerCheck(brokers).Check},
	)
}

// buildMetrics creates the task counters. A failure is logged, never
// returned: a missing counter must not stop the service from doing work.
func buildMetrics(logger *slog.Logger) *observability.Metrics {
	metrics, err := observability.NewMetrics()
	if err != nil {
		logger.Error("task metrics unavailable", "error", err)
		return nil
	}
	return metrics
}

// serveWhileWarmingUp starts the HTTP listener and, when waitReady is not
// nil (Kafka catalogue mode), holds readiness not-ready until waitReady
// returns within timeout (gap fix D24, ADR-0044). /healthz answers from
// the start, so the startup probe no longer has to cover the catalogue
// replay; /readyz answers 503 {"status":"not_ready"} until the replay has
// caught up, so no Service routes traffic to the pod before then. A replay
// that fails or times out is still a fatal boot error: the listener is shut
// down and the error returned.
func serveWhileWarmingUp(srv *http.Server, readiness *inboundhttp.Readiness, waitReady func(context.Context) error, timeout time.Duration, logger *slog.Logger) error {
	if waitReady != nil {
		readiness.StartWarmUp()
	}
	go serveHTTP(srv, logger, srv.Addr)
	if waitReady == nil {
		return nil
	}
	waitCtx, cancelWait := context.WithTimeout(context.Background(), timeout)
	defer cancelWait()
	if err := waitReady(waitCtx); err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return fmt.Errorf("process-path catalogue did not become ready within %s: %w", timeout, err)
	}
	readiness.FinishWarmUp()
	logger.Info("readiness: warm-up finished; /readyz now reports ready")
	return nil
}

// closeCatalogue closes the Kafka process-path catalogue when one is wired.
func closeCatalogue(c *kafkacatalog.Consumer) {
	if c != nil {
		_ = c.Close()
	}
}

// startRelay runs the outbox relay in the background and returns its stop
// func plus a channel closed once it has returned.
func startRelay(relay *postgres.OutboxRelay, logger *slog.Logger) (context.CancelFunc, <-chan struct{}) {
	relayDone := make(chan struct{})
	relayCtx, stopRelay := context.WithCancel(context.Background())
	go runRelay(relayCtx, relayDone, relay, logger)
	return stopRelay, relayDone
}

// outboundLookups bundles the two ACL lookups the HTTP use cases read:
// product classification (local copy of product-master events, ADR-0039)
// and station location role (facility-layout over HTTP, behind the shared
// circuit-breaker recorder).
type outboundLookups struct {
	classification ports.ProductClassificationLookup
	locationRole   ports.LocationRoleLookup
	// packs and caseBreaker back POST /tasks/{id}/scans and
	// POST /tasks/{id}/break-case (ADR 0054). Nil in unit tests that build a
	// server over test doubles.
	packs       ports.PackViewLookup
	caseBreaker ports.CaseBreaker
	// cases backs ship-in-own-container on POST /tasks/{id}/seal-package
	// (ADR 0056): the same local PackView as packs, read for a SKU's CASE
	// levels. Nil exactly when packs is nil.
	cases ports.CaseLevelLookup
}

// buildOutboundLookups wires the location-role lookup plus its
// circuit-breaker gauge next to the already-built classification lookup.
// circuitBreakerMetrics wires the facility-layout breaker's OnStateChange
// into the circuit_breaker.state gauge (ADR-0029), reusing the SAME OTel
// MeterProvider observability.Setup already installed rather than standing
// up a second Prometheus registry. Errors here mirror NewMetrics' contract
// (invalid instrument name only, a programming error) -- non-fatal: a nil
// recorder just means this process runs without the gauge, never without
// the breaker itself.
//
// LOCATION_ROLE_MODE is a REST sync edge to facility-layout, not a database
// or a broker: it stays out of the real-wiring migration (ADR-0024).
func buildOutboundLookups(classification ports.ProductClassificationLookup, packs ports.PackViewLookup, inventoryStorageURL string, logger *slog.Logger) outboundLookups {
	circuitBreakerMetrics, cbmErr := observability.NewCircuitBreakerMetrics()
	if cbmErr != nil {
		logger.Warn("circuit breaker metrics unavailable; breakers will run without the circuit_breaker.state gauge", "error", cbmErr)
	}
	return outboundLookups{
		classification: classification,
		packs:          packs,
		cases:          caseLevelsOf(packs),
		caseBreaker:    inventorystorage.NewBreakerClient(inventorystorage.NewClient(inventoryStorageURL, nil), circuitBreakerMetrics),
		locationRole:   buildLocationRoleLookup(getenv("LOCATION_ROLE_MODE", "permissive"), os.Getenv("FACILITY_LAYOUT_BASE_URL"), circuitBreakerMetrics, logger),
	}
}

// caseLevelsOf returns the CASE-level read side of the PackView behind packs
// (the Postgres store implements both), or nil when packs is nil or does not
// offer it.
func caseLevelsOf(packs ports.PackViewLookup) ports.CaseLevelLookup {
	cases, _ := packs.(ports.CaseLevelLookup)
	return cases
}

// newHTTPServer builds the handler set and the HTTP server around it, with
// the idempotency pool bound (the real runtime always has one; only the unit
// tests build a server over test doubles without it).
func newHTTPServer(storage storageAdapters, publisher ports.EventPublisher, clock system.Clock, metrics *observability.Metrics, lookups outboundLookups, hintLookup ports.InterleavingHintLookup, createTask *usecases.CreateTask, readiness *inboundhttp.Readiness, logger *slog.Logger, httpAddr string) *http.Server {
	handlers := buildHandlers(storage, publisher, clock, metrics, lookups.classification, lookups.locationRole, hintLookup, createTask, readiness, logger)
	handlers.SealPackage.CaseLevels = lookups.cases
	if lookups.packs != nil {
		handlers.ConfirmPickScan = &usecases.ConfirmPickScan{Tasks: storage.taskRepo, Packs: lookups.packs, Clock: clock, UnitOfWork: storage.uow}
	}
	if lookups.caseBreaker != nil {
		handlers.CompleteCaseBreak = &usecases.CompleteCaseBreak{Tasks: storage.taskRepo, Breaker: lookups.caseBreaker, Publisher: publisher, Clock: clock, Metrics: metricsPort(metrics), UnitOfWork: storage.uow}
	}
	router := inboundhttp.NewRouter(handlers, logger, inboundhttp.WithIdempotencyPool(storage.pool))
	return &http.Server{Addr: httpAddr, Handler: router, ReadHeaderTimeout: 5 * time.Second}
}

// wireWorkReleasedConsumer builds the WorkReleased consumer plus its
// dead-letter writer (<topic>.dlq, ADR-0004's documented gap) and returns one
// close func releasing the DLQ writer and then the consumer.
func wireWorkReleasedConsumer(brokers []string, consumerGroup string, apply *usecases.ApplyWorkReleased, logger *slog.Logger) (*inboundkafka.Consumer, func()) {
	consumer := inboundkafka.NewConsumerWithGroup(brokers, workReleasedTopic, consumerGroup, apply, logger)
	dlqWriter := wireDeadLetter(consumer, brokers, logger)
	return consumer, func() {
		_ = dlqWriter.Close()
		_ = consumer.Close()
	}
}

// setupTelemetry wires OTel and returns the shutdown flush, which the
// caller defers. A setup error is logged, not returned: an unreachable
// Collector must not stop the service.
func setupTelemetry(rootCtx context.Context, logger *slog.Logger) func(context.Context) error {
	serviceName := observability.ServiceName()
	otelShutdown, err := observability.Setup(rootCtx, serviceName, observability.ServiceVersion(), observability.Endpoint())
	if err != nil {
		logger.Error("opentelemetry setup degraded", "error", err)
	}
	if otelShutdown == nil {
		logger.Warn("opentelemetry disabled; traces and metrics will not be exported")
	}
	logger.Info("telemetry configured",
		"service_name", serviceName,
		"service_version", observability.ServiceVersion(),
		"otlp_endpoint", observability.Endpoint(),
	)
	return otelShutdown
}

// telemetryCloser returns the func that flushes OTel on exit (a no-op when
// telemetry is disabled), bounded to 5s.
func telemetryCloser(otelShutdown func(context.Context) error, logger *slog.Logger) func() {
	if otelShutdown == nil {
		return func() {}
	}
	return func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := otelShutdown(flushCtx); err != nil {
			logger.Error("opentelemetry shutdown failed", "error", err)
		}
	}
}

// loadedCatalogue is the resolved process-path catalogue source: the
// catalogue itself, and (Kafka source only) its consumer, the replay wait
// and a channel closed when the consumer loop returned.
type loadedCatalogue struct {
	catalogue ports.PathCatalogue
	kafka     *kafkacatalog.Consumer
	wait      func(context.Context) error
	done      <-chan struct{}
}

// loadCatalogue resolves the process-path catalogue from its switchable
// SOURCE (PATH_CATALOGUE_SOURCE=file|kafka, defaulting to "file" so
// nothing about existing deployments changes unless explicitly opted in).
// The catalogue source is configuration, not a database or a broker
// stand-in: the real-wiring ADR leaves it as it was (warehouse-infra sets
// kafka; "file" is the documented rollback switch).
//
// "file": the original behavior. Loaded and validated once at boot,
// before anything else stands up — a missing or malformed catalogue
// file stops this service from starting at all, never falls back to
// a partial/empty catalogue (see filecatalog.Load's doc comment).
//
// "kafka": consumes process-path-management's published events
// instead of warehouse-infra's static YAML file (see
// internal/adapters/outbound/kafkacatalog's package doc comment for
// the full rationale and how it preserves the same "never serve
// traffic against an incomplete catalogue" guarantee via a
// readiness gate rather than a boot-time file read). The consumer loop
// is started here on consumerCtx, and the returned wait blocks
// until the replay reaches the startup watermark; the caller runs it
// only after the HTTP listener is up, with /readyz false meanwhile
// (gap fix D24, ADR-0044). wait is nil in file mode.
func loadCatalogue(rootCtx, consumerCtx context.Context, brokers []string, logger *slog.Logger) (loadedCatalogue, error) {
	if getenv("PATH_CATALOGUE_SOURCE", "file") != "kafka" {
		catalogue, err := filecatalog.Load(getenv("PATH_CATALOGUE_FILE", "/etc/fulfillment-execution/process-paths.yaml"))
		if err != nil {
			return loadedCatalogue{}, fmt.Errorf("failed to load the process-path catalogue: %w", err)
		}
		logger.Info("process-path catalogue source configured", "source", "file", "paths", catalogue.Ids())
		return loadedCatalogue{catalogue: catalogue}, nil
	}

	var kafkaCatalogue *kafkacatalog.Consumer
	// Retried: kafkacatalog.NewConsumer's newTargetOffsets dials the
	// broker synchronously (kafkago.DialContext) to capture the
	// readiness watermark before any consuming begins, and that dial
	// is exactly this fleet's known ~10s post-start
	// first-outbound-dial reset (Istio native sidecars;
	// holdApplicationUntilProxyStarts is a no-op for them). A single
	// attempt turns that transient condition into CrashLoopBackOff.
	if err := bootretry.Retry(rootCtx, logger, "dial process-path kafka catalogue", func() error {
		c, err := kafkacatalog.NewConsumer(rootCtx, brokers, logger)
		if err != nil {
			return err
		}
		kafkaCatalogue = c
		return nil
	}); err != nil {
		return loadedCatalogue{}, fmt.Errorf("failed to start the process-path Kafka catalogue: %w", err)
	}
	logger.Info("process-path catalogue source configured", "source", "kafka", "topic", kafkacatalog.Topic)

	done := make(chan struct{})
	go func() {
		defer close(done)
		logger.Info("process-path catalogue consumer running", "topic", kafkacatalog.Topic)
		if err := kafkaCatalogue.Run(consumerCtx); err != nil {
			logger.Error("process-path catalogue consumer stopped", "error", err)
		}
	}()
	return loadedCatalogue{catalogue: kafkaCatalogue, kafka: kafkaCatalogue, wait: catalogueWaiter(kafkaCatalogue, logger), done: done}, nil
}

// catalogueWaiter returns the replay wait for the Kafka catalogue: it
// blocks until every partition has reached the offset captured at startup.
func catalogueWaiter(c *kafkacatalog.Consumer, logger *slog.Logger) func(context.Context) error {
	return func(ctx context.Context) error {
		logger.Info("http listener up; /readyz stays not ready until the process-path catalogue has replayed its initial history")
		if err := c.WaitReady(ctx); err != nil {
			return err
		}
		logger.Info("process-path catalogue is ready", "paths", c.Ids())
		return nil
	}
}

// storageAdapters groups the Postgres persistence adapters. pool and uow are
// always set by openStorage; the zero-pool form exists only for unit tests
// that build a server over test doubles.
type storageAdapters struct {
	taskRepo          ports.TaskRepo
	stationRepo       ports.StationRepo
	packageRepo       ports.PackageRepo
	processedEvents   ports.ProcessedEvents
	consolidationRepo ports.OrderConsolidationRepo
	pool              *pgxpool.Pool
	uow               ports.UnitOfWork
}

// openStorage opens the Postgres outbound adapters; an empty databaseURL is
// the boot error errNoDatabase (there is no in-memory path). It runs the
// schema migrations, opens the pool, and verifies it with a ping, each
// under boot retry: this fleet's Istio native sidecars reset EVERY pod's
// first outbound TCP dial ~10s after the app starts
// (holdApplicationUntilProxyStarts is a no-op for native sidecars). A single
// attempt turns that transient condition into CrashLoopBackOff; the retry
// still fails closed once its budget is exhausted.
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
// advisory lock never behaves as a real mutex. Losing replicas crash-loop
// with `pq: unnamed prepared statement does not exist` / `pq: canceling
// statement due to statement timeout` until one wins the race. See
// ADR-0031 (and order-management's ADR-0029, the reference
// implementation this mirrors) for the full incident and fix. Callers
// pass MIGRATIONS_DATABASE_URL when set (warehouse-infra provisions it as
// a direct, non-pooled DSN alongside DATABASE_URL for all 9 OLTP
// services, PR #44) or fall back to databaseURL itself for any
// environment that doesn't provision the split (local dev, CI
// integration tests).
func openStorage(rootCtx context.Context, logger *slog.Logger, databaseURL, migrationsDatabaseURL, migrationsPath string) (storageAdapters, error) {
	if databaseURL == "" {
		return storageAdapters{}, errNoDatabase
	}

	if err := bootretry.Retry(rootCtx, logger, "run migrations", func() error {
		return postgres.Migrate(migrationsDatabaseURL, migrationsPath)
	}); err != nil {
		return storageAdapters{}, err
	}
	pool, err := postgres.NewPool(rootCtx, databaseURL)
	if err != nil {
		return storageAdapters{}, err
	}
	// ParseConfig/NewWithConfig do not themselves establish a
	// connection, so without this the first-dial reset would surface
	// inside the first real request instead of at boot.
	if err := bootretry.Retry(rootCtx, logger, "ping database", func() error {
		return pool.Ping(rootCtx)
	}); err != nil {
		pool.Close()
		return storageAdapters{}, err
	}
	if err := postgres.RecordPoolStats(pool); err != nil {
		logger.Error("pgxpool metrics unavailable", "error", err)
	}
	return storageAdapters{
		taskRepo:          postgres.NewTaskRepo(pool),
		stationRepo:       postgres.NewStationRepo(pool),
		packageRepo:       postgres.NewPackageRepo(pool),
		processedEvents:   postgres.NewProcessedEventsRepo(pool),
		consolidationRepo: postgres.NewOrderConsolidationRepo(pool),
		pool:              pool,
		uow:               postgres.NewUnitOfWork(pool),
	}, nil
}

// buildHandlers wires every use case into its inbound HTTP handler.
// createTask is built by the caller (the WorkReleased consumer shares it)
// and passed in; everything else is constructed here.
func buildHandlers(storage storageAdapters, publisher ports.EventPublisher, clock system.Clock, metrics *observability.Metrics, classificationLookup ports.ProductClassificationLookup, locationRoleLookup ports.LocationRoleLookup, hintLookup ports.InterleavingHintLookup, createTask *usecases.CreateTask, readiness *inboundhttp.Readiness, logger *slog.Logger) *inboundhttp.Handlers {
	return &inboundhttp.Handlers{
		CreateTask:         createTask,
		ClaimNext:          &usecases.ClaimNext{Tasks: storage.taskRepo, Stations: storage.stationRepo, Publisher: publisher, Clock: clock, Metrics: metricsPort(metrics), UnitOfWork: storage.uow, Hints: hintLookup, Logger: logger},
		RenewLease:         &usecases.RenewLease{Tasks: storage.taskRepo, Clock: clock},
		CompleteTask:       &usecases.CompleteTask{Tasks: storage.taskRepo, Publisher: publisher, Clock: clock, Metrics: metricsPort(metrics), UnitOfWork: storage.uow},
		SealPackage:        &usecases.SealPackage{Tasks: storage.taskRepo, Packages: storage.packageRepo, Publisher: publisher, Clock: clock, NewId: newPackageId, ClassificationLookup: classificationLookup, UnitOfWork: storage.uow},
		RunSlam:            &usecases.RunSlam{Packages: storage.packageRepo, Publisher: publisher, Clock: clock, UnitOfWork: storage.uow},
		GetQueueDepth:      &usecases.GetQueueDepth{Tasks: storage.taskRepo},
		ExpireLeases:       &usecases.ExpireLeases{Tasks: storage.taskRepo, Publisher: publisher, Clock: clock, UnitOfWork: storage.uow},
		RegisterStation:    &usecases.RegisterStation{Stations: storage.stationRepo, Publisher: publisher, LocationLookup: locationRoleLookup},
		GetTasksByOrderRef: &usecases.GetTasksByOrderRef{Tasks: storage.taskRepo},
		CheckInStation:     &usecases.CheckInStation{Stations: storage.stationRepo},
		CheckOutStation:    &usecases.CheckOutStation{Stations: storage.stationRepo},
		ArriveAtRebin: &usecases.ArriveAtRebin{
			Consolidations: storage.consolidationRepo,
			CreateTask:     createTask,
			Publisher:      publisher,
			Clock:          clock,
			UnitOfWork:     storage.uow,
		},
		GetInstalledCapacity: &usecases.GetInstalledCapacity{Stations: storage.stationRepo},
		SweepCPTMisses:       &usecases.SweepCPTMisses{Tasks: storage.taskRepo, Publisher: publisher, Clock: clock, UnitOfWork: storage.uow},
		// Package read model (ADR-0033): GET /packages/{id} and
		// GET /packages?orderRef=.
		GetPackage:            &usecases.GetPackage{Packages: storage.packageRepo},
		GetPackagesByOrderRef: &usecases.GetPackagesByOrderRef{Packages: storage.packageRepo},
		// readiness backs GET /readyz (ADR-0029 §graceful shutdown):
		// flipped to not-ready as the FIRST step of shutdown, below,
		// before anything else stops.
		Readiness: readiness,
	}
}

// wireDeadLetter wires the WorkReleased consumer's dead-letter writer
// (ADR-0004's documented gap): a message that fails processing is
// published to <topic>.dlq instead of only being logged. The DLQ writer
// needs the same brokers as every other writer and is always wired now that
// there is no log-only mode. AllowAutoTopicCreation mirrors every other
// writer in this codebase (outboundkafka.NewDeadLetterWriter). It returns
// the writer for the caller to close.
func wireDeadLetter(consumer *inboundkafka.Consumer, brokers []string, logger *slog.Logger) *kafkago.Writer {
	dlqWriter := newDeadLetterWriter(brokers)
	consumer.DeadLetter = dlqWriter
	logger.Info("dead-letter handling configured", "dlq_topic", inboundkafka.DeadLetterTopic(workReleasedTopic))
	return dlqWriter
}

// serveHTTP runs the HTTP server, logging a failed ListenAndServe unless
// it is the expected ErrServerClosed of shutdown. Meant to be started
// with `go`.
func serveHTTP(srv *http.Server, logger *slog.Logger, addr string) {
	logger.Info("http server listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("http server failed", "error", err)
	}
}

// startWorkReleasedConsumer runs the WorkReleased consumer loop in the
// background and returns a channel closed once the loop has fully
// returned, so graceful shutdown can wait for a real stop.
func startWorkReleasedConsumer(consumer *inboundkafka.Consumer, ctx context.Context, logger *slog.Logger, brokers []string) <-chan struct{} {
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		logger.Info("kafka consumer starting", "topic", workReleasedTopic, "brokers", brokers)
		if err := consumer.Run(ctx); err != nil {
			logger.Error("kafka consumer stopped", "error", err)
		}
	}()
	return consumerDone
}

// runRelay drains the outbox onto both Kafka topics until ctx is
// cancelled, then closes done. Meant to be started with `go`.
func runRelay(ctx context.Context, done chan<- struct{}, relay *postgres.OutboxRelay, logger *slog.Logger) {
	defer close(done)
	logger.Info("outbox relay running", "topics", []string{outboundkafka.Topic, outboundkafka.AnalyticsTopic})
	if err := relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("outbox relay stopped", "error", err)
	}
}

// namedDone is a background loop and the channel closed when it returned.
type namedDone struct {
	name string
	done <-chan struct{}
}

// watchDone returns a channel that receives the name of every loop that
// returns. The loops only return when stopped or on an unrecoverable failure,
// so a value read while the service is serving means one died on its own.
func watchDone(loops ...namedDone) <-chan string {
	died := make(chan string, len(loops))
	for _, l := range loops {
		if l.done == nil {
			continue
		}
		go func() {
			<-l.done
			died <- l.name
		}()
	}
	return died
}

// serveUntilSignal blocks until SIGINT/SIGTERM, ctx cancellation or one of
// the loops returning on its own, then stops the Kafka consumers and drains
// the HTTP server and the outbox relay, bounded by a 10s deadline. A loop
// that died on its own is returned as an error so the process exits non-zero
// and the orchestrator restarts it, instead of sitting "alive" with a dead
// consumer or relay.
func serveUntilSignal(ctx context.Context, logger *slog.Logger, srv *http.Server, readiness *inboundhttp.Readiness, stopRelay context.CancelFunc, relayDone <-chan struct{}, cancelConsumer context.CancelFunc, consumerDone <-chan struct{}, loops ...namedDone) error {
	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	died := watchDone(loops...)
	var dead error
	select {
	case <-sigCtx.Done():
	case name := <-died:
		dead = fmt.Errorf("%s stopped unexpectedly: exiting so the orchestrator restarts the service", name)
		logger.Error("a background loop stopped on its own; shutting down", "loop", name)
	}

	// Graceful shutdown (ADR-0029 §graceful shutdown), in order:
	//
	//  1. Flip readiness to not-ready FIRST, before anything else
	//     stops — a Kubernetes readinessProbe polling /readyz needs a
	//     window to observe this and stop routing NEW traffic to this
	//     pod before step 2 below ever closes the listener, so a
	//     request racing the SIGTERM is far less likely to be routed
	//     here only to hit a closing connection.
	//  2. Stop accepting new HTTP connections and drain in-flight
	//     requests, bounded by shutdownCtx.
	//  3. Stop the outbox relay and the Kafka consumers' loops
	//     cleanly: cancel their contexts (no new message is
	//     fetched/handled after this) and wait, bounded by the SAME
	//     shutdownCtx, for their goroutines to actually finish
	//     in-flight work rather than merely asking them to stop and
	//     moving on.
	//  4. Only THEN do the deferred consumer.Close()/kafkaCatalogue.
	//     Close()/pool.Close() calls (registered earlier, so by defer's
	//     LIFO order they run AFTER every consumer/relay goroutine above
	//     has already stopped touching them, not before).
	readiness.SetNotReady()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := srv.Shutdown(shutdownCtx)

	// Let the relay finish its in-flight pass so an event committed by a
	// request that completed just before shutdown is not stranded until
	// the next pod boots; bounded by the same shutdown deadline.
	stopRelay()
	select {
	case <-relayDone:
	case <-shutdownCtx.Done():
		logger.Warn("outbox relay did not stop before the shutdown deadline")
	}

	// Stop the consumers' loops cleanly: cancel so no NEW message is
	// fetched, then wait (bounded) for any message already being handled
	// to finish before this function returns and the deferred
	// consumer.Close()/kafkaCatalogue.Close() calls run.
	cancelConsumer()
	select {
	case <-consumerDone:
	case <-shutdownCtx.Done():
		logger.Warn("kafka consumer did not stop before the shutdown deadline")
	}

	return errors.Join(dead, err)
}

// buildEventPublisher resolves the publisher env config
// (OUTBOX_RELAY_INTERVAL, OUTBOX_MAX_ATTEMPTS) and delegates to the wiring
// shared with cmd/mcp (composition.BuildEventPublisher), so both deployables
// publish through the SAME transactional outbox (ADR-0008, ADR-0020). This is
// the one process that also runs the outbox relay.
func buildEventPublisher(storage storageAdapters, brokers []string, logger *slog.Logger) (ports.EventPublisher, *postgres.OutboxRelay, func(), error) {
	return composition.BuildEventPublisher(composition.PublisherConfig{
		Brokers:          brokers,
		RunRelay:         true,
		RelayInterval:    durationEnv("OUTBOX_RELAY_INTERVAL", time.Second),
		RelayMaxAttempts: outboxMaxAttempts(logger),
	}, storage.pool, storage.taskRepo, storage.stationRepo, logger)
}

// outboxMaxAttempts parses OUTBOX_MAX_ATTEMPTS (gap fix D12, ADR 0042): how
// many failed sends exclude an outbox row from the relay's claims. Unset
// means postgres.DefaultOutboxMaxAttempts (20); 0 means unlimited.
func outboxMaxAttempts(logger *slog.Logger) int {
	return intEnv(logger, "OUTBOX_MAX_ATTEMPTS", postgres.DefaultOutboxMaxAttempts)
}

// intEnv parses key as a non-negative integer, falling back (with a WARN) on
// a malformed or negative value: like the relay interval, the cap is a
// tuning knob that never fails the boot.
func intEnv(logger *slog.Logger, key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n < 0 {
		logger.Warn("ignoring invalid integer env value", "key", key, "value", v, "default", fallback)
		return fallback
	}
	return n
}

// durationEnv parses key as a time.Duration, falling back on absence or a
// malformed/non-positive value: the relay interval is a tuning knob, not
// a contract, so it never fails the boot.
func durationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

// metricsPort converts a possibly-nil *observability.Metrics into a
// ports.Metrics, avoiding the typed-nil trap: assigning a nil *Metrics
// straight into the interface field would produce a non-nil interface whose
// method calls panic, defeating the use cases' nil check.
func metricsPort(m *observability.Metrics) ports.Metrics {
	if m == nil {
		return nil
	}
	return m
}

// newLogger builds a JSON slog.Logger writing to stdout, with its minimum
// level set from a LOG_LEVEL value (debug|info|warn|warning|error,
// case-insensitive, defaulting to info for anything else). The JSON handler
// is wrapped so records logged with a context carrying an active span also
// carry that span's trace_id and span_id.
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

// buildLocationRoleLookup selects the outbound ports.LocationRoleLookup
// adapter via LOCATION_ROLE_MODE (http|permissive), defaulting to
// "permissive" so existing tests, CI and deployments that do not set the
// env var are unaffected (see ADR-0024). "http" requires
// FACILITY_LAYOUT_BASE_URL. This is a REST sync edge to a sibling context,
// out of scope of the real-wiring migration (a follow-up).
//
// In http mode the real Client is wrapped in retry (jittered, max 3
// attempts -- GetRole is a pure read, safe to retry) plus a
// per-dependency circuit breaker (ADR-0029): on a trip, calls fall back
// to the SAME fail-open behaviour this client already had. recorder
// feeds the breaker's state transitions into the circuit_breaker.state
// gauge; nil is fine.
func buildLocationRoleLookup(mode, facilityLayoutBaseURL string, recorder resilience.StateRecorder, logger *slog.Logger) ports.LocationRoleLookup {
	if !strings.EqualFold(mode, "http") {
		return facilitylayout.NewPermissiveLookup()
	}
	logger.Info("location role lookup configured", "mode", "http", "facility_layout_base_url", facilityLayoutBaseURL, "circuit_breaker", "enabled", "retry", "enabled")
	return facilitylayout.NewBreakerClient(facilitylayout.NewClient(facilityLayoutBaseURL, nil), recorder)
}

func newTaskId() shared.TaskId {
	return shared.TaskId(uuidLike())
}

func newPackageId() shared.PackageId {
	return shared.PackageId(uuidLike())
}

// uuidLike generates a time-ordered, sufficiently-unique id without pulling
// in an external UUID dependency.
func uuidLike() string {
	return time.Now().UTC().Format("20060102T150405.000000000")
}

// newDeadLetterWriter builds the WorkReleased consumer's DLQ writer through
// the shared outbound-kafka constructor, so it carries the same Hash balancer,
// RequireAll acks and 10ms BatchTimeout as every other writer.
func newDeadLetterWriter(brokers []string) *kafkago.Writer {
	return outboundkafka.NewDeadLetterWriter(brokers)
}
