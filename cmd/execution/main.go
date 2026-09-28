// Command execution is the composition root for the Fulfillment Execution
// service: it wires env config to adapters, adapters to use cases, and use
// cases to the HTTP router.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	inboundhttp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/bootretry"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/facilitylayout"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/filecatalog"
	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafkacatalog"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/productclassification"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/observability"
	"github.com/claudioed/fulfillment-execution/internal/resilience"
)

// workReleasedTopic is the topic wes-work-planning publishes WorkReleased
// events to.
const workReleasedTopic = "warehouse.work-planning.events"

func main() {
	if err := run(); err != nil {
		slog.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	rootCtx := context.Background()
	httpAddr := getenv("HTTP_ADDR", ":8080")
	databaseURL := os.Getenv("DATABASE_URL")
	// MIGRATIONS_DATABASE_URL, when set, is a DIRECT (non-pooled,
	// session-mode) Postgres connection string used ONLY for the
	// golang-migrate startup step inside openStorage below — the pgxpool
	// this process serves requests through still uses databaseURL,
	// unchanged. See openStorage's doc comment and ADR-0031 for the full
	// "why": golang-migrate's postgres driver takes a session-scoped
	// `SELECT pg_advisory_lock($1)` to serialize concurrent migration
	// runs, which PgBouncer's transaction-pooling mode (this fleet's
	// pool_mode for every OLTP DATABASE_URL, warehouse-infra PR #43)
	// does not support — see order-management ADR-0029, the reference
	// implementation this fix mirrors. Falls back to databaseURL when
	// unset, which is every environment that doesn't provision the split
	// (local dev, CI integration tests, a cluster whose Terraform
	// predates this fix) — byte-identical to this service's behavior
	// before this change in that case.
	migrationsDatabaseURL := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
	kafkaBrokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")

	// Telemetry comes up right after the logger and before any adapter, so
	// everything built below is instrumented. An unreachable Collector is
	// not fatal: the OTLP exporters dial lazily, so the service starts and
	// serves normally with telemetry dropped on the floor.
	otelShutdown := setupTelemetry(rootCtx, logger)
	if otelShutdown != nil {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := otelShutdown(ctx); err != nil {
				logger.Error("opentelemetry shutdown failed", "error", err)
			}
		}()
	}

	metrics, err := observability.NewMetrics()
	if err != nil {
		// A missing counter must not stop the service from doing work.
		logger.Error("task metrics unavailable", "error", err)
		metrics = nil
	}

	// consumerCtx/cancelConsumer are declared here because the Kafka
	// catalogue source needs its consumer loop RUNNING before WaitReady is
	// called inside loadCatalogue -- WaitReady blocks on messages actually
	// being read, so starting the goroutine after the wait would deadlock
	// until WaitReadyTimeout on every single boot.
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()

	catalogue, kafkaCatalogue, err := loadCatalogue(rootCtx, consumerCtx, kafkaBrokers, logger)
	if err != nil {
		return err
	}

	storage, err := openStorage(rootCtx, logger, databaseURL, migrationsDatabaseURL, "migrations")
	if err != nil {
		return err
	}
	if storage.pool != nil {
		defer storage.pool.Close()
	}

	publisher, relay, closePublisher := buildEventPublisher(storage.pool, kafkaBrokers, storage.taskRepo, storage.stationRepo, logger)
	defer closePublisher()
	clock := memory.SystemClock{}

	// readiness gates GET /readyz (ADR-0029 §graceful shutdown). The
	// zero value is ready; SetNotReady is called as the FIRST step of
	// the shutdown sequence below, before the HTTP server itself stops
	// accepting connections, so a Kubernetes readinessProbe has a
	// chance to observe the flip and stop routing new traffic during
	// the drain window that follows.
	readiness := &inboundhttp.Readiness{}

	lookups := buildOutboundLookups(logger)

	createTask := &usecases.CreateTask{Tasks: storage.taskRepo, Publisher: publisher, Clock: clock, NewId: newTaskId, UnitOfWork: storage.uow}
	srv := newHTTPServer(storage, publisher, clock, metrics, lookups, createTask, readiness, logger, httpAddr)

	consumer, closeConsumer := wireWorkReleasedConsumer(kafkaBrokers, catalogue, storage, createTask, logger)
	defer closeConsumer()
	if kafkaCatalogue != nil {
		defer func() { _ = kafkaCatalogue.Close() }()
	}

	go serveHTTP(srv, logger, httpAddr)
	consumerDone := startWorkReleasedConsumer(consumer, consumerCtx, logger, kafkaBrokers)

	// The outbox relay (ADR 0020) runs alongside the HTTP server in the
	// same process, draining outbox_events onto both Kafka topics. It is
	// only wired when both Postgres and the kafka publisher are configured.
	relayDone := make(chan struct{})
	relayCtx, stopRelay := context.WithCancel(context.Background())
	defer stopRelay()
	if relay != nil {
		go runRelay(relayCtx, relayDone, relay, logger)
	} else {
		close(relayDone)
	}

	return waitForShutdown(logger, srv, readiness, stopRelay, relayDone, cancelConsumer, consumerDone)
}

// outboundLookups bundles the two cross-context ACL lookups: product
// classification (inventory-storage) and station location role
// (facility-layout), each behind the shared circuit-breaker recorder.
type outboundLookups struct {
	classification ports.ProductClassificationLookup
	locationRole   ports.LocationRoleLookup
}

// buildOutboundLookups wires both outbound lookups plus their shared
// circuit-breaker gauge. circuitBreakerMetrics wires both outbound
// breakers' OnStateChange into the circuit_breaker.state gauge
// (ADR-0029), reusing the SAME OTel MeterProvider observability.Setup
// already installed rather than standing up a second Prometheus registry.
// Errors here mirror NewMetrics' contract (invalid instrument name only,
// a programming error) -- non-fatal: a nil recorder just means this
// process runs without the gauge, never without the breaker itself.
func buildOutboundLookups(logger *slog.Logger) outboundLookups {
	circuitBreakerMetrics, cbmErr := observability.NewCircuitBreakerMetrics()
	if cbmErr != nil {
		logger.Warn("circuit breaker metrics unavailable; breakers will run without the circuit_breaker.state gauge", "error", cbmErr)
	}
	return outboundLookups{
		classification: buildClassificationLookup(getenv("PRODUCT_CLASSIFICATION_MODE", "permissive"), os.Getenv("INVENTORY_STORAGE_BASE_URL"), circuitBreakerMetrics, logger),
		locationRole:   buildLocationRoleLookup(getenv("LOCATION_ROLE_MODE", "permissive"), os.Getenv("FACILITY_LAYOUT_BASE_URL"), circuitBreakerMetrics, logger),
	}
}

// newHTTPServer builds the handler set and the HTTP server around it,
// with the idempotency pool bound when Postgres is configured.
func newHTTPServer(storage storageAdapters, publisher ports.EventPublisher, clock memory.SystemClock, metrics *observability.Metrics, lookups outboundLookups, createTask *usecases.CreateTask, readiness *inboundhttp.Readiness, logger *slog.Logger, httpAddr string) *http.Server {
	handlers := buildHandlers(storage, publisher, clock, metrics, lookups.classification, lookups.locationRole, createTask, readiness)
	router := inboundhttp.NewRouter(handlers, logger, inboundhttp.WithIdempotencyPool(storage.pool))
	return &http.Server{Addr: httpAddr, Handler: router, ReadHeaderTimeout: 5 * time.Second}
}

// wireWorkReleasedConsumer builds the WorkReleased consumer plus its
// dead-letter writer (when EVENT_PUBLISHER=kafka) and returns one close
// func releasing the DLQ writer and then the consumer, mirroring the
// original defer registration order.
func wireWorkReleasedConsumer(brokers []string, catalogue ports.PathCatalogue, storage storageAdapters, createTask *usecases.CreateTask, logger *slog.Logger) (*inboundkafka.Consumer, func()) {
	consumerGroup := getenv("WORK_RELEASED_CONSUMER_GROUP", "fulfillment-execution")
	consumer := inboundkafka.NewConsumerWithGroup(brokers, workReleasedTopic, consumerGroup, createTask, storage.processedEvents, catalogue, logger)
	dlqWriter := maybeWireDeadLetter(consumer, brokers, logger)
	return consumer, func() {
		if dlqWriter != nil {
			_ = dlqWriter.Close()
		}
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

// loadCatalogue resolves the process-path catalogue from its switchable
// SOURCE (PATH_CATALOGUE_SOURCE=file|kafka, defaulting to "file" so
// nothing about existing deployments changes unless explicitly opted in —
// same additive-and-defaulted-off convention as EVENT_PUBLISHER).
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
// readiness gate rather than a boot-time file read). consumerCtx must
// already be live: the catalogue's consumer loop has to be RUNNING
// before WaitReady is called, or the readiness wait deadlocks.
func loadCatalogue(rootCtx, consumerCtx context.Context, brokers []string, logger *slog.Logger) (ports.PathCatalogue, *kafkacatalog.Consumer, error) {
	switch getenv("PATH_CATALOGUE_SOURCE", "file") {
	case "kafka":
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
			return nil, nil, fmt.Errorf("failed to start the process-path Kafka catalogue: %w", err)
		}
		catalogue := ports.PathCatalogue(kafkaCatalogue)
		logger.Info("process-path catalogue source configured", "source", "kafka", "topic", kafkacatalog.Topic)

		go func() {
			logger.Info("process-path catalogue consumer running", "topic", kafkacatalog.Topic)
			if err := kafkaCatalogue.Run(consumerCtx); err != nil {
				logger.Error("process-path catalogue consumer stopped", "error", err)
			}
		}()

		waitCtx, cancelWait := context.WithTimeout(rootCtx, kafkacatalog.WaitReadyTimeout)
		logger.Info("waiting for the process-path catalogue to replay its initial history before accepting traffic")
		waitErr := kafkaCatalogue.WaitReady(waitCtx)
		cancelWait()
		if waitErr != nil {
			return nil, nil, fmt.Errorf("process-path catalogue did not become ready within %s: %w", kafkacatalog.WaitReadyTimeout, waitErr)
		}
		logger.Info("process-path catalogue is ready", "paths", kafkaCatalogue.Ids())
		return catalogue, kafkaCatalogue, nil
	default:
		catalogue, err := filecatalog.Load(getenv("PATH_CATALOGUE_FILE", "/etc/fulfillment-execution/process-paths.yaml"))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to load the process-path catalogue: %w", err)
		}
		logger.Info("process-path catalogue source configured", "source", "file", "paths", catalogue.Ids())
		return catalogue, nil, nil
	}
}

// storageAdapters groups the environment-selected persistence adapters.
// pool and uow are nil when running in-memory: the use cases then run
// Save and Publish back to back (see ports.UnitOfWork).
type storageAdapters struct {
	taskRepo          ports.TaskRepo
	stationRepo       ports.StationRepo
	packageRepo       ports.PackageRepo
	processedEvents   ports.ProcessedEvents
	consolidationRepo ports.OrderConsolidationRepo
	pool              *pgxpool.Pool
	uow               ports.UnitOfWork
}

// openStorage selects the in-memory or Postgres outbound adapters. The
// Postgres path runs the schema migrations, opens the pool, and verifies
// it with a ping, each under boot retry: this fleet's Istio native
// sidecars reset EVERY pod's first outbound TCP dial ~10s after the app
// starts (holdApplicationUntilProxyStarts is a no-op for native
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
// advisory lock never behaves as a real mutex. Losing replicas crash-loop
// with `pq: unnamed prepared statement does not exist` / `pq: canceling
// statement due to statement timeout` until one wins the race. See
// ADR-0031 (and order-management's ADR-0029, the reference
// implementation this mirrors) for the full incident and fix. Callers
// pass MIGRATIONS_DATABASE_URL when set (warehouse-infra provisions it as
// a direct, non-pooled DSN alongside DATABASE_URL for all 9 OLTP
// services, PR #44) or fall back to databaseURL itself for any
// environment that doesn't provision the split (local dev, CI
// integration tests) — byte-identical to this function's behavior before
// this parameter existed in that case.
func openStorage(rootCtx context.Context, logger *slog.Logger, databaseURL, migrationsDatabaseURL, migrationsPath string) (storageAdapters, error) {
	if databaseURL == "" {
		logger.Info("database url not configured; using in-memory adapters")
		return storageAdapters{
			taskRepo:          memory.NewTaskRepo(),
			stationRepo:       memory.NewStationRepo(),
			packageRepo:       memory.NewPackageRepo(),
			processedEvents:   memory.NewProcessedEventsRepo(),
			consolidationRepo: memory.NewOrderConsolidationRepo(),
		}, nil
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
func buildHandlers(storage storageAdapters, publisher ports.EventPublisher, clock memory.SystemClock, metrics *observability.Metrics, classificationLookup ports.ProductClassificationLookup, locationRoleLookup ports.LocationRoleLookup, createTask *usecases.CreateTask, readiness *inboundhttp.Readiness) *inboundhttp.Handlers {
	return &inboundhttp.Handlers{
		CreateTask:         createTask,
		ClaimNext:          &usecases.ClaimNext{Tasks: storage.taskRepo, Stations: storage.stationRepo, Publisher: publisher, Clock: clock, Metrics: metricsPort(metrics), UnitOfWork: storage.uow},
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
		// readiness backs GET /readyz (ADR-0029 §graceful shutdown):
		// flipped to not-ready as the FIRST step of shutdown, below,
		// before anything else stops.
		Readiness: readiness,
	}
}

// maybeWireDeadLetter wires the WorkReleased consumer's dead-letter writer
// (ADR-0004's documented gap): a message that fails processing is
// published to <topic>.dlq instead of only being logged. Wired the same
// way EVENT_PUBLISHER is — only when EVENT_PUBLISHER=kafka, since the DLQ
// writer needs the same brokers and there is no dead-letter concept for
// the log publisher's local/no-broker dev mode. AllowAutoTopicCreation
// mirrors every other writer in this codebase (outboundkafka.NewPublisher).
// It returns the writer for the caller to close, or nil when not wired.
func maybeWireDeadLetter(consumer *inboundkafka.Consumer, brokers []string, logger *slog.Logger) *kafkago.Writer {
	if getenv("EVENT_PUBLISHER", "log") != "kafka" {
		return nil
	}
	dlqWriter := &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Balancer:               &kafkago.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
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

// waitForShutdown blocks until SIGINT/SIGTERM, stops the Kafka consumers,
// and drains the HTTP server and the outbox relay, bounded by a 10s
// deadline.
func waitForShutdown(logger *slog.Logger, srv *http.Server, readiness *inboundhttp.Readiness, stopRelay context.CancelFunc, relayDone <-chan struct{}, cancelConsumer context.CancelFunc, consumerDone <-chan struct{}) error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

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
	//  3. Stop the outbox relay and the WorkReleased Kafka consumer's
	//     loop cleanly: cancel their contexts (no new message is
	//     fetched/handled after this) and wait, bounded by the SAME
	//     shutdownCtx, for their goroutines to actually finish
	//     in-flight work rather than merely asking them to stop and
	//     moving on.
	//  4. Only THEN do the deferred consumer.Close()/kafkaCatalogue.
	//     Close()/pool.Close() calls (registered earlier, so by defer's
	//     LIFO order they run AFTER every consumer/relay goroutine above
	//     has already stopped touching them, not before).
	readiness.SetNotReady()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := srv.Shutdown(ctx)

	// Let the relay finish its in-flight pass so an event committed by a
	// request that completed just before shutdown is not stranded until
	// the next pod boots; bounded by the same shutdown deadline.
	stopRelay()
	select {
	case <-relayDone:
	case <-ctx.Done():
		logger.Warn("outbox relay did not stop before the shutdown deadline")
	}

	// Stop the WorkReleased consumer's loop cleanly: cancel so no NEW
	// message is fetched, then wait (bounded) for any message already
	// being handled to finish before this function returns and the
	// deferred consumer.Close()/kafkaCatalogue.Close() calls run.
	cancelConsumer()
	select {
	case <-consumerDone:
	case <-ctx.Done():
		logger.Warn("kafka consumer did not stop before the shutdown deadline")
	}

	return err
}

// buildEventPublisher wires the outbound event publisher, returning it,
// the outbox relay to run alongside the HTTP server (nil when there is
// none), and a close function.
//
// The default is the log publisher, so a local dev run with no Kafka is
// still fully functional. With EVENT_PUBLISHER=kafka every domain event
// is fanned to BOTH the integration topic (TaskCompleted only, enriched)
// and the dedicated analytics topic (ADR-0012):
//
//   - with Postgres configured (pool != nil) the use cases publish into
//     the transactional outbox (ADR 0020): both publishers act only as
//     Encoders inside the use case's transaction, and the relay forwards
//     the stored rows to Kafka. The store and the topics can no longer
//     diverge.
//   - with in-memory adapters (pool == nil) events go straight to the
//     broker through MultiPublisher as before — there is no transaction to
//     bind them to.
func buildEventPublisher(pool *pgxpool.Pool, brokers []string, tasks ports.TaskRepo, stations ports.StationRepo, logger *slog.Logger) (ports.EventPublisher, *postgres.OutboxRelay, func()) {
	if getenv("EVENT_PUBLISHER", "log") != "kafka" {
		logger.Info("event publisher configured", "publisher", "log")
		return events.NewLogPublisher(logger), nil, func() {}
	}

	// EVENT_ENVELOPE_MODE selects the integration publisher's wire
	// envelope shape (ADR-0027 Phase 4): flat (default, today's
	// byte-identical envelope), cloudevents (CloudEvents 1.0 structured
	// mode), or dual (both, two physical messages per event). Logged once
	// here so a deployed pod's actual behavior is always visible in its
	// own startup log, regardless of which branch below constructs the
	// publisher.
	envelopeMode := outboundkafka.ParseEnvelopeMode(getenv("EVENT_ENVELOPE_MODE", ""))
	logger.Info("event envelope mode", "mode", string(envelopeMode))

	if pool == nil {
		kafkaPublisher := outboundkafka.NewPublisherWithMode(brokers, tasks, stations, uuidLike, envelopeMode)
		analyticsPub := outboundkafka.NewAnalyticsPublisher(brokers, tasks, uuidLike)
		logger.Info("event publisher configured", "publisher", "kafka", "mode", "direct",
			"topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic, "brokers", brokers)
		return events.NewMultiPublisher(kafkaPublisher, analyticsPub), nil, func() {
			_ = kafkaPublisher.Close()
			_ = analyticsPub.Close()
		}
	}

	// Encoders only: no writer is ever opened for them, the relay's
	// topic-less sink is the single Kafka connection this process holds
	// for publishing.
	integration := outboundkafka.NewPublisherWithWriterAndMode(nil, tasks, stations, uuidLike, envelopeMode)
	analytics := outboundkafka.NewAnalyticsPublisherWithWriter(nil, tasks, uuidLike)
	sink := outboundkafka.NewRelaySink(brokers)
	relay := postgres.NewOutboxRelay(pool, sink, logger,
		postgres.WithInterval(durationEnv("OUTBOX_RELAY_INTERVAL", time.Second)))
	logger.Info("event publisher configured", "publisher", "kafka", "mode", "outbox",
		"topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic, "brokers", brokers)
	return postgres.NewOutboxPublisher(pool, integration, analytics), relay, func() { _ = sink.Close() }
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

// buildClassificationLookup selects the outbound
// ports.ProductClassificationLookup adapter via PRODUCT_CLASSIFICATION_MODE
// (http|permissive), defaulting to "permissive" so existing tests, CI and
// deployments that do not set the env var are unaffected — mirrors
// inventory-storage's own LOCATION_LOOKUP_MODE=http|permissive pattern for
// its facilitylayout adapter (see ADR-0010). "http" requires
// INVENTORY_STORAGE_BASE_URL.
//
// In http mode the real Client is wrapped in retry (jittered, max 3
// attempts -- GetClassification is a pure read, safe to retry) plus a
// per-dependency circuit breaker (ADR-0029): on a trip, calls fall back
// to the SAME fail-open behaviour this client already had. recorder
// feeds the breaker's state transitions into the circuit_breaker.state
// gauge; nil is fine (see resilience.RecordStateChange's doc comment).
func buildClassificationLookup(mode, inventoryStorageBaseURL string, recorder resilience.StateRecorder, logger *slog.Logger) ports.ProductClassificationLookup {
	if !strings.EqualFold(mode, "http") {
		return productclassification.NewPermissiveLookup()
	}
	logger.Info("product classification lookup configured", "mode", "http", "inventory_storage_base_url", inventoryStorageBaseURL, "circuit_breaker", "enabled", "retry", "enabled")
	return productclassification.NewBreakerClient(productclassification.NewClient(inventoryStorageBaseURL, nil), recorder)
}

// buildLocationRoleLookup selects the outbound ports.LocationRoleLookup
// adapter via LOCATION_ROLE_MODE (http|permissive), defaulting to
// "permissive" so existing tests, CI and deployments that do not set the
// env var are unaffected — mirrors buildClassificationLookup's own
// PRODUCT_CLASSIFICATION_MODE pattern exactly (see ADR-0024). "http"
// requires FACILITY_LAYOUT_BASE_URL.
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
