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
	"syscall"
	"time"

	inboundhttp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafkacatalog"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/system"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/observability"
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
	srv := newHTTPServer(storage, publisher, clock, metrics, lookups, interleaving.lookup, createTask, readiness, logger, getenv("HTTP_ADDR", ":8080"), buildWorkPlanningClient(logger, os.Getenv("WORK_PLANNING_A2A_URL")))

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

// newHTTPServer builds the handler set and the HTTP server around it, with
// the idempotency pool bound (the real runtime always has one; only the unit
// tests build a server over test doubles without it).
func newHTTPServer(storage storageAdapters, publisher ports.EventPublisher, clock system.Clock, metrics *observability.Metrics, lookups outboundLookups, hintLookup ports.InterleavingHintLookup, createTask *usecases.CreateTask, readiness *inboundhttp.Readiness, logger *slog.Logger, httpAddr string, planning ports.WorkPlanning) *http.Server {
	handlers := buildHandlers(storage, publisher, clock, metrics, lookups.classification, lookups.locationRole, hintLookup, createTask, readiness, logger, planning)
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

// serveHTTP runs the HTTP server, logging a failed ListenAndServe unless
// it is the expected ErrServerClosed of shutdown. Meant to be started
// with `go`.
func serveHTTP(srv *http.Server, logger *slog.Logger, addr string) {
	logger.Info("http server listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("http server failed", "error", err)
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
