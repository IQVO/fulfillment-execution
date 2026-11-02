package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/bootretry"
	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/composition"
)

// startRelay runs the outbox relay in the background and returns its stop
// func plus a channel closed once it has returned.
func startRelay(relay *postgres.OutboxRelay, logger *slog.Logger) (context.CancelFunc, <-chan struct{}) {
	relayDone := make(chan struct{})
	relayCtx, stopRelay := context.WithCancel(context.Background())
	go runRelay(relayCtx, relayDone, relay, logger)
	return stopRelay, relayDone
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

// runRelay drains the outbox onto both Kafka topics until ctx is
// cancelled, then closes done. Meant to be started with `go`.
func runRelay(ctx context.Context, done chan<- struct{}, relay *postgres.OutboxRelay, logger *slog.Logger) {
	defer close(done)
	logger.Info("outbox relay running", "topics", []string{outboundkafka.Topic, outboundkafka.AnalyticsTopic})
	if err := relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("outbox relay stopped", "error", err)
	}
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

// newDeadLetterWriter builds the WorkReleased consumer's DLQ writer through
// the shared outbound-kafka constructor, so it carries the same Hash balancer,
// RequireAll acks and 10ms BatchTimeout as every other writer.
func newDeadLetterWriter(brokers []string) *kafkago.Writer {
	return outboundkafka.NewDeadLetterWriter(brokers)
}
