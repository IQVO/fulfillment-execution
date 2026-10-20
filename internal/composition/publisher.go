// Package composition holds wiring helpers shared by the module's
// composition roots (cmd/execution and cmd/mcp). It exists so that every
// deployable that executes write use cases builds its ports.EventPublisher
// the SAME way — in particular so a TaskCompleted raised through the MCP
// adapter is stored in the transactional outbox exactly like one raised
// through REST (ADR-0008, ADR-0020).
//
// It reads NO environment: the roots resolve KAFKA_BROKERS,
// OUTBOX_RELAY_INTERVAL and OUTBOX_MAX_ATTEMPTS (and validate
// EVENT_PUBLISHER) and pass them in via PublisherConfig. Only the
// cmd/** roots may import this package.
package composition

import (
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// ErrNoPool and ErrNoBrokers are returned when the real runtime is not
// configured: there is no direct-to-Kafka path and no log sink to fall back
// to (the real-wiring ADR).
var (
	ErrNoPool    = errors.New("event publisher: a Postgres pool is required (DATABASE_URL): events leave only through the transactional outbox")
	ErrNoBrokers = errors.New("event publisher: Kafka brokers are required (KAFKA_BROKERS)")
)

// PublisherConfig is the already-resolved publisher configuration.
type PublisherConfig struct {
	// Brokers is the parsed KAFKA_BROKERS list.
	Brokers []string
	// RunRelay asks for the outbox relay to be built (cmd/execution). The
	// relay is deliberately NOT built by cmd/mcp: only one process drains
	// outbox_events, while every writer process inserts into it.
	RunRelay bool
	// RelayInterval is the relay poll interval (OUTBOX_RELAY_INTERVAL).
	RelayInterval time.Duration
	// RelayMaxAttempts is the relay's attempt cap (OUTBOX_MAX_ATTEMPTS,
	// default postgres.DefaultOutboxMaxAttempts, 0 = unlimited; ADR 0042): a
	// row that failed this many times is excluded from claims so one poison
	// row cannot stop the relay forever.
	RelayMaxAttempts int
}

// BuildEventPublisher wires the outbound event publisher, returning it, the
// outbox relay to run (nil when RunRelay is false) and a close function.
//
// Every domain event is fanned to BOTH the integration topic (TaskCompleted,
// TaskCPTMissed, PackageManifested, …) and the dedicated analytics topic
// (ADR-0012), always as CloudEvents 1.0 structured-mode messages (ADR-0032 —
// there is no envelope toggle). The CloudEvents id is a UUID v4
// (uuid.NewString). The use cases publish into the transactional outbox
// (ADR 0020): both publishers act only as Encoders inside the use case's
// transaction, and the relay forwards the stored rows to Kafka, so the store
// and the topics can no longer diverge. A nil pool or an empty broker list is
// an error: there is no in-memory, log-only or direct-to-Kafka mode.
func BuildEventPublisher(cfg PublisherConfig, pool *pgxpool.Pool, tasks ports.TaskRepo, stations ports.StationRepo, logger *slog.Logger) (ports.EventPublisher, *postgres.OutboxRelay, func(), error) {
	if pool == nil {
		return nil, nil, func() {}, ErrNoPool
	}
	if len(cfg.Brokers) == 0 {
		return nil, nil, func() {}, ErrNoBrokers
	}

	// Encoders only: no writer is ever opened for them.
	integration := outboundkafka.NewPublisherWithWriter(nil, tasks, stations, uuid.NewString)
	analytics := outboundkafka.NewAnalyticsPublisherWithWriter(nil, tasks, uuid.NewString)
	outbox := postgres.NewOutboxPublisher(pool, integration, analytics)

	if !cfg.RunRelay {
		logger.Info("event publisher configured", "publisher", "kafka", "mode", "outbox-writer-only",
			"topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic)
		return outbox, nil, func() {}, nil
	}

	// The relay's topic-less sink is the single Kafka connection this
	// process holds for publishing.
	sink := outboundkafka.NewRelaySink(cfg.Brokers)
	relay := postgres.NewOutboxRelay(pool, sink, logger,
		postgres.WithInterval(cfg.RelayInterval),
		postgres.WithMaxAttempts(cfg.RelayMaxAttempts))
	logger.Info("event publisher configured", "publisher", "kafka", "mode", "outbox",
		"topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic, "brokers", cfg.Brokers,
		"outbox_max_attempts", cfg.RelayMaxAttempts)
	return outbox, relay, func() { _ = sink.Close() }, nil
}
