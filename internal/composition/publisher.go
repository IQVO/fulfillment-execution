// Package composition holds wiring helpers shared by the module's
// composition roots (cmd/execution and cmd/mcp). It exists so that every
// deployable that executes write use cases builds its ports.EventPublisher
// the SAME way — in particular so a TaskCompleted raised through the MCP
// adapter is stored in the transactional outbox exactly like one raised
// through REST (ADR-0008, ADR-0020).
//
// It reads NO environment: the roots resolve EVENT_PUBLISHER, KAFKA_BROKERS
// and OUTBOX_RELAY_INTERVAL and pass them in via PublisherConfig. Only the
// cmd/** roots may import this package.
package composition

import (
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// KafkaPublisher is the EVENT_PUBLISHER value that selects Kafka.
const KafkaPublisher = "kafka"

// PublisherConfig is the already-resolved publisher configuration.
type PublisherConfig struct {
	// Kind is the EVENT_PUBLISHER value; anything but "kafka" selects the
	// log publisher.
	Kind string
	// Brokers is the parsed KAFKA_BROKERS list.
	Brokers []string
	// RunRelay asks for the outbox relay to be built (cmd/execution). The
	// relay is deliberately NOT built by cmd/mcp: only one process drains
	// outbox_events, while every writer process inserts into it.
	RunRelay bool
	// RelayInterval is the relay poll interval (OUTBOX_RELAY_INTERVAL).
	RelayInterval time.Duration
}

// BuildEventPublisher wires the outbound event publisher, returning it, the
// outbox relay to run (nil when none was requested or applicable) and a
// close function.
//
// The default is the log publisher, so a local dev run with no Kafka is
// still fully functional. With Kind "kafka" every domain event is fanned to
// BOTH the integration topic (TaskCompleted, TaskCPTMissed,
// PackageManifested) and the dedicated analytics topic (ADR-0012), always
// as CloudEvents 1.0 structured-mode messages (ADR-0032 — there is no
// envelope toggle). The CloudEvents id is a UUID v4 (uuid.NewString):
//
//   - with Postgres configured (pool != nil) the use cases publish into
//     the transactional outbox (ADR 0020): both publishers act only as
//     Encoders inside the use case's transaction, and the relay forwards
//     the stored rows to Kafka. The store and the topics can no longer
//     diverge.
//   - with in-memory adapters (pool == nil) events go straight to the
//     broker through MultiPublisher as before — there is no transaction to
//     bind them to.
func BuildEventPublisher(cfg PublisherConfig, pool *pgxpool.Pool, tasks ports.TaskRepo, stations ports.StationRepo, logger *slog.Logger) (ports.EventPublisher, *postgres.OutboxRelay, func()) {
	if cfg.Kind != KafkaPublisher {
		logger.Info("event publisher configured", "publisher", "log")
		return events.NewLogPublisher(logger), nil, func() {}
	}

	if pool == nil {
		kafkaPublisher := outboundkafka.NewPublisher(cfg.Brokers, tasks, stations, uuid.NewString)
		analyticsPub := outboundkafka.NewAnalyticsPublisher(cfg.Brokers, tasks, uuid.NewString)
		logger.Info("event publisher configured", "publisher", "kafka", "mode", "direct",
			"topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic, "brokers", cfg.Brokers)
		return events.NewMultiPublisher(kafkaPublisher, analyticsPub), nil, func() {
			_ = kafkaPublisher.Close()
			_ = analyticsPub.Close()
		}
	}

	// Encoders only: no writer is ever opened for them.
	integration := outboundkafka.NewPublisherWithWriter(nil, tasks, stations, uuid.NewString)
	analytics := outboundkafka.NewAnalyticsPublisherWithWriter(nil, tasks, uuid.NewString)
	outbox := postgres.NewOutboxPublisher(pool, integration, analytics)

	if !cfg.RunRelay {
		logger.Info("event publisher configured", "publisher", "kafka", "mode", "outbox-writer-only",
			"topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic)
		return outbox, nil, func() {}
	}

	// The relay's topic-less sink is the single Kafka connection this
	// process holds for publishing.
	sink := outboundkafka.NewRelaySink(cfg.Brokers)
	relay := postgres.NewOutboxRelay(pool, sink, logger, postgres.WithInterval(cfg.RelayInterval))
	logger.Info("event publisher configured", "publisher", "kafka", "mode", "outbox",
		"topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic, "brokers", cfg.Brokers)
	return outbox, relay, func() { _ = sink.Close() }
}
