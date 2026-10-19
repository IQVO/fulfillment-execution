package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/interleavinghintcopy"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

// TASK_INTERLEAVING_MODE values (ADR 0051, then the real-wiring ADR). The
// consumer always runs: "kafka" (the default) is the only accepted value;
// "permissive" and "off" (no consumer, no hints) are rejected at boot.
const interleavingModeKafka = "kafka"

// interleavingConfig is the validated task-interleaving env config.
type interleavingConfig struct {
	consumerGroup string
}

// parseInterleavingConfig validates TASK_INTERLEAVING_MODE (empty = kafka,
// case-insensitive) and TASK_INTERLEAVING_CONSUMER_GROUP. Every invalid
// combination is a boot error naming the variable, checked before anything
// dials.
func parseInterleavingConfig(mode, consumerGroup string) (interleavingConfig, error) {
	m := strings.ToLower(strings.TrimSpace(mode))
	if m == "permissive" || m == "off" {
		return interleavingConfig{}, fmt.Errorf("TASK_INTERLEAVING_MODE=%s is not supported: this context runs on real Kafka", m)
	}
	if m != "" && m != interleavingModeKafka {
		return interleavingConfig{}, fmt.Errorf("unknown TASK_INTERLEAVING_MODE %q: want kafka", mode)
	}
	group := strings.TrimSpace(consumerGroup)
	if group == "" {
		return interleavingConfig{}, errors.New(
			"TASK_INTERLEAVING_CONSUMER_GROUP is required: the task-interleaving consumer always runs on real Kafka (a stable consumer group id for the warehouse.task-interleaving.events consumer)")
	}
	return interleavingConfig{consumerGroup: group}, nil
}

// interleavingWiring is what the composition root needs from the
// task-interleaving choice: the lookup claimNext reads and the
// InterleavingHintIssued consumer that feeds the copy plus the func that
// closes its reader.
type interleavingWiring struct {
	lookup   ports.InterleavingHintLookup
	consumer *inboundkafka.InterleavingHintConsumer
	close    func()
}

// buildInterleaving wires the adapters for cfg: it reads and writes the
// Postgres copy (interleaving_hints), dedupes on the SAME processed_events
// store and UnitOfWork the other consumers use, and builds (but does not
// start) the warehouse.task-interleaving.events consumer. A storage without
// a Postgres pool is an error: there is no in-memory copy. The hints stay
// advisory and fail-open at claim time (ADR 0051): an empty copy behaves as
// without hints.
func buildInterleaving(cfg interleavingConfig, storage storageAdapters, brokers []string, logger *slog.Logger) (interleavingWiring, error) {
	if storage.pool == nil {
		return interleavingWiring{}, errNoDatabase
	}
	store := interleavinghintcopy.NewPostgresStore(storage.pool)
	apply := &usecases.ApplyInterleavingHint{
		Processed:  storage.processedEvents,
		Hints:      store,
		UnitOfWork: storage.uow,
	}
	consumer, err := inboundkafka.NewInterleavingHintConsumer(brokers, inboundkafka.TaskInterleavingTopic, cfg.consumerGroup, apply, logger)
	if err != nil {
		return interleavingWiring{}, err
	}
	logger.Info("interleaving hints configured", "mode", interleavingModeKafka,
		"topic", inboundkafka.TaskInterleavingTopic, "consumer_group", cfg.consumerGroup)
	return interleavingWiring{
		lookup:   store,
		consumer: consumer,
		close:    func() { _ = consumer.Close() },
	}, nil
}

// startInterleavingHintConsumer runs the consumer loop in the background
// and returns a channel closed once it has returned (immediately closed
// when there is no consumer), so graceful shutdown waits for a real stop.
func startInterleavingHintConsumer(consumer *inboundkafka.InterleavingHintConsumer, ctx context.Context, logger *slog.Logger) <-chan struct{} {
	done := make(chan struct{})
	if consumer == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		logger.Info("interleaving hint consumer starting", "topic", inboundkafka.TaskInterleavingTopic)
		if err := consumer.Run(ctx); err != nil {
			logger.Error("interleaving hint consumer stopped", "error", err)
		}
	}()
	return done
}
