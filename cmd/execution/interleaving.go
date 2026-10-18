package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/interleavinghintcopy"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

// TASK_INTERLEAVING_MODE values (ADR 0051).
const (
	interleavingModeKafka      = "kafka"
	interleavingModePermissive = "permissive"
)

// interleavingConfig is the validated task-interleaving env config.
type interleavingConfig struct {
	mode          string
	consumerGroup string
}

// interleavingConfigFromEnv reads and validates TASK_INTERLEAVING_MODE and
// TASK_INTERLEAVING_CONSUMER_GROUP. run() calls it first, before anything
// dials: an unknown mode or kafka mode without a consumer group stops the
// boot loudly.
func interleavingConfigFromEnv() (interleavingConfig, error) {
	return parseInterleavingConfig(
		getenv("TASK_INTERLEAVING_MODE", interleavingModePermissive),
		os.Getenv("TASK_INTERLEAVING_CONSUMER_GROUP"),
	)
}

// parseInterleavingConfig validates TASK_INTERLEAVING_MODE (empty =
// permissive, case-insensitive) and TASK_INTERLEAVING_CONSUMER_GROUP. Every
// invalid combination is a boot error, checked before anything dials.
func parseInterleavingConfig(mode, consumerGroup string) (interleavingConfig, error) {
	m := strings.ToLower(strings.TrimSpace(mode))
	if m == "" {
		m = interleavingModePermissive
	}
	switch m {
	case interleavingModePermissive:
		return interleavingConfig{mode: m}, nil
	case interleavingModeKafka:
		group := strings.TrimSpace(consumerGroup)
		if group == "" {
			return interleavingConfig{}, errors.New(
				"TASK_INTERLEAVING_MODE=kafka requires TASK_INTERLEAVING_CONSUMER_GROUP (a stable consumer group id for the warehouse.task-interleaving.events consumer)")
		}
		return interleavingConfig{mode: m, consumerGroup: group}, nil
	default:
		return interleavingConfig{}, fmt.Errorf("unknown TASK_INTERLEAVING_MODE %q: want kafka or permissive", mode)
	}
}

// interleavingWiring is what the composition root needs from the
// task-interleaving choice: the lookup claimNext reads (nil in permissive
// mode — claimNext then behaves exactly as before ADR 0051) and, in kafka
// mode, the InterleavingHintIssued consumer that feeds the copy plus the
// func that closes its reader.
type interleavingWiring struct {
	lookup   ports.InterleavingHintLookup
	consumer *inboundkafka.InterleavingHintConsumer
	close    func()
}

// setupInterleaving validates the env config and builds the wiring in one
// step, keeping run()'s body flat.
func setupInterleaving(storage storageAdapters, brokers []string, logger *slog.Logger) (interleavingWiring, error) {
	cfg, err := interleavingConfigFromEnv()
	if err != nil {
		return interleavingWiring{}, err
	}
	return buildInterleaving(cfg, storage, brokers, logger)
}

// buildInterleaving wires the adapters for cfg. kafka mode reads and writes
// the Postgres copy (interleaving_hints) when a database is configured, or
// an in-memory copy otherwise, dedupes on the SAME processed_events store
// and UnitOfWork the other consumers use, and builds (but does not start)
// the warehouse.task-interleaving.events consumer.
func buildInterleaving(cfg interleavingConfig, storage storageAdapters, brokers []string, logger *slog.Logger) (interleavingWiring, error) {
	if cfg.mode != interleavingModeKafka {
		logger.Info("interleaving hints configured", "mode", interleavingModePermissive)
		return interleavingWiring{close: func() {}}, nil
	}
	var store interface {
		ports.InterleavingHintStore
		ports.InterleavingHintLookup
	}
	if storage.pool != nil {
		store = interleavinghintcopy.NewPostgresStore(storage.pool)
	} else {
		store = interleavinghintcopy.NewMemoryStore()
		logger.Warn("interleaving hint copy is in memory (DATABASE_URL unset): hints are advisory, so a restart simply behaves as without hints")
	}
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
