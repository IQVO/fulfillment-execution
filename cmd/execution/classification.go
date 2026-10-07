package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

// PRODUCT_CLASSIFICATION_MODE values (ADR-0039). "http" (the live lookup to
// inventory-storage, ADR-0010) was removed and is rejected at boot, so a
// stale deployment fails loudly instead of silently running permissive.
const (
	classificationModeKafka      = "kafka"
	classificationModePermissive = "permissive"
	classificationModeRemoved    = "http"
)

// classificationConfig is the validated product-classification env config.
type classificationConfig struct {
	mode          string
	consumerGroup string
}

// errClassificationModeHTTPRemoved is the boot error for the retired mode.
var errClassificationModeHTTPRemoved = errors.New(
	"PRODUCT_CLASSIFICATION_MODE=http was removed (ADR-0039): classifications now come from a local copy of " +
		"product-master's ProductClassified events; set PRODUCT_CLASSIFICATION_MODE=kafka together with " +
		"PRODUCT_CLASSIFICATION_CONSUMER_GROUP, or permissive")

// classificationConfigFromEnv reads and validates
// PRODUCT_CLASSIFICATION_MODE and PRODUCT_CLASSIFICATION_CONSUMER_GROUP.
// run() calls it first, before anything dials: an unknown or removed mode
// (including the retired "http") or kafka mode without a consumer group
// stops the boot loudly.
func classificationConfigFromEnv() (classificationConfig, error) {
	return parseClassificationConfig(
		getenv("PRODUCT_CLASSIFICATION_MODE", classificationModePermissive),
		os.Getenv("PRODUCT_CLASSIFICATION_CONSUMER_GROUP"),
	)
}

// parseClassificationConfig validates PRODUCT_CLASSIFICATION_MODE (empty =
// permissive, case-insensitive) and PRODUCT_CLASSIFICATION_CONSUMER_GROUP.
// Every invalid combination is a boot error, checked before anything dials.
func parseClassificationConfig(mode, consumerGroup string) (classificationConfig, error) {
	m := strings.ToLower(strings.TrimSpace(mode))
	if m == "" {
		m = classificationModePermissive
	}
	switch m {
	case classificationModePermissive:
		return classificationConfig{mode: m}, nil
	case classificationModeKafka:
		group := strings.TrimSpace(consumerGroup)
		if group == "" {
			return classificationConfig{}, errors.New(
				"PRODUCT_CLASSIFICATION_MODE=kafka requires PRODUCT_CLASSIFICATION_CONSUMER_GROUP (a stable consumer group id for the warehouse.product-master.events consumer)")
		}
		return classificationConfig{mode: m, consumerGroup: group}, nil
	case classificationModeRemoved:
		return classificationConfig{}, errClassificationModeHTTPRemoved
	default:
		return classificationConfig{}, fmt.Errorf("unknown PRODUCT_CLASSIFICATION_MODE %q: want kafka or permissive", mode)
	}
}

// classificationWiring is what the composition root needs from the
// product-classification choice: the lookup SealPackage reads and, in kafka
// mode, the ProductClassified consumer that feeds the copy plus the func
// that closes its reader.
type classificationWiring struct {
	lookup   ports.ProductClassificationLookup
	apply    *usecases.ApplyProductClassified
	consumer *inboundkafka.ProductClassifiedConsumer
	close    func()
}

// buildClassification wires the adapters for cfg. kafka mode reads and
// writes the Postgres copy (product_classification_copy) when a database is
// configured, or an in-memory copy otherwise, dedupes on the SAME
// processed_events store and UnitOfWork the WorkReleased consumer uses, and
// builds (but does not start) the warehouse.product-master.events consumer.
func buildClassification(cfg classificationConfig, storage storageAdapters, brokers []string, logger *slog.Logger) (classificationWiring, error) {
	if cfg.mode != classificationModeKafka {
		logger.Info("product classification lookup configured", "mode", classificationModePermissive)
		return classificationWiring{lookup: productclassificationcopy.NewPermissiveLookup(), close: func() {}}, nil
	}
	var (
		lookup ports.ProductClassificationLookup
		store  ports.ProductClassificationCopy
	)
	if storage.pool != nil {
		pg := productclassificationcopy.NewPostgresStore(storage.pool)
		lookup, store = pg, pg
	} else {
		mem := productclassificationcopy.NewMemoryStore()
		lookup, store = mem, mem
		logger.Warn("product classification copy is in memory (DATABASE_URL unset): it is rebuilt from the topic only for a new consumer group")
	}
	apply := &usecases.ApplyProductClassified{
		Processed:  storage.processedEvents,
		Copy:       store,
		UnitOfWork: storage.uow,
	}
	consumer, err := inboundkafka.NewProductClassifiedConsumer(brokers, inboundkafka.ProductMasterTopic, cfg.consumerGroup, apply, logger)
	if err != nil {
		return classificationWiring{}, err
	}
	logger.Info("product classification lookup configured", "mode", classificationModeKafka,
		"topic", inboundkafka.ProductMasterTopic, "consumer_group", cfg.consumerGroup)
	return classificationWiring{
		lookup:   lookup,
		apply:    apply,
		consumer: consumer,
		close:    func() { _ = consumer.Close() },
	}, nil
}

// startProductClassifiedConsumer runs the consumer loop in the background
// and returns a channel closed once it has returned (immediately closed
// when there is no consumer), so graceful shutdown waits for a real stop.
func startProductClassifiedConsumer(consumer *inboundkafka.ProductClassifiedConsumer, ctx context.Context, logger *slog.Logger) <-chan struct{} {
	done := make(chan struct{})
	if consumer == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		logger.Info("product classification consumer starting", "topic", inboundkafka.ProductMasterTopic)
		if err := consumer.Run(ctx); err != nil {
			logger.Error("product classification consumer stopped", "error", err)
		}
	}()
	return done
}

// joinDone returns a channel closed once every input channel is closed.
func joinDone(chans ...<-chan struct{}) <-chan struct{} {
	all := make(chan struct{})
	go func() {
		defer close(all)
		for _, c := range chans {
			<-c
		}
	}()
	return all
}
