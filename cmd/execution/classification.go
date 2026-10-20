package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

// PRODUCT_CLASSIFICATION_MODE values (ADR-0039, then the real-wiring ADR).
// The consumer always runs: "kafka" (the default) is the only accepted
// value. "http" (the live lookup to inventory-storage, ADR-0010) was removed
// earlier; "permissive" and "off" (no consumer, no lookup) are removed now.
// Both are rejected at boot, so a stale deployment fails loudly instead of
// silently running without product classifications.
const (
	classificationModeKafka   = "kafka"
	classificationModeRemoved = "http"
)

// classificationConfig is the validated product-classification env config.
type classificationConfig struct {
	consumerGroup string
}

// errClassificationModeHTTPRemoved is the boot error for the retired mode.
var errClassificationModeHTTPRemoved = errors.New(
	"PRODUCT_CLASSIFICATION_MODE=http was removed (ADR-0039): classifications come from a local copy of " +
		"product-master's ProductClassified events; leave PRODUCT_CLASSIFICATION_MODE unset (or kafka) and set " +
		"PRODUCT_CLASSIFICATION_CONSUMER_GROUP")

// parseClassificationConfig validates PRODUCT_CLASSIFICATION_MODE (empty =
// kafka, case-insensitive) and PRODUCT_CLASSIFICATION_CONSUMER_GROUP. Every
// invalid combination is a boot error naming the variable, checked before
// anything dials.
func parseClassificationConfig(mode, consumerGroup string) (classificationConfig, error) {
	m := strings.ToLower(strings.TrimSpace(mode))
	if m == classificationModeRemoved {
		return classificationConfig{}, errClassificationModeHTTPRemoved
	}
	if m == "permissive" || m == "off" {
		return classificationConfig{}, fmt.Errorf("PRODUCT_CLASSIFICATION_MODE=%s is not supported: this context runs on real Kafka", m)
	}
	if m != "" && m != classificationModeKafka {
		return classificationConfig{}, fmt.Errorf("unknown PRODUCT_CLASSIFICATION_MODE %q: want kafka", mode)
	}
	group := strings.TrimSpace(consumerGroup)
	if group == "" {
		return classificationConfig{}, errors.New(
			"PRODUCT_CLASSIFICATION_CONSUMER_GROUP is required: the product-classification consumer always runs on real Kafka (a stable consumer group id for the warehouse.product-master.events consumer)")
	}
	return classificationConfig{consumerGroup: group}, nil
}

// classificationWiring is what the composition root needs from the
// product-classification choice: the lookup SealPackage reads, the
// ProductClassified consumer that feeds the copy and the func that closes
// its reader.
type classificationWiring struct {
	lookup   ports.ProductClassificationLookup
	apply    *usecases.ApplyProductClassified
	consumer *inboundkafka.ProductClassifiedConsumer
	close    func()
}

// buildClassification wires the adapters for cfg: it reads and writes the
// Postgres copy (product_classification_copy), dedupes on the SAME
// processed_events store and UnitOfWork the WorkReleased consumer uses, and
// builds (but does not start) the warehouse.product-master.events consumer.
// A storage without a Postgres pool is an error: there is no in-memory copy.
func buildClassification(cfg classificationConfig, storage storageAdapters, brokers []string, logger *slog.Logger) (classificationWiring, error) {
	if storage.pool == nil {
		return classificationWiring{}, errNoDatabase
	}
	pg := productclassificationcopy.NewPostgresStore(storage.pool)
	apply := &usecases.ApplyProductClassified{
		Processed:  storage.processedEvents,
		Copy:       pg,
		UnitOfWork: storage.uow,
	}
	consumer, err := inboundkafka.NewProductClassifiedConsumer(brokers, inboundkafka.ProductMasterTopic, cfg.consumerGroup, apply, logger)
	if err != nil {
		return classificationWiring{}, err
	}
	logger.Info("product classification lookup configured", "mode", classificationModeKafka,
		"topic", inboundkafka.ProductMasterTopic, "consumer_group", cfg.consumerGroup)
	return classificationWiring{
		lookup:   pg,
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
