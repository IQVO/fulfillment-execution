package main

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/packview"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

// errNoPackViewGroup is the boot error of PACK_VIEW_CONSUMER_GROUP (ADR 0054):
// the pack-level consumer always runs on real Kafka under its OWN stable group
// (a second consumer of warehouse.product-master.events next to the product
// classification one, which must not share its group).
var errNoPackViewGroup = errors.New(
	"PACK_VIEW_CONSUMER_GROUP is required: the pack-level consumer always runs on real Kafka (a stable consumer group id for the warehouse.product-master.events consumer feeding the local PackView, distinct from PRODUCT_CLASSIFICATION_CONSUMER_GROUP)")

// errNoInventoryStorage is the boot error of INVENTORY_STORAGE_BASE_URL
// (ADR 0054): completing a BREAK_CASE task calls inventory-storage, so there
// is no degraded mode without it.
var errNoInventoryStorage = errors.New(
	"INVENTORY_STORAGE_BASE_URL is required: completing a BREAK_CASE task calls inventory-storage POST /containers/{lpn}:break")

// parsePackViewGroup validates PACK_VIEW_CONSUMER_GROUP, and rejects the
// product-classification group: two consumers on one topic need two groups.
func parsePackViewGroup(consumerGroup, classificationGroup string) (string, error) {
	group := strings.TrimSpace(consumerGroup)
	if group == "" {
		return "", errNoPackViewGroup
	}
	if group == strings.TrimSpace(classificationGroup) {
		return "", errors.New("PACK_VIEW_CONSUMER_GROUP must differ from PRODUCT_CLASSIFICATION_CONSUMER_GROUP: both read warehouse.product-master.events and a shared group would split its partitions between them")
	}
	return group, nil
}

// packViewWiring is what the composition root needs from the PackView: the
// lookup ConfirmPickScan reads, the pack-level consumer that feeds it and the
// func that closes its reader.
type packViewWiring struct {
	lookup   ports.PackViewLookup
	consumer *inboundkafka.PackLevelConsumer
	close    func()
}

// buildPackView wires the Postgres PackView (migration 0022), dedupes on the
// SAME processed_events store and UnitOfWork as the other consumers and
// builds (but does not start) the pack-level consumer. A storage without a
// Postgres pool is an error: there is no in-memory PackView at runtime.
func buildPackView(group string, storage storageAdapters, brokers []string, logger *slog.Logger) (packViewWiring, error) {
	if storage.pool == nil {
		return packViewWiring{}, errNoDatabase
	}
	store := packview.NewPostgresStore(storage.pool)
	apply := &usecases.ApplyPackLevel{Processed: storage.processedEvents, View: store, UnitOfWork: storage.uow}
	consumer, err := inboundkafka.NewPackLevelConsumer(brokers, inboundkafka.ProductMasterTopic, group, apply, logger)
	if err != nil {
		return packViewWiring{}, err
	}
	logger.Info("pack view configured", "topic", inboundkafka.ProductMasterTopic, "consumer_group", group)
	return packViewWiring{lookup: store, consumer: consumer, close: func() { _ = consumer.Close() }}, nil
}

// startPackLevelConsumer runs the consumer loop in the background and returns
// a channel closed once it has returned, so graceful shutdown waits for a
// real stop.
func startPackLevelConsumer(consumer *inboundkafka.PackLevelConsumer, ctx context.Context, logger *slog.Logger) <-chan struct{} {
	done := make(chan struct{})
	if consumer == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		logger.Info("pack level consumer starting", "topic", inboundkafka.ProductMasterTopic)
		if err := consumer.Run(ctx); err != nil {
			logger.Error("pack level consumer stopped", "error", err)
		}
	}()
	return done
}

// lookupConsumers are the done channels of the three local-copy consumers
// (product classification, interleaving hints, pack levels).
type lookupConsumers struct {
	classification, interleaving, packView <-chan struct{}
}

// startLookupConsumers starts the three local-copy consumers in the
// background; each channel closes once its loop has returned.
func startLookupConsumers(ctx context.Context, logger *slog.Logger, c classificationWiring, i interleavingWiring, p packViewWiring) lookupConsumers {
	return lookupConsumers{
		classification: startProductClassifiedConsumer(c.consumer, ctx, logger),
		interleaving:   startInterleavingHintConsumer(i.consumer, ctx, logger),
		packView:       startPackLevelConsumer(p.consumer, ctx, logger),
	}
}
