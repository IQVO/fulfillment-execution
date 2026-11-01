package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/bootretry"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/filecatalog"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafkacatalog"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// closeCatalogue closes the Kafka process-path catalogue when one is wired.
func closeCatalogue(c *kafkacatalog.Consumer) {
	if c != nil {
		_ = c.Close()
	}
}

// loadedCatalogue is the resolved process-path catalogue source: the
// catalogue itself, and (Kafka source only) its consumer, the replay wait
// and a channel closed when the consumer loop returned.
type loadedCatalogue struct {
	catalogue ports.PathCatalogue
	kafka     *kafkacatalog.Consumer
	wait      func(context.Context) error
	done      <-chan struct{}
}

// loadCatalogue resolves the process-path catalogue from its switchable
// SOURCE (PATH_CATALOGUE_SOURCE=file|kafka, defaulting to "file" so
// nothing about existing deployments changes unless explicitly opted in).
// The catalogue source is configuration, not a database or a broker
// stand-in: the real-wiring ADR leaves it as it was (warehouse-infra sets
// kafka; "file" is the documented rollback switch).
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
// readiness gate rather than a boot-time file read). The consumer loop
// is started here on consumerCtx, and the returned wait blocks
// until the replay reaches the startup watermark; the caller runs it
// only after the HTTP listener is up, with /readyz false meanwhile
// (gap fix D24, ADR-0044). wait is nil in file mode.
func loadCatalogue(rootCtx, consumerCtx context.Context, brokers []string, logger *slog.Logger) (loadedCatalogue, error) {
	if getenv("PATH_CATALOGUE_SOURCE", "file") != "kafka" {
		catalogue, err := filecatalog.Load(getenv("PATH_CATALOGUE_FILE", "/etc/fulfillment-execution/process-paths.yaml"))
		if err != nil {
			return loadedCatalogue{}, fmt.Errorf("failed to load the process-path catalogue: %w", err)
		}
		logger.Info("process-path catalogue source configured", "source", "file", "paths", catalogue.Ids())
		return loadedCatalogue{catalogue: catalogue}, nil
	}

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
		return loadedCatalogue{}, fmt.Errorf("failed to start the process-path Kafka catalogue: %w", err)
	}
	logger.Info("process-path catalogue source configured", "source", "kafka", "topic", kafkacatalog.Topic)

	done := make(chan struct{})
	go func() {
		defer close(done)
		logger.Info("process-path catalogue consumer running", "topic", kafkacatalog.Topic)
		if err := kafkaCatalogue.Run(consumerCtx); err != nil {
			logger.Error("process-path catalogue consumer stopped", "error", err)
		}
	}()
	return loadedCatalogue{catalogue: kafkaCatalogue, kafka: kafkaCatalogue, wait: catalogueWaiter(kafkaCatalogue, logger), done: done}, nil
}

// catalogueWaiter returns the replay wait for the Kafka catalogue: it
// blocks until every partition has reached the offset captured at startup.
func catalogueWaiter(c *kafkacatalog.Consumer, logger *slog.Logger) func(context.Context) error {
	return func(ctx context.Context) error {
		logger.Info("http listener up; /readyz stays not ready until the process-path catalogue has replayed its initial history")
		if err := c.WaitReady(ctx); err != nil {
			return err
		}
		logger.Info("process-path catalogue is ready", "paths", c.Ids())
		return nil
	}
}
