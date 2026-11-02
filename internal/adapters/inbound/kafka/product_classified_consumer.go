package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

// ProductMasterTopic is product-master's integration topic (its AsyncAPI,
// channel warehouse.product-master.events). Key and subject = the SKU.
const ProductMasterTopic = "warehouse.product-master.events"

// TypeProductClassified is the FULL CloudEvents `type` product-master
// publishes ProductClassified under (product-master ADR 0004). The consumer
// dispatches on this exact string; every other type on the topic
// (ProductRegistered, ProductDescriptionChanged, ProductDimensionsDeclared,
// ProductMeasured, anything added later) is ignored and committed past.
const TypeProductClassified = "com.warehouse.wms.product-master.product.ProductClassified"

// Retry bounds for a transient apply failure. The SAME message is retried
// until it succeeds or the consumer is stopped; its offset is never
// committed on failure.
const (
	classifiedRetryInitialInterval = 200 * time.Millisecond
	classifiedRetryMaxInterval     = 5 * time.Second
)

// ErrInvalidProductClassified marks a valid CloudEvent of the
// ProductClassified type whose payload breaks the contract (missing sku,
// version < 1, dot_hazard_class outside 1-9, undecodable data). It is
// deterministic: the consumer WARN-logs it and commits past, never retries.
var ErrInvalidProductClassified = errors.New("kafka: invalid ProductClassified payload")

// ProductClassifiedData is the payload of product-master's ProductClassified
// event (full-state replacement of one SKU's classification). Optional
// fields are omitted by the producer when unset.
type ProductClassifiedData struct {
	SKU                  string   `json:"sku"`
	HandlingTags         []string `json:"handling_tags"`
	TemperatureClass     string   `json:"temperature_class,omitempty"`
	DOTHazardClass       *int     `json:"dot_hazard_class,omitempty"`
	ClassificationSource string   `json:"classification_source,omitempty"`
	Version              int64    `json:"version"`
}

// ProductClassifiedConsumer keeps the local classification copy (ADR-0039)
// current from product-master's ProductClassified events.
//
// Delivery contract: FetchMessage, then the CloudEvents-id claim and the
// version-guarded upsert in ONE transaction (usecases.ApplyProductClassified),
// then CommitMessages. A transient failure is retried on the same message
// with capped backoff; a message that is not a valid CloudEvent, or a
// ProductClassified with an invalid payload, is WARN-logged and committed
// past; any other type is ignored and committed past.
type ProductClassifiedConsumer struct {
	reader fetchCommitReader
	closer interface{ Close() error }
	topic  string
	apply  *usecases.ApplyProductClassified
	logger *slog.Logger

	retryInitial time.Duration
	retryMax     time.Duration
}

// NewProductClassifiedConsumer builds the consumer over a kafka-go reader in
// consumer group groupID (from PRODUCT_CLASSIFICATION_CONSUMER_GROUP; never a
// literal). A brand-new group starts at the EARLIEST offset: the copy must
// see the whole classification history, not only what is published after
// the first deploy.
func NewProductClassifiedConsumer(brokers []string, topic, groupID string, apply *usecases.ApplyProductClassified, logger *slog.Logger) (*ProductClassifiedConsumer, error) {
	if groupID == "" {
		return nil, errors.New("kafka: ProductClassified consumer needs a consumer group (PRODUCT_CLASSIFICATION_CONSUMER_GROUP)")
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafkago.FirstOffset,
		MaxWait:     readerMaxWait,
	})
	c := NewProductClassifiedConsumerFromReader(reader, topic, apply, logger)
	c.closer = reader
	return c, nil
}

// NewProductClassifiedConsumerFromReader builds the consumer over any
// fetchCommitReader (unit tests pass a fake).
func NewProductClassifiedConsumerFromReader(reader fetchCommitReader, topic string, apply *usecases.ApplyProductClassified, logger *slog.Logger) *ProductClassifiedConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &ProductClassifiedConsumer{
		reader:       reader,
		topic:        topic,
		apply:        apply,
		logger:       logger,
		retryInitial: classifiedRetryInitialInterval,
		retryMax:     classifiedRetryMaxInterval,
	}
}

// WithRetryIntervals overrides the transient-failure backoff bounds (tests).
func (c *ProductClassifiedConsumer) WithRetryIntervals(initial, maxInterval time.Duration) *ProductClassifiedConsumer {
	c.retryInitial, c.retryMax = initial, maxInterval
	return c
}

// Close releases the underlying Kafka reader (a no-op for an injected one).
func (c *ProductClassifiedConsumer) Close() error {
	if c.closer == nil {
		return nil
	}
	return c.closer.Close()
}

// Run fetches, applies and commits until ctx is cancelled. Broker read
// errors are retried with backoff (never giving up). A message whose
// handling is interrupted by ctx is NOT committed: it is redelivered to the
// next owner of the partition.
func (c *ProductClassifiedConsumer) Run(ctx context.Context) error {
	readPolicy := newRecoverBackoff()
	for {
		msg, ok := c.fetchRecovering(ctx, readPolicy)
		if !ok {
			return nil
		}
		if !c.handleUntilDone(ctx, msg) {
			return nil
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// The effect is committed and deduped on the CloudEvents id,
			// so a redelivery caused by this lost commit is harmless.
			c.logger.WarnContext(ctx, "kafka commit failed; the message will be redelivered and deduplicated",
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
		}
	}
}

// fetchRecovering fetches the next message, riding out broker failures with
// backoff (the same never-give-up policy as readRecovering). ok=false only
// when ctx is done.
func (c *ProductClassifiedConsumer) fetchRecovering(ctx context.Context, policy backoff.BackOff) (kafkago.Message, bool) {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err == nil {
			policy.Reset()
			return msg, true
		}
		if ctx.Err() != nil {
			return kafkago.Message{}, false
		}
		wait := policy.NextBackOff()
		c.logger.WarnContext(ctx, "kafka fetch failed; retrying", "topic", c.topic, "retry_in", wait.String(), "error", err)
		select {
		case <-ctx.Done():
			return kafkago.Message{}, false
		case <-time.After(wait):
		}
	}
}

// handleUntilDone retries Handle on the SAME message until it returns nil.
// It returns false only when ctx ended first (the message stays uncommitted).
func (c *ProductClassifiedConsumer) handleUntilDone(ctx context.Context, msg kafkago.Message) bool {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(c.retryInitial),
		backoff.WithMaxInterval(c.retryMax),
		backoff.WithMaxElapsedTime(0),
	)
	for {
		err := c.Handle(ctx, msg)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		wait := policy.NextBackOff()
		c.logger.ErrorContext(ctx, "applying ProductClassified failed; retrying the same message",
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "retry_in", wait.String(), "error", err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(wait):
		}
	}
}

// Handle processes one message in a "kafka.consume <topic>" span (parent =
// the producer's span from the headers). It returns nil when the message is
// done with (applied, stale, duplicate, ignored type, or skipped as
// invalid) and an error only for a transient failure worth retrying.
func (c *ProductClassifiedConsumer) Handle(ctx context.Context, msg kafkago.Message) error {
	return handleConsumedEvent(ctx, msg, c.logger, decodeProductClassified,
		ErrInvalidProductClassified, "skipping ProductClassified with an invalid payload",
		"ProductClassified",
		func(req usecases.ProductClassifiedRequest) string { return req.EventId },
		c.apply.Execute)
}

// decodeProductClassified decodes raw as a CloudEvents 1.0 event. ok=false
// (nil error) for a valid event of any other type. Errors wrap
// cloudevents.ErrNotCloudEvent or ErrInvalidProductClassified.
func decodeProductClassified(raw []byte) (usecases.ProductClassifiedRequest, bool, error) {
	e, err := cloudevents.Decode(raw)
	if err != nil {
		return usecases.ProductClassifiedRequest{}, false, fmt.Errorf("kafka: %w", err)
	}
	if e.Type() != TypeProductClassified {
		return usecases.ProductClassifiedRequest{}, false, nil
	}
	var data ProductClassifiedData
	if err := e.DataAs(&data); err != nil {
		return usecases.ProductClassifiedRequest{}, false, fmt.Errorf("%w: event %s: %w", ErrInvalidProductClassified, e.ID(), err)
	}
	if data.SKU == "" {
		return usecases.ProductClassifiedRequest{}, false, fmt.Errorf("%w: event %s: empty sku", ErrInvalidProductClassified, e.ID())
	}
	if data.Version < 1 {
		return usecases.ProductClassifiedRequest{}, false, fmt.Errorf("%w: event %s: version %d < 1", ErrInvalidProductClassified, e.ID(), data.Version)
	}
	dot := 0
	if data.DOTHazardClass != nil {
		dot = *data.DOTHazardClass
		if dot < 1 || dot > 9 {
			return usecases.ProductClassifiedRequest{}, false, fmt.Errorf("%w: event %s: dot_hazard_class %d outside 1-9", ErrInvalidProductClassified, e.ID(), dot)
		}
	}
	return usecases.ProductClassifiedRequest{
		EventId: e.ID(),
		Record: ports.ProductClassificationRecord{
			SKU:              data.SKU,
			HandlingTags:     data.HandlingTags,
			TemperatureClass: data.TemperatureClass,
			DOTHazardClass:   dot,
			Version:          data.Version,
		},
	}, true, nil
}
