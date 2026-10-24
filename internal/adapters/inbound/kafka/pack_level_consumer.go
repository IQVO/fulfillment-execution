package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared/gs1"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
	"github.com/claudioed/fulfillment-execution/internal/observability"
)

// The FULL CloudEvents types product-master publishes its pack-level facts
// under (product-master ADR 0011), on ProductMasterTopic, subject and key =
// the SKU. Payloads are snake_case. Every other type on the topic is ignored.
const (
	TypeProductPackLevelDeclared = "com.warehouse.wms.product-master.product.ProductPackLevelDeclared"
	TypeProductPackLevelRetired  = "com.warehouse.wms.product-master.product.ProductPackLevelRetired"
)

// Retry bounds for a transient apply failure (the same message is retried
// until it succeeds or the consumer is stopped; its offset is never
// committed on failure).
const (
	packLevelRetryInitialInterval = 200 * time.Millisecond
	packLevelRetryMaxInterval     = 5 * time.Second
)

// ErrInvalidPackLevel marks a valid CloudEvent of a pack-level type whose
// payload breaks the contract (missing sku, unknown level, invalid GTIN,
// units_per_pack or version below 1, undecodable data). It is deterministic:
// the consumer WARN-logs it and commits past, never retries.
var ErrInvalidPackLevel = errors.New("kafka: invalid pack-level payload")

// ProductPackLevelDeclaredData is the payload of ProductPackLevelDeclared
// (the FULL level after the change). dims (the optional DECLARED dimensions,
// used by ship-in-own-container, ADR 0056) are kept; is_orderable and
// is_despatch_unit are not needed here and ignored.
type ProductPackLevelDeclaredData struct {
	SKU           string         `json:"sku"`
	Level         string         `json:"level"`
	GTIN          string         `json:"gtin"`
	ChildGTIN     string         `json:"child_gtin,omitempty"`
	QtyOfChild    int            `json:"qty_of_child,omitempty"`
	UnitsPerPack  int            `json:"units_per_pack"`
	Dims          *PackLevelDims `json:"dims,omitempty"`
	EffectiveFrom time.Time      `json:"effective_from"`
	Version       int64          `json:"version"`
}

// PackLevelDims is the optional `dims` of ProductPackLevelDeclared:
// millimetres and grams. Absent fields decode as 0 and make the dims
// unusable (see declaredDims).
type PackLevelDims struct {
	LengthMM int `json:"length_mm"`
	WidthMM  int `json:"width_mm"`
	HeightMM int `json:"height_mm"`
	WeightG  int `json:"weight_g"`
}

// declaredDims keeps dims only when all four measures are positive. A
// partial or non-positive dims is dropped (the level is still applied): the
// quantity facts are what picking needs, and a level without usable dims
// simply cannot ship in its own container (ErrCaseDimensionsUnknown).
func declaredDims(d *PackLevelDims) *ports.PackDims {
	if d == nil || d.LengthMM < 1 || d.WidthMM < 1 || d.HeightMM < 1 || d.WeightG < 1 {
		return nil
	}
	return &ports.PackDims{LengthMM: d.LengthMM, WidthMM: d.WidthMM, HeightMM: d.HeightMM, WeightG: d.WeightG}
}

// ProductPackLevelRetiredData is the payload of ProductPackLevelRetired.
type ProductPackLevelRetiredData struct {
	SKU         string    `json:"sku"`
	Level       string    `json:"level"`
	GTIN        string    `json:"gtin"`
	EffectiveTo time.Time `json:"effective_to"`
	Reason      string    `json:"reason"`
	Version     int64     `json:"version"`
}

// PackLevelConsumer keeps the local PackView (ADR 0054) current from
// product-master's ProductPackLevelDeclared / ProductPackLevelRetired events.
//
// Delivery contract (that of ProductClassifiedConsumer): FetchMessage, then
// the CloudEvents-id claim and the version-guarded write in ONE transaction
// (usecases.ApplyPackLevel), then CommitMessages. A transient failure is
// retried on the same message with capped backoff; a message that is not a
// valid CloudEvent, or a pack-level event with an invalid payload, is
// WARN-logged and committed past; any other type is ignored and committed past.
type PackLevelConsumer struct {
	reader MessageFetchCommitter
	closer interface{ Close() error }
	topic  string
	apply  *usecases.ApplyPackLevel
	logger *slog.Logger

	retryInitial time.Duration
	retryMax     time.Duration
}

// NewPackLevelConsumer builds the consumer over a kafka-go reader in
// consumer group groupID (from PACK_VIEW_CONSUMER_GROUP; never a
// literal). A brand-new group starts at the EARLIEST offset: the copy must
// see the whole classification history, not only what is published after
// the first deploy.
func NewPackLevelConsumer(brokers []string, topic, groupID string, apply *usecases.ApplyPackLevel, logger *slog.Logger) (*PackLevelConsumer, error) {
	if groupID == "" {
		return nil, errors.New("kafka: PackLevel consumer needs a consumer group (PACK_VIEW_CONSUMER_GROUP)")
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafkago.FirstOffset,
		MaxWait:     readerMaxWait,
	})
	c := NewPackLevelConsumerFromReader(reader, topic, apply, logger)
	c.closer = reader
	return c, nil
}

// NewPackLevelConsumerFromReader builds the consumer over any
// MessageFetchCommitter (unit tests pass a fake).
func NewPackLevelConsumerFromReader(reader MessageFetchCommitter, topic string, apply *usecases.ApplyPackLevel, logger *slog.Logger) *PackLevelConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &PackLevelConsumer{
		reader:       reader,
		topic:        topic,
		apply:        apply,
		logger:       logger,
		retryInitial: packLevelRetryInitialInterval,
		retryMax:     packLevelRetryMaxInterval,
	}
}

// WithRetryIntervals overrides the transient-failure backoff bounds (tests).
func (c *PackLevelConsumer) WithRetryIntervals(initial, maxInterval time.Duration) *PackLevelConsumer {
	c.retryInitial, c.retryMax = initial, maxInterval
	return c
}

// readerMaxWait bounds how long a consumer group reader's fetch blocks on a
// quiet partition. kafka-go's Reader.Close waits for that in-flight fetch, and
// the default (10s) times the number of consumers closing one after another
// (deferred, in run) exceeded the e2e graceful-stop budget once a fourth
// consumer existed. Polling twice a second costs nothing on these low-volume
// topics and makes Close prompt.
const readerMaxWait = 500 * time.Millisecond

// Close releases the underlying Kafka reader (a no-op for an injected one).
func (c *PackLevelConsumer) Close() error {
	if c.closer == nil {
		return nil
	}
	return c.closer.Close()
}

// Run fetches, applies and commits until ctx is cancelled. Broker read
// errors are retried with backoff (never giving up). A message whose
// handling is interrupted by ctx is NOT committed: it is redelivered to the
// next owner of the partition.
func (c *PackLevelConsumer) Run(ctx context.Context) error {
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
func (c *PackLevelConsumer) fetchRecovering(ctx context.Context, policy backoff.BackOff) (kafkago.Message, bool) {
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
func (c *PackLevelConsumer) handleUntilDone(ctx context.Context, msg kafkago.Message) bool {
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
		c.logger.ErrorContext(ctx, "applying a pack-level event failed; retrying the same message",
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
func (c *PackLevelConsumer) Handle(ctx context.Context, msg kafkago.Message) error {
	ctx = observability.ExtractKafkaTrace(ctx, msg.Headers)
	ctx, span := otel.Tracer(observability.InstrumentationName).Start(ctx,
		"kafka.consume "+msg.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(msg.Topic),
			semconv.MessagingOperationName("process"),
		),
	)
	defer span.End()

	req, ok, err := decodePackLevel(msg.Value)
	switch {
	case errors.Is(err, cloudevents.ErrNotCloudEvent):
		c.logger.WarnContext(ctx, "skipping kafka message that is not a valid CloudEvent",
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	case errors.Is(err, ErrInvalidPackLevel):
		c.logger.WarnContext(ctx, "skipping a pack-level event with an invalid payload",
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	case err != nil:
		return err
	case !ok:
		return nil
	}

	if err := c.apply.Execute(ctx, req); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("kafka: apply pack level %s: %w", req.EventId, err)
	}
	return nil
}

// decodePackLevel decodes raw as a CloudEvents 1.0 event. ok=false (nil
// error) for a valid event of any other type. Errors wrap
// cloudevents.ErrNotCloudEvent or ErrInvalidPackLevel.
func decodePackLevel(raw []byte) (usecases.PackLevelRequest, bool, error) {
	e, err := cloudevents.Decode(raw)
	if err != nil {
		return usecases.PackLevelRequest{}, false, fmt.Errorf("kafka: %w", err)
	}
	switch e.Type() {
	case TypeProductPackLevelDeclared:
		var data ProductPackLevelDeclaredData
		if err := e.DataAs(&data); err != nil {
			return usecases.PackLevelRequest{}, false, fmt.Errorf("%w: event %s: %v", ErrInvalidPackLevel, e.ID(), err)
		}
		rec, err := declaredRecord(data)
		if err != nil {
			return usecases.PackLevelRequest{}, false, fmt.Errorf("%w: event %s: %v", ErrInvalidPackLevel, e.ID(), err)
		}
		return usecases.PackLevelRequest{EventId: e.ID(), Record: rec}, true, nil
	case TypeProductPackLevelRetired:
		var data ProductPackLevelRetiredData
		if err := e.DataAs(&data); err != nil {
			return usecases.PackLevelRequest{}, false, fmt.Errorf("%w: event %s: %v", ErrInvalidPackLevel, e.ID(), err)
		}
		g, err := gs1.ParseGTIN(data.GTIN)
		switch {
		case err != nil:
			return usecases.PackLevelRequest{}, false, fmt.Errorf("%w: event %s: gtin: %v", ErrInvalidPackLevel, e.ID(), err)
		case data.Version < 1:
			return usecases.PackLevelRequest{}, false, fmt.Errorf("%w: event %s: version %d < 1", ErrInvalidPackLevel, e.ID(), data.Version)
		case data.EffectiveTo.IsZero():
			return usecases.PackLevelRequest{}, false, fmt.Errorf("%w: event %s: effective_to is required", ErrInvalidPackLevel, e.ID())
		}
		return usecases.PackLevelRequest{
			EventId: e.ID(), Retire: true, RetiredAt: data.EffectiveTo,
			Record: ports.PackRecord{GTIN: g.String(), Version: data.Version},
		}, true, nil
	default:
		return usecases.PackLevelRequest{}, false, nil
	}
}

// declaredRecord validates a Declared payload and normalises its GTINs to 14
// digits (gs1).
func declaredRecord(data ProductPackLevelDeclaredData) (ports.PackRecord, error) {
	level := task.PackLevel(data.Level)
	switch level {
	case task.PackLevelEach, task.PackLevelInner, task.PackLevelCase, task.PackLevelPallet:
	default:
		return ports.PackRecord{}, fmt.Errorf("unknown level %q", data.Level)
	}
	g, err := gs1.ParseGTIN(data.GTIN)
	if err != nil {
		return ports.PackRecord{}, fmt.Errorf("gtin: %w", err)
	}
	child := ""
	if data.ChildGTIN != "" {
		c, err := gs1.ParseGTIN(data.ChildGTIN)
		if err != nil {
			return ports.PackRecord{}, fmt.Errorf("child_gtin: %w", err)
		}
		child = c.String()
	}
	switch {
	case strings.TrimSpace(data.SKU) == "":
		return ports.PackRecord{}, errors.New("empty sku")
	case data.UnitsPerPack < 1:
		return ports.PackRecord{}, fmt.Errorf("units_per_pack %d < 1", data.UnitsPerPack)
	case data.Version < 1:
		return ports.PackRecord{}, fmt.Errorf("version %d < 1", data.Version)
	case data.EffectiveFrom.IsZero():
		return ports.PackRecord{}, errors.New("effective_from is required")
	}
	return ports.PackRecord{
		GTIN: g.String(), SKU: data.SKU, Level: level, UnitsPerPack: data.UnitsPerPack,
		ChildGTIN: child, Version: data.Version, EffectiveFrom: data.EffectiveFrom,
		Dims: declaredDims(data.Dims),
	}, nil
}
