package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	"github.com/claudioed/fulfillment-execution/internal/observability"
)

// TaskInterleavingTopic is task-interleaving's integration topic (its
// AsyncAPI, channel warehouse.task-interleaving.events). Key and subject =
// the station id.
const TaskInterleavingTopic = "warehouse.task-interleaving.events"

// TypeInterleavingHintIssued is the FULL CloudEvents `type`
// task-interleaving publishes InterleavingHintIssued under (its ADR 0002).
// The consumer dispatches on this exact string; every other type on the
// topic is ignored and committed past.
const TypeInterleavingHintIssued = "com.warehouse.wes.task-interleaving.stationtrip.InterleavingHintIssued"

// Retry bounds for a transient apply failure. The SAME message is retried
// until it succeeds or the consumer is stopped; its offset is never
// committed on failure.
const (
	hintRetryInitialInterval = 200 * time.Millisecond
	hintRetryMaxInterval     = 5 * time.Second
)

// ErrInvalidInterleavingHint marks a valid CloudEvent of the
// InterleavingHintIssued type whose payload breaks the pinned contract
// (missing hint_id/station_id/site_id/expires_at, empty candidates, a rank
// below 1, a blank candidate task_id). It is deterministic: the consumer
// WARN-logs it and commits past, never retries — a malformed hint is an
// advisory hint that costs nothing to drop.
var ErrInvalidInterleavingHint = errors.New("kafka: invalid InterleavingHintIssued payload")

// InterleavingHintIssuedData is the payload of task-interleaving's
// InterleavingHintIssued event (the pinned contract of its apis/asyncapi.yaml).
// Only the fields this context acts on are decoded; after_task_id,
// origin_location, policy_version, task_type, location and detour_m are
// carried by the contract but unused here — claimNext needs only the ranked
// candidate task ids.
type InterleavingHintIssuedData struct {
	HintId     string    `json:"hint_id"`
	StationId  string    `json:"station_id"`
	SiteId     string    `json:"site_id"`
	ExpiresAt  time.Time `json:"expires_at"`
	Candidates []struct {
		Rank   int    `json:"rank"`
		TaskId string `json:"task_id"`
	} `json:"candidates"`
}

// InterleavingHintConsumer keeps the local hint copy (ADR 0051) current
// from task-interleaving's InterleavingHintIssued events.
//
// Delivery contract: FetchMessage, then the CloudEvents-id claim and the
// per-station superseding upsert in ONE transaction
// (usecases.ApplyInterleavingHint), then CommitMessages. A transient
// failure is retried on the same message with capped backoff; a message
// that is not a valid CloudEvent, or an InterleavingHintIssued with an
// invalid payload, is WARN-logged and committed past; any other type is
// ignored and committed past.
type InterleavingHintConsumer struct {
	reader MessageFetchCommitter
	closer interface{ Close() error }
	topic  string
	apply  *usecases.ApplyInterleavingHint
	logger *slog.Logger

	retryInitial time.Duration
	retryMax     time.Duration
}

// NewInterleavingHintConsumer builds the consumer over a kafka-go reader in
// consumer group groupID (from TASK_INTERLEAVING_CONSUMER_GROUP; never a
// literal). A brand-new group starts at the LATEST offset: hints are
// advisory and expire within seconds, so replaying history is pointless —
// the next hint for a station supersedes anything missed.
func NewInterleavingHintConsumer(brokers []string, topic, groupID string, apply *usecases.ApplyInterleavingHint, logger *slog.Logger) (*InterleavingHintConsumer, error) {
	if groupID == "" {
		return nil, errors.New("kafka: InterleavingHint consumer needs a consumer group (TASK_INTERLEAVING_CONSUMER_GROUP)")
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafkago.LastOffset,
	})
	c := NewInterleavingHintConsumerFromReader(reader, topic, apply, logger)
	c.closer = reader
	return c, nil
}

// NewInterleavingHintConsumerFromReader builds the consumer over any
// MessageFetchCommitter (unit tests pass a fake).
func NewInterleavingHintConsumerFromReader(reader MessageFetchCommitter, topic string, apply *usecases.ApplyInterleavingHint, logger *slog.Logger) *InterleavingHintConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &InterleavingHintConsumer{
		reader:       reader,
		topic:        topic,
		apply:        apply,
		logger:       logger,
		retryInitial: hintRetryInitialInterval,
		retryMax:     hintRetryMaxInterval,
	}
}

// WithRetryIntervals overrides the transient-failure backoff bounds (tests).
func (c *InterleavingHintConsumer) WithRetryIntervals(initial, maxInterval time.Duration) *InterleavingHintConsumer {
	c.retryInitial = initial
	c.retryMax = maxInterval
	return c
}

// Close releases the underlying Kafka reader (a no-op for an injected one).
func (c *InterleavingHintConsumer) Close() error {
	if c.closer == nil {
		return nil
	}
	return c.closer.Close()
}

// Run fetches, applies and commits until ctx is cancelled. Broker read
// errors are retried with backoff (never giving up). A message whose
// handling is interrupted by ctx is NOT committed: it is redelivered to the
// next owner of the partition.
func (c *InterleavingHintConsumer) Run(ctx context.Context) error {
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
// backoff. ok=false only when ctx is done.
func (c *InterleavingHintConsumer) fetchRecovering(ctx context.Context, policy backoff.BackOff) (kafkago.Message, bool) {
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
func (c *InterleavingHintConsumer) handleUntilDone(ctx context.Context, msg kafkago.Message) bool {
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
		c.logger.ErrorContext(ctx, "applying InterleavingHintIssued failed; retrying the same message",
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
// done with (applied, duplicate, ignored type, or skipped as invalid) and
// an error only for a transient failure worth retrying.
func (c *InterleavingHintConsumer) Handle(ctx context.Context, msg kafkago.Message) error {
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

	req, ok, err := decodeInterleavingHintIssued(msg.Value)
	switch {
	case errors.Is(err, cloudevents.ErrNotCloudEvent):
		c.logger.WarnContext(ctx, "skipping kafka message that is not a valid CloudEvent",
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return nil
	case errors.Is(err, ErrInvalidInterleavingHint):
		c.logger.WarnContext(ctx, "skipping InterleavingHintIssued with an invalid payload",
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
		return fmt.Errorf("kafka: apply InterleavingHintIssued %s: %w", req.EventId, err)
	}
	return nil
}

// decodeInterleavingHintIssued decodes raw as a CloudEvents 1.0 event.
// ok=false (nil error) for a valid event of any other type. Errors wrap
// cloudevents.ErrNotCloudEvent or ErrInvalidInterleavingHint.
func decodeInterleavingHintIssued(raw []byte) (usecases.InterleavingHintRequest, bool, error) {
	e, err := cloudevents.Decode(raw)
	if err != nil {
		return usecases.InterleavingHintRequest{}, false, fmt.Errorf("kafka: %w", err)
	}
	if e.Type() != TypeInterleavingHintIssued {
		return usecases.InterleavingHintRequest{}, false, nil
	}
	var data InterleavingHintIssuedData
	if err := e.DataAs(&data); err != nil {
		return usecases.InterleavingHintRequest{}, false, fmt.Errorf("%w: event %s: %v", ErrInvalidInterleavingHint, e.ID(), err)
	}
	if data.HintId == "" || data.StationId == "" || data.SiteId == "" {
		return usecases.InterleavingHintRequest{}, false, fmt.Errorf("%w: event %s: hint_id, station_id and site_id are required", ErrInvalidInterleavingHint, e.ID())
	}
	if data.ExpiresAt.IsZero() {
		return usecases.InterleavingHintRequest{}, false, fmt.Errorf("%w: event %s: expires_at is required", ErrInvalidInterleavingHint, e.ID())
	}
	if len(data.Candidates) == 0 {
		return usecases.InterleavingHintRequest{}, false, fmt.Errorf("%w: event %s: candidates must be non-empty", ErrInvalidInterleavingHint, e.ID())
	}
	candidates := make([]ports.InterleavingHintCandidate, len(data.Candidates))
	for i, c := range data.Candidates {
		if c.Rank < 1 || c.TaskId == "" {
			return usecases.InterleavingHintRequest{}, false, fmt.Errorf("%w: event %s: candidate %d has rank %d and task_id %q", ErrInvalidInterleavingHint, e.ID(), i, c.Rank, c.TaskId)
		}
		candidates[i] = ports.InterleavingHintCandidate{Rank: c.Rank, TaskId: c.TaskId}
	}
	return usecases.InterleavingHintRequest{
		EventId: e.ID(),
		Hint: ports.InterleavingHint{
			HintId:     data.HintId,
			StationId:  data.StationId,
			SiteId:     data.SiteId,
			ExpiresAt:  data.ExpiresAt,
			Candidates: candidates,
			ReceivedAt: time.Now().UTC(),
		},
	}, true, nil
}
