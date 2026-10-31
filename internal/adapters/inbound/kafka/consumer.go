// Package kafka provides the inbound adapter that consumes WorkReleased
// CloudEvents (ADR-0032) from Work Planning and turns each one into a Task via the existing
// CreateTask use case — the intended use of that use case, so it is called
// directly rather than through a new one.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
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

// maxHandlerAttempts bounds HandleMessage's in-process retry (ADR-0029
// §DLQ) before a message is dead-lettered: 1 initial attempt plus up to
// 2 retries, mirroring order-management's RepromiseConsumer
// (maxHandlerAttempts there, same bound) — the fleet reference for this
// phase.
const maxHandlerAttempts = 3

const (
	retryInitialInterval = 100 * time.Millisecond
	retryMaxInterval     = 2 * time.Second
)

// Consumer reads WorkReleased CloudEvents off warehouse.work-planning.events
// and creates a Task for each one, exactly once per CloudEvents id despite
// Kafka's at-least-once delivery.
//
// DeadLetter, when non-nil, is where a message that fails HandleMessage is
// published instead of being silently dropped after logging (see Run and
// sendToDeadLetter). It is nil-safe: a Consumer built without one (every
// pre-existing test in this package, and any deployment that predates this
// feature) behaves exactly as before — log and continue, same as ADR-0004
// originally documented. No dead-letter naming convention already existed
// anywhere in this fleet (checked every repo's Go source for
// "dead letter"/"DLQ" before choosing one), so DeadLetterTopic's
// "<topic>.dlq" suffix is this consumer's own convention, not an existing
// fleet standard being followed.
type Consumer struct {
	Reader *kafkago.Reader
	// Apply is the use case that turns one WorkReleased occurrence into a
	// Task exactly once: it claims the CloudEvents id, validates the
	// path_id against the catalogue, saves the Task and publishes
	// TaskCreated all inside ONE UnitOfWork (see usecases.ApplyWorkReleased,
	// which fixed the lost-event bug this consumer's own claim-then-create
	// shape had).
	Apply      *usecases.ApplyWorkReleased
	Processed  ports.ProcessedEvents
	Catalogue  ports.PathCatalogue
	Logger     *slog.Logger
	DeadLetter DeadLetterSink
	// testReader, when set, replaces Reader for FetchMessage/CommitMessages
	// (unit tests of the broker-outage recovery in Run).
	testReader fetchCommitReader
}

// DeadLetterSink publishes one or more messages, matching the subset of
// *kafkago.Writer this consumer needs so tests can substitute a fake
// without a live broker.
type DeadLetterSink interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// DeadLetterTopic derives the dead-letter topic name for topic: this
// consumer's own "<topic>.dlq" convention.
func DeadLetterTopic(topic string) string {
	return topic + ".dlq"
}

// NewConsumer constructs a Consumer reading topic from brokers as part of
// consumer group "fulfillment-execution".
func NewConsumer(brokers []string, topic string, apply *usecases.ApplyWorkReleased, logger *slog.Logger) *Consumer {
	return newConsumer(brokers, topic, "fulfillment-execution", kafkago.FirstOffset, apply, logger)
}

// NewConsumerWithGroup constructs an isolated Consumer. Its first assignment
// begins at the latest offset, so a system-test database is populated only by
// events released after the test's service process is ready.
func NewConsumerWithGroup(brokers []string, topic, groupID string, apply *usecases.ApplyWorkReleased, logger *slog.Logger) *Consumer {
	return newConsumer(brokers, topic, groupID, kafkago.LastOffset, apply, logger)
}

func newConsumer(brokers []string, topic, groupID string, startOffset int64, apply *usecases.ApplyWorkReleased, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: startOffset,
		MaxWait:     readerMaxWait,
	})
	return &Consumer{Reader: reader, Apply: apply, Logger: logger}
}

// Run reads and handles messages until ctx is cancelled. Each message is
// FETCHED (not read-and-auto-committed) and its offset is committed only once
// it is settled: applied, or published to DeadLetter (when configured — see
// the Consumer type doc comment) with the failure's error message attached
// after Handle's in-process retries are exhausted. A failure to publish to
// DeadLetter itself is logged separately and does NOT stop the loop — a
// broker blip on the DLQ publish must not wedge the main consumer, which is
// the whole point of this feature.
//
// Cancelling ctx while a message is mid-handle is a clean stop (nil): the
// message is neither dead-lettered (only cancellation interrupted it) nor
// committed, so it is redelivered on the next start and ApplyWorkReleased's
// CloudEvents-id claim makes the replay safe. A commit failure is logged and
// the loop continues: the message is redelivered and deduplicated.
func (c *Consumer) Run(ctx context.Context) error {
	policy := newRecoverBackoff()
	for {
		msg, ok := fetchRecovering(ctx, c.reader(), policy, c.Logger, c.topic())
		if !ok {
			return nil
		}
		if !c.settle(ctx, msg) {
			return nil
		}
		if err := c.reader().CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.Logger.WarnContext(ctx, "kafka commit failed; the message will be redelivered and deduplicated",
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
		}
	}
}

// settle handles msg and, when Handle fails for good, dead-letters it. It
// reports false -- nothing dead-lettered, nothing to commit -- when ctx was
// cancelled mid-handle: a message only shutdown interrupted is not poison.
func (c *Consumer) settle(ctx context.Context, msg kafkago.Message) bool {
	err := c.Handle(ctx, msg)
	if err == nil {
		return true
	}
	if ctx.Err() != nil {
		return false
	}
	if errors.Is(err, cloudevents.ErrNotCloudEvent) {
		// Deterministic poison message (not a valid CloudEvents
		// 1.0 event, e.g. the retired flat envelope): never
		// retried, never parsed any other way — dead-lettered.
		c.Logger.WarnContext(ctx, "kafka message is not a valid CloudEvent; dead-lettering",
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
	} else {
		c.Logger.ErrorContext(ctx, "kafka message handling failed after retries",
			"topic", msg.Topic, "attempts", maxHandlerAttempts, "error", err)
	}
	c.SendToDeadLetter(ctx, msg, err)
	return true
}

// sendToDeadLetter publishes msg to DeadLetterTopic(msg.Topic), preserving
// the original key/value/headers and adding failure context as extra
// headers, so a message that fails processing is preserved for later
// inspection/replay instead of being lost after only a log line. A no-op
// when c.DeadLetter is nil (see the Consumer type doc comment). A
// publish failure here is logged and swallowed — it must never propagate
// back into Run's loop.
func (c *Consumer) SendToDeadLetter(ctx context.Context, msg kafkago.Message, cause error) {
	if c.DeadLetter == nil {
		return
	}
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "x-dlq-error", Value: []byte(cause.Error())},
		kafkago.Header{Key: "x-dlq-original-topic", Value: []byte(msg.Topic)},
		kafkago.Header{Key: "x-dlq-original-partition", Value: []byte(strconv.Itoa(msg.Partition))},
		kafkago.Header{Key: "x-dlq-original-offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
	)
	dlqMsg := kafkago.Message{
		Topic:   DeadLetterTopic(msg.Topic),
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	}
	if err := c.DeadLetter.WriteMessages(ctx, dlqMsg); err != nil {
		c.Logger.ErrorContext(ctx, "failed to publish message to dead-letter topic",
			"dlq_topic", dlqMsg.Topic, "original_error", cause, "dlq_error", err)
	}
}

// Close releases the underlying Kafka reader.
func (c *Consumer) Close() error {
	return c.Reader.Close()
}

// Handle processes one consumed message inside a
// "kafka.consume <topic>" span whose parent is the producer's span, read
// from the message headers. That link is what makes a WorkReleased published
// by wes-work-planning and the Task created here parts of a single
// distributed trace.
//
// The message is decoded and its CloudEvents id claimed via MarkProcessed
// exactly once; the remaining, retryable work (Catalogue.Lookup then
// CreateTask.Execute) is retried in-process, with jittered backoff, up
// to maxHandlerAttempts total attempts (ADR-0029 §DLQ) — a transient
// blip (a momentary downstream hiccup, a lost connection) heals itself
// without ever reaching the DLQ, all inside this ONE span/consume
// attempt from Run's perspective. Only once every attempt is exhausted
// does the returned error propagate to Run, which then dead-letters the
// message (see Run's doc comment) — mirrors order-management's
// RepromiseConsumer.handleWithRetry (same bound, same backoff shape),
// the fleet reference for this phase, adapted for this consumer's own
// MarkProcessed-then-create shape (see handleMessageWithRetry's doc
// comment for why the claim is deliberately NOT inside the retry loop).
//
// It is exported separately from Run so the propagation can be tested
// without a live broker.
func (c *Consumer) Handle(ctx context.Context, msg kafkago.Message) error {
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

	if err := c.handleMessageWithRetry(ctx, msg.Value); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// handleMessageWithRetry retries the atomic application of one WorkReleased
// occurrence up to maxHandlerAttempts times with jittered exponential
// backoff, bounded by ctx's own deadline/cancellation.
//
// Unlike the pre-atomicity-fix shape, there is no separate claim step to
// keep out of the retry loop: ApplyWorkReleased.Execute claims the
// CloudEvents id and creates the Task inside ONE UnitOfWork, so a failed
// attempt leaves NO processed marker behind (a transactional scope rolls
// the claim back with the task; the in-memory configuration undoes it via
// ports.ProcessedEventReleaser). Retrying the whole call is therefore
// always safe — attempt 2 either genuinely re-applies (the failure
// blipped) or reports the already-applied redelivery (attempt 1 actually
// succeeded and the error was a commit-time false negative, which the
// processed-events claim still dedupes).
func (c *Consumer) handleMessageWithRetry(ctx context.Context, raw []byte) error {
	env, ok, err := decodeWorkReleased(raw)
	if err != nil || !ok {
		return err
	}

	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxHandlerAttempts-1), ctx)

	req, rerr := requestOf(env)
	if rerr != nil {
		// A deterministic payload-contract violation (transfer_ref with
		// an unknown work_kind, an invalid relocation move): like an
		// unknown path_id, retrying can never fix it. Returned as a
		// permanent error, so Run dead-letters it.
		return rerr
	}

	return backoff.Retry(func() error {
		if err := c.Apply.Execute(ctx, req); err != nil {
			return fmt.Errorf("kafka: apply work released: %w", err)
		}
		return nil
	}, bounded)
}

// HandleMessage decodes raw as a CloudEvents 1.0 structured-mode event
// (see decodeWorkReleased) and, if it is a WorkReleased event, applies it
// atomically via ApplyWorkReleased — the CloudEvents-id claim, the
// process-path catalogue validation, the Task save and the TaskCreated
// outbox publish all inside ONE UnitOfWork. It is exported separately
// from Handle/Run so tests can feed it a raw CloudEvent without a live
// broker. It performs exactly ONE attempt (no retry) — Handle is what
// wraps the retryable portion.
func (c *Consumer) HandleMessage(ctx context.Context, raw []byte) error {
	env, ok, err := decodeWorkReleased(raw)
	if err != nil || !ok {
		return err
	}

	req, err := requestOf(env)
	if err != nil {
		return err
	}
	return c.Apply.Execute(ctx, req)
}

// reader returns the fetch/commit seam: the test hook when set, else Reader.
func (c *Consumer) reader() fetchCommitReader {
	if c.testReader != nil {
		return c.testReader
	}
	return c.Reader
}

func (c *Consumer) topic() string {
	if c.Reader == nil {
		return ""
	}
	return c.Reader.Config().Topic
}
