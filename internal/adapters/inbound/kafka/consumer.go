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
	"github.com/claudioed/fulfillment-execution/internal/domain/pathcatalog"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
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

// TypeWorkReleased is the FULL CloudEvents `type` wes-work-planning
// publishes WorkReleased under (fleet cross-service contract, ADR-0032).
// This consumer dispatches on the exact string — never a short name or a
// suffix match.
const TypeWorkReleased = "com.warehouse.wes.work-planning.workunit.WorkReleased"

// workReleased is one decoded WorkReleased occurrence: the CloudEvents
// `id` (this consumer's idempotency key) plus the payload read via DataAs.
type workReleased struct {
	EventId string
	Data    WorkReleasedData
}

// decodeWorkReleased decodes raw as a CloudEvents 1.0 structured-mode
// event. ok is false (and err nil) for a valid event of any other type —
// unknown types are ignored for forward compatibility. A message that is
// not a valid CloudEvents 1.0 event (including the retired flat envelope)
// returns an error wrapping cloudevents.ErrNotCloudEvent: a deterministic
// poison message that Run dead-letters, never parsed any other way.
func decodeWorkReleased(raw []byte) (workReleased, bool, error) {
	e, err := cloudevents.Decode(raw)
	if err != nil {
		return workReleased{}, false, fmt.Errorf("kafka: %w", err)
	}
	if e.Type() != TypeWorkReleased {
		return workReleased{}, false, nil
	}
	var data WorkReleasedData
	if err := e.DataAs(&data); err != nil {
		return workReleased{}, false, fmt.Errorf("kafka: decode %s data: %w", TypeWorkReleased, err)
	}
	return workReleased{EventId: e.ID(), Data: data}, true, nil
}

// WorkReleasedData is the payload of a WorkReleased event.
type WorkReleasedData struct {
	PathId     string    `json:"path_id"`
	WorkUnitId string    `json:"work_unit_id"`
	CPT        time.Time `json:"cpt"`
	Ref        string    `json:"ref"`
	// Fragile is an optional packing hint set by wes-work-planning at
	// release time, sourced from inventory-storage's ProductClassification
	// (true if the upstream order line was classified Fragile). It is
	// omitted, not required: any already-documented producer that predates
	// this field simply does not send it, and it defaults to false — a
	// known simplification for this round, matching the existing path_id
	// prefix convention (see README's Integration section).
	Fragile bool `json:"fragile"`
	// GiftWrap is an optional packing hint set by wes-work-planning at
	// work-enqueue time — a caller-stated request that this order's
	// package be gift-wrapped, not a product classification. It is
	// omitted, not required, and never published as explicit false: any
	// producer that does not carry a gift-wrap request for the order
	// simply omits the field, and it defaults to false (see ADR-0011).
	GiftWrap bool `json:"gift_wrap"`
}

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
	Reader     *kafkago.Reader
	CreateTask *usecases.CreateTask
	Processed  ports.ProcessedEvents
	Catalogue  ports.PathCatalogue
	Logger     *slog.Logger
	DeadLetter DeadLetterSink
	// testReader, when set, replaces Reader for ReadMessage (unit tests of
	// the broker-outage recovery in Run).
	testReader messageReader
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
func NewConsumer(brokers []string, topic string, createTask *usecases.CreateTask, processed ports.ProcessedEvents, catalogue ports.PathCatalogue, logger *slog.Logger) *Consumer {
	return newConsumer(brokers, topic, "fulfillment-execution", kafkago.FirstOffset, createTask, processed, catalogue, logger)
}

// NewConsumerWithGroup constructs an isolated Consumer. Its first assignment
// begins at the latest offset, so a system-test database is populated only by
// events released after the test's service process is ready.
func NewConsumerWithGroup(brokers []string, topic, groupID string, createTask *usecases.CreateTask, processed ports.ProcessedEvents, catalogue ports.PathCatalogue, logger *slog.Logger) *Consumer {
	return newConsumer(brokers, topic, groupID, kafkago.LastOffset, createTask, processed, catalogue, logger)
}

func newConsumer(brokers []string, topic, groupID string, startOffset int64, createTask *usecases.CreateTask, processed ports.ProcessedEvents, catalogue ports.PathCatalogue, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: startOffset,
	})
	return &Consumer{Reader: reader, CreateTask: createTask, Processed: processed, Catalogue: catalogue, Logger: logger}
}

// Run reads and handles messages until ctx is cancelled or the reader
// returns a fatal error. A message that fails Handle (which itself
// retries in-process — see Handle's doc comment) is published to
// DeadLetter (when configured — see the Consumer type doc comment) with
// the failure's error message attached, then the loop continues; Kafka's
// consumer-group offset is already advanced by the time Handle returns
// (ReadMessage auto-commits), so a message that is ultimately
// dead-lettered is never redelivered — this is a durable record for
// alerting/replay, not a mechanism to avoid losing the offset. A failure
// to publish to DeadLetter itself is logged separately and does NOT stop
// the loop — a broker blip on the DLQ publish must not wedge the main
// consumer, which is the whole point of this feature.
func (c *Consumer) Run(ctx context.Context) error {
	policy := newRecoverBackoff()
	for {
		msg, ok := readRecovering(ctx, c.reader(), policy, c.Logger, c.topic())
		if !ok {
			return nil
		}
		if err := c.Handle(ctx, msg); err != nil {
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
		}
	}
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

// handleMessageWithRetry retries HandleMessage up to maxHandlerAttempts
// times with jittered exponential backoff, bounded by ctx's own
// deadline/cancellation.
//
// It does NOT simply wrap HandleMessage whole: HandleMessage's first
// step, MarkProcessed, claims the CloudEvents id exactly once (idempotency gate)
// and is not safe to re-enter after it has already returned isNew=true
// — a second call for the same id always reports isNew=false, so
// naively retrying the WHOLE of HandleMessage would make attempt 2
// silently report success without ever creating a Task, the moment a
// LATER step (Catalogue.Lookup or CreateTask.Execute) merely blipped.
// So the claim happens exactly once, up front, via its own small retry
// (transient Processed-store errors ARE safely retryable — the claim
// has not yet succeeded, so retrying it cannot double-effect anything);
// only once isNew is confirmed true does the retryable, event-creating
// work (handleClaimedEvent) get its own up-to-maxHandlerAttempts
// retries.
func (c *Consumer) handleMessageWithRetry(ctx context.Context, raw []byte) error {
	env, ok, err := decodeWorkReleased(raw)
	if err != nil || !ok {
		return err
	}

	isNew, err := c.markProcessedWithRetry(ctx, env.EventId)
	if err != nil {
		return fmt.Errorf("kafka: mark processed: %w", err)
	}
	if !isNew {
		// Already applied by a prior delivery of this CloudEvents id; ack
		// without creating a duplicate Task.
		return nil
	}

	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxHandlerAttempts-1), ctx)

	return backoff.Retry(func() error {
		return c.handleClaimedEvent(ctx, env)
	}, bounded)
}

// markProcessedWithRetry retries ONLY the MarkProcessed claim itself
// (a transient Processed-store error), up to maxHandlerAttempts
// attempts — safe to retry in isolation because, until it returns
// isNew=true, no event-creating work has happened yet for this
// CloudEvents id.
func (c *Consumer) markProcessedWithRetry(ctx context.Context, eventId string) (bool, error) {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxHandlerAttempts-1), ctx)

	return backoff.RetryNotifyWithData(func() (bool, error) {
		return c.Processed.MarkProcessed(ctx, eventId)
	}, bounded, nil)
}

// handleClaimedEvent performs the actual, non-idempotent work for a
// WorkReleased event ALREADY claimed by markProcessedWithRetry (isNew
// was true) — the retryable portion of message handling. It is safe to
// call more than once for the SAME env only because the caller
// (handleMessageWithRetry) guarantees it is only ever entered after a
// single successful claim, i.e. genuine retries of a transient
// Catalogue/CreateTask failure, never a redelivery racing a fresh
// MarkProcessed claim.
func (c *Consumer) handleClaimedEvent(ctx context.Context, env workReleased) error {
	pathDef, err := c.Catalogue.Lookup(env.Data.PathId)
	if err != nil {
		// A path_id this catalogue does not recognize is a hard error —
		// NOT a silent default to task.Pick. The old prefix-guessing
		// convention (documented as a "known simplification" that this
		// catalogue retires) meant a malformed path_id quietly became a
		// Pick task; that was a real, acknowledged bug, not a feature.
		return fmt.Errorf("kafka: path_id %q not found in the process-path catalogue: %w", env.Data.PathId, err)
	}

	taskType := task.Type(pathDef.Id)
	required := shared.NewCapabilitySet(capabilitiesOf(pathDef)...)
	orderRef := shared.OrderRef(env.Data.WorkUnitId)

	if _, err := c.CreateTask.Execute(ctx, taskType, shared.NewCPT(env.Data.CPT), orderRef, required, env.Data.Fragile, env.Data.GiftWrap); err != nil {
		return fmt.Errorf("kafka: create task: %w", err)
	}
	return nil
}

// HandleMessage decodes raw as a CloudEvents 1.0 structured-mode event
// (see decodeWorkReleased) and, if it is a not-yet-processed WorkReleased
// event, creates a Task via CreateTask. It is exported separately from
// Handle/Run so tests can feed it a raw CloudEvent without a live broker.
// It performs exactly ONE attempt at each step (no retry) — Handle is
// what wraps the retryable portion (see handleMessageWithRetry's doc
// comment for why the claim and the retryable work must not share a
// single retry loop).
func (c *Consumer) HandleMessage(ctx context.Context, raw []byte) error {
	env, ok, err := decodeWorkReleased(raw)
	if err != nil || !ok {
		return err
	}

	isNew, err := c.Processed.MarkProcessed(ctx, env.EventId)
	if err != nil {
		return fmt.Errorf("kafka: mark processed: %w", err)
	}
	if !isNew {
		// Already applied by a prior delivery of this CloudEvents id; ack without
		// creating a duplicate Task.
		return nil
	}

	return c.handleClaimedEvent(ctx, env)
}

// capabilitiesOf converts a catalogue path definition's declared
// capability strings into the domain's shared.Capability type.
func capabilitiesOf(def pathcatalog.PathDefinition) []shared.Capability {
	out := make([]shared.Capability, len(def.RequiredCapabilities))
	for i, c := range def.RequiredCapabilities {
		out[i] = shared.Capability(c)
	}
	return out
}

// reader returns the read seam: the test hook when set, else Reader.
func (c *Consumer) reader() messageReader {
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
