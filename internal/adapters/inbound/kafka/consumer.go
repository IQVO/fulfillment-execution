// Package kafka provides the inbound adapter that consumes WorkReleased
// events from Work Planning and turns each one into a Task via the existing
// CreateTask use case — the intended use of that use case, so it is called
// directly rather than through a new one.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/pathcatalog"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
	"github.com/claudioed/fulfillment-execution/internal/observability"
)

// Envelope is the CloudEvents-like wrapper shared across all four
// warehouse-systems services. It is also the normalized internal shape
// that decodeEnvelope produces from EITHER wire shape this consumer
// accepts (see decodeEnvelope) — everything downstream of decode keys off
// this struct and is unaware of which wire shape a message arrived in.
type Envelope struct {
	EventId    string           `json:"event_id"`
	EventType  string           `json:"event_type"`
	OccurredAt time.Time        `json:"occurred_at"`
	Source     string           `json:"source"`
	Data       WorkReleasedData `json:"data"`
}

// cloudEventsProbe is decoded first, from every message, solely to
// discriminate the new CloudEvents 1.0 structured envelope (ADR-0027 /
// wes-work-planning ADR-0021, Phase 2 dual-read) from today's flat
// platform envelope. A non-empty specversion is the ONLY thing that picks
// the CloudEvents decode path — the field is deliberately never added to
// the flat envelope (see ADR-0027's design decision #3), so this
// discriminator cannot go ambiguous.
type cloudEventsProbe struct {
	Specversion string `json:"specversion"`
}

// cloudEventsEnvelope is the CloudEvents 1.0 structured-mode shape from
// ADR-0027's schema table. data is byte-identical to the flat envelope's
// data — WorkReleasedData is not duplicated or changed by this shape.
type cloudEventsEnvelope struct {
	Specversion     string           `json:"specversion"`
	Id              string           `json:"id"`
	Type            string           `json:"type"`
	Source          string           `json:"source"`
	Subject         string           `json:"subject"`
	Time            time.Time        `json:"time"`
	Datacontenttype string           `json:"datacontenttype"`
	Data            WorkReleasedData `json:"data"`
}

// cloudEventsTypePrefix is the reverse-DNS prefix wes-work-planning's
// WorkReleased CloudEvents "type" field carries on the wire (ADR-0027's
// field-mapping table; also already documented, unimplemented until this
// migration, in wes-work-planning's own apis/asyncapi.yaml). Stripping it
// back to the bare event name is what lets the existing, unchanged
// switch/case-style dispatch in HandleMessage keep working unmodified for
// either wire shape.
const cloudEventsTypePrefix = "com.warehouse.wes.work-planning.workunit."

// bareEventType strips a known CloudEvents reverse-DNS type prefix back to
// the bare event name (e.g. "WorkReleased") the existing dispatch logic
// already keys on. A type that doesn't carry the expected prefix falls
// back to stripping everything up to the last '.', and failing that,
// returns the value unchanged — HandleMessage's own "not WorkReleased, so
// ignore" check is what actually guards against an unrecognized value, so
// this never needs to itself decide validity.
func bareEventType(t string) string {
	if strings.HasPrefix(t, cloudEventsTypePrefix) {
		return strings.TrimPrefix(t, cloudEventsTypePrefix)
	}
	if i := strings.LastIndex(t, "."); i >= 0 {
		return t[i+1:]
	}
	return t
}

// decodeEnvelope accepts EITHER today's flat platform envelope or the new
// CloudEvents 1.0 structured envelope on the same topic (ADR-0027 Phase 2
// dual-read), discriminated solely by the presence of a specversion field,
// and normalizes either shape to the same Envelope representation the
// rest of this file's (unchanged) handling logic already expects. A
// message that fails to decode under either shape — including one
// carrying an unrecognized/malformed specversion value — fails exactly
// the same way a malformed flat message always has here: a wrapped error
// returned to the caller (Handle/Run then log it and route it to the
// dead-letter sink when configured, per the Consumer type doc comment),
// never a panic.
func decodeEnvelope(raw []byte) (Envelope, error) {
	var probe cloudEventsProbe
	if err := json.Unmarshal(raw, &probe); err != nil {
		return Envelope{}, fmt.Errorf("kafka: decode envelope: %w", err)
	}

	if probe.Specversion == "" {
		var env Envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			return Envelope{}, fmt.Errorf("kafka: decode envelope: %w", err)
		}
		return env, nil
	}

	if probe.Specversion != "1.0" {
		return Envelope{}, fmt.Errorf("kafka: unsupported CloudEvents specversion %q", probe.Specversion)
	}

	var ce cloudEventsEnvelope
	if err := json.Unmarshal(raw, &ce); err != nil {
		return Envelope{}, fmt.Errorf("kafka: decode envelope: %w", err)
	}
	return Envelope{
		EventId:    ce.Id,
		EventType:  bareEventType(ce.Type),
		OccurredAt: ce.Time,
		Source:     ce.Source,
		Data:       ce.Data,
	}, nil
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

// Consumer reads WorkReleased events off warehouse.work-planning.events and
// creates a Task for each one, exactly once per event_id despite Kafka's
// at-least-once delivery.
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
// returns a fatal error. A message that fails Handle is published to
// DeadLetter (when configured — see the Consumer type doc comment) with
// the failure's error message attached, then the loop continues; this
// consumer makes exactly one processing attempt per message (Kafka's
// consumer-group offset is already advanced by the time Handle returns,
// so there is no in-process retry to exhaust — every handling failure is
// treated as non-retryable here, the same "log and move on" decision
// ADR-0004 already made, now with the message preserved instead of
// dropped). A failure to publish to DeadLetter itself is logged
// separately and does NOT stop the loop — a broker blip on the DLQ
// publish must not wedge the main consumer, which is the whole point of
// this feature.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if err := c.Handle(ctx, msg); err != nil {
			c.Logger.ErrorContext(ctx, "kafka message handling failed", "error", err)
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

	if err := c.HandleMessage(ctx, msg.Value); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// HandleMessage decodes raw as either the flat platform envelope or the
// new CloudEvents 1.0 structured envelope (ADR-0027 Phase 2 dual-read —
// see decodeEnvelope) and, if it is a not-yet-processed WorkReleased
// event, creates a Task via CreateTask. It is exported separately from Run
// so tests can feed it a fake envelope without a live broker.
func (c *Consumer) HandleMessage(ctx context.Context, raw []byte) error {
	env, err := decodeEnvelope(raw)
	if err != nil {
		return err
	}
	if env.EventType != "WorkReleased" {
		return nil
	}

	isNew, err := c.Processed.MarkProcessed(ctx, env.EventId)
	if err != nil {
		return fmt.Errorf("kafka: mark processed: %w", err)
	}
	if !isNew {
		// Already applied by a prior delivery of this event_id; ack without
		// creating a duplicate Task.
		return nil
	}

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

// capabilitiesOf converts a catalogue path definition's declared
// capability strings into the domain's shared.Capability type.
func capabilitiesOf(def pathcatalog.PathDefinition) []shared.Capability {
	out := make([]shared.Capability, len(def.RequiredCapabilities))
	for i, c := range def.RequiredCapabilities {
		out[i] = shared.Capability(c)
	}
	return out
}
