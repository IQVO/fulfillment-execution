package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// Encoded is one wire-ready Kafka message produced by an Encoder. Topic is
// carried explicitly (rather than on the kafkago.Message) because the
// outbox relay's writer has no fixed topic: kafka-go rejects a message
// with Topic set when the Writer also has one, and vice versa, so the
// fixed-topic Publisher/AnalyticsPublisher writers keep their Topic and
// leave the message's empty, while RelaySink applies Encoded.Topic per
// message onto its topic-less writer.
type Encoded struct {
	Topic     string
	EventType string
	Key       []byte
	Value     []byte
	Headers   []kafkago.Header
}

// message converts enc into a kafkago.Message WITHOUT a Topic, for a
// writer that has one configured.
func (enc Encoded) message() kafkago.Message {
	return kafkago.Message{Key: enc.Key, Value: enc.Value, Headers: enc.Headers}
}

// Encoder turns domain events into their Kafka wire form without sending
// them. Both Publisher and AnalyticsPublisher implement it, so the
// transactional outbox (ADR 0020) can persist exactly the bytes each
// publisher would have written — enrichment lookups included — inside the
// use case's transaction, and the relay later sends them verbatim.
type Encoder interface {
	Encode(ctx context.Context, events ...shared.DomainEvent) ([]Encoded, error)
}

// Sender is where already-encoded messages go; RelaySink is the production
// implementation.
type Sender interface {
	Send(ctx context.Context, msgs ...Encoded) error
}

// RelaySink writes Encoded messages onto whatever topic each one names,
// through a single topic-less kafkago.Writer. It is the Kafka-facing half
// of the outbox relay: the OutboxRelay drains rows, RelaySink puts them on
// the wire.
type RelaySink struct {
	Writer Writer
}

// NewRelaySink constructs a RelaySink over a topic-less writer on brokers.
func NewRelaySink(brokers []string) *RelaySink {
	return &RelaySink{
		Writer: &kafkago.Writer{
			BatchTimeout: syncWriterBatchTimeout,
			RequiredAcks: syncWriterRequiredAcks,
			Addr:         kafkago.TCP(brokers...),
			// Balancer is kafkago.Hash, not LeastBytes: see the comment
			// on NewPublisher/NewAnalyticsPublisher — the
			// relay forwards each Encoded's already-set aggregate-id
			// Key verbatim, and LeastBytes would silently ignore it for
			// partition routing.
			Balancer:               &kafkago.Hash{},
			AllowAutoTopicCreation: true,
		},
	}
}

// NewRelaySinkWithWriter constructs a RelaySink over an explicit Writer
// (tests substitute a fake here).
func NewRelaySinkWithWriter(w Writer) *RelaySink {
	return &RelaySink{Writer: w}
}

// Send writes msgs in one WriteMessages call, each addressed to its own
// Encoded.Topic. An Encoded without a Topic is rejected before anything is
// written, since a topic-less writer cannot route it.
func (s *RelaySink) Send(ctx context.Context, msgs ...Encoded) error {
	if len(msgs) == 0 {
		return nil
	}
	out := make([]kafkago.Message, 0, len(msgs))
	for _, m := range msgs {
		if m.Topic == "" {
			return fmt.Errorf("kafka: relay message %s has no topic", m.EventType)
		}
		km := m.message()
		km.Topic = m.Topic
		out = append(out, km)
	}
	if err := s.Writer.WriteMessages(ctx, out...); err != nil {
		return fmt.Errorf("kafka: relay %d message(s): %w", len(out), err)
	}
	return nil
}

// Close releases the underlying Kafka writer.
func (s *RelaySink) Close() error {
	if w, ok := s.Writer.(*kafkago.Writer); ok {
		return w.Close()
	}
	return nil
}

// Compile-time assertions.
var (
	_ Encoder = (*Publisher)(nil)
	_ Encoder = (*AnalyticsPublisher)(nil)
	_ Sender  = (*RelaySink)(nil)
)
