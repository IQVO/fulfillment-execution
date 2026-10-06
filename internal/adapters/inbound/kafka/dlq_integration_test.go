//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// TestRun_PoisonMessage_GoesToDeadLetterTopicAndLoopContinues is the
// end-to-end proof for the DLQ escape hatch ADR-0004 flagged as missing: a
// message that fails processing (here, an unknown path_id — see Handle's
// doc comment — and a legacy flat-envelope message that is not a valid
// CloudEvent, ADR-0032) must land on <topic>.dlq instead of being silently dropped
// after a log line, AND the consumer's main loop must keep going and
// process the next, valid message rather than getting wedged.
func TestRun_PoisonMessage_GoesToDeadLetterTopicAndLoopContinues(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("fulfillment-execution-dlq-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.work-planning.events.dlq-itest-%d", time.Now().UnixNano())
	dlqTopic := kafka.DeadLetterTopic(topic)
	createTestTopic(t, ctx, brokers[0], topic)
	createTestTopic(t, ctx, brokers[0], dlqTopic)

	producer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	t.Cleanup(func() { _ = producer.Close() })

	// A poison message (unknown path_id -> Handle returns an error), then a
	// well-formed one, so the test proves both halves: the poison message
	// is dead-lettered, and the good one right after it still gets
	// processed -- the loop was never blocked.
	poison := kafkago.Message{
		Key:   []byte("poison"),
		Value: workReleasedCE("evt-poison", kafka.TypeWorkReleased, "order-poison", map[string]any{"path_id": "NOT-A-REAL-PATH", "work_unit_id": "order-poison", "cpt": "2026-01-01T12:00:00Z"}),
	}
	good := kafkago.Message{
		Key:   []byte("good"),
		Value: workReleasedCE("evt-good", kafka.TypeWorkReleased, "order-good", map[string]any{"path_id": "PICK", "work_unit_id": "order-good", "cpt": "2026-01-01T12:00:00Z"}),
	}
	// The retired flat envelope is a deterministic poison message too
	// (ADR-0032): dead-lettered, never parsed into a Task.
	legacyFlat := kafkago.Message{
		Key:   []byte("legacy-flat"),
		Value: []byte(`{"event_id":"evt-flat","event_type":"WorkReleased","occurred_at":"2026-01-01T11:00:00Z","source":"wes-work-planning","data":{"path_id":"PICK","work_unit_id":"order-flat","cpt":"2026-01-01T12:00:00Z"}}`),
	}
	if err := producer.WriteMessages(ctx, poison, legacyFlat, good); err != nil {
		t.Fatalf("seed publish: %v", err)
	}

	tasks := memory.NewTaskRepo()
	apply := newApplyWorkReleased(tasks, memory.NewProcessedEventsRepo(), testCatalogue())

	consumer := kafka.NewConsumer(brokers, topic, apply, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { _ = consumer.Close() })

	dlqWriter := &kafkago.Writer{Addr: kafkago.TCP(brokers...)}
	t.Cleanup(func() { _ = dlqWriter.Close() })
	consumer.DeadLetter = dlqWriter

	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()
	t.Cleanup(func() {
		runCancel()
		<-done
	})

	// Assert the poison message shows up on the DLQ topic.
	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: dlqTopic, GroupID: fmt.Sprintf("dlq-assert-%d", time.Now().UnixNano()), StartOffset: kafkago.FirstOffset})
	t.Cleanup(func() { _ = dlqReader.Close() })
	dlqCtx, dlqCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dlqCancel()
	wantDLQ := []string{"poison", "legacy-flat"}
	for _, wantKey := range wantDLQ {
		dlqMsg, err := dlqReader.ReadMessage(dlqCtx)
		if err != nil {
			t.Fatalf("expected the %q message on the dead-letter topic, got error: %v", wantKey, err)
		}
		if string(dlqMsg.Key) != wantKey {
			t.Fatalf("expected key %q on the DLQ, got %q", wantKey, dlqMsg.Key)
		}
		foundErrorHeader := false
		for _, h := range dlqMsg.Headers {
			if h.Key == "x-dlq-error" {
				foundErrorHeader = true
			}
		}
		if !foundErrorHeader {
			t.Fatalf("expected x-dlq-error header on the dead-lettered message, got headers %+v", dlqMsg.Headers)
		}
	}

	// Assert the loop was not blocked: the good message right after the
	// poison one still got processed into a Task.
	deadline := time.Now().Add(30 * time.Second)
	for {
		n, err := tasks.CountByTypeAndStatus(ctx, task.Pick, task.Pending)
		if err != nil {
			t.Fatalf("CountByTypeAndStatus: %v", err)
		}
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected the good message after the poison one to still be processed into a Task; got %d pending PICK tasks", n)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func createTestTopic(t *testing.T, ctx context.Context, broker, topic string) {
	t.Helper()
	conn, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatalf("dial Kafka controller: %v", err)
	}
	defer func() { _ = conn.Close() }()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("find Kafka controller: %v", err)
	}
	controllerConn, err := kafkago.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		t.Fatalf("dial Kafka controller %s:%d: %v", controller.Host, controller.Port, err)
	}
	defer func() { _ = controllerConn.Close() }()
	if err := controllerConn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) == 1 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for topic %s leader: %v", topic, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatalf("topic %s leader was not ready before timeout", topic)
}
