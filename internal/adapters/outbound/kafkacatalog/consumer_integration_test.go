//go:build integration

package kafkacatalog

import (
	"context"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
)

// TestNewConsumer_TwoInstancesInARow_BothReplayFully is the regression test
// for the shared-consumer-group bug: each process must replay the complete
// topic history into its own cache. It owns its broker via Testcontainers so
// the integration job runs this assertion rather than silently skipping it.
func TestNewConsumer_TwoInstancesInARow_BothReplayFully(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("fulfillment-execution-kafkacatalog-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.process-path-management.events.itest-%d", time.Now().UnixNano())
	createTopic(t, ctx, brokers[0], topic)

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	t.Cleanup(func() { _ = writer.Close() })
	seed := envelopeMsg(t, 0, 0, TypeProcessPathCreated, pathData{PathId: "ITEST", MatchPrefix: "itest", Direct: true, RequiredCapabilities: []string{"itest"}})
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Value:   seed.Value,
		Headers: []kafkago.Header{cloudevents.ContentTypeHeader()},
	}); err != nil {
		t.Fatalf("seed publish: %v", err)
	}

	target, err := newTargetOffsets(ctx, brokers, topic)
	if err != nil {
		t.Fatalf("newTargetOffsets: %v", err)
	}
	if len(target) == 0 {
		t.Fatal("expected at least one partition with a message")
	}

	// First instance consumes and commits. The second instance must use a
	// different group and still replay from FirstOffset.
	group1 := uniqueConsumerGroup()
	reader1 := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: group1, StartOffset: kafkago.FirstOffset})
	msg1, err := reader1.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("first reader ReadMessage: %v", err)
	}
	if err := reader1.CommitMessages(ctx, msg1); err != nil {
		t.Fatalf("first reader commit: %v", err)
	}
	if err := reader1.Close(); err != nil {
		t.Fatalf("first reader close: %v", err)
	}

	group2 := uniqueConsumerGroup()
	if group1 == group2 {
		t.Fatal("expected distinct consumer groups")
	}
	reader2 := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: group2, StartOffset: kafkago.FirstOffset})
	t.Cleanup(func() { _ = reader2.Close() })
	msg2, err := reader2.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("second independent reader failed to replay: %v", err)
	}
	if msg2.Offset != 0 {
		t.Fatalf("expected second instance to replay from offset 0, got %d", msg2.Offset)
	}

	// The replayed CloudEvents wire message must decode and populate the
	// catalogue exactly as the production Run loop would apply it.
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	if err := c.handle(msg2); err != nil {
		t.Fatalf("handle replayed CloudEvent: %v", err)
	}
	if _, err := c.Lookup("itest"); err != nil {
		t.Fatalf("expected ITEST in the catalogue after replay, got: %v", err)
	}
}

func createTopic(t *testing.T, ctx context.Context, broker, topic string) {
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
