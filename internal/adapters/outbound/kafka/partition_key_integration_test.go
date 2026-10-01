//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	adapter "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// This file proves, against a REAL Kafka broker (Testcontainers), that
// every outbound Kafka writer in this package routes same-key messages
// onto the same partition. A fake-Writer unit test (see publisher_test.go/
// analytics_publisher_test.go) only proves Message.Key is SET correctly —
// it says nothing about the Writer's Balancer actually using that key to
// pick a partition, because the fake Writer never simulates partition
// routing at all.
//
// This fleet's Writers were all built with `Balancer: &kafkago.LeastBytes{}`
// (a load-balance-by-cumulative-bytes strategy that ignores Message.Key
// entirely, despite a correct, non-nil, per-aggregate Key being set on
// every message) until fixed here to `&kafkago.Hash{}` (FNV-1a over
// Message.Key). See order-management PR #111 for the reference fix and
// the fleet-ops skill's kafka-go-key-balancer-mismatch.md for the full
// writeup of the bug class. Every business topic runs at 8 partitions
// since warehouse-infra PR #42, so this class of bug silently breaks
// per-aggregate event ordering without any other visible symptom.

// startKafkaContainer starts a fresh, isolated broker for one test and
// returns its bootstrap brokers. Never sourced from KAFKA_BROKERS or
// localhost, so CI cannot silently skip this contract.
func startKafkaContainer(t *testing.T, clusterID string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(clusterID),
	)
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate Kafka container: %v", err)
		}
	})

	brokers, err := container.Brokers(context.Background())
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}
	return brokers
}

// createTopicWithPartitions creates topic with numPartitions on brokers and
// waits for it to be ready, so the writer under test never races topic
// creation. numPartitions=8 mirrors the Phase 3 partition scaleup
// (warehouse-infra PR #42) that exposed this bug fleet-wide.
func createTopicWithPartitions(t *testing.T, brokers []string, topic string, numPartitions int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial Kafka broker: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: numPartitions, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) >= numPartitions {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("Kafka topic %q never became ready with %d partitions", topic, numPartitions)
}

// readAllPartitionsOnce does ONE pass across every partition of topic and
// returns, for every message key observed, the partition it landed on and
// how many times that key was seen — a single full scan rather than one
// scan per key, so a test asserting on several keys stays fast. Per-
// partition reads use a bounded MaxWait (kafka-go's own batch-read
// timeout) plus a bounded context, so an empty partition never stalls the
// whole scan.
func readAllPartitionsOnce(t *testing.T, brokers []string, topic string, numPartitions int) map[string]map[int]int {
	t.Helper()
	byKey, _ := readAllPartitionsWithMessages(t, brokers, topic, numPartitions)
	return byKey
}

// readAllPartitionsWithMessages is readAllPartitionsOnce that also returns
// every message read, so a test can assert on the real wire format.
func readAllPartitionsWithMessages(t *testing.T, brokers []string, topic string, numPartitions int) (map[string]map[int]int, []kafkago.Message) {
	t.Helper()
	partitionOf := map[string]map[int]int{}
	var all []kafkago.Message
	for p := 0; p < numPartitions; p++ {
		reader := kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			Partition:   p,
			StartOffset: kafkago.FirstOffset,
			MaxWait:     500 * time.Millisecond,
		})
		func() {
			defer func() { _ = reader.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			for {
				msg, err := reader.ReadMessage(ctx)
				if err != nil {
					return // timeout/EOF: no more messages on this partition
				}
				all = append(all, msg)
				key := string(msg.Key)
				if partitionOf[key] == nil {
					partitionOf[key] = map[int]int{}
				}
				partitionOf[key][p]++
			}
		}()
	}
	return partitionOf, all
}

// assertCloudEventsOnTheWire fails t unless every message read off the real
// broker is a valid CloudEvents 1.0 event of the wantType, whose subject is
// the message key, carrying the structured-mode content-type header
// (ADR-0032).
func assertCloudEventsOnTheWire(t *testing.T, msgs []kafkago.Message, wantTypes ...string) {
	t.Helper()
	if len(msgs) == 0 {
		t.Fatal("no messages read off the broker")
	}
	allowed := map[string]bool{}
	for _, w := range wantTypes {
		allowed[w] = true
	}
	for i, m := range msgs {
		e, err := cloudevents.Decode(m.Value)
		if err != nil {
			t.Fatalf("message %d is not a valid CloudEvent: %v (%s)", i, err, m.Value)
		}
		if !allowed[e.Type()] {
			t.Fatalf("message %d: type = %q, want one of %v", i, e.Type(), wantTypes)
		}
		if e.Subject() != string(m.Key) {
			t.Fatalf("message %d: subject %q != key %q", i, e.Subject(), m.Key)
		}
		if !hasContentTypeHeader(m.Headers) {
			t.Fatalf("message %d: missing content-type header, got %v", i, m.Headers)
		}
	}
}

// assertSingleKeyPartition fails t unless key was observed on exactly one
// partition in byKey, with exactly wantCount messages there.
func assertSingleKeyPartition(t *testing.T, byKey map[string]map[int]int, key string, wantCount int) {
	t.Helper()
	partitions, ok := byKey[key]
	if !ok {
		t.Fatalf("no message observed with key %q; observed keys = %v", key, byKey)
	}
	if len(partitions) != 1 {
		t.Fatalf("messages for key %q landed on %d distinct partitions (%v), want exactly 1 — the Hash balancer must map a key to a single partition deterministically", key, len(partitions), partitions)
	}
	for p, count := range partitions {
		if count != wantCount {
			t.Fatalf("found %d message(s) for key %q on partition %d, want %d (all events for one aggregate must share a partition)", count, key, p, wantCount)
		}
	}
}

// TestPublisherKeysSameTaskCPTMissedOntoTheSamePartition is the real-Kafka
// guarantee behind the Publisher (publisher.go) fix: on an 8-partition
// topic, every TaskCPTMissed event for the SAME task id must land on the
// SAME partition (Key = TaskId, see encodeTaskCPTMissed), and an event for
// a DIFFERENT task id is free to land elsewhere. This fails under
// LeastBytes (messages scatter despite an identical key) and passes only
// with Hash.
func TestPublisherKeysSameTaskCPTMissedOntoTheSamePartition(t *testing.T) {
	brokers := startKafkaContainer(t, "fulfillment-execution-kafka-itest-publisher")
	const numPartitions = 8
	createTopicWithPartitions(t, brokers, adapter.Topic, numPartitions)

	ids := []string{"evt-1", "evt-2", "evt-3"}
	i := 0
	publisher := adapter.NewPublisher(brokers, nil, nil, func() string {
		id := ids[i]
		i++
		return id
	})
	t.Cleanup(func() { _ = publisher.Close() })

	occurredAt := time.Now().UTC().Truncate(time.Second)
	const sameTask = shared.TaskId("task-itest-same-partition")
	const otherTask = shared.TaskId("task-itest-other-partition")

	events := []shared.DomainEvent{
		shared.NewTaskCPTMissed(sameTask, "order-1", "PICK", occurredAt.Add(time.Hour), occurredAt),
		shared.NewTaskCPTMissed(sameTask, "order-1", "PICK", occurredAt.Add(2*time.Hour), occurredAt),
		shared.NewTaskCPTMissed(otherTask, "order-2", "PACK", occurredAt.Add(time.Hour), occurredAt),
	}
	ctx := context.Background()
	for _, e := range events {
		if err := publisher.Publish(ctx, e); err != nil {
			t.Fatalf("publish event: %v", err)
		}
	}

	byKey, msgs := readAllPartitionsWithMessages(t, brokers, adapter.Topic, numPartitions)
	assertSingleKeyPartition(t, byKey, string(sameTask), 2)
	assertSingleKeyPartition(t, byKey, string(otherTask), 1)
	assertCloudEventsOnTheWire(t, msgs, "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed")
}

// TestAnalyticsPublisherKeysSameTaskOntoTheSamePartition mirrors the above
// for AnalyticsPublisher (analytics_publisher.go) on the analytics topic:
// TaskCreated/TaskClaimed events for the same TaskId (Key, see
// marshalData) must land on the same partition of an 8-partition topic.
func TestAnalyticsPublisherKeysSameTaskOntoTheSamePartition(t *testing.T) {
	brokers := startKafkaContainer(t, "fulfillment-execution-kafka-itest-analytics")
	const numPartitions = 8
	createTopicWithPartitions(t, brokers, adapter.AnalyticsTopic, numPartitions)

	ids := []string{"evt-1", "evt-2", "evt-3"}
	i := 0
	publisher := adapter.NewAnalyticsPublisher(brokers, nil, func() string {
		id := ids[i]
		i++
		return id
	})
	t.Cleanup(func() { _ = publisher.Close() })

	occurredAt := time.Now().UTC().Truncate(time.Second)
	const sameTask = shared.TaskId("task-itest-analytics-same")
	const otherTask = shared.TaskId("task-itest-analytics-other")

	events := []shared.DomainEvent{
		shared.NewTaskCreated(sameTask, occurredAt),
		shared.NewTaskClaimed(sameTask, "station-1", occurredAt),
		shared.NewTaskCreated(otherTask, occurredAt),
	}
	ctx := context.Background()
	for _, e := range events {
		if err := publisher.Publish(ctx, e); err != nil {
			t.Fatalf("publish event: %v", err)
		}
	}

	byKey, msgs := readAllPartitionsWithMessages(t, brokers, adapter.AnalyticsTopic, numPartitions)
	assertSingleKeyPartition(t, byKey, string(sameTask), 2)
	assertSingleKeyPartition(t, byKey, string(otherTask), 1)
	assertCloudEventsOnTheWire(t, msgs,
		"com.warehouse.wes.fulfillment-execution.task.TaskCreated",
		"com.warehouse.wes.fulfillment-execution.task.TaskClaimed")
}

// TestRelaySinkKeysSamePackageOntoTheSamePartition mirrors the above for
// RelaySink (encoded.go), the outbox relay's Kafka-facing half: Encoded
// messages sharing a Key (here, PackageId) must land on the same
// partition of an 8-partition topic even though RelaySink's Writer is
// topic-less and each Encoded carries its own Topic.
func TestRelaySinkKeysSamePackageOntoTheSamePartition(t *testing.T) {
	brokers := startKafkaContainer(t, "fulfillment-execution-kafka-itest-relay")
	const numPartitions = 8
	topic := fmt.Sprintf("warehouse.fulfillment.events.itest-relay-%d", time.Now().UnixNano())
	createTopicWithPartitions(t, brokers, topic, numPartitions)

	sink := adapter.NewRelaySink(brokers)
	t.Cleanup(func() { _ = sink.Close() })

	const samePackage = "package-itest-same-partition"
	const otherPackage = "package-itest-other-partition"

	msgs := []adapter.Encoded{
		{Topic: topic, EventType: "PackageManifested", Key: []byte(samePackage), Value: []byte(`{"n":1}`)},
		{Topic: topic, EventType: "PackageManifested", Key: []byte(samePackage), Value: []byte(`{"n":2}`)},
		{Topic: topic, EventType: "PackageManifested", Key: []byte(otherPackage), Value: []byte(`{"n":3}`)},
	}
	if err := sink.Send(context.Background(), msgs...); err != nil {
		t.Fatalf("send relay messages: %v", err)
	}

	byKey := readAllPartitionsOnce(t, brokers, topic, numPartitions)
	assertSingleKeyPartition(t, byKey, samePackage, 2)
	assertSingleKeyPartition(t, byKey, otherPackage, 1)
}
