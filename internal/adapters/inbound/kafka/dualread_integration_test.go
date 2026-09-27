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
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// TestRun_FlatAndCloudEventsWithSameEventId_CreateTaskFiresExactlyOnce is
// the idempotency-safety test ADR-0027 (and its wes-work-planning
// companion, ADR-0021) calls out as load-bearing for the later dual-write
// phase: once a producer starts emitting BOTH a flat and a CloudEvents
// message for the same logical event (same event_id/id) on the same
// topic, this dual-read-capable consumer must still create exactly one
// Task, relying on the existing event_id-keyed idempotency gate
// (ports.ProcessedEvents) — no new dedup logic is added by this migration,
// this test proves the pre-existing gate already covers the new shape.
func TestRun_FlatAndCloudEventsWithSameEventId_CreateTaskFiresExactlyOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("fulfillment-execution-dualread-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.work-planning.events.dualread-itest-%d", time.Now().UnixNano())
	createTestTopic(t, ctx, brokers[0], topic)

	producer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	t.Cleanup(func() { _ = producer.Close() })

	const sharedEventId = "evt-dualread-shared"

	// Same event_id/id, same logical WorkReleased content, one flat
	// message and one CloudEvents message -- exactly the collision shape
	// ADR-0027's Phase 4/5 dual-write mode will produce for real once the
	// publishers grow EVENT_ENVELOPE_MODE=dual. This consumer must already
	// be safe against it today, per the ADR's phase-ordering rationale.
	flat := kafkago.Message{
		Key: []byte(sharedEventId),
		Value: []byte(`{
			"event_id": "` + sharedEventId + `",
			"event_type": "WorkReleased",
			"occurred_at": "2026-01-01T12:00:00Z",
			"source": "wes-work-planning",
			"data": {"path_id": "PICK", "work_unit_id": "order-dualread", "cpt": "2026-01-01T13:00:00Z", "ref": "release-1"}
		}`),
	}
	cloudEvent := kafkago.Message{
		Key: []byte(sharedEventId),
		Value: []byte(`{
			"specversion": "1.0",
			"id": "` + sharedEventId + `",
			"type": "com.warehouse.wes.work-planning.workunit.WorkReleased",
			"source": "/warehouse/wes-work-planning",
			"subject": "order-dualread",
			"time": "2026-01-01T12:00:00Z",
			"datacontenttype": "application/json",
			"data": {"path_id": "PICK", "work_unit_id": "order-dualread", "cpt": "2026-01-01T13:00:00Z", "ref": "release-1"}
		}`),
	}
	if err := producer.WriteMessages(ctx, flat, cloudEvent); err != nil {
		t.Fatalf("seed publish: %v", err)
	}

	tasks := memory.NewTaskRepo()
	createTask := &usecases.CreateTask{
		Tasks:     tasks,
		Publisher: events.NewBufferedPublisher(),
		Clock:     memory.NewFixedClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)),
		NewId:     idSeq("t"),
	}

	consumer := kafka.NewConsumer(brokers, topic, createTask, memory.NewProcessedEventsRepo(), testCatalogue(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { _ = consumer.Close() })

	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()
	t.Cleanup(func() {
		runCancel()
		<-done
	})

	// Poll until at least one message has been processed into a Task,
	// then hold for a further quiet window to prove the SECOND message
	// (whichever shape arrives second) is a no-op via the existing
	// event_id idempotency gate, not a second Task.
	deadline := time.Now().Add(30 * time.Second)
	for {
		n, err := tasks.CountByTypeAndStatus(ctx, task.Pick, task.Pending)
		if err != nil {
			t.Fatalf("CountByTypeAndStatus: %v", err)
		}
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected at least 1 pending PICK task from either message, got %d", n)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Give the consumer loop time to also process the second physical
	// message (both were written before Run started, so both are already
	// available to be read back-to-back).
	time.Sleep(3 * time.Second)

	n, err := tasks.CountByTypeAndStatus(ctx, task.Pick, task.Pending)
	if err != nil {
		t.Fatalf("CountByTypeAndStatus: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected CreateTask to fire exactly once for both messages sharing event_id %q, got %d pending PICK tasks", sharedEventId, n)
	}
}
