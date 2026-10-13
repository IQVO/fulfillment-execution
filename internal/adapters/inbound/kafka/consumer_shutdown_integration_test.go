//go:build integration

package kafka_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// parkingUnitOfWork parks the WorkReleased application until the handler's
// context is cancelled, then reports that cancellation -- exactly what the
// Postgres UnitOfWork does when the pod is told to stop mid-transaction.
type parkingUnitOfWork struct{ entered chan struct{} }

func (u *parkingUnitOfWork) Execute(ctx context.Context, _ func(ctx context.Context) error) error {
	select {
	case u.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

// shutdownLogs is a goroutine-safe log sink.
type shutdownLogs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *shutdownLogs) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *shutdownLogs) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestRun_ShutdownMidHandle_IsCleanDeadLettersNothingAndRedelivers pins the
// shutdown contract of the WorkReleased consumer: cancelling Run's context
// while a healthy WorkReleased is being applied is a clean stop (Run returns
// nil), the message is NOT dead-lettered, and -- because its offset was never
// committed -- the next consumer in the same group receives it again and
// creates the Task (the CloudEvents-id claim makes the replay safe).
//
// Before the fix the consumer read with ReadMessage, which commits the offset
// BEFORE handling, and routed the retry wrapper's cancellation error into the
// dead-letter path as if the message were poison: the healthy WorkReleased was
// either dead-lettered or, when the DLQ publish failed on the cancelled
// context, lost for good.
func TestRun_ShutdownMidHandle_IsCleanDeadLettersNothingAndRedelivers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("fe-shutdown-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}

	topic := fmt.Sprintf("warehouse.work-planning.events.shutdown-itest-%d", time.Now().UnixNano())
	dlqTopic := kafka.DeadLetterTopic(topic)
	createTestTopic(t, ctx, brokers[0], topic)
	createTestTopic(t, ctx, brokers[0], dlqTopic)

	producer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	t.Cleanup(func() { _ = producer.Close() })
	healthy := kafkago.Message{
		Key: []byte("order-shutdown"),
		Value: workReleasedCE("evt-shutdown", kafka.TypeWorkReleased, "order-shutdown",
			map[string]any{"path_id": "PICK", "work_unit_id": "order-shutdown", "cpt": "2026-01-01T12:00:00Z"}),
	}
	publishDeadline := time.Now().Add(30 * time.Second)
	for {
		err := producer.WriteMessages(ctx, healthy)
		if err == nil {
			break
		}
		if time.Now().After(publishDeadline) {
			t.Fatalf("publish healthy WorkReleased: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}

	dlqWriter := &kafkago.Writer{Addr: kafkago.TCP(brokers...)}
	t.Cleanup(func() { _ = dlqWriter.Close() })

	// First process: parks on the message and is shut down mid-handle.
	uow := &parkingUnitOfWork{entered: make(chan struct{}, 1)}
	logs := &shutdownLogs{}
	parked := kafka.NewConsumer(brokers, topic, &usecases.ApplyWorkReleased{
		Processed:  memory.NewProcessedEventsRepo(),
		Catalogue:  testCatalogue(),
		UnitOfWork: uow,
	}, slog.New(slog.NewTextHandler(logs, nil)))
	parked.DeadLetter = dlqWriter

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- parked.Run(runCtx) }()

	select {
	case <-uow.entered:
	case <-time.After(60 * time.Second):
		t.Fatalf("the consumer never started handling the message; logs:\n%s", logs.String())
	}
	runCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run after its context was cancelled mid-handle = %v, want nil (clean shutdown)", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consumer did not stop after context cancellation")
	}
	_ = parked.Close()

	if out := logs.String(); strings.Contains(out, "dead-letter") {
		t.Errorf("a healthy message only interrupted by shutdown was routed to the dead-letter path:\n%s", out)
	}
	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: dlqTopic, Partition: 0, MinBytes: 1, MaxBytes: 10e6})
	t.Cleanup(func() { _ = dlqReader.Close() })
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readCancel()
	if dead, err := dlqReader.ReadMessage(readCtx); err == nil {
		t.Fatalf("a healthy message was dead-lettered on shutdown: key=%q headers=%v", string(dead.Key), dead.Headers)
	}

	// Second process, same consumer group: the uncommitted WorkReleased is
	// redelivered and becomes a Task -- nothing was lost by the shutdown.
	tasks := memory.NewTaskRepo()
	restarted := kafka.NewConsumer(brokers, topic, newApplyWorkReleased(tasks, memory.NewProcessedEventsRepo(), testCatalogue()),
		slog.New(slog.NewTextHandler(logs, nil)))
	restarted.DeadLetter = dlqWriter
	t.Cleanup(func() { _ = restarted.Close() })
	restartCtx, restartCancel := context.WithCancel(ctx)
	restartDone := make(chan error, 1)
	go func() { restartDone <- restarted.Run(restartCtx) }()
	t.Cleanup(func() {
		restartCancel()
		<-restartDone
	})

	deadline := time.Now().Add(60 * time.Second)
	for {
		n, err := tasks.CountByTypeAndStatus(ctx, task.Pick, task.Pending)
		if err != nil {
			t.Fatalf("CountByTypeAndStatus: %v", err)
		}
		if n == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the WorkReleased interrupted by shutdown was never redelivered (pending PICK tasks = %d): its offset was committed before it was handled", n)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
