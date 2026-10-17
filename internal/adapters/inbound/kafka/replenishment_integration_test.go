//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// replenishmentTaskCompleted is the TaskCompleted data inventory-storage's
// consumer decodes for a REPLENISH completion (its taskCompletedData, verbatim
// field names and types, request_ref included).
type replenishmentTaskCompleted struct {
	TaskID     string `json:"task_id"`
	TaskType   string `json:"task_type"`
	WorkUnitID string `json:"work_unit_id"`
	OrderRef   string `json:"order_ref"`
	LineNo     *int   `json:"line_no"`
	SKU        string `json:"sku"`
	FromBin    string `json:"from_bin"`
	ToBin      string `json:"to_bin"`
	Quantity   *int   `json:"quantity"`
	RequestRef string `json:"request_ref"`
	Relocation string `json:"relocation_ref"`
}

// newReplenishmentRig wires the same real infrastructure as the RELOCATE test
// (Kafka + Postgres, the running consumer, the outbox relay) with a station
// that holds the replenish capability.
func newReplenishmentRig(t *testing.T, ctx context.Context) relocationRig {
	t.Helper()
	kcontainer, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("fe-replenish-itest"))
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(kcontainer) })
	brokers, err := kcontainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}

	pool := transferDB(t)
	tasks := postgres.NewTaskRepo(pool)
	stations := postgres.NewStationRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ceId := 0
	newCEId := func() string { ceId++; return fmt.Sprintf("ce-rep-%d", ceId) }
	pub := postgres.NewOutboxPublisher(pool, outboundkafka.NewPublisherWithWriter(nil, tasks, stations, newCEId))
	relaySink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = relaySink.Close() })

	clock := fixedClockAt(time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC))
	n := 0
	newId := func() shared.TaskId { n++; return shared.TaskId(fmt.Sprintf("t-rep-%d", n)) }
	create := &usecases.CreateTask{Tasks: tasks, Publisher: pub, Clock: clock, NewId: newId, UnitOfWork: uow}
	apply := &usecases.ApplyWorkReleased{CreateTask: create, Processed: postgres.NewProcessedEventsRepo(pool), Catalogue: testCatalogue(), UnitOfWork: uow}

	rig := relocationRig{
		brokers:  brokers,
		topic:    fmt.Sprintf("warehouse.work-planning.events.replenish-itest-%d", time.Now().UnixNano()),
		tasks:    tasks,
		relay:    postgres.NewOutboxRelay(pool, relaySink, logger),
		claim:    &usecases.ClaimNext{Tasks: tasks, Stations: stations, Publisher: pub, Clock: clock, UnitOfWork: uow},
		complete: &usecases.CompleteTask{Tasks: tasks, Publisher: pub, Clock: fixedClockAt(time.Date(2026, 10, 17, 9, 3, 0, 0, time.UTC)), UnitOfWork: uow},
	}
	createTestTopic(t, ctx, brokers[0], rig.topic)
	createTestTopic(t, ctx, brokers[0], outboundkafka.Topic)

	consumer := inboundkafka.NewConsumer(brokers, rig.topic, apply, logger)
	t.Cleanup(func() { _ = consumer.Close() })
	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()
	t.Cleanup(func() {
		runCancel()
		<-done
	})

	// Two stations: one with replenish (and the forklift the path requires),
	// one without, which must never be handed the task.
	if err := stations.Save(ctx, station.New("station-rep", shared.NewCapabilitySet("replenish", "forklift"))); err != nil {
		t.Fatalf("save station: %v", err)
	}
	if err := stations.Save(ctx, station.New("station-plain", shared.NewCapabilitySet("pick"))); err != nil {
		t.Fatalf("save station: %v", err)
	}
	return rig
}

func (r relocationRig) waitForReplenishTask(t *testing.T, ctx context.Context) *task.Task {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		list, err := r.tasks.FindClaimableByType(ctx, task.Replenish, time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC))
		if err != nil {
			t.Fatalf("find claimable: %v", err)
		}
		if len(list) == 1 {
			return list[0]
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("the WorkReleased consumer never created the REPLENISH task")
	return nil
}

// readReplenishmentCompleted reads warehouse.fulfillment.events until the
// TaskCompleted CloudEvent of taskId arrives, bounded by a deadline.
func (r relocationRig) readReplenishmentCompleted(t *testing.T, ctx context.Context, taskId shared.TaskId) replenishmentTaskCompleted {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: r.brokers, Topic: outboundkafka.Topic, GroupID: fmt.Sprintf("replenish-assert-%d", time.Now().UnixNano()), StartOffset: kafkago.FirstOffset})
	t.Cleanup(func() { _ = reader.Close() })
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
		m, err := reader.ReadMessage(rctx)
		rcancel()
		if err != nil {
			continue
		}
		e, err := cloudevents.Decode(m.Value)
		if err != nil || e.Type() != "com.warehouse.wes.fulfillment-execution.task.TaskCompleted" || e.Subject() != string(taskId) {
			continue
		}
		if e.DataSchema() != "urn:warehouse:fulfillment-execution:events:TaskCompleted:v1" || string(m.Key) != string(taskId) {
			t.Fatalf("dataschema/key = %q/%q", e.DataSchema(), m.Key)
		}
		var data replenishmentTaskCompleted
		if err := json.Unmarshal(e.Data(), &data); err != nil {
			t.Fatalf("unmarshal TaskCompleted data: %v", err)
		}
		return data
	}
	t.Fatal("TaskCompleted never reached the wire")
	return replenishmentTaskCompleted{}
}

// TestWorkReleasedReplenish_TaskCompletesAndTheMoveReachesTheWire is the
// end-to-end proof of ADR 0050 on real infrastructure (testcontainers Kafka +
// Postgres, the real consumer, UnitOfWork, outbox and relay):
//
//  1. a WorkReleased on the REPLENISH path carrying data.replenishment is
//     published on the work-planning topic;
//  2. the consumer creates a REPLENISH task with the move, persisted in
//     Postgres (migration 0020);
//  3. a station without the replenish capability is never given it; one with
//     it claims and completes it with the counted quantity (below the plan);
//  4. the relay puts TaskCompleted on warehouse.fulfillment.events with sku,
//     from_bin, to_bin, the confirmed quantity and request_ref, decoded the
//     way inventory-storage decodes them.
func TestWorkReleasedReplenish_TaskCompletesAndTheMoveReachesTheWire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	rig := newReplenishmentRig(t, ctx)

	rig.publishRelease(t, ctx)
	created := rig.waitForReplenishTask(t, ctx)
	want := task.ReplenishmentMove{RequestId: "req-9", SKU: "SKU-1", FromBin: "WH1-RSV-AMB-B02-03-02-C", ToBin: "WH1-FWD-AMB-A01-01-01-A", Quantity: 24, Priority: task.ReplenishmentPriorityUrgent}
	if created.Replenishment() == nil || *created.Replenishment() != want {
		t.Fatalf("persisted move = %+v, want %+v", created.Replenishment(), want)
	}

	claimed := rig.claimAndComplete(t, ctx)
	if _, err := rig.relay.RelayOnce(ctx); err != nil {
		t.Fatalf("relay: %v", err)
	}
	assertReplenishmentWire(t, rig.readReplenishmentCompleted(t, ctx, claimed.Id()))
}

// publishRelease puts the WorkReleased of replenishment request req-9 on the
// work-planning topic.
func (r relocationRig) publishRelease(t *testing.T, ctx context.Context) {
	t.Helper()
	producer := &kafkago.Writer{Addr: kafkago.TCP(r.brokers...), Topic: r.topic, RequiredAcks: kafkago.RequireAll}
	t.Cleanup(func() { _ = producer.Close() })
	msg := kafkago.Message{
		Key: []byte("replenish-req-9"),
		Value: workReleasedCE("evt-rep-e2e", inboundkafka.TypeWorkReleased, "replenish-req-9", map[string]any{
			"path_id":      "REPLENISH",
			"work_unit_id": "replenish-req-9",
			"cpt":          "2026-10-17T18:00:00Z",
			"ref":          "req-9",
			"replenishment": map[string]any{
				"request_id": "req-9", "sku": "SKU-1",
				"from_bin": "WH1-RSV-AMB-B02-03-02-C", "to_bin": "WH1-FWD-AMB-A01-01-01-A",
				"quantity": 24, "priority": "URGENT",
			},
		}),
	}
	if err := producer.WriteMessages(ctx, msg); err != nil {
		t.Fatalf("publish WorkReleased: %v", err)
	}
}

// claimAndComplete proves the capability matching (a station without
// replenish never gets the task), then claims and completes it with the
// operator's count, which is below the plan.
func (r relocationRig) claimAndComplete(t *testing.T, ctx context.Context) *task.Task {
	t.Helper()
	if got, err := r.claim.Execute(ctx, "station-plain", task.Replenish); got != nil || !errors.Is(err, usecases.ErrNoClaimableTask) {
		t.Fatalf("a station without the replenish capability must not get the task, got %v err=%v", got, err)
	}
	claimed, err := r.claim.Execute(ctx, "station-rep", task.Replenish)
	if err != nil || claimed == nil {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	if err := r.complete.Execute(ctx, claimed.Id(), "station-rep"); err == nil {
		t.Fatal("a REPLENISH task must not complete without the operator's confirmation")
	}
	if err := r.complete.ExecuteWith(ctx, claimed.Id(), "station-rep", task.RelocationConfirmation{Quantity: 20}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	return claimed
}

func assertReplenishmentWire(t *testing.T, got replenishmentTaskCompleted) {
	t.Helper()
	if got.TaskType != "REPLENISH" || got.WorkUnitID != "replenish-req-9" || got.SKU != "SKU-1" ||
		got.FromBin != "WH1-RSV-AMB-B02-03-02-C" || got.ToBin != "WH1-FWD-AMB-A01-01-01-A" || got.RequestRef != "req-9" {
		t.Fatalf("TaskCompleted data = %+v", got)
	}
	if got.Quantity == nil || *got.Quantity != 20 {
		t.Fatalf("quantity = %v, want the confirmed 20", got.Quantity)
	}
	if got.Relocation != "" || got.OrderRef != "" || got.LineNo != nil {
		t.Fatalf("replenishment work carries no relocation_ref/order_ref/line_no, got %q/%q/%v", got.Relocation, got.OrderRef, got.LineNo)
	}
}
