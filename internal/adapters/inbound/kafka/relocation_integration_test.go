//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
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

// relocationTaskCompleted is the TaskCompleted data inventory-storage's
// consumer decodes (its taskCompletedData, verbatim field names and types).
type relocationTaskCompleted struct {
	TaskID        string `json:"task_id"`
	TaskType      string `json:"task_type"`
	WorkUnitID    string `json:"work_unit_id"`
	OrderRef      string `json:"order_ref"`
	LineNo        *int   `json:"line_no"`
	SKU           string `json:"sku"`
	FromBin       string `json:"from_bin"`
	ToBin         string `json:"to_bin"`
	Quantity      *int   `json:"quantity"`
	RelocationRef string `json:"relocation_ref"`
}

// relocationRig is the real infrastructure of the end-to-end test: Kafka and
// Postgres containers, the running WorkReleased consumer, the outbox relay
// and the use cases an operator drives.
type relocationRig struct {
	brokers  []string
	topic    string
	tasks    *postgres.TaskRepo
	relay    *postgres.OutboxRelay
	claim    *usecases.ClaimNext
	complete *usecases.CompleteTask
}

func newRelocationRig(t *testing.T, ctx context.Context) relocationRig {
	t.Helper()
	kcontainer, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("fe-relocate-itest"))
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
	newCEId := func() string { ceId++; return fmt.Sprintf("ce-rel-%d", ceId) }
	pub := postgres.NewOutboxPublisher(pool, outboundkafka.NewPublisherWithWriter(nil, tasks, stations, newCEId))
	relaySink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = relaySink.Close() })

	clock := fixedClockAt(time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC))
	n := 0
	newId := func() shared.TaskId { n++; return shared.TaskId(fmt.Sprintf("t-rel-%d", n)) }
	create := &usecases.CreateTask{Tasks: tasks, Publisher: pub, Clock: clock, NewId: newId, UnitOfWork: uow}
	apply := &usecases.ApplyWorkReleased{CreateTask: create, Processed: postgres.NewProcessedEventsRepo(pool), Catalogue: testCatalogue(), UnitOfWork: uow}

	rig := relocationRig{
		brokers:  brokers,
		topic:    fmt.Sprintf("warehouse.work-planning.events.relocate-itest-%d", time.Now().UnixNano()),
		tasks:    tasks,
		relay:    postgres.NewOutboxRelay(pool, relaySink, logger),
		claim:    &usecases.ClaimNext{Tasks: tasks, Stations: stations, Publisher: pub, Clock: clock, UnitOfWork: uow},
		complete: &usecases.CompleteTask{Tasks: tasks, Publisher: pub, Clock: fixedClockAt(time.Date(2026, 10, 14, 9, 3, 0, 0, time.UTC)), UnitOfWork: uow},
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

	if err := stations.Save(ctx, station.New("station-rel", shared.NewCapabilitySet("relocate"))); err != nil {
		t.Fatalf("save station: %v", err)
	}
	return rig
}

// waitForRelocateTask polls Postgres until the consumer created the task.
func (r relocationRig) waitForRelocateTask(t *testing.T, ctx context.Context) *task.Task {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		list, err := r.tasks.FindClaimableByType(ctx, task.Relocate, time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC))
		if err != nil {
			t.Fatalf("find claimable: %v", err)
		}
		if len(list) == 1 {
			return list[0]
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("the WorkReleased consumer never created the RELOCATE task")
	return nil
}

// readTaskCompleted reads warehouse.fulfillment.events until the
// TaskCompleted CloudEvent of taskId arrives and returns its data.
func (r relocationRig) readTaskCompleted(t *testing.T, ctx context.Context, taskId shared.TaskId) relocationTaskCompleted {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: r.brokers, Topic: outboundkafka.Topic, GroupID: fmt.Sprintf("relocate-assert-%d", time.Now().UnixNano()), StartOffset: kafkago.FirstOffset})
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
		var data relocationTaskCompleted
		if err := json.Unmarshal(e.Data(), &data); err != nil {
			t.Fatalf("unmarshal TaskCompleted data: %v", err)
		}
		return data
	}
	t.Fatal("TaskCompleted never reached the wire")
	return relocationTaskCompleted{}
}

// TestWorkReleasedRelocate_TaskCompletesAndTheMoveReachesTheWire is the
// end-to-end proof of ADR 0047 on real infrastructure (testcontainers Kafka +
// Postgres, the real consumer, UnitOfWork, outbox and relay):
//
//  1. a WorkReleased on the RELOCATE path carrying data.relocation (a Vacate:
//     the destination is open) is published on the work-planning topic;
//  2. the consumer creates a RELOCATE task with the move, persisted in
//     Postgres (migration 0019);
//  3. an operator claims it and completes it with the counted quantity and
//     the scanned reserve bin;
//  4. the relay puts TaskCompleted on warehouse.fulfillment.events with the
//     additive sku, from_bin, to_bin, quantity and relocation_ref fields,
//     decoded exactly the way inventory-storage decodes them.
func TestWorkReleasedRelocate_TaskCompletesAndTheMoveReachesTheWire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	rig := newRelocationRig(t, ctx)

	producer := &kafkago.Writer{Addr: kafkago.TCP(rig.brokers...), Topic: rig.topic, RequiredAcks: kafkago.RequireAll}
	t.Cleanup(func() { _ = producer.Close() })
	msg := kafkago.Message{
		Key: []byte("slot-plan-7-move-3"),
		Value: workReleasedCE("evt-rel-e2e", inboundkafka.TypeWorkReleased, "slot-plan-7-move-3", map[string]any{
			"path_id":      "RELOCATE",
			"work_unit_id": "slot-plan-7-move-3",
			"cpt":          "2026-10-14T18:00:00Z",
			"ref":          "plan-7",
			"relocation":   map[string]any{"plan_id": "plan-7", "move_seq": 3, "kind": "Vacate", "sku": "SKU-1", "from_bin": "WH1-FWD-A01-01-03-A"},
		}),
	}
	if err := producer.WriteMessages(ctx, msg); err != nil {
		t.Fatalf("publish WorkReleased: %v", err)
	}

	created := rig.waitForRelocateTask(t, ctx)
	want := task.RelocationMove{PlanId: "plan-7", MoveSeq: 3, Kind: task.RelocationKindVacate, SKU: "SKU-1", FromBin: "WH1-FWD-A01-01-03-A"}
	if created.Relocation() == nil || *created.Relocation() != want {
		t.Fatalf("persisted move = %+v, want %+v", created.Relocation(), want)
	}

	claimed, err := rig.claim.Execute(ctx, "station-rel", task.Relocate)
	if err != nil || claimed == nil {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	if err := rig.complete.Execute(ctx, claimed.Id(), "station-rel"); err == nil {
		t.Fatal("a RELOCATE task must not complete without the operator's confirmation")
	}
	confirm := task.RelocationConfirmation{Quantity: 24, ToBin: "WH1-RSV-B02-01-01-A"}
	if err := rig.complete.ExecuteWith(ctx, claimed.Id(), "station-rel", confirm); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if _, err := rig.relay.RelayOnce(ctx); err != nil {
		t.Fatalf("relay: %v", err)
	}

	got := rig.readTaskCompleted(t, ctx, claimed.Id())
	if got.TaskType != "RELOCATE" || got.WorkUnitID != "slot-plan-7-move-3" || got.SKU != "SKU-1" ||
		got.FromBin != "WH1-FWD-A01-01-03-A" || got.ToBin != "WH1-RSV-B02-01-01-A" || got.RelocationRef != "plan-7:3" {
		t.Fatalf("TaskCompleted data = %+v", got)
	}
	if got.Quantity == nil || *got.Quantity != 24 {
		t.Fatalf("quantity = %v, want 24", got.Quantity)
	}
	if got.OrderRef != "" || got.LineNo != nil {
		t.Fatalf("relocation work carries no order_ref/line_no, got %q/%v", got.OrderRef, got.LineNo)
	}
}
