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

// pickPlacementRig is the real infrastructure of the pick-placement
// end-to-end test (ADR 0062): Kafka and Postgres containers, the running
// WorkReleased consumer, the outbox relay and the claim use case — the same
// shape as the relocation rig.
type pickPlacementRig struct {
	brokers []string
	topic   string
	tasks   *postgres.TaskRepo
	relay   *postgres.OutboxRelay
	claim   *usecases.ClaimNext
}

func newPickPlacementRig(t *testing.T, ctx context.Context) pickPlacementRig {
	t.Helper()
	kcontainer, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("fe-pickplace-itest"))
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
	newCEId := func() string { ceId++; return fmt.Sprintf("ce-pk-%d", ceId) }
	pub := postgres.NewOutboxPublisher(pool, outboundkafka.NewPublisherWithWriter(nil, tasks, stations, newCEId))
	relaySink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = relaySink.Close() })

	at := time.Date(2026, 11, 6, 9, 0, 0, 0, time.UTC)
	clock := fixedClockAt(at)
	n := 0
	newId := func() shared.TaskId { n++; return shared.TaskId(fmt.Sprintf("t-pk-%d", n)) }
	create := &usecases.CreateTask{Tasks: tasks, Publisher: pub, Clock: clock, NewId: newId, UnitOfWork: uow}
	apply := &usecases.ApplyWorkReleased{CreateTask: create, Processed: postgres.NewProcessedEventsRepo(pool), Catalogue: testCatalogue(), UnitOfWork: uow}

	rig := pickPlacementRig{
		brokers: brokers,
		topic:   fmt.Sprintf("warehouse.work-planning.events.pickplace-itest-%d", time.Now().UnixNano()),
		tasks:   tasks,
		relay:   postgres.NewOutboxRelay(pool, relaySink, logger),
		claim:   &usecases.ClaimNext{Tasks: tasks, Stations: stations, Publisher: pub, Clock: clock, UnitOfWork: uow},
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

	if err := stations.Save(ctx, station.New("station-pick", shared.NewCapabilitySet("pick"))); err != nil {
		t.Fatalf("save station: %v", err)
	}
	return rig
}

// waitForPickTasks polls Postgres until the consumer created want claimable
// PICK tasks.
func (r pickPlacementRig) waitForPickTasks(t *testing.T, ctx context.Context, want int) []*task.Task {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		list, err := r.tasks.FindClaimableByType(ctx, task.Pick, time.Date(2026, 11, 6, 9, 0, 0, 0, time.UTC))
		if err != nil {
			t.Fatalf("find claimable: %v", err)
		}
		if len(list) == want {
			return list
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("the WorkReleased consumer never created %d PICK tasks", want)
	return nil
}

// poolWireMessage is one decoded message of warehouse.fulfillment.events.
type poolWireMessage struct {
	Type    string
	Subject string
	Key     string
	Schema  string
	Data    map[string]any
}

// readWire reads exactly n messages off warehouse.fulfillment.events and
// returns them decoded (the relay published n rows; nothing else writes the
// topic in this rig, so the read is deterministic).
func (r pickPlacementRig) readWire(t *testing.T, ctx context.Context, n int) []poolWireMessage {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: r.brokers, Topic: outboundkafka.Topic, GroupID: fmt.Sprintf("pickplace-assert-%d", time.Now().UnixNano()), StartOffset: kafkago.FirstOffset})
	t.Cleanup(func() { _ = reader.Close() })
	out := make([]poolWireMessage, 0, n)
	for len(out) < n {
		rctx, rcancel := context.WithTimeout(ctx, 30*time.Second)
		m, err := reader.ReadMessage(rctx)
		rcancel()
		if err != nil {
			t.Fatalf("read wire message %d of %d: %v", len(out)+1, n, err)
		}
		e, err := cloudevents.Decode(m.Value)
		if err != nil {
			t.Fatalf("message %d is not a CloudEvent: %v", len(out), err)
		}
		var data map[string]any
		if len(e.Data()) > 0 {
			if err := json.Unmarshal(e.Data(), &data); err != nil {
				t.Fatalf("unmarshal data of %s: %v", e.Type(), err)
			}
		}
		out = append(out, poolWireMessage{Type: e.Type(), Subject: e.Subject(), Key: string(m.Key), Schema: e.DataSchema(), Data: data})
	}
	return out
}

// availableOf returns the TaskAvailable of taskId from the wire messages.
func availableOf(t *testing.T, msgs []poolWireMessage, taskId string) poolWireMessage {
	t.Helper()
	for _, m := range msgs {
		if m.Type == "com.warehouse.wes.fulfillment-execution.task.TaskAvailable" && m.Subject == taskId {
			return m
		}
	}
	t.Fatalf("TaskAvailable of %s never reached the wire among %d messages", taskId, len(msgs))
	return poolWireMessage{}
}

// TestWorkReleasedPick_PlacementReachesThePoolAndIsClaimable is the
// end-to-end proof of ADR 0062 on real infrastructure (testcontainers Kafka
// + Postgres, the real consumer, UnitOfWork, outbox and relay):
//
//  1. a WorkReleased on the PICK path whose pick object carries BOTH
//     site_id and from_bin (the producer's pinned additive contract) and a
//     second one whose pick object predates them (no site, no bin) are
//     published on the work-planning topic;
//  2. the consumer creates both PICK tasks in Postgres (migration 0025
//     persists the site);
//  3. after one relay pass the sited pick's TaskAvailable is on
//     warehouse.fulfillment.events with the plan's site and the pick face
//     (from_location and to_location); the legacy pick produced none —
//     byte-identical to the pre-ADR behavior, it never reaches the wire;
//  4. ClaimNext, unchanged, hands out the sited pick first (earlier CPT)
//     and the legacy pick after it.
func TestWorkReleasedPick_PlacementReachesThePoolAndIsClaimable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	rig := newPickPlacementRig(t, ctx)

	producer := &kafkago.Writer{Addr: kafkago.TCP(rig.brokers...), Topic: rig.topic, RequiredAcks: kafkago.RequireAll}
	t.Cleanup(func() { _ = producer.Close() })

	// The legacy release FIRST (its outbox rows precede the sited one's, so
	// a TaskAvailable for it could never hide after the reader stops).
	legacy := kafkago.Message{
		Key: []byte("ORD-9-line-1"),
		Value: workReleasedCE("evt-pk-legacy", inboundkafka.TypeWorkReleased, "ORD-9-line-1", map[string]any{
			"path_id": "PICK", "work_unit_id": "ORD-9-line-1",
			"cpt": "2026-11-06T12:00:00Z", "ref": "ORD-9",
			"pick": map[string]any{"sku": "SKU-9", "quantity": 2, "unit": "EACH"},
		}),
	}
	if err := producer.WriteMessages(ctx, legacy); err != nil {
		t.Fatalf("publish legacy WorkReleased: %v", err)
	}
	sited := kafkago.Message{
		Key: []byte("ORD-9-line-2"),
		Value: workReleasedCE("evt-pk-sited", inboundkafka.TypeWorkReleased, "ORD-9-line-2", map[string]any{
			"path_id": "PICK", "work_unit_id": "ORD-9-line-2",
			"cpt": "2026-11-06T11:00:00Z", "ref": "ORD-9",
			"pick": map[string]any{"sku": "SKU-9", "quantity": 3, "unit": "EACH", "site_id": "WH1", "from_bin": "A-01-01"},
		}),
	}
	if err := producer.WriteMessages(ctx, sited); err != nil {
		t.Fatalf("publish sited WorkReleased: %v", err)
	}

	created := rig.waitForPickTasks(t, ctx, 2)
	var legacyTask, sitedTask *task.Task
	for _, tk := range created {
		if tk.OrderRef() == "ORD-9-line-2" {
			sitedTask = tk
		} else {
			legacyTask = tk
		}
	}
	if sitedTask == nil || legacyTask == nil {
		t.Fatalf("created tasks = %v", created)
	}
	if plan := sitedTask.PickPlan(); plan == nil || plan.SiteId != "WH1" || plan.FromBin != "A-01-01" {
		t.Fatalf("sited plan = %+v, want site WH1 and pick face A-01-01", sitedTask.PickPlan())
	}
	if place, ok := sitedTask.Placement(); !ok || place.SiteId != "WH1" || place.FromLocation != "A-01-01" {
		t.Fatalf("sited placement = (%+v, %v)", place, ok)
	}
	if plan := legacyTask.PickPlan(); plan == nil || plan.SiteId != "" || plan.FromBin != "" {
		t.Fatalf("legacy plan = %+v, want no site and no bin", legacyTask.PickPlan())
	}
	if _, ok := legacyTask.Placement(); ok {
		t.Fatal("the legacy pick must carry no placement")
	}

	n, err := rig.relay.RelayOnce(ctx)
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	// This rig wires the integration encoder only (like the relocation rig):
	// the sited pick's TaskAvailable is the ONE row the pass can publish —
	// the legacy pick produced none, and the analytics rows have no encoder
	// here.
	if n != 1 {
		t.Fatalf("expected exactly 1 relayed row (the sited pick's TaskAvailable), got %d", n)
	}
	wire := rig.readWire(t, ctx, n)
	available := availableOf(t, wire, string(sitedTask.Id()))
	if available.Schema != "urn:warehouse:fulfillment-execution:events:TaskAvailable:v1" || available.Key != string(sitedTask.Id()) {
		t.Fatalf("schema/key = %q/%q", available.Schema, available.Key)
	}
	for k, want := range map[string]string{
		"task_id": string(sitedTask.Id()), "task_type": "PICK", "site_id": "WH1",
		"from_location": "A-01-01", "to_location": "A-01-01",
	} {
		if got, _ := available.Data[k].(string); got != want {
			t.Errorf("TaskAvailable.%s = %v, want %q", k, available.Data[k], want)
		}
	}
	for _, m := range wire {
		if m.Type == "com.warehouse.wes.fulfillment-execution.task.TaskAvailable" && m.Subject == string(legacyTask.Id()) {
			t.Fatalf("the legacy pick must not be announced, found TaskAvailable on the wire: %v", m.Data)
		}
	}

	// ClaimNext is unchanged: earliest CPT first (the sited pick), then the
	// legacy one — both claimable exactly as before.
	first, err := rig.claim.Execute(ctx, "station-pick", task.Pick)
	if err != nil || first == nil {
		t.Fatalf("claim: %v %v", first, err)
	}
	if first.Id() != sitedTask.Id() {
		t.Fatalf("first claim = %s, want the earlier-CPT sited pick %s", first.Id(), sitedTask.Id())
	}
	second, err := rig.claim.Execute(ctx, "station-pick", task.Pick)
	if err != nil || second == nil {
		t.Fatalf("claim: %v %v", second, err)
	}
	if second.Id() != legacyTask.Id() {
		t.Fatalf("second claim = %s, want the legacy pick %s", second.Id(), legacyTask.Id())
	}
}
