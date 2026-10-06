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

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// transferDB boots one throwaway Postgres (testcontainers, never an
// external DATABASE_URL), migrates it, and truncates every application
// table.
func transferDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("fulfillment_execution"),
		tcpostgres.WithUsername("fulfillment"),
		tcpostgres.WithPassword("fulfillment"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	if err := postgres.Migrate(url, "../../../../migrations"); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	truncateAllTables(t, pool)
	return pool
}

func truncateAllTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	rows, err := pool.Query(ctx, `SELECT quote_ident(tablename) FROM pg_tables WHERE schemaname = 'public' AND tablename <> 'schema_migrations'`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table: %v", err)
		}
		tables = append(tables, name)
	}
	for _, name := range tables {
		if _, err := pool.Exec(ctx, "TRUNCATE TABLE "+name+" RESTART IDENTITY"); err != nil {
			t.Fatalf("truncate %s: %v", name, err)
		}
	}
}

// TestWorkReleasedTransfer_TaskCompletesAndFactsReachTheWire is the
// end-to-end proof of the transfer-task-facts contract, all on real
// infrastructure (testcontainers Kafka + Postgres, the real consumer, the
// real UnitOfWork, the real outbox + relay):
//
//  1. a WorkReleased carrying the transfer block is published on the
//     work-planning topic;
//  2. the consumer turns it into a DISPATCH Task carrying the block
//     (persisted in Postgres, OrderRef = work_unit_id);
//  3. the task is claimed and completed through the real use cases,
//     inside the real UnitOfWork, publishing through the outbox;
//  4. the relay puts TaskCompleted AND exactly one TransferDispatched on
//     warehouse.fulfillment.events as CloudEvents with the exact type,
//     dataschema, subject/key = task_id and payload shape;
//  5. replaying the SAME WorkReleased (Kafka at-least-once redelivery)
//     creates no second task and no second fact.
func TestWorkReleasedTransfer_TaskCompletesAndFactsReachTheWire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	kcontainer, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("fe-transfer-itest"))
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
	processed := postgres.NewProcessedEventsRepo(pool)
	uow := postgres.NewUnitOfWork(pool)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ceId := 0
	newCEId := func() string { ceId++; return fmt.Sprintf("ce-tr-%d", ceId) }
	integration := outboundkafka.NewPublisherWithWriter(nil, tasks, stations, newCEId)
	pub := postgres.NewOutboxPublisher(pool, integration)
	relaySink := outboundkafka.NewRelaySink(brokers)
	t.Cleanup(func() { _ = relaySink.Close() })
	relay := postgres.NewOutboxRelay(pool, relaySink, logger)

	clock := fixedClockAt(time.Date(2026, 10, 6, 15, 4, 5, 0, time.UTC))
	n := 0
	newId := func() shared.TaskId { n++; return shared.TaskId(fmt.Sprintf("t-tr-%d", n)) }
	create := &usecases.CreateTask{Tasks: tasks, Publisher: pub, Clock: clock, NewId: newId, UnitOfWork: uow}
	claim := &usecases.ClaimNext{Tasks: tasks, Stations: stations, Publisher: pub, Clock: clock, UnitOfWork: uow}
	complete := &usecases.CompleteTask{Tasks: tasks, Publisher: pub, Clock: fixedClockAt(time.Date(2026, 10, 6, 15, 8, 5, 0, time.UTC)), UnitOfWork: uow}

	apply := &usecases.ApplyWorkReleased{CreateTask: create, Processed: processed, Catalogue: testCatalogue(), UnitOfWork: uow}

	topic := fmt.Sprintf("warehouse.work-planning.events.transfer-itest-%d", time.Now().UnixNano())
	createTestTopic(t, ctx, brokers[0], topic)
	createTestTopic(t, ctx, brokers[0], outboundkafka.Topic)

	consumer := inboundkafka.NewConsumer(brokers, topic, apply, logger)
	t.Cleanup(func() { _ = consumer.Close() })

	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()
	t.Cleanup(func() {
		runCancel()
		<-done
	})

	// Register a dispatch-capable station so ClaimNext has a claimer.
	if err := stations.Save(ctx, station.New("station-tr", shared.NewCapabilitySet("dispatch"))); err != nil {
		t.Fatalf("save station: %v", err)
	}

	// 1. Publish the transfer WorkReleased.
	producer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	t.Cleanup(func() { _ = producer.Close() })
	msg := kafkago.Message{
		Key: []byte("wu-tr-e2e"),
		Value: workReleasedCE("evt-tr-e2e", inboundkafka.TypeWorkReleased, "wu-tr-e2e", map[string]any{
			"path_id":      "dispatch-dock-1",
			"work_unit_id": "wu-tr-e2e",
			"cpt":          "2026-10-06T18:00:00Z",
			"ref":          "demand-77",
			"transfer_ref": "tr-9f2a",
			"work_kind":    "TRANSFER_DISPATCH",
			"site_id":      "site-north",
			"sku":          "SKU-0042",
			"quantity":     12,
		}),
	}
	if err := producer.WriteMessages(ctx, msg); err != nil {
		t.Fatalf("publish WorkReleased: %v", err)
	}

	// 2. Wait for the task to exist with the correlation block.
	deadline := time.Now().Add(60 * time.Second)
	var created *task.Task
	for time.Now().Before(deadline) {
		list, err := tasks.FindClaimableByType(ctx, task.Dispatch, clock.Now())
		if err != nil {
			t.Fatalf("find claimable: %v", err)
		}
		if len(list) == 1 {
			created = list[0]
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if created == nil {
		t.Fatal("the WorkReleased consumer never created the DISPATCH task")
	}
	tr := created.Transfer()
	if tr == nil || tr.TransferRef != "tr-9f2a" || tr.WorkKind != task.WorkKindTransferDispatch || tr.Quantity != 12 {
		t.Fatalf("persisted transfer block = %+v", tr)
	}
	if created.OrderRef() != shared.OrderRef("wu-tr-e2e") {
		t.Fatalf("orderRef = %q, want the work_unit_id", created.OrderRef())
	}

	// 3. Replay the SAME message before completing: no duplicate task.
	if err := producer.WriteMessages(ctx, msg); err != nil {
		t.Fatalf("republish WorkReleased: %v", err)
	}
	time.Sleep(3 * time.Second)
	list, _ := tasks.FindClaimableByType(ctx, task.Dispatch, clock.Now())
	if len(list) != 1 {
		t.Fatalf("redelivery created a second task: %d tasks", len(list))
	}

	// 4. Claim and complete through the real use cases.
	claimed, err := claim.Execute(ctx, "station-tr", task.Dispatch)
	if err != nil || claimed == nil {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	if err := complete.Execute(ctx, claimed.Id(), "station-tr"); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// 5. Drain the outbox onto the wire.
	if _, err := relay.RelayOnce(ctx); err != nil {
		t.Fatalf("relay: %v", err)
	}

	// 6. Read warehouse.fulfillment.events and assert the facts.
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: outboundkafka.Topic, GroupID: fmt.Sprintf("transfer-assert-%d", time.Now().UnixNano()), StartOffset: kafkago.FirstOffset})
	t.Cleanup(func() { _ = reader.Close() })

	var sawTaskCompleted, sawTransferDispatched bool
	var transferPayload map[string]any
	readDeadline := time.Now().Add(60 * time.Second)
	for !(sawTaskCompleted && sawTransferDispatched) && time.Now().Before(readDeadline) {
		rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
		m, rerr := reader.ReadMessage(rctx)
		rcancel()
		if rerr != nil {
			continue
		}
		e, derr := cloudevents.Decode(m.Value)
		if derr != nil {
			continue
		}
		switch e.Type() {
		case "com.warehouse.wes.fulfillment-execution.task.TaskCompleted":
			sawTaskCompleted = true
		case "com.warehouse.wes.fulfillment-execution.transfer.TransferDispatched":
			sawTransferDispatched = true
			if e.DataSchema() != "urn:warehouse:fulfillment-execution:events:TransferDispatched:v1" {
				t.Fatalf("dataschema = %q", e.DataSchema())
			}
			if e.Subject() != string(claimed.Id()) || string(m.Key) != string(claimed.Id()) {
				t.Fatalf("subject/key = %q/%q, want the task id", e.Subject(), m.Key)
			}
			if err := json.Unmarshal(e.Data(), &transferPayload); err != nil {
				t.Fatalf("unmarshal transfer payload: %v", err)
			}
		}
	}
	if !sawTaskCompleted {
		t.Fatal("TaskCompleted never reached the wire")
	}
	if !sawTransferDispatched {
		t.Fatal("TransferDispatched never reached the wire")
	}
	for k, want := range map[string]any{
		"transfer_ref": "tr-9f2a",
		"demand_id":    "demand-77",
		"work_unit_id": "wu-tr-e2e",
		"task_id":      string(claimed.Id()),
		"work_kind":    "TRANSFER_DISPATCH",
		"site_id":      "site-north",
		"sku":          "SKU-0042",
	} {
		if got := transferPayload[k]; got != want {
			t.Fatalf("payload[%s] = %v, want %v", k, got, want)
		}
	}
	if got := transferPayload["quantity"]; got != float64(12) {
		t.Fatalf("payload[quantity] = %v, want 12", got)
	}

	// 7. Relaying again publishes nothing new (no dup facts).
	if n, err := relay.RelayOnce(ctx); err != nil || n != 0 {
		t.Fatalf("second relay pass must be a no-op, got n=%d err=%v", n, err)
	}
}

func fixedClockAt(t time.Time) fixedWallClock { return fixedWallClock{t: t} }

type fixedWallClock struct{ t time.Time }

func (c fixedWallClock) Now() time.Time { return c.t }
