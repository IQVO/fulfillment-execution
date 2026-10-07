//go:build integration

package kafka_test

import (
	"context"
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
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// TestProductClassified_FromKafkaToSealPackage is the end-to-end proof of
// ADR-0039 on real infrastructure (testcontainers Kafka + Postgres, the real
// consumer, UnitOfWork, local copy and SealPackage):
//
//  1. product-master's ProductClassified events for two SKUs with
//     incompatible DOT hazard classes (1 and 3) land on the topic, plus an
//     ignored ProductRegistered, a non-CloudEvent and a stale version;
//  2. the consumer applies them to product_classification_copy;
//  3. SealPackage, reading the copy through the unchanged port, rejects
//     sealing both SKUs into one package (pack.ErrPackageSegregationViolation)
//     and still seals an unknown SKU next to a hazmat one (fail-open);
//  4. replaying the same CloudEvent creates no second effect.
func TestProductClassified_FromKafkaToSealPackage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	kcontainer, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("fe-classified-itest"))
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(kcontainer) })
	brokers, err := kcontainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}

	pool := transferDB(t)
	store := productclassificationcopy.NewPostgresStore(pool)
	uow := postgres.NewUnitOfWork(pool)
	apply := &usecases.ApplyProductClassified{Processed: postgres.NewProcessedEventsRepo(pool), Copy: store, UnitOfWork: uow}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	topic := fmt.Sprintf("%s.itest-%d", inboundkafka.ProductMasterTopic, time.Now().UnixNano())
	createTestTopic(t, ctx, brokers[0], topic)

	group := fmt.Sprintf("fe-product-classification-itest-%d", time.Now().UnixNano())
	consumer, err := inboundkafka.NewProductClassifiedConsumer(brokers, topic, group, apply, logger)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })
	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()
	t.Cleanup(func() {
		runCancel()
		<-done
	})

	producer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, RequiredAcks: kafkago.RequireAll, BatchTimeout: 10 * time.Millisecond}
	t.Cleanup(func() { _ = producer.Close() })

	class1 := kafkago.Message{Key: []byte("SKU-EXPLOSIVE"), Value: productClassifiedCE("ce-pm-1", "SKU-EXPLOSIVE", map[string]any{
		"sku": "SKU-EXPLOSIVE", "handling_tags": []string{"Hazmat"}, "dot_hazard_class": 1, "classification_source": "native", "version": 2,
	})}
	registered := kafkago.Message{Key: []byte("SKU-EXPLOSIVE"), Value: cloudEvent("ce-pm-r", "/warehouse/product-master",
		"com.warehouse.wms.product-master.product.ProductRegistered", "SKU-EXPLOSIVE", time.Now().UTC(),
		"urn:warehouse:product-master:events:ProductRegistered:v1", map[string]any{"sku": "SKU-EXPLOSIVE", "description": "x", "version": 1})}
	notACloudEvent := kafkago.Message{Key: []byte("SKU-FLAMMABLE"), Value: []byte(`{"sku":"SKU-FLAMMABLE","dot_hazard_class":1}`)}
	class3 := kafkago.Message{Key: []byte("SKU-FLAMMABLE"), Value: productClassifiedCE("ce-pm-2", "SKU-FLAMMABLE", map[string]any{
		"sku": "SKU-FLAMMABLE", "handling_tags": []string{"Hazmat", "TemperatureSensitive"}, "temperature_class": "Chilled",
		"dot_hazard_class": 3, "classification_source": "legacy-import", "version": 5,
	})}
	stale := kafkago.Message{Key: []byte("SKU-FLAMMABLE"), Value: productClassifiedCE("ce-pm-3", "SKU-FLAMMABLE", map[string]any{
		"sku": "SKU-FLAMMABLE", "handling_tags": []string{"Fragile"}, "version": 4,
	})}
	if err := producer.WriteMessages(ctx, registered, class1, notACloudEvent, class3, stale, class1); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Wait until the consumer group has committed past every message: the
	// copy then reflects the whole batch (including the skipped/ignored ones).
	waitForCommittedOffset(t, ctx, brokers[0], group, topic, 6)

	for sku, wantDot := range map[string]int{"SKU-EXPLOSIVE": 1, "SKU-FLAMMABLE": 3} {
		info, err := store.GetClassification(ctx, sku)
		if err != nil || !info.Known || !info.Hazmat || info.DOTHazardClass != wantDot {
			t.Fatalf("%s in the copy = %+v, %v; want Hazmat class %d (the stale v4 must not win)", sku, info, err, wantDot)
		}
	}
	var claims int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE event_id LIKE 'ce-pm-%'`).Scan(&claims); err != nil {
		t.Fatalf("count claims: %v", err)
	}
	if claims != 3 {
		t.Fatalf("processed claims = %d, want 3 (two applied, one stale; the replayed ce-pm-1 deduped, ProductRegistered ignored)", claims)
	}

	// SealPackage through the real Postgres repos, reading the copy.
	tasks := postgres.NewTaskRepo(pool)
	stations := postgres.NewStationRepo(pool)
	packages := postgres.NewPackageRepo(pool)
	publisher := events.NewBufferedPublisher()
	clock := fixedClockAt(time.Date(2026, 10, 6, 15, 0, 0, 0, time.UTC))
	n := 0
	newTaskId := func() shared.TaskId { n++; return shared.TaskId(fmt.Sprintf("t-pc-%d", n)) }
	create := &usecases.CreateTask{Tasks: tasks, Publisher: publisher, Clock: clock, NewId: newTaskId, UnitOfWork: uow}
	claim := &usecases.ClaimNext{Tasks: tasks, Stations: stations, Publisher: publisher, Clock: clock, UnitOfWork: uow}
	if err := stations.Save(ctx, station.New("station-pc", shared.NewCapabilitySet("pack"))); err != nil {
		t.Fatalf("save station: %v", err)
	}
	if err := stations.Save(ctx, station.New("station-pc2", shared.NewCapabilitySet("pack"))); err != nil {
		t.Fatalf("save station 2: %v", err)
	}
	p := 0
	seal := &usecases.SealPackage{
		Tasks: tasks, Packages: packages, Publisher: publisher, Clock: clock,
		NewId:                func() shared.PackageId { p++; return shared.PackageId(fmt.Sprintf("p-pc-%d", p)) },
		ClassificationLookup: store,
		UnitOfWork:           uow,
	}
	claimPack := func(orderRef string, stationId shared.StationId) shared.TaskId {
		t.Helper()
		if _, err := create.Execute(ctx, task.Pack, shared.NewCPT(clock.Now().Add(time.Hour)), shared.OrderRef(orderRef), shared.NewCapabilitySet("pack"), false, false); err != nil {
			t.Fatalf("create pack task: %v", err)
		}
		claimed, err := claim.Execute(ctx, stationId, task.Pack)
		if err != nil || claimed == nil {
			t.Fatalf("claim: %v %v", claimed, err)
		}
		return claimed.Id()
	}

	incompatible := claimPack("order-incompatible", "station-pc")
	if _, err := seal.Execute(ctx, incompatible, "station-pc", []string{"SKU-EXPLOSIVE", "SKU-FLAMMABLE"}); !errors.Is(err, pack.ErrPackageSegregationViolation) {
		t.Fatalf("seal of class 1 + class 3 = %v, want ErrPackageSegregationViolation", err)
	}

	// An SKU the copy does not know is unclassified (fail-open) and seals
	// next to a hazmat item; the package routes to the hazmat lane.
	mixed := claimPack("order-mixed", "station-pc2")
	sealed, err := seal.Execute(ctx, mixed, "station-pc2", []string{"SKU-FLAMMABLE", "SKU-UNKNOWN"})
	if err != nil {
		t.Fatalf("seal with an unknown SKU: %v", err)
	}
	if got := sealed.ScannedHazardClasses(); len(got) != 1 || got[0] != 3 {
		t.Fatalf("hazard classes = %v, want [3]", got)
	}
	if sealed.SortLane() != pack.SortLaneHazmat {
		t.Fatalf("sort lane = %s, want %s", sealed.SortLane(), pack.SortLaneHazmat)
	}
}

// waitForCommittedOffset polls the group's committed offset for partition 0
// of topic until it reaches want.
func waitForCommittedOffset(t *testing.T, ctx context.Context, broker, group, topic string, want int64) {
	t.Helper()
	client := &kafkago.Client{Addr: kafkago.TCP(broker)}
	deadline := time.Now().Add(90 * time.Second)
	var last int64 = -1
	for time.Now().Before(deadline) {
		resp, err := client.OffsetFetch(ctx, &kafkago.OffsetFetchRequest{GroupID: group, Topics: map[string][]int{topic: {0}}})
		if err == nil {
			for _, p := range resp.Topics[topic] {
				if p.Partition == 0 {
					last = p.CommittedOffset
				}
			}
			if last >= want {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("group %s committed offset %d on %s, want %d", group, last, topic, want)
}
