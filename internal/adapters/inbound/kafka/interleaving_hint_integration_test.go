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

	inboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/interleavinghintcopy"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// hintIssuedCE builds one InterleavingHintIssued CloudEvent exactly as
// task-interleaving publishes it (its pinned AsyncAPI contract).
func hintIssuedCE(id, stationId, hintId string, expiresAt time.Time, taskIds ...string) []byte {
	candidates := make([]map[string]any, len(taskIds))
	for i, tid := range taskIds {
		candidates[i] = map[string]any{"rank": i + 1, "task_id": tid, "task_type": "PICK", "location": "WH1-FWD-A01-02-01-A", "detour_m": 4.5}
	}
	return cloudEvent(id, "/warehouse/task-interleaving", inboundkafka.TypeInterleavingHintIssued, stationId,
		time.Now().UTC(), "urn:warehouse:task-interleaving:events:InterleavingHintIssued:v1", map[string]any{
			"hint_id": hintId, "station_id": stationId, "site_id": "SITE-1",
			"after_task_id": "task-done", "origin_location": "WH1-FWD-A01-01-01-A",
			"expires_at": expiresAt.UTC().Format(time.RFC3339Nano), "policy_version": 2,
			"candidates": candidates,
		})
}

func startHintConsumer(t *testing.T, ctx context.Context, brokers []string, topic, group string, apply *usecases.ApplyInterleavingHint) (*inboundkafka.InterleavingHintConsumer, context.CancelFunc, chan error) {
	t.Helper()
	consumer, err := inboundkafka.NewInterleavingHintConsumer(brokers, topic, group, apply, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()
	return consumer, cancel, done
}

// TestInterleavingHint_FromKafkaToClaimNext is the end-to-end proof of ADR
// 0051 on real infrastructure (testcontainers Kafka + Postgres, the real
// consumer, UnitOfWork, hint copy and ClaimNext):
//
//  1. an InterleavingHintIssued lands on the topic and is stored (one ACTIVE
//     hint per station; a newer hint supersedes);
//  2. claimNext uses the hint as a tie-break INSIDE one priority band and
//     never across a CPT boundary;
//  3. an expired hint is ignored (fail-open);
//  4. a duplicate delivery of the same CloudEvents id has exactly one
//     effect, including after a consumer restart (the group redelivers).
func TestInterleavingHint_FromKafkaToClaimNext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	kcontainer, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("fe-hint-itest"))
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(kcontainer) })
	brokers, err := kcontainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}

	pool := transferDB(t)
	store := interleavinghintcopy.NewPostgresStore(pool)
	uow := postgres.NewUnitOfWork(pool)
	apply := &usecases.ApplyInterleavingHint{Processed: postgres.NewProcessedEventsRepo(pool), Hints: store, UnitOfWork: uow}

	topic := fmt.Sprintf("%s.itest-%d", inboundkafka.TaskInterleavingTopic, time.Now().UnixNano())
	createTestTopic(t, ctx, brokers[0], topic)
	group := fmt.Sprintf("fe-interleaving-itest-%d", time.Now().UnixNano())

	producer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, RequiredAcks: kafkago.RequireAll, BatchTimeout: 10 * time.Millisecond}
	t.Cleanup(func() { _ = producer.Close() })

	consumer, stopConsumer, consumerDone := startHintConsumer(t, ctx, brokers, topic, group, apply)

	// The consumer starts at the LATEST offset, so any message published
	// before it joined is legitimately skipped. Publish ignored-type
	// warm-ups until one is committed: that proves the join, and every
	// subsequent message is guaranteed to be fetched.
	base := waitForHintConsumerJoin(t, ctx, producer, brokers[0], group, topic)
	step := int64(0)
	next := func() int64 { step++; return base + step }

	now := time.Now().UTC().Truncate(time.Millisecond)
	hint1 := kafkago.Message{Key: []byte("STN-07"), Value: hintIssuedCE("ce-hint-1", "STN-07", "hint-1", now.Add(10*time.Minute), "t-hint", "t-peer")}
	if err := producer.WriteMessages(ctx, hint1); err != nil {
		t.Fatalf("publish hint1: %v", err)
	}
	waitForCommittedOffset(t, ctx, brokers[0], group, topic, next())

	active, err := store.ActiveForStation(ctx, "STN-07", now)
	if err != nil || active == nil || active.HintId != "hint-1" {
		t.Fatalf("active hint = %+v, %v; want hint-1", active, err)
	}

	// Task pool: t-early in an earlier band, t-hint and t-peer peers in a
	// later band. STN-07 can work all three.
	tasks := postgres.NewTaskRepo(pool)
	stations := postgres.NewStationRepo(pool)
	publisher := events.NewBufferedPublisher()
	clock := fixedClockAt(now)
	saveTask := func(id string, cpt time.Time) {
		t.Helper()
		if err := tasks.Save(ctx, task.New(shared.TaskId(id), task.Pick, shared.NewCPT(cpt), shared.OrderRef("o-"+id), shared.NewCapabilitySet("pick"), false, false)); err != nil {
			t.Fatalf("save task %s: %v", id, err)
		}
	}
	saveTask("t-early", now.Add(time.Hour))
	saveTask("t-hint", now.Add(2*time.Hour))
	saveTask("t-peer", now.Add(2*time.Hour))
	if err := stations.Save(ctx, station.New("STN-07", shared.NewCapabilitySet("pick"))); err != nil {
		t.Fatalf("save station: %v", err)
	}
	claim := &usecases.ClaimNext{Tasks: tasks, Stations: stations, Publisher: publisher, Clock: clock, UnitOfWork: uow, Hints: store}

	// (3) the hint never lifts t-hint across the band boundary: the
	// earlier-CPT t-early wins the first claim.
	first, err := claim.Execute(ctx, "STN-07", task.Pick)
	if err != nil || first == nil {
		t.Fatalf("first claim: %v %v", first, err)
	}
	if first.Id() != "t-early" {
		t.Fatalf("first claim = %s, want t-early (the hint must not cross the CPT boundary)", first.Id())
	}

	// (2) inside t-hint/t-peer's band the rank-1 hinted candidate wins.
	second, err := claim.Execute(ctx, "STN-07", task.Pick)
	if err != nil || second == nil {
		t.Fatalf("second claim: %v %v", second, err)
	}
	if second.Id() != "t-hint" {
		t.Fatalf("second claim = %s, want the rank-1 hinted t-hint", second.Id())
	}

	// (4a) duplicate delivery of the same CloudEvents id has one effect.
	if err := producer.WriteMessages(ctx, hint1); err != nil {
		t.Fatalf("republish hint1: %v", err)
	}
	waitForCommittedOffset(t, ctx, brokers[0], group, topic, next())
	var claims int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE event_id = 'ce-hint-1'`).Scan(&claims); err != nil {
		t.Fatalf("count claims: %v", err)
	}
	if claims != 1 {
		t.Fatalf("processed claims for ce-hint-1 = %d, want exactly 1", claims)
	}

	// (1b) a newer hint for the same station supersedes.
	hint2 := kafkago.Message{Key: []byte("STN-07"), Value: hintIssuedCE("ce-hint-2", "STN-07", "hint-2", now.Add(10*time.Minute), "t-peer")}
	if err := producer.WriteMessages(ctx, hint2); err != nil {
		t.Fatalf("publish hint2: %v", err)
	}
	waitForCommittedOffset(t, ctx, brokers[0], group, topic, next())
	active, _ = store.ActiveForStation(ctx, "STN-07", now)
	if active == nil || active.HintId != "hint-2" || len(active.Candidates) != 1 || active.Candidates[0].TaskId != "t-peer" {
		t.Fatalf("active hint = %+v, want hint-2 [t-peer] (superseded)", active)
	}

	// (4b) redelivery after a RESTART: stop the consumer, start a new one
	// on the same group, and redeliver ce-hint-2 — still exactly one effect.
	stopConsumer()
	<-consumerDone
	if err := consumer.Close(); err != nil {
		t.Fatalf("close consumer: %v", err)
	}
	consumer2, stop2, done2 := startHintConsumer(t, ctx, brokers, topic, group, apply)
	t.Cleanup(func() {
		stop2()
		<-done2
		_ = consumer2.Close()
	})
	if err := producer.WriteMessages(ctx, hint2); err != nil {
		t.Fatalf("redeliver hint2: %v", err)
	}
	waitForCommittedOffset(t, ctx, brokers[0], group, topic, next())
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE event_id = 'ce-hint-2'`).Scan(&claims); err != nil {
		t.Fatalf("count claims: %v", err)
	}
	if claims != 1 {
		t.Fatalf("processed claims for ce-hint-2 = %d after restart redelivery, want exactly 1", claims)
	}
	active, _ = store.ActiveForStation(ctx, "STN-07", now)
	if active == nil || active.HintId != "hint-2" {
		t.Fatalf("active hint after restart = %+v, want hint-2", active)
	}

	// (3b) an expired hint is ignored: STN-08's hint ranks t-peer, but it
	// expired before the claim, so the only remaining task claims in the
	// plain order anyway — and, conclusively, the lookup yields nothing.
	expiredHint := kafkago.Message{Key: []byte("STN-08"), Value: hintIssuedCE("ce-hint-3", "STN-08", "hint-3", now.Add(-time.Minute), "t-peer")}
	if err := producer.WriteMessages(ctx, expiredHint); err != nil {
		t.Fatalf("publish expired hint: %v", err)
	}
	waitForCommittedOffset(t, ctx, brokers[0], group, topic, next())
	if err := stations.Save(ctx, station.New("STN-08", shared.NewCapabilitySet("pick"))); err != nil {
		t.Fatalf("save station STN-08: %v", err)
	}
	if h, _ := store.ActiveForStation(ctx, "STN-08", now); h != nil {
		t.Fatalf("STN-08 active hint = %+v, want nil (expired)", h)
	}
	third, err := claim.Execute(ctx, "STN-08", task.Pick)
	if err != nil || third == nil {
		t.Fatalf("STN-08 claim: %v %v", third, err)
	}
	if third.Id() != "t-peer" {
		t.Fatalf("STN-08 claim = %s, want t-peer (the only pool task left; the expired hint changed nothing)", third.Id())
	}
}

// waitForHintConsumerJoin publishes ignored-type warm-up messages until the
// consumer group commits one, then returns the committed offset — the base
// every later waitForCommittedOffset counts from. Needed because the
// consumer starts at the LATEST offset: a message published before the join
// is legitimately never seen.
func waitForHintConsumerJoin(t *testing.T, ctx context.Context, producer *kafkago.Writer, broker, group, topic string) int64 {
	t.Helper()
	client := &kafkago.Client{Addr: kafkago.TCP(broker)}
	deadline := time.Now().Add(90 * time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		warmup := kafkago.Message{Key: []byte("STN-07"), Value: cloudEvent(fmt.Sprintf("ce-warmup-%d", i), "/warehouse/task-interleaving",
			"com.warehouse.wes.task-interleaving.stationtrip.InterleavingHintConsumed", "STN-07", time.Now().UTC(),
			"urn:warehouse:task-interleaving:analytics:InterleavingHintConsumed:v1", map[string]any{"hint_id": "x"})}
		if err := producer.WriteMessages(ctx, warmup); err != nil {
			t.Fatalf("publish warmup: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
		resp, err := client.OffsetFetch(ctx, &kafkago.OffsetFetchRequest{GroupID: group, Topics: map[string][]int{topic: {0}}})
		if err == nil {
			for _, p := range resp.Topics[topic] {
				if p.Partition == 0 && p.CommittedOffset > 0 {
					return p.CommittedOffset
				}
			}
		}
	}
	t.Fatalf("group %s never committed a warm-up message on %s (consumer never joined)", group, topic)
	return 0
}
