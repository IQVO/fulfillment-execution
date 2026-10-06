//go:build integration

package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// outboxDB returns a pool over the package's shared, migrated testcontainers
// Postgres (see main_integration_test.go — never an external DATABASE_URL),
// with every application table emptied so the test starts clean.
func outboxDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := postgres.NewPool(context.Background(), sharedPostgresURL(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	truncateAll(t, pool)
	return pool
}

// recordingSink records what the relay hands it, optionally failing on
// a given (topic, key) so the stop-at-failed-row behaviour can be
// exercised.
type recordingSink struct {
	sent      []outboundkafka.Encoded
	failTopic string
	failKey   string
	failErr   error
}

func (s *recordingSink) Send(_ context.Context, msgs ...outboundkafka.Encoded) error {
	for _, m := range msgs {
		if s.failErr != nil && m.Topic == s.failTopic && string(m.Key) == s.failKey {
			return s.failErr
		}
		s.sent = append(s.sent, m)
	}
	return nil
}

// failingEncoder stands in for an Encoder whose enrichment lookup fails,
// forcing the outbox publish (and therefore the whole unit of work) to
// fail.
type failingEncoder struct{}

func (failingEncoder) Encode(context.Context, ...shared.DomainEvent) ([]outboundkafka.Encoded, error) {
	return nil, errors.New("encoder: enrichment lookup failed")
}

func countOutbox(t *testing.T, pool *pgxpool.Pool, where string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM outbox_events WHERE "+where).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

var ids = func() func() string {
	n := 0
	return func() string { n++; return "evt-" + string(rune('a'+n)) }
}()

// outboxStack wires the production shape: both publishers as Encoders
// inside one OutboxPublisher, all repos on the same pool, one UnitOfWork.
type outboxStack struct {
	pool     *pgxpool.Pool
	tasks    *postgres.TaskRepo
	stations *postgres.StationRepo
	packages *postgres.PackageRepo
	pub      *postgres.OutboxPublisher
	uow      *postgres.UnitOfWork
	clock    fixedClock
}

func newOutboxStack(t *testing.T) *outboxStack {
	t.Helper()
	pool := outboxDB(t)
	tasks := postgres.NewTaskRepo(pool)
	stations := postgres.NewStationRepo(pool)
	integration := outboundkafka.NewPublisherWithWriter(nil, tasks, stations, ids)
	analytics := outboundkafka.NewAnalyticsPublisherWithWriter(nil, tasks, ids)
	return &outboxStack{
		pool:     pool,
		tasks:    tasks,
		stations: stations,
		packages: postgres.NewPackageRepo(pool),
		pub:      postgres.NewOutboxPublisher(pool, integration, analytics),
		uow:      postgres.NewUnitOfWork(pool),
		clock:    fixedClock{t: time.Now().UTC().Truncate(time.Microsecond)},
	}
}

func taskIds(prefix string) func() shared.TaskId {
	n := 0
	return func() shared.TaskId { n++; return shared.TaskId(prefix + string(rune('0'+n))) }
}

// 1. commit-together: after CompleteTask the task row is Completed AND the
// outbox holds one row per topic — the analytics row for every event, the
// integration row for TaskCompleted, both enriched from the just-saved
// rows inside the same transaction.
func TestOutbox_CompleteTask_CommitsAggregateAndBothTopicsTogether(t *testing.T) {
	s := newOutboxStack(t)
	ctx := context.Background()

	if err := s.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick"))); err != nil {
		t.Fatalf("save station: %v", err)
	}
	create := &usecases.CreateTask{Tasks: s.tasks, Publisher: s.pub, Clock: s.clock, NewId: taskIds("t"), UnitOfWork: s.uow}
	created, err := create.Execute(ctx, task.Pick, shared.NewCPT(s.clock.t.Add(time.Hour)), "wu-1", shared.NewCapabilitySet("pick"), false, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// TaskCreated is analytics-only.
	if got := countOutbox(t, s.pool, "topic = '"+outboundkafka.AnalyticsTopic+"' AND event_type = 'com.warehouse.wes.fulfillment-execution.task.TaskCreated'"); got != 1 {
		t.Fatalf("expected 1 analytics TaskCreated row, got %d", got)
	}
	if got := countOutbox(t, s.pool, "topic = '"+outboundkafka.Topic+"'"); got != 0 {
		t.Fatalf("TaskCreated is not an integration event, expected 0 integration rows, got %d", got)
	}

	claim := &usecases.ClaimNext{Tasks: s.tasks, Stations: s.stations, Publisher: s.pub, Clock: s.clock, UnitOfWork: s.uow}
	if _, err := claim.Execute(ctx, "s1", task.Pick); err != nil {
		t.Fatalf("claim: %v", err)
	}
	complete := &usecases.CompleteTask{Tasks: s.tasks, Publisher: s.pub, Clock: fixedClock{t: s.clock.t.Add(45 * time.Second)}, UnitOfWork: s.uow}
	if err := complete.Execute(ctx, created.Id(), "s1"); err != nil {
		t.Fatalf("complete: %v", err)
	}

	found, err := s.tasks.FindById(ctx, created.Id())
	if err != nil || found == nil || found.Status() != task.Completed {
		t.Fatalf("expected the task persisted as Completed, got %v err=%v", found, err)
	}
	if got := countOutbox(t, s.pool, "published_at IS NULL AND topic = '"+outboundkafka.Topic+"' AND event_type = 'com.warehouse.wes.fulfillment-execution.task.TaskCompleted'"); got != 1 {
		t.Fatalf("expected 1 unpublished integration TaskCompleted row, got %d", got)
	}
	if got := countOutbox(t, s.pool, "published_at IS NULL AND topic = '"+outboundkafka.AnalyticsTopic+"' AND event_type = 'com.warehouse.wes.fulfillment-execution.task.TaskCompleted'"); got != 1 {
		t.Fatalf("expected 1 unpublished analytics TaskCompleted row, got %d", got)
	}
	// The integration payload was enriched INSIDE the transaction from the
	// just-saved task row (work_unit_id) — proof that Encode ran under the
	// unit of work rather than after it.
	var value []byte
	if err := s.pool.QueryRow(ctx, "SELECT value FROM outbox_events WHERE topic = $1 AND event_type = 'com.warehouse.wes.fulfillment-execution.task.TaskCompleted'", outboundkafka.Topic).Scan(&value); err != nil {
		t.Fatalf("read integration row: %v", err)
	}
	if !bytes.Contains(value, []byte(`"work_unit_id":"wu-1"`)) || !bytes.Contains(value, []byte(`"duration_seconds":45`)) {
		t.Fatalf("integration payload not enriched from the transaction's own writes: %s", value)
	}
	// Every row of every topic is unpublished: the broker was never touched.
	if total := countOutbox(t, s.pool, "published_at IS NOT NULL"); total != 0 {
		t.Fatalf("nothing should be published yet, got %d", total)
	}
}

// 2. rollback: if the outbox insert cannot happen (here: an encoder whose
// enrichment lookup fails, the same failure mode a broken task/station
// read would produce), the aggregate change must not survive either.
func TestOutbox_PublishFailure_RollsBackAggregate(t *testing.T) {
	s := newOutboxStack(t)
	ctx := context.Background()
	failing := postgres.NewOutboxPublisher(s.pool, failingEncoder{})
	create := &usecases.CreateTask{Tasks: s.tasks, Publisher: failing, Clock: s.clock, NewId: taskIds("x"), UnitOfWork: s.uow}

	if _, err := create.Execute(ctx, task.Pick, shared.NewCPT(s.clock.t.Add(time.Hour)), "wu-x", shared.NewCapabilitySet("pick"), false, false); err == nil {
		t.Fatal("expected the failing encoder to fail the publish")
	}
	found, err := s.tasks.FindById(ctx, "x1")
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if found != nil {
		t.Fatal("aggregate row survived a failed publish: the unit of work did not roll back")
	}
	if got := countOutbox(t, s.pool, "true"); got != 0 {
		t.Fatalf("expected no outbox rows, got %d", got)
	}
}

// 3. relay: rows go out in id order (which is commit order: create, claim,
// complete — across both topics), get marked published, and a second pass
// is a no-op.
func TestOutboxRelay_PublishesInOrderAndMarksRows(t *testing.T) {
	s := newOutboxStack(t)
	ctx := context.Background()
	_ = s.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pick")))
	create := &usecases.CreateTask{Tasks: s.tasks, Publisher: s.pub, Clock: s.clock, NewId: taskIds("r"), UnitOfWork: s.uow}
	claim := &usecases.ClaimNext{Tasks: s.tasks, Stations: s.stations, Publisher: s.pub, Clock: s.clock, UnitOfWork: s.uow}
	complete := &usecases.CompleteTask{Tasks: s.tasks, Publisher: s.pub, Clock: s.clock, UnitOfWork: s.uow}
	created, err := create.Execute(ctx, task.Pick, shared.NewCPT(s.clock.t.Add(time.Hour)), "wu-r", shared.NewCapabilitySet("pick"), false, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := claim.Execute(ctx, "s1", task.Pick); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := complete.Execute(ctx, created.Id(), "s1"); err != nil {
		t.Fatalf("complete: %v", err)
	}

	sink := &recordingSink{}
	relay := postgres.NewOutboxRelay(s.pool, sink, slog.Default())
	n, err := relay.RelayOnce(ctx)
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	// integration: TaskCompleted; analytics: TaskCreated, TaskClaimed, TaskCompleted.
	if n != 4 || len(sink.sent) != 4 {
		t.Fatalf("expected 4 published, got n=%d sent=%d", n, len(sink.sent))
	}
	want := []struct{ topic, eventType string }{
		{outboundkafka.AnalyticsTopic, "com.warehouse.wes.fulfillment-execution.task.TaskCreated"},
		{outboundkafka.AnalyticsTopic, "com.warehouse.wes.fulfillment-execution.task.TaskClaimed"},
		{outboundkafka.Topic, "com.warehouse.wes.fulfillment-execution.task.TaskCompleted"},
		{outboundkafka.AnalyticsTopic, "com.warehouse.wes.fulfillment-execution.task.TaskCompleted"},
	}
	for i, w := range want {
		got := sink.sent[i]
		if got.Topic != w.topic || got.EventType != w.eventType || string(got.Key) != string(created.Id()) {
			t.Fatalf("message %d: want %s on %s keyed %s, got %s on %s keyed %s", i, w.eventType, w.topic, created.Id(), got.EventType, got.Topic, got.Key)
		}
	}
	if got := countOutbox(t, s.pool, "published_at IS NULL"); got != 0 {
		t.Fatalf("expected every row marked published, %d still pending", got)
	}
	if got := countOutbox(t, s.pool, "attempts = 1 AND last_error IS NULL"); got != 4 {
		t.Fatalf("expected 4 rows with attempts=1 and no error, got %d", got)
	}
	// Every relayed message is a valid CloudEvents 1.0 event carrying the
	// structured-mode content-type header, and its `id` is the one minted
	// once at Encode time and persisted in the outbox row — so a
	// redelivery of the same row carries the same id (ADR-0032).
	for i, m := range sink.sent {
		e, err := cloudevents.Decode(m.Value)
		if err != nil {
			t.Fatalf("message %d is not a valid CloudEvent: %v", i, err)
		}
		if e.Subject() != string(created.Id()) {
			t.Fatalf("message %d: subject = %q, want %q", i, e.Subject(), created.Id())
		}
		var hasCT bool
		for _, h := range m.Headers {
			if h.Key == "content-type" && string(h.Value) == cloudevents.MediaType {
				hasCT = true
			}
		}
		if !hasCT {
			t.Fatalf("message %d: missing CloudEvents content-type header, got %v", i, m.Headers)
		}
		var stored []byte
		if err := s.pool.QueryRow(ctx, "SELECT value FROM outbox_events WHERE value = $1", m.Value).Scan(&stored); err != nil {
			t.Fatalf("message %d: relayed bytes are not the persisted row verbatim: %v", i, err)
		}
	}
	n, err = relay.RelayOnce(ctx)
	if err != nil || n != 0 || len(sink.sent) != 4 {
		t.Fatalf("second pass should be a no-op, got n=%d err=%v sent=%d", n, err, len(sink.sent))
	}
}

// 4. relay stops at a failed row, records the attempt on it, leaves later
// rows pending (ordering preserved), and drains the rest in order once
// the sink recovers.
func TestOutboxRelay_SinkFailure_StopsAtFailedRowAndRetriesLater(t *testing.T) {
	s := newOutboxStack(t)
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: s.tasks, Publisher: s.pub, Clock: s.clock, NewId: taskIds("f"), UnitOfWork: s.uow}
	for i := 0; i < 3; i++ {
		if _, err := create.Execute(ctx, task.Pick, shared.NewCPT(s.clock.t.Add(time.Hour)), "wu-f", shared.NewCapabilitySet("pick"), false, false); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
	}
	// Rows: f1, f2, f3 (all analytics TaskCreated). Fail on f2.
	sink := &recordingSink{failTopic: outboundkafka.AnalyticsTopic, failKey: "f2", failErr: errors.New("broker down")}
	relay := postgres.NewOutboxRelay(s.pool, sink, slog.Default())

	n, err := relay.RelayOnce(ctx)
	if err == nil {
		t.Fatal("expected the failing row to surface an error")
	}
	if n != 1 || len(sink.sent) != 1 || string(sink.sent[0].Key) != "f1" {
		t.Fatalf("expected only f1 published before the failure, got n=%d sent=%v", n, sink.sent)
	}
	if got := countOutbox(t, s.pool, "published_at IS NULL"); got != 2 {
		t.Fatalf("expected f2 and f3 still pending (ordering preserved), got %d pending", got)
	}
	var attempts int
	var lastErr string
	if err := s.pool.QueryRow(ctx, "SELECT attempts, coalesce(last_error,'') FROM outbox_events WHERE key = $1", []byte("f2")).Scan(&attempts, &lastErr); err != nil {
		t.Fatalf("read f2: %v", err)
	}
	if attempts != 1 || lastErr == "" {
		t.Fatalf("expected f2 to record the failed attempt, got attempts=%d last_error=%q", attempts, lastErr)
	}
	if got := countOutbox(t, s.pool, "key = '\\x6633' AND attempts = 0"); got != 1 { // f3 untouched
		t.Fatalf("expected f3 untouched with attempts=0, got %d", got)
	}

	// Broker recovers: the next pass drains the rest, in order.
	sink.failErr = nil
	n, err = relay.RelayOnce(ctx)
	if err != nil || n != 2 {
		t.Fatalf("recovery pass: n=%d err=%v", n, err)
	}
	if string(sink.sent[1].Key) != "f2" || string(sink.sent[2].Key) != "f3" {
		t.Fatalf("expected f2 then f3 after recovery, got %v", sink.sent)
	}
	if got := countOutbox(t, s.pool, "published_at IS NULL"); got != 0 {
		t.Fatalf("expected outbox drained, %d pending", got)
	}
	if got := countOutbox(t, s.pool, "key = '\\x6632' AND attempts = 2 AND last_error IS NULL"); got != 1 {
		t.Fatalf("expected f2 to show attempts=2 and a cleared error after recovery, got %d", got)
	}
}

// Trace headers survive the round trip through the outbox JSONB column.
func TestOutbox_HeadersRoundTrip(t *testing.T) {
	s := newOutboxStack(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO outbox_events (topic, event_type, key, value, headers)
		VALUES ($1, 'X', 'k', 'v', '[{"key":"traceparent","value":"00-abc-def-01"}]')
	`, outboundkafka.Topic); err != nil {
		t.Fatalf("seed: %v", err)
	}
	sink := &recordingSink{}
	if _, err := postgres.NewOutboxRelay(s.pool, sink, nil).RelayOnce(ctx); err != nil {
		t.Fatalf("relay: %v", err)
	}
	if len(sink.sent) != 1 || len(sink.sent[0].Headers) != 1 || sink.sent[0].Headers[0].Key != "traceparent" || string(sink.sent[0].Headers[0].Value) != "00-abc-def-01" {
		t.Fatalf("headers did not round-trip: %+v", sink.sent)
	}
}

// Nested unit of work: ArriveAtRebin -> CreateTask joins the outer
// transaction, so a failure at the very end (OrderConsolidated's outbox
// insert) rolls back the consolidation row AND the nested PACK task.
func TestOutbox_ArriveAtRebin_NestedScopeRollsBackEverything(t *testing.T) {
	s := newOutboxStack(t)
	ctx := context.Background()
	consolidations := postgres.NewOrderConsolidationRepo(s.pool)

	// A publisher that succeeds for everything except OrderConsolidated.
	pub := &selectiveFailPublisher{inner: s.pub, failOn: "OrderConsolidated"}
	createTask := &usecases.CreateTask{Tasks: s.tasks, Publisher: pub, Clock: s.clock, NewId: taskIds("n"), UnitOfWork: s.uow}
	uc := &usecases.ArriveAtRebin{Consolidations: consolidations, CreateTask: createTask, Publisher: pub, Clock: s.clock, UnitOfWork: s.uow}

	err := uc.Execute(ctx, "order-n", "line-1", []string{"line-1"}, shared.NewCPT(s.clock.t.Add(time.Hour)), shared.NewCapabilitySet("pack"), false, false)
	if err == nil {
		t.Fatal("expected the final publish failure to propagate")
	}
	if oc, _ := consolidations.FindByOrderRef(ctx, "order-n"); oc != nil {
		t.Fatal("consolidation row survived: the nested scope did not roll back with the outer one")
	}
	if tasks, _ := s.tasks.FindByOrderRef(ctx, "order-n"); len(tasks) != 0 {
		t.Fatalf("PACK task created by the nested CreateTask survived the rollback: %d rows", len(tasks))
	}
	if got := countOutbox(t, s.pool, "true"); got != 0 {
		t.Fatalf("expected no outbox rows after rollback, got %d", got)
	}

	// And the happy path commits everything in one go.
	pub.failOn = ""
	if err := uc.Execute(ctx, "order-ok", "line-1", []string{"line-1"}, shared.NewCPT(s.clock.t.Add(time.Hour)), shared.NewCapabilitySet("pack"), false, false); err != nil {
		t.Fatalf("happy path: %v", err)
	}
	if oc, _ := consolidations.FindByOrderRef(ctx, "order-ok"); oc == nil || !oc.IsComplete() {
		t.Fatal("expected a complete consolidation row")
	}
	if tasks, _ := s.tasks.FindByOrderRef(ctx, "order-ok"); len(tasks) != 1 || tasks[0].Type() != task.Pack {
		t.Fatalf("expected one PACK task, got %v", tasks)
	}
	// ItemArrivedAtRebin/OrderConsolidated are outside both contracts;
	// only the nested TaskCreated reaches the analytics topic.
	if got := countOutbox(t, s.pool, "event_type = 'com.warehouse.wes.fulfillment-execution.task.TaskCreated' AND topic = '"+outboundkafka.AnalyticsTopic+"'"); got != 1 {
		t.Fatalf("expected the nested TaskCreated outbox row, got %d", got)
	}
}

type selectiveFailPublisher struct {
	inner  *postgres.OutboxPublisher
	failOn string
}

func (p *selectiveFailPublisher) Publish(ctx context.Context, evts ...shared.DomainEvent) error {
	for _, e := range evts {
		if p.failOn != "" && e.EventName() == p.failOn {
			return errors.New("forced failure on " + p.failOn)
		}
	}
	return p.inner.Publish(ctx, evts...)
}

// 5. TaskCPTMissed (ADR-0025) round-trips through the real outbox -> relay
// path exactly like every other event: SweepCPTMisses writes it into
// outbox_events inside its own transaction, and RelayOnce delivers it
// on-topic, keyed by TaskId, with the OrderRef/TaskType/CPT fields intact.
func TestOutbox_SweepCPTMisses_RoundTripsThroughRelay(t *testing.T) {
	s := newOutboxStack(t)
	ctx := context.Background()
	create := &usecases.CreateTask{Tasks: s.tasks, Publisher: s.pub, Clock: s.clock, NewId: taskIds("c"), UnitOfWork: s.uow}
	created, err := create.Execute(ctx, task.Pick, shared.NewCPT(s.clock.t.Add(time.Minute)), "order-cpt", shared.NewCapabilitySet("pick"), false, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	overdueClock := fixedClock{t: s.clock.t.Add(2 * time.Minute)}
	sweep := &usecases.SweepCPTMisses{Tasks: s.tasks, Publisher: s.pub, Clock: overdueClock, UnitOfWork: s.uow}
	reported, err := sweep.Execute(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if reported != 1 {
		t.Fatalf("expected 1 task reported, got %d", reported)
	}

	if got := countOutbox(t, s.pool, "published_at IS NULL AND topic = '"+outboundkafka.Topic+"' AND event_type = 'com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed'"); got != 1 {
		t.Fatalf("expected 1 unpublished integration TaskCPTMissed row, got %d", got)
	}

	sink := &recordingSink{}
	relay := postgres.NewOutboxRelay(s.pool, sink, slog.Default())
	if _, err := relay.RelayOnce(ctx); err != nil {
		t.Fatalf("relay: %v", err)
	}
	var found *outboundkafka.Encoded
	for i := range sink.sent {
		if sink.sent[i].Topic == outboundkafka.Topic && sink.sent[i].EventType == "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" {
			found = &sink.sent[i]
		}
	}
	if found == nil {
		t.Fatalf("expected a relayed TaskCPTMissed message, got %+v", sink.sent)
	}
	if string(found.Key) != string(created.Id()) {
		t.Fatalf("expected message keyed by TaskId %s, got %s", created.Id(), found.Key)
	}
	if !bytes.Contains(found.Value, []byte(`"order_ref":"order-cpt"`)) || !bytes.Contains(found.Value, []byte(`"task_type":"PICK"`)) {
		t.Fatalf("TaskCPTMissed payload missing OrderRef/TaskType: %s", found.Value)
	}
}

// 6. PackageManifested (ADR-0025) round-trips through the real outbox ->
// relay path alongside LabelApplied on a successful SLAM pass — additive,
// not a replacement.
func TestOutbox_RunSlam_LabelAppliedAndPackageManifestedRoundTripTogether(t *testing.T) {
	s := newOutboxStack(t)
	ctx := context.Background()
	p := pack.New("pkg-manifest-1", "order-manifest", "task-1", false, false)
	// A non-zero hazard class is used solely so ScannedHazardClasses is a
	// non-nil slice — packages.scanned_hazard_classes is NOT NULL and
	// Package.ScanItem's hazardClass=0 shortcut never appends to it (see
	// package.go); mirrors postgres_integration_test.go's own
	// TestPackageRepo_SaveAndFindById fixture for the same reason.
	if err := p.ScanItemWithClass("sku-1", 3); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if err := p.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}
	if err := s.packages.Save(ctx, p); err != nil {
		t.Fatalf("save package: %v", err)
	}

	slam := &usecases.RunSlam{Packages: s.packages, Publisher: s.pub, Clock: s.clock, UnitOfWork: s.uow}
	if err := slam.Execute(ctx, p.Id(), 2.0, 2.0); err != nil {
		t.Fatalf("slam: %v", err)
	}

	if got := countOutbox(t, s.pool, "published_at IS NULL AND topic = '"+outboundkafka.Topic+"' AND event_type = 'com.warehouse.wes.fulfillment-execution.package.PackageManifested'"); got != 1 {
		t.Fatalf("expected 1 unpublished integration PackageManifested row, got %d", got)
	}

	sink := &recordingSink{}
	relay := postgres.NewOutboxRelay(s.pool, sink, slog.Default())
	if _, err := relay.RelayOnce(ctx); err != nil {
		t.Fatalf("relay: %v", err)
	}
	var foundManifested, foundLabelApplied bool
	var manifestedPayload []byte
	for _, m := range sink.sent {
		if m.Topic != outboundkafka.Topic {
			continue
		}
		switch m.EventType {
		case "com.warehouse.wes.fulfillment-execution.package.PackageManifested":
			foundManifested = true
			manifestedPayload = m.Value
			if string(m.Key) != string(p.Id()) {
				t.Fatalf("expected PackageManifested keyed by PackageId %s, got %s", p.Id(), m.Key)
			}
		}
	}
	// LabelApplied is not on the integration contract (see api-and-
	// integration.md): only TaskCompleted/TaskCPTMissed/PackageManifested
	// are. Confirm it stayed off the integration topic while still
	// landing on analytics.
	if foundLabelApplied {
		t.Fatal("LabelApplied unexpectedly reached the integration topic")
	}
	if !foundManifested {
		t.Fatalf("expected a relayed PackageManifested message, got %+v", sink.sent)
	}
	if !bytes.Contains(manifestedPayload, []byte(`"order_ref":"order-manifest"`)) {
		t.Fatalf("PackageManifested payload missing OrderRef: %s", manifestedPayload)
	}
	if got := countOutbox(t, s.pool, "topic = '"+outboundkafka.AnalyticsTopic+"' AND event_type = 'com.warehouse.wes.fulfillment-execution.package.LabelApplied'"); got != 1 {
		t.Fatalf("expected LabelApplied on the analytics topic, got %d", got)
	}
}

// 7. SealPackage retry-safety against a REAL Postgres round-trip: a
// retried POST /tasks/{id}/seal-package for the same task must return the
// original Package and must NOT insert a second row — proving migration
// 0011's task_id column, its partial unique index, and package_repo.go's
// ON CONFLICT / FindByTaskId actually work against a real database, not
// just the in-memory adapter used by the unit tests in usecases_test.go.
func TestOutbox_SealPackage_RetrySameTaskId_IsIdempotentAgainstRealPostgres(t *testing.T) {
	s := newOutboxStack(t)
	ctx := context.Background()

	if err := s.stations.Save(ctx, station.New("s1", shared.NewCapabilitySet("pack"))); err != nil {
		t.Fatalf("save station: %v", err)
	}
	create := &usecases.CreateTask{Tasks: s.tasks, Publisher: s.pub, Clock: s.clock, NewId: taskIds("seal"), UnitOfWork: s.uow}
	if _, err := create.Execute(ctx, task.Pack, shared.NewCPT(s.clock.Now().Add(time.Hour)), "order-seal-retry", shared.NewCapabilitySet("pack"), false, false); err != nil {
		t.Fatalf("create task: %v", err)
	}
	claim := &usecases.ClaimNext{Tasks: s.tasks, Stations: s.stations, Publisher: s.pub, Clock: s.clock, UnitOfWork: s.uow}
	claimed, err := claim.Execute(ctx, "s1", task.Pack)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	pkgIds := func() func() shared.PackageId {
		n := 0
		return func() shared.PackageId { n++; return shared.PackageId("seal-pkg-" + string(rune('0'+n))) }
	}()
	seal := &usecases.SealPackage{Tasks: s.tasks, Packages: s.packages, Publisher: s.pub, Clock: s.clock, NewId: pkgIds, UnitOfWork: s.uow}

	first, err := seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-retry-1"})
	if err != nil {
		t.Fatalf("first seal: %v", err)
	}

	// Simulate a client retry (e.g. after a dropped response) with the
	// exact same taskId/stationId/contents.
	second, err := seal.Execute(ctx, claimed.Id(), "s1", []string{"sku-retry-1"})
	if err != nil {
		t.Fatalf("retried seal: %v", err)
	}
	if second.Id() != first.Id() {
		t.Fatalf("expected the retry to return the SAME package id %q, got %q", first.Id(), second.Id())
	}

	var count int
	if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM packages WHERE task_id = $1", string(claimed.Id())).Scan(&count); err != nil {
		t.Fatalf("count packages: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 package row for task %s after a retried seal, got %d", claimed.Id(), count)
	}

	stored, err := s.packages.FindByTaskId(ctx, claimed.Id())
	if err != nil {
		t.Fatalf("FindByTaskId: %v", err)
	}
	if stored == nil || stored.Id() != first.Id() {
		t.Fatalf("expected FindByTaskId to resolve to the original package, got %+v", stored)
	}
}
