package kafka_test

import (
	"context"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// fakeAnalyticsWriter captures the messages handed to WriteMessages so a test
// can assert on the published envelope without a live broker.
type fakeAnalyticsWriter struct {
	msgs []kafkago.Message
}

func (w *fakeAnalyticsWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	w.msgs = append(w.msgs, msgs...)
	return nil
}

// fakeTaskRepo is a minimal ports.TaskRepo whose FindById returns a task of a
// fixed type, so the publisher's task_type enrichment can be asserted without
// a real repository. byOrderRef backs FindByOrderRef for the
// on-time-to-CPT enrichment lookup (see onTimeToCPTFields); the rest satisfy
// the interface.
type fakeTaskRepo struct {
	taskType   task.Type
	found      bool
	byOrderRef map[shared.OrderRef][]*task.Task
}

func (r fakeTaskRepo) FindById(_ context.Context, id shared.TaskId) (*task.Task, error) {
	if !r.found {
		return nil, nil
	}
	return task.New(id, r.taskType, shared.NewCPT(time.Now()), "order-1", shared.NewCapabilitySet(), false, false), nil
}
func (fakeTaskRepo) Save(context.Context, *task.Task) error { return nil }
func (fakeTaskRepo) FindClaimableByType(context.Context, task.Type, time.Time) ([]*task.Task, error) {
	return nil, nil
}
func (fakeTaskRepo) FindAllClaimed(context.Context) ([]*task.Task, error) { return nil, nil }
func (fakeTaskRepo) FindOpenPastCPT(context.Context, time.Time) ([]*task.Task, error) {
	return nil, nil
}
func (fakeTaskRepo) CountByTypeAndStatus(context.Context, task.Type, task.Status) (int, error) {
	return 0, nil
}
func (r fakeTaskRepo) FindByOrderRef(_ context.Context, orderRef shared.OrderRef) ([]*task.Task, error) {
	return r.byOrderRef[orderRef], nil
}

func TestAnalyticsPublisher_PublishesEachEventType(t *testing.T) {
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, tt := range eachEventTypeCases(at) {
		t.Run(tt.name, func(t *testing.T) {
			assertAnalyticsEventPublished(t, tt, at)
		})
	}
}

// analyticsEventCase is one event-type scenario of the analytics contract:
// the routing key, full CloudEvents type, and one representative data
// field asserted per event.
type analyticsEventCase struct {
	name          string
	event         shared.DomainEvent
	wantType      string
	wantKey       string
	wantDataField string
	wantDataValue any
}

// eachEventTypeCases is the table behind
// TestAnalyticsPublisher_PublishesEachEventType: one case per event type
// on the analytics contract.
func eachEventTypeCases(at time.Time) []analyticsEventCase {
	return []analyticsEventCase{
		{
			name:          "TaskCreated",
			event:         shared.NewTaskCreated("t1", at),
			wantType:      "com.warehouse.wes.fulfillment-execution.task.TaskCreated",
			wantKey:       "t1",
			wantDataField: "task_id",
			wantDataValue: "t1",
		},
		{
			name:          "TaskClaimed",
			event:         shared.NewTaskClaimed("t2", "s2", at),
			wantType:      "com.warehouse.wes.fulfillment-execution.task.TaskClaimed",
			wantKey:       "t2",
			wantDataField: "station_id",
			wantDataValue: "s2",
		},
		{
			name:          "LeaseExpired",
			event:         shared.NewLeaseExpired("t3", at),
			wantType:      "com.warehouse.wes.fulfillment-execution.task.LeaseExpired",
			wantKey:       "t3",
			wantDataField: "task_id",
			wantDataValue: "t3",
		},
		{
			name:          "TaskCompleted",
			event:         shared.NewTaskCompleted("t4", "s4", at),
			wantType:      "com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
			wantKey:       "t4",
			wantDataField: "station_id",
			wantDataValue: "s4",
		},
		{
			name:          "ItemPicked",
			event:         shared.NewItemPicked("t5", at),
			wantType:      "com.warehouse.wes.fulfillment-execution.task.ItemPicked",
			wantKey:       "t5",
			wantDataField: "task_id",
			wantDataValue: "t5",
		},
		{
			name:          "PackageSealed",
			event:         shared.NewPackageSealed("p6", at),
			wantType:      "com.warehouse.wes.fulfillment-execution.package.PackageSealed",
			wantKey:       "p6",
			wantDataField: "package_id",
			wantDataValue: "p6",
		},
		{
			name:          "WeightDiscrepancyDetected",
			event:         shared.NewWeightDiscrepancyDetected("p7", 1000, 1200, at),
			wantType:      "com.warehouse.wes.fulfillment-execution.package.WeightDiscrepancyDetected",
			wantKey:       "p7",
			wantDataField: "actual_g",
			wantDataValue: float64(1200),
		},
		{
			name:          "LabelApplied",
			event:         shared.NewLabelApplied("p8", at),
			wantType:      "com.warehouse.wes.fulfillment-execution.package.LabelApplied",
			wantKey:       "p8",
			wantDataField: "package_id",
			wantDataValue: "p8",
		},
		{
			name:          "PackageDiverted",
			event:         shared.NewPackageDiverted("p9", at),
			wantType:      "com.warehouse.wes.fulfillment-execution.package.PackageDiverted",
			wantKey:       "p9",
			wantDataField: "package_id",
			wantDataValue: "p9",
		},
	}
}

// assertAnalyticsEventPublished publishes tt's event through a real
// analytics publisher backed by a recording writer and asserts the
// CloudEvents attributes plus tt's key/type/data expectations.
func assertAnalyticsEventPublished(t *testing.T, tt analyticsEventCase, at time.Time) {
	t.Helper()

	w := &fakeAnalyticsWriter{}
	p := outboundkafka.NewAnalyticsPublisher(nil, fakeTaskRepo{found: false}, func() string { return "evt-fixed" })
	p.Writer = w

	if err := p.Publish(context.Background(), tt.event); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(w.msgs))
	}
	msg := w.msgs[0]
	if string(msg.Key) != tt.wantKey {
		t.Errorf("key = %q, want %q", string(msg.Key), tt.wantKey)
	}

	env, data := decodeCE[map[string]any](t, msg.Value)
	if env.Type() != tt.wantType {
		t.Errorf("type = %q, want %q", env.Type(), tt.wantType)
	}
	if env.ID() != "evt-fixed" {
		t.Errorf("id = %q, want evt-fixed", env.ID())
	}
	if env.Source() != "/warehouse/fulfillment-execution" {
		t.Errorf("source = %q, want /warehouse/fulfillment-execution", env.Source())
	}
	if env.Subject() != tt.wantKey {
		t.Errorf("subject = %q, want %q (the aggregate id)", env.Subject(), tt.wantKey)
	}
	if want := "urn:warehouse:fulfillment-execution:analytics:" + tt.name + ":v1"; env.DataSchema() != want {
		t.Errorf("dataschema = %q, want %q", env.DataSchema(), want)
	}
	if !env.Time().Equal(at) {
		t.Errorf("time = %v, want %v", env.Time(), at)
	}
	if _, has := data["schema_version"]; has {
		t.Error("data must not carry the retired schema_version field")
	}
	if !hasContentTypeHeader(msg.Headers) {
		t.Errorf("missing CloudEvents content-type header, got %v", msg.Headers)
	}

	if got := data[tt.wantDataField]; got != tt.wantDataValue {
		t.Errorf("data[%q] = %v (%T), want %v (%T)", tt.wantDataField, got, got, tt.wantDataValue, tt.wantDataValue)
	}
}

func TestAnalyticsPublisher_SkipsUnknownEvents(t *testing.T) {
	w := &fakeAnalyticsWriter{}
	p := outboundkafka.NewAnalyticsPublisher(nil, fakeTaskRepo{found: false}, func() string { return "evt" })
	p.Writer = w

	if err := p.Publish(context.Background(), unknownEvent{}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 0 {
		t.Fatalf("expected unknown event to be skipped, got %d messages", len(w.msgs))
	}
}

type unknownEvent struct{}

func (unknownEvent) EventName() string     { return "Unknown" }
func (unknownEvent) OccurredAt() time.Time { return time.Time{} }

func TestAnalyticsPublisher_WeightDiscrepancyExpectedActual(t *testing.T) {
	w := &fakeAnalyticsWriter{}
	p := outboundkafka.NewAnalyticsPublisher(nil, fakeTaskRepo{found: false}, func() string { return "evt" })
	p.Writer = w

	at := time.Now()
	if err := p.Publish(context.Background(), shared.NewWeightDiscrepancyDetected("pkg", 900, 1100, at)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	_, data := decodeCE[map[string]any](t, w.msgs[0].Value)
	if data["expected_g"] != float64(900) {
		t.Errorf("expected_g = %v, want 900", data["expected_g"])
	}
	if data["package_id"] != "pkg" {
		t.Errorf("package_id = %v, want pkg", data["package_id"])
	}
}

// TestAnalyticsPublisher_EnrichesTaskType asserts a task-scoped event is
// stamped with the owning task's process path, looked up via the TaskRepo —
// the enrichment that populates the report's task_type dimension.
func TestAnalyticsPublisher_EnrichesTaskType(t *testing.T) {
	w := &fakeAnalyticsWriter{}
	p := outboundkafka.NewAnalyticsPublisher(nil, fakeTaskRepo{taskType: task.Pack, found: true}, func() string { return "evt" })
	p.Writer = w

	if err := p.Publish(context.Background(), shared.NewTaskCompleted("t1", "s1", time.Now())); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	_, data := decodeCE[map[string]any](t, w.msgs[0].Value)
	if data["task_type"] != string(task.Pack) {
		t.Errorf("task_type = %v, want %v", data["task_type"], task.Pack)
	}
	if data["station_id"] != "s1" {
		t.Errorf("station_id = %v, want s1", data["station_id"])
	}
}

// TestAnalyticsPublisher_PackageManifested_OnTimeAndLateAndBoundary asserts
// the on-time-to-CPT enrichment (ADR-0026): the publisher resolves the
// originating SLAM task via FindByOrderRef, compares the manifest's
// occurred_at against that task's CPT, and marks resolved=true. The
// boundary case (manifested exactly at CPT) is asserted separately and
// explicitly — it must count as ON TIME (manifestedAt <= cpt), the
// deliberate mirror of task.Task.IsCPTMissed's own now>=cpt-counts-as-missed
// boundary.
func TestAnalyticsPublisher_PackageManifested_OnTimeAndLateAndBoundary(t *testing.T) {
	cpt := time.Date(2026, 3, 1, 18, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		manifestedAt time.Time
		wantOnTime   bool
	}{
		{"before CPT", cpt.Add(-time.Minute), true},
		{"exactly at CPT (boundary — must count as on time)", cpt, true},
		{"after CPT", cpt.Add(time.Minute), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertPackageManifestedOnTime(t, cpt, tt.manifestedAt, tt.wantOnTime)
		})
	}
}

// newClaimedSlamTask builds the claimed SLAM task for "order-9" whose CPT
// the on-time enrichment resolves: the publisher finds it via
// FindByOrderRef and compares the manifest's occurred_at against its CPT.
func newClaimedSlamTask(t *testing.T, cpt time.Time) *task.Task {
	t.Helper()

	tk := task.New("slam-1", task.Slam, shared.NewCPT(cpt), "order-9", shared.NewCapabilitySet(), false, false)
	if err := tk.Claim("station-9", shared.NewCapabilitySet(), cpt.Add(-time.Hour), time.Hour); err != nil {
		t.Fatalf("claim: %v", err)
	}
	return tk
}

// assertPackageManifestedOnTime publishes one PackageManifested event at
// manifestedAt against a repo whose SLAM task has the given CPT, and
// asserts the resolved enrichment's fields, including the on_time verdict.
func assertPackageManifestedOnTime(t *testing.T, cpt, manifestedAt time.Time, wantOnTime bool) {
	t.Helper()

	w := &fakeAnalyticsWriter{}
	repo := fakeTaskRepo{byOrderRef: map[shared.OrderRef][]*task.Task{"order-9": {newClaimedSlamTask(t, cpt)}}}
	p := outboundkafka.NewAnalyticsPublisher(nil, repo, func() string { return "evt" })
	p.Writer = w

	evt := shared.NewPackageManifested("pkg-1", "order-9", manifestedAt)
	if err := p.Publish(context.Background(), evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(w.msgs))
	}
	env, data := decodeCE[map[string]any](t, w.msgs[0].Value)
	if env.Type() != "com.warehouse.wes.fulfillment-execution.package.PackageManifested" {
		t.Fatalf("type = %q", env.Type())
	}
	if data["resolved"] != true {
		t.Fatalf("resolved = %v, want true", data["resolved"])
	}
	if data["task_type"] != string(task.Slam) {
		t.Errorf("task_type = %v, want %v", data["task_type"], task.Slam)
	}
	if data["station_id"] != "station-9" {
		t.Errorf("station_id = %v, want station-9", data["station_id"])
	}
	if data["on_time"] != wantOnTime {
		t.Errorf("on_time = %v, want %v", data["on_time"], wantOnTime)
	}
}

// TestAnalyticsPublisher_PackageManifested_UnresolvedWhenNoSLAMTask asserts
// the fail-soft convention (ADR-0026): when no SLAM task can be found for
// the package's OrderRef (an edge case that should not happen in practice —
// every Package descends from a SLAM task by construction), the publisher
// marshals resolved=false rather than erroring the whole publish, so the
// consumer can skip recording instead of projecting a wrong dimension.
func TestAnalyticsPublisher_PackageManifested_UnresolvedWhenNoSLAMTask(t *testing.T) {
	w := &fakeAnalyticsWriter{}
	// No SLAM task for "order-missing" — byOrderRef has no entry.
	repo := fakeTaskRepo{byOrderRef: map[shared.OrderRef][]*task.Task{}}
	p := outboundkafka.NewAnalyticsPublisher(nil, repo, func() string { return "evt" })
	p.Writer = w

	if err := p.Publish(context.Background(), shared.NewPackageManifested("pkg-1", "order-missing", time.Now())); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	_, data := decodeCE[map[string]any](t, w.msgs[0].Value)
	if data["resolved"] != false {
		t.Errorf("resolved = %v, want false", data["resolved"])
	}
}
