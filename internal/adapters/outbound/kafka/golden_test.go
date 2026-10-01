package kafka_test

import (
	"context"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// Golden exact-JSON tests (ADR-0032): one per published CloudEvents
// `type`, on both the integration and the analytics stream. Each asserts
// every required context attribute, the full type string, the Kafka key,
// and the structured-mode content-type header. `data` is the
// byte-identical payload shape this service published before the
// CloudEvents cutover.

var goldenAt = time.Date(2026, 9, 30, 12, 0, 0, 0, time.FixedZone("BRT", -3*3600))

const goldenID = "6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b"

func goldenID1() string { return goldenID }

// assertGolden checks one written message against its expected value.
func assertGolden(t *testing.T, msg kafkago.Message, wantKey, want string) {
	t.Helper()
	if string(msg.Key) != wantKey {
		t.Errorf("key = %q, want %q", msg.Key, wantKey)
	}
	if !hasContentTypeHeader(msg.Headers) {
		t.Errorf("missing content-type: application/cloudevents+json; charset=UTF-8 header, got %v", msg.Headers)
	}
	assertJSONEqual(t, msg.Value, want)
}

func TestGolden_Integration_TaskCompleted(t *testing.T) {
	tasks := memory.NewTaskRepo()
	stations := memory.NewStationRepo()
	claimedAt := goldenAt.Add(-245 * time.Second)
	tk := task.New("task-8a1f", task.Pick, shared.NewCPT(goldenAt.Add(time.Hour)), "wu-8a1f", shared.NewCapabilitySet("pick"), false, false)
	if err := tk.Claim("station-03", shared.NewCapabilitySet("pick"), claimedAt, 10*time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	_ = tasks.Save(context.Background(), tk)
	st := station.New("station-03", shared.NewCapabilitySet("pick"))
	_ = st.CheckIn("worker-42")
	_ = stations.Save(context.Background(), st)

	w := &fakeWriter{}
	p := outboundkafka.NewPublisherWithWriter(w, tasks, stations, goldenID1)
	if err := p.Publish(context.Background(), shared.NewTaskCompleted("task-8a1f", "station-03", goldenAt)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(w.msgs))
	}
	assertGolden(t, w.msgs[0], "task-8a1f", `{
		"specversion":"1.0",
		"id":"`+goldenID+`",
		"source":"/warehouse/fulfillment-execution",
		"type":"com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
		"subject":"task-8a1f",
		"time":"2026-09-30T15:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:fulfillment-execution:events:TaskCompleted:v1",
		"data":{"task_id":"task-8a1f","station_id":"station-03","work_unit_id":"wu-8a1f","associate_id":"worker-42","duration_seconds":245,"task_type":"PICK"}
	}`)
}

func TestGolden_Integration_TaskCPTMissed(t *testing.T) {
	w := &fakeWriter{}
	p := outboundkafka.NewPublisherWithWriter(w, nil, nil, goldenID1)
	cpt := time.Date(2026, 9, 30, 14, 55, 0, 0, time.UTC)
	if err := p.Publish(context.Background(), shared.NewTaskCPTMissed("task-8a1f", "wu-8a1f", "PICK", cpt, goldenAt)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	assertGolden(t, w.msgs[0], "task-8a1f", `{
		"specversion":"1.0",
		"id":"`+goldenID+`",
		"source":"/warehouse/fulfillment-execution",
		"type":"com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed",
		"subject":"task-8a1f",
		"time":"2026-09-30T15:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:fulfillment-execution:events:TaskCPTMissed:v1",
		"data":{"task_id":"task-8a1f","order_ref":"wu-8a1f","task_type":"PICK","cpt":"2026-09-30T14:55:00Z"}
	}`)
}

func TestGolden_Integration_PackageManifested(t *testing.T) {
	w := &fakeWriter{}
	p := outboundkafka.NewPublisherWithWriter(w, nil, nil, goldenID1)
	if err := p.Publish(context.Background(), shared.NewPackageManifested("pkg-1029", "wu-8a1f", goldenAt)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	assertGolden(t, w.msgs[0], "pkg-1029", `{
		"specversion":"1.0",
		"id":"`+goldenID+`",
		"source":"/warehouse/fulfillment-execution",
		"type":"com.warehouse.wes.fulfillment-execution.package.PackageManifested",
		"subject":"pkg-1029",
		"time":"2026-09-30T15:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:fulfillment-execution:events:PackageManifested:v1",
		"data":{"package_id":"pkg-1029","order_ref":"wu-8a1f"}
	}`)
}

// goldenAnalyticsCase is one analytics-stream golden: the event, its
// expected key/subject, entity segment, and exact data payload.
type goldenAnalyticsCase struct {
	event  shared.DomainEvent
	name   string
	entity string
	key    string
	data   string
}

func TestGolden_Analytics_EveryType(t *testing.T) {
	cpt := goldenAt.Add(time.Hour)
	slam := task.New("slam-1", task.Slam, shared.NewCPT(cpt), "wu-8a1f", shared.NewCapabilitySet(), false, false)
	if err := slam.Claim("station-09", shared.NewCapabilitySet(), goldenAt.Add(-time.Minute), time.Hour); err != nil {
		t.Fatalf("claim: %v", err)
	}
	repo := fakeTaskRepo{taskType: task.Pick, found: true, byOrderRef: map[shared.OrderRef][]*task.Task{"wu-8a1f": {slam}}}

	cases := []goldenAnalyticsCase{
		{shared.NewTaskCreated("t1", goldenAt), "TaskCreated", "task", "t1", `{"task_id":"t1","task_type":"PICK"}`},
		{shared.NewTaskClaimed("t1", "s1", goldenAt), "TaskClaimed", "task", "t1", `{"task_id":"t1","task_type":"PICK","station_id":"s1"}`},
		{shared.NewLeaseExpired("t1", goldenAt), "LeaseExpired", "task", "t1", `{"task_id":"t1","task_type":"PICK"}`},
		{shared.NewTaskCompleted("t1", "s1", goldenAt), "TaskCompleted", "task", "t1", `{"task_id":"t1","task_type":"PICK","station_id":"s1"}`},
		{shared.NewItemPicked("t1", goldenAt), "ItemPicked", "task", "t1", `{"task_id":"t1","task_type":"PICK"}`},
		{shared.NewPackageSealed("p1", goldenAt), "PackageSealed", "package", "p1", `{"package_id":"p1"}`},
		{shared.NewWeightDiscrepancyDetected("p1", 1000, 1200, goldenAt), "WeightDiscrepancyDetected", "package", "p1", `{"package_id":"p1","expected_g":1000,"actual_g":1200}`},
		{shared.NewLabelApplied("p1", goldenAt), "LabelApplied", "package", "p1", `{"package_id":"p1"}`},
		{shared.NewPackageDiverted("p1", goldenAt), "PackageDiverted", "package", "p1", `{"package_id":"p1"}`},
		{shared.NewPackageManifested("p1", "wu-8a1f", goldenAt), "PackageManifested", "package", "p1", `{"package_id":"p1","order_ref":"wu-8a1f","task_type":"SLAM","station_id":"station-09","on_time":true,"resolved":true}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := &fakeAnalyticsWriter{}
			p := outboundkafka.NewAnalyticsPublisherWithWriter(w, repo, goldenID1)
			if err := p.Publish(context.Background(), tc.event); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if len(w.msgs) != 1 {
				t.Fatalf("expected 1 message, got %d", len(w.msgs))
			}
			assertGolden(t, w.msgs[0], tc.key, `{
				"specversion":"1.0",
				"id":"`+goldenID+`",
				"source":"/warehouse/fulfillment-execution",
				"type":"com.warehouse.wes.fulfillment-execution.`+tc.entity+`.`+tc.name+`",
				"subject":"`+tc.key+`",
				"time":"2026-09-30T15:00:00Z",
				"datacontenttype":"application/json",
				"dataschema":"urn:warehouse:fulfillment-execution:analytics:`+tc.name+`:v1",
				"data":`+tc.data+`
			}`)
		})
	}
}
