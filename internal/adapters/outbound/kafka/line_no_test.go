package kafka_test

import (
	"context"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// line_no (decision 18, per-line confirm-pick, ADR 0041) is the 1-based
// order line the completed task's work was released for (Task.SourceLineNo,
// from WorkReleased.line_no), next to order_ref. Additive and optional:
// present only when known, ABSENT from the JSON (never 0) otherwise.

func newTestTaskFromOrderLine(t *testing.T, tasks *memory.TaskRepo, orderRef shared.OrderRef, sourceOrderId string, lineNo int) {
	t.Helper()
	tk := task.New("task-1", task.Pick, shared.NewCPT(epoch.Add(time.Hour)), orderRef, shared.NewCapabilitySet("pick"), false, false).
		WithSourceOrderId(sourceOrderId).
		WithSourceLineNo(lineNo)
	if err := tasks.Save(context.Background(), tk); err != nil {
		t.Fatalf("save task: %v", err)
	}
}

func TestPublish_TaskCompletedCarriesLineNo(t *testing.T) {
	tasks := memory.NewTaskRepo()
	newTestTaskFromOrderLine(t, tasks, "ord-77-line-2", "ord-77", 2)
	w := &fakeWriter{}
	p := &outboundkafka.Publisher{Writer: w, Tasks: tasks, NewId: func() string { return "evt-1" }}

	if err := p.Publish(context.Background(), shared.NewTaskCompleted("task-1", "station-1", epoch)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, data := decodeCE[outboundkafka.TaskCompletedData](t, w.msgs[0].Value)
	if data.LineNo != 2 {
		t.Errorf("Data.LineNo = %d, want 2", data.LineNo)
	}
	if data.OrderRef != "ord-77" {
		t.Errorf("Data.OrderRef = %q, want ord-77 (unchanged)", data.OrderRef)
	}
}

func TestPublish_TaskCompletedOmitsLineNoWhenUnknown(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, tasks *memory.TaskRepo)
	}{
		// The "-line-3" suffix of the work unit id must never be parsed.
		{"order task without a line", func(t *testing.T, tasks *memory.TaskRepo) {
			newTestTaskFromOrderLine(t, tasks, "ord-77-line-3", "ord-77", 0)
		}},
		{"task not found", func(*testing.T, *memory.TaskRepo) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tasks := memory.NewTaskRepo()
			tc.setup(t, tasks)
			w := &fakeWriter{}
			p := &outboundkafka.Publisher{Writer: w, Tasks: tasks, NewId: func() string { return "evt-1" }}
			if err := p.Publish(context.Background(), shared.NewTaskCompleted("task-1", "station-1", epoch)); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			_, data := decodeCE[map[string]any](t, w.msgs[0].Value)
			if _, present := data["line_no"]; present {
				t.Errorf("line_no must be omitted when unknown, got %v", data)
			}
		})
	}
}

func TestAnalyticsPublisher_TaskCompleted_CarriesLineNo(t *testing.T) {
	for _, tc := range []struct {
		name    string
		repo    fakeTaskRepo
		want    float64
		wantKey bool
	}{
		{"line set", fakeTaskRepo{taskType: task.Pick, found: true, sourceLineNo: 3}, 3, true},
		{"line unknown is omitted", fakeTaskRepo{taskType: task.Pick, found: true}, 0, false},
		{"task not found is omitted", fakeTaskRepo{}, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &fakeAnalyticsWriter{}
			p := outboundkafka.NewAnalyticsPublisherWithWriter(w, tc.repo, func() string { return "evt" })
			if err := p.Publish(context.Background(), shared.NewTaskCompleted("t1", "s1", time.Now())); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			_, data := decodeCE[map[string]any](t, w.msgs[0].Value)
			got, present := data["line_no"]
			if present != tc.wantKey {
				t.Fatalf("line_no present = %v, want %v (data=%v)", present, tc.wantKey, data)
			}
			if tc.wantKey && got != tc.want {
				t.Errorf("line_no = %v, want %v", got, tc.want)
			}
		})
	}
}

// line_no must not leak onto the other task-scoped analytics events.
func TestAnalyticsPublisher_LineNoOnlyOnTaskCompleted(t *testing.T) {
	w := &fakeAnalyticsWriter{}
	p := outboundkafka.NewAnalyticsPublisherWithWriter(w, fakeTaskRepo{taskType: task.Pick, found: true, sourceLineNo: 3}, func() string { return "evt" })
	at := time.Now()
	if err := p.Publish(context.Background(),
		shared.NewTaskCreated("t1", at),
		shared.NewTaskClaimed("t1", "s1", at),
		shared.NewLeaseExpired("t1", at),
		shared.NewItemPicked("t1", at),
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	for _, m := range w.msgs {
		if _, data := decodeCE[map[string]any](t, m.Value); data["line_no"] != nil {
			t.Errorf("line_no leaked onto %s: %v", string(m.Key), data)
		}
	}
}

// Byte-exact goldens: the integration TaskCompleted with a known line. The
// pre-existing goldens (no line) stay byte-identical, which is the
// backward-compatibility proof: the diff is the one field, only where set.
func TestGolden_Integration_TaskCompleted_WithLineNo(t *testing.T) {
	tasks := memory.NewTaskRepo()
	stations := memory.NewStationRepo()
	claimedAt := goldenAt.Add(-245 * time.Second)
	tk := task.New("task-8a1f", task.Pick, shared.NewCPT(goldenAt.Add(time.Hour)), "order-8a1f-line-2", shared.NewCapabilitySet("pick"), false, false).
		WithSourceOrderId("order-8a1f").WithSourceLineNo(2)
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
	assertGolden(t, w.msgs[0], "task-8a1f", `{
		"specversion":"1.0",
		"id":"`+goldenID+`",
		"source":"/warehouse/fulfillment-execution",
		"type":"com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
		"subject":"task-8a1f",
		"time":"2026-09-30T15:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:fulfillment-execution:events:TaskCompleted:v1",
		"data":{"task_id":"task-8a1f","station_id":"station-03","work_unit_id":"order-8a1f-line-2","associate_id":"worker-42","duration_seconds":245,"task_type":"PICK","order_ref":"order-8a1f","line_no":2}
	}`)
}

func TestGolden_Analytics_TaskCompleted_WithLineNo(t *testing.T) {
	repo := fakeTaskRepo{taskType: task.Pick, found: true, sourceLineNo: 2}
	w := &fakeAnalyticsWriter{}
	p := outboundkafka.NewAnalyticsPublisherWithWriter(w, repo, goldenID1)
	if err := p.Publish(context.Background(), shared.NewTaskCompleted("t1", "s1", goldenAt)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	assertGolden(t, w.msgs[0], "t1", `{
		"specversion":"1.0",
		"id":"`+goldenID+`",
		"source":"/warehouse/fulfillment-execution",
		"type":"com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
		"subject":"t1",
		"time":"2026-09-30T15:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:fulfillment-execution:analytics:TaskCompleted:v1",
		"data":{"task_id":"t1","task_type":"PICK","station_id":"s1","order_ref":"order-1","line_no":2}
	}`)
}
