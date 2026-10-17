package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// Golden exact-JSON tests for the two pool events on the integration topic
// (ADR 0049): TaskAvailable and TaskClaimed. Both are the consumer-first
// contract of task-interleaving; a change here is a wire-contract change.

const (
	binSource = "WH1-RSV-AMB-B02-03-02-C"
	binDest   = "WH1-FWD-AMB-A01-01-01-A"

	typeTaskAvailable = "com.warehouse.wes.fulfillment-execution.task.TaskAvailable"
	typeTaskClaimed   = "com.warehouse.wes.fulfillment-execution.task.TaskClaimed"
)

// poolFixture saves one Pending task built by mk into a memory repo and
// returns a publisher over it plus the writer it writes to.
func poolFixture(t *testing.T, tk *task.Task) (*fakeWriter, *outboundkafka.Publisher) {
	t.Helper()
	tasks := memory.NewTaskRepo()
	if err := tasks.Save(context.Background(), tk); err != nil {
		t.Fatalf("save: %v", err)
	}
	w := &fakeWriter{}
	return w, outboundkafka.NewPublisherWithWriter(w, tasks, memory.NewStationRepo(), goldenID1)
}

func relocateTask(t *testing.T, id shared.TaskId, kind, from, to string, cpt shared.CPT, caps shared.CapabilitySet) *task.Task {
	t.Helper()
	move, err := task.NewRelocationMove("plan-7", 3, kind, "SKU-1", from, to)
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	return task.NewRelocationTask(id, cpt, "slot-plan-7-move-3", caps, move)
}

func publishOne(t *testing.T, w *fakeWriter, p *outboundkafka.Publisher, ev shared.DomainEvent) []byte {
	t.Helper()
	if err := p.Publish(context.Background(), ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 1 {
		t.Fatalf("expected exactly 1 message, got %d", len(w.msgs))
	}
	return w.msgs[0].Value
}

// goldenTaskAvailable is the exact TaskAvailable of a Relocate move released
// with a CPT and two capabilities (sorted on the wire).
const goldenTaskAvailable = `{
	"specversion":"1.0",
	"id":"` + goldenID + `",
	"source":"/warehouse/fulfillment-execution",
	"type":"` + typeTaskAvailable + `",
	"subject":"task-rel-1",
	"time":"2026-09-30T15:00:00Z",
	"datacontenttype":"application/json",
	"dataschema":"urn:warehouse:fulfillment-execution:events:TaskAvailable:v1",
	"data":{"task_id":"task-rel-1","task_type":"RELOCATE","site_id":"WH1",
		"from_location":"` + binSource + `","to_location":"` + binDest + `",
		"required_capabilities":["forklift","relocate"],
		"cpt":"2026-09-30T16:00:00Z","priority_band":"RELOCATE:2026-09-30T16:00:00Z",
		"available_at":"2026-09-30T15:00:00Z"}
}`

// goldenTaskClaimed is the exact TaskClaimed integration row.
const goldenTaskClaimed = `{
	"specversion":"1.0",
	"id":"` + goldenID + `",
	"source":"/warehouse/fulfillment-execution",
	"type":"` + typeTaskClaimed + `",
	"subject":"task-rel-1",
	"time":"2026-09-30T15:00:00Z",
	"datacontenttype":"application/json",
	"dataschema":"urn:warehouse:fulfillment-execution:events:TaskClaimed:v1",
	"data":{"task_id":"task-rel-1","task_type":"RELOCATE","station_id":"station-07","claimed_at":"2026-09-30T15:00:00Z"}
}`

func goldenRelocate(t *testing.T) *task.Task {
	t.Helper()
	return relocateTask(t, "task-rel-1", "Relocate", binSource, binDest,
		shared.NewCPT(goldenAt.Add(time.Hour)), shared.NewCapabilitySet("relocate", "forklift"))
}

func TestGolden_Integration_TaskAvailable(t *testing.T) {
	w, p := poolFixture(t, goldenRelocate(t))
	publishOne(t, w, p, shared.NewTaskAvailable("task-rel-1", goldenAt))
	assertGolden(t, w.msgs[0], "task-rel-1", goldenTaskAvailable)
}

// Both pool events leave on the integration topic (the outbox row's topic),
// keyed by task id.
func TestEncode_PoolEventsGoToTheIntegrationTopic(t *testing.T) {
	_, p := poolFixture(t, goldenRelocate(t))
	encs, err := p.Encode(context.Background(), shared.NewTaskAvailable("task-rel-1", goldenAt), shared.NewTaskClaimed("task-rel-1", "station-07", goldenAt))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encs) != 2 {
		t.Fatalf("expected 2 encoded messages, got %d", len(encs))
	}
	for i, wantType := range []string{typeTaskAvailable, typeTaskClaimed} {
		if encs[i].Topic != outboundkafka.Topic || encs[i].EventType != wantType || string(encs[i].Key) != "task-rel-1" {
			t.Errorf("message %d = topic %q type %q key %q", i, encs[i].Topic, encs[i].EventType, encs[i].Key)
		}
	}
}

func TestGolden_Integration_TaskClaimed(t *testing.T) {
	w, p := poolFixture(t, goldenRelocate(t))
	publishOne(t, w, p, shared.NewTaskClaimed("task-rel-1", "station-07", goldenAt))
	assertGolden(t, w.msgs[0], "task-rel-1", goldenTaskClaimed)
}

// availableData decodes the data member of a published TaskAvailable.
func availableData(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var env struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return env.Data
}

// A task released without a CPT omits cpt (never "" / the zero time) and one
// without capabilities carries an empty array, never null.
func TestGolden_TaskAvailable_NoCPTAndNoCapabilities(t *testing.T) {
	tk := relocateTask(t, "task-rel-2", "Relocate", binSource, binDest, shared.NewCPT(time.Time{}), shared.NewCapabilitySet())
	w, p := poolFixture(t, tk)
	data := availableData(t, publishOne(t, w, p, shared.NewTaskAvailable("task-rel-2", goldenAt)))
	if _, ok := data["cpt"]; ok {
		t.Errorf("cpt must be omitted when the task has none, data = %v", data)
	}
	caps, ok := data["required_capabilities"].([]any)
	if !ok || len(caps) != 0 {
		t.Errorf("required_capabilities = %#v, want an empty array", data["required_capabilities"])
	}
}

// An Assign leaves the source open and a Vacate the destination (the operator
// scans them at completion): the one known bin stands for both ends.
func TestGolden_TaskAvailable_OpenSideMoves(t *testing.T) {
	cases := []struct {
		name, kind, from, to, want string
	}{
		{"assign", "Assign", "", binDest, binDest},
		{"vacate", "Vacate", binSource, "", binSource},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tk := relocateTask(t, "task-open", tc.kind, tc.from, tc.to, shared.NewCPT(goldenAt), shared.NewCapabilitySet("relocate"))
			w, p := poolFixture(t, tk)
			data := availableData(t, publishOne(t, w, p, shared.NewTaskAvailable("task-open", goldenAt)))
			if data["from_location"] != tc.want || data["to_location"] != tc.want || data["site_id"] != "WH1" {
				t.Errorf("from/to/site = %v / %v / %v, want %s / %s / WH1", data["from_location"], data["to_location"], data["site_id"], tc.want, tc.want)
			}
		})
	}
}

// A task with no location in this service (every type but RELOCATE today,
// ADR 0049), and a task that cannot be found, publish no TaskAvailable.
func TestPublish_TaskAvailable_NotAnnouncedWithoutPlacement(t *testing.T) {
	pick := task.New("task-p", task.Pick, shared.NewCPT(goldenAt), "wu-p", shared.NewCapabilitySet("pick"), false, false)
	w, p := poolFixture(t, pick)
	if err := p.Publish(context.Background(), shared.NewTaskAvailable("task-p", goldenAt), shared.NewTaskAvailable("ghost", goldenAt)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 0 {
		t.Fatalf("expected no TaskAvailable for a PICK task or an unknown task, got %d", len(w.msgs))
	}
}

// A RELOCATE task whose bins carry no determinable site is not announced.
func TestPublish_TaskAvailable_NotAnnouncedWithoutSite(t *testing.T) {
	tk := relocateTask(t, "task-nosite", "Relocate", "FWD-A", "RSV-2", shared.NewCPT(goldenAt), shared.NewCapabilitySet("relocate"))
	w, p := poolFixture(t, tk)
	if err := p.Publish(context.Background(), shared.NewTaskAvailable("task-nosite", goldenAt)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 0 {
		t.Fatalf("expected no TaskAvailable without a site, got %d", len(w.msgs))
	}
}

// TaskClaimed is published for every task type (it leaves the pool), carrying
// the task type read off the task.
func TestPublish_TaskClaimed_AnyTaskType(t *testing.T) {
	pick := task.New("task-p", task.Pick, shared.NewCPT(goldenAt), "wu-p", shared.NewCapabilitySet("pick"), false, false)
	w, p := poolFixture(t, pick)
	data := availableData(t, publishOne(t, w, p, shared.NewTaskClaimed("task-p", "s1", goldenAt)))
	if data["task_type"] != "PICK" || data["station_id"] != "s1" || data["claimed_at"] != "2026-09-30T15:00:00Z" {
		t.Fatalf("TaskClaimed data = %v", data)
	}
}

// consumerTaskAvailable and consumerTaskClaimed are field-for-field copies of
// task-interleaving's taskAvailableData / taskClaimedData
// (internal/adapters/inbound/kafka/fulfillment_consumer.go on its develop).
// They are restated here, never imported: if this test needs an edit, the
// consumer's contract changed.
type consumerTaskAvailable struct {
	TaskID               string   `json:"task_id"`
	TaskType             string   `json:"task_type"`
	SiteID               string   `json:"site_id"`
	FromLocation         string   `json:"from_location"`
	ToLocation           string   `json:"to_location"`
	RequiredCapabilities []string `json:"required_capabilities"`
	PriorityBand         string   `json:"priority_band"`
}

type consumerTaskClaimed struct {
	TaskID    string    `json:"task_id"`
	TaskType  string    `json:"task_type"`
	StationID string    `json:"station_id"`
	ClaimedAt time.Time `json:"claimed_at"`
}

func TestGolden_TaskAvailable_DecodesAsTaskInterleavingExpects(t *testing.T) {
	e, got := decodeCE[consumerTaskAvailable](t, []byte(goldenTaskAvailable))
	if e.Type() != typeTaskAvailable {
		t.Fatalf("type = %q", e.Type())
	}
	want := consumerTaskAvailable{
		TaskID: "task-rel-1", TaskType: "RELOCATE", SiteID: "WH1",
		FromLocation: binSource, ToLocation: binDest,
		RequiredCapabilities: []string{"forklift", "relocate"},
		PriorityBand:         "RELOCATE:2026-09-30T16:00:00Z",
	}
	if got.TaskID != want.TaskID || got.TaskType != want.TaskType || got.SiteID != want.SiteID ||
		got.FromLocation != want.FromLocation || got.ToLocation != want.ToLocation ||
		got.PriorityBand != want.PriorityBand || len(got.RequiredCapabilities) != 2 ||
		got.RequiredCapabilities[0] != "forklift" || got.RequiredCapabilities[1] != "relocate" {
		t.Fatalf("decoded = %+v, want %+v", got, want)
	}
}

func TestGolden_TaskClaimed_DecodesAsTaskInterleavingExpects(t *testing.T) {
	e, got := decodeCE[consumerTaskClaimed](t, []byte(goldenTaskClaimed))
	if e.Type() != typeTaskClaimed {
		t.Fatalf("type = %q", e.Type())
	}
	if got.TaskID != "task-rel-1" || got.TaskType != "RELOCATE" || got.StationID != "station-07" ||
		!got.ClaimedAt.Equal(time.Date(2026, 9, 30, 15, 0, 0, 0, time.UTC)) {
		t.Fatalf("decoded = %+v", got)
	}
}

// The pinned shape has exactly these members: a field added or renamed here
// without the consumer is a contract change.
func TestGolden_PoolEvents_ExactMembers(t *testing.T) {
	cases := []struct {
		name, raw string
		want      []string
	}{
		{"TaskAvailable", goldenTaskAvailable, []string{"task_id", "task_type", "site_id", "from_location", "to_location", "required_capabilities", "cpt", "priority_band", "available_at"}},
		{"TaskClaimed", goldenTaskClaimed, []string{"task_id", "task_type", "station_id", "claimed_at"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, data := decodeCE[map[string]any](t, []byte(tc.raw))
			if len(data) != len(tc.want) {
				t.Fatalf("members = %v, want %v", data, tc.want)
			}
			for _, k := range tc.want {
				if _, ok := data[k]; !ok {
					t.Errorf("missing member %q", k)
				}
			}
		})
	}
}
