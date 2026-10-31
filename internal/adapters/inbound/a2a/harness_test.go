package a2a

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// harness builds Deps over in-memory repos, seeding through the real use
// cases with a fixed clock - the same approach as the MCP adapter's harness.
type harness struct {
	deps      Deps
	tasks     *memory.TaskRepo
	publisher *events.BufferedPublisher
	create    *usecases.CreateTask
	claim     *usecases.ClaimNext
	station   *usecases.RegisterStation
	clock     *memory.FixedClock
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	tasks := memory.NewTaskRepo()
	stations := memory.NewStationRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(base)

	n := 0
	newId := func() shared.TaskId {
		n++
		return shared.TaskId([]byte{'t', byte('0' + n)})
	}
	return &harness{
		deps: Deps{
			GetQueueDepth: &usecases.GetQueueDepth{Tasks: tasks},
			CompleteTask:  &usecases.CompleteTask{Tasks: tasks, Publisher: publisher, Clock: clock},
			Tasks:         tasks,
			Now:           clock.Now,
		},
		tasks:     tasks,
		publisher: publisher,
		create:    &usecases.CreateTask{Tasks: tasks, Publisher: publisher, Clock: clock, NewId: newId},
		claim:     &usecases.ClaimNext{Tasks: tasks, Stations: stations, Publisher: publisher, Clock: clock},
		station:   &usecases.RegisterStation{Stations: stations, Publisher: publisher},
		clock:     clock,
	}
}

func (h *harness) seedPending(t *testing.T, tt task.Type, orderRef string, cptOffset time.Duration, capability string) {
	t.Helper()
	_, err := h.create.Execute(context.Background(), tt, shared.NewCPT(base.Add(cptOffset)), shared.OrderRef(orderRef), shared.NewCapabilitySet(shared.Capability(capability)), false, false)
	if err != nil {
		t.Fatalf("seedPending: %v", err)
	}
}

func (h *harness) registerStation(t *testing.T, id, capability string) {
	t.Helper()
	if _, err := h.station.Execute(context.Background(), id, []string{capability}, ""); err != nil {
		t.Fatalf("register station: %v", err)
	}
}

// claimedPick seeds one PICK task claimed by station s1 and returns its id.
func (h *harness) claimedPick(t *testing.T) string {
	t.Helper()
	h.registerStation(t, "s1", "pick")
	h.seedPending(t, task.Pick, "o1", time.Hour, "pick")
	claimed, err := h.claim.Execute(context.Background(), "s1", task.Pick)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return string(claimed.Id())
}

// ---- the wire: JSON-RPC over HTTP, driven by the SDK's own client ----------

// wireEnvelope is a TEST-LOCAL copy of the answer contract. If the adapter's
// envelope drifts, this suite fails instead of silently following it.
type wireEnvelope struct {
	Skill string `json:"skill"`
	Data  struct {
		BacklogStatus *struct {
			ProcessPath    string `json:"processPath"`
			Depth          int    `json:"depth"`
			CandidateCount int    `json:"candidateCount"`
			Best           *struct {
				TaskId   string `json:"taskId"`
				Type     string `json:"type"`
				CPT      string `json:"cpt"`
				OrderRef string `json:"orderRef"`
			} `json:"best"`
			StuckCount int `json:"stuckCount"`
			Stuck      []struct {
				TaskId         string `json:"taskId"`
				Type           string `json:"type"`
				LeaseStationId string `json:"leaseStationId"`
				Reason         string `json:"reason"`
			} `json:"stuck"`
		} `json:"backlogStatus"`
		TaskCompleted *struct {
			TaskId    string `json:"taskId"`
			StationId string `json:"stationId"`
			Completed bool   `json:"completed"`
		} `json:"taskCompleted"`
	} `json:"data"`
	Problem *struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Detail string `json:"detail"`
		Status int    `json:"status"`
	} `json:"problem"`
}

type wire struct {
	client *a2aclient.Client
}

func newWire(t *testing.T, deps Deps) *wire {
	t.Helper()
	srv := httptest.NewServer(Handler(NewExecutor(deps)))
	t.Cleanup(srv.Close)
	card := Card(Config{AgentURL: srv.URL + "/", Version: "test"})
	client, err := a2aclient.NewFromCard(context.Background(), card)
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}
	return &wire{client: client}
}

// send sends one message and returns the raw reply message.
func (w *wire) send(t *testing.T, msg *a2a.Message) *a2a.Message {
	t.Helper()
	resp, err := w.client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: msg})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	reply, ok := resp.(*a2a.Message)
	if !ok || len(reply.Parts) < 2 {
		t.Fatalf("reply = %T, want a message with a data and a text part", resp)
	}
	return reply
}

// decode extracts the envelope and the one-line text summary from a reply.
func decode(t *testing.T, reply *a2a.Message) (wireEnvelope, string) {
	t.Helper()
	data, ok := reply.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("first reply part is %T, want data", reply.Parts[0].Content)
	}
	raw, _ := json.Marshal(data.Value)
	var env wireEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("reply is not the skill envelope: %v (%s)", err, raw)
	}
	text, _ := reply.Parts[1].Content.(a2a.Text)
	return env, string(text)
}

// call sends one message part and returns the decoded envelope and summary.
func (w *wire) call(t *testing.T, part *a2a.Part) (wireEnvelope, string) {
	t.Helper()
	return decode(t, w.send(t, a2a.NewMessage(a2a.MessageRoleUser, part)))
}

func (w *wire) skill(t *testing.T, skill string, args map[string]any) (wireEnvelope, string) {
	t.Helper()
	return w.call(t, a2a.NewDataPart(map[string]any{"skill": skill, "arguments": args}))
}

func wantProblem(t *testing.T, env wireEnvelope, status int, slug string) {
	t.Helper()
	if env.Problem == nil {
		t.Fatalf("expected a %d problem, got data: %+v", status, env.Data)
	}
	if env.Problem.Status != status || env.Problem.Type != problemTypeBase+slug {
		t.Fatalf("problem = %+v, want %d %s", env.Problem, status, slug)
	}
}
