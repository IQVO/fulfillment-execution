package a2a

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

func TestWireBacklogStatusReadsDepthBestAndLeases(t *testing.T) {
	h := newHarness(t)
	h.registerStation(t, "s1", "pick")
	h.seedPending(t, task.Pick, "o-late", 2*time.Hour, "pick")
	h.seedPending(t, task.Pick, "o-early", time.Hour, "pick")
	h.seedPending(t, task.Pack, "o-pack", time.Hour, "pack")
	if _, err := h.claim.Execute(context.Background(), "s1", task.Pick); err != nil {
		t.Fatal(err)
	}
	w := newWire(t, h.deps)

	env, text := w.skill(t, skillBacklogStatus, map[string]any{"processPath": "PICK"})
	b := env.Data.BacklogStatus
	if env.Problem != nil || b == nil {
		t.Fatalf("envelope = %+v, want backlogStatus data", env)
	}
	if env.Skill != skillBacklogStatus || b.ProcessPath != "PICK" || b.Depth != 1 || b.CandidateCount != 1 {
		t.Fatalf("backlog = %+v, want PICK depth 1 / 1 claimable (the other PICK is claimed, the PACK is another path)", b)
	}
	if b.Best == nil || b.Best.OrderRef != "o-late" || b.Best.Type != "PICK" {
		t.Fatalf("best = %+v, want the remaining PICK task o-late", b.Best)
	}
	if b.StuckCount != 0 || len(b.Stuck) != 0 {
		t.Fatalf("a healthy lease was flagged: %+v", b.Stuck)
	}
	if !strings.Contains(text, "PICK: 1 pending, 1 claimable") {
		t.Errorf("summary = %q", text)
	}
}

func TestWireBacklogStatusFlagsLapsingLeasesOfThatPathOnly(t *testing.T) {
	h := newHarness(t)
	h.registerStation(t, "s1", "pick")
	h.seedPending(t, task.Pick, "o1", time.Hour, "pick")
	if _, err := h.claim.Execute(context.Background(), "s1", task.Pick); err != nil {
		t.Fatal(err)
	}
	w := newWire(t, h.deps)

	// The 5-minute lease is healthy at base: only a wide window flags it.
	env, _ := w.skill(t, skillBacklogStatus, map[string]any{"processPath": "PICK"})
	if env.Data.BacklogStatus == nil || env.Data.BacklogStatus.StuckCount != 0 {
		t.Fatalf("healthy lease flagged with the default window: %+v", env)
	}
	env, _ = w.skill(t, skillBacklogStatus, map[string]any{"processPath": "PICK", "withinSeconds": 600})
	if s := env.Data.BacklogStatus; s == nil || s.StuckCount != 1 || s.Stuck[0].LeaseStationId != "s1" || s.Stuck[0].Reason == "" {
		t.Fatalf("expiring lease not flagged within 600s: %+v", env.Data.BacklogStatus)
	}
	// A different path never sees it.
	env, _ = w.skill(t, skillBacklogStatus, map[string]any{"processPath": "PACK", "withinSeconds": 600})
	if s := env.Data.BacklogStatus; s == nil || s.StuckCount != 0 {
		t.Fatalf("a PICK lease leaked into the PACK answer: %+v", env.Data.BacklogStatus)
	}
	// Past expiry the default window flags it too.
	h.clock.Advance(6 * time.Minute)
	env, _ = w.skill(t, skillBacklogStatus, map[string]any{"processPath": "PICK"})
	if s := env.Data.BacklogStatus; s == nil || s.StuckCount != 1 || !strings.Contains(s.Stuck[0].Reason, "already expired") {
		t.Fatalf("expired lease not flagged: %+v", env.Data.BacklogStatus)
	}
}

func TestWireBacklogStatusEmptyQueue(t *testing.T) {
	w := newWire(t, newHarness(t).deps)
	env, _ := w.skill(t, skillBacklogStatus, map[string]any{"processPath": "SLAM"})
	b := env.Data.BacklogStatus
	if b == nil || b.Depth != 0 || b.CandidateCount != 0 || b.Best != nil || b.StuckCount != 0 {
		t.Fatalf("empty queue = %+v, want zeros and no best (not an error)", b)
	}
}

func TestWireBacklogStatusAcceptsEveryKnownPath(t *testing.T) {
	w := newWire(t, newHarness(t).deps)
	for _, p := range []string{"PICK", "PACK", "SLAM", "RELOCATE", "REPLENISH", "BREAK_CASE"} {
		env, _ := w.skill(t, skillBacklogStatus, map[string]any{"processPath": p})
		if env.Problem != nil || env.Data.BacklogStatus == nil || env.Data.BacklogStatus.ProcessPath != p {
			t.Errorf("%s: envelope = %+v", p, env)
		}
	}
}

func TestWireTextPartHoldingTheSkillJSONIsAccepted(t *testing.T) {
	w := newWire(t, newHarness(t).deps)
	env, _ := w.call(t, a2a.NewTextPart(`{"skill":"backlog_status","arguments":{"processPath":"PICK"}}`))
	if env.Problem != nil || env.Data.BacklogStatus == nil {
		t.Fatalf("envelope = %+v, want backlogStatus data", env)
	}
}

func TestWireReplyKeepsTheCallersContext(t *testing.T) {
	w := newWire(t, newHarness(t).deps)
	msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{"skill": skillBacklogStatus, "arguments": map[string]any{"processPath": "PICK"}}))
	msg.ContextID = "saga-42"
	reply := w.send(t, msg)
	if reply.ContextID != "saga-42" || reply.Role != a2a.MessageRoleAgent {
		t.Fatalf("reply context/role = %q/%q, want saga-42/agent", reply.ContextID, reply.Role)
	}
}

// Every request-shape failure is ANSWERED in-band as a 400 problem, never a
// JSON-RPC error, and the summary text part carries the same status.
func TestWireArgumentValidation(t *testing.T) {
	w := newWire(t, newHarness(t).deps)
	skill := func(s string, args any) *a2a.Part {
		return a2a.NewDataPart(map[string]any{"skill": s, "arguments": args})
	}
	cases := []struct {
		name string
		part *a2a.Part
		slug string
		want string
	}{
		{"unknown skill", skill("claim_next_task", map[string]any{}), "unknown-skill", `"claim_next_task" is not offered`},
		{"no skill field", a2a.NewDataPart(map[string]any{"arguments": map[string]any{}}), "malformed-skill-request", `no "skill"`},
		{"unknown top-level field", a2a.NewDataPart(map[string]any{"skill": skillBacklogStatus, "args": 1}), "malformed-skill-request", "unknown field"},
		{"plain text is not a skill request", a2a.NewTextPart("how deep is the pick queue?"), "malformed-skill-request", "not valid"},
		{"trailing data after the document", a2a.NewTextPart(`{"skill":"backlog_status","arguments":{"processPath":"PICK"}} {"x":1}`), "malformed-skill-request", "after the JSON document"},
		{"missing processPath", skill(skillBacklogStatus, map[string]any{}), "invalid-request", "processPath is required"},
		{"blank processPath", skill(skillBacklogStatus, map[string]any{"processPath": "  "}), "invalid-request", "processPath is required"},
		{"unknown processPath", skill(skillBacklogStatus, map[string]any{"processPath": "FLY"}), "invalid-request", `unknown process path "FLY"`},
		{"lowercase processPath is not a path", skill(skillBacklogStatus, map[string]any{"processPath": "pick"}), "invalid-request", `unknown process path "pick"`},
		{"misspelled optional argument", skill(skillBacklogStatus, map[string]any{"processPath": "PICK", "withinSecs": 60}), "invalid-request", `unknown field "withinSecs"`},
		{"negative withinSeconds", skill(skillBacklogStatus, map[string]any{"processPath": "PICK", "withinSeconds": -1}), "invalid-request", "must not be negative"},
		{"withinSeconds of the wrong type", skill(skillBacklogStatus, map[string]any{"processPath": "PICK", "withinSeconds": "60"}), "invalid-request", "withinSeconds"},
		{"arguments is not an object", skill(skillBacklogStatus, "PICK"), "invalid-request", "arguments"},
		{"complete_task without taskId", skill(skillCompleteTask, map[string]any{"stationId": "s1"}), "invalid-request", "taskId is required"},
		{"complete_task without stationId", skill(skillCompleteTask, map[string]any{"taskId": "t1"}), "invalid-request", "stationId is required"},
		{"complete_task quantity 0", skill(skillCompleteTask, map[string]any{"taskId": "t1", "stationId": "s1", "quantity": 0}), "invalid-request", "quantity must be at least 1"},
		{"complete_task blank fromBin", skill(skillCompleteTask, map[string]any{"taskId": "t1", "stationId": "s1", "fromBin": " \u00a0"}), "invalid-request", "fromBin must not be blank"},
		{"complete_task blank toBin", skill(skillCompleteTask, map[string]any{"taskId": "t1", "stationId": "s1", "toBin": ""}), "invalid-request", "toBin must not be blank"},
		{"complete_task misspelled argument", skill(skillCompleteTask, map[string]any{"taskId": "t1", "stationId": "s1", "qty": 1}), "invalid-request", `unknown field "qty"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env, text := w.call(t, tc.part)
			wantProblem(t, env, 400, tc.slug)
			if !strings.Contains(env.Problem.Detail, tc.want) {
				t.Errorf("detail = %q, want it to contain %q", env.Problem.Detail, tc.want)
			}
			if !strings.Contains(text, "(400)") {
				t.Errorf("summary = %q, want the 400 status", text)
			}
		})
	}
}

// failingQueries is a TaskQueries whose reads fail with a secret-bearing
// error, to prove an internal failure is answered as a generic 500 problem.
type failingQueries struct{}

func (failingQueries) FindClaimableByType(context.Context, task.Type, time.Time) ([]*task.Task, error) {
	return nil, errors.New("pq: password authentication failed for user fulfillment-secret")
}

func (failingQueries) FindAllClaimed(context.Context) ([]*task.Task, error) { return nil, nil }

func TestWireInternalFailureIsAGeneric500Problem(t *testing.T) {
	h := newHarness(t)
	h.deps.Tasks = failingQueries{}
	env, text := newWire(t, h.deps).skill(t, skillBacklogStatus, map[string]any{"processPath": "PICK"})
	wantProblem(t, env, 500, "internal-error")
	if strings.Contains(env.Problem.Detail, "secret") || strings.Contains(text, "secret") {
		t.Errorf("the underlying error leaked into the answer: %+v / %q", env.Problem, text)
	}
}

func TestSkillRunsInASpanNamedAfterTheSkill(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})

	w := newWire(t, newHarness(t).deps)
	w.skill(t, skillBacklogStatus, map[string]any{"processPath": "PICK"})
	w.skill(t, skillCompleteTask, map[string]any{"taskId": "nope", "stationId": "s1"})

	spans := map[string]string{}
	failed := map[string]bool{}
	for _, s := range exporter.GetSpans() {
		for _, kv := range s.Attributes {
			if string(kv.Key) == "a2a.skill" {
				spans[s.Name] = kv.Value.AsString()
				failed[s.Name] = s.Status.Code == 1
			}
		}
	}
	if spans["a2a.skill backlog_status"] != skillBacklogStatus || spans["a2a.skill complete_task"] != skillCompleteTask {
		t.Fatalf("spans = %v, want one a2a.skill span per skill carrying the a2a.skill attribute", spans)
	}
	if !failed["a2a.skill complete_task"] || failed["a2a.skill backlog_status"] {
		t.Errorf("error status by span = %v: only the rejected complete_task should be marked failed", failed)
	}
}
