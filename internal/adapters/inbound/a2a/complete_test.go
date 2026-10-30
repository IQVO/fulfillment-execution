package a2a

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

func completedEvents(h *harness) int {
	n := 0
	for _, e := range h.publisher.Events() {
		if _, ok := e.(shared.TaskCompleted); ok {
			n++
		}
	}
	return n
}

func TestWireCompleteTaskCompletesAndPublishesOnce(t *testing.T) {
	h := newHarness(t)
	taskId := h.claimedPick(t)
	w := newWire(t, h.deps)

	env, text := w.skill(t, skillCompleteTask, map[string]any{"taskId": taskId, "stationId": "s1"})
	done := env.Data.TaskCompleted
	if env.Problem != nil || done == nil || !done.Completed || done.TaskId != taskId || done.StationId != "s1" {
		t.Fatalf("envelope = %+v, want taskCompleted for %s/s1", env, taskId)
	}
	if text != fmt.Sprintf("completed %s for station s1", taskId) {
		t.Errorf("summary = %q", text)
	}
	got, err := h.tasks.FindById(context.Background(), shared.TaskId(taskId))
	if err != nil || got == nil || got.Status() != task.Completed {
		t.Fatalf("task not persisted as Completed: %v err=%v", got, err)
	}
	if n := completedEvents(h); n != 1 {
		t.Fatalf("%d TaskCompleted events published, want exactly 1", n)
	}

	// Not idempotent: the repeat is a domain conflict and writes nothing.
	again, _ := w.skill(t, skillCompleteTask, map[string]any{"taskId": taskId, "stationId": "s1"})
	wantProblem(t, again, 409, "task-already-completed")
	if n := completedEvents(h); n != 1 {
		t.Fatalf("%d TaskCompleted events after the rejected repeat, want still 1", n)
	}
}

// A rejected completion is answered with the REST status class and writes
// nothing.
func TestWireCompleteTaskDomainRejections(t *testing.T) {
	h := newHarness(t)
	taskId := h.claimedPick(t)
	h.registerStation(t, "s2", "pick")
	w := newWire(t, h.deps)

	cases := []struct {
		name   string
		args   map[string]any
		status int
		slug   string
	}{
		{"unknown task", map[string]any{"taskId": "ghost", "stationId": "s1"}, 404, "task-not-found"},
		{"station does not own the claim", map[string]any{"taskId": taskId, "stationId": "s2"}, 409, "task-not-owner"},
		{"quantity on a PICK task", map[string]any{"taskId": taskId, "stationId": "s1", "quantity": 2}, 422, "not-relocation-task"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env, _ := w.skill(t, skillCompleteTask, tc.args)
			wantProblem(t, env, tc.status, tc.slug)
		})
	}
	if n := completedEvents(h); n != 0 {
		t.Fatalf("%d TaskCompleted events published by rejected calls, want 0", n)
	}

	// An unclaimed task is a conflict too.
	h.seedPending(t, task.Pick, "o2", 2*time.Hour, "pick")
	env, _ := w.skill(t, skillCompleteTask, map[string]any{"taskId": "t2", "stationId": "s1"})
	wantProblem(t, env, 409, "task-not-claimed")
}

func TestWireCompleteRelocateNeedsTheOperatorConfirmation(t *testing.T) {
	h := newHarness(t)
	h.registerStation(t, "s1", "relocate")
	move, err := task.NewRelocationMove("plan-7", 3, "Assign", "SKU-1", "", "FWD-B")
	if err != nil {
		t.Fatal(err)
	}
	created, err := h.create.ExecuteRelocation(context.Background(), shared.NewCPT(base.Add(time.Hour)), "slot-plan-7-move-3", shared.NewCapabilitySet("relocate"), move)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.claim.Execute(context.Background(), "s1", task.Relocate); err != nil {
		t.Fatal(err)
	}
	w := newWire(t, h.deps)
	id := string(created.Id())

	env, _ := w.skill(t, skillCompleteTask, map[string]any{"taskId": id, "stationId": "s1"})
	wantProblem(t, env, 422, "relocation-confirmation-required")
	env, _ = w.skill(t, skillCompleteTask, map[string]any{"taskId": id, "stationId": "s1", "quantity": 5, "fromBin": "WRONG", "toBin": "WRONG"})
	wantProblem(t, env, 422, "invalid-relocation-confirmation")
	if n := completedEvents(h); n != 0 {
		t.Fatalf("%d TaskCompleted events from rejected calls, want 0", n)
	}
	env, _ = w.skill(t, skillCompleteTask, map[string]any{"taskId": id, "stationId": "s1", "quantity": 5, "fromBin": "SRC-1"})
	if env.Problem != nil || env.Data.TaskCompleted == nil {
		t.Fatalf("confirmed relocation = %+v, want completed", env)
	}
	if n := completedEvents(h); n != 1 {
		t.Fatalf("%d TaskCompleted events, want 1", n)
	}
}

func TestWireCompleteReplenishNeedsTheCountedQuantity(t *testing.T) {
	h := newHarness(t)
	h.registerStation(t, "s1", "replenish")
	move, err := task.NewReplenishmentMove("req-9", "SKU-1", "WH1-RSV-AMB-B02-03-02-C", "WH1-FWD-AMB-A01-01-01-A", 24, "NORMAL")
	if err != nil {
		t.Fatal(err)
	}
	created, err := h.create.ExecuteReplenishment(context.Background(), shared.NewCPT(base.Add(time.Hour)), "replenish-req-9", shared.NewCapabilitySet("replenish"), move)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.claim.Execute(context.Background(), "s1", task.Replenish); err != nil {
		t.Fatal(err)
	}
	w := newWire(t, h.deps)
	id := string(created.Id())

	env, _ := w.skill(t, skillCompleteTask, map[string]any{"taskId": id, "stationId": "s1"})
	wantProblem(t, env, 422, "replenishment-confirmation-required")
	env, _ = w.skill(t, skillCompleteTask, map[string]any{"taskId": id, "stationId": "s1", "quantity": 24})
	if env.Problem != nil || env.Data.TaskCompleted == nil {
		t.Fatalf("counted replenishment = %+v, want completed", env)
	}
}

// The status classes of the typed errors the skills can raise are pinned
// here against the REST adapter's statusFor (internal/adapters/inbound/http
// errors.go): an agent must see the outcome a REST caller would.
func TestProblemClassificationMirrorsREST(t *testing.T) {
	cases := []struct {
		err    error
		status int
		slug   string
	}{
		{usecases.ErrTaskNotFound, 404, "task-not-found"},
		{task.ErrAlreadyCompleted, 409, "task-already-completed"},
		{task.ErrNotClaimed, 409, "task-not-claimed"},
		{task.ErrNotOwner, 409, "task-not-owner"},
		{task.ErrCaseBreakCompletionRequired, 409, "case-break-completion-required"},
		{task.ErrRelocationConfirmationRequired, 422, "relocation-confirmation-required"},
		{task.ErrInvalidRelocationConfirmation, 422, "invalid-relocation-confirmation"},
		{task.ErrNotRelocationTask, 422, "not-relocation-task"},
		{task.ErrReplenishmentConfirmationRequired, 422, "replenishment-confirmation-required"},
		{task.ErrInvalidReplenishmentConfirmation, 422, "invalid-replenishment-confirmation"},
		{errors.New("boom"), 500, "internal-error"},
	}
	for _, tc := range cases {
		// Wrapped, as use cases and repositories wrap their errors.
		p := problemFromUseCase(fmt.Errorf("wrapped: %w", tc.err))
		if p.Status != tc.status || p.Type != problemTypeBase+tc.slug {
			t.Errorf("%v -> %d %s, want %d %s", tc.err, p.Status, p.Type, tc.status, tc.slug)
		}
	}
}

// A BREAK_CASE task completes only through its own break-case flow, which the
// A2A surface (like MCP) does not offer: complete_task answers the REST 409.
func TestWireCompleteBreakCaseIsRefusedAsAConflict(t *testing.T) {
	h := newHarness(t)
	h.registerStation(t, "s1", "breakcase")
	brk, err := task.NewCaseBreak("LPN-1", "SKU-1", "WH1-FWD-AMB-A01-01-01-A", "pick-1")
	if err != nil {
		t.Fatal(err)
	}
	created, err := h.create.ExecuteCaseBreak(context.Background(), shared.NewCPT(base.Add(time.Hour)), "break-1", shared.NewCapabilitySet("breakcase"), brk)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.claim.Execute(context.Background(), "s1", task.BreakCase); err != nil {
		t.Fatal(err)
	}

	env, _ := newWire(t, h.deps).skill(t, skillCompleteTask, map[string]any{"taskId": string(created.Id()), "stationId": "s1"})
	wantProblem(t, env, 409, "case-break-completion-required")
	if n := completedEvents(h); n != 0 {
		t.Fatalf("%d TaskCompleted events from a refused completion, want 0", n)
	}
}
