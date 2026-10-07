package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// transferStack wires the in-memory production shape for the transfer
// fact tests: CreateTask + CompleteTask over the same repo and publisher.
type transferStack struct {
	tasks     *memory.TaskRepo
	publisher *events.BufferedPublisher
	create    *usecases.CreateTask
	complete  *usecases.CompleteTask
}

func newTransferStack() *transferStack {
	tasks := memory.NewTaskRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(epochT)
	create := &usecases.CreateTask{Tasks: tasks, Publisher: publisher, Clock: clock, NewId: fixedTaskId("t-tr")}
	complete := &usecases.CompleteTask{Tasks: tasks, Publisher: publisher, Clock: memory.NewFixedClock(epochT.Add(5 * time.Minute))}
	return &transferStack{tasks: tasks, publisher: publisher, create: create, complete: complete}
}

var epochT = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func fixedTaskId(prefix string) func() shared.TaskId {
	n := 0
	return func() shared.TaskId {
		n++
		return shared.TaskId(prefix + string(rune('0'+n)))
	}
}

func transferDetails(kind task.WorkKind) *task.TransferDetails {
	return &task.TransferDetails{
		TransferRef: "tr-9f2a",
		DemandId:    "demand-77",
		WorkKind:    kind,
		SiteId:      "site-north",
		SKU:         "SKU-0042",
		Quantity:    12,
	}
}

// claimForTest claims a task directly through the domain so CompleteTask
// has a valid claim to complete (in-process stand-in for ClaimNext).
func claimForTest(t *testing.T, tasks *memory.TaskRepo, id shared.TaskId) {
	t.Helper()
	tk, err := tasks.FindById(context.Background(), id)
	if err != nil || tk == nil {
		t.Fatalf("find %s: %v", id, err)
	}
	if err := tk.Claim("station-01", shared.NewCapabilitySet("pick", "dispatch", "arrival"), epochT, 10*time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := tasks.Save(context.Background(), tk); err != nil {
		t.Fatalf("save claimed: %v", err)
	}
}

// TestTransferFields_SurviveDecodePersistReloadCompletion is the field-
// survival invariant: a WorkReleased carrying the transfer block creates
// a Task whose correlation block survives the Save -> FindById reload
// (the memory repo clones like Postgres rehydration does) and whose
// completion publishes the fact selected by the work kind.
func TestTransferFields_SurviveDecodePersistReloadCompletion(t *testing.T) {
	s := newTransferStack()
	ctx := context.Background()

	created, err := s.create.ExecuteTransfer(ctx, task.Dispatch, shared.NewCPT(epochT.Add(time.Hour)), shared.OrderRef("wu-tr-1"), shared.NewCapabilitySet("dispatch"), false, false, transferDetails(task.WorkKindTransferDispatch))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	reloaded, err := s.tasks.FindById(ctx, created.Id())
	if err != nil || reloaded == nil {
		t.Fatalf("reload: %v %v", reloaded, err)
	}
	tr := reloaded.Transfer()
	if tr == nil || tr.TransferRef != "tr-9f2a" || tr.DemandId != "demand-77" || tr.WorkKind != task.WorkKindTransferDispatch || tr.SiteId != "site-north" || tr.SKU != "SKU-0042" || tr.Quantity != 12 {
		t.Fatalf("transfer block did not survive the reload: %+v", tr)
	}

	claimForTest(t, s.tasks, created.Id())
	if err := s.complete.Execute(ctx, created.Id(), "station-01"); err != nil {
		t.Fatalf("complete: %v", err)
	}

	found, _ := s.tasks.FindById(ctx, created.Id())
	if found.Status() != task.Completed {
		t.Fatalf("status = %q, want COMPLETED", found.Status())
	}
	facts := collectTransferFacts(t, s)
	if len(facts) != 1 || facts[0] != "TransferDispatched" {
		t.Fatalf("transfer facts = %v, want exactly [TransferDispatched] (selected by work_kind)", facts)
	}
}

// collectTransferFacts returns the EventNames of every transfer fact the
// stack published, failing the test on more than one.
func collectTransferFacts(t *testing.T, s *transferStack) []string {
	t.Helper()
	var facts []string
	for _, e := range s.publisher.Events() {
		switch e.(type) {
		case shared.TransferPicked, shared.TransferDispatched, shared.TransferArrived:
			facts = append(facts, e.EventName())
		}
	}
	return facts
}

// TestCompleteTask_FactSelectionPerWorkKind pins the per-work_kind
// selection table on its own: TRANSFER_PICK -> TransferPicked,
// TRANSFER_DISPATCH -> TransferDispatched, TRANSFER_ARRIVAL ->
// TransferArrived — exactly one fact each.
func TestCompleteTask_FactSelectionPerWorkKind(t *testing.T) {
	cases := []struct {
		kind task.WorkKind
		want string
	}{
		{task.WorkKindTransferPick, "TransferPicked"},
		{task.WorkKindTransferDispatch, "TransferDispatched"},
		{task.WorkKindTransferArrival, "TransferArrived"},
	}
	for _, tc := range cases {
		s := newTransferStack()
		ctx := context.Background()
		created, err := s.create.ExecuteTransfer(ctx, task.Pick, shared.NewCPT(epochT.Add(time.Hour)), shared.OrderRef("wu-"+string(tc.kind)), shared.NewCapabilitySet("pick"), false, false, transferDetails(tc.kind))
		if err != nil {
			t.Fatalf("%s: create: %v", tc.kind, err)
		}
		claimForTest(t, s.tasks, created.Id())
		if err := s.complete.Execute(ctx, created.Id(), "station-01"); err != nil {
			t.Fatalf("%s: complete: %v", tc.kind, err)
		}
		facts := collectTransferFacts(t, s)
		if len(facts) != 1 || facts[0] != tc.want {
			t.Fatalf("work_kind %s: facts = %v, want exactly [%s]", tc.kind, facts, tc.want)
		}
		// TaskCompleted is still published alongside (unchanged contract).
		var sawTaskCompleted bool
		for _, e := range s.publisher.Events() {
			if _, ok := e.(shared.TaskCompleted); ok {
				sawTaskCompleted = true
			}
		}
		if !sawTaskCompleted {
			t.Fatalf("work_kind %s: TaskCompleted must still be published", tc.kind)
		}
	}
}

// TestCompleteTask_NonTransferTaskPublishesNoTransferFact is the negative
// half of the contract: a task without a correlation block completes
// normally and publishes no transfer fact at all.
func TestCompleteTask_NonTransferTaskPublishesNoTransferFact(t *testing.T) {
	s := newTransferStack()
	ctx := context.Background()
	created, err := s.create.Execute(ctx, task.Pick, shared.NewCPT(epochT.Add(time.Hour)), shared.OrderRef("wu-plain"), shared.NewCapabilitySet("pick"), false, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	claimForTest(t, s.tasks, created.Id())
	if err := s.complete.Execute(ctx, created.Id(), "station-01"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	for _, e := range s.publisher.Events() {
		switch e.(type) {
		case shared.TransferPicked, shared.TransferDispatched, shared.TransferArrived:
			t.Fatalf("non-transfer task published a transfer fact: %s", e.EventName())
		}
	}
}

// TestTransferFactPayload_CarriesCorrelationAndCompletionTime checks the
// fact's own payload: work_unit_id is the task's OrderRef, task_id is the
// task, and occurred-at is the completion time (the fixed completion
// clock, not the creation clock).
func TestTransferFactPayload_CarriesCorrelationAndCompletionTime(t *testing.T) {
	s := newTransferStack()
	ctx := context.Background()
	created, err := s.create.ExecuteTransfer(ctx, task.Arrival, shared.NewCPT(epochT.Add(time.Hour)), shared.OrderRef("wu-tr-2"), shared.NewCapabilitySet("arrival"), false, false, transferDetails(task.WorkKindTransferArrival))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	claimForTest(t, s.tasks, created.Id())
	if err := s.complete.Execute(ctx, created.Id(), "station-01"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	for _, e := range s.publisher.Events() {
		if f, ok := e.(shared.TransferArrived); ok {
			if f.WorkUnitId != "wu-tr-2" || string(f.TaskId) != string(created.Id()) {
				t.Fatalf("fact ids = %q/%q, want wu-tr-2/%s", f.WorkUnitId, f.TaskId, created.Id())
			}
			if !f.OccurredAt().Equal(epochT.Add(5 * time.Minute)) {
				t.Fatalf("fact time = %v, want the completion time %v", f.OccurredAt(), epochT.Add(5*time.Minute))
			}
		}
	}
}
