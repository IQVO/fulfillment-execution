package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/pathcatalog"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// relocateCatalogue is the process-path catalogue with the RELOCATE path
// process-path-management publishes (capability "relocate") next to PICK.
func relocateCatalogue() *pathcatalog.Catalogue {
	return pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", Direct: true, RequiredCapabilities: []string{"pick"}},
		{Id: "RELOCATE", MatchPrefix: "relocate", Direct: true, RequiredCapabilities: []string{"relocate"}},
	})
}

type relocateFixture struct {
	tasks     *memory.TaskRepo
	publisher *events.BufferedPublisher
	processed *memory.ProcessedEventsRepo
	apply     *usecases.ApplyWorkReleased
	complete  *usecases.CompleteTask
}

func newRelocateFixture() relocateFixture {
	tasks := memory.NewTaskRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(atomicEpoch)
	processed := memory.NewProcessedEventsRepo()
	create := &usecases.CreateTask{Tasks: tasks, Publisher: publisher, Clock: clock, NewId: fixedTaskId("rel")}
	return relocateFixture{
		tasks:     tasks,
		publisher: publisher,
		processed: processed,
		apply:     &usecases.ApplyWorkReleased{CreateTask: create, Processed: processed, Catalogue: relocateCatalogue()},
		complete:  &usecases.CompleteTask{Tasks: tasks, Publisher: publisher, Clock: clock},
	}
}

func relocationMove(t *testing.T, kind, from, to string) *task.RelocationMove {
	t.Helper()
	m, err := task.NewRelocationMove("plan-7", 3, kind, "SKU-1", from, to)
	if err != nil {
		t.Fatalf("NewRelocationMove: %v", err)
	}
	return &m
}

// releaseAndClaim applies a RELOCATE WorkReleased and claims the created
// task for station s1, returning it.
func (f relocateFixture) releaseAndClaim(t *testing.T, move *task.RelocationMove) *task.Task {
	t.Helper()
	req := usecases.WorkReleasedRequest{EventId: "evt-rel-1", PathId: "RELOCATE", WorkUnitId: "slot-plan-7-3", Ref: "plan-7",
		CPT: atomicEpoch.Add(time.Hour), Relocation: move}
	if err := f.apply.Execute(context.Background(), req); err != nil {
		t.Fatalf("apply: %v", err)
	}
	list, err := f.tasks.FindClaimableByType(context.Background(), task.Relocate, atomicEpoch)
	if err != nil || len(list) != 1 {
		t.Fatalf("expected 1 RELOCATE task, got %d err=%v", len(list), err)
	}
	tk := list[0]
	if err := tk.Claim("s1", shared.NewCapabilitySet("relocate"), atomicEpoch, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := f.tasks.Save(context.Background(), tk); err != nil {
		t.Fatalf("save claim: %v", err)
	}
	return tk
}

func lastTaskCompleted(t *testing.T, p *events.BufferedPublisher) shared.TaskCompleted {
	t.Helper()
	var found *shared.TaskCompleted
	for _, e := range p.Events() {
		if tc, ok := e.(shared.TaskCompleted); ok {
			found = &tc
		}
	}
	if found == nil {
		t.Fatal("no TaskCompleted published")
	}
	return *found
}

func TestApplyWorkReleased_RelocatePathCreatesRelocateTaskWithTheMove(t *testing.T) {
	f := newRelocateFixture()
	move := relocationMove(t, "Assign", "", "FWD-B")
	tk := f.releaseAndClaim(t, move)

	if tk.Type() != task.Relocate {
		t.Fatalf("type = %s, want RELOCATE", tk.Type())
	}
	if tk.Relocation() == nil || *tk.Relocation() != *move {
		t.Fatalf("move = %+v, want %+v", tk.Relocation(), move)
	}
	if !tk.RequiredCapabilities().HasAll(shared.NewCapabilitySet("relocate")) {
		t.Fatalf("required capabilities = %v, want relocate", tk.RequiredCapabilities())
	}
	if tk.SourceOrderId() != "" || tk.SourceLineNo() != 0 {
		t.Fatalf("relocation work is not order work: source order %q line %d", tk.SourceOrderId(), tk.SourceLineNo())
	}
}

func TestApplyWorkReleased_RelocatePathWithoutMoveIsRejectedAndRetryable(t *testing.T) {
	f := newRelocateFixture()
	req := usecases.WorkReleasedRequest{EventId: "evt-rel-2", PathId: "RELOCATE", WorkUnitId: "wu", CPT: atomicEpoch}
	if err := f.apply.Execute(context.Background(), req); !errors.Is(err, usecases.ErrRelocationMoveMissing) {
		t.Fatalf("err = %v, want ErrRelocationMoveMissing", err)
	}
	if n, _ := f.tasks.CountByTypeAndStatus(context.Background(), task.Relocate, task.Pending); n != 0 {
		t.Fatalf("no task may be created, got %d", n)
	}
	isNew, err := f.processed.MarkProcessed(context.Background(), "evt-rel-2")
	if err != nil || !isNew {
		t.Fatalf("the failed release must not leave a processed marker (isNew=%v err=%v)", isNew, err)
	}
}

func TestApplyWorkReleased_MoveOnAnotherPathIsRejected(t *testing.T) {
	f := newRelocateFixture()
	req := usecases.WorkReleasedRequest{EventId: "evt-rel-3", PathId: "PICK", WorkUnitId: "wu", CPT: atomicEpoch,
		Relocation: relocationMove(t, "Relocate", "A", "B")}
	if err := f.apply.Execute(context.Background(), req); !errors.Is(err, usecases.ErrRelocationMoveOnOtherPath) {
		t.Fatalf("err = %v, want ErrRelocationMoveOnOtherPath", err)
	}
	if n, _ := f.tasks.CountByTypeAndStatus(context.Background(), task.Pick, task.Pending); n != 0 {
		t.Fatalf("no task may be created, got %d", n)
	}
}

func TestCompleteTask_RelocateWithConfirmationPublishesTheExecutedMove(t *testing.T) {
	for _, tc := range []struct {
		name             string
		kind, from, to   string
		confirm          task.RelocationConfirmation
		wantFrom, wantTo string
	}{
		{"relocate", "Relocate", "FWD-A", "FWD-B", task.RelocationConfirmation{Quantity: 12}, "FWD-A", "FWD-B"},
		{"assign scans the source", "Assign", "", "FWD-B", task.RelocationConfirmation{Quantity: 5, FromBin: "RSV-9"}, "RSV-9", "FWD-B"},
		{"vacate scans the destination", "Vacate", "FWD-A", "", task.RelocationConfirmation{Quantity: 7, ToBin: "RSV-2"}, "FWD-A", "RSV-2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRelocateFixture()
			tk := f.releaseAndClaim(t, relocationMove(t, tc.kind, tc.from, tc.to))

			if err := f.complete.ExecuteWith(context.Background(), tk.Id(), "s1", tc.confirm); err != nil {
				t.Fatalf("complete: %v", err)
			}

			got := lastTaskCompleted(t, f.publisher)
			want := shared.TaskRelocation{SKU: "SKU-1", FromBin: tc.wantFrom, ToBin: tc.wantTo, Quantity: tc.confirm.Quantity, Ref: "plan-7:3"}
			if got.Relocation == nil || *got.Relocation != want {
				t.Fatalf("TaskCompleted.Relocation = %+v, want %+v", got.Relocation, want)
			}
			saved, _ := f.tasks.FindById(context.Background(), tk.Id())
			if saved.Status() != task.Completed || saved.RelocationResult() == nil || saved.RelocationResult().Quantity != tc.confirm.Quantity {
				t.Fatalf("saved task = %s / %+v", saved.Status(), saved.RelocationResult())
			}
		})
	}
}

func TestCompleteTask_RelocateWithoutConfirmationIsRejectedAndPublishesNothing(t *testing.T) {
	f := newRelocateFixture()
	tk := f.releaseAndClaim(t, relocationMove(t, "Relocate", "A", "B"))
	before := len(f.publisher.Events())

	if err := f.complete.Execute(context.Background(), tk.Id(), "s1"); !errors.Is(err, task.ErrRelocationConfirmationRequired) {
		t.Fatalf("err = %v, want ErrRelocationConfirmationRequired", err)
	}
	if err := f.complete.ExecuteWith(context.Background(), tk.Id(), "s1", task.RelocationConfirmation{Quantity: -2}); !errors.Is(err, task.ErrInvalidRelocationConfirmation) {
		t.Fatalf("negative quantity: err = %v, want ErrInvalidRelocationConfirmation", err)
	}
	if len(f.publisher.Events()) != before {
		t.Fatal("a rejected completion must publish nothing")
	}
	saved, _ := f.tasks.FindById(context.Background(), tk.Id())
	if saved.Status() != task.Claimed {
		t.Fatalf("status = %s, want Claimed", saved.Status())
	}
}

func TestCompleteTask_RelocationInputsOnANonRelocateTaskAreRejected(t *testing.T) {
	f := newRelocateFixture()
	req := usecases.WorkReleasedRequest{EventId: "evt-pick", PathId: "PICK", WorkUnitId: "ORD-1-line-1", Ref: "ORD-1", CPT: atomicEpoch.Add(time.Hour)}
	if err := f.apply.Execute(context.Background(), req); err != nil {
		t.Fatalf("apply: %v", err)
	}
	list, _ := f.tasks.FindClaimableByType(context.Background(), task.Pick, atomicEpoch)
	tk := list[0]
	if err := tk.Claim("s1", shared.NewCapabilitySet("pick"), atomicEpoch, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	_ = f.tasks.Save(context.Background(), tk)

	if err := f.complete.ExecuteWith(context.Background(), tk.Id(), "s1", task.RelocationConfirmation{Quantity: 3}); !errors.Is(err, task.ErrNotRelocationTask) {
		t.Fatalf("err = %v, want ErrNotRelocationTask", err)
	}
	if err := f.complete.ExecuteWith(context.Background(), tk.Id(), "s1", task.RelocationConfirmation{}); err != nil {
		t.Fatalf("a PICK completes without relocation inputs: %v", err)
	}
	if got := lastTaskCompleted(t, f.publisher); got.Relocation != nil {
		t.Fatalf("a PICK TaskCompleted carries no relocation, got %+v", got.Relocation)
	}
}
