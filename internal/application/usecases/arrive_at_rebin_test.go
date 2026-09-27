package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/consolidation"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

func newArriveAtRebinHarness() (*usecases.ArriveAtRebin, *memory.OrderConsolidationRepo, *memory.TaskRepo, *events.BufferedPublisher) {
	tasks := memory.NewTaskRepo()
	consolidations := memory.NewOrderConsolidationRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(epoch)
	createTask := &usecases.CreateTask{Tasks: tasks, Publisher: publisher, Clock: clock, NewId: idSeq("pack-t")}
	uc := &usecases.ArriveAtRebin{
		Consolidations: consolidations,
		CreateTask:     createTask,
		Publisher:      publisher,
		Clock:          clock,
	}
	return uc, consolidations, tasks, publisher
}

func TestArriveAtRebin_DoesNotCreatePackTaskUntilAllLinesArrive(t *testing.T) {
	uc, _, tasks, publisher := newArriveAtRebinHarness()
	ctx := context.Background()
	required := []string{"line-1", "line-2"}
	cpt := shared.NewCPT(epoch.Add(time.Hour))

	if err := uc.Execute(ctx, "order-1", "line-1", required, cpt, shared.NewCapabilitySet("pack"), false, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	packTasks, _ := tasks.FindByOrderRef(ctx, "order-1")
	if len(packTasks) != 0 {
		t.Fatalf("expected no PACK task before consolidation completes, got %d", len(packTasks))
	}
	if len(publisher.Events()) != 1 || publisher.Events()[0].EventName() != "ItemArrivedAtRebin" {
		t.Fatalf("expected a single ItemArrivedAtRebin event, got %+v", publisher.Events())
	}
}

func TestArriveAtRebin_CreatesPackTaskOnceConsolidationCompletes(t *testing.T) {
	uc, _, tasks, publisher := newArriveAtRebinHarness()
	ctx := context.Background()
	required := []string{"line-1", "line-2"}
	cpt := shared.NewCPT(epoch.Add(time.Hour))
	packCaps := shared.NewCapabilitySet("pack")

	if err := uc.Execute(ctx, "order-1", "line-1", required, cpt, packCaps, false, false); err != nil {
		t.Fatalf("unexpected error on first arrival: %v", err)
	}
	if err := uc.Execute(ctx, "order-1", "line-2", required, cpt, packCaps, true, true); err != nil {
		t.Fatalf("unexpected error on second arrival: %v", err)
	}

	packTasks, _ := tasks.FindByOrderRef(ctx, "order-1")
	if len(packTasks) != 1 {
		t.Fatalf("expected exactly 1 PACK task once consolidation completes, got %d", len(packTasks))
	}
	if packTasks[0].Type() != task.Pack {
		t.Fatalf("expected a PACK task, got %s", packTasks[0].Type())
	}
	if !packTasks[0].Fragile() || !packTasks[0].GiftWrap() {
		t.Fatalf("expected the PACK task to carry the hints passed on the completing arrival, got fragile=%v giftWrap=%v",
			packTasks[0].Fragile(), packTasks[0].GiftWrap())
	}

	names := make([]string, 0)
	for _, e := range publisher.Events() {
		names = append(names, e.EventName())
	}
	foundConsolidated := false
	for _, n := range names {
		if n == "OrderConsolidated" {
			foundConsolidated = true
		}
	}
	if !foundConsolidated {
		t.Fatalf("expected an OrderConsolidated event, got %v", names)
	}
}

func TestArriveAtRebin_SingleLineOrderCompletesImmediately(t *testing.T) {
	uc, _, tasks, _ := newArriveAtRebinHarness()
	ctx := context.Background()
	cpt := shared.NewCPT(epoch.Add(time.Hour))

	if err := uc.Execute(ctx, "order-1", "line-1", []string{"line-1"}, cpt, shared.NewCapabilitySet("pack"), false, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	packTasks, _ := tasks.FindByOrderRef(ctx, "order-1")
	if len(packTasks) != 1 {
		t.Fatalf("expected a single-line order to produce its PACK task on the only arrival, got %d tasks", len(packTasks))
	}
}

func TestArriveAtRebin_RejectsUnknownLine(t *testing.T) {
	uc, _, _, _ := newArriveAtRebinHarness()
	ctx := context.Background()
	cpt := shared.NewCPT(epoch.Add(time.Hour))

	// First call establishes the required set as {line-1}; a second call
	// for the SAME order but a line outside that set must be rejected.
	if err := uc.Execute(ctx, "order-1", "line-1", []string{"line-1"}, cpt, shared.NewCapabilitySet("pack"), false, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err := uc.Execute(ctx, "order-1", "line-not-in-order", []string{"line-1"}, cpt, shared.NewCapabilitySet("pack"), false, false)
	if !errors.Is(err, consolidation.ErrUnknownLine) {
		t.Fatalf("expected ErrUnknownLine, got %v", err)
	}
}

// Idempotency: redelivering the SAME arrival must not create a second
// PACK task.
func TestArriveAtRebin_RedeliveredArrivalDoesNotDuplicatePackTask(t *testing.T) {
	uc, _, tasks, _ := newArriveAtRebinHarness()
	ctx := context.Background()
	cpt := shared.NewCPT(epoch.Add(time.Hour))
	required := []string{"line-1"}

	if err := uc.Execute(ctx, "order-1", "line-1", required, cpt, shared.NewCapabilitySet("pack"), false, false); err != nil {
		t.Fatalf("unexpected error on first delivery: %v", err)
	}
	if err := uc.Execute(ctx, "order-1", "line-1", required, cpt, shared.NewCapabilitySet("pack"), false, false); err != nil {
		t.Fatalf("unexpected error on redelivered arrival: %v", err)
	}

	packTasks, _ := tasks.FindByOrderRef(ctx, "order-1")
	if len(packTasks) != 1 {
		t.Fatalf("expected exactly 1 PACK task despite the redelivered arrival, got %d", len(packTasks))
	}
}

func TestArriveAtRebin_PropagatesConsolidationLookupError(t *testing.T) {
	uc, _, _, publisher := newFailingArriveAtRebinHarness(func(consolidations *errConsolidationRepo, tasks *errTaskRepo) {
		consolidations.failFindByOrderRef = true
		tasks.failSave = true // any later write must never be reached
	})

	err := uc.Execute(context.Background(), "order-1", "line-1", []string{"line-1"}, shared.NewCPT(epoch.Add(time.Hour)), shared.NewCapabilitySet("pack"), false, false)
	if !errors.Is(err, errFake) {
		t.Fatalf("expected consolidation lookup error to propagate, got %v", err)
	}
	if len(publisher.Events()) != 0 {
		t.Fatalf("expected no events on lookup failure, got %+v", publisher.Events())
	}
}

// The consolidation Save runs FIRST inside the atomic scope ("state, then
// event"), so a Save failure must also suppress the ItemArrivedAtRebin
// publish and every downstream step.
func TestArriveAtRebin_PropagatesConsolidationSaveError(t *testing.T) {
	uc, _, tasks, publisher := newFailingArriveAtRebinHarness(func(consolidations *errConsolidationRepo, tasks *errTaskRepo) {
		consolidations.failSave = true
	})
	ctx := context.Background()

	err := uc.Execute(ctx, "order-1", "line-1", []string{"line-1"}, shared.NewCPT(epoch.Add(time.Hour)), shared.NewCapabilitySet("pack"), false, false)
	if !errors.Is(err, errFake) {
		t.Fatalf("expected consolidation save error to propagate, got %v", err)
	}
	if len(publisher.Events()) != 0 {
		t.Fatalf("expected no events to be published when the consolidation save fails, got %+v", publisher.Events())
	}
	packTasks, _ := tasks.FindByOrderRef(ctx, "order-1")
	if len(packTasks) != 0 {
		t.Fatalf("expected no PACK task when the consolidation save fails, got %d", len(packTasks))
	}
}

// A single-line order completes consolidation on its only arrival, so the
// nested CreateTask runs: forcing ITS task save to fail proves the PACK
// creation error propagates out of ArriveAtRebin. The arrival event was
// already published (state, then event, then fan-in) and must remain
// visible, but no OrderConsolidated may follow a failed PACK creation.
func TestArriveAtRebin_PropagatesPackTaskCreationError(t *testing.T) {
	uc, consolidations, tasks, publisher := newFailingArriveAtRebinHarness(func(consolidations *errConsolidationRepo, tasks *errTaskRepo) {
		tasks.failSave = true
	})
	ctx := context.Background()

	err := uc.Execute(ctx, "order-1", "line-1", []string{"line-1"}, shared.NewCPT(epoch.Add(time.Hour)), shared.NewCapabilitySet("pack"), false, false)
	if !errors.Is(err, errFake) {
		t.Fatalf("expected PACK task creation error to propagate, got %v", err)
	}
	if len(publisher.Events()) != 1 || publisher.Events()[0].EventName() != "ItemArrivedAtRebin" {
		t.Fatalf("expected only the ItemArrivedAtRebin event, got %+v", publisher.Events())
	}
	packTasks, _ := tasks.FindByOrderRef(ctx, "order-1")
	if len(packTasks) != 0 {
		t.Fatalf("expected no PACK task to exist, got %d", len(packTasks))
	}
	saved, findErr := consolidations.FindByOrderRef(ctx, "order-1")
	if findErr != nil || saved == nil || !saved.IsComplete() {
		t.Fatalf("expected the consolidation itself to be saved complete before PACK creation, got %+v (%v)", saved, findErr)
	}
}

// newFailingArriveAtRebinHarness mirrors newArriveAtRebinHarness but wires
// the consolidation and task repos through their failing wrappers, letting
// each error-propagation branch be forced by the configure callback.
func newFailingArriveAtRebinHarness(configure func(consolidations *errConsolidationRepo, tasks *errTaskRepo)) (*usecases.ArriveAtRebin, *errConsolidationRepo, *errTaskRepo, *events.BufferedPublisher) {
	tasks := newErrTaskRepo()
	consolidations := newErrConsolidationRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(epoch)
	configure(consolidations, tasks)
	createTask := &usecases.CreateTask{Tasks: tasks, Publisher: publisher, Clock: clock, NewId: idSeq("pack-t")}
	uc := &usecases.ArriveAtRebin{
		Consolidations: consolidations,
		CreateTask:     createTask,
		Publisher:      publisher,
		Clock:          clock,
	}
	return uc, consolidations, tasks, publisher
}
