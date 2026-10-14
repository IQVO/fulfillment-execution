package mcp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// relocateHarness seeds one RELOCATE task (an Assign move: the source is
// open) and claims it for station s1.
func relocateHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.station.Execute(ctx, "s1", []string{"relocate"}, ""); err != nil {
		t.Fatalf("register: %v", err)
	}
	move, err := task.NewRelocationMove("plan-7", 3, "Assign", "SKU-1", "", "FWD-B")
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	if _, err := h.create.ExecuteRelocation(ctx, shared.NewCPT(base.Add(time.Hour)), "slot-plan-7-move-3", shared.NewCapabilitySet("relocate"), move); err != nil {
		t.Fatalf("create: %v", err)
	}
	return h
}

// TestFindClaimableWork_RelocateShowsTheMove: RELOCATE is a known process
// path for the read tools, and the best candidate carries the planned move.
func TestFindClaimableWork_RelocateShowsTheMove(t *testing.T) {
	h := relocateHarness(t)
	depth, err := h.deps.getQueueStatus(context.Background(), queueStatusInput{ProcessPath: "RELOCATE"})
	if err != nil || depth.Depth != 1 {
		t.Fatalf("get_queue_status RELOCATE = %+v, %v; want depth 1", depth, err)
	}
	out, err := h.deps.findClaimableWork(context.Background(), findClaimableInput{ProcessPath: "RELOCATE"})
	if err != nil {
		t.Fatalf("find_claimable_work: %v", err)
	}
	if out.Best == nil || out.Best.Relocation == nil {
		t.Fatalf("best = %+v, want a RELOCATE task with its move", out.Best)
	}
	want := relocationView{PlanId: "plan-7", MoveSeq: 3, Kind: "Assign", SKU: "SKU-1", ToBin: "FWD-B", Ref: "plan-7:3"}
	if *out.Best.Relocation != want {
		t.Fatalf("relocation = %+v, want %+v", *out.Best.Relocation, want)
	}
}

func TestFindClaimableWork_NonRelocateHasNoMove(t *testing.T) {
	h := newHarness(t)
	h.seedPending(t, task.Pick, "o1", time.Hour, "pick")
	out, err := h.deps.findClaimableWork(context.Background(), findClaimableInput{ProcessPath: "PICK"})
	if err != nil || out.Best == nil {
		t.Fatalf("find: %+v %v", out, err)
	}
	if out.Best.Relocation != nil {
		t.Fatalf("a PICK candidate carries no relocation, got %+v", out.Best.Relocation)
	}
}

// claimedRelocation returns the harness and the id of its RELOCATE task,
// claimed by s1.
func claimedRelocation(t *testing.T) (*harness, shared.TaskId) {
	t.Helper()
	h := relocateHarness(t)
	claimed, err := h.claim.Execute(context.Background(), "s1", task.Relocate)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return h, claimed.Id()
}

// TestCompleteTask_RelocateConfirmationCompletesTheMove: complete_task
// passes the optional quantity / fromBin through to the use case.
func TestCompleteTask_RelocateConfirmationCompletesTheMove(t *testing.T) {
	ctx := context.Background()
	h, id := claimedRelocation(t)
	out, err := h.deps.completeTask(ctx, completeTaskInput{TaskId: string(id), StationId: "s1", Quantity: 7, FromBin: "RSV-9"})
	if err != nil || !out.Completed {
		t.Fatalf("complete = %+v, %v", out, err)
	}
	saved, _ := h.tasks.FindById(ctx, id)
	want := task.RelocationResult{Quantity: 7, FromBin: "RSV-9", ToBin: "FWD-B"}
	if saved.RelocationResult() == nil || *saved.RelocationResult() != want {
		t.Fatalf("result = %+v, want %+v", saved.RelocationResult(), want)
	}
}

// TestCompleteTask_RelocateWithoutConfirmationIsRejected: without the
// inputs (or with the open side unscanned) the task stays claimed.
func TestCompleteTask_RelocateWithoutConfirmationIsRejected(t *testing.T) {
	ctx := context.Background()
	h, id := claimedRelocation(t)
	if _, err := h.deps.completeTask(ctx, completeTaskInput{TaskId: string(id), StationId: "s1"}); !errors.Is(err, task.ErrRelocationConfirmationRequired) {
		t.Fatalf("err = %v, want ErrRelocationConfirmationRequired", err)
	}
	if _, err := h.deps.completeTask(ctx, completeTaskInput{TaskId: string(id), StationId: "s1", Quantity: 7}); !errors.Is(err, task.ErrInvalidRelocationConfirmation) {
		t.Fatalf("open source not scanned: err = %v, want ErrInvalidRelocationConfirmation", err)
	}
	if saved, _ := h.tasks.FindById(ctx, id); saved.Status() != task.Claimed {
		t.Fatalf("status = %s, want CLAIMED", saved.Status())
	}
}

// TestCompleteTask_RelocationInputsOnAPick: rejected by the domain; a
// negative quantity is rejected before the use case.
func TestCompleteTask_RelocationInputsOnAPick(t *testing.T) {
	ctx := context.Background()
	h, taskId := completeHarness(t)
	if _, err := h.deps.completeTask(ctx, completeTaskInput{TaskId: taskId, StationId: "s1", Quantity: 2}); !errors.Is(err, task.ErrNotRelocationTask) {
		t.Fatalf("err = %v, want ErrNotRelocationTask", err)
	}
	if _, err := h.deps.completeTask(ctx, completeTaskInput{TaskId: taskId, StationId: "s1", Quantity: -1}); err == nil {
		t.Fatal("quantity -1 must be rejected")
	}
	if saved, _ := h.tasks.FindById(ctx, shared.TaskId(taskId)); saved.Status() != task.Claimed {
		t.Fatalf("status = %s, want CLAIMED", saved.Status())
	}
}
