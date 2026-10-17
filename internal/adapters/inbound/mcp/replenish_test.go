package mcp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

const (
	replSrc = "WH1-RSV-AMB-B02-03-02-C"
	replDst = "WH1-FWD-AMB-A01-01-01-A"
)

// replenishHarness seeds one REPLENISH task and registers station s1 with
// the replenish capability.
func replenishHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.station.Execute(ctx, "s1", []string{"replenish"}, ""); err != nil {
		t.Fatalf("register: %v", err)
	}
	move, err := task.NewReplenishmentMove("req-9", "SKU-1", replSrc, replDst, 24, "NORMAL")
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	if _, err := h.create.ExecuteReplenishment(ctx, shared.NewCPT(base.Add(time.Hour)), "replenish-req-9", shared.NewCapabilitySet("replenish"), move); err != nil {
		t.Fatalf("create: %v", err)
	}
	return h
}

// TestFindClaimableWork_ReplenishShowsTheMove: REPLENISH is a known process
// path for the read tools, and the best candidate carries the planned move.
func TestFindClaimableWork_ReplenishShowsTheMove(t *testing.T) {
	h := replenishHarness(t)
	depth, err := h.deps.getQueueStatus(context.Background(), queueStatusInput{ProcessPath: "REPLENISH"})
	if err != nil || depth.Depth != 1 {
		t.Fatalf("get_queue_status REPLENISH = %+v, %v; want depth 1", depth, err)
	}
	out, err := h.deps.findClaimableWork(context.Background(), findClaimableInput{ProcessPath: "REPLENISH"})
	if err != nil {
		t.Fatalf("find_claimable_work: %v", err)
	}
	if out.Best == nil || out.Best.Replenishment == nil || out.Best.Relocation != nil {
		t.Fatalf("best = %+v, want a REPLENISH task with its move", out.Best)
	}
	want := replenishmentView{RequestId: "req-9", SKU: "SKU-1", FromBin: replSrc, ToBin: replDst, Quantity: 24, Priority: "NORMAL"}
	if *out.Best.Replenishment != want {
		t.Fatalf("replenishment = %+v, want %+v", *out.Best.Replenishment, want)
	}
}

func TestFindClaimableWork_NonReplenishHasNoReplenishment(t *testing.T) {
	h := newHarness(t)
	h.seedPending(t, task.Pick, "o1", time.Hour, "pick")
	out, err := h.deps.findClaimableWork(context.Background(), findClaimableInput{ProcessPath: "PICK"})
	if err != nil || out.Best == nil {
		t.Fatalf("find: %+v %v", out, err)
	}
	if out.Best.Replenishment != nil {
		t.Fatalf("a PICK candidate carries no replenishment, got %+v", out.Best.Replenishment)
	}
}

func claimedReplenishment(t *testing.T) (*harness, shared.TaskId) {
	t.Helper()
	h := replenishHarness(t)
	claimed, err := h.claim.Execute(context.Background(), "s1", task.Replenish)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return h, claimed.Id()
}

// TestCompleteTask_ReplenishConfirmationCompletesTheMove: complete_task
// passes the counted quantity through to the use case.
func TestCompleteTask_ReplenishConfirmationCompletesTheMove(t *testing.T) {
	ctx := context.Background()
	h, id := claimedReplenishment(t)
	out, err := h.deps.completeTask(ctx, completeTaskInput{TaskId: string(id), StationId: "s1", Quantity: 20, ToBin: replDst})
	if err != nil || !out.Completed {
		t.Fatalf("complete = %+v, %v", out, err)
	}
	saved, _ := h.tasks.FindById(ctx, id)
	want := task.ReplenishmentResult{Quantity: 20, FromBin: replSrc, ToBin: replDst}
	if saved.ReplenishmentResult() == nil || *saved.ReplenishmentResult() != want {
		t.Fatalf("result = %+v, want %+v", saved.ReplenishmentResult(), want)
	}
}

// TestCompleteTask_ReplenishRejections: without the quantity, or with a scan
// that differs from the plan, the task stays claimed.
func TestCompleteTask_ReplenishRejections(t *testing.T) {
	ctx := context.Background()
	h, id := claimedReplenishment(t)
	if _, err := h.deps.completeTask(ctx, completeTaskInput{TaskId: string(id), StationId: "s1"}); !errors.Is(err, task.ErrReplenishmentConfirmationRequired) {
		t.Fatalf("err = %v, want ErrReplenishmentConfirmationRequired", err)
	}
	if _, err := h.deps.completeTask(ctx, completeTaskInput{TaskId: string(id), StationId: "s1", Quantity: 7, FromBin: "WH1-RSV-AMB-Z99-01-01-A"}); !errors.Is(err, task.ErrInvalidReplenishmentConfirmation) {
		t.Fatalf("scan differs: err = %v, want ErrInvalidReplenishmentConfirmation", err)
	}
	if _, err := h.deps.completeTask(ctx, completeTaskInput{TaskId: string(id), StationId: "s1", FromBin: replSrc}); !errors.Is(err, task.ErrInvalidReplenishmentConfirmation) {
		t.Fatalf("bin without quantity: err = %v, want ErrInvalidReplenishmentConfirmation", err)
	}
	if saved, _ := h.tasks.FindById(ctx, id); saved.Status() != task.Claimed {
		t.Fatalf("status = %s, want CLAIMED", saved.Status())
	}
}

func TestParseProcessPath_AcceptsReplenish(t *testing.T) {
	got, err := parseProcessPath("REPLENISH")
	if err != nil || got != task.Replenish {
		t.Fatalf("parseProcessPath(REPLENISH) = %q, %v", got, err)
	}
	if _, err := parseProcessPath("replenish"); err == nil {
		t.Fatal("process paths are matched exactly")
	}
}
