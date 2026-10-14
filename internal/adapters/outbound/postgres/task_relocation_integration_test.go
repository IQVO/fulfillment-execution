//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// TestTaskRepo_RelocationRoundTrip proves migration 0019 (ADR 0047): a
// RELOCATE task's planned move round-trips on every read path, the executed
// result recorded by CompleteRelocation round-trips after the completion is
// saved, and a non-RELOCATE task (the shape of every pre-migration row)
// rehydrates with neither.
func TestTaskRepo_RelocationRoundTrip(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewTaskRepo(pool)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	cpt := shared.NewCPT(now.Add(time.Hour))

	move, err := task.NewRelocationMove("plan-7", 0, "Assign", "SKU-1", "", "FWD-B")
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	rel := task.NewRelocationTask("integration-task-relocate", cpt, "slot-plan-7-move-0", shared.NewCapabilitySet("relocate"), move)
	plain := task.New("integration-task-not-relocate", task.Pick, cpt, "wu-plain", shared.NewCapabilitySet("pick"), false, false)
	for _, tk := range []*task.Task{rel, plain} {
		if err := repo.Save(ctx, tk); err != nil {
			t.Fatalf("save %s: %v", tk.Id(), err)
		}
	}

	got, err := repo.FindById(ctx, "integration-task-relocate")
	if err != nil || got == nil {
		t.Fatalf("find: %v %v", got, err)
	}
	if got.Type() != task.Relocate || got.Relocation() == nil || *got.Relocation() != move {
		t.Fatalf("round-trip: type %s move %+v, want RELOCATE %+v", got.Type(), got.Relocation(), move)
	}
	if got.RelocationResult() != nil {
		t.Fatalf("a pending task has no result, got %+v", got.RelocationResult())
	}

	// Bulk read path (scanTasks) carries the move too.
	list, err := repo.FindClaimableByType(ctx, task.Relocate, now)
	if err != nil || len(list) != 1 || list[0].Relocation() == nil || list[0].Relocation().Ref() != "plan-7:0" {
		t.Fatalf("FindClaimableByType = %v err=%v, want one task with move plan-7:0", list, err)
	}

	// Claim, complete with the operator's confirmation, save, reload.
	if err := got.Claim("s1", shared.NewCapabilitySet("relocate"), now, time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := got.CompleteRelocation("s1", now.Add(time.Second), task.RelocationConfirmation{Quantity: 9, FromBin: "RSV-4"}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := repo.Save(ctx, got); err != nil {
		t.Fatalf("save completed: %v", err)
	}
	done, err := repo.FindById(ctx, "integration-task-relocate")
	if err != nil || done == nil {
		t.Fatalf("find completed: %v %v", done, err)
	}
	want := task.RelocationResult{Quantity: 9, FromBin: "RSV-4", ToBin: "FWD-B"}
	if done.Status() != task.Completed || done.RelocationResult() == nil || *done.RelocationResult() != want {
		t.Fatalf("completed round-trip: %s %+v, want COMPLETED %+v", done.Status(), done.RelocationResult(), want)
	}
	if *done.Relocation() != move {
		t.Fatalf("the planned move must survive the completion, got %+v", done.Relocation())
	}

	other, err := repo.FindById(ctx, "integration-task-not-relocate")
	if err != nil || other == nil {
		t.Fatalf("find plain: %v %v", other, err)
	}
	if other.Relocation() != nil || other.RelocationResult() != nil {
		t.Fatalf("a PICK task carries no relocation, got %+v / %+v", other.Relocation(), other.RelocationResult())
	}
}

// TestTaskRepo_RelocationColumnsAreNullForOtherTasks pins the additive
// migration's storage rule: no relocation column is written for a
// non-RELOCATE task (NULL, never "" or 0).
func TestTaskRepo_RelocationColumnsAreNullForOtherTasks(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewTaskRepo(pool)
	ctx := context.Background()
	tk := task.New("integration-task-null-reloc", task.Pack, shared.NewCPT(time.Now().Add(time.Hour)), "wu-null", shared.NewCapabilitySet("pack"), false, false)
	if err := repo.Save(ctx, tk); err != nil {
		t.Fatalf("save: %v", err)
	}
	var nonNull int
	err := pool.QueryRow(ctx, `
		SELECT num_nonnulls(relocation_plan_id, relocation_move_seq, relocation_kind, relocation_sku,
		       relocation_from_bin, relocation_to_bin, relocated_quantity, relocated_from_bin, relocated_to_bin)
		FROM tasks WHERE id = $1`, "integration-task-null-reloc").Scan(&nonNull)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if nonNull != 0 {
		t.Fatalf("%d relocation columns are non-NULL on a PACK task, want 0", nonNull)
	}
}
