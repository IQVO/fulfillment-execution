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

// TestTaskRepo_TransferDetailsRoundTrip proves the migration's new
// nullable correlation columns round-trip a transfer task exactly: save a
// DISPATCH task carrying the block, rehydrate it, compare field by field.
// The NULL side is covered by TestTaskRepo_NonTransferTaskRehydratesWithoutBlock:
// a task saved without a block rehydrates with Transfer() == nil, the same
// shape every pre-migration row has.
func TestTaskRepo_TransferDetailsRoundTrip(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewTaskRepo(pool)
	ctx := context.Background()

	details := task.TransferDetails{
		TransferRef: "tr-roundtrip-1",
		DemandId:    "demand-88",
		WorkKind:    task.WorkKindTransferArrival,
		SiteId:      "site-south",
		SKU:         "SKU-9001",
		Quantity:    7,
	}
	id := shared.TaskId("integration-task-transfer-roundtrip")
	tk := task.NewTransferTask(id, task.Arrival, shared.NewCPT(time.Now().Add(time.Hour)), "wu-roundtrip", shared.NewCapabilitySet("arrival"), false, false, details)
	if err := repo.Save(ctx, tk); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := repo.FindById(ctx, id)
	if err != nil || got == nil {
		t.Fatalf("find: %v %v", got, err)
	}
	tr := got.Transfer()
	if tr == nil {
		t.Fatal("transfer block lost on rehydrate")
	}
	if *tr != details {
		t.Fatalf("round-trip mismatch: saved %+v, rehydrated %+v", details, *tr)
	}
	if got.Type() != task.Arrival {
		t.Fatalf("type = %q, want ARRIVAL", got.Type())
	}
}

// TestTaskRepo_NonTransferTaskRehydratesWithoutBlock covers the NULL
// side: a task saved without a correlation block rehydrates with
// Transfer() == nil — the same shape every pre-migration row has, so the
// migration is backward compatible for existing data.
func TestTaskRepo_NonTransferTaskRehydratesWithoutBlock(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewTaskRepo(pool)
	ctx := context.Background()

	id := shared.TaskId("integration-task-non-transfer")
	tk := task.New(id, task.Pick, shared.NewCPT(time.Now().Add(time.Hour)), "wu-plain", shared.NewCapabilitySet("pick"), false, false)
	if err := repo.Save(ctx, tk); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := repo.FindById(ctx, id)
	if err != nil || got == nil {
		t.Fatalf("find: %v %v", got, err)
	}
	if got.Transfer() != nil {
		t.Fatalf("non-transfer task rehydrated with a block: %+v", got.Transfer())
	}

	// And a legacy NULL row (written directly, as pre-migration rows
	// look) rehydrates without a block too.
	if _, err := pool.Exec(ctx, `INSERT INTO tasks (id, task_type, status, cpt, order_ref, required_capabilities, fragile, gift_wrap)
		VALUES ('integration-task-legacy-null', 'PICK', 'PENDING', now() + interval '1 hour', 'wu-legacy', '{}', false, false)`); err != nil {
		t.Fatalf("seed legacy row: %v", err)
	}
	legacy, err := repo.FindById(ctx, "integration-task-legacy-null")
	if err != nil || legacy == nil {
		t.Fatalf("find legacy: %v %v", legacy, err)
	}
	if legacy.Transfer() != nil {
		t.Fatalf("legacy NULL row rehydrated with a block: %+v", legacy.Transfer())
	}
}

// TestTaskRepo_UnknownWorkKindFailsRehydrate pins the read-side guard: a
// row whose work_kind is not one of the three declared kinds fails the
// rehydrate (matching the rehydrate-validation discipline of PR #157 for
// status/type strings) instead of silently creating a task whose
// completion would panic in fact selection.
func TestTaskRepo_UnknownWorkKindFailsRehydrate(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewTaskRepo(pool)
	ctx := context.Background()

	if _, err := pool.Exec(ctx, `INSERT INTO tasks (id, task_type, status, cpt, order_ref, required_capabilities, fragile, gift_wrap, transfer_ref, work_kind)
		VALUES ('integration-task-bad-kind', 'PICK', 'PENDING', now() + interval '1 hour', 'wu-bad', '{}', false, false, 'tr-bad', 'NOT_A_KIND')`); err != nil {
		t.Fatalf("seed bad row: %v", err)
	}
	if _, err := repo.FindById(ctx, "integration-task-bad-kind"); err == nil {
		t.Fatal("expected the rehydrate to fail on an unknown work_kind")
	}
}
