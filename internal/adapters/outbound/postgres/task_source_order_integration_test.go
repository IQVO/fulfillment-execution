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

// TestTaskRepo_SourceOrderIdRoundTrip proves migration 0016's nullable
// source_order_id column round-trips (ADR 0040): a task saved with an order
// id rehydrates with it on every read path, and a task saved without one
// (the shape of every pre-migration row) rehydrates with "". The task's own
// orderRef (the work unit id) is stored in its own column and unaffected.
func TestTaskRepo_SourceOrderIdRoundTrip(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewTaskRepo(pool)
	ctx := context.Background()
	cpt := shared.NewCPT(time.Now().Add(time.Hour))

	with := task.New("integration-task-src-order", task.Pick, cpt, "ORD-5-line-1", shared.NewCapabilitySet("pick"), false, false).WithSourceOrderId("ORD-5")
	without := task.New("integration-task-no-src-order", task.Pick, cpt, "ORD-5-line-2", shared.NewCapabilitySet("pick"), false, false)
	for _, tk := range []*task.Task{with, without} {
		if err := repo.Save(ctx, tk); err != nil {
			t.Fatalf("save %s: %v", tk.Id(), err)
		}
	}

	got, err := repo.FindById(ctx, "integration-task-src-order")
	if err != nil || got == nil {
		t.Fatalf("find: %v %v", got, err)
	}
	if got.SourceOrderId() != "ORD-5" || got.OrderRef() != "ORD-5-line-1" {
		t.Fatalf("round-trip: SourceOrderId=%q OrderRef=%q, want ORD-5 / ORD-5-line-1", got.SourceOrderId(), got.OrderRef())
	}

	got, err = repo.FindById(ctx, "integration-task-no-src-order")
	if err != nil || got == nil {
		t.Fatalf("find: %v %v", got, err)
	}
	if got.SourceOrderId() != "" {
		t.Fatalf("SourceOrderId = %q, want empty for a task saved without one", got.SourceOrderId())
	}

	// The bulk read path (scanTasks) carries it too.
	list, err := repo.FindByOrderRef(ctx, "ORD-5-line-1")
	if err != nil || len(list) != 1 || list[0].SourceOrderId() != "ORD-5" {
		t.Fatalf("FindByOrderRef = %v err=%v, want one task with SourceOrderId ORD-5", list, err)
	}
}
