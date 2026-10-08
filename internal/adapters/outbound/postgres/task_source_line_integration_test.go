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

// TestTaskRepo_SourceLineNoRoundTrip proves migration 0017's nullable
// source_line_no column round-trips (ADR 0041, decision 18): a task saved
// with a line rehydrates with it on every read path, and a task saved
// without one (the shape of every pre-migration row) rehydrates with 0
// (unknown). The work unit id and source order id are unaffected.
func TestTaskRepo_SourceLineNoRoundTrip(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewTaskRepo(pool)
	ctx := context.Background()
	cpt := shared.NewCPT(time.Now().Add(time.Hour))

	with := task.New("integration-task-src-line", task.Pick, cpt, "ORD-6-line-3", shared.NewCapabilitySet("pick"), false, false).
		WithSourceOrderId("ORD-6").WithSourceLineNo(3)
	without := task.New("integration-task-no-src-line", task.Pick, cpt, "ORD-6-line-4", shared.NewCapabilitySet("pick"), false, false).
		WithSourceOrderId("ORD-6")
	for _, tk := range []*task.Task{with, without} {
		if err := repo.Save(ctx, tk); err != nil {
			t.Fatalf("save %s: %v", tk.Id(), err)
		}
	}

	got, err := repo.FindById(ctx, "integration-task-src-line")
	if err != nil || got == nil {
		t.Fatalf("find: %v %v", got, err)
	}
	if got.SourceLineNo() != 3 || got.SourceOrderId() != "ORD-6" || got.OrderRef() != "ORD-6-line-3" {
		t.Fatalf("round-trip: SourceLineNo=%d SourceOrderId=%q OrderRef=%q, want 3 / ORD-6 / ORD-6-line-3", got.SourceLineNo(), got.SourceOrderId(), got.OrderRef())
	}

	got, err = repo.FindById(ctx, "integration-task-no-src-line")
	if err != nil || got == nil {
		t.Fatalf("find: %v %v", got, err)
	}
	if got.SourceLineNo() != 0 {
		t.Fatalf("SourceLineNo = %d, want 0 (unknown) for a task saved without one", got.SourceLineNo())
	}

	// The bulk read path (scanTasks) carries it too.
	list, err := repo.FindByOrderRef(ctx, "ORD-6-line-3")
	if err != nil || len(list) != 1 || list[0].SourceLineNo() != 3 {
		t.Fatalf("FindByOrderRef = %v err=%v, want one task with SourceLineNo 3", list, err)
	}
}
