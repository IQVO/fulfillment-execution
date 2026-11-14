package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

var orderSiteCPT = shared.NewCPT(time.Date(2026, 11, 14, 18, 0, 0, 0, time.UTC))

func orderSiteTask(id, orderRef, sourceOrderId, site string) *task.Task {
	return task.New(shared.TaskId(id), task.Pick, orderSiteCPT, shared.OrderRef(orderRef), shared.NewCapabilitySet("pick"), false, false).
		WithSourceOrderId(sourceOrderId).WithOrderSiteId(site)
}

// The in-memory repo stands in for Postgres, which rehydrates a fresh
// aggregate on every read: the order site must round-trip and the store
// must hold COPIES, never the caller's pointer.
func TestTaskRepo_OrderSiteRoundTripsAndIsCopied(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewTaskRepo()
	tk := orderSiteTask("t-1", "ORD-1-line-1", "ORD-1", "WH1")
	if err := repo.Save(ctx, tk); err != nil {
		t.Fatal(err)
	}

	// Mutating the saved pointer afterwards must not reach the store.
	tk.WithOrderSiteId("OTHER")
	got, err := repo.FindById(ctx, "t-1")
	if err != nil || got == nil {
		t.Fatalf("FindById = %v, %v", got, err)
	}
	if got.OrderSiteId() != "WH1" {
		t.Fatalf("stored order site = %q, want WH1 (the repo must store a copy)", got.OrderSiteId())
	}

	// Mutating a read result must not reach the store either.
	got.WithOrderSiteId("MUTATED")
	again, _ := repo.FindById(ctx, "t-1")
	if again.OrderSiteId() != "WH1" {
		t.Fatalf("stored order site = %q after mutating a read, want WH1 (the repo must return copies)", again.OrderSiteId())
	}

	// SaveClaim stores a copy that keeps the site too.
	claimable, _ := repo.FindById(ctx, "t-1")
	if err := claimable.Claim("stn-1", shared.NewCapabilitySet("pick"), orderSiteCPT.Time().Add(-time.Hour), 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	if ok, err := repo.SaveClaim(ctx, claimable, orderSiteCPT.Time().Add(-time.Hour)); err != nil || !ok {
		t.Fatalf("SaveClaim = %v, %v", ok, err)
	}
	claimed, _ := repo.FindById(ctx, "t-1")
	if claimed.OrderSiteId() != "WH1" {
		t.Fatalf("order site after SaveClaim = %q, want WH1", claimed.OrderSiteId())
	}
}

func TestTaskRepo_FindBySourceOrderId(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewTaskRepo()
	for _, tk := range []*task.Task{
		orderSiteTask("t-b", "ORD-1-line-2", "ORD-1", ""),
		orderSiteTask("t-a", "ORD-1-line-1", "ORD-1", "WH1"),
		orderSiteTask("t-c", "ORD-2-line-1", "ORD-2", "WH2"),
		orderSiteTask("t-d", "ORD-1", "", "WH9"), // no source order id: never matched, not even by ""
	} {
		if err := repo.Save(ctx, tk); err != nil {
			t.Fatal(err)
		}
	}

	got, err := repo.FindBySourceOrderId(ctx, "ORD-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Id() != "t-a" || got[1].Id() != "t-b" {
		t.Fatalf("FindBySourceOrderId(ORD-1) = %v, want [t-a t-b] ordered by id", ids(got))
	}
	if got[0].OrderSiteId() != "WH1" || got[1].OrderSiteId() != "" {
		t.Fatalf("sites = %q, %q, want WH1 and empty", got[0].OrderSiteId(), got[1].OrderSiteId())
	}

	// The results are copies.
	got[0].WithOrderSiteId("MUTATED")
	if again, _ := repo.FindById(ctx, "t-a"); again.OrderSiteId() != "WH1" {
		t.Fatalf("stored order site = %q after mutating a result, want WH1", again.OrderSiteId())
	}

	none, err := repo.FindBySourceOrderId(ctx, "ORD-404")
	if err != nil || len(none) != 0 {
		t.Fatalf("FindBySourceOrderId(unknown) = %v, %v, want empty and no error", ids(none), err)
	}
	empty, err := repo.FindBySourceOrderId(ctx, "")
	if err != nil || len(empty) != 0 {
		t.Fatalf("FindBySourceOrderId(\"\") = %v, %v, want empty: \"\" is not an order id", ids(empty), err)
	}
}

func ids(ts []*task.Task) []shared.TaskId {
	out := make([]shared.TaskId, 0, len(ts))
	for _, tk := range ts {
		out = append(out, tk.Id())
	}
	return out
}
