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

// TestTaskRepo_OrderSiteIdRoundTrip proves migration 0028's nullable
// order_site_id column round-trips (ADR 0065): a task saved with the
// order's site rehydrates with it on every read path, a task saved without
// one (the shape of every pre-migration row) rehydrates with "", and the
// column is its own: the pick plan's site and the transfer block's site
// stay in theirs.
func TestTaskRepo_OrderSiteIdRoundTrip(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewTaskRepo(pool)
	ctx := context.Background()
	cpt := shared.NewCPT(time.Now().Add(time.Hour).Truncate(time.Microsecond))
	caps := shared.NewCapabilitySet("pick")

	plan, err := task.NewPickPlan("sku-1", 2, "EACH", "PICKSITE", "BIN-1")
	if err != nil {
		t.Fatal(err)
	}
	with := task.New("it-order-site", task.Pick, cpt, "ORD-7-line-1", caps, false, false).
		WithSourceOrderId("ORD-7").WithOrderSiteId("WH1").WithPickPlan(&plan, 0)
	without := task.New("it-no-order-site", task.Pick, cpt, "ORD-7-line-2", caps, false, false).WithSourceOrderId("ORD-7")
	transfer := task.NewTransferTask("it-transfer-site", task.Pick, cpt, "DEMAND-1", caps, false, false,
		task.TransferDetails{TransferRef: "tr-1", DemandId: "DEMAND-1", WorkKind: task.WorkKindTransferPick, SiteId: "ORIGIN", SKU: "sku-1", Quantity: 3})
	for _, tk := range []*task.Task{with, without, transfer} {
		if err := repo.Save(ctx, tk); err != nil {
			t.Fatalf("save %s: %v", tk.Id(), err)
		}
	}

	got, err := repo.FindById(ctx, "it-order-site")
	if err != nil || got == nil {
		t.Fatalf("find: %v %v", got, err)
	}
	if got.OrderSiteId() != "WH1" {
		t.Fatalf("OrderSiteId = %q, want WH1", got.OrderSiteId())
	}
	if got.PickPlan() == nil || got.PickPlan().SiteId != "PICKSITE" {
		t.Fatalf("pick plan site = %+v, want PICKSITE kept in its own column", got.PickPlan())
	}

	got, err = repo.FindById(ctx, "it-no-order-site")
	if err != nil || got == nil {
		t.Fatalf("find: %v %v", got, err)
	}
	if got.OrderSiteId() != "" {
		t.Fatalf("OrderSiteId = %q, want empty for a task saved without one", got.OrderSiteId())
	}

	got, err = repo.FindById(ctx, "it-transfer-site")
	if err != nil || got == nil {
		t.Fatalf("find: %v %v", got, err)
	}
	if got.OrderSiteId() != "" || got.Transfer() == nil || got.Transfer().SiteId != "ORIGIN" {
		t.Fatalf("transfer task: OrderSiteId=%q transfer=%+v, want empty order site and the transfer site ORIGIN untouched", got.OrderSiteId(), got.Transfer())
	}

	// The upsert path rewrites the column: clearing it stores NULL again.
	if err := repo.Save(ctx, with.WithOrderSiteId("")); err != nil {
		t.Fatal(err)
	}
	if got, _ = repo.FindById(ctx, "it-order-site"); got.OrderSiteId() != "" {
		t.Fatalf("OrderSiteId = %q after an upsert that cleared it, want empty", got.OrderSiteId())
	}
	if err := repo.Save(ctx, with.WithOrderSiteId("WH2")); err != nil {
		t.Fatal(err)
	}
	if got, _ = repo.FindById(ctx, "it-order-site"); got.OrderSiteId() != "WH2" {
		t.Fatalf("OrderSiteId = %q after an upsert, want WH2", got.OrderSiteId())
	}
}

// TestTaskRepo_FindBySourceOrderId proves the lookup behind the rebin PACK
// task's site inheritance (ADR 0065): every task whose source_order_id is
// the order, ordered by id, with its order site; an unknown order and the
// empty string match nothing.
func TestTaskRepo_FindBySourceOrderId(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewTaskRepo(pool)
	ctx := context.Background()
	cpt := shared.NewCPT(time.Now().Add(time.Hour).Truncate(time.Microsecond))
	caps := shared.NewCapabilitySet("pick")

	for _, tk := range []*task.Task{
		task.New("it-src-b", task.Pick, cpt, "ORD-8-line-2", caps, false, false).WithSourceOrderId("ORD-8"),
		task.New("it-src-a", task.Pick, cpt, "ORD-8-line-1", caps, false, false).WithSourceOrderId("ORD-8").WithOrderSiteId("WH1"),
		task.New("it-src-other", task.Pick, cpt, "ORD-9-line-1", caps, false, false).WithSourceOrderId("ORD-9").WithOrderSiteId("WH2"),
		task.New("it-src-none", task.Pick, cpt, "ORD-8", caps, false, false), // NULL source_order_id
	} {
		if err := repo.Save(ctx, tk); err != nil {
			t.Fatalf("save %s: %v", tk.Id(), err)
		}
	}

	got, err := repo.FindBySourceOrderId(ctx, "ORD-8")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Id() != "it-src-a" || got[1].Id() != "it-src-b" {
		t.Fatalf("FindBySourceOrderId(ORD-8) returned %d tasks, want [it-src-a it-src-b] ordered by id", len(got))
	}
	if got[0].OrderSiteId() != "WH1" || got[1].OrderSiteId() != "" {
		t.Fatalf("sites = %q, %q, want WH1 and empty", got[0].OrderSiteId(), got[1].OrderSiteId())
	}

	for _, id := range []string{"ORD-404", ""} {
		none, err := repo.FindBySourceOrderId(ctx, id)
		if err != nil || len(none) != 0 {
			t.Fatalf("FindBySourceOrderId(%q) = %d tasks, %v, want none", id, len(none), err)
		}
	}
}
