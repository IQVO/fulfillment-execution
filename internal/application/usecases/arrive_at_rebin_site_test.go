package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// seedOrderTask saves a task released for sourceOrderId at site ("" = no
// site) the way ApplyWorkReleased would have.
func seedOrderTask(t *testing.T, repo interface {
	Save(context.Context, *task.Task) error
}, id string, taskType task.Type, orderRef, sourceOrderId, site string) {
	t.Helper()
	tk := task.New(shared.TaskId(id), taskType, shared.NewCPT(epoch.Add(time.Hour)), shared.OrderRef(orderRef), shared.NewCapabilitySet("pick"), false, false).
		WithSourceOrderId(sourceOrderId).WithOrderSiteId(site)
	if err := repo.Save(context.Background(), tk); err != nil {
		t.Fatalf("seed %s: %v", id, err)
	}
}

// packTaskOf returns the single PACK task of orderRef created at rebin.
func packTaskOf(t *testing.T, tasks *memory.TaskRepo, orderRef shared.OrderRef) *task.Task {
	t.Helper()
	all, err := tasks.FindByOrderRef(context.Background(), orderRef)
	if err != nil || len(all) != 1 || all[0].Type() != task.Pack {
		t.Fatalf("want exactly one PACK task for %s, got %d err=%v", orderRef, len(all), err)
	}
	return all[0]
}

// ADR 0065: the rebin PACK task takes the order site from the order's PICK
// tasks (released with WorkReleased.site_id), so the PackageManifested sealed
// from it can publish site_id. FE-only: nothing new rides on the rebin
// arrival request.
func TestArriveAtRebin_PackTaskTakesTheSiteOfTheOrdersPickTasks(t *testing.T) {
	for _, tc := range []struct {
		name string
		seed func(t *testing.T, tasks *memory.TaskRepo)
		want string
	}{
		{"one PICK with a site", func(t *testing.T, tasks *memory.TaskRepo) {
			seedOrderTask(t, tasks, "pick-a", task.Pick, "order-1-line-1", "order-1", "WH1")
		}, "WH1"},
		{"the first non-empty site by task id wins over an earlier siteless PICK", func(t *testing.T, tasks *memory.TaskRepo) {
			seedOrderTask(t, tasks, "pick-a", task.Pick, "order-1-line-1", "order-1", "")
			seedOrderTask(t, tasks, "pick-b", task.Pick, "order-1-line-2", "order-1", "WH1")
			seedOrderTask(t, tasks, "pick-c", task.Pick, "order-1-line-3", "order-1", "WH2")
		}, "WH1"},
		{"no PICK carries a site: the PACK task has none", func(t *testing.T, tasks *memory.TaskRepo) {
			seedOrderTask(t, tasks, "pick-a", task.Pick, "order-1-line-1", "order-1", "")
			seedOrderTask(t, tasks, "pick-b", task.Pick, "order-1-line-2", "order-1", "")
		}, ""},
		{"no PICK task at all: the PACK task has none", func(*testing.T, *memory.TaskRepo) {}, ""},
		{"another order's PICK site never leaks", func(t *testing.T, tasks *memory.TaskRepo) {
			seedOrderTask(t, tasks, "pick-a", task.Pick, "order-2-line-1", "order-2", "WH9")
			seedOrderTask(t, tasks, "pick-b", task.Pick, "order-10-line-1", "order-10", "WH8")
		}, ""},
		{"only PICK tasks count: a site on another task type is ignored", func(t *testing.T, tasks *memory.TaskRepo) {
			seedOrderTask(t, tasks, "slam-a", task.Slam, "order-1-slam", "order-1", "WH7")
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uc, _, tasks, _ := newArriveAtRebinHarness()
			tc.seed(t, tasks)
			cpt := shared.NewCPT(epoch.Add(2 * time.Hour))

			if err := uc.Execute(context.Background(), "order-1", "line-1", []string{"line-1"}, cpt, shared.NewCapabilitySet("pack"), false, false); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			pack := packTaskOf(t, tasks, "order-1")
			if got := pack.OrderSiteId(); got != tc.want {
				t.Fatalf("PACK OrderSiteId = %q, want %q", got, tc.want)
			}
			if pack.SourceOrderId() != "order-1" || !pack.CPT().Time().Equal(cpt.Time()) {
				t.Fatalf("PACK source order %q cpt %v: the site must not disturb the stamp or the CPT", pack.SourceOrderId(), pack.CPT().Time())
			}
		})
	}
}

// A multi-line order creates its PACK task on the LAST arrival; the site is
// read then, not on the first arrival.
func TestArriveAtRebin_MultiLineOrderReadsTheSiteWhenPackIsCreated(t *testing.T) {
	uc, _, tasks, _ := newArriveAtRebinHarness()
	ctx := context.Background()
	seedOrderTask(t, tasks, "pick-a", task.Pick, "order-1-line-1", "order-1", "WH1")
	required := []string{"line-1", "line-2"}
	cpt := shared.NewCPT(epoch.Add(time.Hour))
	caps := shared.NewCapabilitySet("pack")

	if err := uc.Execute(ctx, "order-1", "line-1", required, cpt, caps, false, false); err != nil {
		t.Fatal(err)
	}
	if err := uc.Execute(ctx, "order-1", "line-2", required, cpt, caps, false, false); err != nil {
		t.Fatal(err)
	}
	all, _ := tasks.FindByOrderRef(ctx, "order-1")
	if len(all) != 1 || all[0].OrderSiteId() != "WH1" {
		t.Fatalf("PACK tasks = %d, site %q, want one PACK task at WH1", len(all), siteOfFirst(all))
	}
}

func siteOfFirst(ts []*task.Task) string {
	if len(ts) == 0 {
		return "<none>"
	}
	return ts[0].OrderSiteId()
}

// The site lookup runs inside the atomic scope: a transient repo failure must
// fail the arrival (so the caller retries) instead of silently creating a
// PACK task with no site — outbound-shipping ignores a manifest without one.
func TestArriveAtRebin_SiteLookupFailureFailsTheArrivalAndCreatesNoPackTask(t *testing.T) {
	uc, _, tasks, publisher := newFailingArriveAtRebinHarness(func(_ *errConsolidationRepo, tasks *errTaskRepo) {
		tasks.failFindBySourceOrderId = true
	})
	ctx := context.Background()

	err := uc.Execute(ctx, "order-1", "line-1", []string{"line-1"}, shared.NewCPT(epoch.Add(time.Hour)), shared.NewCapabilitySet("pack"), false, false)
	if !errors.Is(err, errFake) {
		t.Fatalf("expected the site lookup error to propagate, got %v", err)
	}
	if all, _ := tasks.FindByOrderRef(ctx, "order-1"); len(all) != 0 {
		t.Fatalf("expected no PACK task, got %d", len(all))
	}
	for _, e := range publisher.Events() {
		if e.EventName() == "OrderConsolidated" {
			t.Fatalf("no OrderConsolidated may follow a failed PACK creation, got %+v", publisher.Events())
		}
	}
}
