package kafka_test

import (
	"context"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
)

// pickFace is the planned source pick face of these fixtures (ADR 0054's
// from_bin); not a facility-layout LocationCode on purpose — the site of a
// pick never comes from the bin (ADR 0062).
const pickFace = "A-01-01"

// workReleasedJSONWithPick builds a WorkReleased of the PICK path carrying
// data.pick exactly as wes-work-planning publishes it (ADR 0054 plus the
// pinned additive site_id of ADR 0062: omit-when-unknown).
func workReleasedJSONWithPick(eventId, workUnitId string, pick map[string]any) []byte {
	data := map[string]any{
		"path_id":      "PICK",
		"work_unit_id": workUnitId,
		"cpt":          "2026-08-21T23:00:00Z",
		"ref":          "ORD-7",
	}
	if pick != nil {
		data["pick"] = pick
	}
	return workReleasedCE(eventId, kafka.TypeWorkReleased, workUnitId, data)
}

// WorkReleased.data.pick.site_id and from_bin are decoded into the plan
// (the producer's pinned additive contract), and a plan with both gives the
// task a placement (ADR 0062).
func TestHandleMessage_ReadsPickSiteAndFromBin(t *testing.T) {
	c, tasks := newConsumer(t)

	raw := workReleasedJSONWithPick("evt-pick-site", "ORD-7-line-1", map[string]any{
		"sku": "SKU-1", "quantity": 2, "unit": "EACH",
		"site_id": "WH1", "from_bin": pickFace,
	})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tk := pickTask(t, tasks)
	plan := tk.PickPlan()
	if plan == nil || plan.SiteId != "WH1" || plan.FromBin != pickFace {
		t.Fatalf("plan = %+v, want site WH1 and pick face %s", plan, pickFace)
	}
	place, ok := tk.Placement()
	if !ok || place.SiteId != "WH1" || place.FromLocation != pickFace || place.ToLocation != pickFace {
		t.Fatalf("Placement() = (%+v, %v), want site WH1 and both ends %s", place, ok, pickFace)
	}
}

// A producer that predates site_id (every WorkReleased today) must decode
// exactly as before: the site is empty, the pick face passes through, and
// the task carries no placement — it is not announced (ADR 0062,
// omit-when-unknown).
func TestHandleMessage_PickWithoutSiteBehavesAsBefore(t *testing.T) {
	c, tasks := newConsumer(t)

	raw := workReleasedJSONWithPick("evt-pick-nosite", "ORD-7-line-2", map[string]any{
		"sku": "SKU-1", "quantity": 2, "unit": "EACH", "from_bin": pickFace,
	})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tk := pickTask(t, tasks)
	if plan := tk.PickPlan(); plan == nil || plan.SiteId != "" || plan.FromBin != pickFace {
		t.Fatalf("plan = %+v, want no site and pick face %s", plan, pickFace)
	}
	if _, ok := tk.Placement(); ok {
		t.Fatal("a pick with no site must carry no placement")
	}
}

// The site alone is not enough either: a plan with a site but no bin has no
// from_location, so it is not announced.
func TestHandleMessage_PickWithoutBinIsNotAnnounced(t *testing.T) {
	c, tasks := newConsumer(t)

	raw := workReleasedJSONWithPick("evt-pick-nobin", "ORD-7-line-3", map[string]any{
		"sku": "SKU-1", "quantity": 2, "unit": "EACH", "site_id": "WH1",
	})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tk := pickTask(t, tasks)
	if plan := tk.PickPlan(); plan == nil || plan.SiteId != "WH1" || plan.FromBin != "" {
		t.Fatalf("plan = %+v, want site WH1 and no pick face", plan)
	}
	if _, ok := tk.Placement(); ok {
		t.Fatal("a pick with no pick face must carry no placement")
	}
}

// A PICK released with no pick object at all (every order-driven release
// today) keeps completing as before: no plan, no placement.
func TestHandleMessage_PickWithoutPlanHasNoPlacement(t *testing.T) {
	c, tasks := newConsumer(t)

	if err := c.HandleMessage(context.Background(), workReleasedJSONWithPick("evt-pick-noplan", "ORD-7-line-4", nil)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tk := pickTask(t, tasks)
	if tk.PickPlan() != nil {
		t.Fatalf("plan = %+v, want none", tk.PickPlan())
	}
	if _, ok := tk.Placement(); ok {
		t.Fatal("a pick with no plan must carry no placement")
	}
}
