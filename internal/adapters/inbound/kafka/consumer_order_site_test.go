package kafka_test

import (
	"context"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
)

// WorkReleased.site_id on ORDER work is the fulfillment site the order
// named (order-management's OrderAllocated.site_id, copied by
// wes-work-planning). It is decoded and kept on the created task as its
// order site (ADR 0065) — until now it was used only for transfer work.
func TestHandleMessage_OrderWorkKeepsSiteId(t *testing.T) {
	c, tasks := newConsumer(t)

	raw := workReleasedJSONWithLine("evt-order-site", "PICK", "ORD-7-line-1", map[string]any{"site_id": "WH1"})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tk := pickTask(t, tasks)
	if got := tk.OrderSiteId(); got != "WH1" {
		t.Fatalf("OrderSiteId = %q, want WH1 (from data.site_id)", got)
	}
	if got := tk.SourceOrderId(); got != "ORD-7" {
		t.Fatalf("SourceOrderId = %q, want ORD-7 (unchanged by the site)", got)
	}
	if tk.Transfer() != nil {
		t.Fatalf("an order release must not become transfer work: %+v", tk.Transfer())
	}
}

// A producer that does not know the site (omit-when-unknown, every
// WorkReleased before the order site flowed through) decodes exactly as
// before: no order site.
func TestHandleMessage_OrderWorkWithoutSiteIdHasNoSite(t *testing.T) {
	c, tasks := newConsumer(t)

	if err := c.HandleMessage(context.Background(), workReleasedJSONWithLine("evt-no-site", "PICK", "ORD-7-line-1", nil)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := pickTask(t, tasks).OrderSiteId(); got != "" {
		t.Fatalf("OrderSiteId = %q, want empty when data.site_id is absent", got)
	}
}

// An invalid site is tolerated, never a DLQ: the handler returns nil (no
// retry, no dead-letter), and the task is created without a site.
func TestHandleMessage_InvalidSiteIdIsToleratedAsNoSite(t *testing.T) {
	for _, bad := range []string{"wh1", "WH-1", " WH1", "WH 1", "  ", "WH1\n"} {
		t.Run(bad, func(t *testing.T) {
			c, tasks := newConsumer(t)

			raw := workReleasedJSONWithLine("evt-bad-site", "PICK", "ORD-7-line-1", map[string]any{"site_id": bad})
			if err := c.HandleMessage(context.Background(), raw); err != nil {
				t.Fatalf("an invalid site_id must not fail the message: %v", err)
			}
			if got := pickTask(t, tasks).OrderSiteId(); got != "" {
				t.Fatalf("OrderSiteId = %q, want empty for the invalid site %q", got, bad)
			}
		})
	}
}

// Transfer work keeps its site inside the transfer block only: the order
// site stays empty and the transfer fact's site is untouched.
func TestHandleMessage_TransferWorkKeepsSiteOnlyInTheTransferBlock(t *testing.T) {
	c, tasks := newConsumer(t)

	raw := workReleasedJSONWithLine("evt-tr-site", "PICK", "demand-1", map[string]any{
		"ref":          "demand-1",
		"transfer_ref": "tr-1",
		"work_kind":    "TRANSFER_PICK",
		"site_id":      "WHA",
		"sku":          "SKU-1",
		"quantity":     3,
	})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tk := pickTask(t, tasks)
	if tk.OrderSiteId() != "" {
		t.Fatalf("OrderSiteId = %q, want empty for transfer work", tk.OrderSiteId())
	}
	if tr := tk.Transfer(); tr == nil || tr.SiteId != "WHA" {
		t.Fatalf("transfer = %+v, want its site WHA unchanged", tr)
	}
}

// The order site and the pick plan's site are independent fields: a PICK
// release carrying both keeps both, each in its own place.
func TestHandleMessage_OrderSiteAndPickPlanSiteAreIndependent(t *testing.T) {
	c, tasks := newConsumer(t)

	raw := workReleasedCE("evt-both-sites", kafka.TypeWorkReleased, "ORD-7-line-1", map[string]any{
		"path_id":      "PICK",
		"work_unit_id": "ORD-7-line-1",
		"cpt":          "2026-08-21T23:00:00Z",
		"ref":          "ORD-7",
		"site_id":      "WH1",
		"pick":         map[string]any{"sku": "SKU-1", "quantity": 2, "unit": "EACH", "site_id": "WH2", "from_bin": pickFace},
	})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tk := pickTask(t, tasks)
	if tk.OrderSiteId() != "WH1" || tk.PickPlan() == nil || tk.PickPlan().SiteId != "WH2" {
		t.Fatalf("order site = %q, pick plan = %+v, want WH1 and the plan's own site WH2", tk.OrderSiteId(), tk.PickPlan())
	}
}
