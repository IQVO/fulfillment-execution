package task_test

import (
	"errors"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// The order site is the fulfillment site the ORDER named
// (WorkReleased.site_id on order work). It is distinct from the transfer
// block's SiteId and from the pick plan's SiteId. Empty by default.

func TestOrderSiteId_EmptyByDefault(t *testing.T) {
	if got := newPickTask().OrderSiteId(); got != "" {
		t.Fatalf("OrderSiteId = %q, want empty by default", got)
	}
}

func TestParseOrderSite(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "empty is a valid absent site", in: "", want: ""},
		{name: "upper-case letters", in: "WHA", want: "WHA"},
		{name: "digits", in: "42", want: "42"},
		{name: "letters and digits", in: "WH1", want: "WH1"},
		{name: "single character", in: "A", want: "A"},
		{name: "range edges A Z 0 9 are valid", in: "AZ09", want: "AZ09"},
		{name: "just below A is refused", in: "@", wantErr: true},
		{name: "just above Z is refused", in: "[", wantErr: true},
		{name: "just below 0 is refused", in: "/", wantErr: true},
		{name: "just above 9 is refused", in: ":", wantErr: true},
		{name: "one bad character after valid ones is refused", in: "WH1x", wantErr: true},
		{name: "lower case is refused", in: "wh1", wantErr: true},
		{name: "mixed case is refused", in: "Wh1", wantErr: true},
		{name: "hyphen is refused", in: "WH-1", wantErr: true},
		{name: "underscore is refused", in: "WH_1", wantErr: true},
		{name: "leading space is refused", in: " WH1", wantErr: true},
		{name: "trailing space is refused", in: "WH1 ", wantErr: true},
		{name: "whitespace only is refused", in: "  ", wantErr: true},
		{name: "trailing newline is refused", in: "WH1\n", wantErr: true},
		{name: "non-ASCII letter is refused", in: "WHÄ", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := task.ParseOrderSite(tc.in)
			if tc.wantErr {
				if !errors.Is(err, task.ErrInvalidOrderSite) {
					t.Fatalf("ParseOrderSite(%q) err = %v, want ErrInvalidOrderSite", tc.in, err)
				}
				if got != "" {
					t.Fatalf("ParseOrderSite(%q) = %q alongside an error, want empty", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseOrderSite(%q) unexpected error: %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("ParseOrderSite(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestWithOrderSiteId_StampsAValidSite(t *testing.T) {
	tk := newPickTask().WithOrderSiteId("WH1")
	if got := tk.OrderSiteId(); got != "WH1" {
		t.Fatalf("OrderSiteId = %q, want WH1", got)
	}
}

// An invalid site never enters the aggregate: the domain tolerates it as
// "no site" (like WithSourceLineNo does for an out-of-range line), so the
// consumer can never fail a release over a malformed optional field.
func TestWithOrderSiteId_InvalidLeavesNoSite(t *testing.T) {
	for _, bad := range []string{"wh1", "WH-1", " WH1", "WH1 ", "  "} {
		tk := newPickTask().WithOrderSiteId("WH1").WithOrderSiteId(bad)
		if got := tk.OrderSiteId(); got != "" {
			t.Errorf("WithOrderSiteId(%q): OrderSiteId = %q, want empty (stale value must be cleared too)", bad, got)
		}
	}
}

func TestWithOrderSiteId_EmptyClearsTheSite(t *testing.T) {
	if got := newPickTask().WithOrderSiteId("WH1").WithOrderSiteId("").OrderSiteId(); got != "" {
		t.Fatalf("OrderSiteId = %q, want empty", got)
	}
}

// The order site must not bleed into the other site-shaped fields of the
// aggregate: the transfer block keeps its own SiteId and the pick plan its
// own.
func TestWithOrderSiteId_LeavesTransferAndPickSiteAlone(t *testing.T) {
	plan, err := task.NewPickPlan("sku-1", 2, "EACH", "PICKSITE", "BIN-1")
	if err != nil {
		t.Fatal(err)
	}
	tk := newPickTask().WithPickPlan(&plan, 0).WithOrderSiteId("ORDERSITE")
	if got := tk.PickPlan().SiteId; got != "PICKSITE" {
		t.Errorf("pick plan site = %q, want PICKSITE untouched", got)
	}
	if got := tk.OrderSiteId(); got != "ORDERSITE" {
		t.Errorf("OrderSiteId = %q, want ORDERSITE", got)
	}

	transfer := task.NewTransferTask(shared.TaskId("tt"), task.Pick, shared.NewCPT(now), "demand-1", shared.NewCapabilitySet("pick"), false, false,
		task.TransferDetails{TransferRef: "tr-1", DemandId: "demand-1", WorkKind: task.WorkKindTransferPick, SiteId: "ORIGIN", SKU: "sku-1", Quantity: 3})
	if got := transfer.OrderSiteId(); got != "" {
		t.Errorf("a transfer task's OrderSiteId = %q, want empty", got)
	}
}
