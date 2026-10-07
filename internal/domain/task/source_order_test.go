package task_test

import (
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// The source order id is the upstream ORDER the task's work was released
// for (WorkReleased.ref). It is distinct from OrderRef, which is the work
// unit id (per order line). Empty by default.

func TestSourceOrderId_EmptyByDefault(t *testing.T) {
	if got := newPickTask().SourceOrderId(); got != "" {
		t.Fatalf("SourceOrderId = %q, want empty by default", got)
	}
}

func TestWithSourceOrderId_StampsWithoutTouchingOrderRef(t *testing.T) {
	tk := task.New("t1", task.Pick, shared.NewCPT(now), "ORD-1-line-1", shared.NewCapabilitySet("pick"), false, false).WithSourceOrderId("ORD-1")
	if got := tk.SourceOrderId(); got != "ORD-1" {
		t.Errorf("SourceOrderId = %q, want ORD-1", got)
	}
	if got := tk.OrderRef(); got != "ORD-1-line-1" {
		t.Errorf("OrderRef = %q, want the unchanged work unit id", got)
	}
}
