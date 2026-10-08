package task_test

import (
	"math"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// The source line number is the 1-based order line the task's work was
// released for (WorkReleased.line_no, per-line confirm-pick, ADR 0041).
// 0 means unknown; it is never derived from the "<order>-line-<n>" id.

func TestSourceLineNo_UnknownByDefault(t *testing.T) {
	if got := newPickTask().SourceLineNo(); got != 0 {
		t.Fatalf("SourceLineNo = %d, want 0 (unknown) by default", got)
	}
}

func TestWithSourceLineNo_StampsWithoutTouchingOrderRefOrSourceOrder(t *testing.T) {
	tk := task.New("t1", task.Pick, shared.NewCPT(now), "ORD-1-line-3", shared.NewCapabilitySet("pick"), false, false).
		WithSourceOrderId("ORD-1").
		WithSourceLineNo(3)
	if got := tk.SourceLineNo(); got != 3 {
		t.Errorf("SourceLineNo = %d, want 3", got)
	}
	if got := tk.OrderRef(); got != "ORD-1-line-3" {
		t.Errorf("OrderRef = %q, want the unchanged work unit id", got)
	}
	if got := tk.SourceOrderId(); got != "ORD-1" {
		t.Errorf("SourceOrderId = %q, want ORD-1", got)
	}
}

// The source_line_no column is a 32-bit INTEGER, so the domain can only hold
// 1..math.MaxInt32; anything above is unknown (0), never an out-of-range
// value that would fail the tasks insert.
func TestWithSourceLineNo_BoundaryTable(t *testing.T) {
	for _, tc := range []struct {
		in   int
		want int
	}{
		{0, 0},
		{1, 1},
		{math.MaxInt32, math.MaxInt32},
		{math.MaxInt32 + 1, 0},
		{math.MaxInt64, 0},
	} {
		if got := newPickTask().WithSourceLineNo(tc.in).SourceLineNo(); got != tc.want {
			t.Errorf("WithSourceLineNo(%d).SourceLineNo() = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestWithSourceLineNo_NonPositiveMeansUnknown(t *testing.T) {
	for _, n := range []int{0, -1} {
		if got := newPickTask().WithSourceLineNo(n).SourceLineNo(); got != 0 {
			t.Errorf("WithSourceLineNo(%d).SourceLineNo() = %d, want 0 (unknown)", n, got)
		}
	}
}
