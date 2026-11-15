package consolidation_test

import (
	"errors"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/domain/consolidation"
)

func TestNew_IncompleteWithNoArrivals(t *testing.T) {
	oc := consolidation.New("order-1", []string{"line-1", "line-2"})
	if oc.IsComplete() {
		t.Fatal("expected incomplete with zero lines arrived")
	}
	if oc.OrderRef() != "order-1" {
		t.Fatalf("expected OrderRef order-1, got %q", oc.OrderRef())
	}
}

func TestRecordArrival_IncompleteUntilAllRequiredLinesArrive(t *testing.T) {
	oc := consolidation.New("order-1", []string{"line-1", "line-2"})

	if err := oc.RecordArrival("line-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if oc.IsComplete() {
		t.Fatal("expected incomplete with one of two lines arrived")
	}

	if err := oc.RecordArrival("line-2"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !oc.IsComplete() {
		t.Fatal("expected complete once all required lines have arrived")
	}
}

// Invariant: an arrival for a line that was never part of this order's
// required set is rejected — this is a caller/data-integrity error, not a
// legitimate case to silently ignore.
func TestRecordArrival_RejectsUnknownLine(t *testing.T) {
	oc := consolidation.New("order-1", []string{"line-1"})
	err := oc.RecordArrival("line-not-in-order")
	if !errors.Is(err, consolidation.ErrUnknownLine) {
		t.Fatalf("expected ErrUnknownLine, got %v", err)
	}
	if oc.IsComplete() {
		t.Fatal("a rejected arrival must not affect completeness")
	}
}

// Idempotency: a redelivered ItemArrivedAtRebin event for an
// already-recorded line must be a no-op success, not an error — mirrors
// WorkPool.Complete's redelivery tolerance elsewhere in this fleet.
func TestRecordArrival_IdempotentOnRedelivery(t *testing.T) {
	oc := consolidation.New("order-1", []string{"line-1"})
	if err := oc.RecordArrival("line-1"); err != nil {
		t.Fatalf("unexpected error on first arrival: %v", err)
	}
	if err := oc.RecordArrival("line-1"); err != nil {
		t.Fatalf("expected idempotent re-arrival to be a no-op success, got %v", err)
	}
	if !oc.IsComplete() {
		t.Fatal("expected complete after the (idempotent) arrival")
	}
}

// HasArrived is how the use case recognises a redelivery: true only for a
// required line that has really arrived, false for a line not yet seen and
// for a line outside the required set (that one must still fail in
// RecordArrival, never be swallowed as a duplicate).
func TestHasArrived(t *testing.T) {
	oc := consolidation.New("order-1", []string{"line-1", "line-2"})
	if oc.HasArrived("line-1") {
		t.Fatal("line-1 has not arrived yet")
	}
	if err := oc.RecordArrival("line-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !oc.HasArrived("line-1") {
		t.Fatal("line-1 arrived")
	}
	if oc.HasArrived("line-2") {
		t.Fatal("line-2 has not arrived")
	}
	if oc.HasArrived("line-9") {
		t.Fatal("a line outside the required set never counts as arrived")
	}
	rehydrated := consolidation.Rehydrate("order-1", []string{"line-1", "line-2"}, []string{"line-2"})
	if !rehydrated.HasArrived("line-2") || rehydrated.HasArrived("line-1") {
		t.Fatal("HasArrived must reflect rehydrated state")
	}
}

func TestRequiredAndArrivedLineIds_ReflectState(t *testing.T) {
	oc := consolidation.New("order-1", []string{"line-1", "line-2"})
	_ = oc.RecordArrival("line-1")

	required := oc.RequiredLineIds()
	if len(required) != 2 {
		t.Fatalf("expected 2 required line ids, got %v", required)
	}
	arrived := oc.ArrivedLineIds()
	if len(arrived) != 1 || arrived[0] != "line-1" {
		t.Fatalf("expected arrived line ids [line-1], got %v", arrived)
	}
}

func TestRehydrate_ReconstructsArrivedState(t *testing.T) {
	oc := consolidation.Rehydrate("order-1", []string{"line-1", "line-2"}, []string{"line-1"})
	if oc.IsComplete() {
		t.Fatal("expected incomplete: only one of two required lines was rehydrated as arrived")
	}
	if err := oc.RecordArrival("line-2"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !oc.IsComplete() {
		t.Fatal("expected complete once the second line arrives after rehydration")
	}
}

// A single-line order (the ClassSingle case from order-management's
// FulfillmentClass) is complete the moment its one required line arrives —
// proving this aggregate imposes no minimum-line-count assumption.
func TestSingleLineOrder_CompletesOnFirstArrival(t *testing.T) {
	oc := consolidation.New("order-1", []string{"line-1"})
	if err := oc.RecordArrival("line-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !oc.IsComplete() {
		t.Fatal("expected a single-line order to complete on its only line's arrival")
	}
}
