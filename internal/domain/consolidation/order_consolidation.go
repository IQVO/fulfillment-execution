// Package consolidation implements OrderConsolidation: a small,
// execution-scoped aggregate that tracks fan-in of an order's required
// lines arriving at Rebin, so a Pack task is only created once every line
// has arrived. It has no visibility outside fulfillment-execution and
// holds no reference to Task — lines are identified by string id only.
//
// This is deliberately NOT a new bounded context: Rebin is a queue at the
// same cadence (seconds, per-task) as Pick/Pack/SLAM, which this context
// already owns as task.Type. The fan-in problem this package solves was
// previously an acknowledged gap in this service's own documentation (see
// business-context/process-paths.md's "OrderConvergence... belongs
// upstream" note) — this package is that fan-in tracker, scoped to
// exactly the lifetime of one order's Rebin-arrival set.
package consolidation

import "errors"

// ErrUnknownLine is returned when RecordArrival is called with a line id
// that was not part of this order's required-lines set at construction.
var ErrUnknownLine = errors.New("consolidation: line is not part of this order")

// OrderConsolidation tracks which of an order's required lines have
// arrived at Rebin. IsComplete reports whether every required line has
// arrived, which is the trigger for creating a PACK task for the order.
type OrderConsolidation struct {
	orderRef      string
	requiredLines map[string]struct{}
	arrivedLines  map[string]struct{}
}

// New constructs consolidation tracking for an order given its required
// line ids. requiredLineIds is typically the set of an order's PICK task
// ids (or another stable per-line identifier) that must all reach Rebin
// before the order can be packed.
func New(orderRef string, requiredLineIds []string) *OrderConsolidation {
	required := make(map[string]struct{}, len(requiredLineIds))
	for _, id := range requiredLineIds {
		required[id] = struct{}{}
	}
	return &OrderConsolidation{
		orderRef:      orderRef,
		requiredLines: required,
		arrivedLines:  make(map[string]struct{}),
	}
}

// Rehydrate reconstructs an OrderConsolidation from persisted state
// without re-validating construction invariants. Only outbound repository
// adapters call this.
func Rehydrate(orderRef string, requiredLineIds []string, arrivedLineIds []string) *OrderConsolidation {
	oc := New(orderRef, requiredLineIds)
	for _, id := range arrivedLineIds {
		oc.arrivedLines[id] = struct{}{}
	}
	return oc
}

// OrderRef identifies the order this consolidation tracker belongs to.
func (o *OrderConsolidation) OrderRef() string { return o.orderRef }

// RequiredLineIds returns the full set of line ids required for
// completion, in no particular order.
func (o *OrderConsolidation) RequiredLineIds() []string {
	out := make([]string, 0, len(o.requiredLines))
	for id := range o.requiredLines {
		out = append(out, id)
	}
	return out
}

// ArrivedLineIds returns the line ids that have arrived so far, in no
// particular order.
func (o *OrderConsolidation) ArrivedLineIds() []string {
	out := make([]string, 0, len(o.arrivedLines))
	for id := range o.arrivedLines {
		out = append(out, id)
	}
	return out
}

// RecordArrival marks one required line as having reached Rebin.
// Idempotent on redelivery: recording an already-arrived line a second
// time is a no-op success, matching WorkPool.Complete's redelivered-event
// tolerance elsewhere in this fleet (a redelivered ItemArrivedAtRebin
// event must not error the consumer). Returns ErrUnknownLine if lineId
// was not part of this order's required set at construction — that is a
// caller/data-integrity error, not a legitimate redelivery.
func (o *OrderConsolidation) RecordArrival(lineId string) error {
	if _, ok := o.requiredLines[lineId]; !ok {
		return ErrUnknownLine
	}
	o.arrivedLines[lineId] = struct{}{}
	return nil
}

// HasArrived reports whether lineId is a required line that has already
// arrived at Rebin. It is how a caller recognises a redelivered arrival
// before doing anything with side effects; a line outside the required
// set is never "arrived" (RecordArrival rejects it with ErrUnknownLine).
func (o *OrderConsolidation) HasArrived(lineId string) bool {
	_, ok := o.arrivedLines[lineId]
	return ok
}

// IsComplete reports whether every required line has arrived at Rebin.
func (o *OrderConsolidation) IsComplete() bool {
	return len(o.arrivedLines) == len(o.requiredLines)
}
