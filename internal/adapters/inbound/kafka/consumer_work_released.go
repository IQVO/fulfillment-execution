package kafka

import (
	"fmt"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
)

// TypeWorkReleased is the FULL CloudEvents `type` wes-work-planning
// publishes WorkReleased under (fleet cross-service contract, ADR-0032).
// This consumer dispatches on the exact string — never a short name or a
// suffix match.
const TypeWorkReleased = "com.warehouse.wes.work-planning.workunit.WorkReleased"

// workReleased is one decoded WorkReleased occurrence: the CloudEvents
// `id` (this consumer's idempotency key) plus the payload read via DataAs.
type workReleased struct {
	EventId string
	Data    WorkReleasedData
}

// decodeWorkReleased decodes raw as a CloudEvents 1.0 structured-mode
// event. ok is false (and err nil) for a valid event of any other type —
// unknown types are ignored for forward compatibility. A message that is
// not a valid CloudEvents 1.0 event (including the retired flat envelope)
// returns an error wrapping cloudevents.ErrNotCloudEvent: a deterministic
// poison message that Run dead-letters, never parsed any other way.
func decodeWorkReleased(raw []byte) (workReleased, bool, error) {
	e, err := cloudevents.Decode(raw)
	if err != nil {
		return workReleased{}, false, fmt.Errorf("kafka: %w", err)
	}
	if e.Type() != TypeWorkReleased {
		return workReleased{}, false, nil
	}
	var data WorkReleasedData
	if err := e.DataAs(&data); err != nil {
		return workReleased{}, false, fmt.Errorf("kafka: decode %s data: %w", TypeWorkReleased, err)
	}
	return workReleased{EventId: e.ID(), Data: data}, true, nil
}

// WorkReleasedData is the payload of a WorkReleased event.
type WorkReleasedData struct {
	PathId     string    `json:"path_id"`
	WorkUnitId string    `json:"work_unit_id"`
	CPT        time.Time `json:"cpt"`
	Ref        string    `json:"ref"`
	// LineNo is the optional 1-based order line this release is for
	// (wes-work-planning adds it for order-originated work; decision 18,
	// per-line confirm-pick, ADR 0041). Absent (0) from any producer that
	// predates it; the consumer then leaves the task's line unknown and
	// never guesses it from the "<order>-line-<n>" work unit id.
	LineNo lineNo `json:"line_no,omitempty"`
	// Fragile is an optional packing hint set by wes-work-planning at
	// release time, sourced from inventory-storage's ProductClassification
	// (true if the upstream order line was classified Fragile). It is
	// omitted, not required: any already-documented producer that predates
	// this field simply does not send it, and it defaults to false — a
	// known simplification for this round, matching the existing path_id
	// prefix convention (see README's Integration section).
	Fragile bool `json:"fragile"`
	// GiftWrap is an optional packing hint set by wes-work-planning at
	// work-enqueue time — a caller-stated request that this order's
	// package be gift-wrapped, not a product classification. It is
	// omitted, not required, and never published as explicit false: any
	// producer that does not carry a gift-wrap request for the order
	// simply omits the field, and it defaults to false (see ADR-0011).
	GiftWrap bool `json:"gift_wrap"`
	// TransferRef, when present, marks this release as inter-warehouse-
	// transfer work (network-inventory-planning's transfer saga). The
	// transfer fields below are a strictly additive, all-optional
	// correlation block: a legacy payload that omits every one of them
	// decodes exactly as before and creates a non-transfer task. Absent
	// means absent — none of them is ever published as an explicit empty
	// string or zero. demand_id on the resulting transfer fact is the
	// release's own ref.
	TransferRef string `json:"transfer_ref,omitempty"`
	// WorkKind is the kind of transfer work (TRANSFER_PICK |
	// TRANSFER_DISPATCH | TRANSFER_ARRIVAL); it selects which transfer
	// fact the created task's completion publishes.
	WorkKind string `json:"work_kind,omitempty"`
	// SiteId is the site the transfer fact is about (origin for
	// pick/dispatch work, destination for arrival work — the producer
	// decides).
	SiteId string `json:"site_id,omitempty"`
	// SKU is the stock the transfer moves.
	SKU string `json:"sku,omitempty"`
	// Quantity is the number of units of SKU the transfer moves.
	Quantity int `json:"quantity,omitempty"`
	// Relocation, when present, is the slot-plan move of work released on
	// the RELOCATE path (ADR 0047): wes-work-planning plans one RELOCATE
	// work unit per move of an approved slot plan. Absent on every other
	// release. A nested object, not top-level fields, so it can not collide
	// with the transfer block's sku and its presence is one unambiguous
	// marker.
	Relocation *WorkReleasedRelocation `json:"relocation,omitempty"`
	// Replenishment, when present, is the pick-face replenishment move of
	// work released on the REPLENISH path (ADR 0050): wes-work-planning
	// plans one REPLENISH work unit per pick-face-replenishment request.
	// Absent on every other release. A nested object next to relocation,
	// so its presence is one unambiguous marker.
	Replenishment *WorkReleasedReplenishment `json:"replenishment,omitempty"`
	// Pick, when present, is the pick plan of work released on the PICK
	// path (ADR 0054): the SKU, the planned quantity in EACHES and the unit
	// (EACH | CASE) the operator scans. Absent on every other release and on
	// a PICK released without scan confirmation.
	Pick *WorkReleasedPick `json:"pick,omitempty"`
	// CaseBreak, when present, is the sealed-case break of work released on
	// the BREAK_CASE path (ADR 0054). Absent on every other release.
	CaseBreak *WorkReleasedCaseBreak `json:"case_break,omitempty"`
}

// WorkReleasedPick is WorkReleased.data.pick (ADR 0054). quantity is in
// eaches (the ledger unit) whatever the unit; from_bin is optional.
type WorkReleasedPick struct {
	SKU      string `json:"sku"`
	Quantity *int   `json:"quantity"`
	Unit     string `json:"unit"`
	FromBin  string `json:"from_bin,omitempty"`
}

// WorkReleasedCaseBreak is WorkReleased.data.case_break (ADR 0054): break
// the sealed case lpn of sku into loose_bin for the EACH pick pick_task_id.
type WorkReleasedCaseBreak struct {
	LPN        string `json:"lpn"`
	SKU        string `json:"sku"`
	LooseBin   string `json:"loose_bin"`
	PickTaskId string `json:"pick_task_id"`
}

// WorkReleasedReplenishment is WorkReleased.data.replenishment (ADR 0050):
// one replenishment request of pick-face-replenishment, as planned by
// wes-work-planning. Every member is required; quantity is a pointer so an
// absent value is told apart from 0 and rejected. priority is NORMAL or
// URGENT.
type WorkReleasedReplenishment struct {
	RequestId string `json:"request_id"`
	SKU       string `json:"sku"`
	FromBin   string `json:"from_bin"`
	ToBin     string `json:"to_bin"`
	Quantity  *int   `json:"quantity"`
	Priority  string `json:"priority"`
}

// WorkReleasedRelocation is WorkReleased.data.relocation (ADR 0047): one
// move of slotting-optimization's SlotPlanApproved, as planned by
// wes-work-planning. kind is the SlotPlanApproved move kind (Relocate |
// Assign | Vacate); from_bin is omitted for an Assign and to_bin for a
// Vacate (the operator scans the open side at completion). move_seq is a
// pointer so an absent value is told apart from move 0 and rejected.
type WorkReleasedRelocation struct {
	PlanId  string `json:"plan_id"`
	MoveSeq *int   `json:"move_seq"`
	Kind    string `json:"kind"`
	SKU     string `json:"sku"`
	FromBin string `json:"from_bin,omitempty"`
	ToBin   string `json:"to_bin,omitempty"`
}
