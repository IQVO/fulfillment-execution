// Package task implements the Task aggregate: the unit of physical work
// (Pick, Pack, or SLAM) that moves through Pending -> Claimed(leased) ->
// Completed, or has its lease expire back to Pending.
package task

import (
	"errors"
	"math"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// Type is the process path a task belongs to.
type Type string

const (
	Pick     Type = "PICK"
	Pack     Type = "PACK"
	Slam     Type = "SLAM"
	Rebin    Type = "REBIN"
	Dispatch Type = "DISPATCH"
	Arrival  Type = "ARRIVAL"
)

// Status is the lifecycle state of a task.
type Status string

const (
	Pending   Status = "PENDING"
	Claimed   Status = "CLAIMED"
	Completed Status = "COMPLETED"
)

// ErrUnknownType is returned by ParseType for a string that is not one of
// the declared task types.
var ErrUnknownType = errors.New("task: unknown type")

// ErrUnknownStatus is returned by ParseStatus for a string that is not one
// of the declared task statuses.
var ErrUnknownStatus = errors.New("task: unknown status")

// ErrUnknownWorkKind is returned by ParseWorkKind for a string that is not
// one of the declared transfer work kinds.
var ErrUnknownWorkKind = errors.New("task: unknown transfer work kind")

// WorkKind is the kind of inter-warehouse-transfer work a task carries,
// stamped by wes-work-planning at release time (work_kind on WorkReleased's
// optional transfer correlation block). It selects which transfer fact a
// completion publishes — see the transfer-task-facts ADR.
type WorkKind string

const (
	// WorkKindTransferPick is transfer work whose completion reports that
	// the transfer's stock was picked at the origin site.
	WorkKindTransferPick WorkKind = "TRANSFER_PICK"
	// WorkKindTransferDispatch is transfer work whose completion reports
	// that the transfer left the origin site.
	WorkKindTransferDispatch WorkKind = "TRANSFER_DISPATCH"
	// WorkKindTransferArrival is transfer work whose completion reports
	// that the transfer arrived at the destination site.
	WorkKindTransferArrival WorkKind = "TRANSFER_ARRIVAL"
)

// ParseWorkKind validates the wire/persisted string form of a WorkKind.
// Matching is exact (case-sensitive), like ParseType: every writer
// persists the canonical constant.
func ParseWorkKind(value string) (WorkKind, error) {
	switch WorkKind(value) {
	case WorkKindTransferPick, WorkKindTransferDispatch, WorkKindTransferArrival:
		return WorkKind(value), nil
	default:
		return "", ErrUnknownWorkKind
	}
}

// TransferDetails is the optional inter-warehouse-transfer correlation a
// Task carries when it was released for a network-inventory-planning
// transfer rather than a customer order. A nil *TransferDetails means the
// task is not transfer work: it publishes no transfer fact on completion
// and every field here is meaningless for it.
//
// TransferRef is the transfer's own correlation id (network-inventory-
// planning's TransferAllocationRequested transfer_id); DemandId is the
// work-demand reference the release carried (WorkReleased's ref, or an
// explicit demand_id when the producer sends one); SiteId is the site the
// fact is about (origin for pick/dispatch work, destination for arrival
// work — the producer decides); SKU and Quantity are the stock the task
// moves. WorkKind selects the completion fact (TransferPicked /
// TransferDispatched / TransferArrived).
//
// These fields never affect claiming, capability matching or lease
// behaviour — they are pure correlation payload, carried on the task so
// the completion path can publish the fact without a lookup back into
// another context.
type TransferDetails struct {
	TransferRef string
	DemandId    string
	WorkKind    WorkKind
	SiteId      string
	SKU         string
	Quantity    int
}

// ParseType validates the persisted string form of a Type. Matching is
// exact (case-sensitive): every writer persists the canonical constant.
func ParseType(value string) (Type, error) {
	switch Type(value) {
	case Pick, Pack, Slam, Rebin, Dispatch, Arrival:
		return Type(value), nil
	default:
		return "", ErrUnknownType
	}
}

// ParseStatus validates the persisted string form of a Status.
func ParseStatus(value string) (Status, error) {
	switch Status(value) {
	case Pending, Claimed, Completed:
		return Status(value), nil
	default:
		return "", ErrUnknownStatus
	}
}

var (
	// ErrCapabilityMismatch is returned when a station lacks the
	// capabilities required by the task it is trying to claim.
	ErrCapabilityMismatch = errors.New("task: station capabilities do not match required capabilities")
	// ErrAlreadyClaimed is returned when a claim is attempted on a task
	// that already has an active (unexpired) claim — enforces at-most-once.
	ErrAlreadyClaimed = errors.New("task: already claimed")
	// ErrNotClaimed is returned when an operation requiring an active claim
	// (renew, complete) is attempted on a task with no active claim.
	ErrNotClaimed = errors.New("task: not claimed")
	// ErrNotOwner is returned when a station attempts to renew or complete
	// a claim it does not own.
	ErrNotOwner = errors.New("task: station does not own the claim")
	// ErrAlreadyCompleted is returned on any attempt to act on a completed task.
	ErrAlreadyCompleted = errors.New("task: already completed")
)

// Lease represents a time-boxed claim on a task by a station. If not
// confirmed/completed before Expiry, the task returns to Pending.
type Lease struct {
	StationId shared.StationId
	Expiry    time.Time
}

func (l Lease) expired(now time.Time) bool {
	return !now.Before(l.Expiry)
}

// Task is the aggregate root for a unit of Pick, Pack, or SLAM work.
type Task struct {
	id                   shared.TaskId
	taskType             Type
	status               Status
	cpt                  shared.CPT
	orderRef             shared.OrderRef
	requiredCapabilities shared.CapabilitySet
	lease                *Lease
	fragile              bool
	giftWrap             bool
	claimedAt            *time.Time
	// transfer, when non-nil, is the inter-warehouse-transfer correlation
	// this task carries (see TransferDetails). Nil for every non-transfer
	// task — including all tasks created before this field existed.
	transfer *TransferDetails
	// sourceOrderId is the upstream ORDER this task's work was released for
	// (WorkReleased.ref on order-originated work). It is NOT orderRef: for
	// order work orderRef is the per-line work unit id "<order>-line-<n>".
	// Empty for transfer work (its ref is a demand id), for work released
	// without a ref, for tasks created via REST/MCP, and for every task
	// created before this field existed. Published as TaskCompleted.order_ref
	// (audit decision 17, ADR 0040).
	sourceOrderId string
	// sourceLineNo is the 1-based order line this task's work was released
	// for (WorkReleased.line_no on order-originated work; per-line
	// confirm-pick, decision 18, ADR 0041). 0 means unknown: transfer work,
	// a release without line_no, REST/MCP-created tasks, and every task
	// created before this field existed. Never parsed from the
	// "<order>-line-<n>" work unit id. Published as TaskCompleted.line_no.
	sourceLineNo int
}

// New creates a task in the Pending state, ready for the pool. fragile is a
// packing hint stamped by wes-work-planning at release time, sourced from
// inventory-storage's ProductClassification: true if the upstream order
// line was classified Fragile. It does not affect claiming or capability
// matching — a Pack station later derives Package.FragileHandling from it
// (see SealPackage). giftWrap is the same shape of packing hint, stamped by
// wes-work-planning from an explicit gift-wrap request made at
// work-enqueue time (not a product classification) — see ADR-0011. It
// likewise does not affect claiming or capability matching — a Pack
// station later derives Package.GiftWrapRequested from it.
func New(id shared.TaskId, taskType Type, cpt shared.CPT, orderRef shared.OrderRef, required shared.CapabilitySet, fragile bool, giftWrap bool) *Task {
	return &Task{
		id:                   id,
		taskType:             taskType,
		status:               Pending,
		cpt:                  cpt,
		orderRef:             orderRef,
		requiredCapabilities: required,
		fragile:              fragile,
		giftWrap:             giftWrap,
	}
}

// NewTransferTask creates a task in the Pending state carrying an
// inter-warehouse-transfer correlation block. Same construction
// invariants as New; transfer is carried verbatim (the consumer has
// already validated its WorkKind — see ParseWorkKind).
func NewTransferTask(id shared.TaskId, taskType Type, cpt shared.CPT, orderRef shared.OrderRef, required shared.CapabilitySet, fragile bool, giftWrap bool, transfer TransferDetails) *Task {
	t := New(id, taskType, cpt, orderRef, required, fragile, giftWrap)
	cp := transfer
	t.transfer = &cp
	return t
}

// Rehydrate reconstructs a Task from persisted state without re-validating
// construction invariants (used by repository adapters). claimedAt restores
// the timestamp of the task's current (or most recent) claim, recorded by
// Claim — nil for tasks persisted before this field existed, or that have
// never been claimed.
func Rehydrate(id shared.TaskId, taskType Type, status Status, cpt shared.CPT, orderRef shared.OrderRef, required shared.CapabilitySet, lease *Lease, fragile bool, giftWrap bool, claimedAt *time.Time) *Task {
	return &Task{
		id:                   id,
		taskType:             taskType,
		status:               status,
		cpt:                  cpt,
		orderRef:             orderRef,
		requiredCapabilities: required,
		lease:                lease,
		fragile:              fragile,
		giftWrap:             giftWrap,
		claimedAt:            claimedAt,
	}
}

// RehydrateTransfer is Rehydrate for a task that carries an
// inter-warehouse-transfer correlation block. transfer may be nil, which
// makes this exactly Rehydrate — repos use it uniformly so one read path
// serves both shapes.
func RehydrateTransfer(id shared.TaskId, taskType Type, status Status, cpt shared.CPT, orderRef shared.OrderRef, required shared.CapabilitySet, lease *Lease, fragile bool, giftWrap bool, claimedAt *time.Time, transfer *TransferDetails) *Task {
	t := Rehydrate(id, taskType, status, cpt, orderRef, required, lease, fragile, giftWrap, claimedAt)
	if transfer != nil {
		cp := *transfer
		t.transfer = &cp
	}
	return t
}

func (t *Task) Id() shared.TaskId                          { return t.id }
func (t *Task) Type() Type                                 { return t.taskType }
func (t *Task) Status() Status                             { return t.status }
func (t *Task) CPT() shared.CPT                            { return t.cpt }
func (t *Task) OrderRef() shared.OrderRef                  { return t.orderRef }
func (t *Task) RequiredCapabilities() shared.CapabilitySet { return t.requiredCapabilities }
func (t *Task) Lease() *Lease                              { return t.lease }

// ClaimedAt returns the time the task's current (or most recently active)
// claim started, as recorded by Claim. It is nil for a task that has never
// been claimed, and — deliberately — is NOT cleared by Complete, so
// duration-since-claim can still be computed at completion time (see
// CompleteTask's Kafka enrichment). A task persisted before this field
// existed also reports nil (see Rehydrate).
func (t *Task) ClaimedAt() *time.Time { return t.claimedAt }

// Fragile reports whether this task's upstream order line was classified
// Fragile by inventory-storage's ProductClassification, as stamped by
// wes-work-planning at release time. It is a packing hint for the Pack
// path (see Package.FragileHandling) — it does not gate claiming.
func (t *Task) Fragile() bool { return t.fragile }

// GiftWrap reports whether this task's order was flagged for gift wrap at
// work-enqueue time, as stamped by wes-work-planning onto WorkReleased's
// optional data.gift_wrap field. Unlike Fragile, this is not sourced from
// inventory-storage's ProductClassification — it is a caller-stated
// characteristic of the released work itself. It is a packing hint for the
// Pack path (see Package.GiftWrapRequested) — it does not gate claiming and,
// deliberately unlike hazmat, is never used for station-eligibility/
// capability matching (see ADR-0011).
func (t *Task) GiftWrap() bool { return t.giftWrap }

// Transfer returns the inter-warehouse-transfer correlation block this
// task carries, or nil when this is not transfer work. The returned
// pointer is the task's own — callers must not mutate it.
func (t *Task) Transfer() *TransferDetails { return t.transfer }

// SourceOrderId returns the upstream order this task's work was released
// for, or "" when unknown (see the sourceOrderId field).
func (t *Task) SourceOrderId() string { return t.sourceOrderId }

// WithSourceOrderId stamps the upstream order id and returns the task, for
// use at creation time and when a repository rehydrates the column. An
// empty id leaves the task without one.
func (t *Task) WithSourceOrderId(orderId string) *Task {
	t.sourceOrderId = orderId
	return t
}

// SourceLineNo returns the 1-based order line this task's work was released
// for, or 0 when unknown (see the sourceLineNo field).
func (t *Task) SourceLineNo() int { return t.sourceLineNo }

// WithSourceLineNo stamps the order line number and returns the task, for
// use at creation time and when a repository rehydrates the column. A
// number outside 1..MaxSourceLineNo leaves the line unknown (0): the column
// is a 32-bit INTEGER, so the domain never holds a value the insert would
// reject.
func (t *Task) WithSourceLineNo(lineNo int) *Task {
	if lineNo < 1 || lineNo > MaxSourceLineNo {
		lineNo = 0
	}
	t.sourceLineNo = lineNo
	return t
}

// MaxSourceLineNo is the highest valid order line number (math.MaxInt32):
// the width of the source_line_no column and of the line_no field in the
// WorkReleased and TaskCompleted contracts.
const MaxSourceLineNo = math.MaxInt32

// IsAvailable reports whether the task can be claimed at `now`: it is
// Pending, or Claimed with an expired lease (which frees it in the caller's
// view without mutating state — callers should call ExpireLeaseIfDue first
// for state that must be persisted).
func (t *Task) IsAvailable(now time.Time) bool {
	if t.status == Pending {
		return true
	}
	if t.status == Claimed && t.lease != nil && t.lease.expired(now) {
		return true
	}
	return false
}

// IsCPTMissed reports whether this task is still open (Pending or Claimed —
// i.e. not Completed) at or after its CPT deadline. Mirrors Lease.expired's
// boundary convention exactly: a task due EXACTLY at now counts as missed
// (not-before, not strictly-after), so a sweep tick that lands precisely on
// the CPT catches it rather than waiting for the next tick. Pure domain
// logic, no I/O — the Clock-driven sweep (SweepCPTMisses) is the caller
// that supplies now and decides what to do about a true result.
func (t *Task) IsCPTMissed(now time.Time) bool {
	if t.status == Completed {
		return false
	}
	return !now.Before(t.cpt.Time())
}

// ExpireLeaseIfDue frees a Claimed task whose lease has passed, returning it
// to Pending. Returns true if it freed the task.
func (t *Task) ExpireLeaseIfDue(now time.Time) bool {
	if t.status != Claimed || t.lease == nil || !t.lease.expired(now) {
		return false
	}
	t.status = Pending
	t.lease = nil
	return true
}

// Claim assigns the task to a station for the given lease duration, enforcing
// at-most-once assignment and capability match.
func (t *Task) Claim(stationId shared.StationId, stationCapabilities shared.CapabilitySet, now time.Time, leaseDuration time.Duration) error {
	if t.status == Completed {
		return ErrAlreadyCompleted
	}
	// An expired lease implicitly frees the task before re-evaluating the claim.
	t.ExpireLeaseIfDue(now)
	if t.status == Claimed {
		return ErrAlreadyClaimed
	}
	if !stationCapabilities.HasAll(t.requiredCapabilities) {
		return ErrCapabilityMismatch
	}
	t.status = Claimed
	t.lease = &Lease{StationId: stationId, Expiry: now.Add(leaseDuration)}
	claimedAt := now
	t.claimedAt = &claimedAt
	return nil
}

// RenewLease extends an active claim's lease. Only the owning station may renew.
func (t *Task) RenewLease(stationId shared.StationId, now time.Time, leaseDuration time.Duration) error {
	if t.status == Completed {
		return ErrAlreadyCompleted
	}
	if t.status != Claimed || t.lease == nil {
		return ErrNotClaimed
	}
	if t.lease.expired(now) {
		t.status = Pending
		t.lease = nil
		return ErrNotClaimed
	}
	if t.lease.StationId != stationId {
		return ErrNotOwner
	}
	t.lease.Expiry = now.Add(leaseDuration)
	return nil
}

// VerifyHeldBy reports whether stationId currently holds an ACTIVE claim on
// this task at `now`: a lease exists, belongs to stationId, and has not
// expired (expiry is inclusive, the same boundary as Complete/RenewLease).
// Failures follow Complete/RenewLease so the same condition yields the same
// error across endpoints (ADR-0038, decision 2026-10-06):
//   - no lease, or an EXPIRED lease (whoever held it) -> ErrNotClaimed;
//   - an active lease held by ANOTHER station -> ErrNotOwner.
//
// It is read-only: unlike Complete/RenewLease it never frees the task, so a
// caller that only needs an ownership guard (SealPackage) does not have to
// persist the task.
func (t *Task) VerifyHeldBy(stationId shared.StationId, now time.Time) error {
	if t.lease == nil || t.lease.expired(now) {
		return ErrNotClaimed
	}
	if t.lease.StationId != stationId {
		return ErrNotOwner
	}
	return nil
}

// Complete finishes the task. Only the owning station may complete it, the
// claim must still be active, and a completed task cannot be completed
// again (no double-complete).
func (t *Task) Complete(stationId shared.StationId, now time.Time) error {
	if t.status == Completed {
		return ErrAlreadyCompleted
	}
	if t.status != Claimed || t.lease == nil {
		return ErrNotClaimed
	}
	if t.lease.expired(now) {
		t.status = Pending
		t.lease = nil
		return ErrNotClaimed
	}
	if t.lease.StationId != stationId {
		return ErrNotOwner
	}
	t.status = Completed
	t.lease = nil
	return nil
}
