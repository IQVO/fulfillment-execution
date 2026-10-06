// Package pack implements the Package aggregate: pack output, from scanned
// contents through sealing to the SLAM weigh-check (label applied or
// diverted on discrepancy). Named `pack` because `package` is a Go keyword.
package pack

import (
	"errors"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// Status is the lifecycle state of a package.
type Status string

const (
	Open     Status = "OPEN"
	Sealed   Status = "SEALED"
	Labeled  Status = "LABELED"
	Diverted Status = "DIVERTED"
)

// ErrUnknownStatus is returned by ParseStatus for a string that is not one
// of the declared package statuses.
var ErrUnknownStatus = errors.New("package: unknown status")

// ParseStatus validates the persisted string form of a Status. Matching is
// exact (case-sensitive): every writer persists the canonical constant.
func ParseStatus(value string) (Status, error) {
	switch Status(value) {
	case Open, Sealed, Labeled, Diverted:
		return Status(value), nil
	default:
		return "", ErrUnknownStatus
	}
}

// SortLane values are the WES-tier sortation routing decision Package
// derives via SortLane() — see ADR-0010. This is a decision only: no WCS
// device/conveyor integration exists or is planned in this repository. A
// real WCS system would consume SortLane as its own inbound fact.
const (
	SortLaneHazmat        = "HAZMAT_LANE"
	SortLaneFragileNoTilt = "FRAGILE_NO_TILT"
	SortLaneStandard      = "STANDARD"
)

var (
	// ErrNoScannedContents is returned when sealing is attempted with no
	// scanned contents (cannot seal without scanned contents).
	ErrNoScannedContents = errors.New("package: cannot seal without scanned contents")
	// ErrAlreadySealed is returned when sealing an already-sealed package.
	ErrAlreadySealed = errors.New("package: already sealed")
	// ErrNotSealed is returned when SLAM is attempted before the package is sealed.
	ErrNotSealed = errors.New("package: must be sealed before SLAM")
	// ErrAlreadyProcessed is returned when SLAM is attempted on a package
	// that already has a label applied or was diverted.
	ErrAlreadyProcessed = errors.New("package: SLAM already processed")
	// ErrPackageSegregationViolation is returned when a scanned item's DOT
	// hazard class is incompatible — per the class-level segregation
	// matrix in segregation.go — with an already-scanned item's hazard
	// class in the same package. An item with no hazard class (Known
	// false, or Known true but not Hazmat) never triggers or blocks this
	// check: fail-open, consistent with every other classification
	// consumer in this system (see ADR-0010).
	ErrPackageSegregationViolation = errors.New("package: item's DOT hazard class is incompatible with an already-scanned item")
)

// WeightTolerance is the maximum allowed |actual-expected| deviation, in the
// same unit as the weights passed to Weigh (e.g. kilograms).
const WeightTolerance = 0.05

// Package is the aggregate root for pack output: an order becoming a sealed carton.
type Package struct {
	id                   shared.PackageId
	orderRef             shared.OrderRef
	taskId               shared.TaskId
	status               Status
	scannedContents      []string
	fragileHandling      bool
	giftWrapRequested    bool
	scannedHazardClasses []int
}

// New creates an empty, open package for the given order. taskId is the
// owning Pack task's id (see SealPackage), used only as SealPackage's
// dedupe key — it carries no other domain meaning. fragileHandling is
// derived from the owning Pack task's Fragile flag (itself stamped by
// wes-work-planning from inventory-storage's ProductClassification) — true
// if any of the package's scanned/sealed contents came from an order line
// classified Fragile, so downstream packing/sortation can route it
// accordingly. giftWrapRequested is derived the same way from the owning
// task's GiftWrap flag (itself stamped by wes-work-planning from an
// explicit gift-wrap request made at work-enqueue time, not a product
// classification) — see ADR-0011. Both flags are independently derived and
// kept separate; neither is merged into the other.
func New(id shared.PackageId, orderRef shared.OrderRef, taskId shared.TaskId, fragileHandling bool, giftWrapRequested bool) *Package {
	return &Package{id: id, orderRef: orderRef, taskId: taskId, status: Open, fragileHandling: fragileHandling, giftWrapRequested: giftWrapRequested}
}

// Rehydrate reconstructs a Package from persisted state. scannedHazardClasses
// carries the DOT hazard class (1-9) recorded per already-scanned item at
// the same index into scannedContents that had a known Hazmat classification
// at scan time — see ScanItemWithClass. A nil/empty slice is valid: it means
// no scanned item in this package ever carried a hazard class (the common
// case, and the only case for every package sealed before this feature).
// taskId is empty ("") for any package sealed before this field was added
// (migration backfills nothing — see the postgres adapter's Save comment);
// SealPackage's idempotency guard treats an empty taskId as "not
// dedupe-able", never as a false match against a later call's real taskId.
func Rehydrate(id shared.PackageId, orderRef shared.OrderRef, taskId shared.TaskId, status Status, scannedContents []string, fragileHandling bool, scannedHazardClasses []int, giftWrapRequested bool) *Package {
	return &Package{
		id:                   id,
		orderRef:             orderRef,
		taskId:               taskId,
		status:               status,
		scannedContents:      scannedContents,
		fragileHandling:      fragileHandling,
		scannedHazardClasses: scannedHazardClasses,
		giftWrapRequested:    giftWrapRequested,
	}
}

func (p *Package) Id() shared.PackageId      { return p.id }
func (p *Package) OrderRef() shared.OrderRef { return p.orderRef }

// TaskId is the id of the Pack task this package was sealed for, or "" for
// a package sealed before this field was added. See SealPackage's doc
// comment for why this is the dedupe key for POST /tasks/{id}/seal-package.
func (p *Package) TaskId() shared.TaskId     { return p.taskId }
func (p *Package) Status() Status            { return p.status }
func (p *Package) ScannedContents() []string { return p.scannedContents }

// ScannedHazardClasses returns the DOT hazard class recorded for each
// already-scanned item that carried a known Hazmat classification at scan
// time — see ScanItemWithClass. The returned slice is independent of
// ScannedContents' indexing (an unclassified item never appends here), so
// callers needing per-SKU hazard class must correlate via ScanItemWithClass
// call order themselves; this accessor exists for persistence round-trip
// and for SortLane, neither of which needs that correlation.
func (p *Package) ScannedHazardClasses() []int { return p.scannedHazardClasses }

// FragileHandling reports whether this package requires fragile packing
// care, derived at construction time from the owning task's Fragile flag.
func (p *Package) FragileHandling() bool { return p.fragileHandling }

// GiftWrapRequested reports whether this package should be gift-wrapped,
// derived at construction time from the owning task's GiftWrap flag. Unlike
// hazmat, this is not a station-eligibility/capability concern — any
// station may fulfill a gift-wrap request; it is a packing concern only,
// same category as FragileHandling (see ADR-0011).
func (p *Package) GiftWrapRequested() bool { return p.giftWrapRequested }

// ScanItem records a scanned item as part of the package's contents, with no
// DOT hazard class information (equivalent to ScanItemWithClass(sku, 0)).
// Existing callers that never look up classification keep working exactly
// as before this feature — permissive by construction.
func (p *Package) ScanItem(sku string) error {
	return p.ScanItemWithClass(sku, 0)
}

// ScanItemWithClass records a scanned item as part of the package's
// contents, additionally checking hazardClass (a DOT hazard class 1-9, or 0
// meaning "no hazard class" — unclassified, or classified but not Hazmat)
// against every already-scanned item's hazard class via the segregation
// matrix (segregation.go) before appending. It rejects with
// ErrPackageSegregationViolation on the first incompatible pair found,
// leaving the package's scanned contents unchanged (the item is not
// appended). hazardClass == 0 never triggers or blocks segregation and is
// never itself recorded into ScannedHazardClasses — fail-open for
// unclassified/non-hazmat items, matching every other classification
// consumer in this system (see ADR-0010).
func (p *Package) ScanItemWithClass(sku string, hazardClass int) error {
	if p.status != Open {
		return ErrAlreadySealed
	}
	if hazardClass != 0 {
		for _, existing := range p.scannedHazardClasses {
			if IsSegregationIncompatible(hazardClass, existing) {
				return ErrPackageSegregationViolation
			}
		}
	}
	p.scannedContents = append(p.scannedContents, sku)
	if hazardClass != 0 {
		p.scannedHazardClasses = append(p.scannedHazardClasses, hazardClass)
	}
	return nil
}

// SortLane derives the WES-tier sortation routing decision for this
// package, in this priority order (see ADR-0010):
//
//  1. HAZMAT_LANE — if any scanned item carried a DOT hazard class (i.e.
//     ScannedHazardClasses is non-empty).
//  2. FRAGILE_NO_TILT — else if FragileHandling() is true (ADR-0009's
//     existing, already-shipped derived flag).
//  3. STANDARD — otherwise.
//
// Hazmat beats fragile deliberately: a package containing regulated
// hazardous material has a real physical/regulatory routing requirement
// that supersedes a packing-care hint. Computed lazily on every call
// rather than stored, so it can never drift from ScannedHazardClasses/
// FragileHandling and needs no migration when either input changes shape —
// see ADR-0010 for the tradeoff (a stored field would save a slice length
// check, an unmeasurable cost at this scale).
func (p *Package) SortLane() string {
	if len(p.scannedHazardClasses) > 0 {
		return SortLaneHazmat
	}
	if p.fragileHandling {
		return SortLaneFragileNoTilt
	}
	return SortLaneStandard
}

// Seal closes the package. Cannot seal without scanned contents.
func (p *Package) Seal() error {
	if p.status != Open {
		return ErrAlreadySealed
	}
	if len(p.scannedContents) == 0 {
		return ErrNoScannedContents
	}
	p.status = Sealed
	return nil
}

// Weigh runs the SLAM weigh-check: if the actual weight is within
// WeightTolerance of the expected weight the label is applied, otherwise the
// package is diverted. Returns true if the label was applied.
func (p *Package) Weigh(expectedWeight, actualWeight float64) (labelApplied bool, err error) {
	if p.status == Open {
		return false, ErrNotSealed
	}
	if p.status != Sealed {
		return false, ErrAlreadyProcessed
	}
	deviation := expectedWeight - actualWeight
	if deviation < 0 {
		deviation = -deviation
	}
	if deviation > WeightTolerance {
		p.status = Diverted
		return false, nil
	}
	p.status = Labeled
	return true, nil
}
