package task

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// PickUnit is the unit a PICK task asks the operator to pick (ADR 0054): the
// loose EACH, or the sealed CASE. The planned quantity is always in eaches
// (the ledger unit); the unit only says which GTIN level the operator scans.
type PickUnit string

const (
	// PickUnitEach: the operator picks loose eaches and scans each GTINs.
	PickUnitEach PickUnit = "EACH"
	// PickUnitCase: the operator picks whole cases and scans case GTINs.
	PickUnitCase PickUnit = "CASE"
)

// PackLevel is the packaging level a GTIN identifies (product-master's
// PackLevel enum). Picking implements only EACH and CASE in v1.
type PackLevel string

const (
	PackLevelEach   PackLevel = "EACH"
	PackLevelInner  PackLevel = "INNER"
	PackLevelCase   PackLevel = "CASE"
	PackLevelPallet PackLevel = "PALLET"
)

var (
	// ErrInvalidPickPlan: the released pick plan is incomplete or inconsistent.
	ErrInvalidPickPlan = errors.New("task: invalid pick plan")
	// ErrUnknownPickUnit: a pick unit other than EACH, CASE.
	ErrUnknownPickUnit = errors.New("task: unknown pick unit")
	// ErrNoPickPlan: a pick scan on a task that carries no pick plan.
	ErrNoPickPlan = errors.New("task: the task carries no pick plan to scan against")
	// ErrScanSkuMismatch: the scanned GTIN belongs to another SKU than the
	// pick's (this is the guard against the each-EAN printed on a case of
	// another product).
	ErrScanSkuMismatch = errors.New("task: the scanned GTIN is not the SKU of this pick")
	// ErrScanLevelMismatch: the scanned GTIN's pack level is not the pick unit
	// (a case GTIN on an EACH pick, an each GTIN on a CASE pick, or a level
	// picking does not implement).
	ErrScanLevelMismatch = errors.New("task: the scanned pack level does not match the pick unit")
	// ErrInvalidScanCount: the scan count is below 1.
	ErrInvalidScanCount = errors.New("task: scan count must be at least 1")
	// ErrPickOverQuantity: the scan would take the picked eaches over the plan.
	ErrPickOverQuantity = errors.New("task: the scan would pick more than the planned quantity")
	// ErrPickIncomplete: completion of a PICK task whose scans do not add up to
	// the planned quantity (short picks are not modelled, ADR 0040).
	ErrPickIncomplete = errors.New("task: the scanned eaches do not add up to the planned quantity")
)

// PickPlan is what a PICK task asks for, stamped at release time
// (WorkReleased.data.pick): the SKU, the quantity in EACHES, the unit the
// operator scans and, optionally, the pick-face bin the stock is taken from.
type PickPlan struct {
	SKU      string
	Quantity int
	Unit     PickUnit
	FromBin  string
}

// NewPickPlan validates and builds a plan. Every string is trimmed; fromBin
// is optional.
func NewPickPlan(sku string, quantity int, unit, fromBin string) (PickPlan, error) {
	u := PickUnit(strings.TrimSpace(unit))
	if u != PickUnitEach && u != PickUnitCase {
		return PickPlan{}, fmt.Errorf("%w: %q", ErrUnknownPickUnit, unit)
	}
	p := PickPlan{SKU: strings.TrimSpace(sku), Quantity: quantity, Unit: u, FromBin: strings.TrimSpace(fromBin)}
	if p.SKU == "" {
		return PickPlan{}, fmt.Errorf("%w: sku is required", ErrInvalidPickPlan)
	}
	if p.Quantity < 1 {
		return PickPlan{}, fmt.Errorf("%w: quantity must be at least 1, got %d", ErrInvalidPickPlan, quantity)
	}
	return p, nil
}

// ScannedPack is a scanned GTIN resolved against the local PackView: the SKU
// and level it identifies and its direct factor to eaches. The domain never
// parses or looks up a GTIN; the use case hands the resolution in.
type ScannedPack struct {
	GTIN         string
	SKU          string
	Level        PackLevel
	UnitsPerPack int
}

// PickPlan returns the plan of a PICK task, or nil (a task released without
// one, or any other task type). The pointer is the task's own: do not mutate.
func (t *Task) PickPlan() *PickPlan { return t.pickPlan }

// PickedEaches returns the eaches confirmed by scans so far.
func (t *Task) PickedEaches() int { return t.pickedEaches }

// WithPickPlan stamps the plan and the eaches confirmed so far and returns
// the task (creation, and repository rehydration). The plan is copied. A nil
// plan clears both.
func (t *Task) WithPickPlan(plan *PickPlan, pickedEaches int) *Task {
	t.pickPlan, t.pickedEaches = nil, 0
	if plan != nil {
		cp := *plan
		t.pickPlan, t.pickedEaches = &cp, pickedEaches
	}
	return t
}

// ConfirmPickScan confirms one scan of a PICK task against its plan (ADR
// 0054) and returns the eaches it added. The claim rules are those of
// Complete (an active lease owned by stationId); the scanned pack must be the
// plan's SKU at the plan's unit: an EACH pick takes count eaches, a CASE pick
// takes count x UnitsPerPack. A scan that would exceed the planned quantity is
// refused. A refused scan changes nothing.
func (t *Task) ConfirmPickScan(stationId shared.StationId, now time.Time, pack ScannedPack, count int) (int, error) {
	if t.status == Completed {
		return 0, ErrAlreadyCompleted
	}
	if t.taskType != Pick || t.pickPlan == nil {
		return 0, ErrNoPickPlan
	}
	if err := t.VerifyHeldBy(stationId, now); err != nil {
		return 0, err
	}
	if count < 1 {
		return 0, fmt.Errorf("%w, got %d", ErrInvalidScanCount, count)
	}
	if pack.SKU != t.pickPlan.SKU {
		return 0, fmt.Errorf("%w: GTIN %s is %q, the pick is %q", ErrScanSkuMismatch, pack.GTIN, pack.SKU, t.pickPlan.SKU)
	}
	if string(pack.Level) != string(t.pickPlan.Unit) {
		return 0, fmt.Errorf("%w: GTIN %s is a %s, the pick unit is %s", ErrScanLevelMismatch, pack.GTIN, pack.Level, t.pickPlan.Unit)
	}
	eaches := count
	if t.pickPlan.Unit == PickUnitCase {
		eaches = count * pack.UnitsPerPack
	}
	if t.pickedEaches+eaches > t.pickPlan.Quantity {
		return 0, fmt.Errorf("%w: %d picked + %d scanned > %d planned", ErrPickOverQuantity, t.pickedEaches, eaches, t.pickPlan.Quantity)
	}
	t.pickedEaches += eaches
	return eaches, nil
}
