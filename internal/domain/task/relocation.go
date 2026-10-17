package task

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Relocate is the task type of slot-plan relocation work (ADR 0047): one
// planned move of a SKU's stock from one bin to another inside a site,
// planned by wes-work-planning from slotting-optimization's SlotPlanApproved
// and released on the RELOCATE process path. It is created only from
// released work, never through POST /tasks, because it needs the move.
const Relocate Type = "RELOCATE"

// RelocationKind is the kind of slot-plan move, exactly the `kind` values of
// slotting-optimization's SlotPlanApproved moves.
type RelocationKind string

const (
	// RelocationKindRelocate moves a SKU from one forward slot to another:
	// both bins are known when the work is released.
	RelocationKindRelocate RelocationKind = "Relocate"
	// RelocationKindAssign brings a SKU into a forward slot: the destination
	// is known, the source (a bin holding the SKU that is not a forward
	// slot) is chosen and scanned by the operator.
	RelocationKindAssign RelocationKind = "Assign"
	// RelocationKindVacate empties a forward slot: the source is known, the
	// destination (a reserve bin) is chosen and scanned by the operator.
	RelocationKindVacate RelocationKind = "Vacate"
)

var (
	// ErrUnknownRelocationKind: a kind other than Relocate, Assign, Vacate.
	ErrUnknownRelocationKind = errors.New("task: unknown relocation kind")
	// ErrInvalidRelocationMove: the released move is incomplete or
	// inconsistent (missing plan id or sku, negative move_seq, a bin its
	// kind requires is missing, or from_bin equals to_bin).
	ErrInvalidRelocationMove = errors.New("task: invalid relocation move")
	// ErrRelocationConfirmationRequired: a RELOCATE task was completed
	// without the operator-confirmed quantity and scanned bins.
	ErrRelocationConfirmationRequired = errors.New("task: a RELOCATE task completes only with the confirmed quantity and the bins the move left open")
	// ErrInvalidRelocationConfirmation: the confirmation is unusable (a
	// quantity below 1, a bin the move left open was not scanned, a
	// scanned bin differs from the planned one, or both bins are equal).
	ErrInvalidRelocationConfirmation = errors.New("task: invalid relocation confirmation")
	// ErrNotRelocationTask: confirmation inputs (quantity, from_bin,
	// to_bin) were given for a task that is neither RELOCATE nor REPLENISH
	// work (ADR 0050 extends the inputs to REPLENISH; the name and the REST
	// slug stay for compatibility).
	ErrNotRelocationTask = errors.New("task: quantity, from_bin and to_bin apply only to RELOCATE and REPLENISH tasks")
)

// ParseRelocationKind validates the wire/persisted form of a RelocationKind
// (exact, case-sensitive match).
func ParseRelocationKind(value string) (RelocationKind, error) {
	k := RelocationKind(value)
	if k == RelocationKindRelocate || k == RelocationKindAssign || k == RelocationKindVacate {
		return k, nil
	}
	return "", fmt.Errorf("%w: %q", ErrUnknownRelocationKind, value)
}

// RelocationMove is the planned move a RELOCATE task executes, stamped at
// release time (WorkReleased.relocation). FromBin is empty for an Assign and
// ToBin is empty for a Vacate: the operator scans the open side.
type RelocationMove struct {
	PlanId  string
	MoveSeq int
	Kind    RelocationKind
	SKU     string
	FromBin string
	ToBin   string
}

// NewRelocationMove validates and builds a move. Every string is trimmed.
// A Relocate needs both bins, an Assign needs to_bin, a Vacate needs
// from_bin; the other side may be present (then it is fixed) or empty
// (then it is scanned at completion). Two known bins must differ.
func NewRelocationMove(planId string, moveSeq int, kind string, sku, fromBin, toBin string) (RelocationMove, error) {
	k, err := ParseRelocationKind(strings.TrimSpace(kind))
	if err != nil {
		return RelocationMove{}, err
	}
	m := RelocationMove{
		PlanId:  strings.TrimSpace(planId),
		MoveSeq: moveSeq,
		Kind:    k,
		SKU:     strings.TrimSpace(sku),
		FromBin: strings.TrimSpace(fromBin),
		ToBin:   strings.TrimSpace(toBin),
	}
	if err := m.validate(); err != nil {
		return RelocationMove{}, err
	}
	return m, nil
}

func (m RelocationMove) validate() error {
	if m.PlanId == "" {
		return fmt.Errorf("%w: plan_id is required", ErrInvalidRelocationMove)
	}
	if m.MoveSeq < 0 {
		return fmt.Errorf("%w: move_seq must be >= 0, got %d", ErrInvalidRelocationMove, m.MoveSeq)
	}
	if m.SKU == "" {
		return fmt.Errorf("%w: sku is required", ErrInvalidRelocationMove)
	}
	if m.needsFromBin() && m.FromBin == "" {
		return fmt.Errorf("%w: a %s move needs from_bin", ErrInvalidRelocationMove, m.Kind)
	}
	if m.needsToBin() && m.ToBin == "" {
		return fmt.Errorf("%w: a %s move needs to_bin", ErrInvalidRelocationMove, m.Kind)
	}
	if m.FromBin != "" && m.FromBin == m.ToBin {
		return fmt.Errorf("%w: from_bin and to_bin are both %q", ErrInvalidRelocationMove, m.FromBin)
	}
	return nil
}

// needsFromBin: Relocate and Vacate know their source at release time.
func (m RelocationMove) needsFromBin() bool { return m.Kind != RelocationKindAssign }

// needsToBin: Relocate and Assign know their destination at release time.
func (m RelocationMove) needsToBin() bool { return m.Kind != RelocationKindVacate }

// Ref is the move's correlation reference, "<plan_id>:<move_seq>",
// published as TaskCompleted.relocation_ref.
func (m RelocationMove) Ref() string {
	return m.PlanId + ":" + strconv.Itoa(m.MoveSeq)
}

// RelocationConfirmation is what the operator reports when completing a
// RELOCATE task: the counted quantity and the bins scanned.
type RelocationConfirmation struct {
	Quantity int
	FromBin  string
	ToBin    string
}

// RelocationResult is the executed move recorded on completion: the
// confirmed quantity and the actual source and destination bins.
type RelocationResult struct {
	Quantity int
	FromBin  string
	ToBin    string
}

// resolve checks a confirmation against the planned move: the quantity is at
// least 1, every open bin is scanned, a scanned bin on a planned side
// matches the plan, and the two bins differ.
func (m RelocationMove) resolve(c RelocationConfirmation) (RelocationResult, error) {
	if c.Quantity < 1 {
		return RelocationResult{}, fmt.Errorf("%w: quantity must be at least 1, got %d", ErrInvalidRelocationConfirmation, c.Quantity)
	}
	from, err := resolveBin("from_bin", m.FromBin, c.FromBin)
	if err != nil {
		return RelocationResult{}, err
	}
	to, err := resolveBin("to_bin", m.ToBin, c.ToBin)
	if err != nil {
		return RelocationResult{}, err
	}
	if from == to {
		return RelocationResult{}, fmt.Errorf("%w: from_bin and to_bin are both %q", ErrInvalidRelocationConfirmation, from)
	}
	return RelocationResult{Quantity: c.Quantity, FromBin: from, ToBin: to}, nil
}

// resolveBin returns the bin for one side of the move: the planned bin (a
// scan, when given, must equal it) or, when the plan left the side open,
// the scanned bin (required).
func resolveBin(field, planned, scanned string) (string, error) {
	scanned = strings.TrimSpace(scanned)
	if planned == "" {
		if scanned == "" {
			return "", fmt.Errorf("%w: %s must be scanned: the move left it open", ErrInvalidRelocationConfirmation, field)
		}
		return scanned, nil
	}
	if scanned != "" && scanned != planned {
		return "", fmt.Errorf("%w: scanned %s %q differs from the planned %q", ErrInvalidRelocationConfirmation, field, scanned, planned)
	}
	return planned, nil
}

// HasInput reports whether c carries any relocation input at all.
func (c RelocationConfirmation) HasInput() bool {
	return c.Quantity != 0 || strings.TrimSpace(c.FromBin) != "" || strings.TrimSpace(c.ToBin) != ""
}
