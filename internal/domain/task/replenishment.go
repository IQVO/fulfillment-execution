package task

import (
	"errors"
	"fmt"
	"strings"
)

// Replenish is the task type of pick-face replenishment work (ADR 0050): one
// planned move of a SKU's stock from a reserve (source) bin to a forward
// pick face (destination bin), requested by pick-face-replenishment, planned
// by wes-work-planning and released on the REPLENISH process path. It is a
// sibling of Relocate and, like it, is created only from released work,
// never through POST /tasks, because it needs the move.
const Replenish Type = "REPLENISH"

// MaxReplenishmentRequestIdLen bounds the request id: it becomes
// TaskCompleted.request_ref, which inventory-storage stores as the
// RelocateStock source_ref (limited to 200 characters).
const MaxReplenishmentRequestIdLen = 200

// ReplenishmentPriority is the urgency pick-face-replenishment stamped on the
// request: exactly the `priority` values of WorkReleased.replenishment.
type ReplenishmentPriority string

const (
	// ReplenishmentPriorityNormal is a request detected below the minimum
	// while the pick face still has usable stock.
	ReplenishmentPriorityNormal ReplenishmentPriority = "NORMAL"
	// ReplenishmentPriorityUrgent is a request detected with no usable stock
	// left in the pick face (pickers are about to short).
	ReplenishmentPriorityUrgent ReplenishmentPriority = "URGENT"
)

var (
	// ErrUnknownReplenishmentPriority: a priority other than NORMAL, URGENT.
	ErrUnknownReplenishmentPriority = errors.New("task: unknown replenishment priority")
	// ErrInvalidReplenishmentMove: the released move is incomplete or
	// inconsistent (missing request id, sku or bin, quantity below 1, an
	// over-long request id, or from_bin equal to to_bin).
	ErrInvalidReplenishmentMove = errors.New("task: invalid replenishment move")
	// ErrReplenishmentConfirmationRequired: a REPLENISH task was completed
	// without the operator-confirmed quantity.
	ErrReplenishmentConfirmationRequired = errors.New("task: a REPLENISH task completes only with the confirmed quantity")
	// ErrInvalidReplenishmentConfirmation: the confirmation is unusable (a
	// quantity below 1, or a scanned bin that differs from the planned one).
	ErrInvalidReplenishmentConfirmation = errors.New("task: invalid replenishment confirmation")
)

// ParseReplenishmentPriority validates the wire/persisted form of a
// ReplenishmentPriority (exact, case-sensitive match).
func ParseReplenishmentPriority(value string) (ReplenishmentPriority, error) {
	p := ReplenishmentPriority(value)
	if p == ReplenishmentPriorityNormal || p == ReplenishmentPriorityUrgent {
		return p, nil
	}
	return "", fmt.Errorf("%w: %q", ErrUnknownReplenishmentPriority, value)
}

// ReplenishmentMove is the planned move a REPLENISH task executes, stamped at
// release time (WorkReleased.replenishment). Both bins and the planned
// quantity are always known: unlike a RELOCATE Assign or Vacate, no side is
// left open for the operator to choose.
type ReplenishmentMove struct {
	RequestId string
	SKU       string
	FromBin   string
	ToBin     string
	Quantity  int
	Priority  ReplenishmentPriority
}

// NewReplenishmentMove validates and builds a move. Every string is trimmed.
func NewReplenishmentMove(requestId, sku, fromBin, toBin string, quantity int, priority string) (ReplenishmentMove, error) {
	p, err := ParseReplenishmentPriority(strings.TrimSpace(priority))
	if err != nil {
		return ReplenishmentMove{}, err
	}
	m := ReplenishmentMove{
		RequestId: strings.TrimSpace(requestId),
		SKU:       strings.TrimSpace(sku),
		FromBin:   strings.TrimSpace(fromBin),
		ToBin:     strings.TrimSpace(toBin),
		Quantity:  quantity,
		Priority:  p,
	}
	if err := m.validate(); err != nil {
		return ReplenishmentMove{}, err
	}
	return m, nil
}

func (m ReplenishmentMove) validate() error {
	if m.RequestId == "" {
		return fmt.Errorf("%w: request_id is required", ErrInvalidReplenishmentMove)
	}
	if len(m.RequestId) > MaxReplenishmentRequestIdLen {
		return fmt.Errorf("%w: request_id is longer than %d characters", ErrInvalidReplenishmentMove, MaxReplenishmentRequestIdLen)
	}
	if m.SKU == "" {
		return fmt.Errorf("%w: sku is required", ErrInvalidReplenishmentMove)
	}
	if m.FromBin == "" {
		return fmt.Errorf("%w: from_bin is required", ErrInvalidReplenishmentMove)
	}
	if m.ToBin == "" {
		return fmt.Errorf("%w: to_bin is required", ErrInvalidReplenishmentMove)
	}
	if m.FromBin == m.ToBin {
		return fmt.Errorf("%w: from_bin and to_bin are both %q", ErrInvalidReplenishmentMove, m.FromBin)
	}
	if m.Quantity < 1 {
		return fmt.Errorf("%w: quantity must be at least 1, got %d", ErrInvalidReplenishmentMove, m.Quantity)
	}
	return nil
}

// Ref is the move's correlation reference: the request id, published as
// TaskCompleted.request_ref (mirrors RelocationMove.Ref).
func (m ReplenishmentMove) Ref() string { return m.RequestId }

// ReplenishmentConfirmation is what the operator reports when completing a
// REPLENISH task: the counted quantity and, optionally, the bins scanned.
// It is the same shape as RelocationConfirmation: REST and MCP take one set
// of completion inputs for both move task types.
type ReplenishmentConfirmation = RelocationConfirmation

// ReplenishmentResult is the executed move recorded on completion: the
// confirmed quantity and the actual source and destination bins.
type ReplenishmentResult struct {
	Quantity int
	FromBin  string
	ToBin    string
}

// resolve checks a confirmation against the planned move: the quantity is at
// least 1 and a scanned bin matches the planned one (a scan is optional,
// because both bins are known). Like RELOCATE, the confirmed quantity is the
// operator's count: it need not equal the planned quantity, and
// inventory-storage validates it against the ledger.
func (m ReplenishmentMove) resolve(c ReplenishmentConfirmation) (ReplenishmentResult, error) {
	if c.Quantity < 1 {
		return ReplenishmentResult{}, fmt.Errorf("%w: quantity must be at least 1, got %d", ErrInvalidReplenishmentConfirmation, c.Quantity)
	}
	if err := matchScan("from_bin", m.FromBin, c.FromBin); err != nil {
		return ReplenishmentResult{}, err
	}
	if err := matchScan("to_bin", m.ToBin, c.ToBin); err != nil {
		return ReplenishmentResult{}, err
	}
	return ReplenishmentResult{Quantity: c.Quantity, FromBin: m.FromBin, ToBin: m.ToBin}, nil
}

// matchScan checks one optional scan against the planned bin.
func matchScan(field, planned, scanned string) error {
	scanned = strings.TrimSpace(scanned)
	if scanned != "" && scanned != planned {
		return fmt.Errorf("%w: scanned %s %q differs from the planned %q", ErrInvalidReplenishmentConfirmation, field, scanned, planned)
	}
	return nil
}
