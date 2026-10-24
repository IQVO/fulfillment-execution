package ports

import (
	"context"
	"errors"
)

var (
	// ErrCaseBreakRejected: inventory-storage refused the break for a reason
	// a retry cannot fix (unknown container, container not SEALED, not at the
	// site, reserved, bin/handling-unit mismatch). Permanent.
	ErrCaseBreakRejected = errors.New("ports: inventory-storage rejected the case break")
	// ErrCaseBreakUnavailable: inventory-storage could not be reached or
	// answered with a server error. Transient: retrying with the same
	// idempotency key is safe.
	ErrCaseBreakUnavailable = errors.New("ports: inventory-storage is unavailable for the case break")
)

// CaseBreakRequest is one break of a sealed case into loose eaches.
// IdempotencyKey is stable per BREAK_CASE task, so a retry after a lost
// response (or a failed local save) replays the first answer instead of
// breaking twice.
type CaseBreakRequest struct {
	LPN            string
	LooseBin       string
	IdempotencyKey string
}

// CaseBreaker is the outbound port to inventory-storage's
// POST /containers/{lpn}:break {looseBin} (ADR 0054). Quantity is conserved
// by inventory-storage: the loose eaches produced equal the container's
// eaches; this context never states a quantity for the break.
type CaseBreaker interface {
	BreakCase(ctx context.Context, req CaseBreakRequest) error
}
