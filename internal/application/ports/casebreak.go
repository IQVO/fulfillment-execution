package ports

import (
	"context"
	"errors"
)

var (
	// ErrCaseBreakRejected: inventory-storage refused the break with a problem
	// type it stores and replays (a FINAL or OUTCOME refusal), an unknown
	// problem type, or a 4xx without a problem body. A retry cannot fix it.
	// Permanent.
	ErrCaseBreakRejected = errors.New("ports: inventory-storage rejected the case break")
	// ErrCaseBreakUnavailable: inventory-storage could not be reached or
	// answered with a server error. Transient: retrying with the same
	// idempotency key is safe.
	ErrCaseBreakUnavailable = errors.New("ports: inventory-storage is unavailable for the case break")
	// ErrCaseBreakRefusalReleased: inventory-storage refused the break with a
	// problem type it declares RETRYABLE (ADR 0053 there): nothing was
	// committed, the Idempotency-Key is released and the same request under
	// the SAME key executes again and may pass (a bin registered later, a
	// concurrent writer done). It always wraps ErrCaseBreakUnavailable: the
	// task stays claimed, the answer is a 5xx that is not stored, and a retry
	// is safe (ADR 0066). Which refusals count is decided by the outbound
	// adapter alone.
	ErrCaseBreakRefusalReleased = errors.New("ports: inventory-storage refused the case break for a reason that can pass")
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
// POST /containers/{lpn}/case-breaks {looseBin} (ADR 0054). Quantity is conserved
// by inventory-storage: the loose eaches produced equal the container's
// eaches; this context never states a quantity for the break.
type CaseBreaker interface {
	BreakCase(ctx context.Context, req CaseBreakRequest) error
}
