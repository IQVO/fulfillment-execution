package task

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// BreakCase is the task type that opens one sealed case into loose eaches at
// a pick face (ADR 0054). It is created when an EACH pick finds only a
// sealed case at its source bin, claimed like any other task, and completing
// it asks inventory-storage to break the case (POST /containers/{lpn}:break);
// it is never completed without that call succeeding.
const BreakCase Type = "BREAK_CASE"

// maxLPNLen mirrors inventory-storage's container license plate bound.
const maxLPNLen = 64

var lpnPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

var (
	// ErrInvalidCaseBreak: the break is incomplete (blank lpn, sku, loose bin
	// or pick task) or the lpn is not a valid license plate.
	ErrInvalidCaseBreak = errors.New("task: invalid case break")
	// ErrNotCaseBreakTask: a case-break operation on a task of another type.
	ErrNotCaseBreakTask = errors.New("task: not a BREAK_CASE task")
	// ErrCaseBreakCompletionRequired: a BREAK_CASE task was completed through
	// the plain path, which would skip the inventory-storage break.
	ErrCaseBreakCompletionRequired = errors.New("task: a BREAK_CASE task completes only through the case break")
)

// CaseBreak is what a BREAK_CASE task executes: break the sealed case lpn
// (of sku) into loose eaches in looseBin, the pick face of the EACH pick
// PickTaskId that found only the sealed case.
type CaseBreak struct {
	LPN        string
	SKU        string
	LooseBin   string
	PickTaskId string
}

// NewCaseBreak validates and builds a break. Every string is trimmed.
func NewCaseBreak(lpn, sku, looseBin, pickTaskId string) (CaseBreak, error) {
	b := CaseBreak{
		LPN:        strings.TrimSpace(lpn),
		SKU:        strings.TrimSpace(sku),
		LooseBin:   strings.TrimSpace(looseBin),
		PickTaskId: strings.TrimSpace(pickTaskId),
	}
	switch {
	case b.LPN == "" || len(b.LPN) > maxLPNLen || !lpnPattern.MatchString(b.LPN):
		return CaseBreak{}, fmt.Errorf("%w: lpn %q is not 1-%d characters of [A-Za-z0-9._-]", ErrInvalidCaseBreak, lpn, maxLPNLen)
	case b.SKU == "":
		return CaseBreak{}, fmt.Errorf("%w: sku is required", ErrInvalidCaseBreak)
	case b.LooseBin == "":
		return CaseBreak{}, fmt.Errorf("%w: loose bin is required", ErrInvalidCaseBreak)
	case b.PickTaskId == "":
		return CaseBreak{}, fmt.Errorf("%w: pick task id is required", ErrInvalidCaseBreak)
	}
	return b, nil
}

// NewCaseBreakTask creates a Pending BREAK_CASE task carrying the break.
// Same construction invariants as New; the break was validated by
// NewCaseBreak. A break carries no packing hints.
func NewCaseBreakTask(id shared.TaskId, cpt shared.CPT, orderRef shared.OrderRef, required shared.CapabilitySet, brk CaseBreak) *Task {
	return New(id, BreakCase, cpt, orderRef, required, false, false).WithCaseBreak(&brk)
}

// CaseBreak returns the break of a BREAK_CASE task, or nil for any other task.
// The pointer is the task's own: do not mutate it.
func (t *Task) CaseBreak() *CaseBreak { return t.caseBreak }

// WithCaseBreak stamps the break and returns the task (creation, and
// repository rehydration). The break is copied; nil clears it.
func (t *Task) WithCaseBreak(brk *CaseBreak) *Task {
	t.caseBreak = nil
	if brk != nil {
		cp := *brk
		t.caseBreak = &cp
	}
	return t
}

// AuthorizeCaseBreak checks, without changing the task, that stationId may
// complete this BREAK_CASE task now (a completed task is
// ErrAlreadyCompleted; no or an expired lease ErrNotClaimed; another
// station's lease ErrNotOwner) and returns the break to execute. The use case
// calls inventory-storage between this and CompleteCaseBreak.
func (t *Task) AuthorizeCaseBreak(stationId shared.StationId, now time.Time) (CaseBreak, error) {
	if t.taskType != BreakCase || t.caseBreak == nil {
		return CaseBreak{}, ErrNotCaseBreakTask
	}
	if t.status == Completed {
		return CaseBreak{}, ErrAlreadyCompleted
	}
	if err := t.VerifyHeldBy(stationId, now); err != nil {
		return CaseBreak{}, err
	}
	return *t.caseBreak, nil
}

// CompleteCaseBreak finishes the BREAK_CASE task once inventory-storage has
// broken the case. The same claim rules as Complete.
func (t *Task) CompleteCaseBreak(stationId shared.StationId, now time.Time) error {
	if t.taskType != BreakCase || t.caseBreak == nil {
		return ErrNotCaseBreakTask
	}
	if err := t.checkCompletable(stationId, now); err != nil {
		return err
	}
	t.finish()
	return nil
}
