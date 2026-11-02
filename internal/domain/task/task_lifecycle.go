package task

import (
	"fmt"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

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
// again (no double-complete). A RELOCATE or REPLENISH task can not be
// completed this way: it needs the operator's confirmation
// (CompleteRelocation, ADR 0047; CompleteReplenishment, ADR 0050).
func (t *Task) Complete(stationId shared.StationId, now time.Time) error {
	if err := t.checkCompletable(stationId, now); err != nil {
		return err
	}
	if t.taskType == Relocate {
		return ErrRelocationConfirmationRequired
	}
	if t.taskType == Replenish {
		return ErrReplenishmentConfirmationRequired
	}
	if t.taskType == BreakCase {
		return ErrCaseBreakCompletionRequired
	}
	if t.pickPlan != nil && t.pickedEaches != t.pickPlan.Quantity {
		return fmt.Errorf("%w: %d scanned of %d planned", ErrPickIncomplete, t.pickedEaches, t.pickPlan.Quantity)
	}
	t.finish()
	return nil
}

// CompleteReplenishment finishes a REPLENISH task with the operator's
// confirmation (ADR 0050): the same claim rules as Complete, then the
// confirmed quantity (at least 1) and any scanned bin are checked against
// the planned move and recorded as ReplenishmentResult.
func (t *Task) CompleteReplenishment(stationId shared.StationId, now time.Time, c ReplenishmentConfirmation) error {
	if err := t.checkCompletable(stationId, now); err != nil {
		return err
	}
	if t.replenishment == nil {
		return ErrNotRelocationTask
	}
	result, err := t.replenishment.resolve(c)
	if err != nil {
		return err
	}
	t.replenishmentResult = &result
	t.finish()
	return nil
}

// CompleteRelocation finishes a RELOCATE task with the operator's
// confirmation (ADR 0047): the same claim rules as Complete, then the
// confirmed quantity (at least 1) and the bins the move left open (scanned)
// are checked against the planned move and recorded as RelocationResult.
func (t *Task) CompleteRelocation(stationId shared.StationId, now time.Time, c RelocationConfirmation) error {
	if err := t.checkCompletable(stationId, now); err != nil {
		return err
	}
	if t.relocation == nil {
		return ErrNotRelocationTask
	}
	result, err := t.relocation.resolve(c)
	if err != nil {
		return err
	}
	t.relocationResult = &result
	t.finish()
	return nil
}

// checkCompletable applies the claim rules shared by Complete and
// CompleteRelocation. An expired lease frees the task (Pending) and
// reports ErrNotClaimed.
func (t *Task) checkCompletable(stationId shared.StationId, now time.Time) error {
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
	return nil
}

func (t *Task) finish() {
	t.status = Completed
	t.lease = nil
}
