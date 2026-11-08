package task_test

import (
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// at builds a UTC time on the fixture date the per-stop duration tests use
// (ADR 0064). clock is "HH:MM" or "HH:MM:SS".
func at(clock string) time.Time {
	for _, layout := range []string{"15:04:05", "15:04"} {
		if parsed, err := time.Parse(layout, clock); err == nil {
			return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(parsed.Hour())*time.Hour + time.Duration(parsed.Minute())*time.Minute + time.Duration(parsed.Second())*time.Second)
		}
	}
	panic("test fixture clock must be HH:MM or HH:MM:SS: " + clock)
}

// claimedPickTask is a PICK task claimed at at — the fixture the per-stop
// duration tests build on.
func claimedPickTask(at time.Time) *task.Task {
	tk := newPickTask()
	if err := tk.Claim(shared.StationId("stn-1"), shared.NewCapabilitySet("pick"), at, 5*time.Minute); err != nil {
		panic(err)
	}
	return tk
}

// TestDurationBase_IsWorkStartWhenSet pins the per-stop rule (ADR 0064):
// once a work start is recorded, the duration base is that instant, not the
// claim.
func TestDurationBase_IsWorkStartWhenSet(t *testing.T) {
	tk := claimedPickTask(at("10:00"))
	tk.StartWork(at("10:04"))
	if got := tk.DurationBase(); !got.Equal(at("10:04")) {
		t.Fatalf("base = %v", got)
	}
}

// TestDurationBase_IsClaimTimeWhenWorkStartUnset pins the fallback: a task
// that never entered a batch (every task today, and every pre-migration
// row) reports the claim time, so duration_seconds — and its published
// bytes — are exactly what they were before batching existed.
func TestDurationBase_IsClaimTimeWhenWorkStartUnset(t *testing.T) {
	tk := claimedPickTask(at("10:00"))
	if got := tk.DurationBase(); !got.Equal(at("10:00")) {
		t.Fatalf("base = %v, want the claim time", got)
	}
}

// TestDurationBase_NilWhenNeverClaimed: no claim and no work start means no
// duration can be computed (the publisher omits duration_seconds).
func TestDurationBase_NilWhenNeverClaimed(t *testing.T) {
	if got := newPickTask().DurationBase(); got != nil {
		t.Fatalf("base = %v, want nil", got)
	}
}

// TestStartWork_IsWriteOnce: a replayed completion (or a doubled
// next-stop-clock tick) must not move an already-started clock — the
// recorded work start stays the FIRST one.
func TestStartWork_IsWriteOnce(t *testing.T) {
	tk := claimedPickTask(at("10:00"))
	tk.StartWork(at("10:04"))
	tk.StartWork(at("10:09"))
	if got := tk.DurationBase(); !got.Equal(at("10:04")) {
		t.Fatalf("base = %v, want the first work start 10:04", got)
	}
}

// TestWithWorkStartedAt_RoundTripsThroughNil: the rehydrate stamp copies a
// set value and leaves a nil one unset.
func TestWithWorkStartedAt_RoundTripsThroughNil(t *testing.T) {
	tk := claimedPickTask(at("10:00"))
	tk.StartWork(at("10:04"))

	reh := task.Rehydrate(tk.Id(), tk.Type(), tk.Status(), tk.CPT(), tk.OrderRef(), tk.RequiredCapabilities(), tk.Lease(), tk.Fragile(), tk.GiftWrap(), tk.ClaimedAt()).WithWorkStartedAt(tk.WorkStartedAt())
	if got := reh.DurationBase(); got == nil || !got.Equal(at("10:04")) {
		t.Fatalf("rehydrated base = %v, want 10:04", got)
	}

	plain := task.Rehydrate(tk.Id(), tk.Type(), tk.Status(), tk.CPT(), tk.OrderRef(), tk.RequiredCapabilities(), tk.Lease(), tk.Fragile(), tk.GiftWrap(), tk.ClaimedAt())
	if got := plain.DurationBase(); got == nil || !got.Equal(at("10:00")) {
		t.Fatalf("nil-stamped base = %v, want the claim time 10:00", got)
	}
}
