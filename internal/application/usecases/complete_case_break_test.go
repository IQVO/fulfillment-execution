package usecases_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// scriptedBreaker answers each BreakCase call with the next scripted error (nil
// = success) and records every request, so a test can assert the Idempotency-Key
// of each attempt. Past the script it repeats the last answer.
type scriptedBreaker struct {
	answers []error
	seen    []ports.CaseBreakRequest
}

func (b *scriptedBreaker) BreakCase(_ context.Context, r ports.CaseBreakRequest) error {
	b.seen = append(b.seen, r)
	i := min(len(b.seen), len(b.answers)) - 1
	return b.answers[i]
}

type caseBreakFixture struct {
	uc        *usecases.CompleteCaseBreak
	tasks     *memory.TaskRepo
	published *events.BufferedPublisher
	clock     *memory.FixedClock
	breaker   *scriptedBreaker
}

// newCaseBreakFixture seeds one BREAK_CASE task claimed by station s-1.
func newCaseBreakFixture(t *testing.T, answers ...error) caseBreakFixture {
	t.Helper()
	ctx := context.Background()
	clock := memory.NewFixedClock(atomicEpoch)
	brk, err := task.NewCaseBreak("LPN-1", "SKU-1", "BIN-LOOSE", "pick-1")
	if err != nil {
		t.Fatalf("NewCaseBreak: %v", err)
	}
	tk := task.NewCaseBreakTask("tb-1", shared.NewCPT(atomicEpoch.Add(48*time.Hour)), "o-1", shared.NewCapabilitySet("break"), brk)
	if err := tk.Claim("s-1", shared.NewCapabilitySet("break"), clock.Now(), usecases.DefaultLeaseDuration); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	tasks := memory.NewTaskRepo()
	if err := tasks.Save(ctx, tk); err != nil {
		t.Fatalf("Save: %v", err)
	}
	published := events.NewBufferedPublisher()
	breaker := &scriptedBreaker{answers: answers}
	return caseBreakFixture{
		uc:    &usecases.CompleteCaseBreak{Tasks: tasks, Breaker: breaker, Publisher: published, Clock: clock},
		tasks: tasks, published: published, clock: clock, breaker: breaker,
	}
}

func (f caseBreakFixture) status(t *testing.T) task.Status {
	t.Helper()
	tk, err := f.tasks.FindById(context.Background(), "tb-1")
	if err != nil || tk == nil {
		t.Fatalf("FindById: %v %v", tk, err)
	}
	return tk.Status()
}

func releasedRefusal() error {
	return fmt.Errorf("%w: %w: status 404: bin-not-found", ports.ErrCaseBreakRefusalReleased, ports.ErrCaseBreakUnavailable)
}

// A refusal inventory-storage releases leaves the case-break retryable: the task
// stays Claimed, nothing is published, and the retry under the SAME key
// (break-case:<taskId>, no attempt part) completes it when the callee now passes.
func TestCompleteCaseBreak_AReleasedRefusalStaysRetryableAndALaterSuccessCompletesUnderTheSameKey(t *testing.T) {
	f := newCaseBreakFixture(t, releasedRefusal(), nil)
	ctx := context.Background()

	err := f.uc.Execute(ctx, "tb-1", "s-1")
	if !errors.Is(err, ports.ErrCaseBreakRefusalReleased) || !errors.Is(err, ports.ErrCaseBreakUnavailable) || errors.Is(err, ports.ErrCaseBreakRejected) {
		t.Fatalf("first attempt = %v, want a released refusal that is retryable and not a rejection", err)
	}
	if st := f.status(t); st != task.Claimed || len(f.published.Events()) != 0 {
		t.Fatalf("after the released refusal status=%s events=%d, want Claimed and none", st, len(f.published.Events()))
	}

	if err := f.uc.Execute(ctx, "tb-1", "s-1"); err != nil {
		t.Fatalf("retry after the callee passes: %v", err)
	}
	if st := f.status(t); st != task.Completed || len(f.published.Events()) != 1 {
		t.Fatalf("after the retry status=%s events=%d, want Completed and one TaskCompleted", st, len(f.published.Events()))
	}
	if len(f.breaker.seen) != 2 || f.breaker.seen[0].IdempotencyKey != "break-case:tb-1" || f.breaker.seen[1].IdempotencyKey != "break-case:tb-1" {
		t.Fatalf("keys = %+v, want the same break-case:tb-1 on both attempts", f.breaker.seen)
	}
}

// A FINAL or OUTCOME refusal is terminal exactly as before: a rejection, the task
// stays Claimed, and the answer is not the retryable one.
func TestCompleteCaseBreak_AFinalRefusalIsARejectionNotARetry(t *testing.T) {
	rejected := fmt.Errorf("%w: status 400: empty-bin-id", ports.ErrCaseBreakRejected)
	f := newCaseBreakFixture(t, rejected)

	err := f.uc.Execute(context.Background(), "tb-1", "s-1")
	if !errors.Is(err, ports.ErrCaseBreakRejected) || errors.Is(err, ports.ErrCaseBreakUnavailable) || errors.Is(err, ports.ErrCaseBreakRefusalReleased) {
		t.Fatalf("err = %v, want only ErrCaseBreakRejected", err)
	}
	if st := f.status(t); st != task.Claimed || len(f.published.Events()) != 0 {
		t.Fatalf("status=%s events=%d, want Claimed and none", st, len(f.published.Events()))
	}
}

// What bounds a refusal that never passes: every attempt reuses the key while the
// lease lasts, and when the claim expires the case-break ends where it always
// did (task-not-claimed, the task back to the pool) without calling the callee.
func TestCompleteCaseBreak_AForeverReleasedRefusalEndsWhenTheLeaseExpires(t *testing.T) {
	f := newCaseBreakFixture(t, releasedRefusal())
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := f.uc.Execute(ctx, "tb-1", "s-1"); !errors.Is(err, ports.ErrCaseBreakRefusalReleased) {
			t.Fatalf("attempt %d = %v, want the released refusal", i, err)
		}
	}
	f.clock.Advance(usecases.DefaultLeaseDuration + 1)
	err := f.uc.Execute(ctx, "tb-1", "s-1")
	if !errors.Is(err, task.ErrNotClaimed) {
		t.Fatalf("after the lease = %v, want task.ErrNotClaimed", err)
	}
	if len(f.breaker.seen) != 3 {
		t.Fatalf("callee called %d times, want 3 (none after the lease expired)", len(f.breaker.seen))
	}
	for _, r := range f.breaker.seen {
		if r.IdempotencyKey != "break-case:tb-1" {
			t.Fatalf("key = %q, want break-case:tb-1 on every attempt", r.IdempotencyKey)
		}
	}
}
