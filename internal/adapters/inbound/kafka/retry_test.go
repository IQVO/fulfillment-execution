package kafka_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/pathcatalog"
)

// flakyCatalogue wraps a real *pathcatalog.Catalogue's Lookup, failing
// the first failUntilCalls calls with a transient error before
// delegating to the real lookup — models a downstream blip (a
// momentary hiccup) that Handle's in-process retry (ADR-0029 §DLQ)
// should absorb transparently, distinct from a genuinely unknown
// path_id (which is a real, non-transient error and must NOT be
// retried away).
type flakyCatalogue struct {
	inner          *pathcatalog.Catalogue
	failUntilCalls int32
	calls          int32
}

func (f *flakyCatalogue) Lookup(id string) (pathcatalog.PathDefinition, error) {
	n := atomic.AddInt32(&f.calls, 1)
	if n <= f.failUntilCalls {
		return pathcatalog.PathDefinition{}, errors.New("transient catalogue lookup failure")
	}
	return f.inner.Lookup(id)
}

func newConsumerWithCatalogue(t *testing.T, catalogue *flakyCatalogue) (*kafka.Consumer, *memory.TaskRepo, *fakeDeadLetterSink) {
	t.Helper()
	tasks := memory.NewTaskRepo()
	apply := newApplyWorkReleased(tasks, memory.NewProcessedEventsRepo(), catalogue)
	dlq := &fakeDeadLetterSink{}
	c := &kafka.Consumer{
		Apply:      apply,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		DeadLetter: dlq,
	}
	return c, tasks, dlq
}

// TestHandle_RetriesTransientFailureThenSucceeds is the ADR-0029 §DLQ
// retry acceptance test for this consumer: a downstream blip that
// clears within maxHandlerAttempts (3) is absorbed transparently by
// Handle — the message is processed successfully and NEVER reaches the
// dead-letter topic.
func TestHandle_RetriesTransientFailureThenSucceeds(t *testing.T) {
	catalogue := &flakyCatalogue{inner: testCatalogue(), failUntilCalls: 2}
	c, tasks, dlq := newConsumerWithCatalogue(t, catalogue)

	msg := kafkago.Message{Topic: "warehouse.work-planning.events", Value: workReleasedJSON("evt-retry-1", "PICK", "wu-retry-1")}
	if err := c.Handle(context.Background(), msg); err != nil {
		t.Fatalf("Handle: unexpected error after the transient failure should have been retried away: %v", err)
	}

	if got := totalPending(t, tasks); got != 1 {
		t.Fatalf("expected exactly 1 task created after the retry succeeded, got %d", got)
	}
	if len(dlq.sent) != 0 {
		t.Fatalf("expected NO dead-letter publish for a transient failure that was retried away, got %d", len(dlq.sent))
	}
	if got := atomic.LoadInt32(&catalogue.calls); got != 3 {
		t.Fatalf("catalogue.Lookup calls = %d, want exactly 3 (2 failures + 1 success)", got)
	}
}

// TestHandle_ExhaustsRetriesThenPropagatesError proves a failure that
// persists across the FULL retry budget (maxHandlerAttempts=3) is not
// silently retried forever — Handle returns the error, so Run's caller
// dead-letters the message (see Run's own doc comment; the actual DLQ
// publish over a real Kafka broker is exercised end-to-end by
// TestRun_PoisonMessage_GoesToDeadLetterTopicAndLoopContinues in
// dlq_integration_test.go).
func TestHandle_ExhaustsRetriesThenPropagatesError(t *testing.T) {
	catalogue := &flakyCatalogue{inner: testCatalogue(), failUntilCalls: 1000}
	c, _, _ := newConsumerWithCatalogue(t, catalogue)

	msg := kafkago.Message{Topic: "warehouse.work-planning.events", Value: workReleasedJSON("evt-retry-2", "PICK", "wu-retry-2")}
	err := c.Handle(context.Background(), msg)
	if err == nil {
		t.Fatal("expected Handle to return an error once every retry attempt has failed")
	}
	if got := atomic.LoadInt32(&catalogue.calls); got != 3 {
		t.Fatalf("catalogue.Lookup calls = %d, want exactly 3 (maxHandlerAttempts, no more)", got)
	}
}

// TestHandleMessage_UnknownPathId_IsNotRetried proves a genuinely
// unknown path_id (a permanent, non-transient error — see PathCatalogue's
// doc comment) is NOT masked by retry: retrying it would just waste
// maxHandlerAttempts attempts on an error retry can never fix. This
// consumer's retry wraps the WHOLE HandleMessage call, so this test
// pins that a real *pathcatalog.Catalogue (which fails deterministically
// for an unknown id, not flakily) still only takes 3 attempts, not more
// or less, and ultimately still surfaces the same error Handle
// (non-retrying) always returned for it.
func TestHandleMessage_UnknownPathId_IsNotRetried(t *testing.T) {
	catalogue := &flakyCatalogue{inner: testCatalogue(), failUntilCalls: 0}
	c, _, _ := newConsumerWithCatalogue(t, catalogue)

	msg := kafkago.Message{Topic: "warehouse.work-planning.events", Value: workReleasedJSON("evt-unknown", "not-a-real-path", "wu-3")}
	if err := c.Handle(context.Background(), msg); err == nil {
		t.Fatal("expected an error for an unrecognized path_id")
	}
	// A real *pathcatalog.Catalogue fails deterministically (same error
	// every time) for an unknown id, so the retry loop still burns all
	// 3 attempts before giving up -- retry cannot distinguish "will
	// never succeed" from "transient" on its own; that is exactly why
	// the DLQ exists as the backstop.
	if got := atomic.LoadInt32(&catalogue.calls); got != 3 {
		t.Fatalf("catalogue.Lookup calls = %d, want exactly 3", got)
	}
}
