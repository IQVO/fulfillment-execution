package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

// flakyReader fails ReadMessage `failures` times (a broker restart), then
// serves msgs, then blocks until ctx is done.
type flakyReader struct {
	mu       sync.Mutex
	failures int
	msgs     []kafkago.Message
	calls    int
	commits  []kafkago.Message
}

func (r *flakyReader) ReadMessage(ctx context.Context) (kafkago.Message, error) {
	r.mu.Lock()
	r.calls++
	if r.failures > 0 {
		r.failures--
		r.mu.Unlock()
		return kafkago.Message{}, errors.New("fetching message: read tcp 127.0.0.1:52955->127.0.0.1:9092: i/o timeout")
	}
	if len(r.msgs) > 0 {
		m := r.msgs[0]
		r.msgs = r.msgs[1:]
		r.mu.Unlock()
		return m, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}

// FetchMessage serves the same script as ReadMessage: the WorkReleased
// consumer fetches and commits explicitly.
func (r *flakyReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	return r.ReadMessage(ctx)
}

// CommitMessages records the committed offsets.
func (r *flakyReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commits = append(r.commits, msgs...)
	return nil
}

type capturingDLQ struct {
	mu   sync.Mutex
	msgs []kafkago.Message
}

func (d *capturingDLQ) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.msgs = append(d.msgs, msgs...)
	return nil
}

// Regression: one transient read error ("i/o timeout" while the broker
// restarted) used to make Run return; main logged "kafka consumer stopped"
// and the healthy-looking process never created a task again.
func TestConsumer_Run_SurvivesBrokerOutage(t *testing.T) {
	r := &flakyReader{failures: 4, msgs: []kafkago.Message{
		{Topic: "warehouse.work-planning.events", Offset: 7, Value: []byte(`{"not":"a cloudevent"}`)},
	}}
	dlq := &capturingDLQ{}
	c := &Consumer{testReader: r, DeadLetter: dlq, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatalf("Run returned %v; it must only stop when ctx is done", err)
	}
	dlq.mu.Lock()
	got := len(dlq.msgs)
	dlq.mu.Unlock()
	if got != 1 {
		t.Fatalf("messages handled after the outage = %d, want 1 (consumption must resume)", got)
	}
}

func TestReadRecovering_ReturnsPromptlyWhenCancelledDuringBackoff(t *testing.T) {
	r := &flakyReader{failures: 1_000_000}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, ok := readRecovering(ctx, r, newRecoverBackoff(), nil, "t"); ok {
		t.Fatal("readRecovering reported a message from a reader that only fails")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("took %v to observe cancellation", time.Since(start))
	}
}

func TestReadRecovering_ResetsBackoffAfterSuccess(t *testing.T) {
	r := &flakyReader{failures: 2, msgs: []kafkago.Message{{Offset: 1}}}
	policy := newRecoverBackoff()
	if _, ok := readRecovering(context.Background(), r, policy, nil, "t"); !ok {
		t.Fatal("expected a message after the transient failures")
	}
	if first := policy.NextBackOff(); first > 2*recoverInitialInterval {
		t.Fatalf("backoff after success = %v, want reset to ~%v", first, recoverInitialInterval)
	}
}

// cancellingUnitOfWork simulates a shutdown landing inside the use case: it
// cancels the consumer's context and reports the cancellation, as the
// Postgres UnitOfWork does when its transaction is interrupted.
type cancellingUnitOfWork struct{ cancel context.CancelFunc }

func (u cancellingUnitOfWork) Execute(ctx context.Context, _ func(context.Context) error) error {
	u.cancel()
	return ctx.Err()
}

// A shutdown that interrupts a healthy WorkReleased is a clean stop: the
// message is neither dead-lettered (it is not poison) nor committed (so it is
// redelivered), and Run returns nil.
func TestConsumer_Run_ShutdownMidHandleNeitherDeadLettersNorCommits(t *testing.T) {
	raw := []byte(`{"specversion":"1.0","id":"evt-1","source":"/warehouse/wes-work-planning","type":"` + TypeWorkReleased +
		`","subject":"order-1","time":"2026-01-01T11:00:00Z","datacontenttype":"application/json",` +
		`"data":{"path_id":"PICK","work_unit_id":"order-1","cpt":"2026-01-01T12:00:00Z"}}`)
	r := &flakyReader{msgs: []kafkago.Message{{Topic: "warehouse.work-planning.events", Offset: 3, Value: raw}}}
	dlq := &capturingDLQ{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &Consumer{
		testReader: r,
		DeadLetter: dlq,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Apply:      &usecases.ApplyWorkReleased{UnitOfWork: cancellingUnitOfWork{cancel: cancel}},
	}

	if err := c.Run(ctx); err != nil {
		t.Fatalf("Run returned %v, want nil (a shutdown mid-handle is a clean stop)", err)
	}
	dlq.mu.Lock()
	dead := len(dlq.msgs)
	dlq.mu.Unlock()
	if dead != 0 {
		t.Fatalf("dead-lettered %d message(s) on shutdown, want 0", dead)
	}
	r.mu.Lock()
	committed := len(r.commits)
	r.mu.Unlock()
	if committed != 0 {
		t.Fatalf("committed %d message(s) on shutdown, want 0 (it must be redelivered)", committed)
	}
}
