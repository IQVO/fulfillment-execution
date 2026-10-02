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
)

// flakyReader fails ReadMessage `failures` times (a broker restart), then
// serves msgs, then blocks until ctx is done.
type flakyReader struct {
	mu       sync.Mutex
	failures int
	msgs     []kafkago.Message
	calls    int
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
