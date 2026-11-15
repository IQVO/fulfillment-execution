package main_test

// Step definitions of features/mutation_closeout.feature: the few steps the
// documentation-derived vocabulary lacked to pin the survivors of the first
// mutation pass (the WARN a consumer logs when it ignores a malformed value).

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/cucumber/godog"

	kafkaout "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
)

var (
	closeoutLogsMu sync.Mutex
	closeoutLogs   = map[*world]*bytes.Buffer{}
)

// workReleasedLogIsCaptured swaps the consumer's discard logger for a buffer.
func (w *world) workReleasedLogIsCaptured() error {
	buf := &bytes.Buffer{}
	closeoutLogsMu.Lock()
	closeoutLogs[w] = buf
	closeoutLogsMu.Unlock()
	w.w2.workConsumer.Apply.Logger = slog.New(slog.NewTextHandler(buf, nil))
	return nil
}

func (w *world) capturedLog() string {
	closeoutLogsMu.Lock()
	defer closeoutLogsMu.Unlock()
	if buf := closeoutLogs[w]; buf != nil {
		return buf.String()
	}
	return ""
}

func (w *world) capturedLogHasWarn(fragment string) error {
	for _, line := range strings.Split(w.capturedLog(), "\n") {
		if strings.Contains(line, "level=WARN") && strings.Contains(line, fragment) {
			return nil
		}
	}
	return fmt.Errorf("no WARN line contains %q; log was:\n%s", fragment, w.capturedLog())
}

func (w *world) capturedLogHasNoWarn() error {
	for _, line := range strings.Split(w.capturedLog(), "\n") {
		if strings.Contains(line, "level=WARN") {
			return fmt.Errorf("expected no WARN, got: %s", line)
		}
	}
	return nil
}

// recordedEventsAreEncodedWithoutTasks runs the integration encoder with no
// task repository wired, the documented fail-soft case of manifestCorrelation.
func (w *world) recordedEventsAreEncodedWithoutTasks() error {
	newId := func() string {
		w.rules.wireSeq++
		return fmt.Sprintf("00000000-0000-4000-8000-%012d", w.rules.wireSeq)
	}
	pub := kafkaout.NewPublisherWithWriter(nil, nil, w.handlers.ClaimNext.Stations, newId)
	encoded, err := pub.Encode(context.Background(), w.publisher.Events()...)
	if err != nil {
		return fmt.Errorf("encode the recorded events: %w", err)
	}
	w.rules.wire = encoded
	return nil
}

// registerCloseoutSteps registers the closeout vocabulary on sc.
func (w *world) registerCloseoutSteps(sc *godog.ScenarioContext) {
	sc.Step(`^the WorkReleased consumer's log is captured$`, w.workReleasedLogIsCaptured)
	sc.Step(`^the captured log has a WARN containing "([^"]*)"$`, w.capturedLogHasWarn)
	sc.Step(`^the captured log has no WARN$`, w.capturedLogHasNoWarn)
	sc.Step(`^the recorded events are encoded without a task repository$`, w.recordedEventsAreEncodedWithoutTasks)
}
