package main_test

// Step definitions of features/mutation_closeout.feature: the few steps the
// documentation-derived vocabulary lacked to pin the survivors of the first
// mutation pass (the WARN a consumer logs when it ignores a malformed value).

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/cucumber/godog"
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

// registerCloseoutSteps registers the closeout vocabulary on sc.
func (w *world) registerCloseoutSteps(sc *godog.ScenarioContext) {
	sc.Step(`^the WorkReleased consumer's log is captured$`, w.workReleasedLogIsCaptured)
	sc.Step(`^the captured log has a WARN containing "([^"]*)"$`, w.capturedLogHasWarn)
	sc.Step(`^the captured log has no WARN$`, w.capturedLogHasNoWarn)
}
