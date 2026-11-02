package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// quitServer stands in for the Istio sidecar's /quitquitquit endpoint and
// counts how often it was asked to quit.
type quitServer struct {
	*httptest.Server
	calls atomic.Int32
}

func newQuitServer(t *testing.T) *quitServer {
	t.Helper()
	q := &quitServer{}
	q.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/quitquitquit" {
			q.calls.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(q.Close)
	return q
}

func (q *quitServer) url() string { return q.URL + "/quitquitquit" }

// sweepServer answers each POST with the next status of statuses (the last
// one repeats) and counts the calls.
func sweepServer(t *testing.T, statuses ...int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(calls.Add(1))
		if n > len(statuses) {
			n = len(statuses)
		}
		w.WriteHeader(statuses[n-1])
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

// unreachableURL returns a URL on a loopback port nothing listens on: the
// port was bound and released, so a connect is refused at once.
func unreachableURL(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return "http://" + addr + "/task-lease-expirations"
}

func env(kv map[string]string) lookupFunc {
	return func(k string) (string, bool) {
		v, ok := kv[k]
		return v, ok
	}
}

// testRunner never sleeps for real but records every backoff it was asked for.
func testRunner(backoffs *[]time.Duration) runner {
	return runner{
		client: &http.Client{},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		sleep: func(_ context.Context, d time.Duration) error {
			*backoffs = append(*backoffs, d)
			return nil
		},
	}
}

func TestSweepSucceedsOnFirstAttemptAndQuitsSidecar(t *testing.T) {
	srv, calls := sweepServer(t, http.StatusNoContent)
	quit := newQuitServer(t)
	var backoffs []time.Duration

	code := testRunner(&backoffs).runMain(context.Background(), env(map[string]string{
		"SWEEP_URL": srv.URL, "SIDECAR_QUIT_URL": quit.url(),
	}))

	if code != exitOK {
		t.Fatalf("exit code = %d, want %d", code, exitOK)
	}
	if calls.Load() != 1 {
		t.Fatalf("sweep calls = %d, want 1", calls.Load())
	}
	if quit.calls.Load() != 1 {
		t.Fatalf("sidecar quit calls = %d, want 1", quit.calls.Load())
	}
	if len(backoffs) != 0 {
		t.Fatalf("no backoff expected on success, got %v", backoffs)
	}
}

// The give-up path (ADR 0048): an unreachable API must end in a bounded
// number of attempts, a non-zero exit and a sidecar quit, never a hang.
func TestSweepGivesUpOnUnreachableEndpointExitsNonZeroAndQuitsSidecar(t *testing.T) {
	quit := newQuitServer(t)
	var backoffs []time.Duration
	start := time.Now()

	code := testRunner(&backoffs).runMain(context.Background(), env(map[string]string{
		"SWEEP_URL":             unreachableURL(t),
		"SWEEP_MAX_ATTEMPTS":    "3",
		"SWEEP_REQUEST_TIMEOUT": "2s",
		"SWEEP_RETRY_BACKOFF":   "1s",
		"SWEEP_MAX_BACKOFF":     "8s",
		"SIDECAR_QUIT_URL":      quit.url(),
	}))

	if code != exitFailed {
		t.Fatalf("exit code = %d, want %d (give-up must fail the Job)", code, exitFailed)
	}
	if quit.calls.Load() != 1 {
		t.Fatalf("sidecar quit calls = %d, want 1 on the give-up path", quit.calls.Load())
	}
	want := []time.Duration{time.Second, 2 * time.Second}
	if len(backoffs) != len(want) || backoffs[0] != want[0] || backoffs[1] != want[1] {
		t.Fatalf("backoffs = %v, want %v (3 attempts, doubling)", backoffs, want)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("give-up took %v; a refused connection must fail fast", elapsed)
	}
}

func TestSweepRetriesTransientStatusesThenSucceeds(t *testing.T) {
	srv, calls := sweepServer(t, http.StatusServiceUnavailable, http.StatusTooManyRequests, http.StatusOK)
	quit := newQuitServer(t)
	var backoffs []time.Duration

	code := testRunner(&backoffs).runMain(context.Background(), env(map[string]string{
		"SWEEP_URL": srv.URL, "SIDECAR_QUIT_URL": quit.url(),
	}))

	if code != exitOK {
		t.Fatalf("exit code = %d, want %d", code, exitOK)
	}
	if calls.Load() != 3 {
		t.Fatalf("sweep calls = %d, want 3 (503, 429, 200)", calls.Load())
	}
	if quit.calls.Load() != 1 {
		t.Fatalf("sidecar quit calls = %d, want 1", quit.calls.Load())
	}
}

// Every pass sends an Idempotency-Key; the retries of ONE pass reuse it (a
// lost response replays instead of sweeping twice) and the next pass mints a
// new one.
func TestSweepSendsOneIdempotencyKeyPerPassReusedAcrossRetries(t *testing.T) {
	var keys []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		if len(keys)%2 == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	var backoffs []time.Duration
	run := func() {
		t.Helper()
		if code := testRunner(&backoffs).runMain(context.Background(), env(map[string]string{
			"SWEEP_URL": srv.URL, "SIDECAR_QUIT_URL": "",
		})); code != exitOK {
			t.Fatalf("exit code = %d, want %d", code, exitOK)
		}
	}

	run()
	run()

	if len(keys) != 4 || keys[0] == "" || keys[0] != keys[1] || keys[2] != keys[3] || keys[0] == keys[2] {
		t.Fatalf("Idempotency-Keys = %q, want [A A B B] with A != B and none empty", keys)
	}
	if !validIdempotencyKey(keys[0]) {
		t.Fatalf("key %q is not 1-128 printable ASCII", keys[0])
	}
}

func validIdempotencyKey(k string) bool {
	if len(k) < 1 || len(k) > 128 {
		return false
	}
	for _, c := range []byte(k) {
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}

func TestSweepBackoffIsCappedAtMaxBackoff(t *testing.T) {
	srv, calls := sweepServer(t, http.StatusBadGateway)
	var backoffs []time.Duration

	code := testRunner(&backoffs).runMain(context.Background(), env(map[string]string{
		"SWEEP_URL": srv.URL, "SIDECAR_QUIT_URL": "",
		"SWEEP_MAX_ATTEMPTS": "5", "SWEEP_RETRY_BACKOFF": "3s", "SWEEP_MAX_BACKOFF": "5s",
	}))

	if code != exitFailed {
		t.Fatalf("exit code = %d, want %d", code, exitFailed)
	}
	if calls.Load() != 5 {
		t.Fatalf("sweep calls = %d, want 5", calls.Load())
	}
	want := []time.Duration{3 * time.Second, 5 * time.Second, 5 * time.Second, 5 * time.Second}
	for i := range want {
		if i >= len(backoffs) || backoffs[i] != want[i] {
			t.Fatalf("backoffs = %v, want %v", backoffs, want)
		}
	}
}

func TestSweepDoesNotRetryAClientErrorButStillQuits(t *testing.T) {
	srv, calls := sweepServer(t, http.StatusNotFound)
	quit := newQuitServer(t)
	var backoffs []time.Duration

	code := testRunner(&backoffs).runMain(context.Background(), env(map[string]string{
		"SWEEP_URL": srv.URL, "SIDECAR_QUIT_URL": quit.url(),
	}))

	if code != exitFailed {
		t.Fatalf("exit code = %d, want %d", code, exitFailed)
	}
	if calls.Load() != 1 {
		t.Fatalf("sweep calls = %d, want 1 (a 404 is not retried)", calls.Load())
	}
	if quit.calls.Load() != 1 {
		t.Fatalf("sidecar quit calls = %d, want 1", quit.calls.Load())
	}
}

// A server that accepts the connection and never answers is the shape of a
// rolling pod behind the mesh: the per-attempt timeout must cut it.
func TestSweepRequestTimeoutCutsAHangingServer(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(release); srv.Close() })
	quit := newQuitServer(t)
	var backoffs []time.Duration
	start := time.Now()

	code := testRunner(&backoffs).runMain(context.Background(), env(map[string]string{
		"SWEEP_URL": srv.URL, "SIDECAR_QUIT_URL": quit.url(),
		"SWEEP_MAX_ATTEMPTS": "2", "SWEEP_REQUEST_TIMEOUT": "100ms",
	}))

	if code != exitFailed {
		t.Fatalf("exit code = %d, want %d", code, exitFailed)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("hanging server held the sweep for %v; the request timeout must cut it", elapsed)
	}
	if quit.calls.Load() != 1 {
		t.Fatalf("sidecar quit calls = %d, want 1", quit.calls.Load())
	}
}

func TestSweepConfigErrorExitsTwoAndStillQuits(t *testing.T) {
	quit := newQuitServer(t)
	var backoffs []time.Duration

	code := testRunner(&backoffs).runMain(context.Background(), env(map[string]string{
		"SIDECAR_QUIT_URL": quit.url(),
	}))

	if code != exitConfig {
		t.Fatalf("exit code = %d, want %d", code, exitConfig)
	}
	if quit.calls.Load() != 1 {
		t.Fatalf("sidecar quit calls = %d, want 1 on the configuration-error path", quit.calls.Load())
	}
}

func TestSweepCancelledDuringBackoffGivesUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := runner{
		client: &http.Client{},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		sleep: func(ctx context.Context, d time.Duration) error {
			cancel()
			return sleepCtx(ctx, d)
		},
	}
	code := r.runMain(ctx, env(map[string]string{"SWEEP_URL": unreachableURL(t), "SIDECAR_QUIT_URL": ""}))
	if code != exitFailed {
		t.Fatalf("exit code = %d, want %d", code, exitFailed)
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{"SWEEP_URL": "http://svc/task-lease-expirations"}))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	want := config{
		URL: "http://svc/task-lease-expirations", RequestTimeout: 15 * time.Second, MaxAttempts: 4,
		InitialBackoff: time.Second, MaxBackoff: 8 * time.Second, QuitURL: "http://127.0.0.1:15020/quitquitquit",
	}
	if cfg != want {
		t.Fatalf("cfg = %+v, want %+v", cfg, want)
	}
}

func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	cases := map[string]map[string]string{
		"missing url":       {},
		"bad timeout":       {"SWEEP_URL": "http://x", "SWEEP_REQUEST_TIMEOUT": "soon"},
		"zero timeout":      {"SWEEP_URL": "http://x", "SWEEP_REQUEST_TIMEOUT": "0s"},
		"negative backoff":  {"SWEEP_URL": "http://x", "SWEEP_RETRY_BACKOFF": "-1s"},
		"bad max backoff":   {"SWEEP_URL": "http://x", "SWEEP_MAX_BACKOFF": "x"},
		"zero attempts":     {"SWEEP_URL": "http://x", "SWEEP_MAX_ATTEMPTS": "0"},
		"non-int attempts":  {"SWEEP_URL": "http://x", "SWEEP_MAX_ATTEMPTS": "two"},
		"empty url present": {"SWEEP_URL": ""},
	}
	for name, kv := range cases {
		if _, err := loadConfig(env(kv)); err == nil {
			t.Errorf("%s: loadConfig accepted %v", name, kv)
		}
	}
}

func TestLoadConfigEmptyQuitURLDisablesQuit(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{"SWEEP_URL": "http://x", "SIDECAR_QUIT_URL": "", "SWEEP_MAX_ATTEMPTS": "1"}))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.QuitURL != "" || cfg.MaxAttempts != 1 {
		t.Fatalf("cfg = %+v, want empty QuitURL and 1 attempt", cfg)
	}
}

func TestClassifyStatusBoundaries(t *testing.T) {
	cases := []struct {
		status    int
		ok, retry bool
	}{
		{199, false, false}, {200, true, false}, {204, true, false}, {299, true, false}, {300, false, false},
		{404, false, false}, {428, false, false}, {429, false, true}, {430, false, false},
		{499, false, false}, {500, false, true}, {503, false, true},
	}
	for _, c := range cases {
		err := classifyStatus(c.status, nil)
		if (err == nil) != c.ok {
			t.Errorf("status %d: ok = %v, want %v", c.status, err == nil, c.ok)
			continue
		}
		if err != nil && isRetryable(err) != c.retry {
			t.Errorf("status %d: retryable = %v, want %v", c.status, isRetryable(err), c.retry)
		}
	}
}

func isRetryable(err error) bool {
	return errors.Is(err, errRetryable) && !errors.Is(err, errRejected)
}
