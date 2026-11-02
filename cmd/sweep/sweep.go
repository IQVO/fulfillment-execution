package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// Exit codes of the sweep entrypoint. Any non-zero code fails the Job, so a
// sweep that could not run is visible as a failed Job (with logs) instead of
// a pod that hangs until activeDeadlineSeconds (ADR 0048).
const (
	exitOK     = 0
	exitFailed = 1
	exitConfig = 2
)

// Defaults: the worst case (attempts x timeout + the capped backoffs between
// them) must stay well inside the Job's activeDeadlineSeconds (120 s in the
// chart): 4 x 15 s + 3 x 8 s = 84 s. The chart enforces the same budget at
// render time.
const (
	defaultRequestTimeout = 15 * time.Second
	defaultMaxAttempts    = 4
	defaultInitialBackoff = 1 * time.Second
	defaultMaxBackoff     = 8 * time.Second
	defaultQuitURL        = "http://127.0.0.1:15020/quitquitquit"
	quitTimeout           = 2 * time.Second
	// maxBodyLog bounds how much of a response body is read for the log.
	maxBodyLog = 512
)

var (
	// errGaveUp: every attempt failed with a retryable error.
	errGaveUp = errors.New("sweep: gave up")
	// errRejected: the API answered with a non-retryable status (4xx other
	// than 429); retrying the same POST can not succeed.
	errRejected = errors.New("sweep: rejected")
	// errRetryable marks one failed attempt that may succeed on a retry
	// (transport error, timeout, 429, 5xx).
	errRetryable = errors.New("sweep: retryable failure")
)

// config is the sweep entrypoint's configuration, read from the environment
// the chart renders (charts/fulfillment-execution/templates/sweeps-cronjob.yaml).
type config struct {
	// URL is the sweep endpoint to POST (SWEEP_URL, required).
	URL string
	// RequestTimeout bounds ONE attempt end to end: connect, request and
	// response (SWEEP_REQUEST_TIMEOUT, Go duration).
	RequestTimeout time.Duration
	// MaxAttempts bounds the attempts (SWEEP_MAX_ATTEMPTS).
	MaxAttempts int
	// InitialBackoff is the wait after the first failure; it doubles per
	// attempt up to MaxBackoff (SWEEP_RETRY_BACKOFF, SWEEP_MAX_BACKOFF).
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	// QuitURL is the Istio sidecar's quit endpoint, POSTed on EVERY exit
	// path (SIDECAR_QUIT_URL; set it empty to disable, e.g. outside a mesh).
	QuitURL string
}

// lookupFunc is os.LookupEnv's shape, so tests pass a map.
type lookupFunc func(string) (string, bool)

// loadConfig reads the configuration. QuitURL is resolved FIRST, so even a
// configuration error still knows where to send the sidecar quit.
func loadConfig(lookup lookupFunc) (config, error) {
	cfg := config{
		QuitURL:        defaultQuitURL,
		RequestTimeout: defaultRequestTimeout,
		MaxAttempts:    defaultMaxAttempts,
		InitialBackoff: defaultInitialBackoff,
		MaxBackoff:     defaultMaxBackoff,
	}
	if v, ok := lookup("SIDECAR_QUIT_URL"); ok {
		cfg.QuitURL = v
	}
	url, _ := lookup("SWEEP_URL")
	if url == "" {
		return cfg, errors.New("SWEEP_URL is required")
	}
	cfg.URL = url
	if err := readDuration(lookup, "SWEEP_REQUEST_TIMEOUT", &cfg.RequestTimeout); err != nil {
		return cfg, err
	}
	if err := readDuration(lookup, "SWEEP_RETRY_BACKOFF", &cfg.InitialBackoff); err != nil {
		return cfg, err
	}
	if err := readDuration(lookup, "SWEEP_MAX_BACKOFF", &cfg.MaxBackoff); err != nil {
		return cfg, err
	}
	return cfg, readAttempts(lookup, &cfg.MaxAttempts)
}

// readDuration parses an optional positive Go duration into dst.
func readDuration(lookup lookupFunc, key string, dst *time.Duration) error {
	v, ok := lookup(key)
	if !ok || v == "" {
		return nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fmt.Errorf("%s must be a positive Go duration, got %q", key, v)
	}
	*dst = d
	return nil
}

// readAttempts parses the optional SWEEP_MAX_ATTEMPTS (>= 1).
func readAttempts(lookup lookupFunc, dst *int) error {
	v, ok := lookup("SWEEP_MAX_ATTEMPTS")
	if !ok || v == "" {
		return nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return fmt.Errorf("SWEEP_MAX_ATTEMPTS must be an integer >= 1, got %q", v)
	}
	*dst = n
	return nil
}

// sleepFunc waits d or until ctx ends; tests replace it to run instantly.
type sleepFunc func(ctx context.Context, d time.Duration) error

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// runner is one sweep invocation's collaborators.
type runner struct {
	client *http.Client
	logger *slog.Logger
	sleep  sleepFunc
}

// runMain is the whole entrypoint: load the config, trigger the sweep with
// bounded retries, then quit the sidecar — on EVERY exit path, success,
// give-up, rejection and configuration error alike — and return the exit
// code.
func (r runner) runMain(ctx context.Context, lookup lookupFunc) int {
	cfg, err := loadConfig(lookup)
	code := exitOK
	if err != nil {
		r.logger.ErrorContext(ctx, "sweep: invalid configuration", "error", err)
		code = exitConfig
	} else if err := r.trigger(ctx, cfg); err != nil {
		r.logger.ErrorContext(ctx, "sweep: failed; exiting non-zero so the Job fails fast", "url", cfg.URL, "error", err)
		code = exitFailed
	}
	r.quitSidecar(ctx, cfg.QuitURL)
	return code
}

// trigger POSTs cfg.URL up to cfg.MaxAttempts times with exponential backoff
// (capped at cfg.MaxBackoff). A non-retryable rejection stops at once.
func (r runner) trigger(ctx context.Context, cfg config) error {
	// One Idempotency-Key per sweep pass: every retry of THIS pass reuses it
	// (so a lost response replays the stored answer instead of sweeping twice),
	// the next scheduled pass mints a new one.
	key, err := newIdempotencyKey()
	if err != nil {
		return fmt.Errorf("%w: idempotency key: %v", errRejected, err)
	}
	backoff := cfg.InitialBackoff
	var last error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		err := r.attempt(ctx, cfg, key)
		if err == nil {
			r.logger.InfoContext(ctx, "sweep: done", "url", cfg.URL, "attempt", attempt)
			return nil
		}
		if errors.Is(err, errRejected) {
			return err
		}
		last = err
		r.logger.WarnContext(ctx, "sweep: attempt failed", "url", cfg.URL, "attempt", attempt,
			"max_attempts", cfg.MaxAttempts, "error", err)
		if attempt == cfg.MaxAttempts {
			break
		}
		if err := r.sleep(ctx, backoff); err != nil {
			return fmt.Errorf("%w: interrupted while backing off: %v (last error: %v)", errGaveUp, err, last)
		}
		backoff = min(backoff*2, cfg.MaxBackoff)
	}
	return fmt.Errorf("%w after %d attempts: %w", errGaveUp, cfg.MaxAttempts, last)
}

// attempt performs ONE POST bounded by cfg.RequestTimeout and classifies the
// outcome: nil (2xx), errRetryable (transport error, timeout, 429, 5xx) or
// errRejected (any other status).
func (r runner) attempt(ctx context.Context, cfg config, key string) error {
	ctx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, http.NoBody)
	if err != nil {
		return fmt.Errorf("%w: build request: %v", errRejected, err)
	}
	req.Header.Set("Idempotency-Key", key)
	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", errRetryable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyLog))
	return classifyStatus(resp.StatusCode, body)
}

// newIdempotencyKey returns a random RFC 4122 version-4 UUID: 36 printable
// ASCII characters, inside the API's 1-128 Idempotency-Key limit.
func newIdempotencyKey() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

// classifyStatus maps an HTTP status to the attempt outcome.
func classifyStatus(status int, body []byte) error {
	if status >= 200 && status < 300 {
		return nil
	}
	if status == http.StatusTooManyRequests || status >= 500 {
		return fmt.Errorf("%w: HTTP %d: %s", errRetryable, status, body)
	}
	return fmt.Errorf("%w: HTTP %d: %s", errRejected, status, body)
}

// quitSidecar asks the Istio sidecar to exit (best effort, bounded by
// quitTimeout) so the pod can complete. An empty url disables it; a refused
// connection (no sidecar: local run, a namespace without injection) is
// logged at DEBUG and ignored.
func (r runner) quitSidecar(ctx context.Context, url string) {
	if url == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, quitTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, http.NoBody)
	if err != nil {
		r.logger.WarnContext(ctx, "sweep: invalid sidecar quit URL", "url", url, "error", err)
		return
	}
	resp, err := r.client.Do(req)
	if err != nil {
		r.logger.DebugContext(ctx, "sweep: sidecar quit not delivered (no sidecar?)", "url", url, "error", err)
		return
	}
	_ = resp.Body.Close()
	r.logger.InfoContext(ctx, "sweep: sidecar quit requested", "url", url, "status", resp.StatusCode)
}
