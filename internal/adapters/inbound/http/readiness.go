package http

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Dependency is one thing /readyz needs to be true: a name for the response
// and the check itself (Postgres ping, Kafka broker metadata).
type Dependency struct {
	Name  string
	Check func(ctx context.Context) error
}

// Defaults of the dependency checks.
const (
	// DefaultReadinessTTL is how long a dependency verdict is reused.
	DefaultReadinessTTL = 3 * time.Second
	// DefaultReadinessTimeout bounds one round of dependency checks.
	DefaultReadinessTimeout = 2 * time.Second
)

// Readiness is a process-wide, thread-safe readiness gate (ADR-0029
// §graceful shutdown, grown by the real-wiring ADR): GET /readyz reflects it,
// separately from /healthz (liveness — "is the process alive", never flipped
// by shutdown) so a Kubernetes readinessProbe can be pointed at /readyz and a
// livenessProbe/startupProbe can stay pointed at /healthz (see
// charts/fulfillment-execution/values.yaml).
//
// It has three parts. The drain gate: graceful shutdown flips it FIRST
// (SetNotReady), so a readinessProbe stops routing new traffic before the
// listener closes. The warm-up (gap fix D24, ADR-0044): in Kafka catalogue
// mode the listener is up while the process-path catalogue is still
// replaying (StartWarmUp until FinishWarmUp); finishing the warm-up after
// shutdown began never makes the pod ready again. The dependency checks
// (NewReadiness): the process is not ready while Postgres or Kafka is not
// reachable; their verdict is cached for a few seconds so probes cannot
// hammer either of them.
//
// The zero value is READY with no dependency to check (a unit-test fixture;
// every cmd/ binary builds the real gate with NewReadiness).
type Readiness struct {
	// notReady is 0 (ready) or 1 (not ready), holding the atomic
	// int32 gate itself.
	notReady int32
	// warmingUp is 1 between StartWarmUp and FinishWarmUp.
	warmingUp int32

	deps    []Dependency
	ttl     time.Duration
	timeout time.Duration
	now     func() time.Time

	mu        sync.Mutex
	checkedAt time.Time
	failing   []string
}

// NewReadiness builds a gate that also requires every dependency to pass. A
// verdict (pass or fail) is reused for ttl; one round of checks is bounded by
// timeout. Non-positive values fall back to the defaults.
func NewReadiness(ttl, timeout time.Duration, deps ...Dependency) *Readiness {
	if ttl <= 0 {
		ttl = DefaultReadinessTTL
	}
	if timeout <= 0 {
		timeout = DefaultReadinessTimeout
	}
	return &Readiness{deps: deps, ttl: ttl, timeout: timeout, now: time.Now}
}

// StartWarmUp holds the gate not-ready until FinishWarmUp: the HTTP
// listener (and so /healthz for the startup and liveness probes) can serve
// while a boot step that must finish before real traffic, the Kafka
// process-path catalogue replay, is still running (ADR-0044).
func (g *Readiness) StartWarmUp() {
	if g == nil {
		return
	}
	atomic.StoreInt32(&g.warmingUp, 1)
}

// FinishWarmUp ends the warm-up. The gate reports ready again only if
// shutdown has not begun in the meantime.
func (g *Readiness) FinishWarmUp() {
	if g == nil {
		return
	}
	atomic.StoreInt32(&g.warmingUp, 0)
}

// SetNotReady flips the drain gate to not-ready. This is the FIRST step of
// graceful shutdown (ADR-0029): flip readiness before touching the HTTP
// server, any consumer or the relay, so a Kubernetes readinessProbe can
// observe the flip and stop routing new traffic during the drain window that
// follows, before Shutdown ever closes a listener.
func (g *Readiness) SetNotReady() {
	if g == nil {
		return
	}
	atomic.StoreInt32(&g.notReady, 1)
}

// Ready reports whether the drain gate and the warm-up currently say ready
// (nil = ready). It does not run the dependency checks; see Failing.
func (g *Readiness) Ready() bool {
	if g == nil {
		return true
	}
	return atomic.LoadInt32(&g.notReady) == 0 && atomic.LoadInt32(&g.warmingUp) == 0
}

// Failing returns the names of the dependencies that are not reachable, nil
// when all are (or there is none). Within the ttl it answers from the cache.
// The check runs on a context detached from the caller's cancellation, so a
// client that hangs up cannot poison the cached verdict.
func (g *Readiness) Failing(ctx context.Context) []string {
	if g == nil || len(g.deps) == 0 {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.checkedAt.IsZero() && g.now().Sub(g.checkedAt) < g.ttl {
		return append([]string(nil), g.failing...)
	}
	checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.timeout)
	defer cancel()
	var failing []string
	for _, d := range g.deps {
		if err := d.Check(checkCtx); err != nil {
			slog.WarnContext(ctx, "readiness: dependency is not ready", "dependency", d.Name, "error", err)
			failing = append(failing, d.Name)
		}
	}
	g.failing, g.checkedAt = failing, g.now()
	return append([]string(nil), failing...)
}

// GetReadyz handles GET /readyz: 200 {"status":"ready"} while ready; 503
// {"status":"not_ready"} during the boot warm-up and once SetNotReady has
// been called (draining); 503 problem+json (dependency-unavailable, naming
// the dependencies) while Postgres or Kafka is not reachable. A nil
// Readiness on Handlers (the zero value) always reports ready.
func (h *Handlers) GetReadyz(w http.ResponseWriter, r *http.Request) {
	if !h.Readiness.Ready() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	if failing := h.Readiness.Failing(r.Context()); len(failing) > 0 {
		writeProblem(w, http.StatusServiceUnavailable, "dependency-unavailable", "A runtime dependency is unavailable",
			"not ready: "+strings.Join(failing, ", "), r.URL.Path)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
