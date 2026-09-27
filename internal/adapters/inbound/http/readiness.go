package http

import (
	"net/http"
	"sync/atomic"
)

// Readiness is a process-wide, thread-safe readiness gate (ADR-0029
// §graceful shutdown): GET /readyz reflects it, separately from
// /healthz (liveness — "is the process alive", never flipped by
// shutdown) so a Kubernetes readinessProbe can be pointed at /readyz
// and a livenessProbe/startupProbe can stay pointed at /healthz exactly
// as they are today (see charts/fulfillment-execution/values.yaml).
// Mirrors order-management's own internal/adapters/inbound/http.Readiness
// exactly (its ADR-0025), the fleet reference for this phase.
//
// The zero value is READY — a Handlers built without explicitly wiring
// a Readiness (every existing test, and any caller that predates this
// type) behaves exactly as before, with /readyz always reporting ready.
type Readiness struct {
	// notReady is 0 (ready) or 1 (not ready), holding the atomic
	// int32 gate itself. int32 rather than atomic.Bool for build-tag
	// parity with this module's Go version floor; the semantics are
	// identical.
	notReady int32
}

// SetNotReady flips the gate to not-ready. This is the FIRST step of
// graceful shutdown (ADR-0029): flip readiness before touching the HTTP
// server or any consumer, so a Kubernetes readinessProbe can observe
// the flip and stop routing new traffic during the drain window that
// follows, before Shutdown ever closes a listener.
func (g *Readiness) SetNotReady() {
	if g == nil {
		return
	}
	atomic.StoreInt32(&g.notReady, 1)
}

// Ready reports whether the gate currently says ready.
func (g *Readiness) Ready() bool {
	if g == nil {
		return true
	}
	return atomic.LoadInt32(&g.notReady) == 0
}

// GetReadyz handles GET /readyz: 200 {"status":"ready"} while the gate
// is ready, and 503 {"status":"not_ready"} once SetNotReady has been
// called — a nil Readiness on Handlers (the zero value; every
// pre-existing test and caller) always reports ready, matching this
// handler's pre-graceful-shutdown behavior for any caller that has not
// wired one.
func (h *Handlers) GetReadyz(w http.ResponseWriter, _ *http.Request) {
	if !h.Readiness.Ready() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
