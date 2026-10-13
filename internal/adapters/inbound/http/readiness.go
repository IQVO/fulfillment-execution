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
//
// Two independent conditions make it not ready (gap fix D24, ADR-0044):
// the boot-time warm-up (StartWarmUp until FinishWarmUp: in Kafka
// catalogue mode the listener is up while the process-path catalogue is
// still replaying) and shutdown (SetNotReady, never undone). Finishing the
// warm-up after shutdown began therefore never makes the pod ready again.
type Readiness struct {
	// notReady is 0 (ready) or 1 (not ready), holding the atomic
	// int32 gate itself. int32 rather than atomic.Bool for build-tag
	// parity with this module's Go version floor; the semantics are
	// identical.
	notReady int32
	// warmingUp is 1 between StartWarmUp and FinishWarmUp.
	warmingUp int32
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
	return atomic.LoadInt32(&g.notReady) == 0 && atomic.LoadInt32(&g.warmingUp) == 0
}

// GetReadyz handles GET /readyz: 200 {"status":"ready"} while the gate
// is ready, and 503 {"status":"not_ready"} during the boot warm-up and once
// SetNotReady has been called — a nil Readiness on Handlers (the zero value; every
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
