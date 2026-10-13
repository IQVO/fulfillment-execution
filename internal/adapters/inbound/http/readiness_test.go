package http

import (
	"net/http/httptest"
	"testing"
)

func TestReadiness_ZeroValueIsReady(t *testing.T) {
	var g Readiness
	if !g.Ready() {
		t.Fatal("zero-value Readiness must report ready")
	}
}

func TestReadiness_NilIsReady(t *testing.T) {
	var g *Readiness
	if !g.Ready() {
		t.Fatal("a nil *Readiness must report ready (pre-existing callers/tests never wire one)")
	}
	// Must not panic.
	g.SetNotReady()
}

func TestReadiness_SetNotReadyFlipsIt(t *testing.T) {
	var g Readiness
	g.SetNotReady()
	if g.Ready() {
		t.Fatal("Ready() must report false after SetNotReady")
	}
}

func TestGetReadyz_NilReadinessOnHandlers_Returns200(t *testing.T) {
	h := &Handlers{}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)

	h.GetReadyz(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200 (nil Readiness on Handlers means always ready)", rec.Code)
	}
}

func TestGetReadyz_NotReady_Returns503(t *testing.T) {
	readiness := &Readiness{}
	readiness.SetNotReady()
	h := &Handlers{Readiness: readiness}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)

	h.GetReadyz(rec, req)

	if rec.Code != 503 {
		t.Fatalf("status = %d, want 503 after SetNotReady", rec.Code)
	}
}

func TestGetReadyz_Ready_Returns200(t *testing.T) {
	h := &Handlers{Readiness: &Readiness{}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)

	h.GetReadyz(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200 before SetNotReady is ever called", rec.Code)
	}
}

// Gap fix D24 (ADR-0044): in Kafka catalogue mode the listener starts before
// the catalogue replay, with readiness held false until the replay has
// caught up.
func TestReadiness_WarmUpHoldsNotReadyUntilFinished(t *testing.T) {
	var g Readiness
	g.StartWarmUp()
	if g.Ready() {
		t.Fatal("Ready() must report false while warming up")
	}
	g.FinishWarmUp()
	if !g.Ready() {
		t.Fatal("Ready() must report true once the warm-up has finished")
	}
}

// A SIGTERM during warm-up must win: finishing the warm-up after shutdown
// began never makes the pod ready again.
func TestReadiness_FinishWarmUpDoesNotUndoShutdown(t *testing.T) {
	var g Readiness
	g.StartWarmUp()
	g.SetNotReady()
	g.FinishWarmUp()
	if g.Ready() {
		t.Fatal("Ready() must stay false: shutdown began before the warm-up finished")
	}
}

func TestReadiness_NilWarmUpIsANoOp(t *testing.T) {
	var g *Readiness
	g.StartWarmUp()
	g.FinishWarmUp()
	if !g.Ready() {
		t.Fatal("a nil *Readiness must keep reporting ready")
	}
}

func TestGetReadyz_WarmingUp_Returns503(t *testing.T) {
	readiness := &Readiness{}
	readiness.StartWarmUp()
	h := &Handlers{Readiness: readiness}
	rec := httptest.NewRecorder()
	h.GetReadyz(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != 503 {
		t.Fatalf("status = %d, want 503 while warming up", rec.Code)
	}
}
