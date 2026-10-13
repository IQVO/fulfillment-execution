package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

// freeAddr reserves a loopback port and releases it for the server to bind.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// warmUpServer builds the real cmd/execution HTTP server (router, /healthz,
// /readyz) over in-memory storage, bound to a free loopback address.
func warmUpServer(t *testing.T, readiness *inboundhttp.Readiness) *http.Server {
	t.Helper()
	logger := newLogger("error")
	storage := inMemoryStorage()
	publisher := events.NewLogPublisher(logger)
	clock := memory.SystemClock{}
	createTask := &usecases.CreateTask{Tasks: storage.taskRepo, Publisher: publisher, Clock: clock, NewId: newTaskId}
	srv := newHTTPServer(storage, publisher, clock, nil, outboundLookups{}, createTask, readiness, logger, freeAddr(t))
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

func statusOf(t *testing.T, url string) (int, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

// waitForStatus polls url until it answers want or the deadline passes.
func waitForStatus(t *testing.T, url string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last int
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = statusOf(t, url)
		if lastErr == nil && last == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("GET %s: want %d, last status %d err %v", url, want, last, lastErr)
}

// TestServeWhileWarmingUp_ListenerUpAndNotReadyUntilReplayCatchesUp pins gap
// fix D24 (ADR-0044): in Kafka catalogue mode the HTTP listener serves
// /healthz while the catalogue is still replaying, /readyz answers 503 until
// the replay has caught up, and 200 afterwards.
func TestServeWhileWarmingUp_ListenerUpAndNotReadyUntilReplayCatchesUp(t *testing.T) {
	readiness := &inboundhttp.Readiness{}
	srv := warmUpServer(t, readiness)
	base := "http://" + srv.Addr

	release := make(chan struct{})
	waitReady := func(ctx context.Context) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() { done <- serveWhileWarmingUp(srv, readiness, waitReady, 10*time.Second, newLogger("error")) }()

	waitForStatus(t, base+"/healthz", http.StatusOK)
	if got, err := statusOf(t, base+"/readyz"); err != nil || got != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz during the replay: status %d err %v, want 503", got, err)
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serveWhileWarmingUp: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serveWhileWarmingUp did not return after the replay caught up")
	}
	if got, err := statusOf(t, base+"/readyz"); err != nil || got != http.StatusOK {
		t.Fatalf("GET /readyz after the replay: status %d err %v, want 200", got, err)
	}
}

// TestServeWhileWarmingUp_ReplayTimeoutFailsTheBootAndStopsTheListener: a
// replay that does not catch up within the timeout is still a fatal boot
// error (as before D24), and the listener it started is shut down.
func TestServeWhileWarmingUp_ReplayTimeoutFailsTheBootAndStopsTheListener(t *testing.T) {
	readiness := &inboundhttp.Readiness{}
	srv := warmUpServer(t, readiness)
	base := "http://" + srv.Addr

	listening := make(chan struct{})
	waitReady := func(ctx context.Context) error {
		waitForStatus(t, base+"/healthz", http.StatusOK)
		close(listening)
		<-ctx.Done()
		return fmt.Errorf("replay: %w", ctx.Err())
	}
	err := serveWhileWarmingUp(srv, readiness, waitReady, 300*time.Millisecond, newLogger("error"))
	<-listening
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want a wrapped context.DeadlineExceeded", err)
	}
	if readiness.Ready() {
		t.Fatal("readiness must not report ready after a failed replay")
	}
	if _, err := statusOf(t, base+"/healthz"); err == nil {
		t.Fatal("the listener must be shut down after a failed replay")
	}
}

// TestServeWhileWarmingUp_FileModeIsReadyAtOnce: with no replay to wait for
// (file catalogue), the listener starts ready, exactly as before D24.
func TestServeWhileWarmingUp_FileModeIsReadyAtOnce(t *testing.T) {
	readiness := &inboundhttp.Readiness{}
	srv := warmUpServer(t, readiness)
	if err := serveWhileWarmingUp(srv, readiness, nil, time.Second, newLogger("error")); err != nil {
		t.Fatalf("serveWhileWarmingUp: %v", err)
	}
	if !readiness.Ready() {
		t.Fatal("file mode must be ready as soon as the listener starts")
	}
	waitForStatus(t, "http://"+srv.Addr+"/readyz", http.StatusOK)
}
