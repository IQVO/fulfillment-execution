package main

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
)

// ADR-0057: a background loop (outbox relay, Kafka consumer) that returns
// while the service is meant to run ends the process; a stop the service
// asked for is clean. These tests pin that rule on watchDone (the classifier)
// and on serveUntilSignal (what run() returns).

func recvName(ch <-chan string) (string, bool) {
	select {
	case n := <-ch:
		return n, true
	case <-time.After(300 * time.Millisecond):
		return "", false
	}
}

func TestWatchDone(t *testing.T) {
	closed := func() <-chan struct{} { c := make(chan struct{}); close(c); return c }
	open := func() <-chan struct{} { return make(chan struct{}) }
	cases := []struct {
		name  string
		loops []namedDone
		want  string // "" = nothing is reported
	}{
		{"a loop that returned is a death", []namedDone{{"pack level consumer", closed()}}, "pack level consumer"},
		{"loops still running report nothing", []namedDone{{"a", open()}, {"b", open()}}, ""},
		{"a nil done channel is not watched", []namedDone{{"file-mode catalogue", nil}}, ""},
		{"only the loop that returned is named", []namedDone{{"a", open()}, {"interleaving hint consumer", closed()}}, "interleaving hint consumer"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, reported := recvName(watchDone(tc.loops...))
			if got != tc.want || reported != (tc.want != "") {
				t.Fatalf("watchDone reported (%q, %v), want %q", got, reported, tc.want)
			}
		})
	}
}

// Several loops dying together must not block each other: nobody reads the
// channel until all of them have returned, and every name is still delivered.
func TestWatchDone_SeveralDeathsNeverBlock(t *testing.T) {
	names := []string{"a", "b", "c", "d"}
	loops := make([]namedDone, 0, len(names))
	for _, n := range names {
		c := make(chan struct{})
		close(c)
		loops = append(loops, namedDone{n, c})
	}
	died := watchDone(loops...)
	time.Sleep(50 * time.Millisecond) // let every watcher send before the first read
	seen := map[string]bool{}
	for range names {
		n, ok := recvName(died)
		if !ok {
			t.Fatalf("only %d of %d deaths delivered: a watcher blocked", len(seen), len(names))
		}
		seen[n] = true
	}
	if len(seen) != len(names) {
		t.Fatalf("deaths delivered = %v, want all of %v", seen, names)
	}
}

// shutdownRig is serveUntilSignal's collaborators as test doubles. Closing the
// consumer/relay done channels is what the real stop funcs cause.
type shutdownRig struct {
	relayDone, consumerDone chan struct{}
	relayStopped            chan struct{}
	consumerCancelled       chan struct{}
	once                    [2]sync.Once
}

func newShutdownRig() *shutdownRig {
	return &shutdownRig{
		relayDone: make(chan struct{}), consumerDone: make(chan struct{}),
		relayStopped: make(chan struct{}), consumerCancelled: make(chan struct{}),
	}
}

func (r *shutdownRig) stopRelay() {
	r.once[0].Do(func() { close(r.relayStopped); close(r.relayDone) })
}

func (r *shutdownRig) cancelConsumer() {
	r.once[1].Do(func() { close(r.consumerCancelled); close(r.consumerDone) })
}

func (r *shutdownRig) serve(ctx context.Context, loops ...namedDone) error {
	return serveUntilSignal(ctx, quietLogger(), &http.Server{}, inboundhttp.NewReadiness(time.Second, time.Second),
		r.stopRelay, r.relayDone, r.cancelConsumer, r.consumerDone, loops...)
}

func serveResult(t *testing.T, serve func() error) error {
	t.Helper()
	res := make(chan error, 1)
	go func() { res <- serve() }()
	select {
	case err := <-res:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("serveUntilSignal did not return")
		return nil
	}
}

func wasClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// Any one of the background loops dying makes serveUntilSignal return an error
// naming it (so run() exits non-zero and the kubelet restarts the pod), after
// the usual graceful stop of the relay and the consumers.
func TestServeUntilSignal_ADyingLoopEndsTheProcess(t *testing.T) {
	for _, name := range []string{
		"outbox relay", "WorkReleased consumer", "product classification consumer",
		"interleaving hint consumer", "pack level consumer", "process-path catalogue consumer",
	} {
		t.Run(name, func(t *testing.T) {
			rig := newShutdownRig()
			dying := make(chan struct{})
			loops := []namedDone{{"unrelated loop", make(chan struct{})}, {name, dying}}
			close(dying)
			err := serveResult(t, func() error { return rig.serve(context.Background(), loops...) })
			if err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("serveUntilSignal error = %v, want one naming %q", err, name)
			}
			if !wasClosed(rig.relayStopped) || !wasClosed(rig.consumerCancelled) {
				t.Fatal("a death must still run the graceful stop of the relay and the consumers")
			}
		})
	}
}

// A shutdown the service asked for (ctx cancelled: SIGTERM, or the integration
// test stopping run()) is clean even though every loop returns as a result of
// it. Loops return only because stopRelay/cancelConsumer were called, so they
// must never be mistaken for deaths (the network-fulfillment shutdown race).
func TestServeUntilSignal_ARequestedShutdownIsClean(t *testing.T) {
	rig := newShutdownRig()
	ctx, cancel := context.WithCancel(context.Background())
	loops := []namedDone{{"outbox relay", rig.relayDone}, {"pack level consumer", rig.consumerDone}}
	cancel()
	if err := serveResult(t, func() error { return rig.serve(ctx, loops...) }); err != nil {
		t.Fatalf("a requested shutdown returned %v, want nil", err)
	}
	if !wasClosed(rig.relayStopped) || !wasClosed(rig.consumerCancelled) {
		t.Fatal("a requested shutdown must stop the relay and the consumers")
	}
}
