package main

import (
	"errors"
	"net"
	"testing"
	"time"
)

func TestParseBrokers(t *testing.T) {
	cases := map[string]int{"": 0, " , ": 0, "a:9092": 1, "a:9092, b:9092,": 2}
	for raw, want := range cases {
		if got := parseBrokers(raw); len(got) != want {
			t.Errorf("parseBrokers(%q) = %v, want %d broker(s)", raw, got, want)
		}
	}
}

// KAFKA_BROKERS has no localhost default: a projector without it refuses to
// boot, naming the variable, before it opens any connection or binds the
// admin listener.
func TestRun_WithoutBrokersIsABootError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	t.Setenv("ANALYTICS_DATABASE_URL", "postgres://u:***@127.0.0.1:1/a?sslmode=disable")
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("ADMIN_ADDR", addr)
	if err := run(); !errors.Is(err, errMissingBrokers) {
		t.Fatalf("run() = %v, want errMissingBrokers", err)
	}
	if conn, derr := net.DialTimeout("tcp", addr, 300*time.Millisecond); derr == nil {
		_ = conn.Close()
		t.Fatalf("something listens on %s after a refused boot", addr)
	}
}
