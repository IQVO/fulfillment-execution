package main

import (
	"net"
	"strings"
	"testing"
	"time"
)

// validEnv returns an env lookup holding a complete valid configuration,
// with overrides applied (an empty override removes the variable).
func validEnv(overrides map[string]string) func(string) string {
	env := map[string]string{
		"DATABASE_URL":  "postgres://u:***@db:5432/fe?sslmode=disable",
		"KAFKA_BROKERS": "kafka-1:9092,kafka-2:9092",
		"A2A_AGENT_URL": "http://fulfillment-execution-a2a.warehouse-systems.svc.cluster.local:8093/",
	}
	for k, v := range overrides {
		env[k] = v
	}
	return func(k string) string { return env[k] }
}

func TestLoadBootConfig_ValidEnvironment(t *testing.T) {
	cfg, err := loadBootConfig(validEnv(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.kafkaBrokers) != 2 || cfg.kafkaBrokers[0] != "kafka-1:9092" {
		t.Errorf("brokers = %v", cfg.kafkaBrokers)
	}
	if cfg.agentURL != "http://fulfillment-execution-a2a.warehouse-systems.svc.cluster.local:8093/" {
		t.Errorf("agentURL = %q", cfg.agentURL)
	}
}

// Every mandatory variable, and every rejected value, is a boot error that
// names the variable (the real-wiring ADR).
func TestLoadBootConfig_EveryProblemNamesItsVariable(t *testing.T) {
	cases := []struct {
		name string
		set  map[string]string
		want string
	}{
		{"no database", map[string]string{"DATABASE_URL": ""}, "DATABASE_URL"},
		{"no brokers", map[string]string{"KAFKA_BROKERS": ""}, "KAFKA_BROKERS"},
		{"blank brokers", map[string]string{"KAFKA_BROKERS": " , "}, "KAFKA_BROKERS"},
		{"no agent url", map[string]string{"A2A_AGENT_URL": ""}, "A2A_AGENT_URL is required"},
		{"blank agent url", map[string]string{"A2A_AGENT_URL": "   "}, "A2A_AGENT_URL is required"},
		{"relative agent url", map[string]string{"A2A_AGENT_URL": "/a2a"}, "A2A_AGENT_URL"},
		{"schemeless agent url", map[string]string{"A2A_AGENT_URL": "fulfillment-execution-a2a:8093"}, "A2A_AGENT_URL"},
		{"non-http agent url", map[string]string{"A2A_AGENT_URL": "ftp://host/"}, "A2A_AGENT_URL"},
		{"hostless agent url", map[string]string{"A2A_AGENT_URL": "http:///a2a"}, "A2A_AGENT_URL"},
		{"log publisher", map[string]string{"EVENT_PUBLISHER": "log"}, "EVENT_PUBLISHER=log is not supported"},
		{"unknown publisher", map[string]string{"EVENT_PUBLISHER": "rabbit"}, `unknown EVENT_PUBLISHER "rabbit"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadBootConfig(validEnv(tc.set))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestLoadBootConfig_KafkaPublisherSpellingsAreAccepted(t *testing.T) {
	for _, v := range []string{"", "kafka", "KAFKA"} {
		if _, err := loadBootConfig(validEnv(map[string]string{"EVENT_PUBLISHER": v})); err != nil {
			t.Errorf("EVENT_PUBLISHER=%q: unexpected error %v", v, err)
		}
	}
}

func TestLoadBootConfig_ReportsEveryProblemAtOnce(t *testing.T) {
	_, err := loadBootConfig(func(string) string { return "" })
	for _, name := range []string{"DATABASE_URL", "KAFKA_BROKERS", "A2A_AGENT_URL"} {
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Fatalf("err = %v, want %s named", err, name)
		}
	}
}

// run() refuses a misconfigured environment BEFORE it binds the listener.
func TestRun_MisconfigurationIsRefusedBeforeTheListenerBinds(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	for _, name := range []string{"DATABASE_URL", "KAFKA_BROKERS", "EVENT_PUBLISHER", "A2A_AGENT_URL"} {
		t.Setenv(name, "")
	}
	t.Setenv("A2A_ADDR", addr)

	err = run()
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("run() = %v, want the DATABASE_URL boot error", err)
	}
	if conn, derr := net.DialTimeout("tcp", addr, 300*time.Millisecond); derr == nil {
		_ = conn.Close()
		t.Fatalf("something listens on %s after a refused boot", addr)
	}
}

// A relative or non-http A2A_AGENT_URL must be refused even when everything
// else is valid: the Agent Card would advertise an endpoint nobody can dial.
func TestRun_AgentURLMisconfigurationIsRefusedBeforeTheListenerBinds(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	t.Setenv("DATABASE_URL", "postgres://u:***@127.0.0.1:1/db?sslmode=disable")
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:1")
	t.Setenv("EVENT_PUBLISHER", "")
	t.Setenv("A2A_AGENT_URL", "not-a-url")
	t.Setenv("A2A_ADDR", addr)

	err = run()
	if err == nil || !strings.Contains(err.Error(), "A2A_AGENT_URL") {
		t.Fatalf("run() = %v, want the A2A_AGENT_URL boot error", err)
	}
	if conn, derr := net.DialTimeout("tcp", addr, 300*time.Millisecond); derr == nil {
		_ = conn.Close()
		t.Fatalf("something listens on %s after a refused boot", addr)
	}
}
