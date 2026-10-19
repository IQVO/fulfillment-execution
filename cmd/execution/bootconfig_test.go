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
		"DATABASE_URL":                          "postgres://u:***@db:5432/fe?sslmode=disable",
		"KAFKA_BROKERS":                         "kafka-1:9092,kafka-2:9092",
		"WORK_RELEASED_CONSUMER_GROUP":          "fulfillment-execution",
		"PRODUCT_CLASSIFICATION_CONSUMER_GROUP": "fulfillment-execution-product-classification",
		"TASK_INTERLEAVING_CONSUMER_GROUP":      "fulfillment-execution-interleaving",
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
	if len(cfg.kafkaBrokers) != 2 || cfg.kafkaBrokers[1] != "kafka-2:9092" {
		t.Errorf("brokers = %v", cfg.kafkaBrokers)
	}
	if cfg.workReleasedGroup != "fulfillment-execution" ||
		cfg.classification.consumerGroup != "fulfillment-execution-product-classification" ||
		cfg.interleaving.consumerGroup != "fulfillment-execution-interleaving" {
		t.Errorf("groups = %q / %q / %q", cfg.workReleasedGroup, cfg.classification.consumerGroup, cfg.interleaving.consumerGroup)
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
		{"no work-released group", map[string]string{"WORK_RELEASED_CONSUMER_GROUP": ""}, "WORK_RELEASED_CONSUMER_GROUP"},
		{"blank work-released group", map[string]string{"WORK_RELEASED_CONSUMER_GROUP": "  "}, "WORK_RELEASED_CONSUMER_GROUP"},
		{"no classification group", map[string]string{"PRODUCT_CLASSIFICATION_CONSUMER_GROUP": ""}, "PRODUCT_CLASSIFICATION_CONSUMER_GROUP"},
		{"no interleaving group", map[string]string{"TASK_INTERLEAVING_CONSUMER_GROUP": ""}, "TASK_INTERLEAVING_CONSUMER_GROUP"},
		{"classification permissive", map[string]string{"PRODUCT_CLASSIFICATION_MODE": "permissive"}, "PRODUCT_CLASSIFICATION_MODE=permissive is not supported"},
		{"classification PERMISSIVE", map[string]string{"PRODUCT_CLASSIFICATION_MODE": "PERMISSIVE"}, "PRODUCT_CLASSIFICATION_MODE=permissive is not supported"},
		{"classification off", map[string]string{"PRODUCT_CLASSIFICATION_MODE": "off"}, "PRODUCT_CLASSIFICATION_MODE=off is not supported"},
		{"classification http", map[string]string{"PRODUCT_CLASSIFICATION_MODE": "http"}, "PRODUCT_CLASSIFICATION_MODE=http was removed"},
		{"classification unknown", map[string]string{"PRODUCT_CLASSIFICATION_MODE": "grpc"}, `unknown PRODUCT_CLASSIFICATION_MODE "grpc"`},
		{"interleaving permissive", map[string]string{"TASK_INTERLEAVING_MODE": "permissive"}, "TASK_INTERLEAVING_MODE=permissive is not supported"},
		{"interleaving off", map[string]string{"TASK_INTERLEAVING_MODE": "off"}, "TASK_INTERLEAVING_MODE=off is not supported"},
		{"interleaving unknown", map[string]string{"TASK_INTERLEAVING_MODE": "grpc"}, `unknown TASK_INTERLEAVING_MODE "grpc"`},
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

func TestLoadBootConfig_AcceptedSpellings(t *testing.T) {
	for _, set := range []map[string]string{
		{"EVENT_PUBLISHER": "kafka"},
		{"EVENT_PUBLISHER": "KAFKA"},
		{"PRODUCT_CLASSIFICATION_MODE": "kafka"},
		{"PRODUCT_CLASSIFICATION_MODE": " Kafka "},
		{"TASK_INTERLEAVING_MODE": "kafka"},
	} {
		if _, err := loadBootConfig(validEnv(set)); err != nil {
			t.Errorf("%v: unexpected error %v", set, err)
		}
	}
}

// All problems are reported together so an operator fixes them in one go.
func TestLoadBootConfig_ReportsEveryProblemAtOnce(t *testing.T) {
	_, err := loadBootConfig(func(string) string { return "" })
	if err == nil {
		t.Fatal("an empty environment must be refused")
	}
	for _, name := range []string{"DATABASE_URL", "KAFKA_BROKERS", "WORK_RELEASED_CONSUMER_GROUP", "PRODUCT_CLASSIFICATION_CONSUMER_GROUP", "TASK_INTERLEAVING_CONSUMER_GROUP"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error does not name %s: %v", name, err)
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

	for _, name := range []string{"DATABASE_URL", "KAFKA_BROKERS", "WORK_RELEASED_CONSUMER_GROUP", "PRODUCT_CLASSIFICATION_CONSUMER_GROUP", "TASK_INTERLEAVING_CONSUMER_GROUP", "EVENT_PUBLISHER", "PRODUCT_CLASSIFICATION_MODE", "TASK_INTERLEAVING_MODE"} {
		t.Setenv(name, "")
	}
	t.Setenv("HTTP_ADDR", addr)

	err = run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("run() = %v, want the DATABASE_URL boot error", err)
	}
	if conn, derr := net.DialTimeout("tcp", addr, 300*time.Millisecond); derr == nil {
		_ = conn.Close()
		t.Fatalf("something listens on %s after a refused boot", addr)
	}
}
