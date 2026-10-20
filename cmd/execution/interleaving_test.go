package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/interleavinghintcopy"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
)

// TestParseInterleavingConfig pins the TASK_INTERLEAVING_MODE boot contract
// (ADR 0051, then the real-wiring ADR): kafka only (empty = kafka),
// "permissive"/"off" rejected, a consumer group always required.
func TestParseInterleavingConfig(t *testing.T) {
	cases := []struct {
		name      string
		mode      string
		group     string
		wantGroup string
		wantErr   string
	}{
		{name: "empty defaults to kafka", mode: "", group: "g", wantGroup: "g"},
		{name: "kafka with group", mode: "kafka", group: "fulfillment-execution-interleaving", wantGroup: "fulfillment-execution-interleaving"},
		{name: "kafka is case-insensitive and trims", mode: " Kafka ", group: " g1 ", wantGroup: "g1"},
		{name: "no group is a boot error", mode: "kafka", wantErr: "TASK_INTERLEAVING_CONSUMER_GROUP"},
		{name: "blank group is a boot error", mode: "", group: "   ", wantErr: "TASK_INTERLEAVING_CONSUMER_GROUP"},
		{name: "permissive is rejected", mode: "permissive", group: "g", wantErr: "TASK_INTERLEAVING_MODE=permissive is not supported: this context runs on real Kafka"},
		{name: "off is rejected", mode: "OFF", group: "g", wantErr: "TASK_INTERLEAVING_MODE=off is not supported"},
		{name: "unknown mode", mode: "grpc", group: "g", wantErr: `unknown TASK_INTERLEAVING_MODE "grpc"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseInterleavingConfig(tc.mode, tc.group)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.consumerGroup != tc.wantGroup {
				t.Fatalf("cfg = %+v, want group %q", cfg, tc.wantGroup)
			}
		})
	}
}

// TestBuildInterleaving_BuildsThePostgresCopyAndTheConsumer pins that the
// hint consumer is always built and reads/writes the Postgres copy.
func TestBuildInterleaving_BuildsThePostgresCopyAndTheConsumer(t *testing.T) {
	wiring, err := buildInterleaving(interleavingConfig{consumerGroup: "g-it"}, lazyStorage(t), []string{"localhost:9092"}, quietLogger())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer wiring.close()
	if _, ok := wiring.lookup.(*interleavinghintcopy.PostgresStore); !ok {
		t.Fatalf("lookup = %T, want *PostgresStore", wiring.lookup)
	}
	if wiring.consumer == nil {
		t.Fatal("the InterleavingHintIssued consumer must always be built")
	}
}

func TestBuildInterleaving_WithoutPostgresIsABootError(t *testing.T) {
	_, err := buildInterleaving(interleavingConfig{consumerGroup: "g"}, storageAdapters{processedEvents: memory.NewProcessedEventsRepo()}, []string{"localhost:9092"}, quietLogger())
	if !errors.Is(err, errNoDatabase) {
		t.Fatalf("err = %v, want errNoDatabase: there is no in-memory hint copy", err)
	}
}

// TestStartInterleavingHintConsumer_NilClosesImmediately mirrors the other
// consumers' start helpers: no consumer means an already-closed channel.
func TestStartInterleavingHintConsumer_NilClosesImmediately(t *testing.T) {
	done := startInterleavingHintConsumer(nil, t.Context(), quietLogger())
	select {
	case <-done:
	default:
		t.Fatal("nil consumer must yield an already-closed done channel")
	}
}
