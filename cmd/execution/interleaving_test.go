package main

import (
	"strings"
	"testing"
)

// TestParseInterleavingConfig pins the TASK_INTERLEAVING_MODE boot contract
// of ADR-0051: kafka|permissive only, kafka without a consumer group
// rejected, empty = permissive.
func TestParseInterleavingConfig(t *testing.T) {
	cases := []struct {
		name      string
		mode      string
		group     string
		wantMode  string
		wantGroup string
		wantErr   string
	}{
		{name: "empty defaults to permissive", mode: "", wantMode: interleavingModePermissive},
		{name: "permissive", mode: "permissive", wantMode: interleavingModePermissive},
		{name: "permissive ignores a group", mode: "PERMISSIVE", group: "g", wantMode: interleavingModePermissive},
		{name: "kafka with group", mode: "kafka", group: "fulfillment-execution-interleaving", wantMode: interleavingModeKafka, wantGroup: "fulfillment-execution-interleaving"},
		{name: "kafka is case-insensitive and trims", mode: " Kafka ", group: " g1 ", wantMode: interleavingModeKafka, wantGroup: "g1"},
		{name: "kafka without group is a boot error", mode: "kafka", wantErr: "TASK_INTERLEAVING_CONSUMER_GROUP"},
		{name: "kafka with blank group is a boot error", mode: "kafka", group: "   ", wantErr: "TASK_INTERLEAVING_CONSUMER_GROUP"},
		{name: "unknown mode", mode: "grpc", wantErr: `unknown TASK_INTERLEAVING_MODE "grpc"`},
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
			if cfg.mode != tc.wantMode || cfg.consumerGroup != tc.wantGroup {
				t.Fatalf("cfg = %+v, want mode %q group %q", cfg, tc.wantMode, tc.wantGroup)
			}
		})
	}
}

// TestBuildInterleaving_PermissiveWiresNoLookup pins the fail-open default:
// without TASK_INTERLEAVING_MODE=kafka there is no consumer and claimNext's
// Hints is nil — byte-for-byte the pre-ADR-0051 path.
func TestBuildInterleaving_PermissiveWiresNoLookup(t *testing.T) {
	wiring, err := buildInterleaving(interleavingConfig{mode: interleavingModePermissive}, inMemoryStorage(), nil, newLogger("error"))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer wiring.close()
	if wiring.lookup != nil || wiring.consumer != nil {
		t.Fatalf("permissive wiring = %+v, want no lookup and no consumer", wiring)
	}
}

// TestBuildInterleaving_KafkaWithoutDatabaseUsesMemory pins the in-memory
// fallback (DATABASE_URL unset): the hint copy is in memory — harmless for
// an advisory, seconds-lived hint.
func TestBuildInterleaving_KafkaWithoutDatabaseUsesMemory(t *testing.T) {
	wiring, err := buildInterleaving(interleavingConfig{mode: interleavingModeKafka, consumerGroup: "g-it"}, inMemoryStorage(), []string{"localhost:9092"}, newLogger("error"))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer wiring.close()
	if wiring.lookup == nil || wiring.consumer == nil {
		t.Fatalf("kafka wiring = %+v, want a lookup and a consumer", wiring)
	}
}

// TestStartInterleavingHintConsumer_NilClosesImmediately mirrors the other
// consumers' start helpers: no consumer means an already-closed channel.
func TestStartInterleavingHintConsumer_NilClosesImmediately(t *testing.T) {
	done := startInterleavingHintConsumer(nil, t.Context(), newLogger("error"))
	select {
	case <-done:
	default:
		t.Fatal("nil consumer must yield an already-closed done channel")
	}
}
