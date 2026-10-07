package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

// TestParseClassificationConfig pins the PRODUCT_CLASSIFICATION_MODE boot
// contract of ADR-0039: kafka|permissive only, "http" removed and rejected,
// kafka without a consumer group rejected, empty = permissive.
func TestParseClassificationConfig(t *testing.T) {
	cases := []struct {
		name      string
		mode      string
		group     string
		wantMode  string
		wantGroup string
		wantErr   string
	}{
		{name: "empty defaults to permissive", mode: "", wantMode: classificationModePermissive},
		{name: "permissive", mode: "permissive", wantMode: classificationModePermissive},
		{name: "permissive ignores a group", mode: "PERMISSIVE", group: "g", wantMode: classificationModePermissive},
		{name: "kafka with group", mode: "kafka", group: "fulfillment-execution-product-classification", wantMode: classificationModeKafka, wantGroup: "fulfillment-execution-product-classification"},
		{name: "kafka is case-insensitive and trims", mode: " Kafka ", group: " g1 ", wantMode: classificationModeKafka, wantGroup: "g1"},
		{name: "kafka without group is a boot error", mode: "kafka", wantErr: "PRODUCT_CLASSIFICATION_CONSUMER_GROUP"},
		{name: "kafka with blank group is a boot error", mode: "kafka", group: "   ", wantErr: "PRODUCT_CLASSIFICATION_CONSUMER_GROUP"},
		{name: "http is removed", mode: "http", wantErr: "PRODUCT_CLASSIFICATION_MODE=http was removed"},
		{name: "HTTP is removed too", mode: "HTTP", group: "g", wantErr: "PRODUCT_CLASSIFICATION_MODE=http was removed"},
		{name: "unknown mode", mode: "grpc", wantErr: `unknown PRODUCT_CLASSIFICATION_MODE "grpc"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseClassificationConfig(tc.mode, tc.group)
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

func TestParseClassificationConfig_HTTPErrorIsTheSentinel(t *testing.T) {
	_, err := parseClassificationConfig("http", "")
	if !errors.Is(err, errClassificationModeHTTPRemoved) {
		t.Fatalf("err = %v, want errClassificationModeHTTPRemoved", err)
	}
}

func TestClassificationConfigFromEnv(t *testing.T) {
	t.Setenv("PRODUCT_CLASSIFICATION_MODE", "http")
	t.Setenv("PRODUCT_CLASSIFICATION_CONSUMER_GROUP", "")
	if _, err := classificationConfigFromEnv(); !errors.Is(err, errClassificationModeHTTPRemoved) {
		t.Fatalf("PRODUCT_CLASSIFICATION_MODE=http must fail boot, got %v", err)
	}

	t.Setenv("PRODUCT_CLASSIFICATION_MODE", "kafka")
	if _, err := classificationConfigFromEnv(); err == nil {
		t.Fatal("kafka without PRODUCT_CLASSIFICATION_CONSUMER_GROUP must fail boot")
	}

	t.Setenv("PRODUCT_CLASSIFICATION_CONSUMER_GROUP", "group-from-env")
	cfg, err := classificationConfigFromEnv()
	if err != nil || cfg.consumerGroup != "group-from-env" {
		t.Fatalf("cfg = %+v err = %v, want the group read from the env", cfg, err)
	}

	t.Setenv("PRODUCT_CLASSIFICATION_MODE", "")
	cfg, err = classificationConfigFromEnv()
	if err != nil || cfg.mode != classificationModePermissive {
		t.Fatalf("unset mode must be permissive, got %+v err = %v", cfg, err)
	}
}

func inMemoryStorage() storageAdapters {
	return storageAdapters{processedEvents: memory.NewProcessedEventsRepo()}
}

func TestBuildClassification_PermissiveHasNoConsumer(t *testing.T) {
	w, err := buildClassification(classificationConfig{mode: classificationModePermissive}, inMemoryStorage(), []string{"broker:9092"}, newLogger("error"))
	if err != nil {
		t.Fatalf("buildClassification: %v", err)
	}
	if _, ok := w.lookup.(productclassificationcopy.PermissiveLookup); !ok {
		t.Fatalf("lookup = %T, want PermissiveLookup", w.lookup)
	}
	if w.consumer != nil || w.apply != nil {
		t.Fatal("permissive mode must not build a consumer")
	}
	w.close() // must be safe
}

func TestBuildClassification_KafkaWithoutDatabaseUsesMemoryCopyAndBuildsConsumer(t *testing.T) {
	w, err := buildClassification(classificationConfig{mode: classificationModeKafka, consumerGroup: "g"}, inMemoryStorage(), []string{"broker:9092"}, newLogger("error"))
	if err != nil {
		t.Fatalf("buildClassification: %v", err)
	}
	defer w.close()
	if _, ok := w.lookup.(*productclassificationcopy.MemoryStore); !ok {
		t.Fatalf("lookup = %T, want *MemoryStore", w.lookup)
	}
	if w.consumer == nil || w.apply == nil {
		t.Fatal("kafka mode must build the ProductClassified consumer")
	}

	// The use case writes the SAME store the lookup reads.
	err = w.apply.Execute(context.Background(), usecasesRequest("evt-1", "SKU-1", 3))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	info, err := w.lookup.GetClassification(context.Background(), "SKU-1")
	if err != nil || !info.Known || !info.Hazmat || info.DOTHazardClass != 3 {
		t.Fatalf("lookup after apply = %+v err = %v", info, err)
	}
}

func TestBuildClassification_KafkaWithoutGroupFails(t *testing.T) {
	if _, err := buildClassification(classificationConfig{mode: classificationModeKafka}, inMemoryStorage(), []string{"broker:9092"}, newLogger("error")); err == nil {
		t.Fatal("a kafka wiring without a consumer group must fail")
	}
}

func TestStartProductClassifiedConsumer_NilConsumerIsDoneImmediately(t *testing.T) {
	done := startProductClassifiedConsumer(nil, context.Background(), newLogger("error"))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a nil consumer must report done immediately")
	}
}

func TestJoinDone_WaitsForEveryChannel(t *testing.T) {
	a, b := make(chan struct{}), make(chan struct{})
	all := joinDone(a, b)
	close(a)
	select {
	case <-all:
		t.Fatal("joinDone closed before every input closed")
	case <-time.After(50 * time.Millisecond):
	}
	close(b)
	select {
	case <-all:
	case <-time.After(time.Second):
		t.Fatal("joinDone never closed")
	}
}

func usecasesRequest(eventId, sku string, dot int) usecases.ProductClassifiedRequest {
	return usecases.ProductClassifiedRequest{
		EventId: eventId,
		Record:  ports.ProductClassificationRecord{SKU: sku, HandlingTags: []string{"Hazmat"}, DOTHazardClass: dot, Version: 1},
	}
}
