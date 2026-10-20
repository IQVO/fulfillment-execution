package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/productclassificationcopy"
)

// TestParseClassificationConfig pins the PRODUCT_CLASSIFICATION_MODE boot
// contract (ADR-0039, then the real-wiring ADR): kafka only (empty = kafka),
// "http" removed, "permissive"/"off" rejected, a consumer group always
// required.
func TestParseClassificationConfig(t *testing.T) {
	cases := []struct {
		name      string
		mode      string
		group     string
		wantGroup string
		wantErr   string
	}{
		{name: "empty defaults to kafka", mode: "", group: "g", wantGroup: "g"},
		{name: "kafka with group", mode: "kafka", group: "fulfillment-execution-product-classification", wantGroup: "fulfillment-execution-product-classification"},
		{name: "kafka is case-insensitive and trims", mode: " Kafka ", group: " g1 ", wantGroup: "g1"},
		{name: "no group is a boot error", mode: "kafka", wantErr: "PRODUCT_CLASSIFICATION_CONSUMER_GROUP"},
		{name: "blank group is a boot error", mode: "", group: "   ", wantErr: "PRODUCT_CLASSIFICATION_CONSUMER_GROUP"},
		{name: "permissive is rejected", mode: "permissive", group: "g", wantErr: "PRODUCT_CLASSIFICATION_MODE=permissive is not supported: this context runs on real Kafka"},
		{name: "PERMISSIVE is rejected too", mode: "PERMISSIVE", group: "g", wantErr: "PRODUCT_CLASSIFICATION_MODE=permissive is not supported"},
		{name: "off is rejected", mode: "off", group: "g", wantErr: "PRODUCT_CLASSIFICATION_MODE=off is not supported"},
		{name: "http is removed", mode: "http", wantErr: "PRODUCT_CLASSIFICATION_MODE=http was removed"},
		{name: "HTTP is removed too", mode: "HTTP", group: "g", wantErr: "PRODUCT_CLASSIFICATION_MODE=http was removed"},
		{name: "unknown mode", mode: "grpc", group: "g", wantErr: `unknown PRODUCT_CLASSIFICATION_MODE "grpc"`},
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
			if cfg.consumerGroup != tc.wantGroup {
				t.Fatalf("cfg = %+v, want group %q", cfg, tc.wantGroup)
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

// lazyStorage is the storage the build functions need: a pgxpool that never
// dials (pgxpool connects lazily) and an in-memory processed-events store.
// The memory double is fine in a TEST; no cmd/** non-test file imports it.
func lazyStorage(t *testing.T) storageAdapters {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://u:***@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return storageAdapters{processedEvents: memory.NewProcessedEventsRepo(), pool: pool}
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestBuildClassification_BuildsThePostgresCopyAndTheConsumer(t *testing.T) {
	w, err := buildClassification(classificationConfig{consumerGroup: "g"}, lazyStorage(t), []string{"broker:9092"}, quietLogger())
	if err != nil {
		t.Fatalf("buildClassification: %v", err)
	}
	defer w.close()
	if _, ok := w.lookup.(*productclassificationcopy.PostgresStore); !ok {
		t.Fatalf("lookup = %T, want *PostgresStore", w.lookup)
	}
	if w.consumer == nil || w.apply == nil {
		t.Fatal("the ProductClassified consumer must always be built")
	}
}

func TestBuildClassification_WithoutPostgresIsABootError(t *testing.T) {
	_, err := buildClassification(classificationConfig{consumerGroup: "g"}, storageAdapters{processedEvents: memory.NewProcessedEventsRepo()}, []string{"broker:9092"}, quietLogger())
	if !errors.Is(err, errNoDatabase) {
		t.Fatalf("err = %v, want errNoDatabase: there is no in-memory copy", err)
	}
}

func TestBuildClassification_WithoutGroupFails(t *testing.T) {
	if _, err := buildClassification(classificationConfig{}, lazyStorage(t), []string{"broker:9092"}, quietLogger()); err == nil {
		t.Fatal("a wiring without a consumer group must fail")
	}
}

func TestStartProductClassifiedConsumer_NilConsumerIsDoneImmediately(t *testing.T) {
	done := startProductClassifiedConsumer(nil, context.Background(), quietLogger())
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
