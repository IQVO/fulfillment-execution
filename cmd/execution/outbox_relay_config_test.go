package main

import (
	"io"
	"log/slog"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
)

// TestOutboxMaxAttempts pins OUTBOX_MAX_ATTEMPTS (gap fix D12, ADR 0042):
// unset means the fleet default 20, 0 means unlimited, a malformed or
// negative value falls back to the default instead of failing the boot.
func TestOutboxMaxAttempts(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := []struct {
		name string
		raw  string
		want int
	}{
		{"unset", "", postgres.DefaultOutboxMaxAttempts},
		{"explicit", "5", 5},
		{"padded", " 7 ", 7},
		{"zero is unlimited", "0", 0},
		{"malformed", "lots", postgres.DefaultOutboxMaxAttempts},
		{"negative", "-3", postgres.DefaultOutboxMaxAttempts},
	}
	if postgres.DefaultOutboxMaxAttempts != 20 {
		t.Fatalf("DefaultOutboxMaxAttempts = %d, want the fleet default 20", postgres.DefaultOutboxMaxAttempts)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OUTBOX_MAX_ATTEMPTS", tt.raw)
			if got := outboxMaxAttempts(logger); got != tt.want {
				t.Fatalf("outboxMaxAttempts(%q) = %d, want %d", tt.raw, got, tt.want)
			}
		})
	}
}
