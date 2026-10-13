package main

import (
	"context"
	"log/slog"
	"testing"
)

// TestNewLoggerLevels pins LOG_LEVEL parsing for cmd/mcp (gap fix D22): every
// fleet binary accepts "warning" exactly like "warn", case-insensitively,
// and anything unknown means info.
func TestNewLoggerLevels(t *testing.T) {
	tests := []struct {
		level string
		want  slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"WARNING", slog.LevelWarn},
		{"error", slog.LevelError},
		{"", slog.LevelInfo},
		{"verbose", slog.LevelInfo},
	}
	ctx := context.Background()
	for _, tt := range tests {
		logger := newLogger(tt.level)
		if !logger.Enabled(ctx, tt.want) || (tt.want > slog.LevelDebug && logger.Enabled(ctx, tt.want-4)) {
			t.Errorf("newLogger(%q): want minimum level %v", tt.level, tt.want)
		}
	}
}
