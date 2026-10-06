package task_test

import (
	"errors"
	"testing"

	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

func TestParseStatus(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    task.Status
		wantErr error
	}{
		{name: "pending", raw: "PENDING", want: task.Pending},
		{name: "claimed", raw: "CLAIMED", want: task.Claimed},
		{name: "completed", raw: "COMPLETED", want: task.Completed},
		{name: "unknown", raw: "CANCELLED", wantErr: task.ErrUnknownStatus},
		{name: "wrong case", raw: "pending", wantErr: task.ErrUnknownStatus},
		{name: "empty", raw: "", wantErr: task.ErrUnknownStatus},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := task.ParseStatus(tc.raw)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("expected error %v, got %v", tc.wantErr, err)
				}
				if got != "" {
					t.Fatalf("expected zero value on error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}
}

func TestParseType(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    task.Type
		wantErr error
	}{
		{name: "pick", raw: "PICK", want: task.Pick},
		{name: "pack", raw: "PACK", want: task.Pack},
		{name: "slam", raw: "SLAM", want: task.Slam},
		{name: "rebin", raw: "REBIN", want: task.Rebin},
		{name: "unknown", raw: "PUTAWAY", wantErr: task.ErrUnknownType},
		{name: "wrong case", raw: "pick", wantErr: task.ErrUnknownType},
		{name: "empty", raw: "", wantErr: task.ErrUnknownType},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := task.ParseType(tc.raw)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("expected error %v, got %v", tc.wantErr, err)
				}
				if got != "" {
					t.Fatalf("expected zero value on error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}
}
