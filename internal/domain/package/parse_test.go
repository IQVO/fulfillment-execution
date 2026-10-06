package pack_test

import (
	"errors"
	"testing"

	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
)

func TestParseStatus(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    pack.Status
		wantErr error
	}{
		{name: "open", raw: "OPEN", want: pack.Open},
		{name: "sealed", raw: "SEALED", want: pack.Sealed},
		{name: "labeled", raw: "LABELED", want: pack.Labeled},
		{name: "diverted", raw: "DIVERTED", want: pack.Diverted},
		{name: "unknown", raw: "SHIPPED", wantErr: pack.ErrUnknownStatus},
		{name: "wrong case", raw: "sealed", wantErr: pack.ErrUnknownStatus},
		{name: "empty", raw: "", wantErr: pack.ErrUnknownStatus},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := pack.ParseStatus(tc.raw)
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
