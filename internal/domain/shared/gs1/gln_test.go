package gs1

import (
	"errors"
	"testing"
)

func TestParseGLN(t *testing.T) {
	for _, in := range []string{"4006381333931", "4012345000009", "7614141000008", "0000000000000"} {
		t.Run(in, func(t *testing.T) {
			g, err := ParseGLN(in)
			if err != nil {
				t.Fatalf("ParseGLN(%q) error = %v", in, err)
			}
			if g.String() != in || g.IsZero() {
				t.Errorf("String() = %q, IsZero() = %v", g.String(), g.IsZero())
			}
		})
	}
}

func TestParseGLNRejects(t *testing.T) {
	tests := []struct{ name, in string }{
		{"empty", ""},
		{"twelve digits", "400638133393"},
		{"fourteen digits", "40063813339310"},
		{"bad check digit", "4006381333932"},
		{"letters", "400638133393A"},
		{"space", "4006381 33931"},
		{"fullwidth", "４００６３８１３３３９３１"},
		{"gtin 14", "13041090004824"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g, err := ParseGLN(tc.in)
			if !errors.Is(err, ErrInvalidGLN) {
				t.Fatalf("ParseGLN(%q) error = %v, want ErrInvalidGLN", tc.in, err)
			}
			if !g.IsZero() {
				t.Errorf("non-zero GLN returned with an error: %v", g)
			}
		})
	}
}

func TestZeroGLN(t *testing.T) {
	var g GLN
	if !g.IsZero() || g.String() != "" {
		t.Errorf("zero GLN: IsZero=%v String=%q", g.IsZero(), g.String())
	}
	if errors.Is(ErrInvalidGLN, ErrInvalidScan) {
		t.Error("ErrInvalidGLN is a value error, not a scan error")
	}
}
