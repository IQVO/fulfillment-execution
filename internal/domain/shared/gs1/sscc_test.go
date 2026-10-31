package gs1

import (
	"errors"
	"testing"
)

func TestParseSSCC(t *testing.T) {
	tests := []struct {
		in        string
		extension int
		check     int
	}{
		{"376104250021234569", 3, 9},
		{"306141411234567891", 3, 1},
		{"106141410000000002", 1, 2},
		{"006141410000000005", 0, 5},
		{"900000000000000003", 9, 3},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			s, err := ParseSSCC(tc.in)
			if err != nil {
				t.Fatalf("ParseSSCC(%q) error = %v", tc.in, err)
			}
			if s.String() != tc.in {
				t.Errorf("String() = %q", s.String())
			}
			if s.ExtensionDigit() != tc.extension {
				t.Errorf("ExtensionDigit() = %d, want %d", s.ExtensionDigit(), tc.extension)
			}
			if s.CheckDigit() != tc.check {
				t.Errorf("CheckDigit() = %d, want %d", s.CheckDigit(), tc.check)
			}
			if s.IsZero() {
				t.Error("IsZero() on a parsed SSCC")
			}
		})
	}
}

func TestParseSSCCRejects(t *testing.T) {
	tests := []struct{ name, in string }{
		{"empty", ""},
		{"seventeen digits", "37610425002123456"},
		{"nineteen digits", "3761042500212345690"},
		{"twenty digits with AI", "00376104250021234569"},
		{"bad check digit", "376104250021234568"},
		{"letters", "37610425002123456X"},
		{"space", "3761042500 1234569"},
		{"fullwidth", "３７６１０４２５００２１２３４５６９"},
		{"gtin 14", "13041090004824"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := ParseSSCC(tc.in)
			if !errors.Is(err, ErrInvalidSSCC) {
				t.Fatalf("ParseSSCC(%q) error = %v, want ErrInvalidSSCC", tc.in, err)
			}
			if !s.IsZero() {
				t.Errorf("non-zero SSCC returned with an error: %v", s)
			}
		})
	}
}

func TestNewSSCCComputesCheckDigit(t *testing.T) {
	s, err := NewSSCC("37610425002123456")
	if err != nil {
		t.Fatal(err)
	}
	if s.String() != "376104250021234569" {
		t.Errorf("NewSSCC = %q, want 376104250021234569", s)
	}
	for _, bad := range []string{"", "3761042500212345", "376104250021234567", "3761042500212345A"} {
		if _, err := NewSSCC(bad); !errors.Is(err, ErrInvalidSSCC) {
			t.Errorf("NewSSCC(%q) error = %v, want ErrInvalidSSCC", bad, err)
		}
	}
}

func TestZeroSSCC(t *testing.T) {
	var s SSCC
	if !s.IsZero() || s.String() != "" {
		t.Errorf("zero SSCC: IsZero=%v String=%q", s.IsZero(), s.String())
	}
}

func TestZeroSSCCDigitAccessors(t *testing.T) {
	var s SSCC
	if s.ExtensionDigit() != -1 || s.CheckDigit() != -1 {
		t.Errorf("zero SSCC digits = (%d,%d), want (-1,-1)", s.ExtensionDigit(), s.CheckDigit())
	}
}
