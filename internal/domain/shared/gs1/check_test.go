package gs1

import (
	"errors"
	"testing"
)

func TestMod10CheckDigit(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    int
	}{
		// GS1 General Specifications section 7.9 style worked examples.
		{"gtin-14 each payload", "0304109000482", 7},
		{"gtin-14 case payload", "1304109000482", 4},
		{"gtin-14 pallet payload", "2304109000482", 1},
		{"gtin-14 variable measure payload", "9304109000482", 0},
		{"ean-13 stabilo", "400638133393", 1},
		{"ean-13 wikipedia", "590123412345", 7},
		{"upc-a wikipedia", "03600029145", 2},
		{"ean-8 wikipedia", "9638507", 4},
		{"ean-8 second", "7351353", 7},
		{"sscc gs1 uk sample", "37610425002123456", 9},
		{"sscc extension 3", "30614141123456789", 1},
		{"all zeros", "0000000", 0},
		{"single digit", "5", 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Mod10CheckDigit(tc.payload)
			if err != nil {
				t.Fatalf("Mod10CheckDigit(%q) error = %v", tc.payload, err)
			}
			if got != tc.want {
				t.Fatalf("Mod10CheckDigit(%q) = %d, want %d", tc.payload, got, tc.want)
			}
		})
	}
}

func TestMod10CheckDigitRejectsBadPayload(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{"empty", ""},
		{"letter", "12A45"},
		{"space", "123 45"},
		{"sign", "-12345"},
		{"fullwidth digits", "１２３４５"},
		{"eighteen digits", "123456789012345678"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Mod10CheckDigit(tc.payload); !errors.Is(err, ErrInvalidCheckInput) {
				t.Fatalf("Mod10CheckDigit(%q) error = %v, want ErrInvalidCheckInput", tc.payload, err)
			}
		})
	}
}

func TestValidMod10(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"13041090004824", true},
		{"03041090004827", true},
		{"23041090004821", true},
		{"4006381333931", true},
		{"96385074", true},
		{"376104250021234569", true},
		{"13041090004825", false}, // off by one
		{"13041090004820", false},
		{"00", true}, // shortest valid input: one payload digit and its check digit
		{"17", true},
		{"18", false},
		{"3", false},   // payload empty
		{"", false},    // nothing
		{"1A", false},  // not digits
		{"0000", true}, // zero payload has zero check digit; callers add policy on top
	}
	for _, tc := range tests {
		if got := ValidMod10(tc.in); got != tc.want {
			t.Errorf("ValidMod10(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// Every single-digit error in a valid number must be caught (mod-10 guarantees this).
func TestValidMod10DetectsEverySingleDigitError(t *testing.T) {
	const valid = "13041090004824"
	for i := 0; i < len(valid); i++ {
		for d := byte('0'); d <= '9'; d++ {
			if d == valid[i] {
				continue
			}
			mutated := valid[:i] + string(d) + valid[i+1:]
			if ValidMod10(mutated) {
				t.Errorf("ValidMod10(%q) = true; single-digit change at %d went undetected", mutated, i)
			}
		}
	}
}

// Swapping two adjacent digits is detected unless they differ by 5 (a known mod-10 blind spot);
// the test pins the behaviour so a future "improvement" cannot silently change it.
func TestValidMod10AdjacentTransposition(t *testing.T) {
	if ValidMod10("31041090004824") {
		t.Error("transposition of 1 and 3 must be detected")
	}
	// 0 and 5 swapped: 5304... vs 0534... differ by 5, undetected by mod-10 by construction.
	a, _ := Mod10CheckDigit("0504109000482")
	b, _ := Mod10CheckDigit("5004109000482")
	if a != b {
		t.Errorf("check digits of 05.. and 50.. should coincide (mod-10 blind spot), got %d and %d", a, b)
	}
}
