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

func TestBuildSSCC(t *testing.T) {
	tests := []struct {
		name      string
		extension int
		prefix    string
		serial    string
		want      string
	}{
		{"seven digit prefix, serial padded", 3, "7610425", "12345", "376104250000123457"},
		{"serial of one digit", 0, "6141411", "1", "061414110000000013"},
		{"seven digit prefix, serial fills the room (9 digits)", 3, "7610425", "999999999", "376104259999999999"},
		{"ten digit prefix, serial fills the room (6 digits)", 9, "0614141234", "123456", "906141412341234562"},
		{"nine digit prefix", 5, "761042512", "1234567", "576104251212345679"},
		{"ten digit prefix, shorter serial", 5, "7610425123", "123456", "576104251231234565"},
		{"all zero", 0, "0000000", "0", "000000000000000000"},
		{"serial with leading zeros keeps its width", 3, "7610425", "000012345", "376104250000123457"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := BuildSSCC(tc.extension, tc.prefix, tc.serial)
			if err != nil {
				t.Fatalf("BuildSSCC(%d, %q, %q) error = %v", tc.extension, tc.prefix, tc.serial, err)
			}
			if s.String() != tc.want {
				t.Errorf("BuildSSCC = %q, want %q", s, tc.want)
			}
			if _, err := ParseSSCC(s.String()); err != nil {
				t.Errorf("built SSCC does not parse back: %v", err)
			}
		})
	}
}

func TestBuildSSCCRejects(t *testing.T) {
	tests := []struct {
		name      string
		extension int
		prefix    string
		serial    string
	}{
		{"extension -1", -1, "7610425", "1"},
		{"extension 10", 10, "7610425", "1"},
		{"prefix of 6 digits", 3, "761042", "1"},
		{"prefix of 11 digits", 3, "76104251234", "1"},
		{"empty prefix", 3, "", "1"},
		{"non numeric prefix", 3, "761042X", "1"},
		{"empty serial", 3, "7610425", ""},
		{"non numeric serial", 3, "7610425", "12A"},
		{"serial of 10 digits after a 7 digit prefix", 3, "7610425", "1234567890"},
		{"serial of 7 digits after a 10 digit prefix", 3, "7610425123", "1234567"},
		{"serial with a sign", 3, "7610425", "-1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := BuildSSCC(tc.extension, tc.prefix, tc.serial)
			if !errors.Is(err, ErrInvalidSSCC) {
				t.Fatalf("BuildSSCC(%d, %q, %q) error = %v, want ErrInvalidSSCC", tc.extension, tc.prefix, tc.serial, err)
			}
			if !s.IsZero() {
				t.Errorf("non-zero SSCC returned with an error: %v", s)
			}
		})
	}
}

// The boundary values themselves are accepted: extension 0 and 9.
func TestBuildSSCCExtensionBounds(t *testing.T) {
	for _, ext := range []int{0, 9} {
		s, err := BuildSSCC(ext, "7610425", "1")
		if err != nil || s.ExtensionDigit() != ext {
			t.Errorf("BuildSSCC(%d) = %v, %v", ext, s, err)
		}
	}
}

func TestSSCCHumanReadable(t *testing.T) {
	s := mustSSCC(t, "376104250021234569")
	if got := s.HumanReadable(); got != "(00)376104250021234569" {
		t.Errorf("HumanReadable() = %q", got)
	}
	if got := (SSCC{}).HumanReadable(); got != "" {
		t.Errorf("zero SSCC HumanReadable() = %q, want empty", got)
	}
}
