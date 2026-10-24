package gs1

import (
	"errors"
	"strings"
	"testing"
	"time"
)

const gsep = "\x1d" // ASCII GS, the FNC1 separator a scanner emits after a variable-length AI

func mustGTIN(t testing.TB, s string) GTIN {
	t.Helper()
	g, err := ParseGTIN(s)
	if err != nil {
		t.Fatalf("fixture GTIN %q: %v", s, err)
	}
	return g
}

func mustSSCC(t testing.TB, s string) SSCC {
	t.Helper()
	v, err := ParseSSCC(s)
	if err != nil {
		t.Fatalf("fixture SSCC %q: %v", s, err)
	}
	return v
}

func TestParseScanGS1128(t *testing.T) {
	const sscc = "376104250021234569"
	tests := []struct {
		name string
		in   string
		want func(t *testing.T) Scan
	}{
		{"case gtin only", "]C1" + "01" + "13041090004824", func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, GTIN: mustGTIN(t, "13041090004824")}
		}},
		{"gtin expiry lot (lot last, no separator needed)", "]C1" + "01" + "13041090004824" + "17" + "261231" + "10" + "LOT-42", func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, GTIN: mustGTIN(t, "13041090004824"),
				Expiry: Date{Year: 2026, Month: time.December, Day: 31}, Lot: "LOT-42"}
		}},
		{"gtin lot separator expiry", "]C1" + "01" + "03041090004827" + "10" + "AB12" + gsep + "17" + "270630", func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, GTIN: mustGTIN(t, "03041090004827"),
				Lot: "AB12", Expiry: Date{Year: 2027, Month: time.June, Day: 30}}
		}},
		{"sscc only", "]C1" + "00" + sscc, func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, SSCC: mustSSCC(t, sscc)}
		}},
		{"sscc with content and count", "]C1" + "00" + sscc + "02" + "03041090004827" + "37" + "48", func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, SSCC: mustSSCC(t, sscc),
				ContentGTIN: mustGTIN(t, "03041090004827"), Count: 48}
		}},
		{"pallet of cases: content before count before sscc", "]C1" + "02" + "23041090004821" + "37" + "12" + gsep + "00" + sscc, func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, SSCC: mustSSCC(t, sscc),
				ContentGTIN: mustGTIN(t, "23041090004821"), Count: 12}
		}},
		{"sscc, content, count, lot, expiry", "]C1" + "00" + sscc + "02" + "13041090004824" + "37" + "20" + gsep + "10" + "L9" + gsep + "17" + "260100", func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, SSCC: mustSSCC(t, sscc),
				ContentGTIN: mustGTIN(t, "13041090004824"), Count: 20, Lot: "L9",
				Expiry: Date{Year: 2026, Month: time.January, Day: 31}} // DD=00 means last day of the month
		}},
		{"trailing separator tolerated", "]C1" + "01" + "13041090004824" + "10" + "LOT" + gsep, func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, GTIN: mustGTIN(t, "13041090004824"), Lot: "LOT"}
		}},
		{"lot without separator absorbs trailing digits (documented)", "]C1" + "01" + "13041090004824" + "10" + "ABC17261231", func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, GTIN: mustGTIN(t, "13041090004824"), Lot: "ABC17261231"}
		}},
		{"lot of twenty characters", "]C1" + "01" + "13041090004824" + "10" + strings.Repeat("A", 20), func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, GTIN: mustGTIN(t, "13041090004824"), Lot: strings.Repeat("A", 20)}
		}},
		{"count of eight digits with leading zeros", "]C1" + "00" + sscc + "02" + "03041090004827" + "37" + "00000012", func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, SSCC: mustSSCC(t, sscc),
				ContentGTIN: mustGTIN(t, "03041090004827"), Count: 12}
		}},
		{"count of one", "]C1" + "00" + sscc + "02" + "03041090004827" + "37" + "1", func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, SSCC: mustSSCC(t, sscc),
				ContentGTIN: mustGTIN(t, "03041090004827"), Count: 1}
		}},
		{"leap day expiry", "]C1" + "01" + "13041090004824" + "17" + "280229", func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, GTIN: mustGTIN(t, "13041090004824"),
				Expiry: Date{Year: 2028, Month: time.February, Day: 29}}
		}},
		{"february DD=00 in a leap year is the 29th", "]C1" + "01" + "13041090004824" + "17" + "280200", func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, GTIN: mustGTIN(t, "13041090004824"),
				Expiry: Date{Year: 2028, Month: time.February, Day: 29}}
		}},
		{"element string without symbology identifier", "01" + "13041090004824" + "10" + "X", func(t *testing.T) Scan {
			return Scan{GTIN: mustGTIN(t, "13041090004824"), Lot: "X"}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseScan(tc.in)
			if err != nil {
				t.Fatalf("ParseScan(%q) error = %v", tc.in, err)
			}
			if want := tc.want(t); got != want {
				t.Fatalf("ParseScan(%q)\n got  %+v\n want %+v", tc.in, got, want)
			}
		})
	}
}

func TestParseScanRetailAndTransportSymbologies(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		sym   Symbology
		gtin  string
		fixed string
	}{
		{"ean-13", "]E0" + "5901234123457", SymbologyEANUPC, "05901234123457", ""},
		{"ean-8", "]E0" + "96385074", SymbologyEANUPC, "00000096385074", ""},
		{"upc-a as 12 digits", "]E0" + "036000291452", SymbologyEANUPC, "00036000291452", ""},
		{"upc-a reported as 13 digits", "]E0" + "0036000291452", SymbologyEANUPC, "00036000291452", ""},
		{"itf-14 case", "]I1" + "13041090004824", SymbologyITF14, "13041090004824", ""},
		{"itf-14 each-indicator", "]I1" + "03041090004827", SymbologyITF14, "03041090004827", ""},
		{"bare ean-13 (wedge strips the identifier)", "5901234123457", "", "05901234123457", ""},
		{"bare ean-8", "96385074", "", "00000096385074", ""},
		{"bare gtin-14", "13041090004824", "", "13041090004824", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseScan(tc.in)
			if err != nil {
				t.Fatalf("ParseScan(%q) error = %v", tc.in, err)
			}
			want := Scan{Symbology: tc.sym, GTIN: mustGTIN(t, tc.gtin)}
			if got != want {
				t.Fatalf("ParseScan(%q)\n got  %+v\n want %+v", tc.in, got, want)
			}
		})
	}
}

func TestParseScanRejects(t *testing.T) {
	const sscc = "376104250021234569"
	tests := []struct {
		name string
		in   string
		want error
	}{
		{"empty", "", ErrInvalidScan},
		{"only symbology identifier", "]C1", ErrMissingAI},
		{"only gs", "]C1" + gsep, ErrInvalidScan},
		{"unsupported symbology", "]Q3" + "01" + "13041090004824", ErrUnsupportedSymbology},
		{"unsupported data matrix", "]d2" + "01" + "13041090004824", ErrUnsupportedSymbology},
		{"one character cannot be an AI", "]C1" + "0", ErrUnknownAI},
		{"unknown AI", "]C1" + "99" + "ABC", ErrUnknownAI},
		{"unknown AI after valid", "]C1" + "01" + "13041090004824" + "21" + "SER1", ErrUnknownAI},
		{"leading separator", "]C1" + gsep + "01" + "13041090004824", ErrInvalidScan},
		{"separator after fixed length AI", "]C1" + "01" + "13041090004824" + gsep + "17" + "261231", ErrInvalidScan},
		{"AI 01 and AI 02 together", "]C1" + "01" + "13041090004824" + "02" + "03041090004827" + "37" + "48", ErrConflictingAIs},
		{"AI 01 and AI 02 together with sscc", "]C1" + "00" + sscc + "01" + "13041090004824" + "02" + "03041090004827" + "37" + "48", ErrConflictingAIs},
		{"AI 02 without 00 and 37", "]C1" + "02" + "03041090004827", ErrMissingAI},
		{"AI 02 with 37 but no sscc", "]C1" + "02" + "03041090004827" + "37" + "48", ErrMissingAI},
		{"AI 02 with sscc but no count", "]C1" + "00" + sscc + "02" + "03041090004827", ErrMissingAI},
		{"AI 37 alone", "]C1" + "37" + "48", ErrMissingAI},
		{"lot alone identifies nothing", "]C1" + "10" + "LOT", ErrMissingAI},
		{"expiry alone identifies nothing", "]C1" + "17" + "261231", ErrMissingAI},
		{"duplicate AI 10", "]C1" + "10" + "A" + gsep + "10" + "B", ErrDuplicateAI},
		{"duplicate AI 01", "]C1" + "01" + "13041090004824" + "01" + "03041090004827", ErrDuplicateAI},
		{"gtin bad check digit", "]C1" + "01" + "13041090004825", ErrInvalidScan},
		{"sscc bad check digit", "]C1" + "00" + "376104250021234568", ErrInvalidScan},
		{"content gtin bad check digit", "]C1" + "00" + sscc + "02" + "03041090004828" + "37" + "48", ErrInvalidScan},
		{"truncated gtin", "]C1" + "01" + "130410900", ErrInvalidScan},
		{"truncated sscc", "]C1" + "00" + "3761042500", ErrInvalidScan},
		{"non digit in gtin", "]C1" + "01" + "1304109000482A", ErrInvalidScan},
		{"lot too long", "]C1" + "01" + "13041090004824" + "10" + strings.Repeat("A", 21), ErrInvalidScan},
		{"empty lot", "]C1" + "01" + "13041090004824" + "10" + gsep + "17" + "261231", ErrInvalidScan},
		{"lot with space", "]C1" + "01" + "13041090004824" + "10" + "AB CD", ErrInvalidScan},
		{"lot with hash", "]C1" + "01" + "13041090004824" + "10" + "AB#CD", ErrInvalidScan},
		{"lot with non ascii", "]C1" + "01" + "13041090004824" + "10" + "LÓT", ErrInvalidScan},
		{"count zero", "]C1" + "00" + sscc + "02" + "03041090004827" + "37" + "0", ErrInvalidScan},
		{"count nine digits", "]C1" + "00" + sscc + "02" + "03041090004827" + "37" + "123456789", ErrInvalidScan},
		{"count non digit", "]C1" + "00" + sscc + "02" + "03041090004827" + "37" + "4X", ErrInvalidScan},
		{"expiry month 13", "]C1" + "01" + "13041090004824" + "17" + "261301", ErrInvalidScan},
		{"expiry month 00", "]C1" + "01" + "13041090004824" + "17" + "260001", ErrInvalidScan},
		{"expiry day 32", "]C1" + "01" + "13041090004824" + "17" + "261232", ErrInvalidScan},
		{"expiry 30 february", "]C1" + "01" + "13041090004824" + "17" + "260230", ErrInvalidScan},
		{"expiry 29 february non leap", "]C1" + "01" + "13041090004824" + "17" + "270229", ErrInvalidScan},
		{"expiry short", "]C1" + "01" + "13041090004824" + "17" + "2612", ErrInvalidScan},
		{"expiry non digit", "]C1" + "01" + "13041090004824" + "17" + "26AB31", ErrInvalidScan},
		{"ean with sscc length", "]E0" + sscc, ErrInvalidScan},
		{"ean bad check digit", "]E0" + "5901234123458", ErrInvalidScan},
		{"ean letters", "]E0" + "590123412345X", ErrInvalidScan},
		{"itf-14 thirteen digits", "]I1" + "5901234123457", ErrInvalidScan},
		{"itf-14 bad check digit", "]I1" + "13041090004825", ErrInvalidScan},
		{"gibberish", "hello world", ErrInvalidScan},
		{"nul byte", "]C1" + "01" + "13041090004824" + "10" + "A\x00B", ErrInvalidScan},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseScan(tc.in)
			if !errors.Is(err, tc.want) {
				t.Fatalf("ParseScan(%q) error = %v, want %v", tc.in, err, tc.want)
			}
			if !errors.Is(err, ErrInvalidScan) {
				t.Errorf("every parse error must wrap ErrInvalidScan, got %v", err)
			}
			if got != (Scan{}) {
				t.Errorf("ParseScan(%q) returned a partial scan %+v with an error", tc.in, got)
			}
		})
	}
}

func TestDateString(t *testing.T) {
	d := Date{Year: 2026, Month: time.March, Day: 9}
	if d.String() != "2026-03-09" {
		t.Errorf("Date.String() = %q", d.String())
	}
	if (Date{}).String() != "" || !(Date{}).IsZero() {
		t.Error("zero Date must be empty and IsZero")
	}
}

func TestScanErrorsAreDistinct(t *testing.T) {
	all := []error{ErrUnsupportedSymbology, ErrUnknownAI, ErrDuplicateAI, ErrConflictingAIs, ErrMissingAI}
	for i, a := range all {
		for j, b := range all {
			if i != j && errors.Is(a, b) {
				t.Errorf("%v must not match %v", a, b)
			}
		}
	}
}

// FuzzParseScan: whatever a scanner (or an attacker) sends, the parser must not panic, and a
// successful parse must satisfy the structural rules the callers rely on.
func FuzzParseScan(f *testing.F) {
	for _, seed := range []string{
		"", "]C1", "]E0", "]I1", "]C1" + gsep,
		"]C101130410900048241726123110LOT-42",
		"]C1" + "01" + "03041090004827" + "10" + "AB12" + gsep + "17" + "270630",
		"]C100376104250021234569020304109000482737" + "48",
		"]C100376104250021234569" + "02" + "13041090004824" + "37" + "20" + gsep + "10" + "L9" + gsep + "17" + "260100",
		"]E05901234123457", "]E096385074", "]I113041090004824", "13041090004824", "96385074",
		"]C10113041090004824" + "02" + "03041090004827", "99", "]Z0", "\x00", "]C1" + strings.Repeat("1", 64),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, in string) {
		got, err := ParseScan(in)
		if err != nil {
			if !errors.Is(err, ErrInvalidScan) {
				t.Fatalf("error %v does not wrap ErrInvalidScan", err)
			}
			if got != (Scan{}) {
				t.Fatalf("partial scan %+v returned with error %v", got, err)
			}
			return
		}
		assertScanInvariants(t, got)
	})
}

func assertScanInvariants(t *testing.T, s Scan) {
	t.Helper()
	assertScanCombination(t, s)
	assertScanElementBounds(t, s)
	for _, g := range []GTIN{s.GTIN, s.ContentGTIN} {
		if !g.IsZero() && (len(g.String()) != 14 || !ValidMod10(g.String())) {
			t.Fatalf("invalid GTIN accepted: %q", g)
		}
	}
	if !s.SSCC.IsZero() && (len(s.SSCC.String()) != 18 || !ValidMod10(s.SSCC.String())) {
		t.Fatalf("invalid SSCC accepted: %q", s.SSCC)
	}
}

func assertScanCombination(t *testing.T, s Scan) {
	t.Helper()
	if s.SSCC.IsZero() && s.GTIN.IsZero() && s.ContentGTIN.IsZero() {
		t.Fatalf("a successful scan identifies nothing: %+v", s)
	}
	if !s.GTIN.IsZero() && !s.ContentGTIN.IsZero() {
		t.Fatalf("AI 01 and AI 02 together: %+v", s)
	}
	if !s.ContentGTIN.IsZero() && (s.SSCC.IsZero() || s.Count < 1) {
		t.Fatalf("AI 02 without AI 00 and AI 37: %+v", s)
	}
}

func assertScanElementBounds(t *testing.T, s Scan) {
	t.Helper()
	if s.Count < 0 || s.Count > 99999999 {
		t.Fatalf("count out of range: %d", s.Count)
	}
	if len(s.Lot) > 20 {
		t.Fatalf("lot longer than 20: %q", s.Lot)
	}
}

func TestParseScanTruncatedSymbologyIdentifier(t *testing.T) {
	for _, in := range []string{"]", "]C", "]E"} {
		if _, err := ParseScan(in); !errors.Is(err, ErrInvalidScan) {
			t.Errorf("ParseScan(%q) error = %v, want ErrInvalidScan", in, err)
		}
	}
}

// The GS1 AI-encodable character set 82: every other byte must be refused in a lot.
func TestLotCharacterSet(t *testing.T) {
	const allowed = "!\"%&'()*+,-./0123456789:;<=>?ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"
	if len(allowed) != 82 {
		t.Fatalf("fixture has %d characters, want 82", len(allowed))
	}
	base := "]C1" + "01" + "13041090004824" + "10"
	for c := 0; c < 256; c++ {
		in := base + "A" + string([]byte{byte(c)}) + "B"
		_, err := ParseScan(in)
		inSet := strings.IndexByte(allowed, byte(c)) >= 0
		switch {
		case inSet && err != nil:
			t.Errorf("byte %#x (%q) must be allowed in a lot: %v", c, rune(c), err)
		case !inSet && c != 0x1d && err == nil:
			t.Errorf("byte %#x (%q) must be refused in a lot", c, rune(c))
		}
	}
}
