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
		{"unknown AI after valid", "]C1" + "01" + "13041090004824" + "22" + "SER1", ErrUnknownAI},
		{"AI 40 is not an AI (400 needs its third digit)", "]C1" + "01" + "13041090004824" + "40", ErrUnknownAI},
		{"three digit prefix 401 is unknown", "]C1" + "01" + "13041090004824" + "401" + "PO1", ErrUnknownAI},
		{"one digit after a valid element", "]C1" + "01" + "13041090004824" + "4", ErrUnknownAI},
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
		"]C1" + "01" + "13041090004824" + "15" + "261200" + "21" + "SER1" + gsep + "400" + "PO-9" + gsep + "410" + "4006381333931",
		"]C1" + "00" + "376104250021234569" + "413" + "4012345000009" + "11" + "260101" + "13" + "260102",
		"]C1" + "01" + "13041090004824" + "21" + strings.Repeat("S", 20), "]C1" + "01" + "13041090004824" + "400",
		"]C1" + "01" + "13041090004824" + "4", "]C1" + "01" + "13041090004824" + "41",
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
	assertLabelElementBounds(t, s)
}

func assertLabelElementBounds(t *testing.T, s Scan) {
	t.Helper()
	if len(s.Serial) > 20 || len(s.PurchaseOrder) > 30 {
		t.Fatalf("serial or purchase order too long: %q %q", s.Serial, s.PurchaseOrder)
	}
	for _, g := range []GLN{s.ShipToGLN, s.ShipForGLN} {
		if !g.IsZero() && (len(g.String()) != 13 || !ValidMod10(g.String())) {
			t.Fatalf("invalid GLN accepted: %q", g)
		}
	}
	for _, d := range []Date{s.ProductionDate, s.PackagingDate, s.BestBefore, s.Expiry} {
		if !d.IsZero() && (d.Month < 1 || d.Month > 12 || d.Day < 1 || d.Day > 31) {
			t.Fatalf("invalid date accepted: %+v", d)
		}
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

const (
	gtinCase  = "13041090004824"
	glnShipTo = "4006381333931"
	glnShipFr = "4012345000009"
)

func mustGLN(t testing.TB, s string) GLN {
	t.Helper()
	g, err := ParseGLN(s)
	if err != nil {
		t.Fatalf("fixture GLN %q: %v", s, err)
	}
	return g
}

// dateOf returns the date field the given date AI fills.
func dateOf(s Scan, ai string) Date {
	switch ai {
	case "11":
		return s.ProductionDate
	case "13":
		return s.PackagingDate
	case "15":
		return s.BestBefore
	}
	return s.Expiry
}

// All four date AIs share one validation: both sides of every boundary, on every AI.
func TestDateAIsShareOneValidation(t *testing.T) {
	tests := []struct {
		name   string
		yymmdd string
		want   Date // zero: must be refused
	}{
		{"month 00", "260001", Date{}},
		{"month 01", "260101", Date{Year: 2026, Month: time.January, Day: 1}},
		{"month 12", "261201", Date{Year: 2026, Month: time.December, Day: 1}},
		{"month 13", "261301", Date{}},
		{"day 00 in a 31 day month", "260100", Date{Year: 2026, Month: time.January, Day: 31}},
		{"day 00 in a 30 day month", "260400", Date{Year: 2026, Month: time.April, Day: 30}},
		{"day 00 in february", "260200", Date{Year: 2026, Month: time.February, Day: 28}},
		{"day 00 in february of a leap year", "240200", Date{Year: 2024, Month: time.February, Day: 29}},
		{"day 01", "260101", Date{Year: 2026, Month: time.January, Day: 1}},
		{"day 28 february", "260228", Date{Year: 2026, Month: time.February, Day: 28}},
		{"day 29 february common year", "260229", Date{}},
		{"day 29 february 2024", "240229", Date{Year: 2024, Month: time.February, Day: 29}},
		{"day 29 february 2000", "000229", Date{Year: 2000, Month: time.February, Day: 29}},
		{"day 30 february leap year", "240230", Date{}},
		{"day 30 april", "260430", Date{Year: 2026, Month: time.April, Day: 30}},
		{"day 31 april", "260431", Date{}},
		{"day 31 january", "260131", Date{Year: 2026, Month: time.January, Day: 31}},
		{"day 32 january", "260132", Date{}},
		{"year 99 reads as 2099", "991231", Date{Year: 2099, Month: time.December, Day: 31}},
		{"non digit", "26AB31", Date{}},
		{"five digits", "26123", Date{}},
	}
	for _, ai := range []string{"11", "13", "15", "17"} {
		for _, tc := range tests {
			t.Run(ai+" "+tc.name, func(t *testing.T) { checkDateAI(t, ai, tc.yymmdd, tc.want) })
		}
	}
}

func checkDateAI(t *testing.T, ai, yymmdd string, want Date) {
	t.Helper()
	in := "]C1" + "01" + gtinCase + ai + yymmdd
	got, err := ParseScan(in)
	if want.IsZero() {
		if !errors.Is(err, ErrInvalidScan) {
			t.Fatalf("ParseScan(%q) error = %v, want ErrInvalidScan", in, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("ParseScan(%q) error = %v", in, err)
	}
	if d := dateOf(got, ai); d != want {
		t.Fatalf("ParseScan(%q) date = %+v, want %+v", in, d, want)
	}
	for _, other := range []string{"11", "13", "15", "17"} {
		if other != ai && !dateOf(got, other).IsZero() {
			t.Errorf("AI %s filled the date of AI %s", ai, other)
		}
	}
}

// ParseScanAt applies the GS1 century window; both sides of each edge, and the leap rule in
// 1900, 2000, 2024 and 2100.
func TestParseScanAtCenturyWindow(t *testing.T) {
	tests := []struct {
		name string
		ref  int
		yy   string
		want int // year; 0: must be refused
		mmdd string
	}{
		{"ref 2026 yy 26", 2026, "26", 2026, "0615"},
		{"ref 2026 yy 00", 2026, "00", 2000, "0615"},
		{"ref 2026 yy 76 is 50 ahead", 2026, "76", 2076, "0615"},
		{"ref 2026 yy 77 is 51 ahead", 2026, "77", 1977, "0615"},
		{"ref 2026 yy 99", 2026, "99", 1999, "0615"},
		{"ref 2000 yy 50", 2000, "50", 2050, "0615"},
		{"ref 2000 yy 51", 2000, "51", 1951, "0615"},
		{"ref 2050 yy 99 is 49 ahead", 2050, "99", 2099, "0615"},
		{"ref 2050 yy 00 is 50 behind", 2050, "00", 2100, "0615"},
		{"ref 2051 yy 01 is 50 behind", 2051, "01", 2101, "0615"},
		{"ref 2051 yy 02 is 49 behind", 2051, "02", 2002, "0615"},
		{"ref 2076 yy 26 is 50 behind", 2076, "26", 2126, "0615"},
		{"ref 2076 yy 27 is 49 behind", 2076, "27", 2027, "0615"},
		{"ref 1999 yy 49", 1999, "49", 2049, "0615"},
		{"ref 1999 yy 50", 1999, "50", 1950, "0615"},
		{"1900 is not a leap year: 29 february refused", 1900, "00", 0, "0229"},
		{"1900: 28 february", 1900, "00", 1900, "0228"},
		{"1900: day 00 of february", 1900, "00", 1900, "0200"},
		{"2000 is a leap year", 2000, "00", 2000, "0229"},
		{"2024 is a leap year", 2024, "24", 2024, "0229"},
		{"2100 is not a leap year: 29 february refused", 2100, "00", 0, "0229"},
		{"2100: 28 february", 2100, "00", 2100, "0228"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := "]C1" + "01" + gtinCase + "15" + tc.yy + tc.mmdd
			got, err := ParseScanAt(in, tc.ref)
			if tc.want == 0 {
				if !errors.Is(err, ErrInvalidScan) {
					t.Fatalf("ParseScanAt(%q, %d) error = %v, want ErrInvalidScan", in, tc.ref, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseScanAt(%q, %d) error = %v", in, tc.ref, err)
			}
			if got.BestBefore.Year != tc.want {
				t.Errorf("ParseScanAt(%q, %d) year = %d, want %d", in, tc.ref, got.BestBefore.Year, tc.want)
			}
		})
	}
}

// ParseScan keeps reading 20YY (its behaviour before ParseScanAt existed); ParseScanAt moves
// the same bytes to the previous century.
func TestParseScanReadsTwentyYYParseScanAtWindows(t *testing.T) {
	in := "]C1" + "01" + gtinCase + "17" + "770101"
	got, err := ParseScan(in)
	if err != nil || got.Expiry.Year != 2077 {
		t.Fatalf("ParseScan(%q) = %+v, %v; want year 2077", in, got, err)
	}
	got, err = ParseScanAt(in, 2026)
	if err != nil || got.Expiry.Year != 1977 {
		t.Fatalf("ParseScanAt(%q, 2026) = %+v, %v; want year 1977", in, got, err)
	}
	if _, err := ParseScanAt("", 2026); !errors.Is(err, ErrInvalidScan) {
		t.Errorf("ParseScanAt empty: %v", err)
	}
}

func TestParseScanLabelAIs(t *testing.T) {
	const sscc = "376104250021234569"
	tests := []struct {
		name string
		in   string
		want func(t *testing.T) Scan
	}{
		{"serial last", "]C1" + "01" + gtinCase + "21" + "SN-0001", func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, GTIN: mustGTIN(t, gtinCase), Serial: "SN-0001"}
		}},
		{"serial then separator then best before", "]C1" + "01" + gtinCase + "21" + "S1" + gsep + "15" + "270630", func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, GTIN: mustGTIN(t, gtinCase), Serial: "S1",
				BestBefore: Date{Year: 2027, Month: time.June, Day: 30}}
		}},
		{"serial of one character", "]C1" + "01" + gtinCase + "21" + "7", func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, GTIN: mustGTIN(t, gtinCase), Serial: "7"}
		}},
		{"serial of twenty characters", "]C1" + "01" + gtinCase + "21" + "ABCDEFGHIJ0123456789", func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, GTIN: mustGTIN(t, gtinCase), Serial: "ABCDEFGHIJ0123456789"}
		}},
		{"purchase order of one character", "]C1" + "00" + sscc + "400" + "P", func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, SSCC: mustSSCC(t, sscc), PurchaseOrder: "P"}
		}},
		{"purchase order of thirty characters", "]C1" + "00" + sscc + "400" + "ABCDEFGHIJ0123456789ABCDEFGHIJ", func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, SSCC: mustSSCC(t, sscc), PurchaseOrder: "ABCDEFGHIJ0123456789ABCDEFGHIJ"}
		}},
		{"purchase order then ship-to", "]C1" + "00" + sscc + "400" + "PO/77" + gsep + "410" + glnShipTo, func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, SSCC: mustSSCC(t, sscc), PurchaseOrder: "PO/77", ShipToGLN: mustGLN(t, glnShipTo)}
		}},
		{"ship-to then ship-for, fixed length needs no separator", "]C1" + "00" + sscc + "410" + glnShipTo + "413" + glnShipFr, func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, SSCC: mustSSCC(t, sscc),
				ShipToGLN: mustGLN(t, glnShipTo), ShipForGLN: mustGLN(t, glnShipFr)}
		}},
		{"ship-for alone with gtin", "]C1" + "01" + gtinCase + "413" + glnShipFr, func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, GTIN: mustGTIN(t, gtinCase), ShipForGLN: mustGLN(t, glnShipFr)}
		}},
		{"dates 11 and 13 and 15 and 17 on one label", "]C1" + "01" + gtinCase + "11" + "260101" + "13" + "260102" + "15" + "270300" + "17" + "280229", func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, GTIN: mustGTIN(t, gtinCase),
				ProductionDate: Date{Year: 2026, Month: time.January, Day: 1},
				PackagingDate:  Date{Year: 2026, Month: time.January, Day: 2},
				BestBefore:     Date{Year: 2027, Month: time.March, Day: 31},
				Expiry:         Date{Year: 2028, Month: time.February, Day: 29}}
		}},
		{"a full pallet label", "]C1" + "00" + sscc + "02" + gtinCase + "37" + "24" + gsep + "10" + "LOT9" + gsep + "15" + "270630" + "21" + "S5" + gsep + "400" + "PO1" + gsep + "410" + glnShipTo + "413" + glnShipFr, func(t *testing.T) Scan {
			return Scan{Symbology: SymbologyGS1128, SSCC: mustSSCC(t, sscc), ContentGTIN: mustGTIN(t, gtinCase),
				Count: 24, Lot: "LOT9", BestBefore: Date{Year: 2027, Month: time.June, Day: 30}, Serial: "S5",
				PurchaseOrder: "PO1", ShipToGLN: mustGLN(t, glnShipTo), ShipForGLN: mustGLN(t, glnShipFr)}
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

func TestParseScanLabelAIRejects(t *testing.T) {
	const sscc = "376104250021234569"
	tests := []struct {
		name string
		in   string
		want error
	}{
		{"serial empty", "]C1" + "01" + gtinCase + "21" + gsep + "10" + "L", ErrInvalidScan},
		{"serial 21 characters", "]C1" + "01" + gtinCase + "21" + strings.Repeat("S", 21), ErrInvalidScan},
		{"serial with space", "]C1" + "01" + gtinCase + "21" + "S 1", ErrInvalidScan},
		{"serial without a key", "]C1" + "21" + "SER1", ErrMissingAI},
		{"duplicate serial", "]C1" + "01" + gtinCase + "21" + "A" + gsep + "21" + "B", ErrDuplicateAI},
		{"purchase order empty", "]C1" + "01" + gtinCase + "400" + gsep + "10" + "L", ErrInvalidScan},
		{"purchase order 31 characters", "]C1" + "01" + gtinCase + "400" + strings.Repeat("P", 31), ErrInvalidScan},
		{"purchase order with hash", "]C1" + "01" + gtinCase + "400" + "PO#1", ErrInvalidScan},
		{"duplicate purchase order", "]C1" + "01" + gtinCase + "400" + "A" + gsep + "400" + "B", ErrDuplicateAI},
		{"ship-to bad check digit", "]C1" + "01" + gtinCase + "410" + "4006381333932", ErrInvalidScan},
		{"ship-for bad check digit", "]C1" + "01" + gtinCase + "413" + "4012345000008", ErrInvalidScan},
		{"ship-to twelve digits", "]C1" + "01" + gtinCase + "410" + "400638133393", ErrInvalidScan},
		{"ship-to with a letter", "]C1" + "01" + gtinCase + "410" + "400638133393A", ErrInvalidScan},
		{"ship-to followed by a stray digit", "]C1" + "01" + gtinCase + "410" + glnShipTo + "5", ErrUnknownAI},
		{"duplicate ship-to", "]C1" + "01" + gtinCase + "410" + glnShipTo + "410" + glnShipTo, ErrDuplicateAI},
		{"separator after a fixed length GLN", "]C1" + "01" + gtinCase + "410" + glnShipTo + gsep + "413" + glnShipFr, ErrInvalidScan},
		{"ship-to alone identifies nothing", "]C1" + "410" + glnShipTo, ErrMissingAI},
		{"best before alone identifies nothing", "]C1" + "15" + "261231", ErrMissingAI},
		{"AI 02 rule still holds with the new AIs", "]C1" + "02" + gtinCase + "410" + glnShipTo, ErrMissingAI},
		{"AI 01 and AI 02 rule still holds with the new AIs", "]C1" + "00" + sscc + "01" + gtinCase + "02" + gtinCase + "37" + "1" + gsep + "410" + glnShipTo, ErrConflictingAIs},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseScan(tc.in)
			if !errors.Is(err, tc.want) || !errors.Is(err, ErrInvalidScan) {
				t.Fatalf("ParseScan(%q) error = %v, want %v", tc.in, err, tc.want)
			}
			if got != (Scan{}) {
				t.Errorf("partial scan %+v returned with an error", got)
			}
		})
	}
}

// The GS1 character set 82 holds for AI 21 and AI 400 as it does for AI 10.
func TestSerialAndPurchaseOrderCharacterSet(t *testing.T) {
	const allowed = "!\"%&'()*+,-./0123456789:;<=>?ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"
	for _, ai := range []string{"21", "400"} {
		base := "]C1" + "01" + gtinCase + ai
		for c := 0; c < 256; c++ {
			_, err := ParseScan(base + "A" + string([]byte{byte(c)}) + "B")
			inSet := strings.IndexByte(allowed, byte(c)) >= 0
			switch {
			case inSet && err != nil:
				t.Errorf("AI %s: byte %#x (%q) must be allowed: %v", ai, c, rune(c), err)
			case !inSet && c != 0x1d && err == nil:
				t.Errorf("AI %s: byte %#x (%q) must be refused", ai, c, rune(c))
			}
		}
	}
}

// An AI with no value at all is a malformed element, not an unknown AI.
func TestAIWithoutValueIsNotUnknown(t *testing.T) {
	for _, ai := range []string{"00", "02", "10", "11", "15", "17", "21", "37", "400", "410", "413"} {
		in := "]C1" + "01" + gtinCase + ai
		_, err := ParseScan(in)
		if !errors.Is(err, ErrInvalidScan) || errors.Is(err, ErrUnknownAI) {
			t.Errorf("ParseScan(%q) error = %v, want a malformed element, not ErrUnknownAI", in, err)
		}
	}
}
