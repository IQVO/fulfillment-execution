package gs1

import (
	"errors"
	"testing"
)

func TestParseGTINNormalisesToFourteenDigits(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		want      string
		indicator int
		check     int
	}{
		{"gtin-8", "96385074", "00000096385074", 0, 4},
		{"gtin-8 second", "73513537", "00000073513537", 0, 7},
		{"gtin-12 upc-a", "036000291452", "00036000291452", 0, 2},
		{"gtin-13 ean", "4006381333931", "04006381333931", 0, 1},
		{"gtin-13 ean wikipedia", "5901234123457", "05901234123457", 0, 7},
		{"gtin-14 each", "03041090004827", "03041090004827", 0, 7},
		{"gtin-14 case", "13041090004824", "13041090004824", 1, 4},
		{"gtin-14 pallet", "23041090004821", "23041090004821", 2, 1},
		{"gtin-14 indicator 8", "83041090004823", "83041090004823", 8, 3},
		{"already padded", "00036000291452", "00036000291452", 0, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g, err := ParseGTIN(tc.in)
			if err != nil {
				t.Fatalf("ParseGTIN(%q) error = %v", tc.in, err)
			}
			if g.String() != tc.want {
				t.Errorf("String() = %q, want %q", g.String(), tc.want)
			}
			if g.IndicatorDigit() != tc.indicator {
				t.Errorf("IndicatorDigit() = %d, want %d", g.IndicatorDigit(), tc.indicator)
			}
			if g.CheckDigit() != tc.check {
				t.Errorf("CheckDigit() = %d, want %d", g.CheckDigit(), tc.check)
			}
			if g.IsZero() {
				t.Error("IsZero() = true for a parsed GTIN")
			}
		})
	}
}

func TestParseGTINEquivalentLengthsAreEqual(t *testing.T) {
	a, err := ParseGTIN("036000291452")
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParseGTIN("0036000291452")
	if err != nil {
		t.Fatal(err)
	}
	c, err := ParseGTIN("00036000291452")
	if err != nil {
		t.Fatal(err)
	}
	if a != b || b != c {
		t.Errorf("GTIN-12, GTIN-13 and GTIN-14 forms of one number must compare equal: %v %v %v", a, b, c)
	}
}

func TestParseGTINRejects(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{"empty", ""},
		{"seven digits", "9638507"},
		{"nine digits", "963850740"},
		{"ten digits", "9638507400"},
		{"eleven digits", "96385074000"},
		{"fifteen digits", "040600381333931"},
		{"eighteen digits sscc length", "376104250021234569"},
		{"bad check digit gtin-8", "96385075"},
		{"bad check digit gtin-12", "036000291453"},
		{"bad check digit gtin-13", "4006381333932"},
		{"bad check digit gtin-14", "13041090004825"},
		{"letters", "4006381333A31"},
		{"leading space", " 4006381333931"},
		{"trailing newline", "4006381333931\n"},
		{"inner dash", "4006-381333931"},
		{"plus sign", "+4006381333931"},
		{"fullwidth digits", "４００６３８１３３３９３１"},
		{"all zeros gtin-14", "00000000000000"},
		{"all zeros gtin-8", "00000000"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g, err := ParseGTIN(tc.in)
			if !errors.Is(err, ErrInvalidGTIN) {
				t.Fatalf("ParseGTIN(%q) error = %v, want ErrInvalidGTIN", tc.in, err)
			}
			if !g.IsZero() {
				t.Errorf("ParseGTIN(%q) returned non-zero GTIN %v with an error", tc.in, g)
			}
		})
	}
}

func TestZeroGTIN(t *testing.T) {
	var g GTIN
	if !g.IsZero() {
		t.Error("zero value must report IsZero")
	}
	if g.String() != "" {
		t.Errorf("zero value String() = %q, want empty", g.String())
	}
}

func TestVariableMeasure(t *testing.T) {
	vm, err := ParseGTIN("93041090004820")
	if err != nil {
		t.Fatal(err)
	}
	if !vm.IsVariableMeasure() {
		t.Error("indicator digit 9 must be variable measure")
	}
	if err := RejectVariableMeasure(vm); !errors.Is(err, ErrVariableMeasure) {
		t.Errorf("RejectVariableMeasure(indicator 9) = %v, want ErrVariableMeasure", err)
	}
	for _, in := range []string{"03041090004827", "13041090004824", "83041090004823", "4006381333931"} {
		g, err := ParseGTIN(in)
		if err != nil {
			t.Fatal(err)
		}
		if g.IsVariableMeasure() {
			t.Errorf("%s: IsVariableMeasure() = true", in)
		}
		if err := RejectVariableMeasure(g); err != nil {
			t.Errorf("%s: RejectVariableMeasure() = %v, want nil", in, err)
		}
	}
	// A GTIN-13 starting with 9 (an ISBN-like prefix) pads to indicator 0: it is NOT variable measure.
	g13, err := ParseGTIN("9780306406157")
	if err != nil {
		t.Fatal(err)
	}
	if g13.IsVariableMeasure() {
		t.Error("a GTIN-13 starting with 9 must not be variable measure")
	}
}

func TestNewGTINComputesCheckDigit(t *testing.T) {
	tests := []struct{ payload, want string }{
		{"0304109000482", "03041090004827"},
		{"1304109000482", "13041090004824"},
		{"2304109000482", "23041090004821"},
		{"400638133393", "04006381333931"},
		{"9638507", "00000096385074"},
		{"03600029145", "00036000291452"},
	}
	for _, tc := range tests {
		g, err := NewGTIN(tc.payload)
		if err != nil {
			t.Fatalf("NewGTIN(%q) error = %v", tc.payload, err)
		}
		if g.String() != tc.want {
			t.Errorf("NewGTIN(%q) = %q, want %q", tc.payload, g.String(), tc.want)
		}
	}
	for _, bad := range []string{"", "123456", "12345678901234", "A304109000482", "0000000"} {
		if _, err := NewGTIN(bad); !errors.Is(err, ErrInvalidGTIN) {
			t.Errorf("NewGTIN(%q) error = %v, want ErrInvalidGTIN", bad, err)
		}
	}
}

// The worked case/each relation from the GS1 General Specifications: a case GTIN-14 wraps the
// each GTIN's 13 digit body behind an indicator digit and carries its own check digit.
func TestCaseGTINWrapsEachGTIN(t *testing.T) {
	each, err := ParseGTIN("03041090004827")
	if err != nil {
		t.Fatal(err)
	}
	cs, err := ParseGTIN("13041090004824")
	if err != nil {
		t.Fatal(err)
	}
	if each == cs {
		t.Fatal("case and each GTIN must be distinct identities")
	}
	if each.String()[1:13] != cs.String()[1:13] {
		t.Errorf("case %s must contain the each body %s", cs, each)
	}
	if cs.IndicatorDigit() != 1 || each.IndicatorDigit() != 0 {
		t.Errorf("indicators = (%d,%d), want (1,0)", cs.IndicatorDigit(), each.IndicatorDigit())
	}
}

func TestZeroGTINDigitAccessors(t *testing.T) {
	var g GTIN
	if g.IndicatorDigit() != -1 || g.CheckDigit() != -1 {
		t.Errorf("zero GTIN digits = (%d,%d), want (-1,-1)", g.IndicatorDigit(), g.CheckDigit())
	}
	if g.IsVariableMeasure() {
		t.Error("zero GTIN is not variable measure")
	}
	if err := RejectVariableMeasure(g); err != nil {
		t.Errorf("RejectVariableMeasure(zero) = %v", err)
	}
}
