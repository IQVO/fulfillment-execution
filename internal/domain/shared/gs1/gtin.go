package gs1

import (
	"strconv"
	"strings"
)

const gtinLength = 14

// GTIN is a Global Trade Item Number held in its 14-digit, zero-left-padded form. The zero
// value is "no GTIN". GTINs are comparable: the GTIN-12, GTIN-13 and GTIN-14 spellings of one
// number are equal after parsing.
type GTIN struct{ v string }

// ParseGTIN accepts 8, 12, 13 or 14 ASCII digits with a valid mod-10 check digit and
// normalises to 14 digits. Surrounding whitespace is NOT trimmed (that is an adapter concern).
func ParseGTIN(s string) (GTIN, error) {
	switch len(s) {
	case 8, 12, 13, 14:
	default:
		return GTIN{}, invalidGTIN("length must be 8, 12, 13 or 14 digits")
	}
	if !allDigits(s) {
		return GTIN{}, invalidGTIN("only ASCII digits are allowed")
	}
	if !ValidMod10(s) {
		return GTIN{}, invalidGTIN("check digit does not match")
	}
	if strings.Trim(s, "0") == "" {
		return GTIN{}, invalidGTIN("the all-zero number is not a GTIN")
	}
	return GTIN{v: strings.Repeat("0", gtinLength-len(s)) + s}, nil
}

// NewGTIN builds a GTIN from a payload of 7, 11, 12 or 13 digits by appending the check digit.
func NewGTIN(payload string) (GTIN, error) {
	switch len(payload) {
	case 7, 11, 12, 13:
	default:
		return GTIN{}, invalidGTIN("payload length must be 7, 11, 12 or 13 digits")
	}
	cd, err := Mod10CheckDigit(payload)
	if err != nil {
		return GTIN{}, invalidGTIN("only ASCII digits are allowed")
	}
	return ParseGTIN(payload + strconv.Itoa(cd))
}

// String is the 14-digit form, or "" for the zero value.
func (g GTIN) String() string { return g.v }

// IsZero reports whether g is the zero value (no GTIN).
func (g GTIN) IsZero() bool { return g.v == "" }

// IndicatorDigit is the first digit of the 14-digit form: 0 for items (GTIN-8/12/13), 1-8 for
// packaging levels, 9 for variable measure. -1 for the zero value.
func (g GTIN) IndicatorDigit() int {
	if g.v == "" {
		return -1
	}
	return int(g.v[0] - '0')
}

// CheckDigit is the last digit. -1 for the zero value.
func (g GTIN) CheckDigit() int {
	if g.v == "" {
		return -1
	}
	return int(g.v[gtinLength-1] - '0')
}

// IsVariableMeasure reports indicator digit 9 (a GTIN-14 for a variable measure trade item).
func (g GTIN) IsVariableMeasure() bool { return g.v != "" && g.v[0] == '9' }

// RejectVariableMeasure returns ErrVariableMeasure for a variable measure GTIN, nil otherwise.
func RejectVariableMeasure(g GTIN) error {
	if g.IsVariableMeasure() {
		return ErrVariableMeasure
	}
	return nil
}
