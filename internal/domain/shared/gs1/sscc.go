package gs1

import (
	"strconv"
	"strings"
)

const ssccLength = 18

// SSCC is a Serial Shipping Container Code (AI 00): extension digit, 16 digits of company
// prefix and serial reference, check digit. The zero value is "no SSCC".
type SSCC struct{ v string }

// ParseSSCC accepts exactly 18 ASCII digits with a valid mod-10 check digit.
func ParseSSCC(s string) (SSCC, error) {
	if len(s) != ssccLength {
		return SSCC{}, invalidSSCC("length must be 18 digits")
	}
	if !allDigits(s) {
		return SSCC{}, invalidSSCC("only ASCII digits are allowed")
	}
	if !ValidMod10(s) {
		return SSCC{}, invalidSSCC("check digit does not match")
	}
	return SSCC{v: s}, nil
}

// NewSSCC builds an SSCC from a 17-digit payload by appending the check digit.
func NewSSCC(payload string) (SSCC, error) {
	if len(payload) != ssccLength-1 {
		return SSCC{}, invalidSSCC("payload length must be 17 digits")
	}
	cd, err := Mod10CheckDigit(payload)
	if err != nil {
		return SSCC{}, invalidSSCC("only ASCII digits are allowed")
	}
	return ParseSSCC(payload + strconv.Itoa(cd))
}

// BuildSSCC generates an SSCC from its parts: one extension digit (0-9), the GS1 company
// prefix (7 to 10 digits) and a serial reference. The serial is zero-padded on the left so the
// first 17 digits are filled (17 - 1 - len(prefix) digits of serial reference), then the mod-10
// check digit is appended. A serial that is empty, non-numeric or longer than the room left
// is refused.
//
// Pure formatting and validation: no state, no counter, no randomness. Allocating a serial
// that is unique per company prefix is the calling service's job.
//
// (The older NewSSCC(payload) keeps its meaning: it only appends a check digit.)
func BuildSSCC(extension int, companyPrefix, serial string) (SSCC, error) {
	if extension < 0 || extension > 9 {
		return SSCC{}, invalidSSCC("extension digit must be 0 to 9")
	}
	if len(companyPrefix) < 7 || len(companyPrefix) > 10 {
		return SSCC{}, invalidSSCC("company prefix must be 7 to 10 digits")
	}
	if !allDigits(companyPrefix) {
		return SSCC{}, invalidSSCC("company prefix: only ASCII digits are allowed")
	}
	if serial == "" || !allDigits(serial) {
		return SSCC{}, invalidSSCC("serial reference must be one or more ASCII digits")
	}
	room := ssccLength - 2 - len(companyPrefix)
	if len(serial) > room {
		return SSCC{}, invalidSSCC("serial reference does not fit after the company prefix")
	}
	payload := strconv.Itoa(extension) + companyPrefix + strings.Repeat("0", room-len(serial)) + serial
	return NewSSCC(payload)
}

// HumanReadable is the human-readable interpretation printed under the barcode: the AI in
// parentheses followed by the 18 digits, "(00)376104250021234569". "" for the zero value.
func (s SSCC) HumanReadable() string {
	if s.v == "" {
		return ""
	}
	return "(00)" + s.v
}

// String is the 18-digit form, or "" for the zero value.
func (s SSCC) String() string { return s.v }

// IsZero reports whether s is the zero value (no SSCC).
func (s SSCC) IsZero() bool { return s.v == "" }

// ExtensionDigit is the first digit. -1 for the zero value.
func (s SSCC) ExtensionDigit() int {
	if s.v == "" {
		return -1
	}
	return int(s.v[0] - '0')
}

// CheckDigit is the last digit. -1 for the zero value.
func (s SSCC) CheckDigit() int {
	if s.v == "" {
		return -1
	}
	return int(s.v[ssccLength-1] - '0')
}
