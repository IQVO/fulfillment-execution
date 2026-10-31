package gs1

import "strconv"

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
