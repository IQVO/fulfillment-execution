package gs1

const glnLength = 13

// GLN is a Global Location Number (AI 410 ship-to, AI 413 ship-for, and the party and site
// identifier in general): 13 digits, the last one a mod-10 check digit. The zero value is
// "no GLN". GLNs are comparable.
type GLN struct{ v string }

// ParseGLN accepts exactly 13 ASCII digits with a valid mod-10 check digit. Surrounding
// whitespace is NOT trimmed (that is an adapter concern).
func ParseGLN(s string) (GLN, error) {
	if len(s) != glnLength {
		return GLN{}, invalidGLN("length must be 13 digits")
	}
	if !allDigits(s) {
		return GLN{}, invalidGLN("only ASCII digits are allowed")
	}
	if !ValidMod10(s) {
		return GLN{}, invalidGLN("check digit does not match")
	}
	return GLN{v: s}, nil
}

// String is the 13-digit form, or "" for the zero value.
func (g GLN) String() string { return g.v }

// IsZero reports whether g is the zero value (no GLN).
func (g GLN) IsZero() bool { return g.v == "" }
