package gs1

import (
	"fmt"
	"strings"
	"time"
)

// Symbology is the AIM symbology identifier a scanner prefixes to the data.
type Symbology string

const (
	// SymbologyEANUPC (]E0): EAN-13, EAN-8, UPC-A and UPC-E, delivered as 8, 12 or 13 digits.
	SymbologyEANUPC Symbology = "]E0"
	// SymbologyITF14 (]I1): ITF-14, 14 digits.
	SymbologyITF14 Symbology = "]I1"
	// SymbologyGS1128 (]C1): GS1-128, an element string with FNC1 (ASCII GS) separators.
	SymbologyGS1128 Symbology = "]C1"
)

// groupSeparator is ASCII GS (0x1D), what scanners emit for FNC1 after a variable-length AI.
const groupSeparator = '\x1d'

const (
	maxLotLength   = 20
	maxCountLength = 8
	expiryLength   = 6
)

// Date is a calendar date without a time zone (AI 17 expiry). The zero value is "no date".
type Date struct {
	Year  int
	Month time.Month
	Day   int
}

// IsZero reports whether d is the zero value.
func (d Date) IsZero() bool { return d == Date{} }

// String is ISO 8601 (2026-12-31), or "" for the zero value.
func (d Date) String() string {
	if d.IsZero() {
		return ""
	}
	return fmt.Sprintf("%04d-%02d-%02d", d.Year, int(d.Month), d.Day)
}

// Scan is the result of parsing one barcode read. Absent elements keep their zero value.
type Scan struct {
	Symbology   Symbology // as received; empty when the scanner stripped the identifier
	SSCC        SSCC      // AI 00
	GTIN        GTIN      // AI 01, or the data of an EAN/UPC/ITF-14 symbol
	ContentGTIN GTIN      // AI 02, the GTIN of the items inside a logistic unit
	Count       int       // AI 37, number of ContentGTIN items; 0 when absent
	Lot         string    // AI 10, 1 to 20 characters of the GS1 82 character set
	Expiry      Date      // AI 17; DD=00 is read as the last day of the month
}

// ParseScan parses what a scanner delivers.
//
//   - ]E0 + 8/12/13 digits, ]I1 + 14 digits: one GTIN.
//   - ]C1 + element string: AIs 00, 01, 02, 10, 17, 37. Variable-length AIs (10, 37) end at a GS
//     character or at the end of the data; fixed-length AIs are followed directly by the next AI.
//   - No identifier (a keyboard wedge that strips it): 8, 12, 13 or 14 digits are a GTIN,
//     anything else is read as an element string.
//
// The expiry year is read as 20YY. Every error wraps ErrInvalidScan; on error the Scan is zero.
func ParseScan(raw string) (Scan, error) {
	if raw == "" {
		return Scan{}, invalidScan("empty scan")
	}
	if raw[0] != ']' {
		return parseUnprefixed(raw)
	}
	if len(raw) < 3 {
		return Scan{}, invalidScan("truncated symbology identifier")
	}
	sym, body := Symbology(raw[:3]), raw[3:]
	switch sym {
	case SymbologyEANUPC:
		return parseSingleGTIN(sym, body, false)
	case SymbologyITF14:
		return parseSingleGTIN(sym, body, true)
	case SymbologyGS1128:
		return parseElementString(sym, body)
	default:
		return Scan{}, ErrUnsupportedSymbology
	}
}

func parseUnprefixed(raw string) (Scan, error) {
	if allDigits(raw) {
		switch len(raw) {
		case 8, 12, 13, 14:
			return parseSingleGTIN("", raw, false)
		}
	}
	return parseElementString("", raw)
}

func parseSingleGTIN(sym Symbology, data string, fourteenOnly bool) (Scan, error) {
	if fourteenOnly && len(data) != gtinLength {
		return Scan{}, invalidScan("ITF-14 carries exactly 14 digits")
	}
	g, err := ParseGTIN(data)
	if err != nil {
		return Scan{}, fmt.Errorf("%w: %w", ErrInvalidScan, err)
	}
	return Scan{Symbology: sym, GTIN: g}, nil
}

// aiSpec describes one application identifier: a fixed width, or a maximum width ending at GS.
type aiSpec struct {
	fixed    int // > 0: fixed number of characters
	max      int // > 0 (and fixed == 0): variable length, 1..max characters
	assignTo func(*Scan, string) error
}

var aiTable = map[string]aiSpec{
	"00": {fixed: ssccLength, assignTo: setSSCC},
	"01": {fixed: gtinLength, assignTo: setGTIN},
	"02": {fixed: gtinLength, assignTo: setContentGTIN},
	"17": {fixed: expiryLength, assignTo: setExpiry},
	"10": {max: maxLotLength, assignTo: setLot},
	"37": {max: maxCountLength, assignTo: setCount},
}

func parseElementString(sym Symbology, data string) (Scan, error) {
	scan := Scan{Symbology: sym}
	seen := map[string]bool{}
	rest := data
	for rest != "" {
		ai, value, remaining, err := nextElement(rest)
		if err != nil {
			return Scan{}, err
		}
		if seen[ai] {
			return Scan{}, ErrDuplicateAI
		}
		seen[ai] = true
		if err := aiTable[ai].assignTo(&scan, value); err != nil {
			return Scan{}, err
		}
		rest = remaining
	}
	if err := checkCombination(scan); err != nil {
		return Scan{}, err
	}
	return scan, nil
}

// nextElement splits the leading AI and its value off s.
func nextElement(s string) (ai, value, remaining string, err error) {
	ai = s[:min(2, len(s))]
	spec, ok := aiTable[ai]
	if !ok {
		return "", "", "", ErrUnknownAI
	}
	s = s[2:]
	if spec.fixed > 0 {
		if len(s) < spec.fixed {
			return "", "", "", invalidScan("element shorter than its fixed length")
		}
		return ai, s[:spec.fixed], s[spec.fixed:], nil
	}
	value, remaining, _ = strings.Cut(s, string(groupSeparator))
	if len(value) < 1 || len(value) > spec.max {
		return "", "", "", invalidScan("variable-length element has an invalid length")
	}
	return ai, value, remaining, nil
}

// checkCombination enforces the label-level rules: AI 01 never with AI 02; AI 02 needs AI 00
// and AI 37 (and AI 37 only exists with AI 02); the scan must identify something.
func checkCombination(s Scan) error {
	if !s.GTIN.IsZero() && !s.ContentGTIN.IsZero() {
		return ErrConflictingAIs
	}
	if !s.ContentGTIN.IsZero() && (s.SSCC.IsZero() || s.Count == 0) {
		return ErrMissingAI
	}
	if s.Count != 0 && s.ContentGTIN.IsZero() {
		return ErrMissingAI
	}
	if s.SSCC.IsZero() && s.GTIN.IsZero() && s.ContentGTIN.IsZero() {
		return ErrMissingAI
	}
	return nil
}

func wrapScan(err error) error { return fmt.Errorf("%w: %w", ErrInvalidScan, err) }

func setSSCC(s *Scan, v string) error {
	x, err := ParseSSCC(v)
	if err != nil {
		return wrapScan(err)
	}
	s.SSCC = x
	return nil
}

func setGTIN(s *Scan, v string) error {
	x, err := ParseGTIN(v)
	if err != nil {
		return wrapScan(err)
	}
	s.GTIN = x
	return nil
}

func setContentGTIN(s *Scan, v string) error {
	x, err := ParseGTIN(v)
	if err != nil {
		return wrapScan(err)
	}
	s.ContentGTIN = x
	return nil
}

func setLot(s *Scan, v string) error {
	for i := 0; i < len(v); i++ {
		if !inCharacterSet82(v[i]) {
			return invalidScan("lot has a character outside the GS1 82 character set")
		}
	}
	s.Lot = v
	return nil
}

func setCount(s *Scan, v string) error {
	if !allDigits(v) {
		return invalidScan("count must be digits")
	}
	n := 0
	for i := 0; i < len(v); i++ {
		n = n*10 + int(v[i]-'0')
	}
	if n < 1 {
		return invalidScan("count must be at least 1")
	}
	s.Count = n
	return nil
}

func setExpiry(s *Scan, v string) error {
	if !allDigits(v) {
		return invalidScan("expiry must be YYMMDD digits")
	}
	yy := int(v[0]-'0')*10 + int(v[1]-'0')
	mm := int(v[2]-'0')*10 + int(v[3]-'0')
	dd := int(v[4]-'0')*10 + int(v[5]-'0')
	if mm < 1 || mm > 12 {
		return invalidScan("expiry month out of range")
	}
	year, month := 2000+yy, time.Month(mm)
	last := time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
	if dd == 0 {
		dd = last // GS1: day 00 means the last day of the month
	}
	if dd > last {
		return invalidScan("expiry day out of range")
	}
	s.Expiry = Date{Year: year, Month: month, Day: dd}
	return nil
}

// inCharacterSet82 reports whether c is in the GS1 AI-encodable character set 82 (printable
// ASCII without space, # $ @ [ \ ] ^ ` { | } ~).
func inCharacterSet82(c byte) bool {
	// % & ' ( ) * + , - . / 0-9 : ; < = > ?
	if c >= '%' && c <= '?' {
		return true
	}
	if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' {
		return true
	}
	return c == '!' || c == '"' || c == '_'
}
