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
	maxLotLength           = 20
	maxCountLength         = 8
	dateLength             = 6
	maxSerialLength        = 20
	maxPurchaseOrderLength = 30
)

// Date is a calendar date without a time zone (AIs 11, 13, 15, 17). The zero value is "no date".
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

	// Added with the label AIs; all keep their zero value when the AI is absent.
	ProductionDate Date   // AI 11; DD=00 is read as the last day of the month
	PackagingDate  Date   // AI 13; DD=00 is read as the last day of the month
	BestBefore     Date   // AI 15; DD=00 is read as the last day of the month
	Serial         string // AI 21, 1 to 20 characters of the GS1 82 character set
	PurchaseOrder  string // AI 400, customer purchase order number, 1 to 30 characters of set 82
	ShipToGLN      GLN    // AI 410, ship-to / deliver-to location
	ShipForGLN     GLN    // AI 413, ship-for / deliver-for location
}

// ParseScan parses what a scanner delivers.
//
//   - ]E0 + 8/12/13 digits, ]I1 + 14 digits: one GTIN.
//   - ]C1 + element string: AIs 00, 01, 02, 10, 11, 13, 15, 17, 21, 37, 400, 410, 413.
//     Variable-length AIs (10, 21, 37, 400) end at a GS character or at the end of the data;
//     fixed-length AIs (00, 01, 02, 11, 13, 15, 17, 410, 413) are followed directly by the next AI.
//   - No identifier (a keyboard wedge that strips it): 8, 12, 13 or 14 digits are a GTIN,
//     anything else is read as an element string.
//
// Dates (AIs 11, 13, 15, 17) are YYMMDD: month 01-12, day within the real month length, and
// DD=00 means "day not specified", read as the last day of the month. ParseScan reads the
// two-digit year as 20YY, as it always did. ParseScanAt applies the GS1 century window instead.
// Every error wraps ErrInvalidScan; on error the Scan is zero.
func ParseScan(raw string) (Scan, error) {
	return parseScan(raw, legacyYear)
}

// ParseScanAt is ParseScan with the GS1 century window for the two-digit year of every date
// AI: relative to refYear (the year the caller considers "now"; the package has no clock), a
// YY 51 to 99 above refYear's own YY is read in the previous century, a YY 50 or more below it
// in the next century, anything else in refYear's century. Example, refYear 2026: 00-76 mean
// 2000-2076 and 77-99 mean 1977-1999.
func ParseScanAt(raw string, refYear int) (Scan, error) {
	return parseScan(raw, windowYear(refYear))
}

// yearResolver turns the YY of a date AI into a four-digit year.
type yearResolver func(yy int) int

// legacyYear reads every YY as 20YY (the behaviour of ParseScan before ParseScanAt existed).
func legacyYear(yy int) int { return 2000 + yy }

// windowYear is the GS1 rule: the century is the one that puts the year within the window
// around refYear (51..99 above its YY: previous century; 50..99 below: next century).
func windowYear(refYear int) yearResolver {
	refYY := refYear % 100
	century := refYear - refYY
	return func(yy int) int {
		diff := yy - refYY
		if diff >= 51 {
			return century - 100 + yy
		}
		if diff <= -50 {
			return century + 100 + yy
		}
		return century + yy
	}
}

func parseScan(raw string, year yearResolver) (Scan, error) {
	if raw == "" {
		return Scan{}, invalidScan("empty scan")
	}
	if raw[0] != ']' {
		return parseUnprefixed(raw, year)
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
		return parseElementString(sym, body, year)
	default:
		return Scan{}, ErrUnsupportedSymbology
	}
}

func parseUnprefixed(raw string, year yearResolver) (Scan, error) {
	if allDigits(raw) {
		switch len(raw) {
		case 8, 12, 13, 14:
			return parseSingleGTIN("", raw, false)
		}
	}
	return parseElementString("", raw, year)
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
	dateAt   func(*Scan) *Date // date AIs: the field to fill (assignTo is nil)
}

var aiTable = map[string]aiSpec{
	"00":  {fixed: ssccLength, assignTo: setSSCC},
	"01":  {fixed: gtinLength, assignTo: setGTIN},
	"02":  {fixed: gtinLength, assignTo: setContentGTIN},
	"11":  {fixed: dateLength, dateAt: func(s *Scan) *Date { return &s.ProductionDate }},
	"13":  {fixed: dateLength, dateAt: func(s *Scan) *Date { return &s.PackagingDate }},
	"15":  {fixed: dateLength, dateAt: func(s *Scan) *Date { return &s.BestBefore }},
	"17":  {fixed: dateLength, dateAt: func(s *Scan) *Date { return &s.Expiry }},
	"10":  {max: maxLotLength, assignTo: setLot},
	"21":  {max: maxSerialLength, assignTo: setSerial},
	"37":  {max: maxCountLength, assignTo: setCount},
	"400": {max: maxPurchaseOrderLength, assignTo: setPurchaseOrder},
	"410": {fixed: glnLength, assignTo: setShipToGLN},
	"413": {fixed: glnLength, assignTo: setShipForGLN},
}

func parseElementString(sym Symbology, data string, year yearResolver) (Scan, error) {
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
		if err := assignElement(&scan, aiTable[ai], value, year); err != nil {
			return Scan{}, err
		}
		rest = remaining
	}
	if err := checkCombination(scan); err != nil {
		return Scan{}, err
	}
	return scan, nil
}

func assignElement(scan *Scan, spec aiSpec, value string, year yearResolver) error {
	if spec.dateAt == nil {
		return spec.assignTo(scan, value)
	}
	d, err := parseDate(value, year)
	if err != nil {
		return err
	}
	*spec.dateAt(scan) = d
	return nil
}

// lookupAI finds the AI at the start of s: two digits first, then three (400, 410, 413).
func lookupAI(s string) (string, aiSpec, bool) {
	for _, n := range [...]int{2, 3} {
		if len(s) < n {
			break
		}
		if spec, ok := aiTable[s[:n]]; ok {
			return s[:n], spec, true
		}
	}
	return "", aiSpec{}, false
}

// nextElement splits the leading AI and its value off s.
func nextElement(s string) (ai, value, remaining string, err error) {
	ai, spec, ok := lookupAI(s)
	if !ok {
		return "", "", "", ErrUnknownAI
	}
	s = s[len(ai):]
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
