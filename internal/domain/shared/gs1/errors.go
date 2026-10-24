package gs1

import (
	"errors"
	"fmt"
)

// Sentinel errors. Messages describe the rule, never echo the input (it may be hostile).
var (
	// ErrInvalidCheckInput: the input to the check digit calculation is not 1 to 17 ASCII digits.
	ErrInvalidCheckInput = errors.New("gs1: check digit input must be 1 to 17 ASCII digits")
	// ErrInvalidGTIN: wrong length, non-digit, bad mod-10 check digit, or the all-zero number.
	ErrInvalidGTIN = errors.New("gs1: invalid GTIN")
	// ErrInvalidSSCC: not 18 ASCII digits with a valid mod-10 check digit.
	ErrInvalidSSCC = errors.New("gs1: invalid SSCC")
	// ErrVariableMeasure: GTIN-14 with indicator digit 9 (variable measure trade item).
	ErrVariableMeasure = errors.New("gs1: variable measure GTIN (indicator digit 9) is not supported")

	// ErrInvalidScan is the root of every scan parsing error: errors.Is(err, ErrInvalidScan)
	// holds for all the errors below and for malformed data.
	ErrInvalidScan = errors.New("gs1: invalid scan")
	// ErrUnsupportedSymbology: a symbology identifier other than ]E0, ]I1 and ]C1.
	ErrUnsupportedSymbology = fmt.Errorf("%w: unsupported symbology identifier", ErrInvalidScan)
	// ErrUnknownAI: an application identifier outside 00, 01, 02, 10, 17, 37.
	ErrUnknownAI = fmt.Errorf("%w: unknown application identifier", ErrInvalidScan)
	// ErrDuplicateAI: the same application identifier twice in one element string.
	ErrDuplicateAI = fmt.Errorf("%w: duplicate application identifier", ErrInvalidScan)
	// ErrConflictingAIs: AI 01 and AI 02 on the same label.
	ErrConflictingAIs = fmt.Errorf("%w: AI 01 and AI 02 never appear together", ErrInvalidScan)
	// ErrMissingAI: AI 02 without AI 00 and AI 37, AI 37 without AI 02, or nothing identifying.
	ErrMissingAI = fmt.Errorf("%w: required application identifier missing", ErrInvalidScan)
)

func invalidGTIN(reason string) error { return fmt.Errorf("%w: %s", ErrInvalidGTIN, reason) }
func invalidSSCC(reason string) error { return fmt.Errorf("%w: %s", ErrInvalidSSCC, reason) }
func invalidScan(reason string) error { return fmt.Errorf("%w: %s", ErrInvalidScan, reason) }
