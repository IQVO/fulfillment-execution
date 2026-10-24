// Package gs1 holds the GS1 identification keys every bounded context in the fleet shares:
// the GTIN and SSCC value objects, the mod-10 check digit, and a parser for what a barcode
// scanner hands back (EAN/UPC, ITF-14 and GS1-128 element strings).
//
// It is pure domain code: standard library only, no I/O, no clock. The identical source is
// used by every service that needs to validate a GTIN or SSCC, so the same input is accepted
// or rejected everywhere without a runtime dependency between services.
//
// DO NOT EDIT a copy of this package inside a service. The canonical source is
// warehouse-harness-template (internal/domain/shared/gs1); copies must stay byte-identical and
// are checked by the template's drift sensors. Change the template first, then re-sync.
//
// Rules implemented (fleet contracts G2, G3, G7 and the floor scan grammar):
//
//   - A GTIN is accepted as 8, 12, 13 or 14 digits, its mod-10 check digit is verified, and it is
//     normalised to the 14-digit, zero-left-padded form. The all-zero number is rejected.
//   - GTIN-14 indicator digit 9 means variable measure; IsVariableMeasure/RejectVariableMeasure
//     let a caller refuse it (the fleet does not support it yet).
//   - An SSCC is 18 digits (extension digit, 16 digits, check digit) with a valid check digit.
//   - A scan resolves AIs 00 (SSCC), 01 (GTIN), 02 (content GTIN), 37 (count), 10 (lot) and
//     17 (expiry). AI 01 and AI 02 never occur together, and AI 02 requires AI 00 and AI 37.
package gs1
