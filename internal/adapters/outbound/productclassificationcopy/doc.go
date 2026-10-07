// Package productclassificationcopy implements ports.ProductClassificationLookup
// from a LOCAL copy of product-master's classifications (ADR-0039), replacing
// the retired HTTP client to inventory-storage (ADR-0010's live lookup).
//
// The copy is fed by the inbound ProductClassified consumer
// (internal/adapters/inbound/kafka) through ports.ProductClassificationCopy:
// one row per SKU, replaced only by a strictly newer product version. The
// read side maps a row to exactly what the HTTP client returned:
//
//   - row present: Known=true, Hazmat/Fragile from handling_tags,
//     DOTHazardClass from dot_hazard_class (0 when unset);
//   - row absent: Known=false and a nil error (the client's 404 case);
//   - storage failure: a non-nil error (the client's transport-error case),
//     which SealPackage fails open on for that SKU only.
//
// Three implementations: PostgresStore (production, table
// product_classification_copy), MemoryStore (DATABASE_URL unset: the
// in-memory configuration and unit tests) and PermissiveLookup
// (PRODUCT_CLASSIFICATION_MODE=permissive: never classified).
package productclassificationcopy

import (
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// Handling tag values of product-master's HandlingTag enum this context
// reads. The other tags (TemperatureSensitive, Oversized, HighValue) are
// stored in the copy but have no effect on SealPackage.
const (
	TagHazmat  = "Hazmat"
	TagFragile = "Fragile"
)

// toInfo maps a stored classification to the port's ClassificationInfo,
// the same mapping the retired HTTP client applied to inventory-storage's
// response body.
func toInfo(tags []string, dotHazardClass int) ports.ClassificationInfo {
	info := ports.ClassificationInfo{Known: true, DOTHazardClass: dotHazardClass}
	for _, tag := range tags {
		switch tag {
		case TagHazmat:
			info.Hazmat = true
		case TagFragile:
			info.Fragile = true
		}
	}
	return info
}
