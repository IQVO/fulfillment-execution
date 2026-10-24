package ports

import (
	"context"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// PackRecord is one pack level of a SKU as carried by product-master's
// ProductPackLevelDeclared / ProductPackLevelRetired events (ADR 0054): the
// local PackView row, keyed by the 14-digit GTIN. A retired level keeps its
// row (the GTIN stays resolvable for packs already on the shelf) with
// EffectiveTo set.
type PackRecord struct {
	GTIN          string
	SKU           string
	Level         task.PackLevel
	UnitsPerPack  int
	ChildGTIN     string
	Version       int64
	EffectiveFrom time.Time
	EffectiveTo   *time.Time
	// Dims are the DECLARED dimensions and weight of the level (the optional
	// `dims` of ProductPackLevelDeclared); nil when product-master declared
	// none. Ship-in-own-container reads them (ADR 0056).
	Dims *PackDims
}

// PackDims are a pack level's dimensions (millimetres) and weight (grams), as
// product-master declares them.
type PackDims struct {
	LengthMM int
	WidthMM  int
	HeightMM int
	WeightG  int
}

// PackViewLookup is the read side of the PackView: resolve a scanned GTIN to
// the SKU and level it identifies. ok is false for a GTIN the view has never
// seen. The pick-confirm hot path reads only this local copy; it never calls
// product-master.
type PackViewLookup interface {
	FindByGTIN(ctx context.Context, gtin string) (rec PackRecord, ok bool, err error)
}

// PackViewCopy is the write side of the PackView, fed by the pack-level
// consumer. Writes join the transaction a UnitOfWork bound to ctx so the
// processed-event claim and the write commit together.
type PackViewCopy interface {
	// UpsertIfNewer inserts rec or replaces the stored row of the same GTIN
	// when rec.Version is greater (a re-declaration clears EffectiveTo). It
	// reports whether a row was written.
	UpsertIfNewer(ctx context.Context, rec PackRecord) (applied bool, err error)
	// RetireIfNewer end-dates the stored row of gtin when version is greater
	// than the stored one. A GTIN the view has no row for is not an error
	// (applied=false): a retirement never creates a level.
	RetireIfNewer(ctx context.Context, gtin string, effectiveTo time.Time, version int64) (applied bool, err error)
}

// CaseLevelLookup is the read side ship-in-own-container needs (ADR 0056):
// every CASE level of a SKU the local PackView holds, active or retired, so
// the domain can tell "no case" from "case retired". It is the same local
// copy as PackViewLookup; it never calls product-master.
type CaseLevelLookup interface {
	FindCaseLevels(ctx context.Context, sku string) ([]PackRecord, error)
}
