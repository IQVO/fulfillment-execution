package pack

import (
	"errors"
	"time"
)

// ContainerSource says what a package ships in. Today the only own container
// is a product-master CASE level (ADR 0056); an ordinary carton has no
// container source at all (OwnContainer() is nil).
type ContainerSource string

const (
	// ContainerSourceCase: the package is one full, sealed CASE of the SKU,
	// shipped in its own container with the case's dimensions and weight.
	ContainerSourceCase ContainerSource = "CASE"
)

var (
	// ErrNotSingleLine: ship-in-own-container needs exactly one scanned
	// order line (one SKU entry in the package contents).
	ErrNotSingleLine = errors.New("package: ship-in-own-container needs exactly one order line in the package")
	// ErrOwnContainerQuantityMismatch: the order line quantity is not
	// exactly the units per pack of the SKU's active CASE level.
	ErrOwnContainerQuantityMismatch = errors.New("package: the order line quantity is not exactly one full case")
	// ErrNoCaseLevel: the local PackView knows no CASE level of the SKU
	// that is in effect.
	ErrNoCaseLevel = errors.New("package: the SKU has no active CASE level")
	// ErrCaseLevelRetired: the SKU's CASE level exists but is end-dated.
	ErrCaseLevelRetired = errors.New("package: the SKU's CASE level is retired")
	// ErrCaseDimensionsUnknown: the active CASE level carries no usable
	// dimensions and weight (product-master declares them optionally), so
	// there is nothing to ship by.
	ErrCaseDimensionsUnknown = errors.New("package: the active CASE level has no dimensions and weight")
)

// Dimensions are the physical size and weight of a container: millimetres
// and grams, the units of product-master's pack-level dims.
type Dimensions struct {
	LengthMM int
	WidthMM  int
	HeightMM int
	WeightG  int
}

// Valid reports whether every measure is strictly positive.
func (d Dimensions) Valid() bool {
	return d.LengthMM > 0 && d.WidthMM > 0 && d.HeightMM > 0 && d.WeightG > 0
}

// CaseLevel is one CASE pack level of a SKU as the local PackView holds it.
type CaseLevel struct {
	GTIN          string
	UnitsPerPack  int
	Dims          *Dimensions
	Version       int64
	EffectiveFrom time.Time
	// EffectiveTo is nil while the level is open-ended.
	EffectiveTo *time.Time
}

// activeAt reports whether the level is in effect at instant at: started
// (inclusive) and not yet ended (the end is exclusive).
func (c CaseLevel) activeAt(at time.Time) bool {
	if c.EffectiveFrom.After(at) {
		return false
	}
	return c.EffectiveTo == nil || at.Before(*c.EffectiveTo)
}

// ResolveActiveCase picks the CASE level in effect at instant at. When two
// levels overlap (a supersession whose retirement has not arrived yet) the
// one with the greater version wins: it is the newer declaration. With no
// active level it reports ErrCaseLevelRetired when some level has ended, and
// ErrNoCaseLevel when none ever applied (none known, or only future-dated).
func ResolveActiveCase(levels []CaseLevel, at time.Time) (CaseLevel, error) {
	var (
		best    CaseLevel
		found   bool
		retired bool
	)
	for _, l := range levels {
		switch {
		case l.activeAt(at):
			if !found || l.Version > best.Version {
				best, found = l, true
			}
		case !l.EffectiveFrom.After(at):
			// started and not active: it has ended.
			retired = true
		}
	}
	switch {
	case found:
		return best, nil
	case retired:
		return CaseLevel{}, ErrCaseLevelRetired
	default:
		return CaseLevel{}, ErrNoCaseLevel
	}
}

// OwnContainer is the container a package ships in when the order line is
// exactly one full case: where it comes from, the case GTIN and its
// dimensions and weight, copied at seal time (a later re-declaration of the
// case dims must not change a sealed package).
type OwnContainer struct {
	Source ContainerSource
	GTIN   string
	Dims   Dimensions
}

// NewOwnContainerFromCase builds the own container for an order line of
// lineQuantity eaches against the active CASE level c. The quantity must be
// EXACTLY c.UnitsPerPack: one each short is a partial case and one more is
// a case plus loose eaches, neither of which can ship in the case. The
// dimensions are checked after the quantity.
func NewOwnContainerFromCase(lineQuantity int, c CaseLevel) (OwnContainer, error) {
	if lineQuantity != c.UnitsPerPack {
		return OwnContainer{}, ErrOwnContainerQuantityMismatch
	}
	if c.Dims == nil || !c.Dims.Valid() {
		return OwnContainer{}, ErrCaseDimensionsUnknown
	}
	return OwnContainer{Source: ContainerSourceCase, GTIN: c.GTIN, Dims: *c.Dims}, nil
}

// WithOwnContainer records that the package ships in oc. Allowed only while
// the package is open (before Seal), like ScanItem.
func (p *Package) WithOwnContainer(oc OwnContainer) error {
	if p.status != Open {
		return ErrAlreadySealed
	}
	p.ownContainer = &oc
	return nil
}

// RestoreOwnContainer sets the own container of a rehydrated package, in any
// status; nil leaves it an ordinary carton. Persistence adapters only.
func (p *Package) RestoreOwnContainer(oc *OwnContainer) *Package {
	p.ownContainer = oc
	return p
}

// OwnContainer is the container the package ships in, or nil for an
// ordinary (cartonised) package.
func (p *Package) OwnContainer() *OwnContainer { return p.ownContainer }
