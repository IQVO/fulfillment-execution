package usecases

import (
	"context"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// SealPackage runs the Pack path: a station scans an order's contents and
// seals them into a Package, referenced by the Pack task it is working.
//
// Execute is idempotent on taskId (REST_AUDIT.md's flagged gap): a retried
// POST /tasks/{id}/package-seals for a task that already has a sealed
// Package returns that existing Package rather than creating a second one.
// taskId is the natural dedupe key here — this endpoint is task-scoped
// (exactly one Package is ever sealed per Pack task in this domain), it is
// already the resource identifier in the URL, and unlike a client-supplied
// idempotency key it requires no new client contract. See
// ports.PackageRepo.FindByTaskId and migration 0011.
//
// ClassificationLookup is the per-scanned-SKU outbound classification read
// (see ADR-0010; since ADR-0039 it is answered from a local copy of
// product-master's events, not a REST call). It is nil-safe: a SealPackage built without it (as every
// pre-existing test in this package does) behaves exactly as before this
// feature — every scanned item is treated as unclassified, no segregation
// check runs, ScanItem's plain path is used. This is the same
// "additive, does not alter existing behaviour" discipline ADR-0009's
// Fragile flag and inventory-storage's own StowStock/LocationLookup
// wiring already established.
type SealPackage struct {
	Tasks                ports.TaskRepo
	Packages             ports.PackageRepo
	Publisher            ports.EventPublisher
	Clock                ports.Clock
	NewId                func() shared.PackageId
	ClassificationLookup ports.ProductClassificationLookup
	// CaseLevels answers ship-in-own-container (ADR 0056) from the local
	// PackView. Nil is valid: the ordinary path never reads it, and an
	// own-container seal against a nil CaseLevels finds no CASE level
	// (pack.ErrNoCaseLevel).
	CaseLevels ports.CaseLevelLookup
	// UnitOfWork brackets Save + Publish atomically (ADR 0020); nil runs
	// them back to back.
	UnitOfWork ports.UnitOfWork
}

// Execute validates that stationId holds the active, unexpired claim on
// taskId (which must be a Pack task; Task.VerifyHeldBy: a missing or expired
// lease is task.ErrNotClaimed, as in Complete/RenewLease; an active lease of
// another station is task.ErrNotOwner — ADR-0038), then scans contents and
// seals a new Package for the task's order.
//
// Before doing any of that, it checks whether a Package has already been
// sealed for taskId (idempotency guard, see the type doc comment): if one
// exists, it is returned immediately and Contents/stationId are not
// re-validated against it — a retried call is a no-op from the caller's
// perspective, not a second attempt to seal. Ownership (lease) and task
// existence are still checked on the FIRST call, exactly as before.
//
// When ClassificationLookup is wired, each scanned SKU is looked up live
// before being recorded, and its DOT hazard class (when Hazmat) is checked
// against every already-scanned item's hazard class for same-package
// segregation (pack.ErrPackageSegregationViolation on the first
// incompatible pair). A single SKU's lookup transport error fails open for
// that SKU only — it is recorded as unclassified rather than aborting the
// whole seal. This is a deliberate asymmetry with inventory-storage's
// StowStock, which fails closed when a classified SKU's placement lookup
// errors: that is a harder, at-rest safety gate, whereas this is a
// pack-time hint from a soft dependency that should not halt an active
// pack station over a lookup blip. See ADR-0010.
func (uc *SealPackage) Execute(ctx context.Context, taskId shared.TaskId, stationId shared.StationId, contents []string) (*pack.Package, error) {
	return uc.seal(ctx, taskId, stationId, contents, nil)
}

// ExecuteInOwnContainer is Execute for ship-in-own-container (ADR 0056,
// contract P11-3): the order line is lineQuantity eaches of the single SKU in
// contents, and when that is EXACTLY one full active CASE of the SKU the
// package ships in the case (its dimensions and weight, no cartonisation).
// It is refused, sealing nothing, when contents is not exactly one SKU
// (pack.ErrNotSingleLine), the SKU has no CASE level in the local PackView
// (pack.ErrNoCaseLevel) or only a retired one (pack.ErrCaseLevelRetired),
// lineQuantity is not the case's unitsPerPack (pack.ErrOwnContainerQuantityMismatch)
// or the case has no declared dimensions (pack.ErrCaseDimensionsUnknown).
// Like Execute it is idempotent on taskId: a retry returns the package
// already sealed for the task, whatever the retry's own quantity.
func (uc *SealPackage) ExecuteInOwnContainer(ctx context.Context, taskId shared.TaskId, stationId shared.StationId, contents []string, lineQuantity int) (*pack.Package, error) {
	return uc.seal(ctx, taskId, stationId, contents, &lineQuantity)
}

// seal is the shared body of Execute (ownLineQuantity nil: the ordinary
// cartonised path, unchanged) and ExecuteInOwnContainer.
func (uc *SealPackage) seal(ctx context.Context, taskId shared.TaskId, stationId shared.StationId, contents []string, ownLineQuantity *int) (*pack.Package, error) {
	if existing, err := uc.Packages.FindByTaskId(ctx, taskId); err != nil {
		return nil, err
	} else if existing != nil {
		return existing, nil
	}

	t, err := uc.Tasks.FindById(ctx, taskId)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, ErrTaskNotFound
	}
	if t.Type() != task.Pack {
		return nil, ErrWrongTaskType
	}
	if err := t.VerifyHeldBy(stationId, uc.Clock.Now()); err != nil {
		return nil, err
	}

	p := pack.New(uc.NewId(), t.OrderRef(), taskId, t.Fragile(), t.GiftWrap())
	for _, sku := range contents {
		hazardClass := uc.lookupHazardClass(ctx, sku)
		if err := p.ScanItemWithClass(sku, hazardClass); err != nil {
			return nil, err
		}
	}
	if ownLineQuantity != nil {
		oc, err := uc.ownContainerFor(ctx, contents, *ownLineQuantity)
		if err != nil {
			return nil, err
		}
		if err := p.WithOwnContainer(oc); err != nil {
			return nil, err
		}
	}
	if err := p.Seal(); err != nil {
		return nil, err
	}
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Packages.Save(ctx, p); err != nil {
			return err
		}
		return uc.Publisher.Publish(ctx, sealedEvent(p, uc.Clock.Now()))
	})
	if err != nil {
		return nil, err
	}
	return p, nil
}

// lookupHazardClass returns sku's DOT hazard class (1-9), or 0 meaning "no
// hazard class" — either because ClassificationLookup is nil (permissive
// by construction), the lookup returned Known=false, the SKU is not
// Hazmat, or the lookup itself errored (fail-open per item; see Execute's
// doc comment for why this differs from inventory-storage's StowStock).
func (uc *SealPackage) lookupHazardClass(ctx context.Context, sku string) int {
	if uc.ClassificationLookup == nil {
		return 0
	}
	info, err := uc.ClassificationLookup.GetClassification(ctx, sku)
	if err != nil {
		// Fail-open for this single SKU only: a soft-dependency lookup
		// blip at an active pack station must not halt the whole seal.
		return 0
	}
	if !info.Known || !info.Hazmat {
		return 0
	}
	return info.DOTHazardClass
}

// ownContainerFor decides ship-in-own-container for an order line of
// lineQuantity eaches: exactly one SKU in contents, whose active CASE level in
// the local PackView has unitsPerPack == lineQuantity and declared
// dimensions. The PackView read is a local table (no product-master call).
func (uc *SealPackage) ownContainerFor(ctx context.Context, contents []string, lineQuantity int) (pack.OwnContainer, error) {
	if len(contents) == 0 {
		return pack.OwnContainer{}, pack.ErrNoScannedContents
	}
	if len(contents) != 1 {
		return pack.OwnContainer{}, pack.ErrNotSingleLine
	}
	sku := contents[0]
	var records []ports.PackRecord
	if uc.CaseLevels != nil {
		var err error
		if records, err = uc.CaseLevels.FindCaseLevels(ctx, sku); err != nil {
			return pack.OwnContainer{}, err
		}
	}
	levels := make([]pack.CaseLevel, 0, len(records))
	for _, r := range records {
		l := pack.CaseLevel{GTIN: r.GTIN, UnitsPerPack: r.UnitsPerPack, Version: r.Version, EffectiveFrom: r.EffectiveFrom, EffectiveTo: r.EffectiveTo}
		if d := r.Dims; d != nil {
			l.Dims = &pack.Dimensions{LengthMM: d.LengthMM, WidthMM: d.WidthMM, HeightMM: d.HeightMM, WeightG: d.WeightG}
		}
		levels = append(levels, l)
	}
	active, err := pack.ResolveActiveCase(levels, uc.Clock.Now())
	if err != nil {
		return pack.OwnContainer{}, err
	}
	return pack.NewOwnContainerFromCase(lineQuantity, active)
}

// sealedEvent is PackageSealed for p: the plain event for an ordinary
// package, the own-container variant (ADR 0056) when p ships in its case.
func sealedEvent(p *pack.Package, at time.Time) shared.PackageSealed {
	oc := p.OwnContainer()
	if oc == nil {
		return shared.NewPackageSealed(p.Id(), at)
	}
	return shared.NewPackageSealedInOwnContainer(p.Id(), shared.OwnContainerFact{
		Source: string(oc.Source), GTIN: oc.GTIN,
		LengthMM: oc.Dims.LengthMM, WidthMM: oc.Dims.WidthMM, HeightMM: oc.Dims.HeightMM, WeightG: oc.Dims.WeightG,
	}, at)
}
