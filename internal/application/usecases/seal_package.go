package usecases

import (
	"context"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// SealPackage runs the Pack path: a station scans an order's contents and
// seals them into a Package, referenced by the Pack task it is working.
//
// Execute is idempotent on taskId (REST_AUDIT.md's flagged gap): a retried
// POST /tasks/{id}/seal-package for a task that already has a sealed
// Package returns that existing Package rather than creating a second one.
// taskId is the natural dedupe key here — this endpoint is task-scoped
// (exactly one Package is ever sealed per Pack task in this domain), it is
// already the resource identifier in the URL, and unlike a client-supplied
// idempotency key it requires no new client contract. See
// ports.PackageRepo.FindByTaskId and migration 0011.
//
// ClassificationLookup is the live, synchronous, per-scanned-SKU outbound
// read from inventory-storage's product-classification endpoint (see
// ADR-0010). It is nil-safe: a SealPackage built without it (as every
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
	if err := p.Seal(); err != nil {
		return nil, err
	}
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Packages.Save(ctx, p); err != nil {
			return err
		}
		return uc.Publisher.Publish(ctx, shared.NewPackageSealed(p.Id(), uc.Clock.Now()))
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
