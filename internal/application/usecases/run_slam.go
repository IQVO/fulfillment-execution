package usecases

import (
	"context"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// RunSlam runs the SLAM (Scan, Label, Apply, Manifest) weigh-check on a
// sealed package: a matching weight applies the shipping label and raises
// PackageManifested (the "SLAM pass" half of order-management ADR 0014
// §5's promise feedback loop — see ADR-0025), a discrepancy diverts the
// package instead (not manifested). Since ADR 0061 the manifest carries
// the measured actualWeight (weight_g) for ship correlation.
type RunSlam struct {
	Packages  ports.PackageRepo
	Publisher ports.EventPublisher
	Clock     ports.Clock
	// UnitOfWork brackets Save + Publish atomically (ADR 0020); nil runs
	// them back to back.
	UnitOfWork ports.UnitOfWork
}

// Execute weighs packageId against expectedWeight and records the outcome.
func (uc *RunSlam) Execute(ctx context.Context, packageId shared.PackageId, actualWeight, expectedWeight float64) error {
	p, err := uc.Packages.FindById(ctx, packageId)
	if err != nil {
		return err
	}
	if p == nil {
		return ErrPackageNotFound
	}

	now := uc.Clock.Now()
	labelApplied, err := p.Weigh(expectedWeight, actualWeight)
	if err != nil {
		return err
	}
	return atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Packages.Save(ctx, p); err != nil {
			return err
		}
		if !labelApplied {
			return uc.Publisher.Publish(ctx,
				shared.NewWeightDiscrepancyDetected(packageId, expectedWeight, actualWeight, now),
				shared.NewPackageDiverted(packageId, now),
			)
		}
		return uc.Publisher.Publish(ctx,
			shared.NewLabelApplied(packageId, now),
			shared.NewPackageManifestedWithWeight(packageId, p.OrderRef(), actualWeight, now),
		)
	})
}
