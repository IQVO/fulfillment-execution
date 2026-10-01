package usecases

import (
	"context"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// GetPackage is a read-only query returning one Package by id — the read
// side of the Package aggregate (ADR-0033). It exists so the SLAM operator
// and the outbound dock loader can tell, after POST /packages/{id}/slam
// returned its outcome-agnostic 204, whether a carton was LABELED (goes to
// the truck by its sortLane) or DIVERTED (goes to problem-solve).
type GetPackage struct {
	Packages ports.PackageRepo
}

// Execute returns the package with packageId, or ErrPackageNotFound when
// none exists — the same sentinel RunSlam returns, so both map to the
// package-not-found problem.
func (uc *GetPackage) Execute(ctx context.Context, packageId shared.PackageId) (*pack.Package, error) {
	p, err := uc.Packages.FindById(ctx, packageId)
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, ErrPackageNotFound
	}
	return p, nil
}
