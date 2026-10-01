package usecases

import (
	"context"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// GetPackagesByOrderRef is a read-only query returning every Package
// sealed for an order reference (ADR-0033) — the Package-side twin of
// GetTasksByOrderRef. An order normally yields one carton, but a
// multi-carton order or a re-pack after a SLAM diversion yields several.
type GetPackagesByOrderRef struct {
	Packages ports.PackageRepo
}

// Execute returns every package recorded for orderRef. An unknown orderRef
// returns an empty slice, not an error — the same convention as
// GetTasksByOrderRef.
func (uc *GetPackagesByOrderRef) Execute(ctx context.Context, orderRef shared.OrderRef) ([]*pack.Package, error) {
	return uc.Packages.FindByOrderRef(ctx, orderRef)
}
