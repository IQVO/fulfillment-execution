package productclassificationcopy

import (
	"context"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// PermissiveLookup is the PRODUCT_CLASSIFICATION_MODE=permissive lookup: it
// reads nothing and always reports Known=false, which SealPackage treats as
// "no segregation constraint from this item" (fail-open). It is the default
// mode, so tests, CI and deployments that set nothing behave as before.
type PermissiveLookup struct{}

var _ ports.ProductClassificationLookup = PermissiveLookup{}

// NewPermissiveLookup constructs a PermissiveLookup.
func NewPermissiveLookup() PermissiveLookup {
	return PermissiveLookup{}
}

// GetClassification always reports Known=false.
func (PermissiveLookup) GetClassification(_ context.Context, _ string) (ports.ClassificationInfo, error) {
	return ports.ClassificationInfo{Known: false}, nil
}
