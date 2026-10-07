package facilitylayout

import (
	"context"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// PermissiveLookup is the default ports.LocationRoleLookup: it never
// contacts facility-layout and always reports Known=false, which
// RegisterStation treats as "no role info available, register the
// station with whatever locationCode was supplied, unchecked" (fail-open).
// Selected via LOCATION_ROLE_MODE (default "permissive") — so existing tests, CI
// and deployments that do not set the env var see identical behaviour to
// before this feature existed.
type PermissiveLookup struct{}

// NewPermissiveLookup constructs a PermissiveLookup.
func NewPermissiveLookup() *PermissiveLookup {
	return &PermissiveLookup{}
}

func (PermissiveLookup) GetRole(_ context.Context, _ string) (ports.LocationRoleInfo, error) {
	return ports.LocationRoleInfo{Known: false}, nil
}
