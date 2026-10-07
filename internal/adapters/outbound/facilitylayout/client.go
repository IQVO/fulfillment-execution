// Package facilitylayout provides outbound ports.LocationRoleLookup
// implementations: an HTTP client that calls facility-layout's
// GET /locations/{locationCode} endpoint, and a permissive no-op used by
// default so existing tests, CI and deployments are unaffected
// (permissive-by-default, env-var-selected) — see ADR-0024.
package facilitylayout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
)

// DefaultTimeout bounds a single location-role lookup request, so a slow
// or hanging facility-layout does not stall RegisterStation indefinitely.
const DefaultTimeout = 5 * time.Second

// ErrUnexpectedStatus wraps a facility-layout response status this client
// does not have specific handling for (anything other than 200, 400, or
// 404).
var ErrUnexpectedStatus = errors.New("facility-layout: unexpected response status")

// HTTPDoer is the subset of *http.Client this adapter depends on, so unit
// tests can substitute a fake transport without a real server.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client is a plain net/http implementation of ports.LocationRoleLookup,
// calling facility-layout's GET /locations/{locationCode}.
type Client struct {
	baseURL string
	doer    HTTPDoer
}

// NewClient builds a Client against baseURL (e.g. from
// FACILITY_LAYOUT_BASE_URL). A nil doer defaults to an *http.Client with
// DefaultTimeout.
func NewClient(baseURL string, doer HTTPDoer) *Client {
	if doer == nil {
		doer = &http.Client{Timeout: DefaultTimeout}
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), doer: doer}
}

// locationSlotResponse is the subset of facility-layout's LocationSlot
// schema this client needs — just the resolved role.
type locationSlotResponse struct {
	Role string `json:"role"`
}

// GetRole calls facility-layout's GET /locations/{locationCode} endpoint
// for locationCode.
//
//   - A 400 or 404 (malformed code, or a code that does not resolve to a
//     slot) is treated as Known=false (fail-open): RegisterStation applies
//     no role constraint for a LocationCode facility-layout does not
//     recognize.
//   - Any transport error or unexpected status returns an error, which
//     RegisterStation's caller normalizes to the same fail-open behaviour —
//     a registration-time role check is a soft dependency, not a hard
//     placement gate (see ADR-0024; unlike inventory-storage's StowStock,
//     which fails closed on a classified SKU's placement-lookup error,
//     registering a station is not a safety-critical write).
func (c *Client) GetRole(ctx context.Context, locationCode string) (ports.LocationRoleInfo, error) {
	endpoint := fmt.Sprintf("%s/locations/%s", c.baseURL, url.PathEscape(locationCode))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return ports.LocationRoleInfo{}, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.doer.Do(req)
	if err != nil {
		return ports.LocationRoleInfo{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
		var body locationSlotResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return ports.LocationRoleInfo{}, err
		}
		return ports.LocationRoleInfo{Role: body.Role, Known: true}, nil
	case http.StatusBadRequest, http.StatusNotFound:
		return ports.LocationRoleInfo{Known: false}, nil
	default:
		return ports.LocationRoleInfo{}, fmt.Errorf("%w: %d", ErrUnexpectedStatus, resp.StatusCode)
	}
}
