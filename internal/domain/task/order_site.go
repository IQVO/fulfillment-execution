package task

import (
	"errors"
	"fmt"
)

// ErrInvalidOrderSite is returned by ParseOrderSite for a site code that is
// not empty and not made only of upper-case ASCII letters and digits.
var ErrInvalidOrderSite = errors.New("task: invalid order site")

// ParseOrderSite validates the fulfillment site code an order named
// (WorkReleased.site_id on order work). A site code has the same shape as a
// facility-layout site code, ^[A-Z0-9]+$: matching is exact, with no trimming
// and no case folding, because the code is compared verbatim downstream
// (outbound-shipping looks up the CPT schedule of exactly that site). The
// empty string is valid and means "no site". On error the returned string is
// empty.
func ParseOrderSite(siteId string) (string, error) {
	for i := 0; i < len(siteId); i++ {
		if c := siteId[i]; (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return "", fmt.Errorf("%w: %q must match ^[A-Z0-9]+$ or be empty", ErrInvalidOrderSite, siteId)
		}
	}
	return siteId, nil
}

// OrderSiteId returns the fulfillment site the ORDER this task's work was
// released for named, or "" when unknown (see the orderSiteId field).
func (t *Task) OrderSiteId() string { return t.orderSiteId }

// WithOrderSiteId stamps the order's fulfillment site and returns the task,
// for use at creation time and when a repository rehydrates the column. A
// site that is empty or does not pass ParseOrderSite leaves the task without
// one: the field is optional and informational, so the domain never holds a
// malformed code and a malformed one can never fail a release (the same
// tolerance WithSourceLineNo applies to an out-of-range line). The previous
// value is replaced either way.
func (t *Task) WithOrderSiteId(siteId string) *Task {
	site, err := ParseOrderSite(siteId)
	if err != nil {
		site = ""
	}
	t.orderSiteId = site
	return t
}
