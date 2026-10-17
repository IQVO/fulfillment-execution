package task

import (
	"slices"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// Placement is where the operator must go to work a pooled task: the site and
// the two points of the trip (ADR 0049). FromLocation is where the operator
// goes first, ToLocation where the operator ends.
type Placement struct {
	SiteId       string
	FromLocation string
	ToLocation   string
}

// Placement reports where this task is worked, derived only from what the
// task itself carries; ok is false when no location (or no site) is
// determinable, in which case the task cannot be announced to the location-
// aware consumers (TaskAvailable is not raised on the wire).
//
// Today only a RELOCATE task carries locations: its planned bins. An Assign
// leaves the source open and a Vacate the destination (the operator scans
// them at completion), so the one known bin stands for both ends of the trip.
// The site is the site segment of that bin's facility-layout LocationCode.
// No other task type carries a location, so none has a placement.
func (t *Task) Placement() (Placement, bool) {
	if t.relocation == nil {
		return Placement{}, false
	}
	from, to := t.relocation.FromBin, t.relocation.ToBin
	if from == "" {
		from = to
	}
	if to == "" {
		to = from
	}
	site, ok := shared.SiteOfLocationCode(from)
	if !ok {
		site, ok = shared.SiteOfLocationCode(to)
	}
	if !ok {
		return Placement{}, false
	}
	return Placement{SiteId: site, FromLocation: from, ToLocation: to}, true
}

// PriorityBand is the opaque string published as TaskAvailable.priority_band:
// equal for exactly the tasks claimNext orders as peers (ADR 0049).
// claimNext(stationId, taskType) scans only tasks of one type, earliest CPT
// first, so the ordering key is (task type, CPT instant): same type and same
// instant are peers (the repository may return them in either order), a
// different type is never compared, and a different instant is strictly
// ordered. The instant is normalised to UTC with full precision so two
// representations of one instant produce one band.
func (t *Task) PriorityBand() string {
	return string(t.taskType) + ":" + t.cpt.Time().UTC().Format(time.RFC3339Nano)
}

// RequiredCapabilityNames lists the capabilities claimNext matches a station
// against (Task.RequiredCapabilities), sorted for a deterministic wire form.
// It is empty (never nil) when the task requires none.
func (t *Task) RequiredCapabilityNames() []string {
	out := make([]string, 0, len(t.requiredCapabilities))
	for c := range t.requiredCapabilities {
		out = append(out, string(c))
	}
	slices.Sort(out)
	return out
}
