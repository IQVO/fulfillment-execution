package http

import (
	"time"
)

type registerStationRequest struct {
	StationId    string   `json:"stationId"`
	Capabilities []string `json:"capabilities"`
	// LocationCode is an optional facility-layout LocationCode identifying
	// where this station physically sits (ADR-0024). Omitted entirely
	// means unchecked — no role validation is attempted.
	LocationCode string `json:"locationCode,omitempty"`
}

func (r registerStationRequest) validate() string {
	switch {
	case r.StationId == "":
		return "stationId is required"
	case len(r.Capabilities) == 0:
		return "capabilities must contain at least one capability"
	default:
		return ""
	}
}

type stationResponse struct {
	Id           string   `json:"id"`
	Capabilities []string `json:"capabilities"`
	Occupied     bool     `json:"occupied"`
	// LocationCode is omitted entirely (not defaulted to "") when the
	// station has no recorded locationCode — mirrors the omit-when-unknown
	// discipline of wes-work-planning's travelDistanceM (ADR-0024).
	LocationCode string `json:"locationCode,omitempty"`
}

type checkInStationRequest struct {
	OccupantId string `json:"occupantId"`
}

func (r checkInStationRequest) validate() string {
	if r.OccupantId == "" {
		return "occupantId is required"
	}
	return ""
}

type arriveAtRebinRequest struct {
	OrderRef                 string     `json:"orderRef"`
	LineId                   string     `json:"lineId"`
	RequiredLineIds          []string   `json:"requiredLineIds"`
	PackCPT                  *time.Time `json:"packCpt"`
	PackRequiredCapabilities []string   `json:"packRequiredCapabilities"`
	PackFragile              bool       `json:"packFragile"`
	PackGiftWrap             bool       `json:"packGiftWrap"`
}

// validate reports the first missing required field. requiredLineIds
// establishes the order's fixed required set on the FIRST call for an
// orderRef — every subsequent call for the same order must pass the SAME
// set (see ArriveAtRebin's doc comment), so it is required on every call
// rather than only the first, since the caller (not this handler) is the
// one that knows whether this is the first arrival.
func (r arriveAtRebinRequest) validate() string {
	switch {
	case r.OrderRef == "":
		return "orderRef is required"
	case r.LineId == "":
		return "lineId is required"
	case len(r.RequiredLineIds) == 0:
		return "requiredLineIds must contain at least one line id"
	case r.PackCPT == nil:
		return "packCpt is required"
	case len(r.PackRequiredCapabilities) == 0:
		return "packRequiredCapabilities must contain at least one capability"
	default:
		return ""
	}
}
