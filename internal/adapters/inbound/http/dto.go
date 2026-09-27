package http

import (
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

type createTaskRequest struct {
	Type                 string    `json:"type"`
	CPT                  time.Time `json:"cpt"`
	OrderRef             string    `json:"orderRef"`
	RequiredCapabilities []string  `json:"requiredCapabilities"`
	Fragile              bool      `json:"fragile"`
}

// isValidTaskType reports whether v is one of the process-path task types
// the published contract (apis/openapi.yaml's TaskType enum) defines. The
// HTTP surface must not mint tasks carrying free-form type strings every
// spec-generated client would reject (found by Schemathesis contract
// testing — scripts/contract-test.sh).
func isValidTaskType(v string) bool {
	switch task.Type(v) {
	case task.Pick, task.Pack, task.Slam, task.Rebin:
		return true
	default:
		return false
	}
}

// validate reports the first missing required field, or "" if the request
// is well-formed enough to hand to the use case.
func (r createTaskRequest) validate() string {
	switch {
	case r.Type == "":
		return "type is required"
	case !isValidTaskType(r.Type):
		return "type must be one of PICK, PACK, SLAM, REBIN"
	case r.CPT.IsZero():
		return "cpt is required"
	case r.OrderRef == "":
		return "orderRef is required"
	case len(r.RequiredCapabilities) == 0:
		return "requiredCapabilities must contain at least one capability"
	default:
		return ""
	}
}

type taskResponse struct {
	Id                   string     `json:"id"`
	Type                 string     `json:"type"`
	Status               string     `json:"status"`
	CPT                  time.Time  `json:"cpt"`
	OrderRef             string     `json:"orderRef"`
	RequiredCapabilities []string   `json:"requiredCapabilities"`
	Fragile              bool       `json:"fragile"`
	GiftWrap             bool       `json:"giftWrap"`
	LeaseStationId       *string    `json:"leaseStationId,omitempty"`
	LeaseExpiry          *time.Time `json:"leaseExpiry,omitempty"`
}

type claimNextRequest struct {
	TaskType string `json:"taskType"`
}

func (r claimNextRequest) validate() string {
	if r.TaskType == "" {
		return "taskType is required"
	}
	return ""
}

type renewLeaseRequest struct {
	StationId string `json:"stationId"`
}

func (r renewLeaseRequest) validate() string {
	if r.StationId == "" {
		return "stationId is required"
	}
	return ""
}

type completeTaskRequest struct {
	StationId string `json:"stationId"`
}

func (r completeTaskRequest) validate() string {
	if r.StationId == "" {
		return "stationId is required"
	}
	return ""
}

type sealPackageRequest struct {
	StationId string   `json:"stationId"`
	Contents  []string `json:"contents"`
}

// validate checks only that stationId (the claim owner) is present; an
// empty contents slice is a domain-level concern (pack.ErrNoScannedContents,
// mapped to 422), not a malformed request.
func (r sealPackageRequest) validate() string {
	if r.StationId == "" {
		return "stationId is required"
	}
	return ""
}

type packageResponse struct {
	Id                string   `json:"id"`
	OrderRef          string   `json:"orderRef"`
	Status            string   `json:"status"`
	ScannedContents   []string `json:"scannedContents"`
	FragileHandling   bool     `json:"fragileHandling"`
	GiftWrapRequested bool     `json:"giftWrapRequested"`
	SortLane          string   `json:"sortLane"`
}

type runSlamRequest struct {
	ActualWeight   float64 `json:"actualWeight"`
	ExpectedWeight float64 `json:"expectedWeight"`
}

type queueDepthResponse struct {
	TaskType string `json:"taskType"`
	Depth    int    `json:"depth"`
}

type installedCapacityResponse struct {
	Capability string `json:"capability"`
	Installed  int    `json:"installed"`
}

type expireLeasesResponse struct {
	Freed int `json:"freed"`
}

type sweepCPTMissesResponse struct {
	Reported int `json:"reported"`
}

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
	OrderRef                 string    `json:"orderRef"`
	LineId                   string    `json:"lineId"`
	RequiredLineIds          []string  `json:"requiredLineIds"`
	PackCPT                  time.Time `json:"packCpt"`
	PackRequiredCapabilities []string  `json:"packRequiredCapabilities"`
	PackFragile              bool      `json:"packFragile"`
	PackGiftWrap             bool      `json:"packGiftWrap"`
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
	case r.PackCPT.IsZero():
		return "packCpt is required"
	case len(r.PackRequiredCapabilities) == 0:
		return "packRequiredCapabilities must contain at least one capability"
	default:
		return ""
	}
}
