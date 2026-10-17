package http

import (
	"encoding/json"
	"strings"
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
// testing — scripts/contract-test.sh). RELOCATE is not creatable: a
// RELOCATE task needs the slot-plan move only WorkReleased carries
// (ADR 0047).
func isValidTaskType(v string) bool {
	switch task.Type(v) {
	case task.Pick, task.Pack, task.Slam, task.Rebin, task.Dispatch, task.Arrival:
		return true
	default:
		return false
	}
}

// isClaimableTaskType is isValidTaskType plus RELOCATE and REPLENISH
// (apis/openapi.yaml's AnyTaskType enum): every task type a station may
// claim.
func isClaimableTaskType(v string) bool {
	return isValidTaskType(v) || task.Type(v) == task.Relocate || task.Type(v) == task.Replenish
}

// validate reports the first missing required field, or "" if the request
// is well-formed enough to hand to the use case.
func (r createTaskRequest) validate() string {
	switch {
	case r.Type == "":
		return "type is required"
	case !isValidTaskType(r.Type):
		return "type must be one of PICK, PACK, SLAM, REBIN, DISPATCH, ARRIVAL"
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
	// Relocation is the slot-plan move of a RELOCATE task (ADR 0047),
	// omitted for every other task type.
	Relocation *relocationResponse `json:"relocation,omitempty"`
	// Replenishment is the pick-face replenishment move of a REPLENISH
	// task (ADR 0050), omitted for every other task type.
	Replenishment *replenishmentResponse `json:"replenishment,omitempty"`
}

// replenishmentResponse is a REPLENISH task's planned move, plus the
// executed move once the task is completed.
type replenishmentResponse struct {
	RequestId string                         `json:"requestId"`
	SKU       string                         `json:"sku"`
	FromBin   string                         `json:"fromBin"`
	ToBin     string                         `json:"toBin"`
	Quantity  int                            `json:"quantity"`
	Priority  string                         `json:"priority"`
	Executed  *executedReplenishmentResponse `json:"executed,omitempty"`
}

// executedReplenishmentResponse is the move recorded at completion.
type executedReplenishmentResponse struct {
	Quantity int    `json:"quantity"`
	FromBin  string `json:"fromBin"`
	ToBin    string `json:"toBin"`
}

func toReplenishmentResponse(t *task.Task) *replenishmentResponse {
	m := t.Replenishment()
	if m == nil {
		return nil
	}
	resp := &replenishmentResponse{RequestId: m.RequestId, SKU: m.SKU, FromBin: m.FromBin, ToBin: m.ToBin, Quantity: m.Quantity, Priority: string(m.Priority)}
	if r := t.ReplenishmentResult(); r != nil {
		resp.Executed = &executedReplenishmentResponse{Quantity: r.Quantity, FromBin: r.FromBin, ToBin: r.ToBin}
	}
	return resp
}

// relocationResponse is a RELOCATE task's planned move, plus the executed
// move once the task is completed. An open side (fromBin of an Assign,
// toBin of a Vacate) is omitted: the operator scans it at completion.
type relocationResponse struct {
	PlanId   string                      `json:"planId"`
	MoveSeq  int                         `json:"moveSeq"`
	Kind     string                      `json:"kind"`
	SKU      string                      `json:"sku"`
	FromBin  string                      `json:"fromBin,omitempty"`
	ToBin    string                      `json:"toBin,omitempty"`
	Ref      string                      `json:"ref"`
	Executed *executedRelocationResponse `json:"executed,omitempty"`
}

// executedRelocationResponse is the move recorded at completion.
type executedRelocationResponse struct {
	Quantity int    `json:"quantity"`
	FromBin  string `json:"fromBin"`
	ToBin    string `json:"toBin"`
}

func toRelocationResponse(t *task.Task) *relocationResponse {
	m := t.Relocation()
	if m == nil {
		return nil
	}
	resp := &relocationResponse{PlanId: m.PlanId, MoveSeq: m.MoveSeq, Kind: string(m.Kind), SKU: m.SKU, FromBin: m.FromBin, ToBin: m.ToBin, Ref: m.Ref()}
	if r := t.RelocationResult(); r != nil {
		resp.Executed = &executedRelocationResponse{Quantity: r.Quantity, FromBin: r.FromBin, ToBin: r.ToBin}
	}
	return resp
}

type claimNextRequest struct {
	TaskType string `json:"taskType"`
}

func (r claimNextRequest) validate() string {
	switch {
	case r.TaskType == "":
		return "taskType is required"
	case !isClaimableTaskType(r.TaskType):
		// apis/openapi.yaml claimNextTask: taskType is the AnyTaskType enum,
		// and a value outside it is a 400 (BadRequest), not an empty queue (409).
		return "taskType must be one of PICK, PACK, SLAM, REBIN, DISPATCH, ARRIVAL, RELOCATE, REPLENISH"
	default:
		return ""
	}
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

// completeTaskRequest is POST /tasks/{id}/complete. Quantity, FromBin and
// ToBin are the RELOCATE confirmation (ADR 0047) and the REPLENISH
// confirmation (ADR 0050): optional on the wire, required by the domain for
// a RELOCATE or REPLENISH task (the quantity always; the bins, for RELOCATE,
// only where the move left them open, and for REPLENISH as optional scans
// that must equal the planned bins) and rejected for any other task. Pointers
// tell an absent field from an explicit 0 / "", which the contract forbids.
type completeTaskRequest struct {
	StationId string  `json:"stationId"`
	Quantity  *int    `json:"quantity,omitempty"`
	FromBin   *string `json:"fromBin,omitempty"`
	ToBin     *string `json:"toBin,omitempty"`
}

func (r completeTaskRequest) validate() string {
	switch {
	case r.StationId == "":
		return "stationId is required"
	case r.Quantity != nil && *r.Quantity < 1:
		return "quantity must be at least 1"
	case r.FromBin != nil && isBlankPerContract(*r.FromBin):
		return "fromBin must not be blank"
	case r.ToBin != nil && isBlankPerContract(*r.ToBin):
		return "toBin must not be blank"
	default:
		return ""
	}
}

// isBlankPerContract reports whether s has no character outside ECMA-262's
// \s class, the set the OpenAPI contract's `pattern: '\S'` uses for fromBin
// and toBin (JSON Schema patterns are ECMA-262). strings.TrimSpace follows
// unicode.IsSpace instead, which counts U+0085 as space and U+FEFF as not, so
// request validation and the schema disagreed on both (Schemathesis finding).
func isBlankPerContract(s string) bool {
	return strings.TrimFunc(s, isContractWhitespace) == ""
}

// isContractWhitespace is ECMA-262's \s (WhiteSpace + LineTerminator): TAB,
// LF, VT, FF, CR, SPACE, NBSP, ZWNBSP (U+FEFF), U+1680, U+2000-U+200A,
// U+2028, U+2029, U+202F, U+205F, U+3000.
func isContractWhitespace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', '\u00a0', '\ufeff', '\u1680', '\u2028', '\u2029', '\u202f', '\u205f', '\u3000':
		return true
	}
	return r >= '\u2000' && r <= '\u200a'
}

// confirmation maps the optional RELOCATE inputs onto the domain's
// confirmation; all absent is the zero value (no relocation input).
func (r completeTaskRequest) confirmation() task.RelocationConfirmation {
	var c task.RelocationConfirmation
	if r.Quantity != nil {
		c.Quantity = *r.Quantity
	}
	if r.FromBin != nil {
		c.FromBin = *r.FromBin
	}
	if r.ToBin != nil {
		c.ToBin = *r.ToBin
	}
	return c
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
	ActualWeight   float64
	ExpectedWeight float64

	// hasActual / hasExpected record whether the JSON object carried the
	// key at all. apis/openapi.yaml lists both weights as required, so an
	// ABSENT weight is a 400; but it also documents that an explicit null
	// is accepted and treated as 0, which a plain float64 (or *float64)
	// cannot tell apart from absence. Tracking key presence keeps both
	// documented behaviours.
	hasActual   bool
	hasExpected bool
}

// UnmarshalJSON decodes the weights and remembers which keys were present.
func (r *runSlamRequest) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	var weights struct {
		ActualWeight   float64 `json:"actualWeight"`
		ExpectedWeight float64 `json:"expectedWeight"`
	}
	if err := json.Unmarshal(data, &weights); err != nil {
		return err
	}
	r.ActualWeight, r.ExpectedWeight = weights.ActualWeight, weights.ExpectedWeight
	_, r.hasActual = raw["actualWeight"]
	_, r.hasExpected = raw["expectedWeight"]
	return nil
}

// validate reports the first weight missing from the request, or "".
func (r runSlamRequest) validate() string {
	switch {
	case !r.hasActual:
		return "actualWeight is required"
	case !r.hasExpected:
		return "expectedWeight is required"
	default:
		return ""
	}
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
