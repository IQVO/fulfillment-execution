package http

import (
	"strings"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

type createTaskRequest struct {
	Type                 string     `json:"type"`
	CPT                  *time.Time `json:"cpt"`
	OrderRef             string     `json:"orderRef"`
	RequiredCapabilities []string   `json:"requiredCapabilities"`
	Fragile              bool       `json:"fragile"`
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

// isClaimableTaskType is isValidTaskType plus RELOCATE, REPLENISH and
// BREAK_CASE (apis/openapi.yaml's AnyTaskType enum): every task type a
// station may claim. BREAK_CASE is claimable but not creatable through
// POST /tasks (ADR 0055).
func isClaimableTaskType(v string) bool {
	switch task.Type(v) {
	case task.Relocate, task.Replenish, task.BreakCase:
		return true
	}
	return isValidTaskType(v)
}

// validate reports the first missing required field, or "" if the request
// is well-formed enough to hand to the use case.
func (r createTaskRequest) validate() string {
	switch {
	case r.Type == "":
		return "type is required"
	case !isValidTaskType(r.Type):
		return "type must be one of PICK, PACK, SLAM, REBIN, DISPATCH, ARRIVAL"
	case r.CPT == nil:
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
		return "taskType must be one of PICK, PACK, SLAM, REBIN, DISPATCH, ARRIVAL, RELOCATE, REPLENISH, BREAK_CASE"
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

// pickScanRequest is POST /tasks/{id}/scans (ADR 0054). Count is the number of
// packs scanned (eaches for an EACH GTIN, cases for a CASE GTIN) and defaults
// to 1.
type pickScanRequest struct {
	StationId string `json:"stationId"`
	GTIN      string `json:"gtin"`
	Count     *int   `json:"count,omitempty"`
}

func (r pickScanRequest) validate() string {
	switch {
	case r.StationId == "":
		return "stationId is required"
	case strings.TrimSpace(r.GTIN) == "":
		return "gtin is required"
	case r.Count != nil && *r.Count < 1:
		return "count must be at least 1"
	default:
		return ""
	}
}

func (r pickScanRequest) count() int {
	if r.Count == nil {
		return 1
	}
	return *r.Count
}

// pickScanResponse is what one accepted scan did, in eaches.
type pickScanResponse struct {
	ScannedGTIN   string `json:"scannedGtin"`
	SKU           string `json:"sku"`
	Level         string `json:"level"`
	AddedEaches   int    `json:"addedEaches"`
	PickedEaches  int    `json:"pickedEaches"`
	PlannedEaches int    `json:"plannedEaches"`
}

// breakCaseRequest is POST /tasks/{id}/break-case.
type breakCaseRequest struct {
	StationId string `json:"stationId"`
}

// workRequestResponse is the acknowledgement of an accepted work delegation
// (ADR-0058): the unit Work Planning admitted. The WorkReleased event on
// Kafka remains the authoritative fact.
type workRequestResponse struct {
	WorkUnitId string `json:"workUnitId"`
	CPT        string `json:"cpt"`
	Reference  string `json:"reference"`
}
