package a2a

import (
	"fmt"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// problemTypeBase namespaces this context's problem types. It is the same
// base the REST adapter mints (RFC 7807 practice, ADR-0005), and the slugs
// below reuse the REST problem catalogue's, so a delegating agent sees the
// same problem type a REST caller would.
const problemTypeBase = "https://errors.fulfillment-execution.warehouse-systems.dev/"

// problem is the RFC 7807-shaped rejection an agent client receives instead
// of a result: domain rejections and request-shape failures are business
// answers, not transport errors, so they travel in the message envelope.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
	Status int    `json:"status"`
}

// summary renders the one-line form used for the reply's text part.
func (p *problem) summary() string {
	return fmt.Sprintf("%s (%d): %s", p.Title, p.Status, p.Detail)
}

func problemMalformedRequest(detail string) *problem {
	return &problem{Type: problemTypeBase + "malformed-skill-request", Title: "Malformed skill request", Detail: detail, Status: 400}
}

func problemUnknownSkill(skill string) *problem {
	return &problem{Type: problemTypeBase + "unknown-skill", Title: "Unknown skill", Detail: fmt.Sprintf("skill %q is not offered by this agent; the Agent Card lists the supported skills", skill), Status: 400}
}

func problemInvalidArgument(detail string) *problem {
	return &problem{Type: problemTypeBase + "invalid-request", Title: "The request is malformed or missing a required field", Detail: detail, Status: 400}
}

// envelope is the structured payload of every skill answer: exactly one of
// Data (a skill DTO) or Problem (why there is no result), plus the skill id
// the answer belongs to.
type envelope struct {
	Skill   string   `json:"skill"`
	Data    skillDTO `json:"data,omitempty"`
	Problem *problem `json:"problem,omitempty"`
}

// skillDTO is the union of the skill result DTOs. A2A parts are
// machine-readable JSON; each skill fills its own field and Summary, keeping
// the envelope flat and versioned as one contract.
type skillDTO struct {
	BacklogStatus *backlogStatus `json:"backlogStatus,omitempty"`
	TaskCompleted *taskCompleted `json:"taskCompleted,omitempty"`
	summary       string
}

// Summary is the human-readable one-liner for the reply's text part.
func (s skillDTO) Summary() string { return s.summary }

// backlogStatus is the composed read DTO of the backlog_status skill: queue
// depth, the best claimable task and the lapsing leases of one process path.
// Domain types never cross the boundary.
type backlogStatus struct {
	ProcessPath    string         `json:"processPath"`
	Depth          int            `json:"depth"`
	CandidateCount int            `json:"candidateCount"`
	Best           *claimableWork `json:"best"`
	StuckCount     int            `json:"stuckCount"`
	Stuck          []stuckTask    `json:"stuck"`
}

// claimableWork is the highest-priority claimable task at the message
// boundary. It mirrors the MCP tool DTO of the same read model.
type claimableWork struct {
	TaskId               string   `json:"taskId"`
	Type                 string   `json:"type"`
	CPT                  string   `json:"cpt"`
	OrderRef             string   `json:"orderRef"`
	RequiredCapabilities []string `json:"requiredCapabilities"`
	// Relocation is the slot-plan move of a RELOCATE task (ADR 0047),
	// omitted for every other task type.
	Relocation *relocationView `json:"relocation,omitempty"`
	// Replenishment is the pick-face replenishment move of a REPLENISH task
	// (ADR 0050), omitted for every other task type.
	Replenishment *replenishmentView `json:"replenishment,omitempty"`
}

// relocationView is a RELOCATE task's planned move. An open side (fromBin of
// an Assign, toBin of a Vacate) is omitted: the operator scans it and the
// caller passes it to complete_task.
type relocationView struct {
	PlanId  string `json:"planId"`
	MoveSeq int    `json:"moveSeq"`
	Kind    string `json:"kind"`
	SKU     string `json:"sku"`
	FromBin string `json:"fromBin,omitempty"`
	ToBin   string `json:"toBin,omitempty"`
	Ref     string `json:"ref"`
}

// replenishmentView is a REPLENISH task's planned move.
type replenishmentView struct {
	RequestId string `json:"requestId"`
	SKU       string `json:"sku"`
	FromBin   string `json:"fromBin"`
	ToBin     string `json:"toBin"`
	Quantity  int    `json:"quantity"`
	Priority  string `json:"priority"`
}

// stuckTask is one claimed task whose lease is at or past expiry.
type stuckTask struct {
	TaskId         string `json:"taskId"`
	Type           string `json:"type"`
	LeaseStationId string `json:"leaseStationId"`
	LeaseExpiry    string `json:"leaseExpiry"`
	Reason         string `json:"reason"`
}

// taskCompleted is the write DTO of the complete_task skill. The Task
// aggregate never leaks across the boundary.
type taskCompleted struct {
	TaskId    string `json:"taskId"`
	StationId string `json:"stationId"`
	Completed bool   `json:"completed"`
}

// toClaimableWork maps a domain Task into the boundary DTO.
func toClaimableWork(t *task.Task) claimableWork {
	caps := make([]string, 0, len(t.RequiredCapabilities()))
	for c := range t.RequiredCapabilities() {
		caps = append(caps, string(c))
	}
	out := claimableWork{
		TaskId:               string(t.Id()),
		Type:                 string(t.Type()),
		CPT:                  t.CPT().Time().UTC().Format(time.RFC3339),
		OrderRef:             string(t.OrderRef()),
		RequiredCapabilities: caps,
	}
	if m := t.Relocation(); m != nil {
		out.Relocation = &relocationView{PlanId: m.PlanId, MoveSeq: m.MoveSeq, Kind: string(m.Kind), SKU: m.SKU, FromBin: m.FromBin, ToBin: m.ToBin, Ref: m.Ref()}
	}
	if m := t.Replenishment(); m != nil {
		out.Replenishment = &replenishmentView{RequestId: m.RequestId, SKU: m.SKU, FromBin: m.FromBin, ToBin: m.ToBin, Quantity: m.Quantity, Priority: string(m.Priority)}
	}
	return out
}

// summaryLine renders the one-line text part of a backlog_status answer.
func (b backlogStatus) summaryLine() string {
	line := fmt.Sprintf("%s: %d pending, %d claimable", b.ProcessPath, b.Depth, b.CandidateCount)
	if b.Best != nil {
		line += fmt.Sprintf(" (next %s, CPT %s)", b.Best.TaskId, b.Best.CPT)
	}
	return line + fmt.Sprintf(", %d lapsing lease(s)", b.StuckCount)
}
