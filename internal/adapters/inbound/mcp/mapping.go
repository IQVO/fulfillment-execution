// Package mcp is the inbound Model Context Protocol adapter: it exposes this
// bounded context to the AI ecosystem as a second driving adapter over the
// same application-layer use cases the HTTP adapter uses. It is built on the
// official MCP Go SDK and served over Streamable HTTP.
//
// Per ADR-0008 and the MCP governance charter, this package depends inward on
// the application layer (use cases and ports) and the domain only — never on
// an outbound adapter. The composition root (cmd/mcp) wires concrete
// repositories into the use cases and query port. Tool handlers call use
// cases; domain structs never leak across the tool boundary.
package mcp

import (
	"context"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// TaskQueries is the narrow read-only port this adapter needs beyond the
// GetQueueDepth use case, to answer richer diagnostic questions
// (find_claimable_work, diagnose_stuck_tasks) without reaching into an
// outbound adapter. It is satisfied by the same TaskRepo the use cases use;
// the composition root injects that implementation. Keeping it as a port here
// preserves the dependency rule: the adapter depends on an interface, not on
// outbound/postgres or outbound/memory.
type TaskQueries interface {
	// FindClaimableByType returns Pending or lease-expired tasks of the
	// given type, ordered earliest-CPT-first (highest priority first).
	FindClaimableByType(ctx context.Context, taskType task.Type, now time.Time) ([]*task.Task, error)
	// FindAllClaimed returns every task currently in the Claimed state, used
	// to surface leases near or past expiry.
	FindAllClaimed(ctx context.Context) ([]*task.Task, error)
}

// claimableWork is the structured result of the find_claimable_work tool: a
// single best-fit task view plus how many candidates were available. It is a
// tool-boundary DTO, not a domain type.
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

// replenishmentView is a REPLENISH task's planned move at the tool boundary.
// Both bins are known: the operator may scan them, and must pass the counted
// quantity to complete_task.
type replenishmentView struct {
	RequestId string `json:"requestId"`
	SKU       string `json:"sku"`
	FromBin   string `json:"fromBin"`
	ToBin     string `json:"toBin"`
	Quantity  int    `json:"quantity"`
	Priority  string `json:"priority"`
}

// relocationView is a RELOCATE task's planned move at the tool boundary. An
// open side (fromBin of an Assign, toBin of a Vacate) is omitted: the
// operator scans it and passes it to complete_task.
type relocationView struct {
	PlanId  string `json:"planId"`
	MoveSeq int    `json:"moveSeq"`
	Kind    string `json:"kind"`
	SKU     string `json:"sku"`
	FromBin string `json:"fromBin,omitempty"`
	ToBin   string `json:"toBin,omitempty"`
	Ref     string `json:"ref"`
}

// stuckTask is one entry in the diagnose_stuck_tasks result: a claimed task
// whose lease is at or past expiry, with the reason it is flagged.
type stuckTask struct {
	TaskId         string `json:"taskId"`
	Type           string `json:"type"`
	LeaseStationId string `json:"leaseStationId"`
	LeaseExpiry    string `json:"leaseExpiry"`
	Reason         string `json:"reason"`
}

// toClaimableWork maps a domain Task into the tool-boundary DTO. Nothing but
// this file's DTOs crosses the tool boundary.
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
