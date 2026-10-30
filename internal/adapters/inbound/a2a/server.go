// Package a2a is the inbound Agent2Agent adapter: it exposes this bounded
// context to the fleet's agent ecosystem as a third driving adapter over the
// same application-layer use cases the HTTP and MCP adapters use. It is built
// on the official A2A Go SDK (a2aproject/a2a-go) and speaks JSON-RPC 2.0 over
// HTTP, with the Agent Card served at the well-known path.
//
// Per ADR-0059, A2A is the DELEGATION channel between bounded contexts: an
// agent client sends a goal-shaped skill request as a structured message part
// and this agent answers from its own state through its own use cases. It is
// not a data-sync channel - facts between contexts still travel exclusively
// as CloudEvents on Kafka, including the TaskCompleted raised by the write
// skill through the transactional outbox.
//
// This package is the INBOUND half of A2A in this context. The OUTBOUND half
// (the work-planning delegation client, ADR-0058) lives in
// internal/adapters/outbound/a2a and is deliberately a separate package: the
// two share nothing, and this one never imports the SDK's client package.
//
// Like the MCP adapter (ADR-0008), this package depends inward on the
// application layer (use cases and ports) and the domain only - never on an
// outbound adapter. The composition root (cmd/a2a) wires concrete
// repositories into the use cases. Domain structs never leak across the
// message boundary.
package a2a

import (
	"context"
	"iter"
	"net/http"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// TaskQueries is the narrow read-only port this adapter needs beyond the
// GetQueueDepth use case, to answer the claimable-work and lapsing-lease
// parts of backlog_status. It is the same shape the MCP adapter declares and
// is satisfied by the same TaskRepo; declaring it here keeps this package
// free of any import of a sibling inbound adapter.
type TaskQueries interface {
	// FindClaimableByType returns Pending or lease-expired tasks of the
	// given type, ordered earliest-CPT-first (highest priority first).
	FindClaimableByType(ctx context.Context, taskType task.Type, now time.Time) ([]*task.Task, error)
	// FindAllClaimed returns every task currently in the Claimed state.
	FindAllClaimed(ctx context.Context) ([]*task.Task, error)
}

// Config carries the deployment-specific identity of this agent: the absolute
// URL clients use to reach its JSON-RPC endpoint (advertised on the Agent
// Card) and the card version. It is supplied by the composition root from
// A2A_AGENT_URL; the card MUST advertise a URL the fleet's agent clients can
// actually dial (in-cluster: http://<release>-a2a.<ns>.svc.cluster.local:8093/).
type Config struct {
	AgentURL string
	Version  string
}

// Deps is everything the A2A skills need, injected by the composition root.
// It carries the same use cases the HTTP and MCP adapters use; the adapter
// never constructs an outbound adapter itself.
type Deps struct {
	// GetQueueDepth is the existing read-model use case, reused unchanged.
	GetQueueDepth *usecases.GetQueueDepth
	// CompleteTask is the existing write use case, reused unchanged. Its
	// domain invariants (at-most-once, claim ownership) make a delegated
	// completion safe by construction, exactly as for the MCP tool.
	CompleteTask *usecases.CompleteTask
	// Tasks is the read-only query port for claimable work and lapsing leases.
	Tasks TaskQueries
	// Now supplies the current time; injected so lease diagnostics are
	// deterministic under test. Defaults to time.Now when nil.
	Now func() time.Time
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// Card builds this agent's Agent Card (ADR-0059). The surface is two curated,
// goal-shaped skills - not one skill per endpoint, the same curation
// discipline ADR-0008 applied to MCP tools. backlog_status composes the three
// read models a delegating agent needs to answer "how is this process path's
// backlog doing"; complete_task is the single state-changing delegation, the
// one write the MCP surface already offers. The card declares no security
// schemes on purpose: this surface is unauthenticated by the same fleet-wide
// decision as REST and MCP (ADR-0022); schemes are added here, not in a
// redesign, if that decision is ever reversed.
func Card(cfg Config) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:        "fulfillment-execution",
		Description: "Fulfillment Execution agent for the Warehouse Execution System: reports how one process-path queue is doing (pending depth, the most urgent claimable task, claimed tasks whose lease has lapsed or is about to) and completes a claimed task on behalf of the station that holds the claim, under this context's invariants (at-most-once completion, claim ownership, RELOCATE/REPLENISH confirmation). It never claims, releases or creates work.",
		Version:     cfg.Version,
		Provider: &a2a.AgentProvider{
			Org: "IQVO",
			URL: "https://github.com/IQVO/fulfillment-execution",
		},
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(cfg.AgentURL, a2a.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"application/json", "text/plain"},
		DefaultOutputModes: []string{"application/json", "text/plain"},
		Capabilities:       a2a.AgentCapabilities{Streaming: false},
		Skills: []a2a.AgentSkill{
			{
				ID:          skillBacklogStatus,
				Name:        "Backlog status for a process path",
				Description: "Composed read for one process-path queue (PICK, PACK, SLAM, RELOCATE, REPLENISH or BREAK_CASE): the number of Pending tasks, how many tasks a station could claim now with the highest-priority (earliest-CPT) one, and the claimed tasks of that path whose lease has already expired or expires within withinSeconds (default 0 = only already expired). Changes nothing.",
				Tags:        []string{"fulfillment", "backlog", "read"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"backlog_status","arguments":{"processPath":"PICK"}}`,
					`{"skill":"backlog_status","arguments":{"processPath":"PACK","withinSeconds":300}}`,
				},
			},
			{
				ID:          skillCompleteTask,
				Name:        "Complete a claimed task",
				Description: "Completes a claimed task on behalf of the station that holds its active claim, exactly as the REST and MCP surfaces do, and publishes TaskCompleted through the transactional outbox. Rejected (as a problem answer, nothing written) when the task is unknown, not claimed, already completed, or the station does not own the claim. A RELOCATE task also needs the operator's counted quantity and the scanned fromBin (Assign) or toBin (Vacate) the move left open; a REPLENISH task needs the counted quantity (bins are optional scans that must equal the planned ones); never invent these values. NOT idempotent: a second call on the same task is rejected as already completed.",
				Tags:        []string{"fulfillment", "task", "write"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"complete_task","arguments":{"taskId":"task-1","stationId":"station-1"}}`,
					`{"skill":"complete_task","arguments":{"taskId":"task-2","stationId":"station-1","quantity":4,"fromBin":"A-01-01"}}`,
				},
			},
		},
	}
}

// NewExecutor returns the AgentExecutor that maps an incoming message to a
// skill and answers with a single agent Message (a message-only agent in A2A
// terms): both curated skills are transactional request/response work, so
// there is no long-running Task lifecycle to track in this first A2A
// surface. The response carries the caller's contextId (and taskId when the
// client referenced one) so a delegating saga can group its interactions.
func NewExecutor(deps Deps) a2asrv.AgentExecutor {
	return a2asrv.AgentExecutorFunc(func(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			yield(answer(deps, ctx, ec), nil)
		}
	})
}

// Handler wires the executor into the transport-agnostic A2A request handler
// and wraps it in the JSON-RPC 2.0 HTTP handler - the only transport this
// agent ships (ADR-0059), mirroring ADR-0008's "Streamable HTTP only"
// minimalism for MCP.
func Handler(executor a2asrv.AgentExecutor) http.Handler {
	return a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(executor))
}
