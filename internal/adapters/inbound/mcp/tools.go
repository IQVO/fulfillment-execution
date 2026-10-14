package mcp

import (
	"context"
	"fmt"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// tracerName is the OTel instrumentation scope for MCP tool spans.
const tracerName = "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/mcp"

// Deps is everything the MCP tools need, injected by the composition root.
// It carries the same use cases the HTTP adapter uses plus the narrow read
// port; the adapter never constructs an outbound adapter itself.
type Deps struct {
	// GetQueueDepth is the existing read-model use case, reused unchanged.
	GetQueueDepth *usecases.GetQueueDepth
	// CompleteTask is the existing write use case, reused unchanged. Its
	// domain invariants (at-most-once, ownership) make a model-invoked
	// completion safe by construction.
	CompleteTask *usecases.CompleteTask
	// Tasks is the read-only query port for richer diagnostics.
	Tasks TaskQueries
	// Reports is the client of the fulfillment-reports REST service, backing
	// the curated get_fulfillment_throughput_report tool. When nil, that tool
	// is not registered (an MCP deployment without the reports service).
	Reports ReportsClient
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

// --- get_queue_status ---------------------------------------------------------

type queueStatusInput struct {
	ProcessPath string `json:"processPath" jsonschema:"the process path queue to inspect: PICK, PACK, SLAM or RELOCATE"`
}

type queueStatusOutput struct {
	ProcessPath string `json:"processPath"`
	Depth       int    `json:"depth"`
}

func (d Deps) getQueueStatus(ctx context.Context, in queueStatusInput) (queueStatusOutput, error) {
	t, err := parseProcessPath(in.ProcessPath)
	if err != nil {
		return queueStatusOutput{}, err
	}
	depth, err := d.GetQueueDepth.Execute(ctx, t)
	if err != nil {
		return queueStatusOutput{}, err
	}
	return queueStatusOutput{ProcessPath: string(t), Depth: depth}, nil
}

// --- find_claimable_work ------------------------------------------------------

type findClaimableInput struct {
	ProcessPath string `json:"processPath" jsonschema:"the process path to search: PICK, PACK, SLAM or RELOCATE"`
}

type findClaimableOutput struct {
	ProcessPath    string `json:"processPath"`
	CandidateCount int    `json:"candidateCount"`
	// Best is the highest-priority (earliest CPT) claimable task, or null when
	// the queue has nothing claimable right now.
	Best *claimableWork `json:"best"`
}

func (d Deps) findClaimableWork(ctx context.Context, in findClaimableInput) (findClaimableOutput, error) {
	t, err := parseProcessPath(in.ProcessPath)
	if err != nil {
		return findClaimableOutput{}, err
	}
	candidates, err := d.Tasks.FindClaimableByType(ctx, t, d.now())
	if err != nil {
		return findClaimableOutput{}, err
	}
	out := findClaimableOutput{ProcessPath: string(t), CandidateCount: len(candidates)}
	if len(candidates) > 0 {
		best := toClaimableWork(candidates[0])
		out.Best = &best
	}
	return out, nil
}

// --- diagnose_stuck_tasks -----------------------------------------------------

type diagnoseInput struct {
	// WithinSeconds flags claimed tasks whose lease expires within this many
	// seconds (as well as any already expired). 0 means "only already expired".
	WithinSeconds int `json:"withinSeconds" jsonschema:"flag claimed tasks whose lease expires within this many seconds; 0 = only already-expired leases"`
}

type diagnoseOutput struct {
	Count int         `json:"count"`
	Tasks []stuckTask `json:"tasks"`
}

func (d Deps) diagnoseStuckTasks(ctx context.Context, in diagnoseInput) (diagnoseOutput, error) {
	now := d.now()
	claimed, err := d.Tasks.FindAllClaimed(ctx)
	if err != nil {
		return diagnoseOutput{}, err
	}
	horizon := now.Add(time.Duration(in.WithinSeconds) * time.Second)
	flagged := make([]stuckTask, 0)
	for _, t := range claimed {
		lease := t.Lease()
		if lease == nil {
			continue
		}
		reason := ""
		switch {
		case !lease.Expiry.After(now):
			reason = "lease already expired; task will return to the pool on the next sweep"
		case !lease.Expiry.After(horizon):
			reason = "lease expiring within the requested window"
		default:
			continue
		}
		flagged = append(flagged, stuckTask{
			TaskId:         string(t.Id()),
			Type:           string(t.Type()),
			LeaseStationId: string(lease.StationId),
			LeaseExpiry:    lease.Expiry.UTC().Format(time.RFC3339),
			Reason:         reason,
		})
	}
	return diagnoseOutput{Count: len(flagged), Tasks: flagged}, nil
}

// --- complete_task (write) ----------------------------------------------------

type completeTaskInput struct {
	TaskId    string `json:"taskId" jsonschema:"the id of the task to complete"`
	StationId string `json:"stationId" jsonschema:"the id of the station that holds the active claim and is completing the task"`
	// Quantity, FromBin and ToBin are the RELOCATE confirmation (ADR 0047):
	// optional in the schema, required by the domain for a RELOCATE task
	// and rejected for any other task type.
	Quantity int    `json:"quantity,omitempty" jsonschema:"optional, RELOCATE tasks only (required for them): the units the operator counted and moved, at least 1"`
	FromBin  string `json:"fromBin,omitempty" jsonschema:"optional, RELOCATE tasks only: the source bin the operator scanned; required when the move left the source open (an Assign)"`
	ToBin    string `json:"toBin,omitempty" jsonschema:"optional, RELOCATE tasks only: the destination bin the operator scanned; required when the move left the destination open (a Vacate)"`
}

type completeTaskOutput struct {
	TaskId    string `json:"taskId"`
	StationId string `json:"stationId"`
	Completed bool   `json:"completed"`
}

func (d Deps) completeTask(ctx context.Context, in completeTaskInput) (completeTaskOutput, error) {
	if in.TaskId == "" || in.StationId == "" {
		return completeTaskOutput{}, fmt.Errorf("taskId and stationId are required")
	}
	if in.Quantity < 0 {
		return completeTaskOutput{}, fmt.Errorf("quantity must be at least 1, got %d", in.Quantity)
	}
	confirmation := task.RelocationConfirmation{Quantity: in.Quantity, FromBin: in.FromBin, ToBin: in.ToBin}
	err := d.CompleteTask.ExecuteWith(ctx, shared.TaskId(in.TaskId), shared.StationId(in.StationId), confirmation)
	if err != nil {
		// The use case's domain errors (task not found, not owner, already
		// completed, not claimed, a missing or mismatched RELOCATE
		// confirmation) surface unchanged as the tool error; the
		// at-most-once and ownership invariants make a mistaken model call safe.
		return completeTaskOutput{}, err
	}
	return completeTaskOutput{TaskId: in.TaskId, StationId: in.StationId, Completed: true}, nil
}

// --- registration -------------------------------------------------------------

// registerTools adds every tool to the server, each wrapped so its handler
// runs inside an OTel span named "mcp.tool <name>".
func (d Deps) registerTools(server *mcp.Server) {
	readOnly := true

	addTool(server, &mcp.Tool{
		Name:        "get_queue_status",
		Description: "Return the number of Pending tasks in a process-path queue (PICK, PACK, SLAM, or RELOCATE).",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
	}, d.getQueueStatus)

	addTool(server, &mcp.Tool{
		Name:        "find_claimable_work",
		Description: "Return the highest-priority (earliest-CPT) task a station could claim now in a given process path, plus how many candidates exist. A RELOCATE candidate carries its slot-plan move (sku, bins, kind).",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
	}, d.findClaimableWork)

	addTool(server, &mcp.Tool{
		Name:        "diagnose_stuck_tasks",
		Description: "List claimed tasks whose lease has expired (or expires within a given window), each with the reason it is flagged.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
	}, d.diagnoseStuckTasks)

	// Write tool: completes a claimed task. Annotated destructive
	// (non-read-only, non-idempotent) so a host can see it changes state
	// before letting a model call it. The domain invariants (at-most-once,
	// ownership) bound the risk of a mistaken call.
	destructive := true
	notIdempotent := false
	addTool(server, &mcp.Tool{
		Name:        "complete_task",
		Description: "Complete a claimed task on behalf of the station that holds its active claim. Rejected if the task is not found, not claimed, already completed, or the station does not own the claim. A RELOCATE task also needs the operator's confirmation: quantity (counted, at least 1) and the scanned fromBin (Assign) or toBin (Vacate) the move left open; never invent these values, ask the operator.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: &destructive, IdempotentHint: notIdempotent},
	}, d.completeTask)

	// Curated read-only data-product tools, registered only when the reports
	// client is configured.
	d.registerReportTool(server)
	d.registerOnTimeToCPTTool(server)
}

// addTool registers one tool. It centralises the cross-cutting concern
// every tool shares: a span per call, and mapping a handler error onto
// the span before returning it.
func addTool[In, Out any](
	server *mcp.Server,
	tool *mcp.Tool,
	handle func(context.Context, In) (Out, error),
) {
	mcp.AddTool(server, tool, func(ctx context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, Out, error) {
		var zero Out
		ctx, span := otel.Tracer(tracerName).Start(ctx, "mcp.tool "+tool.Name,
			trace.WithAttributes(
				attribute.String("mcp.tool.name", tool.Name),
			),
		)
		defer span.End()

		out, err := handle(ctx, in)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return nil, zero, err
		}
		return nil, out, nil
	})
}

// parseProcessPath validates a client-supplied process-path string against the
// domain's known task types. Tool arguments come from a model and are
// untrusted, so an unknown value is rejected rather than silently defaulted.
func parseProcessPath(s string) (task.Type, error) {
	switch task.Type(s) {
	case task.Pick, task.Pack, task.Slam, task.Relocate:
		return task.Type(s), nil
	default:
		return "", fmt.Errorf("unknown process path %q: must be PICK, PACK, SLAM or RELOCATE", s)
	}
}
