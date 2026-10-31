package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// tracerName is the OTel instrumentation scope for A2A skill spans, mirroring
// the MCP adapter's mcp.tool spans.
const tracerName = "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/a2a"

// Skill ids advertised on the Agent Card (see Card).
const (
	skillBacklogStatus = "backlog_status"
	skillCompleteTask  = "complete_task"
)

// skillRequest is the wire contract of every skill call: the first meaningful
// message part carries (as a Data part, or a Text part holding the same JSON)
// the skill id and its arguments. Strict on purpose - an agent client is
// machine-to-machine, so unknown fields are rejected, never ignored: a
// misspelled name like "withinSecs" must not silently drop the lease window.
// (Go's encoding/json matches field names case-insensitively, so
// "withinseconds" still reaches withinSeconds; only genuinely unknown names
// are refused.)
type skillRequest struct {
	Skill     string          `json:"skill"`
	Arguments json.RawMessage `json:"arguments"`
}

// backlogStatusArgs and completeTaskArgs are the per-skill argument shapes.
// Optional values are pointers so "absent" and "zero" stay distinguishable,
// the way the REST DTO treats them.
type backlogStatusArgs struct {
	ProcessPath   string `json:"processPath"`
	WithinSeconds *int   `json:"withinSeconds"`
}

type completeTaskArgs struct {
	TaskId    string  `json:"taskId"`
	StationId string  `json:"stationId"`
	Quantity  *int    `json:"quantity"`
	FromBin   *string `json:"fromBin"`
	ToBin     *string `json:"toBin"`
}

// answer maps one incoming message to a skill and returns the agent's reply
// Message. It is the whole executor body: this is a message-only agent, so a
// skill run is exactly one request mapped to one answer (ADR-0059).
func answer(deps Deps, ctx context.Context, ec *a2asrv.ExecutorContext) a2a.Event {
	req, problem := parseSkillRequest(ec)
	if problem != nil {
		return reply(ec, envelope{Skill: "", Problem: problem}, problem.summary())
	}

	ctx, span := otel.Tracer(tracerName).Start(ctx, "a2a.skill "+req.Skill)
	defer span.End()
	span.SetAttributes(attribute.String("a2a.skill", req.Skill))

	dto, problem := run(deps, ctx, req)
	if problem != nil {
		span.SetStatus(codes.Error, problem.Detail)
		return reply(ec, envelope{Skill: req.Skill, Problem: problem}, problem.summary())
	}
	return reply(ec, envelope{Skill: req.Skill, Data: dto}, dto.Summary())
}

// run dispatches one validated request to its skill.
func run(deps Deps, ctx context.Context, req skillRequest) (skillDTO, *problem) {
	switch req.Skill {
	case skillBacklogStatus:
		data, problem := deps.backlogStatus(ctx, req.Arguments)
		if problem != nil {
			return skillDTO{}, problem
		}
		return skillDTO{BacklogStatus: &data, summary: data.summaryLine()}, nil
	case skillCompleteTask:
		data, problem := deps.completeTask(ctx, req.Arguments)
		if problem != nil {
			return skillDTO{}, problem
		}
		return skillDTO{TaskCompleted: &data, summary: fmt.Sprintf("completed %s for station %s", data.TaskId, data.StationId)}, nil
	default:
		return skillDTO{}, problemUnknownSkill(req.Skill)
	}
}

// reply builds the agent Message for one skill run: a Data part carrying the
// structured envelope and a Text part carrying the one-line summary, bound to
// the caller's context (and task, when the client referenced one).
func reply(ec *a2asrv.ExecutorContext, env envelope, summary string) a2a.Event {
	return a2a.NewMessageForTask(a2a.MessageRoleAgent, ec, a2a.NewDataPart(env), a2a.NewTextPart(summary))
}

// parseSkillRequest extracts the skillRequest from the message's first
// meaningful part. A Data part (or a Text part holding the same JSON object)
// is accepted; anything else is a request-shape problem, answered - not
// raised - because a malformed delegation is a business answer for the
// calling agent, not a transport failure.
func parseSkillRequest(ec *a2asrv.ExecutorContext) (skillRequest, *problem) {
	msg := ec.Message
	if msg == nil || len(msg.Parts) == 0 {
		return skillRequest{}, problemMalformedRequest("the message carries no parts")
	}
	raw, err := partJSON(msg.Parts[0])
	if err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the first message part is not a skill request: %v", err))
	}
	var req skillRequest
	if err := decodeStrict(raw, &req); err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the skill request is not valid: %v", err))
	}
	if req.Skill == "" {
		return skillRequest{}, problemMalformedRequest(`the skill request carries no "skill" field`)
	}
	return req, nil
}

// partJSON renders one part's content as the JSON bytes of a skill request.
func partJSON(part *a2a.Part) ([]byte, error) {
	switch content := part.Content.(type) {
	case a2a.Data:
		return json.Marshal(content.Value)
	case a2a.Text:
		return []byte(string(content)), nil
	default:
		return nil, errors.New("expected a data part (or a text part holding the skill JSON)")
	}
}

// decodeStrict decodes one JSON document into dst, rejecting unknown fields
// and trailing data. Absent arguments decode as the empty object so each
// skill's own required-field checks produce the specific problem.
func decodeStrict(raw []byte, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("unexpected data after the JSON document")
	}
	return nil
}

// parseProcessPath validates a caller-supplied process-path string against
// the domain's known task types, exactly as the MCP tools do. The value comes
// from an agent and is untrusted: an unknown one is refused, never defaulted.
func parseProcessPath(s string) (task.Type, error) {
	switch task.Type(s) {
	case task.Pick, task.Pack, task.Slam, task.Relocate, task.Replenish, task.BreakCase:
		return task.Type(s), nil
	default:
		return "", fmt.Errorf("unknown process path %q: must be PICK, PACK, SLAM, RELOCATE, REPLENISH or BREAK_CASE", s)
	}
}

// backlogStatus is the composed read skill: queue depth, the best claimable
// task and the lapsing leases of one process path, from this context's own
// state. It reuses the read models the MCP tools get_queue_status,
// find_claimable_work and diagnose_stuck_tasks use, narrowed to one path.
func (d Deps) backlogStatus(ctx context.Context, rawArgs json.RawMessage) (backlogStatus, *problem) {
	var args backlogStatusArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return backlogStatus{}, problemInvalidArgument(fmt.Sprintf("arguments: %v", err))
	}
	if strings.TrimSpace(args.ProcessPath) == "" {
		return backlogStatus{}, problemInvalidArgument("processPath is required")
	}
	if args.WithinSeconds != nil && *args.WithinSeconds < 0 {
		return backlogStatus{}, problemInvalidArgument("withinSeconds must not be negative")
	}
	path, err := parseProcessPath(args.ProcessPath)
	if err != nil {
		return backlogStatus{}, problemInvalidArgument(err.Error())
	}

	depth, err := d.GetQueueDepth.Execute(ctx, path)
	if err != nil {
		return backlogStatus{}, problemFromUseCase(err)
	}
	out := backlogStatus{ProcessPath: string(path), Depth: depth, Stuck: []stuckTask{}}

	candidates, err := d.Tasks.FindClaimableByType(ctx, path, d.now())
	if err != nil {
		return backlogStatus{}, problemFromUseCase(err)
	}
	out.CandidateCount = len(candidates)
	if len(candidates) > 0 {
		best := toClaimableWork(candidates[0])
		out.Best = &best
	}

	within := 0
	if args.WithinSeconds != nil {
		within = *args.WithinSeconds
	}
	out.Stuck, err = d.lapsingLeases(ctx, path, time.Duration(within)*time.Second)
	if err != nil {
		return backlogStatus{}, problemFromUseCase(err)
	}
	out.StuckCount = len(out.Stuck)
	return out, nil
}

// lapsingLeases lists the claimed tasks of one process path whose lease has
// already expired or expires within the window.
func (d Deps) lapsingLeases(ctx context.Context, path task.Type, window time.Duration) ([]stuckTask, error) {
	claimed, err := d.Tasks.FindAllClaimed(ctx)
	if err != nil {
		return nil, err
	}
	now := d.now()
	horizon := now.Add(window)
	flagged := make([]stuckTask, 0)
	for _, t := range claimed {
		lease := t.Lease()
		if lease == nil || t.Type() != path {
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
	return flagged, nil
}

// completeTask is the single state-changing delegation: it completes a
// claimed task through the same use case REST and MCP use, so the at-most-once
// and ownership invariants and the transactional outbox behave identically
// regardless of caller. Argument validation mirrors the REST DTO.
func (d Deps) completeTask(ctx context.Context, rawArgs json.RawMessage) (taskCompleted, *problem) {
	var args completeTaskArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return taskCompleted{}, problemInvalidArgument(fmt.Sprintf("arguments: %v", err))
	}
	if msg := args.validate(); msg != "" {
		return taskCompleted{}, problemInvalidArgument(msg)
	}
	err := d.CompleteTask.ExecuteWith(ctx, shared.TaskId(args.TaskId), shared.StationId(args.StationId), args.confirmation())
	if err != nil {
		return taskCompleted{}, problemFromUseCase(err)
	}
	return taskCompleted{TaskId: args.TaskId, StationId: args.StationId, Completed: true}, nil
}

// validate applies the same request rules as the REST completeTaskRequest
// (plus the task id, which REST carries in the path).
func (a completeTaskArgs) validate() string {
	switch {
	case strings.TrimSpace(a.TaskId) == "":
		return "taskId is required"
	case a.StationId == "":
		return "stationId is required"
	case a.Quantity != nil && *a.Quantity < 1:
		return "quantity must be at least 1"
	case a.FromBin != nil && isBlankPerContract(*a.FromBin):
		return "fromBin must not be blank"
	case a.ToBin != nil && isBlankPerContract(*a.ToBin):
		return "toBin must not be blank"
	default:
		return ""
	}
}

// confirmation maps the optional RELOCATE/REPLENISH inputs onto the domain's
// confirmation; all absent is the zero value (no confirmation input).
func (a completeTaskArgs) confirmation() task.RelocationConfirmation {
	var c task.RelocationConfirmation
	if a.Quantity != nil {
		c.Quantity = *a.Quantity
	}
	if a.FromBin != nil {
		c.FromBin = *a.FromBin
	}
	if a.ToBin != nil {
		c.ToBin = *a.ToBin
	}
	return c
}

// isBlankPerContract reports whether s has no character outside ECMA-262's \s
// class, the set the REST OpenAPI contract's `pattern: '\S'` uses for fromBin
// and toBin. It is the same rule as the REST adapter's helper of that name,
// duplicated here because the two inbound adapters must not import each other.
func isBlankPerContract(s string) bool {
	return strings.TrimFunc(s, isContractWhitespace) == ""
}

func isContractWhitespace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', '\u00a0', '\ufeff', '\u1680', '\u2028', '\u2029', '\u202f', '\u205f', '\u3000':
		return true
	}
	return r >= '\u2000' && r <= '\u200a'
}

// problemFromUseCase maps a use case error to a problem answer, using the
// SAME classification as the REST adapter (internal/adapters/inbound/http
// statusFor and problemCatalog, restricted to what these two skills can
// raise) so a delegating agent sees the outcome a REST caller would. Domain
// rejections are EXPECTED business outcomes and are answered as RFC 7807
// problems; anything unrecognised is an internal failure, still answered as a
// problem - status 500 - so the calling agent always receives a structured,
// machine-readable result.
func problemFromUseCase(err error) *problem {
	for _, c := range classifications {
		if errors.Is(err, c.err) {
			return &problem{Type: problemTypeBase + c.slug, Title: c.title, Detail: err.Error(), Status: c.status}
		}
	}
	slog.Error("a2a skill failed", "error", err)
	return &problem{Type: problemTypeBase + "internal-error", Title: "Internal server error", Detail: "the skill failed; see server logs (trace id in the response metadata)", Status: 500}
}

// classifications mirrors the REST adapter's statusFor + problemCatalog for
// every typed error reachable from backlog_status and complete_task.
var classifications = []struct {
	err    error
	status int
	slug   string
	title  string
}{
	{usecases.ErrTaskNotFound, 404, "task-not-found", "Task not found"},

	{task.ErrAlreadyCompleted, 409, "task-already-completed", "Task already completed"},
	{task.ErrNotClaimed, 409, "task-not-claimed", "Task is not currently claimed"},
	{task.ErrNotOwner, 409, "task-not-owner", "Station does not own the active claim on this task"},
	{task.ErrCaseBreakCompletionRequired, 409, "case-break-completion-required", "A BREAK_CASE task completes only through POST /tasks/{id}/break-case"},

	{task.ErrRelocationConfirmationRequired, 422, "relocation-confirmation-required", "A RELOCATE task completes only with the confirmed quantity and the bins the move left open"},
	{task.ErrInvalidRelocationConfirmation, 422, "invalid-relocation-confirmation", "The relocation confirmation does not match the planned move"},
	{task.ErrNotRelocationTask, 422, "not-relocation-task", "quantity, fromBin and toBin apply only to RELOCATE and REPLENISH tasks"},
	{task.ErrReplenishmentConfirmationRequired, 422, "replenishment-confirmation-required", "A REPLENISH task completes only with the confirmed quantity"},
	{task.ErrInvalidReplenishmentConfirmation, 422, "invalid-replenishment-confirmation", "The replenishment confirmation does not match the planned move"},
}
