// Shared harness for the MCP eval suites (E1-E3): one place that builds a
// real Streamable HTTP server over in-memory adapters and connects a real
// SDK client to it, so schema evals, wire conformance evals, and the
// Gherkin behavioral evals all exercise exactly the surface a model host
// would.
package mcp_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/mcp"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// evalBase is the fixed instant the canonical eval state is seeded at:
// leases, CPTs, and the diagnostics window are all measured from here.
var evalBase = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// evalHarness is a fully wired MCP server over in-memory repos plus the
// client session talking to it, with the knobs the evals assert on.
type evalHarness struct {
	session         *sdk.ClientSession
	server          *httptest.Server
	tasks           *memory.TaskRepo
	publisher       *events.BufferedPublisher
	clock           *memory.FixedClock
	reports         *fakeReportsClient
	claimedTaskID   string
	lastCallResult  *sdk.CallToolResult
	lastCallErr     error
	lastCallContent string
}

// newEvalDeps builds the DEFAULT tool surface over empty in-memory repos —
// no reports client, so the two conditionally registered report tools are
// absent. This is the surface the E1 golden registry pins; schema and
// conformance evals that do not seed state run against it too.
func newEvalDeps() inboundmcp.Deps {
	tasks := memory.NewTaskRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(evalBase)
	return inboundmcp.Deps{
		GetQueueDepth: &usecases.GetQueueDepth{Tasks: tasks},
		CompleteTask:  &usecases.CompleteTask{Tasks: tasks, Publisher: publisher, Clock: clock},
		Tasks:         tasks,
		Now:           clock.Now,
	}
}

// newEvalHarness seeds the canonical eval state over a real Streamable HTTP
// server with every tool wired (including the report tools, backed by a
// fake reports REST client), and connects a client session to it.
//
// Canonical state, seeded through the real use cases (the same pattern
// server_test.go uses):
//   - station s1 registered (pick capability),
//   - PICK task "o-early" (CPT 13:00) created then CLAIMED by s1 — its lease
//     expires at 12:05 (the 5-minute DefaultLeaseDuration),
//   - PICK task "o-late" (CPT 14:00) pending,
//   - PACK task "o3" (CPT 13:00) pending.
func newEvalHarness(t *testing.T) *evalHarness {
	t.Helper()

	h := &evalHarness{}
	h.tasks = memory.NewTaskRepo()
	stations := memory.NewStationRepo()
	h.publisher = events.NewBufferedPublisher()
	h.clock = memory.NewFixedClock(evalBase)
	h.reports = &fakeReportsClient{}

	n := 0
	create := &usecases.CreateTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock, NewId: func() shared.TaskId {
		n++
		return shared.TaskId([]byte{'t', byte('0' + n)})
	}}
	register := &usecases.RegisterStation{Stations: stations, Publisher: h.publisher}
	claim := &usecases.ClaimNext{Tasks: h.tasks, Stations: stations, Publisher: h.publisher, Clock: h.clock}
	ctx := context.Background()
	if _, err := register.Execute(ctx, "s1", []string{"pick"}, ""); err != nil {
		t.Fatalf("eval harness register station: %v", err)
	}
	if _, err := create.Execute(ctx, task.Pick, shared.NewCPT(evalBase.Add(time.Hour)), "o-early", shared.NewCapabilitySet("pick"), false, false); err != nil {
		t.Fatalf("eval harness create o-early: %v", err)
	}
	if _, err := create.Execute(ctx, task.Pick, shared.NewCPT(evalBase.Add(2*time.Hour)), "o-late", shared.NewCapabilitySet("pick"), false, false); err != nil {
		t.Fatalf("eval harness create o-late: %v", err)
	}
	if _, err := create.Execute(ctx, task.Pack, shared.NewCPT(evalBase.Add(time.Hour)), "o3", shared.NewCapabilitySet("pack"), false, false); err != nil {
		t.Fatalf("eval harness create o3: %v", err)
	}
	claimed, err := claim.Execute(ctx, "s1", task.Pick)
	if err != nil {
		t.Fatalf("eval harness claim: %v", err)
	}
	h.claimedTaskID = string(claimed.Id())

	server := inboundmcp.NewServer(inboundmcp.Deps{
		GetQueueDepth: &usecases.GetQueueDepth{Tasks: h.tasks},
		CompleteTask:  &usecases.CompleteTask{Tasks: h.tasks, Publisher: h.publisher, Clock: h.clock},
		Tasks:         h.tasks,
		Reports:       h.reports,
		Now:           h.clock.Now,
	})
	h.server = httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(h.server.Close)

	client := sdk.NewClient(&sdk.Implementation{Name: "eval-client", Version: "0.0.1"}, nil)
	transport := &sdk.StreamableClientTransport{Endpoint: h.server.URL}
	session, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("eval harness connect: %v", err)
	}
	h.session = session
	t.Cleanup(func() { _ = session.Close() })
	return h
}

// wireSession builds a real Streamable HTTP server over the given deps and
// connects a client session to it, for evals that do not need seeded
// state.
func wireSession(t *testing.T, deps inboundmcp.Deps) *sdk.ClientSession {
	t.Helper()
	server := inboundmcp.NewServer(deps)
	httpSrv := httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(httpSrv.Close)

	client := sdk.NewClient(&sdk.Implementation{Name: "eval-client", Version: "0.0.1"}, nil)
	transport := &sdk.StreamableClientTransport{Endpoint: httpSrv.URL}
	session, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("wire session connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

// callTool invokes a tool and records the result for the Then steps.
func (h *evalHarness) callTool(ctx context.Context, name string, args map[string]any) error {
	res, err := h.session.CallTool(ctx, &sdk.CallToolParams{Name: name, Arguments: args})
	h.lastCallResult, h.lastCallErr = res, err
	h.lastCallContent = ""
	if res != nil {
		for _, c := range res.Content {
			if text, ok := c.(*sdk.TextContent); ok {
				h.lastCallContent += text.Text
			}
		}
	}
	return err
}
