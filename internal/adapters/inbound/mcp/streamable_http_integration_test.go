//go:build integration

// Integration tests for the MCP inbound adapter over the REAL Streamable
// HTTP transport: mcp.Handler(server) mounted on an httptest.Server, driven
// by the SDK's own client (mcp.NewClient + StreamableClientTransport), with
// the REAL Postgres-backed use cases behind it — exactly the deployment
// shape cmd/mcp wires. This proves the wire contract (initialize,
// tools/list, tools/call) end-to-end against real infrastructure, not the
// tool handlers in isolation over in-memory repos.
//
// Postgres comes from testcontainers: one container per package run
// (TestMain in this file), migrated once into a template database, one
// private database per test. Never an external DATABASE_URL, never t.Skip.
package mcp_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundmcp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/mcp"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/postgres"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// One Postgres container serves the whole package, migrated once into a
// template database; each test gets a private clone (milliseconds). See
// internal/application/usecases/usecases_wiring_integration_test.go for
// the same pattern's rationale. Never an external DATABASE_URL, never
// t.Skip.
const templateDB = "mcp_migrated_template"

var (
	sharedBaseURL string
	dbSeq         atomic.Uint64
)

// TestMain owns the package-wide container lifecycle.
func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("fulfillment_execution_mcp"),
		tcpostgres.WithUsername("fulfillment"),
		tcpostgres.WithPassword("fulfillment"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()

	var err2 error
	sharedBaseURL, err2 = container.ConnectionString(ctx, "sslmode=disable")
	if err2 != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err2)
		return 1
	}
	if err := createDatabase(ctx, templateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	migrations, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "migrations"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve migrations dir: %v\n", err)
		return 1
	}
	if err := postgres.Migrate(withDB(sharedBaseURL, templateDB), migrations); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}
	return m.Run()
}

// withDB rewrites the path of a connection URL to the named database.
func withDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// createDatabase creates an empty database inside the shared container.
func createDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, sharedBaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// migratedDB hands the test a connection URL to its own private database
// cloned from the migrated template.
func migratedDB(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("mcp_%d", dbSeq.Add(1))
	conn, err := pgx.Connect(context.Background(), sharedBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, templateDB)); err != nil {
		t.Fatalf("clone database: %v", err)
	}
	return withDB(sharedBaseURL, name)
}

// mcpEpoch is the fixed instant the harness clock and Deps.Now start at.
var mcpEpoch = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

// mcpHarness wires the REAL production stack — Postgres repos, UnitOfWork,
// use cases, inboundmcp.NewServer, inboundmcp.Handler — over a private
// migrated database and serves it over Streamable HTTP. It returns a
// connected SDK client session; the test drives tools/list and tools/call
// exactly like a model host would.
type mcpHarness struct {
	session   *sdkmcp.ClientSession
	publisher *events.BufferedPublisher
	tasks     *postgres.TaskRepo
	clock     *memory.FixedClock
	createdId shared.TaskId
}

// newMCPIntegrationHarness seeds one claimed PICK task on a fresh private
// database (so complete_task has real work to finish and diagnose_stuck_
// tasks has a real lease to inspect) and serves the real MCP stack over
// Streamable HTTP.
func newMCPIntegrationHarness(t *testing.T) *mcpHarness {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, migratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	tasks := postgres.NewTaskRepo(pool)
	stations := postgres.NewStationRepo(pool)
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(mcpEpoch)
	uow := postgres.NewUnitOfWork(pool)

	register := &usecases.RegisterStation{Stations: stations, Publisher: publisher}
	create := &usecases.CreateTask{
		Tasks: tasks, Publisher: publisher, Clock: clock,
		NewId:      func() shared.TaskId { return shared.TaskId("ITCOV-MCP-1") },
		UnitOfWork: uow,
	}
	claim := &usecases.ClaimNext{
		Tasks: tasks, Stations: stations, Publisher: publisher,
		Clock: clock, LeaseDuration: 5 * time.Minute, UnitOfWork: uow,
	}
	complete := &usecases.CompleteTask{
		Tasks: tasks, Publisher: publisher, Clock: clock, UnitOfWork: uow,
	}

	// Seed: one station and one PICK task it has claimed, so the write
	// tool has real state to act on and the diagnostics a real lease.
	if _, err := register.Execute(ctx, "itcov-mcp-station", []string{"pick"}, ""); err != nil {
		t.Fatalf("seed register: %v", err)
	}
	created, err := create.Execute(ctx, task.Pick, shared.NewCPT(mcpEpoch.Add(time.Hour)),
		"ITCOV-MCP-ORDER", shared.NewCapabilitySet("pick"), false, false)
	if err != nil {
		t.Fatalf("seed create: %v", err)
	}
	if _, err := claim.Execute(ctx, "itcov-mcp-station", task.Pick); err != nil {
		t.Fatalf("seed claim: %v", err)
	}

	server := inboundmcp.NewServer(inboundmcp.Deps{
		GetQueueDepth: &usecases.GetQueueDepth{Tasks: tasks},
		CompleteTask:  complete,
		Tasks:         tasks,
		Now:           clock.Now,
	})

	hs := httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(hs.Close)

	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "itcov-test-host", Version: "1.0.0"}, nil)
	session, err := client.Connect(ctx, &sdkmcp.StreamableClientTransport{
		Endpoint: hs.URL, DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatalf("mcp connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	return &mcpHarness{
		session:   session,
		publisher: publisher,
		tasks:     tasks,
		clock:     clock,
		createdId: created.Id(),
	}
}

// TestMCPIntegration_ListToolsExposesTheContract proves the wire contract
// over the real stack: initialize + tools/list expose every registered
// tool, and the write tool carries the non-read-only annotation a host
// needs to gate it.
func TestMCPIntegration_ListToolsExposesTheContract(t *testing.T) {
	h := newMCPIntegrationHarness(t)
	ctx := context.Background()

	list, err := h.session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	names := map[string]bool{}
	for _, tool := range list.Tools {
		names[tool.Name] = true
	}
	for _, want := range []string{
		"get_queue_status", "find_claimable_work", "diagnose_stuck_tasks", "complete_task",
	} {
		if !names[want] {
			t.Fatalf("tools/list must expose %q, got %v", want, names)
		}
	}
	// The write tool must be annotated non-read-only so a host can gate it.
	for _, tool := range list.Tools {
		if tool.Name == "complete_task" {
			if tool.Annotations == nil || tool.Annotations.ReadOnlyHint {
				t.Fatal("complete_task must carry ReadOnlyHint=false")
			}
		}
	}
}

// TestMCPIntegration_ReadToolsRoundTripThroughPostgres drives every read
// tool over the real Postgres-backed stack: queue depth, claimable-work
// discovery and stuck-task diagnostics all answer from the seeded rows.
func TestMCPIntegration_ReadToolsRoundTripThroughPostgres(t *testing.T) {
	h := newMCPIntegrationHarness(t)
	ctx := context.Background()

	// get_queue_status: the seeded PICK task is claimed, so depth is 0.
	queue, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "get_queue_status",
		Arguments: map[string]any{"processPath": "PICK"},
	})
	if err != nil {
		t.Fatalf("tools/call get_queue_status: %v", err)
	}
	if queue.IsError {
		t.Fatalf("get_queue_status returned a tool error: %+v", queue)
	}
	if d := structuredInt(t, queue, "depth"); d != 0 {
		t.Fatalf("seeded PICK task is claimed: get_queue_status depth = %d, want 0", d)
	}

	// find_claimable_work: the claimed task is not claimable right now;
	// the tool must answer (not error) with zero candidates.
	claimable, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "find_claimable_work",
		Arguments: map[string]any{"processPath": "PICK"},
	})
	if err != nil {
		t.Fatalf("tools/call find_claimable_work: %v", err)
	}
	if claimable.IsError {
		t.Fatalf("find_claimable_work returned a tool error: %+v", claimable)
	}
	if c := structuredInt(t, claimable, "candidateCount"); c != 0 {
		t.Fatalf("no claimable PICK work right now: candidateCount = %d, want 0", c)
	}

	// diagnose_stuck_tasks: with Deps.Now at the seed instant the lease is
	// still active, so nothing is flagged; after advancing the clock past
	// the lease the same tool must flag exactly the seeded task.
	diag, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "diagnose_stuck_tasks",
		Arguments: map[string]any{"withinSeconds": 0},
	})
	if err != nil {
		t.Fatalf("tools/call diagnose_stuck_tasks (active lease): %v", err)
	}
	if diag.IsError {
		t.Fatalf("diagnose_stuck_tasks returned a tool error: %+v", diag)
	}
	if c := structuredInt(t, diag, "count"); c != 0 {
		t.Fatalf("active lease must not be flagged: count = %d, want 0", c)
	}

	h.clock.Advance(10 * time.Minute)
	expired, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "diagnose_stuck_tasks",
		Arguments: map[string]any{"withinSeconds": 0},
	})
	if err != nil {
		t.Fatalf("tools/call diagnose_stuck_tasks (expired lease): %v", err)
	}
	if expired.IsError {
		t.Fatalf("diagnose_stuck_tasks (expired lease) returned a tool error: %+v", expired)
	}
	if c := structuredInt(t, expired, "count"); c != 1 {
		t.Fatalf("expired lease must be flagged exactly once: count = %d, want 1", c)
	}
}

// structuredInt reads one integer field out of a tool result's structured
// content, failing the test when the field is missing or not a number.
func structuredInt(t *testing.T, res *sdkmcp.CallToolResult, field string) int {
	t.Helper()
	m, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("tool result has no structured content object: %+v", res.StructuredContent)
	}
	raw, ok := m[field].(float64)
	if !ok {
		t.Fatalf("structured content field %q is not a number: %+v", field, m[field])
	}
	return int(raw)
}

// TestMCPIntegration_WriteToolCompletesThroughPostgres drives the write
// tool end-to-end: complete_task finishes the seeded claimed task through
// the real use case stack, the completion is persisted, and the at-most-
// once invariant rejects a second call as a tool error.
func TestMCPIntegration_WriteToolCompletesThroughPostgres(t *testing.T) {
	h := newMCPIntegrationHarness(t)
	ctx := context.Background()

	res, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "complete_task",
		Arguments: map[string]any{"taskId": string(h.createdId), "stationId": "itcov-mcp-station"},
	})
	if err != nil {
		t.Fatalf("tools/call complete_task: %v", err)
	}
	if res.IsError {
		t.Fatalf("complete_task returned a tool error: %+v", res)
	}

	// The completion really persisted: status Completed over the real
	// repo, and the publisher saw TaskCompleted.
	stored, err := h.tasks.FindById(ctx, h.createdId)
	if err != nil || stored == nil {
		t.Fatalf("find completed task: %v, %v", stored, err)
	}
	if stored.Status() != task.Completed {
		t.Fatalf("expected persisted status Completed, got %s", stored.Status())
	}
	var saw bool
	for _, e := range h.publisher.Events() {
		if e.EventName() == "TaskCompleted" {
			saw = true
		}
	}
	if !saw {
		t.Fatal("complete_task must publish TaskCompleted through the real stack")
	}

	// At-most-once over the wire: the second completion is a domain
	// rejection surfaced as a tool error, not a transport error.
	again, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "complete_task",
		Arguments: map[string]any{"taskId": string(h.createdId), "stationId": "itcov-mcp-station"},
	})
	if err != nil {
		t.Fatalf("tools/call complete_task (second): %v", err)
	}
	if !again.IsError {
		t.Fatal("second complete_task must be rejected as a tool error")
	}
}

// TestMCPIntegration_CallToolRejectsInvalidInput proves invalid input and
// domain rejections come back as TOOL errors (res.IsError), never as
// transport-level failures: an unknown process path, an empty required
// argument, and a completion for a task that does not exist.
func TestMCPIntegration_CallToolRejectsInvalidInput(t *testing.T) {
	h := newMCPIntegrationHarness(t)
	ctx := context.Background()

	cases := []struct {
		name string
		call *sdkmcp.CallToolParams
	}{
		{"unknown process path", &sdkmcp.CallToolParams{
			Name: "get_queue_status", Arguments: map[string]any{"processPath": "FLYING"},
		}},
		{"empty task id", &sdkmcp.CallToolParams{
			Name: "complete_task", Arguments: map[string]any{"taskId": "", "stationId": "itcov-mcp-station"},
		}},
		{"task not found", &sdkmcp.CallToolParams{
			Name: "complete_task", Arguments: map[string]any{"taskId": "ITCOV-NOPE", "stationId": "itcov-mcp-station"},
		}},
	}
	for _, tc := range cases {
		res, err := h.session.CallTool(ctx, tc.call)
		if err != nil {
			t.Fatalf("%s: transport error: %v", tc.name, err)
		}
		if !res.IsError {
			t.Fatalf("%s: must surface a tool error, got success", tc.name)
		}
	}
}
