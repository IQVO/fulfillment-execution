package mcp_test

import (
	"context"
	"regexp"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/fulfillment-execution/internal/adapters/inbound/mcp"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/system"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

// This file is the computational governance gate for the MCP surface
// (Phase 6). It boots the real server, lists its advertised tools over an
// in-process transport, and asserts the estate-wide rules from
// docs/docs/mcp/governance-charter.md. Because it is a plain `go test`, it
// runs inside the existing CI `test` / `arch-test` jobs with no new
// infrastructure — the left-shift equivalent of `make check` for the MCP
// surface. Copy this file into each bounded context's mcp package; only the
// import path and the buildDeps wiring change.

const (
	// maxTools is the charter's curated-surface budget (charter §2).
	maxTools = 8
)

// toolNamePattern enforces the charter's tool-naming convention (§3):
// snake_case, verb_noun, intent-level.
var toolNamePattern = regexp.MustCompile(`^[a-z]+(_[a-z]+)+$`)

// buildDeps wires a minimal, fully in-memory Deps so the server can be built
// for introspection without a database. It mirrors the composition root's
// wiring but needs no real infrastructure.
func buildDeps() inboundmcp.Deps {
	tasks := memory.NewTaskRepo()
	publisher := events.NewBufferedPublisher()
	clock := system.Clock{}
	return inboundmcp.Deps{
		GetQueueDepth: &usecases.GetQueueDepth{Tasks: tasks},
		CompleteTask:  &usecases.CompleteTask{Tasks: tasks, Publisher: publisher, Clock: clock},
		Tasks:         tasks,
		Now:           time.Now,
	}
}

// listTools connects an in-process client to the real server and returns the
// advertised tool set.
func listTools(t *testing.T) []*sdk.Tool {
	t.Helper()
	server := inboundmcp.NewServer(buildDeps())

	client := sdk.NewClient(&sdk.Implementation{Name: "governance", Version: "0"}, nil)
	ct, st := sdk.NewInMemoryTransports()
	ctx := context.Background()
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := client.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	res, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	return res.Tools
}

// TestGovernance_ToolCountWithinBudget enforces charter §2: a curated surface
// of at most maxTools intent-level tools.
func TestGovernance_ToolCountWithinBudget(t *testing.T) {
	tools := listTools(t)
	if len(tools) == 0 {
		t.Fatal("no tools advertised; expected a curated surface")
	}
	if len(tools) > maxTools {
		names := make([]string, 0, len(tools))
		for _, tool := range tools {
			names = append(names, tool.Name)
		}
		t.Fatalf("tool surface has %d tools, over the charter budget of %d: %v", len(tools), maxTools, names)
	}
}

// TestGovernance_ToolNaming enforces charter §3: snake_case, verb_noun names.
func TestGovernance_ToolNaming(t *testing.T) {
	for _, tool := range listTools(t) {
		if !toolNamePattern.MatchString(tool.Name) {
			t.Errorf("tool %q violates the naming convention (want snake_case verb_noun)", tool.Name)
		}
	}
}

// TestGovernance_ToolsAreAnnotated enforces charter §4: every tool must carry
// annotations, and a write (non-read-only) tool must be flagged destructive so
// a host can reason about its risk before letting a model call it. A tool
// missing annotations entirely is a governance failure.
func TestGovernance_ToolsAreAnnotated(t *testing.T) {
	for _, tool := range listTools(t) {
		if tool.Annotations == nil {
			t.Errorf("tool %q has no annotations (charter §4 requires read/destructive intent)", tool.Name)
			continue
		}
		// A tool that is not read-only is a state-changing tool and MUST be
		// annotated destructive.
		if !tool.Annotations.ReadOnlyHint {
			if tool.Annotations.DestructiveHint == nil || !*tool.Annotations.DestructiveHint {
				t.Errorf("write tool %q is not annotated destructive (charter §4)", tool.Name)
			}
		}
	}
}

// TestGovernance_ToolsHaveDescriptions enforces charter §4: a tool description
// is part of the safety surface the model reads, so it must be present.
func TestGovernance_ToolsHaveDescriptions(t *testing.T) {
	for _, tool := range listTools(t) {
		if tool.Description == "" {
			t.Errorf("tool %q has no description (charter §4)", tool.Name)
		}
	}
}
