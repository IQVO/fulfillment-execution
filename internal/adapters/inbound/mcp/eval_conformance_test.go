// E2 — over-the-wire protocol conformance evals.
//
// server_test.go already proves the happy paths (list+call, resource read,
// prompt get). These evals pin the protocol behaviors a model host relies
// on when things go WRONG or get exotic: the initialize handshake and its
// negotiation results, error-shape guarantees for unknown tools / bad
// argument types / unknown extra arguments / unknown resources and prompts,
// discovery of resources, resource templates, and prompts, and session
// lifecycle after close. All against the real Streamable HTTP handler —
// never in-process values.
package mcp_test

import (
	"strings"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// TestEvalConformance_InitializeHandshake pins what a host learns at
// connect time: a negotiated protocol result carrying this server's name
// and non-empty usage instructions.
func TestEvalConformance_InitializeHandshake(t *testing.T) {
	h := newEvalHarness(t)

	init := h.session.InitializeResult()
	if init == nil {
		t.Fatal("no initialize result on the session")
	}
	if init.ServerInfo.Name == "" {
		t.Error("initialize result has no server name")
	}
	if strings.TrimSpace(init.Instructions) == "" {
		t.Error("initialize result carries no instructions — hosts surface these to the model; an empty string wastes the connect handshake")
	}
}

// TestEvalConformance_UnknownToolIsAProtocolError pins that calling a tool
// the server never advertised is rejected at the protocol layer (a
// JSON-RPC error), not silently executed or returned as a tool result.
func TestEvalConformance_UnknownToolIsAProtocolError(t *testing.T) {
	h := newEvalHarness(t)

	if err := h.callTool(t.Context(), "definitely_not_a_tool", map[string]any{}); err == nil {
		t.Fatal("calling an unknown tool must be a protocol error, got success")
	}
}

// TestEvalConformance_WrongTypedArgumentIsRejected pins that a model
// sending a string parameter as a number is rejected by the typed tool
// schema — either as a protocol error or a tool-level error result, never
// as a success that silently coerces.
func TestEvalConformance_WrongTypedArgumentIsRejected(t *testing.T) {
	h := newEvalHarness(t)

	res, err := h.session.CallTool(t.Context(), &sdk.CallToolParams{
		Name:      "get_queue_status",
		Arguments: map[string]any{"processPath": 42},
	})
	switch {
	case err != nil:
		// Protocol-level rejection: acceptable.
	case res != nil && res.IsError:
		// Tool-level rejection: acceptable.
	default:
		t.Fatalf("wrong-typed processPath argument must not succeed: err=%v res=%+v", err, res)
	}
}

// TestEvalConformance_ExtraArgumentsAreRejected pins the strict side of
// argument handling: the typed tool schemas disallow undeclared properties
// (the SDK default, additionalProperties: false), so stray model chatter
// forwarded by a host is rejected loudly rather than silently ignored.
func TestEvalConformance_ExtraArgumentsAreRejected(t *testing.T) {
	h := newEvalHarness(t)

	res, err := h.session.CallTool(t.Context(), &sdk.CallToolParams{
		Name: "get_queue_status",
		Arguments: map[string]any{
			"processPath":   "PICK",
			"model_chatter": "I think this queue might be backed up",
			"step":          2,
		},
	})
	switch {
	case err != nil:
		// Protocol-level rejection: acceptable.
	case res != nil && res.IsError:
		// Tool-level rejection: acceptable.
	default:
		t.Fatalf("unknown extra arguments must not succeed: err=%v res=%+v", err, res)
	}
}

// TestEvalConformance_ResourcesAreDiscoverable pins that the three static
// queue-status resources surface through resources/list — a host can find
// them without out-of-band knowledge.
func TestEvalConformance_ResourcesAreDiscoverable(t *testing.T) {
	h := newEvalHarness(t)

	resources, err := h.session.ListResources(t.Context(), nil)
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}
	want := map[string]bool{
		"queue://fulfillment/PICK/status": false,
		"queue://fulfillment/PACK/status": false,
		"queue://fulfillment/SLAM/status": false,
	}
	for _, r := range resources.Resources {
		if _, ok := want[r.URI]; ok {
			want[r.URI] = true
		}
	}
	for uri, seen := range want {
		if !seen {
			t.Errorf("resource %q not discoverable via resources/list", uri)
		}
	}
}

// TestEvalConformance_NoResourceTemplates pins that this server advertises
// NO resource templates: its resources are static, per-process-path URIs.
// The empty list is the contract — a template appearing here is a surface
// change that must be a conscious one.
func TestEvalConformance_NoResourceTemplates(t *testing.T) {
	h := newEvalHarness(t)

	templates, err := h.session.ListResourceTemplates(t.Context(), nil)
	if err != nil {
		t.Fatalf("list resource templates: %v", err)
	}
	if len(templates.ResourceTemplates) != 0 {
		t.Fatalf("server advertises resource templates %v — this surface is static-resource-only by design", templates.ResourceTemplates)
	}
}

// TestEvalConformance_UnknownResourceIsRejected pins that reading a URI
// outside the three static resources is a protocol error.
func TestEvalConformance_UnknownResourceIsRejected(t *testing.T) {
	h := newEvalHarness(t)

	_, err := h.session.ReadResource(t.Context(), &sdk.ReadResourceParams{URI: "queue://fulfillment/FLYING/status"})
	if err == nil {
		t.Fatal("reading an unknown resource URI must be rejected")
	}
}

// TestEvalConformance_UnknownPromptIsRejected pins that getting a prompt
// the server does not have is a protocol error.
func TestEvalConformance_UnknownPromptIsRejected(t *testing.T) {
	h := newEvalHarness(t)

	_, err := h.session.GetPrompt(t.Context(), &sdk.GetPromptParams{Name: "no_such_prompt"})
	if err == nil {
		t.Fatal("getting an unknown prompt must be rejected")
	}
}

// TestEvalConformance_PromptIsListable pins prompt discovery.
func TestEvalConformance_PromptIsListable(t *testing.T) {
	h := newEvalHarness(t)

	prompts, err := h.session.ListPrompts(t.Context(), nil)
	if err != nil {
		t.Fatalf("list prompts: %v", err)
	}
	found := false
	for _, p := range prompts.Prompts {
		if p.Name == "triage_backlog" {
			found = true
		}
	}
	if !found {
		t.Fatalf("triage_backlog prompt not discoverable: %+v", prompts.Prompts)
	}
}

// TestEvalConformance_SessionCloseEndsCalls pins that after the client
// closes the session, further calls fail loudly instead of silently
// no-op'ing.
func TestEvalConformance_SessionCloseEndsCalls(t *testing.T) {
	h := newEvalHarness(t)

	if err := h.session.Close(); err != nil {
		t.Fatalf("close session: %v", err)
	}
	if _, err := h.session.CallTool(t.Context(), &sdk.CallToolParams{
		Name:      "get_queue_status",
		Arguments: map[string]any{"processPath": "PICK"},
	}); err == nil {
		t.Fatal("calling a tool on a closed session must fail")
	}
}
