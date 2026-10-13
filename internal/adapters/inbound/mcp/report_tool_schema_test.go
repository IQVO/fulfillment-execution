package mcp_test

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// advertisedRequired returns the sorted `required` list of a tool's
// advertised input schema, read over the wire from tools/list.
func advertisedRequired(t *testing.T, cs *sdk.ClientSession, name string) []string {
	t.Helper()
	list, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	for _, tl := range list.Tools {
		if tl.Name != name {
			continue
		}
		raw, err := json.Marshal(tl.InputSchema)
		if err != nil {
			t.Fatalf("marshal input schema: %v", err)
		}
		var schema struct {
			Required []string `json:"required"`
		}
		if err := json.Unmarshal(raw, &schema); err != nil {
			t.Fatalf("decode input schema: %v", err)
		}
		sort.Strings(schema.Required)
		return schema.Required
	}
	t.Fatalf("%s not advertised with a reports client configured", name)
	return nil
}

// TestReportTools_OptionalFiltersAreOptionalOnTheWire pins gap fix D16 at
// the protocol level: both curated report tools require only from and to,
// and a call that leaves the optional filters out reaches the handler
// instead of being rejected by the SDK's schema validation.
func TestReportTools_OptionalFiltersAreOptionalOnTheWire(t *testing.T) {
	h := newEvalHarness(t)
	window := map[string]any{"from": "2026-06-01T00:00:00Z", "to": "2026-06-02T00:00:00Z"}

	for _, name := range []string{"get_fulfillment_throughput_report", "get_on_time_to_cpt"} {
		t.Run(name, func(t *testing.T) {
			if got := advertisedRequired(t, h.session, name); !slices.Equal(got, []string{"from", "to"}) {
				t.Fatalf("required = %v, want [from to] (the filters are optional)", got)
			}
			res, err := h.session.CallTool(context.Background(), &sdk.CallToolParams{Name: name, Arguments: window})
			if err != nil {
				t.Fatalf("call without optional filters: %v", err)
			}
			if res.IsError {
				t.Fatalf("call without optional filters returned a tool error: %+v", res.Content)
			}
			q := h.reports.lastQuery
			if q.From != "2026-06-01T00:00:00Z" || q.TaskType != "" || q.StationId != "" || q.Granularity != "" {
				t.Fatalf("forwarded query = %+v", q)
			}
		})
	}
}
