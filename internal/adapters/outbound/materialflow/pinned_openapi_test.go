package materialflow

// The refusal catalogue the Task 4 EquipmentCommandPort REST client
// classifies, proven against the PINNED copy of material-flow-control's
// apis/openapi.yaml (testdata/material-flow-control.openapi.yaml,
// registered in contracts/pinned.json at IQVO/material-flow-control@develop,
// pinned_commit 12d2d81678ef6417ca393edce186de8556756a0b; the
// pinned-contracts CI job fails when the copy drifts). A pinned file is
// COPIED, never edited: refresh = copy the producer's file over the pin,
// then fix what fails here.
//
// The package holds ONLY this test until Task 4 lands the client: every
// code the client will map to a replay class (FINAL / OUTCOME /
// RETRYABLE) must exist in the pin with its documented class, and the
// request schema the client serializes must match.

import (
	"os"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const pinnedOpenAPI = "testdata/material-flow-control.openapi.yaml"

func TestTheRefusalCatalogueMatchesThePinnedOpenAPI(t *testing.T) {
	doc := loadPinnedOpenAPI(t)

	post := operation(t, doc, "/transport-requests", "post")
	operation(t, doc, "/transport-requests/{transportRequestId}/cancellation", "post")

	t.Run("transport request schema", func(t *testing.T) {
		assertTransportRequestSchema(t, doc)
	})

	t.Run("response catalogue", func(t *testing.T) {
		responses := mapOf(t, post, "responses")
		for _, code := range []string{"202", "400", "409", "410", "422", "500", "503"} {
			if _, ok := responses[code]; !ok {
				t.Errorf("POST /transport-requests documents no %s response", code)
			}
		}
	})

	t.Run("410 transport-request-expired is FINAL", func(t *testing.T) {
		desc := stringOf(component(t, doc, "responses", "Expired"), "description")
		for _, want := range []string{"transport-request-expired", "FINAL"} {
			if !strings.Contains(desc, want) {
				t.Errorf("Expired response description must name %q, got %q", want, desc)
			}
		}
	})

	t.Run("422 rejected is OUTCOME with the four reasons", func(t *testing.T) {
		desc := stringOf(component(t, doc, "responses", "Rejected"), "description")
		for _, want := range []string{
			"unknown-container", "container-busy", "destination-unreachable", "subsystem-unavailable", "OUTCOME",
		} {
			if !strings.Contains(desc, want) {
				t.Errorf("Rejected response description must name %q, got %q", want, desc)
			}
		}
	})
}

// assertTransportRequestSchema pins the create body the Task 4 client
// serializes: the required field set and the purpose/priority enums.
func assertTransportRequestSchema(t *testing.T, doc map[string]any) {
	t.Helper()
	schema := component(t, doc, "schemas", "TransportRequest")
	wantRequired := []string{
		"transport_request_id", "container_id", "source", "destination", "purpose", "priority", "due_by",
	}
	if got := stringListOf(schema, "required"); !slices.Equal(got, wantRequired) {
		t.Errorf("TransportRequest required %v, want %v", got, wantRequired)
	}
	props := mapOf(t, schema, "properties")
	assertEnum(t, props, "purpose", []string{"PICK", "PUTAWAY", "REPLENISH", "OUTBOUND", "RETURN"})
	assertEnum(t, props, "priority", []string{"NORMAL", "URGENT"})
}

func loadPinnedOpenAPI(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(pinnedOpenAPI)
	if err != nil {
		t.Fatalf("read pinned spec %s (copy it from the producer, never hand-write it): %v", pinnedOpenAPI, err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse pinned spec %s: %v", pinnedOpenAPI, err)
	}
	return doc
}

// operation resolves paths.<path>.<method>, failing when either is
// absent from the pin.
func operation(t *testing.T, doc map[string]any, path, method string) map[string]any {
	t.Helper()
	item, ok := mapOf(t, doc, "paths")[path].(map[string]any)
	if !ok {
		t.Fatalf("pinned spec documents no path %s", path)
	}
	op, ok := item[method].(map[string]any)
	if !ok {
		t.Fatalf("pinned spec documents no %s operation on %s", method, path)
	}
	return op
}

// component resolves components.<kind>.<name>, failing when absent.
func component(t *testing.T, doc map[string]any, kind, name string) map[string]any {
	t.Helper()
	c, ok := mapOf(t, mapOf(t, doc, "components"), kind)[name].(map[string]any)
	if !ok {
		t.Fatalf("pinned spec declares no components/%s/%s", kind, name)
	}
	return c
}

func mapOf(t *testing.T, node map[string]any, key string) map[string]any {
	t.Helper()
	m, ok := node[key].(map[string]any)
	if !ok {
		t.Fatalf("node has no %q object", key)
	}
	return m
}

func stringOf(node map[string]any, key string) string {
	s, _ := node[key].(string)
	return s
}

func stringListOf(node map[string]any, key string) []string {
	raw, _ := node[key].([]any)
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func assertEnum(t *testing.T, props map[string]any, field string, want []string) {
	t.Helper()
	p, ok := props[field].(map[string]any)
	if !ok {
		t.Errorf("TransportRequest declares no %s property", field)
		return
	}
	if got := stringListOf(p, "enum"); !slices.Equal(got, want) {
		t.Errorf("%s enum %v, want %v", field, got, want)
	}
}
