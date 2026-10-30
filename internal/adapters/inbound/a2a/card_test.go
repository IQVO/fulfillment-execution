package a2a

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

const testAgentURL = "http://fulfillment-execution-a2a.warehouse-systems.svc.cluster.local:8093/"

// TestCardShape pins the identity, interface and capability surface of the
// Agent Card: the discovery contract other contexts program against.
func TestCardShape(t *testing.T) {
	card := Card(Config{AgentURL: testAgentURL, Version: "1.0.0"})

	if card.Name != "fulfillment-execution" {
		t.Errorf("card name = %q, want fulfillment-execution", card.Name)
	}
	if len(card.SupportedInterfaces) != 1 {
		t.Fatalf("card has %d interfaces, want 1", len(card.SupportedInterfaces))
	}
	iface := card.SupportedInterfaces[0]
	if iface.URL != testAgentURL || iface.ProtocolBinding != a2a.TransportProtocolJSONRPC {
		t.Errorf("interface = %+v, want JSONRPC at the configured agent URL", iface)
	}
	if card.Capabilities.Streaming {
		t.Error("streaming must not be advertised: this is a message-only agent")
	}
	if card.SecuritySchemes != nil {
		t.Error("card must declare no security schemes: the surface is unauthenticated fleet-wide")
	}
	if card.Provider == nil || card.Provider.Org != "IQVO" {
		t.Errorf("provider = %+v, want IQVO", card.Provider)
	}
}

// TestCardSkillsAreCuratedAndTruthful pins the two-skill surface (one read,
// one write) and proves the card never lies about its own contract: every
// advertised example must parse under the SAME strict decoder the executor
// uses, name its own skill, and carry arguments that skill accepts.
func TestCardSkillsAreCuratedAndTruthful(t *testing.T) {
	card := Card(Config{AgentURL: testAgentURL, Version: "1.0.0"})

	tags := map[string]string{skillBacklogStatus: "read", skillCompleteTask: "write"}
	if len(card.Skills) != len(tags) {
		t.Fatalf("card has %d skills, want exactly %d (curated, goal-shaped - not one per endpoint)", len(card.Skills), len(tags))
	}
	for _, s := range card.Skills {
		wantTag, known := tags[s.ID]
		if !known {
			t.Errorf("unexpected skill %q on the card", s.ID)
			continue
		}
		if s.Name == "" || s.Description == "" || len(s.Examples) == 0 {
			t.Errorf("skill %q lacks a name, description or example", s.ID)
		}
		if !containsTag(s.Tags, wantTag) {
			t.Errorf("skill %q must be tagged %q so a delegating agent can tell reads from writes, tags = %v", s.ID, wantTag, s.Tags)
		}
		for _, ex := range s.Examples {
			assertExampleTruthful(t, s.ID, ex)
		}
	}
}

func assertExampleTruthful(t *testing.T, skill, example string) {
	t.Helper()
	var req skillRequest
	if err := decodeStrict([]byte(example), &req); err != nil {
		t.Errorf("skill %q example is not valid under the strict decoder: %v", skill, err)
		return
	}
	if req.Skill != skill {
		t.Errorf("skill %q example names skill %q", skill, req.Skill)
	}
	var err error
	switch skill {
	case skillBacklogStatus:
		err = decodeStrict(req.Arguments, &backlogStatusArgs{})
	case skillCompleteTask:
		var args completeTaskArgs
		if err = decodeStrict(req.Arguments, &args); err == nil && args.validate() != "" {
			err = errors.New(args.validate())
		}
	}
	if err != nil {
		t.Errorf("skill %q example arguments are rejected by its own contract: %v", skill, err)
	}
}

func containsTag(tags []string, want string) bool {
	for _, tag := range tags {
		if tag == want {
			return true
		}
	}
	return false
}

// TestCardNeverOffersWhatMCPWithholds keeps the A2A surface inside the MCP
// charter: the only write is the one complete_task MCP tool, and nothing that
// claims, releases or creates work is advertised.
func TestCardNeverOffersWhatMCPWithholds(t *testing.T) {
	raw, err := json.Marshal(Card(Config{AgentURL: testAgentURL, Version: "1.0.0"}))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, skillID := range []string{"claim_", "release_", "create_", "scan_", "break_case"} {
		if strings.Contains(text, `"id":"`+skillID) {
			t.Errorf("card advertises a %s* skill the MCP surface does not offer", skillID)
		}
	}
	writes := 0
	for _, s := range Card(Config{AgentURL: testAgentURL}).Skills {
		if containsTag(s.Tags, "write") {
			writes++
		}
	}
	if writes != 1 {
		t.Errorf("card advertises %d write skills, want exactly 1 (complete_task, as on MCP)", writes)
	}
}

// TestCardSerializable proves the card marshals to JSON a client can parse
// back, with the discovery fields intact.
func TestCardSerializable(t *testing.T) {
	raw, err := json.Marshal(Card(Config{AgentURL: testAgentURL, Version: "1.0.0"}))
	if err != nil {
		t.Fatalf("marshal card: %v", err)
	}
	var back a2a.AgentCard
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("card JSON does not round-trip: %v", err)
	}
	if back.Name != "fulfillment-execution" || len(back.Skills) != 2 {
		t.Errorf("round-tripped card = %q with %d skills", back.Name, len(back.Skills))
	}
}
