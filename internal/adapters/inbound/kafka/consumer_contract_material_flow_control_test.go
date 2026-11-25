package kafka_test

// Consumer contract test for the material-flow-control edge (plan 05b,
// D6): this service will subscribe to warehouse.material-flow-control.events
// and act on exactly five of the seven documented types —
// TransportAccepted, TransportRejected, ContainerArrivedAtStation,
// TransportCompleted and TransportFailed. ContainerSentToClarification is
// exception-management's fact and SubsystemCapacityChanged is
// wes-work-planning's: this consumer must skip them, never DLQ them.
//
// The contract is the PINNED copy of material-flow-control's
// apis/asyncapi.yaml (testdata/material-flow-control.asyncapi.yaml,
// registered in contracts/pinned.json at
// IQVO/material-flow-control@develop, pinned_commit
// 12d2d81678ef6417ca393edce186de8556756a0b; the pinned-contracts CI job
// fails when the copy drifts). A pinned file is COPIED, never edited.
//
// Task 5 builds the decoder and the handler; until they exist this test
// pins the SCHEMA surface the plan decodes: every field the five
// consumed events must carry, the topic address the consumer subscribes
// to, and the producer's own examples carrying those fields under a
// com.warehouse.wes.material-flow-control.* type.

import (
	"slices"
	"strings"
	"testing"
)

// materialFlowControlTopic is the address the pinned spec declares for
// its materialFlowControlEvents channel — the subscription Task 5 wires.
const materialFlowControlTopic = "warehouse.material-flow-control.events"

// mfcConsumedFields maps each consumed event schema to the data fields
// the plan decodes from it (D6: exactly these five types), each field
// paired with the JSON type the pin declares for it.
//
// carry (Task 1 review): the field->type pairs pin the TYPE, not only
// presence/required — a producer refactor flipping queue_position from
// integer to string must fail here, not first in Task 5's decoder.
var mfcConsumedFields = map[string]map[string]string{
	"TransportAcceptedEvent": {
		"transport_order_id": "string", "transport_request_id": "string", "container_id": "string",
		"purpose": "string", "accepted_at": "string",
	},
	"TransportRejectedEvent": {
		"transport_request_id": "string", "container_id": "string",
		"reason": "string", "rejected_at": "string",
	},
	"ContainerArrivedAtStationEvent": {
		"transport_order_id": "string", "transport_request_id": "string", "container_id": "string",
		"station_id": "string", "queue_position": "integer", "arrived_at": "string",
	},
	"TransportCompletedEvent": {
		"transport_order_id": "string", "transport_request_id": "string", "container_id": "string",
		"destination": "string", "completed_at": "string",
	},
	"TransportFailedEvent": {
		"transport_order_id": "string", "transport_request_id": "string", "container_id": "string",
		"reason": "string", "failed_at": "string",
	},
}

func TestMaterialFlowControlConsumedTypesCarryTheDecodedFields(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedMaterialFlowControlSpec)
	ch := materialFlowControlEventsChannel(t, doc)

	for schema, fields := range mfcConsumedFields {
		t.Run(schema, func(t *testing.T) {
			msg := channelMessage(t, doc, ch, strings.TrimSuffix(schema, "Event"))
			assertSchemaCarriesFields(t, doc, schema, fields)
			assertExamplesCarryFields(t, schema, msg, fields)
		})
	}

	// carry (Task 1 review): the TransportRejected reason enum is what
	// Task 5's table test switches on.
	t.Run("TransportRejectedEvent reason enum", func(t *testing.T) {
		data := eventDataSubschema(t, doc, "TransportRejectedEvent")
		props, ok := data["properties"].(map[string]any)
		if !ok {
			t.Fatalf("TransportRejectedEvent data object declares no properties")
		}
		reason, ok := props["reason"].(map[string]any)
		if !ok {
			t.Fatalf("TransportRejectedEvent data declares no reason property")
		}
		want := []string{"UNKNOWN_CONTAINER", "CONTAINER_BUSY", "DESTINATION_UNREACHABLE", "SUBSYSTEM_UNAVAILABLE"}
		if got := stringList(reason, "enum"); !slices.Equal(got, want) {
			t.Errorf("TransportRejectedEvent reason enum %v, want %v", got, want)
		}
	})

	// D6: the channel documents exactly the seven types of the pin; FE
	// consumes five and must skip (never DLQ) ContainerSentToClarification
	// and SubsystemCapacityChanged.
	for _, name := range []string{
		"TransportAccepted", "TransportRejected", "ContainerArrivedAtStation",
		"TransportCompleted", "TransportFailed",
		"ContainerSentToClarification", "SubsystemCapacityChanged",
	} {
		if _, ok := ch.Messages[name]; !ok {
			t.Errorf("pinned channel documents no %s message", name)
		}
	}
}

// mfcEnvelopeRequired names the CloudEvents attributes the pin's
// CloudEventEnvelope schema requires on every material-flow-control
// event (the envelope also requires data; the event data fields are
// pinned per type by mfcConsumedFields).
var mfcEnvelopeRequired = []string{
	"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema",
}

// carry (Task 1 review): the CloudEvents envelope invariants — the
// required attribute set, the source and specversion consts, every
// example carrying id/source/subject/time, and the dataschema urn
// family — are the E3 gap the review found; a producer dropping subject
// or moving source must fail here, not first in Task 5's decoder.
//
// (Split into two helpers for gocognit; every assertion kept.)
func TestMaterialFlowControlCloudEventsEnvelopeInvariants(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedMaterialFlowControlSpec)
	ch := materialFlowControlEventsChannel(t, doc)
	assertEnvelopeSchema(t, doc)
	assertExamplesCarryEnvelope(t, doc, ch)
}

// assertEnvelopeSchema pins the CloudEventEnvelope schema's required
// attribute set and its source/specversion consts.
func assertEnvelopeSchema(t *testing.T, doc *pinnedAsyncAPI) {
	t.Helper()
	raw, ok := doc.Components.Schemas["CloudEventEnvelope"]
	if !ok {
		t.Fatalf("pinned spec declares no components/schemas/CloudEventEnvelope")
	}
	envelope, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("CloudEventEnvelope schema is not an object")
	}
	required := requiredList(envelope)
	for _, attr := range mfcEnvelopeRequired {
		if !slices.Contains(required, attr) {
			t.Errorf("CloudEventEnvelope does not require %q", attr)
		}
	}
	consts := propertyConsts(envelope)
	if got := consts["source"]; got != "/warehouse/material-flow-control" {
		t.Errorf("CloudEventEnvelope source const %q, want /warehouse/material-flow-control", got)
	}
	if got := consts["specversion"]; got != "1.0" {
		t.Errorf("CloudEventEnvelope specversion const %q, want 1.0", got)
	}
}

// assertExamplesCarryEnvelope pins every message example of the channel
// to carry id/source/subject/time and a dataschema of the
// urn:warehouse:material-flow-control:events:<Type>:v1 family.
func assertExamplesCarryEnvelope(t *testing.T, doc *pinnedAsyncAPI, ch pinnedChannel) {
	t.Helper()
	for name := range ch.Messages {
		msg := channelMessage(t, doc, ch, name)
		if len(msg.Examples) == 0 {
			t.Errorf("pinned spec documents %s without examples", name)
			continue
		}
		wantDataschema := "urn:warehouse:material-flow-control:events:" + name + ":v1"
		for _, ex := range msg.Examples {
			for _, attr := range []string{"id", "source", "subject", "time"} {
				if _, ok := ex.Payload[attr]; !ok {
					t.Errorf("%s example carries no envelope attribute %q", name, attr)
				}
			}
			if got, _ := ex.Payload["dataschema"].(string); got != wantDataschema {
				t.Errorf("%s example dataschema %q, want %q", name, got, wantDataschema)
			}
		}
	}
}

// materialFlowControlEventsChannel resolves the pin's
// materialFlowControlEvents channel and asserts its address pins the
// consumer's subscription (Task 5).
func materialFlowControlEventsChannel(t *testing.T, doc *pinnedAsyncAPI) pinnedChannel {
	t.Helper()
	ch, ok := doc.Channels["materialFlowControlEvents"]
	if !ok {
		t.Fatalf("pinned material-flow-control spec declares no materialFlowControlEvents channel")
	}
	if ch.Address != materialFlowControlTopic {
		t.Errorf("materialFlowControlEvents address %q, want %q", ch.Address, materialFlowControlTopic)
	}
	if len(ch.Messages) == 0 {
		t.Fatalf("materialFlowControlEvents channel declares no messages")
	}
	return ch
}

// channelMessage resolves one message of the channel's messages map,
// failing on a dangling $ref.
func channelMessage(t *testing.T, doc *pinnedAsyncAPI, ch pinnedChannel, name string) pinnedMessage {
	t.Helper()
	ref, ok := ch.Messages[name]
	if !ok {
		t.Fatalf("materialFlowControlEvents channel documents no %s message", name)
	}
	return doc.message(t, refName(t, ref.Ref))
}

// assertSchemaCarriesFields asserts the event schema's data object
// DECLARES every field the plan decodes (as a property), REQUIRES it
// (allOf-aware: the pin composes the CloudEvents envelope and a data
// overlay) and declares it with the pinned JSON type.
func assertSchemaCarriesFields(t *testing.T, doc *pinnedAsyncAPI, schema string, fields map[string]string) {
	t.Helper()
	data := eventDataSubschema(t, doc, schema)
	props, ok := data["properties"].(map[string]any)
	if !ok {
		t.Fatalf("%s data object declares no properties", schema)
	}
	required := requiredList(data)
	for f, wantType := range fields {
		p, ok := props[f].(map[string]any)
		if !ok {
			t.Errorf("%s data has no property %q (the plan decodes it)", schema, f)
			continue
		}
		if !slices.Contains(required, f) {
			t.Errorf("%s data does not require %q (the plan decodes it unconditionally)", schema, f)
		}
		// carry (Task 1 review): the type flip is the drift a producer
		// refactor produces; presence and required alone do not bite.
		if got, _ := p["type"].(string); got != wantType {
			t.Errorf("%s data property %q has type %q, want the pinned %q", schema, f, got, wantType)
		}
	}
}

// assertExamplesCarryFields asserts every pinned example of the message
// is a CloudEvents document of a com.warehouse.wes.material-flow-control.*
// type whose data map carries every decoded field.
func assertExamplesCarryFields(t *testing.T, schema string, msg pinnedMessage, fields map[string]string) {
	t.Helper()
	if len(msg.Examples) == 0 {
		t.Fatalf("pinned material-flow-control spec documents %s without examples", schema)
	}
	for _, ex := range msg.Examples {
		typ, _ := ex.Payload["type"].(string)
		if !strings.HasPrefix(typ, "com.warehouse.wes.material-flow-control.") {
			t.Errorf("%s example type %q lacks the com.warehouse.wes.material-flow-control. prefix", schema, typ)
		}
		data, ok := ex.Payload["data"].(map[string]any)
		if !ok {
			t.Errorf("%s example of type %s carries no data object", schema, typ)
			continue
		}
		for f := range fields {
			if _, ok := data[f]; !ok {
				t.Errorf("%s example of type %s data misses %q", schema, typ, f)
			}
		}
	}
}

// stringList returns node's string list under key (nil when absent), for
// enum-style assertions on raw schema maps.
func stringList(node map[string]any, key string) []string {
	raw, _ := node[key].([]any)
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// eventDataSubschema resolves an event schema (envelope $ref + data
// overlay under allOf) to the data object schema, failing when no
// overlay carries one.
func eventDataSubschema(t *testing.T, doc *pinnedAsyncAPI, schema string) map[string]any {
	t.Helper()
	raw, ok := doc.Components.Schemas[schema]
	if !ok {
		t.Fatalf("pinned spec declares no components/schemas/%s", schema)
	}
	if data := findDataObject(t, doc, raw, map[string]bool{}); data != nil {
		return data
	}
	t.Fatalf("pinned spec's %s declares no data object", schema)
	return nil
}

// findDataObject returns the first `data` property schema carrying its
// own properties, resolving $refs and walking allOf overlays.
func findDataObject(t *testing.T, doc *pinnedAsyncAPI, node any, resolving map[string]bool) map[string]any {
	t.Helper()
	v, ok := node.(map[string]any)
	if !ok {
		return nil
	}
	if schema, ok := doc.resolveSchemaRef(t, v, resolving); ok {
		return findDataObject(t, doc, schema, resolving)
	}
	if props, ok := v["properties"].(map[string]any); ok {
		if data, ok := props["data"].(map[string]any); ok {
			if _, declared := data["properties"]; declared {
				return data
			}
		}
	}
	for _, item := range allOfItems(v) {
		if data := findDataObject(t, doc, item, resolving); data != nil {
			return data
		}
	}
	return nil
}
