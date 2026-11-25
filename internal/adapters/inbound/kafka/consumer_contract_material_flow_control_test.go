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
// the plan decodes from it (D6: exactly these five types).
var mfcConsumedFields = map[string][]string{
	"TransportAcceptedEvent":         {"transport_order_id", "transport_request_id", "container_id", "purpose", "accepted_at"},
	"TransportRejectedEvent":         {"transport_request_id", "container_id", "reason", "rejected_at"},
	"ContainerArrivedAtStationEvent": {"transport_order_id", "transport_request_id", "container_id", "station_id", "queue_position", "arrived_at"},
	"TransportCompletedEvent":        {"transport_order_id", "transport_request_id", "container_id", "destination", "completed_at"},
	"TransportFailedEvent":           {"transport_order_id", "transport_request_id", "container_id", "reason", "failed_at"},
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
// DECLARES every field the plan decodes (as a property) and REQUIRES it
// (allOf-aware: the pin composes the CloudEvents envelope and a data
// overlay).
func assertSchemaCarriesFields(t *testing.T, doc *pinnedAsyncAPI, schema string, fields []string) {
	t.Helper()
	data := eventDataSubschema(t, doc, schema)
	props, ok := data["properties"].(map[string]any)
	if !ok {
		t.Fatalf("%s data object declares no properties", schema)
	}
	required := requiredList(data)
	for _, f := range fields {
		if _, ok := props[f]; !ok {
			t.Errorf("%s data has no property %q (the plan decodes it)", schema, f)
		}
		if !slices.Contains(required, f) {
			t.Errorf("%s data does not require %q (the plan decodes it unconditionally)", schema, f)
		}
	}
}

// assertExamplesCarryFields asserts every pinned example of the message
// is a CloudEvents document of a com.warehouse.wes.material-flow-control.*
// type whose data map carries every decoded field.
func assertExamplesCarryFields(t *testing.T, schema string, msg pinnedMessage, fields []string) {
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
		for _, f := range fields {
			if _, ok := data[f]; !ok {
				t.Errorf("%s example of type %s data misses %q", schema, typ, f)
			}
		}
	}
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
