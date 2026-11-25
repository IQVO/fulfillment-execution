package kafka_test

// Shared support for the event consumer contract tests (E1-E4), which
// validate this repo's inbound Kafka edges against PINNED copies of the
// producing side's AsyncAPI spec (contracts/pinned.json; the
// pinned-contracts CI job fails when a copy drifts from the producer's
// develop). The pinned copies are byte-for-byte producer files: never
// edit them — refresh = copy the producer's file over the pin.
//
// The analytics copy is a SELF-pin: warehouse.fulfillment.analytics is
// this repo's own topic, consumed by its own projector
// (cmd/fulfillment-projector), so this repo is both its producer and its
// consumer and pins its own apis/asyncapi.yaml.

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The pinned spec copies next to this consuming adapter.
const (
	pinnedWorkPlanningSpec        = "testdata/wes-work-planning.asyncapi.yaml"
	pinnedProductMasterSpec       = "testdata/product-master.asyncapi.yaml"
	pinnedTaskInterleavingSpec    = "testdata/task-interleaving.asyncapi.yaml"
	pinnedOwnAnalyticsSpec        = "testdata/fulfillment-execution.asyncapi.yaml"
	pinnedMaterialFlowControlSpec = "testdata/material-flow-control.asyncapi.yaml"
)

// pinnedAsyncAPI is the minimal slice of AsyncAPI 2.6 the contract tests
// read: channels (with their operations), and the components/messages +
// components/schemas the channel message $refs point at.
type pinnedAsyncAPI struct {
	AsyncAPI           string                   `yaml:"asyncapi"`
	DefaultContentType string                   `yaml:"defaultContentType"`
	Channels           map[string]pinnedChannel `yaml:"channels"`
	Components         pinnedComponents         `yaml:"components"`
}

type pinnedChannel struct {
	Subscribe *pinnedOperation `yaml:"subscribe"`
	Publish   *pinnedOperation `yaml:"publish"`
	// AsyncAPI 3.0 shape (material-flow-control): channels keyed by
	// logical name with the topic in address and a channel-level
	// messages map; Subscribe/Publish stay nil for it.
	Address  string                      `yaml:"address"`
	Messages map[string]pinnedMessageRef `yaml:"messages"`
}

type pinnedOperation struct {
	OperationID string            `yaml:"operationId"`
	Message     *pinnedMessageRef `yaml:"message"`
}

// pinnedMessageRef is either a single $ref or a oneOf list of $refs.
type pinnedMessageRef struct {
	Ref   string             `yaml:"$ref"`
	OneOf []pinnedMessageRef `yaml:"oneOf"`
}

type pinnedComponents struct {
	Messages map[string]pinnedMessage `yaml:"messages"`
	Schemas  map[string]any           `yaml:"schemas"`
}

type pinnedMessage struct {
	Name        string          `yaml:"name"`
	ContentType string          `yaml:"contentType"`
	Payload     any             `yaml:"payload"`
	Examples    []pinnedExample `yaml:"examples"`
}

// pinnedExample is a message example: a full wire payload (a structured
// CloudEvents JSON document, as YAML) plus its Kafka headers.
type pinnedExample struct {
	Name    string            `yaml:"name"`
	Headers map[string]string `yaml:"headers"`
	Payload map[string]any    `yaml:"payload"`
}

// loadPinnedAsyncAPI reads and parses one pinned spec copy.
func loadPinnedAsyncAPI(t *testing.T, path string) *pinnedAsyncAPI {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read pinned spec %s (copy it from the producer, never hand-write it): %v", path, err)
	}
	var doc pinnedAsyncAPI
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse pinned spec %s: %v", path, err)
	}
	return &doc
}

// subscribeMessageRefs returns the components/messages names a topic's
// 'subscribe:' operation documents. Fleet convention (channel-perspective,
// inverted from AsyncAPI 2.6's app perspective): a PRODUCER's own topics
// appear under 'subscribe:' — so reading a producer's pinned spec, the
// 'subscribe:' channels are exactly what it publishes, and the message
// list there is the contract a consumer subscribes to. (Not uniform
// fleet-wide: process-path-management documents its own topic under
// 'publish:' — that pin's contract test reads the other key.)
func (d *pinnedAsyncAPI) subscribeMessageRefs(t *testing.T, topic string) []string {
	t.Helper()
	ch, ok := d.Channels[topic]
	if !ok {
		t.Fatalf("pinned spec has no channel for topic %q", topic)
	}
	if ch.Subscribe == nil {
		t.Fatalf("pinned spec documents topic %q without a 'subscribe:' operation", topic)
	}
	if ch.Subscribe.Message == nil {
		t.Fatalf("pinned spec's subscribe operation on %q carries no message", topic)
	}
	var refs []pinnedMessageRef
	if ch.Subscribe.Message.Ref != "" {
		refs = append(refs, *ch.Subscribe.Message)
	}
	refs = append(refs, ch.Subscribe.Message.OneOf...)
	if len(refs) == 0 {
		t.Fatalf("pinned spec's subscribe operation on %q lists no messages", topic)
	}
	names := make([]string, 0, len(refs))
	for _, r := range refs {
		names = append(names, refName(t, r.Ref))
	}
	return names
}

// message resolves one components/messages entry, failing on a dangling
// $ref (a spec whose channel points at a message it does not define).
func (d *pinnedAsyncAPI) message(t *testing.T, name string) pinnedMessage {
	t.Helper()
	msg, ok := d.Components.Messages[name]
	if !ok {
		t.Fatalf("pinned spec references undefined components/messages/%s", name)
	}
	return msg
}

// payloadConsts collects the const values a message payload declares for
// CloudEvents envelope attributes (type, dataschema, ...), wherever the
// spec keeps them — directly under properties, in an allOf overlay on the
// shared envelope schema, or behind a payload-level $ref into
// components/schemas.
func (d *pinnedAsyncAPI) payloadConsts(t *testing.T, payload any) map[string]string {
	t.Helper()
	consts := map[string]string{}
	d.collectPayloadConsts(t, payload, consts, map[string]bool{})
	return consts
}

func (d *pinnedAsyncAPI) collectPayloadConsts(t *testing.T, node any, consts map[string]string, resolving map[string]bool) {
	v, ok := node.(map[string]any)
	if !ok {
		return
	}
	if schema, ok := d.resolveSchemaRef(t, v, resolving); ok {
		d.collectPayloadConsts(t, schema, consts, resolving)
		return
	}
	for attr, c := range propertyConsts(v) {
		if _, seen := consts[attr]; !seen {
			consts[attr] = c
		}
	}
	for _, item := range allOfItems(v) {
		d.collectPayloadConsts(t, item, consts, resolving)
	}
}

// resolveSchemaRef resolves node's top-level $ref into components/schemas
// (cycle-guarded). ok is false when node carries no $ref, or the ref is
// already being resolved.
func (d *pinnedAsyncAPI) resolveSchemaRef(t *testing.T, node map[string]any, resolving map[string]bool) (any, bool) {
	ref, ok := node["$ref"].(string)
	if !ok {
		return nil, false
	}
	name := refName(t, ref)
	if resolving[name] {
		return nil, false
	}
	resolving[name] = true
	schema, ok := d.Components.Schemas[name]
	if !ok {
		t.Fatalf("pinned spec: unresolved schema $ref %q", ref)
	}
	return schema, true
}

// propertyConsts returns the const string each directly-declared property
// of node pins (empty when node pins none).
func propertyConsts(node map[string]any) map[string]string {
	consts := map[string]string{}
	props, ok := node["properties"].(map[string]any)
	if !ok {
		return consts
	}
	for attr, p := range props {
		if pm, ok := p.(map[string]any); ok {
			if c, ok := pm["const"].(string); ok {
				consts[attr] = c
			}
		}
	}
	return consts
}

// allOfItems returns node's allOf entries (nil when none).
func allOfItems(node map[string]any) []any {
	items, _ := node["allOf"].([]any)
	return items
}

// envelopeRequired returns the required attribute names declared by a
// message payload's envelope schema (resolving top-level $refs into
// components/schemas). The subset that names CloudEvents attributes is
// what E3 asserts on.
func (d *pinnedAsyncAPI) envelopeRequired(t *testing.T, payload any) map[string]bool {
	t.Helper()
	required := map[string]bool{}
	d.collectRequired(t, payload, required, map[string]bool{})
	return required
}

func (d *pinnedAsyncAPI) collectRequired(t *testing.T, node any, required map[string]bool, resolving map[string]bool) {
	v, ok := node.(map[string]any)
	if !ok {
		return
	}
	for _, r := range requiredList(v) {
		required[r] = true
	}
	if schema, ok := d.resolveSchemaRef(t, v, resolving); ok {
		d.collectRequired(t, schema, required, resolving)
		return
	}
	for _, item := range allOfItems(v) {
		d.collectRequired(t, item, required, resolving)
	}
}

// requiredList returns node's required attribute names (nil when none).
func requiredList(node map[string]any) []string {
	raw, ok := node["required"].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if s, ok := r.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// refName extracts the last path segment of a local $ref
// ("#/components/messages/TaskCompleted" -> "TaskCompleted").
func refName(t *testing.T, ref string) string {
	t.Helper()
	i := strings.LastIndex(ref, "/")
	if i < 0 || i == len(ref)-1 {
		t.Fatalf("malformed local $ref %q", ref)
	}
	return ref[i+1:]
}

// goldenJSON marshals a pinned example's YAML-decoded payload into the
// JSON bytes a Kafka consumer actually receives (structured-mode
// CloudEvents: the message value IS the JSON envelope).
func goldenJSON(t *testing.T, payload map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal golden payload: %v", err)
	}
	return raw
}

// documentedTypes returns the CloudEvents `type` const each message name
// on a channel pins, so an E1 test can assert the exact decoded type is
// documented (and an E4 test can cite what must be skipped instead).
func (d *pinnedAsyncAPI) documentedTypes(t *testing.T, topic string) map[string]string {
	t.Helper()
	documented := map[string]string{}
	for _, name := range d.subscribeMessageRefs(t, topic) {
		msg := d.message(t, name) // fails the test on a dangling $ref
		if c := d.payloadConsts(t, msg.Payload)["type"]; c != "" {
			documented[c] = name
		}
	}
	if len(documented) == 0 {
		t.Fatalf("pinned spec documents no message types on %q", topic)
	}
	return documented
}

// clonePayload deep-copies a golden example payload so a test can mutate
// it without touching the shared fixture.
func clonePayload(p map[string]any) map[string]any {
	return cloneMap(p).(map[string]any)
}

func cloneMap(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = cloneMap(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = cloneMap(val)
		}
		return out
	default:
		return v
	}
}
