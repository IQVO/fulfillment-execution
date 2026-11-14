package kafkacatalog

// Event consumer contract tests (E1-E4) for the process-path catalogue
// edge: this repo replays process-path-management's topic
// warehouse.process-path-management.events (kafkacatalog.Topic, wired by
// cmd/execution/wire_catalogue.go when PATH_CATALOGUE_SOURCE=kafka) into
// its local PathCatalogue, routing on ProcessPathCreated /
// ProcessPathUpdated / ProcessPathDeactivated. The contract it validates
// against is the PINNED copy of process-path-management's
// apis/asyncapi.yaml (testdata/process-path-management.asyncapi.yaml,
// registered in contracts/pinned.json; the pinned-contracts CI job fails
// when the copy drifts from IQVO/process-path-management@develop).
//
// PERSPECTIVE NOTE: unlike most fleet specs, process-path-management's
// AsyncAPI documents its OWN topic under 'publish:' (app-perspective),
// not under the fleet's usual channel-perspective 'subscribe:' — E1
// reads the publish: operation deliberately.

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"gopkg.in/yaml.v3"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/fulfillment-execution/internal/domain/pathcatalog"
)

const pinnedProcessPathSpec = "testdata/process-path-management.asyncapi.yaml"

// The minimal slice of the pinned spec these tests read.
// ppExample is one named example of a pinned message.
type ppExample struct {
	Name    string         `yaml:"name"`
	Payload map[string]any `yaml:"payload"`
}

type ppSpec struct {
	Channels struct {
		ProcessPathEvents struct {
			Publish struct {
				OperationID string `yaml:"operationId"`
				Message     struct {
					OneOf []struct {
						Ref string `yaml:"$ref"`
					} `yaml:"oneOf"`
				} `yaml:"message"`
			} `yaml:"publish"`
		} `yaml:"warehouse.process-path-management.events"`
	} `yaml:"channels"`
	Components struct {
		Messages map[string]struct {
			Name     string      `yaml:"name"`
			Payload  any         `yaml:"payload"`
			Examples []ppExample `yaml:"examples"`
		} `yaml:"messages"`
	} `yaml:"components"`
}

func loadProcessPathSpec(t *testing.T) *ppSpec {
	t.Helper()
	raw, err := os.ReadFile(pinnedProcessPathSpec)
	if err != nil {
		t.Fatalf("read pinned spec (copy it from the producer, never hand-write it): %v", err)
	}
	var s ppSpec
	if err := yaml.Unmarshal(raw, &s); err != nil {
		t.Fatalf("parse pinned spec: %v", err)
	}
	return &s
}

// channelMessageNames resolves the message names the producer's events
// channel documents (under 'publish:' — see the perspective note above).
func (s *ppSpec) channelMessageNames(t *testing.T) []string {
	t.Helper()
	refs := s.Channels.ProcessPathEvents.Publish.Message.OneOf
	if len(refs) == 0 {
		t.Fatal("pinned spec's publish operation on the events channel lists no messages")
	}
	names := make([]string, 0, len(refs))
	for _, r := range refs {
		i := strings.LastIndex(r.Ref, "/")
		if i < 0 || i == len(r.Ref)-1 {
			t.Fatalf("malformed local $ref %q", r.Ref)
		}
		names = append(names, r.Ref[i+1:])
	}
	return names
}

func (s *ppSpec) message(t *testing.T, name string) (any, []ppExample) {
	t.Helper()
	msg, ok := s.Components.Messages[name]
	if !ok {
		t.Fatalf("pinned spec references undefined components/messages/%s", name)
	}
	return msg.Payload, msg.Examples
}

func newCatalogueConsumer() *Consumer {
	return newTestConsumer(&fakeReader{}, nil)
}

// E1: the topic the reader subscribes to (kafkacatalog.Topic) is
// documented by the producer's pinned spec — under 'publish:', this
// producer's app-perspective — and the channel's message list documents
// every type the decoder routes on. CPTScheduleChanged is documented
// here too: it is inside the channel's catalog but outside this
// consumer's contract (E4's documented skip).
func TestPinnedAsyncAPICoversSubscriptions_ProcessPathEvents(t *testing.T) {
	s := loadProcessPathSpec(t)

	if s.Channels.ProcessPathEvents.Publish.OperationID == "" {
		t.Fatal("pinned spec has no publish operation on warehouse.process-path-management.events")
	}
	documented := map[string]bool{}
	for _, name := range s.channelMessageNames(t) {
		documented[name] = true
	}
	for _, name := range []string{"ProcessPathCreated", "ProcessPathUpdated", "ProcessPathDeactivated"} {
		if !documented[name] {
			t.Errorf("pinned spec does not document %s on the events channel (documented: %v)", name, documented)
		}
	}
	if !documented["CPTScheduleChanged"] {
		t.Logf("note: spec documents %d messages; CPTScheduleChanged not among them", len(documented))
	}
}

// E2: every example of every routed type decodes through the REAL
// handle → applyUpsert/applyDeactivated path and lands in the local
// catalogue with the wire's own facts.
func TestGoldenEventsDecode_ProcessPathCatalogue(t *testing.T) {
	s := loadProcessPathSpec(t)

	for _, name := range []string{"ProcessPathCreated", "ProcessPathUpdated"} {
		_, examples := s.message(t, name)
		if len(examples) == 0 {
			t.Fatalf("pinned spec documents %s without examples", name)
		}
		for _, ex := range examples {
			t.Run(name+"/"+ex.Name, func(t *testing.T) {
				c := newCatalogueConsumer()
				if err := c.handle(kafkago.Message{Value: ppGoldenJSON(t, ex.Payload)}); err != nil {
					t.Fatalf("handle golden example: %v", err)
				}
				assertPathUpserted(t, ex.Payload, c)
			})
		}
	}

	_, examples := s.message(t, "ProcessPathDeactivated")
	if len(examples) == 0 {
		t.Fatal("pinned spec documents ProcessPathDeactivated without examples")
	}
	for _, ex := range examples {
		t.Run("ProcessPathDeactivated/"+ex.Name, func(t *testing.T) {
			c := newCatalogueConsumer()
			if err := c.handle(kafkago.Message{Value: ppGoldenJSON(t, ex.Payload)}); err != nil {
				t.Fatalf("handle golden example: %v", err)
			}
			data := ex.Payload["data"].(map[string]any)
			if _, err := c.Lookup(data["path_id"].(string)); !errors.Is(err, pathcatalog.ErrUnknownPath) {
				t.Errorf("deactivated path %v still resolvable: %v", data["path_id"], err)
			}
		})
	}
}

func assertPathUpserted(t *testing.T, payload map[string]any, c *Consumer) {
	t.Helper()
	data := payload["data"].(map[string]any)
	// Lookup is PREFIX-based (the catalogue resolves work-unit-shaped ids by
	// their match_prefix), so probe with a synthetic id under the wire's own
	// prefix — never the bare path id.
	prefix, _ := data["match_prefix"].(string)
	def, err := c.Lookup(prefix + "-probe")
	if err != nil {
		t.Fatalf("upserted path %v not resolvable via its prefix %q: %v", data["path_id"], prefix, err)
	}
	if def.MatchPrefix != data["match_prefix"] {
		t.Errorf("catalogued match_prefix %q, wire says %q", def.MatchPrefix, data["match_prefix"])
	}
	if want, ok := data["required_capabilities"]; ok {
		if len(def.RequiredCapabilities) != len(want.([]any)) {
			t.Errorf("catalogued %d required capabilities, wire carries %d",
				len(def.RequiredCapabilities), len(want.([]any)))
		}
	}
}

// E3: every golden fixture of every routed type carries the
// fleet-mandatory CloudEvents 1.0 envelope — source
// /warehouse/process-path-management, dataschema
// urn:warehouse:process-path-management:events:<Event>:v1.
func TestCloudEventsEnvelopeInvariants_ProcessPath(t *testing.T) {
	s := loadProcessPathSpec(t)
	for _, name := range []string{"ProcessPathCreated", "ProcessPathUpdated", "ProcessPathDeactivated"} {
		_, examples := s.message(t, name)
		for _, ex := range examples {
			evt, err := cloudevents.Decode(ppGoldenJSON(t, ex.Payload))
			if err != nil {
				t.Fatalf("decode %s example %s: %v", name, ex.Name, err)
			}
			assertProcessPathEnvelope(t, name, ex, evt)
		}
	}
}

// assertProcessPathEnvelope holds one golden process-path example to the
// fleet's CloudEvents 1.0 bar with the producer's identity.
func assertProcessPathEnvelope(t *testing.T, name string, ex ppExample, evt ce.Event) {
	t.Helper()
	if evt.SpecVersion() != cloudevents.SpecVersion {
		t.Errorf("%s/%s: specversion %q", name, ex.Name, evt.SpecVersion())
	}
	if evt.ID() == "" || evt.Subject() == "" || evt.Source() == "" || evt.Time().IsZero() {
		t.Errorf("%s/%s: id/source/subject/time must all be present", name, ex.Name)
	}
	if evt.Source() != "/warehouse/process-path-management" {
		t.Errorf("%s/%s: source %q, want /warehouse/process-path-management", name, ex.Name, evt.Source())
	}
	if evt.DataMediaType() != cloudevents.DataContentType {
		t.Errorf("%s/%s: datacontenttype %q", name, ex.Name, evt.DataMediaType())
	}
	if want := "urn:warehouse:process-path-management:events:" + name + ":v1"; evt.DataSchema() != want {
		t.Errorf("%s/%s: dataschema %q, want %q", name, ex.Name, evt.DataSchema(), want)
	}
}

// E4: the catalogue is a full-replay Conformist — unknown ADDITIVE data
// attributes must not fail the decode or the upsert, CPTScheduleChanged
// (documented on the channel, outside this consumer's contract) is
// skipped without touching the cache, and a message that is not a
// CloudEvent at all is a deterministic decode error wrapping
// ErrNotCloudEvent (the poison signal Run surfaces).
func TestUnknownAttributesTolerated_ProcessPath(t *testing.T) {
	s := loadProcessPathSpec(t)
	_, examples := s.message(t, "ProcessPathUpdated")
	if len(examples) == 0 {
		t.Fatal("pinned spec documents ProcessPathUpdated without examples")
	}
	golden := clonePP(examples[0].Payload)

	tolerant := clonePP(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	c := newCatalogueConsumer()
	if err := c.handle(kafkago.Message{Value: ppGoldenJSON(t, tolerant)}); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	assertPathUpserted(t, tolerant, c)

	cpt := clonePP(golden)
	cpt["type"] = "com.warehouse.wes.process-path-management.schedule.CPTScheduleChanged"
	if err := c.handle(kafkago.Message{Value: ppGoldenJSON(t, cpt)}); err != nil {
		t.Fatalf("documented-but-unrouted CPTScheduleChanged must be skipped, not an error: %v", err)
	}
	if ids := c.Ids(); len(ids) != 1 {
		t.Errorf("CPTScheduleChanged changed the catalogue: %v", ids)
	}

	if err := c.handle(kafkago.Message{Value: ppGoldenJSON(t, map[string]any{"path_id": "LEGACY"})}); err == nil {
		t.Fatal("retired flat envelope decoded without error, want ErrNotCloudEvent")
	} else if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("retired flat envelope decoded as %v, want ErrNotCloudEvent", err)
	}
}

// ppGoldenJSON marshals a pinned example's YAML-decoded payload into the
// JSON bytes the reader delivers.
func ppGoldenJSON(t *testing.T, payload map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal golden payload: %v", err)
	}
	return raw
}

func clonePP(p map[string]any) map[string]any {
	out := make(map[string]any, len(p))
	for k, v := range p {
		out[k] = v
	}
	return out
}
