package kafka_test

// Event consumer contract tests (E1-E4) for the task-interleaving edge:
// this repo keeps its local hint copy (ADR-0051) current off
// task-interleaving's topic warehouse.task-interleaving.events
// (kafka.TaskInterleavingTopic, wired by cmd/execution/interleaving.go),
// routing on com.warehouse.wes.task-interleaving.stationtrip.
// InterleavingHintIssued. The contract it validates against is the PINNED
// copy of task-interleaving's apis/asyncapi.yaml
// (testdata/task-interleaving.asyncapi.yaml, registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/task-interleaving@develop). The channel is
// single-purpose: the producer documents only InterleavingHintIssued
// here ("the hint table of fulfillment-execution is its consumer").
//
// Golden fixtures come from the producer spec's OWN examples.

import (
	"context"
	"errors"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/interleavinghintcopy"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// taskInterleavingGoldenExamples returns the pinned producer spec's own
// InterleavingHintIssued examples for the subscribed channel.
func taskInterleavingGoldenExamples(t *testing.T) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedTaskInterleavingSpec)
	msg := doc.message(t, "InterleavingHintIssued")
	if len(msg.Examples) == 0 {
		t.Fatal("pinned task-interleaving spec documents InterleavingHintIssued without examples")
	}
	return msg.Examples
}

func newHintConsumer(store *interleavinghintcopy.MemoryStore) *kafka.InterleavingHintConsumer {
	return kafka.NewInterleavingHintConsumerFromReader(nil, kafka.TaskInterleavingTopic,
		&usecases.ApplyInterleavingHint{Processed: memory.NewProcessedEventsRepo(), Hints: store}, contractLogger)
}

// E1: the topic the reader subscribes to (kafka.TaskInterleavingTopic)
// is documented by the producer's pinned spec as a 'subscribe:' channel
// whose message list carries the exact type this repo decodes.
func TestPinnedAsyncAPICoversSubscriptions_TaskInterleavingEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedTaskInterleavingSpec)

	documented := doc.documentedTypes(t, kafka.TaskInterleavingTopic)

	if _, ok := documented[kafka.TypeInterleavingHintIssued]; !ok {
		t.Errorf("pinned task-interleaving spec does not document %s on the %s subscribe channel (documented: %v)",
			kafka.TypeInterleavingHintIssued, kafka.TaskInterleavingTopic, documented)
	}
}

// E2: every InterleavingHintIssued example decodes through the REAL
// decode → ApplyInterleavingHint path Handle drives and lands in the
// local hint copy with the wire's own facts (per-station supersede, ranked
// candidate ids, expiry); a replayed CloudEvents id applies nothing.
func TestGoldenEventsDecode_InterleavingHintIssued(t *testing.T) {
	for _, ex := range taskInterleavingGoldenExamples(t) {
		t.Run(ex.Name, func(t *testing.T) {
			store := interleavinghintcopy.NewMemoryStore()
			c := newHintConsumer(store)
			raw := goldenJSON(t, ex.Payload)
			msg := kafkago.Message{Value: raw}

			if err := c.Handle(context.Background(), msg); err != nil {
				t.Fatalf("handle golden example: %v", err)
			}
			assertHintCopied(t, ex, store)

			if err := c.Handle(context.Background(), msg); err != nil {
				t.Fatalf("redelivery: %v", err)
			}
			assertHintCopied(t, ex, store) // unchanged by the replay
		})
	}
}

// assertHintCopied asserts the local copy holds exactly the wire's hint,
// read through the same lookup claimNext uses.
func assertHintCopied(t *testing.T, ex pinnedExample, store *interleavinghintcopy.MemoryStore) {
	t.Helper()
	data := ex.Payload["data"].(map[string]any)
	evtTime, _ := time.Parse(time.RFC3339, ex.Payload["time"].(string))
	hint, err := store.ActiveForStation(context.Background(), shared.StationId(data["station_id"].(string)), evtTime)
	if err != nil {
		t.Fatalf("hint copy lookup: %v", err)
	}
	if hint == nil {
		t.Fatal("golden InterleavingHintIssued left no active hint in the copy")
	}
	if hint.HintId != data["hint_id"] {
		t.Errorf("copied hint id %q, wire says %q", hint.HintId, data["hint_id"])
	}
	if want, _ := time.Parse(time.RFC3339, data["expires_at"].(string)); !hint.ExpiresAt.Equal(want) {
		t.Errorf("copied hint expires_at %v, wire says %v", hint.ExpiresAt, want)
	}
	want := data["candidates"].([]any)
	if len(hint.Candidates) != len(want) {
		t.Fatalf("copied %d candidates, wire carries %d", len(hint.Candidates), len(want))
	}
	for i, raw := range want {
		cand := raw.(map[string]any)
		if hint.Candidates[i].Rank != cand["rank"] || hint.Candidates[i].TaskId != cand["task_id"] {
			t.Errorf("candidate %d copied as {%d %q}, wire says {%v %q}",
				i, hint.Candidates[i].Rank, hint.Candidates[i].TaskId, cand["rank"], cand["task_id"])
		}
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope (source /warehouse/task-interleaving, dataschema
// urn:warehouse:task-interleaving:events:InterleavingHintIssued:v1), and
// the pinned spec's envelope schema requires those attributes.
func TestCloudEventsEnvelopeInvariants_InterleavingHintIssued(t *testing.T) {
	for _, ex := range taskInterleavingGoldenExamples(t) {
		evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
		if err != nil {
			t.Fatalf("decode golden example %s: %v", ex.Name, err)
		}
		if evt.SpecVersion() != cloudevents.SpecVersion {
			t.Errorf("%s: specversion %q", ex.Name, evt.SpecVersion())
		}
		if evt.ID() == "" || evt.Subject() == "" || evt.Source() == "" || evt.Time().IsZero() {
			t.Errorf("%s: id/source/subject/time must all be present", ex.Name)
		}
		if evt.Source() != "/warehouse/task-interleaving" {
			t.Errorf("%s: source %q, want /warehouse/task-interleaving", ex.Name, evt.Source())
		}
		if evt.DataMediaType() != cloudevents.DataContentType {
			t.Errorf("%s: datacontenttype %q", ex.Name, evt.DataMediaType())
		}
		if evt.DataSchema() != "urn:warehouse:task-interleaving:events:InterleavingHintIssued:v1" {
			t.Errorf("%s: dataschema %q", ex.Name, evt.DataSchema())
		}
	}

	doc := loadPinnedAsyncAPI(t, pinnedTaskInterleavingSpec)
	if doc.DefaultContentType != "application/cloudevents+json" {
		t.Errorf("pinned spec defaultContentType %q, want application/cloudevents+json", doc.DefaultContentType)
	}
	required := doc.envelopeRequired(t, doc.message(t, "InterleavingHintIssued").Payload)
	for _, attr := range []string{"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema", "data"} {
		if !required[attr] {
			t.Errorf("pinned spec's InterleavingHintIssued envelope does not require %q", attr)
		}
	}
}

// E4: hints are advisory (fail open) — unknown ADDITIVE data attributes
// must not fail the decode or the apply, an unknown event TYPE is
// silently skipped, and a message that is not a CloudEvent at all is a
// deterministic decode error wrapping ErrNotCloudEvent (the poison
// signal the consumer WARN-logs and commits past, never retries).
func TestUnknownAttributesTolerated_InterleavingHintIssued(t *testing.T) {
	golden := clonePayload(taskInterleavingGoldenExamples(t)[0].Payload)

	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	store := interleavinghintcopy.NewMemoryStore()
	c := newHintConsumer(store)
	if err := c.Handle(context.Background(), kafkago.Message{Value: goldenJSON(t, tolerant)}); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	evtTime, _ := time.Parse(time.RFC3339, golden["time"].(string))
	if hint, err := store.ActiveForStation(context.Background(), "STN-07", evtTime); err != nil || hint == nil {
		t.Errorf("unknown data attributes changed the outcome: hint=%v err=%v", hint, err)
	}

	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wes.task-interleaving.stationtrip.PolicyDeclared"
	if err := c.Handle(context.Background(), kafkago.Message{Value: goldenJSON(t, otherType)}); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}

	_, err := cloudevents.Decode(goldenJSON(t, map[string]any{"hint_id": "h-legacy", "station_id": "STN-07"}))
	if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("retired flat envelope decoded as %v, want ErrNotCloudEvent", err)
	}
}
