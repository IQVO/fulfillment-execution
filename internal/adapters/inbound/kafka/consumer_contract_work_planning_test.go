package kafka_test

// Event consumer contract tests (E1-E4) for the WorkReleased edge: this
// repo's Consumer subscribes to wes-work-planning's shared, fan-out topic
// warehouse.work-planning.events (cmd/execution/main.go's workReleasedTopic
// constant, wired into the reader by wire_handlers.go's
// wireWorkReleasedConsumer) and acts only on
// com.warehouse.wes.work-planning.workunit.WorkReleased
// (kafka.TypeWorkReleased — ADR-0032). The contract it validates against
// is the PINNED copy of wes-work-planning's apis/asyncapi.yaml
// (testdata/wes-work-planning.asyncapi.yaml, registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/wes-work-planning@develop).
//
// Golden fixtures come from the producer spec's OWN examples — every
// WorkReleased example pinned here is a real wire document
// wes-work-planning documents for this channel (pick, order-line, hazmat,
// relocate and replenish releases).

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
)

// workPlanningTopic mirrors cmd/execution/main.go's workReleasedTopic
// constant (the value wireWorkReleasedConsumer wires into the reader).
const workPlanningTopic = "warehouse.work-planning.events"

// workPlanningGoldenExamples returns the pinned producer spec's own
// WorkReleased examples for the subscribed channel.
func workPlanningGoldenExamples(t *testing.T) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedWorkPlanningSpec)
	msg := doc.message(t, "WorkReleased")
	if len(msg.Examples) == 0 {
		t.Fatal("pinned wes-work-planning spec documents WorkReleased without examples")
	}
	return msg.Examples
}

// E1: the topic this repo's reader actually subscribes to is documented
// by the producer's pinned spec as a 'subscribe:' channel — fleet
// channel-perspective: a producer's own topics live under 'subscribe:' —
// AND the message list on that channel documents the exact CloudEvents
// type this repo's decoder routes on.
func TestPinnedAsyncAPICoversSubscriptions_WorkPlanningEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedWorkPlanningSpec)

	documented := doc.documentedTypes(t, workPlanningTopic)

	if _, ok := documented[kafka.TypeWorkReleased]; !ok {
		t.Errorf("pinned wes-work-planning spec does not document %s on the %s subscribe channel (documented: %v)",
			kafka.TypeWorkReleased, workPlanningTopic, documented)
	}
	// Unknown types are skipped, not errors — every OTHER documented type
	// on this shared fan-out topic (WorkReleasedAnalytics rides a
	// different channel; sibling types ride this one) must be ignored.
	if len(documented) < 2 {
		t.Logf("note: pinned spec documents %d type(s) on the shared topic", len(documented))
	}
}

// E2: every WorkReleased example the producer's pinned spec documents on
// the subscribed channel decodes through the REAL decodeWorkReleased →
// ApplyWorkReleased path HandleMessage drives: exactly one Task per
// event, keyed by the fixture's id sequence, carrying the wire's CPT and
// work unit id — and a replayed CloudEvents id (the ProcessedEvents
// claim key) creates nothing more.
func TestGoldenEventsDecode_WorkReleased(t *testing.T) {
	for _, ex := range workPlanningGoldenExamples(t) {
		t.Run(ex.Name, func(t *testing.T) {
			c, tasks := newConsumer(t)
			raw := goldenJSON(t, ex.Payload)

			if err := c.HandleMessage(context.Background(), raw); err != nil {
				t.Fatalf("handle golden example: %v", err)
			}
			assertTaskCarriesWireFacts(t, ex, tasks)

			// Dedupe contract: the CloudEvents id is the claim key — an
			// at-least-once redelivery of the same id creates nothing
			// (the fixture's id sequence would have minted "t2").
			if err := c.HandleMessage(context.Background(), raw); err != nil {
				t.Fatalf("redelivery: %v", err)
			}
			// NOTE: memory.TaskRepo.FindById answers (nil, nil) for an absent
			// id, so existence must be asserted on the returned task, never on
			// the error.
			if got, _ := tasks.FindById(context.Background(), "t2"); got != nil {
				t.Errorf("replayed CloudEvents id %q created a second task", ex.Payload["id"])
			}
		})
	}
}

// assertTaskCarriesWireFacts asserts the fixture-minted task "t1" exists
// and carries the wire's own CPT and work unit id (ApplyWorkReleased
// stamps shared.OrderRef(req.WorkUnitId) — the release's `ref` is the
// producer's correlation field, not the task's order ref), plus the
// order line when the example states one.
func assertTaskCarriesWireFacts(t *testing.T, ex pinnedExample, tasks *memory.TaskRepo) {
	t.Helper()
	created, _ := tasks.FindById(context.Background(), "t1")
	if created == nil {
		t.Fatal("golden event created no task")
	}
	data := ex.Payload["data"].(map[string]any)
	if want, _ := data["cpt"].(string); created.CPT().Time().Format(time.RFC3339) != want {
		t.Errorf("task CPT %v, wire says %q", created.CPT().Time(), want)
	}
	if want, _ := data["work_unit_id"].(string); string(created.OrderRef()) != want {
		t.Errorf("task order ref %q, wire work_unit_id is %q", created.OrderRef(), want)
	}
	if want, ok := data["line_no"]; ok {
		if lineNo, ok := want.(int); ok && created.SourceLineNo() != lineNo {
			t.Errorf("task source line %d, wire says %v", created.SourceLineNo(), want)
		}
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope — specversion 1.0, id, source, type, subject, time,
// datacontenttype application/json, dataschema
// urn:warehouse:wes-work-planning:events:WorkReleased:v1 — and the
// pinned spec's envelope schema requires those attributes.
func TestCloudEventsEnvelopeInvariants_WorkReleased(t *testing.T) {
	for _, ex := range workPlanningGoldenExamples(t) {
		raw := goldenJSON(t, ex.Payload)
		evt, err := cloudevents.Decode(raw)
		if err != nil {
			t.Fatalf("decode golden example %s: %v", ex.Name, err)
		}
		if evt.SpecVersion() != cloudevents.SpecVersion {
			t.Errorf("%s: specversion %q, want %q", ex.Name, evt.SpecVersion(), cloudevents.SpecVersion)
		}
		if evt.ID() == "" || evt.Subject() == "" || evt.Source() == "" || evt.Time().IsZero() {
			t.Errorf("%s: id/source/subject/time must all be present (got %q/%q/%q/%v)",
				ex.Name, evt.ID(), evt.Source(), evt.Subject(), evt.Time())
		}
		if evt.Source() != "/warehouse/wes-work-planning" {
			t.Errorf("%s: source %q, want /warehouse/wes-work-planning", ex.Name, evt.Source())
		}
		if evt.DataMediaType() != cloudevents.DataContentType {
			t.Errorf("%s: datacontenttype %q, want %q", ex.Name, evt.DataMediaType(), cloudevents.DataContentType)
		}
		if evt.DataSchema() != "urn:warehouse:wes-work-planning:events:WorkReleased:v1" {
			t.Errorf("%s: dataschema %q", ex.Name, evt.DataSchema())
		}
	}

	doc := loadPinnedAsyncAPI(t, pinnedWorkPlanningSpec)
	if doc.DefaultContentType != "application/cloudevents+json" {
		t.Errorf("pinned spec defaultContentType %q, want application/cloudevents+json", doc.DefaultContentType)
	}
	required := doc.envelopeRequired(t, doc.message(t, "WorkReleased").Payload)
	for _, attr := range []string{"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema", "data"} {
		if !required[attr] {
			t.Errorf("pinned spec's WorkReleased envelope does not require %q (required: %v)", attr, required)
		}
	}
}

// E4: this consumer is a Conformist on a shared, fan-out topic — unknown
// ADDITIVE attributes in data must not fail the decode or the handling
// (the producer's own ADR-0047/0050 relocation/replenishment blocks are
// the live example), an unknown event TYPE is silently skipped (not an
// error, no task, no DLQ), and a message that is not a CloudEvent at all
// is a deterministic decode error wrapping ErrNotCloudEvent — the exact
// signal Run dead-letters once and never retries (sendToDeadLetter,
// maxHandlerAttempts).
func TestUnknownAttributesTolerated_WorkReleased(t *testing.T) {
	golden := clonePayload(workPlanningGoldenExamples(t)[0].Payload)

	// Unknown additive attributes in data — scalar and nested.
	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	c, tasks := newConsumer(t)
	if err := c.HandleMessage(context.Background(), goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if got := totalPending(t, tasks); got != 1 {
		t.Errorf("unknown data attributes changed the outcome: %d tasks", got)
	}

	// Unknown event TYPE on the shared topic: silently skipped.
	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wes.work-planning.workunit.SomethingElse"
	if err := c.HandleMessage(context.Background(), goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if got := totalPending(t, tasks); got != 1 {
		t.Errorf("unknown event type was acted on (%d tasks)", got)
	}

	// Not a CloudEvent (retired flat envelope): deterministic decode error
	// wrapping ErrNotCloudEvent — the DLQ trigger.
	_, err := cloudevents.Decode(goldenJSON(t, map[string]any{
		"path_id": "PICK", "work_unit_id": "wu-legacy", "cpt": "2026-08-21T23:00:00Z",
	}))
	if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("retired flat envelope decoded as %v, want ErrNotCloudEvent (the DLQ trigger)", err)
	}
}
