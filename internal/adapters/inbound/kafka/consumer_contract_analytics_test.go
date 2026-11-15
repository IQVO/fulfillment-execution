package kafka_test

// Event consumer contract tests (E1-E4) for the analytics SELF-edge: this
// repo's own projector (cmd/fulfillment-projector) consumes this repo's
// own internal topic warehouse.fulfillment.analytics
// (outbound kafka.AnalyticsTopic, wired by cmd/fulfillment-projector's
// buildAnalyticsConsumer), projecting the five throughput-moving event
// types (analytics_consumer.go's switch) into the read model. This repo
// is both the producer and the consumer, so the contract is the SELF-pin
// of its own apis/asyncapi.yaml
// (testdata/fulfillment-execution.asyncapi.yaml, registered in
// contracts/pinned.json): the pin keeps the projector honest against the
// same spec its publisher documents for sibling consumers.
//
// Golden fixtures come from the spec's OWN examples — the projector
// decodes exactly what the AnalyticsPublisher documents it emits.

import (
	"context"
	"errors"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
)

// analyticsProjectedTypes maps each routed CloudEvents type to the
// projection call it must produce (fakeProjection's method label).
var analyticsProjectedTypes = map[string]string{
	kafka.TypeTaskClaimed:               "claimed",
	kafka.TypeTaskCompleted:             "completed",
	kafka.TypeLeaseExpired:              "lease",
	kafka.TypeWeightDiscrepancyDetected: "divert",
	kafka.TypePackageManifested:         "manifested",
}

// analyticsMessageNames maps routed types to their message names in the
// pinned own spec.
func analyticsMessageNames() map[string]string {
	return map[string]string{
		kafka.TypeTaskClaimed:               "AnalyticsTaskClaimed",
		kafka.TypeTaskCompleted:             "AnalyticsTaskCompleted",
		kafka.TypeLeaseExpired:              "AnalyticsLeaseExpired",
		kafka.TypeWeightDiscrepancyDetected: "AnalyticsWeightDiscrepancyDetected",
		kafka.TypePackageManifested:         "AnalyticsPackageManifested",
	}
}

// E1: the topic the projector's reader subscribes to
// (outboundkafka.AnalyticsTopic) is documented by the repo's own pinned
// spec as a 'subscribe:' channel — this repo's producer side — whose
// message list documents every type the projector routes on.
func TestPinnedAsyncAPICoversSubscriptions_OwnAnalytics(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedOwnAnalyticsSpec)

	documented := doc.documentedTypes(t, outboundkafka.AnalyticsTopic)

	for typ := range analyticsProjectedTypes {
		if _, ok := documented[typ]; !ok {
			t.Errorf("pinned own spec does not document %s on the %s subscribe channel (documented: %v)",
				typ, outboundkafka.AnalyticsTopic, documented)
		}
	}
	// The channel documents 13 analytics types; the 8 the projector does
	// not route on are acknowledged untouched (E4).
	if len(documented) < 8 {
		t.Logf("note: pinned spec documents %d type(s) on the analytics channel", len(documented))
	}
}

// ownAnalyticsExamples returns the pinned spec's own examples of one
// routed analytics message.
func ownAnalyticsExamples(t *testing.T, typ string) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedOwnAnalyticsSpec)
	msg := doc.message(t, analyticsMessageNames()[typ])
	if len(msg.Examples) == 0 {
		t.Fatalf("pinned own spec documents %s without examples", analyticsMessageNames()[typ])
	}
	return msg.Examples
}

// E2: every example of every routed type decodes through the REAL
// HandleMessage → ProjectionStore path and produces exactly one
// projection call carrying the wire's own facts; a replayed CloudEvents
// id projects nothing more.
func TestGoldenEventsDecode_OwnAnalytics(t *testing.T) {
	for typ, method := range analyticsProjectedTypes {
		for _, ex := range ownAnalyticsExamples(t, typ) {
			t.Run(ex.Name, func(t *testing.T) {
				proj := &fakeProjection{}
				c := &kafka.AnalyticsConsumer{Projection: proj, Processed: newFakeProcessed(), Logger: contractLogger}
				raw := goldenJSON(t, ex.Payload)

				if err := c.HandleMessage(context.Background(), raw); err != nil {
					t.Fatalf("handle golden example: %v", err)
				}
				assertAnalyticsProjected(t, ex, method, proj)

				if err := c.HandleMessage(context.Background(), raw); err != nil {
					t.Fatalf("redelivery: %v", err)
				}
				if len(proj.calls) != 1 {
					t.Errorf("replayed CloudEvents id %q projected %d more times", ex.Payload["id"], len(proj.calls)-1)
				}
			})
		}
	}
}

// assertAnalyticsProjected asserts exactly one projection call happened,
// routed to method, keyed by the wire CloudEvents id and carrying the
// wire's task/station facts (a weigh-check divert projects as SLAM — the
// consumer's own attribution, not a wire field).
func assertAnalyticsProjected(t *testing.T, ex pinnedExample, method string, proj *fakeProjection) {
	t.Helper()
	if len(proj.calls) != 1 {
		t.Fatalf("golden event produced %d projection calls, want 1", len(proj.calls))
	}
	got := proj.calls[0]
	if got.method != method {
		t.Fatalf("routed to %q, want %q", got.method, method)
	}
	if got.eventId != ex.Payload["id"] {
		t.Errorf("projected event id %q, wire CloudEvents id is %q", got.eventId, ex.Payload["id"])
	}
	data, _ := ex.Payload["data"].(map[string]any)
	str := func(key string) string {
		if v, ok := data[key].(string); ok {
			return v
		}
		return ""
	}
	if want := str("task_id"); got.taskId != want {
		t.Errorf("projected task id %q, wire says %q", got.taskId, want)
	}
	if method == "divert" {
		if got.taskType != "SLAM" {
			t.Errorf("weigh-check divert projected as %q, want SLAM", got.taskType)
		}
	} else if want := str("task_type"); got.taskType != want {
		t.Errorf("projected task type %q, wire says %q", got.taskType, want)
	}
	if want := str("station_id"); got.stationId != want {
		t.Errorf("projected station %q, wire says %q", got.stationId, want)
	}
	wantTime, _ := time.Parse(time.RFC3339, ex.Payload["time"].(string))
	if !got.at.Equal(wantTime) {
		t.Errorf("projected occurrence %v, CloudEvents time is %v", got.at, wantTime)
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope with the ANALYTICS dataschema
// (urn:warehouse:fulfillment-execution:analytics:<Event>:v1) and the
// structured-mode Kafka content-type header of this repo's MediaType
// constant — the header the projector's broker carries on every message.
func TestCloudEventsEnvelopeInvariants_OwnAnalytics(t *testing.T) {
	for typ := range analyticsProjectedTypes {
		for _, ex := range ownAnalyticsExamples(t, typ) {
			evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
			if err != nil {
				t.Fatalf("decode %s example %s: %v", typ, ex.Name, err)
			}
			assertAnalyticsEnvelope(t, typ, ex, evt)
		}
	}

	doc := loadPinnedAsyncAPI(t, pinnedOwnAnalyticsSpec)
	if doc.DefaultContentType != "application/cloudevents+json" {
		t.Errorf("pinned spec defaultContentType %q, want application/cloudevents+json", doc.DefaultContentType)
	}
}

// evtName strips the entity segment off a routed type's last component
// ("...task.TaskClaimed" -> "TaskClaimed").
func evtName(typ string) string {
	for i := len(typ) - 1; i >= 0; i-- {
		if typ[i] == '.' {
			return typ[i+1:]
		}
	}
	return typ
}

// assertAnalyticsEnvelope holds one golden analytics example to the
// fleet's CloudEvents 1.0 bar with this repo's own identity constants.
func assertAnalyticsEnvelope(t *testing.T, typ string, ex pinnedExample, evt ce.Event) {
	t.Helper()
	if evt.SpecVersion() != cloudevents.SpecVersion {
		t.Errorf("%s/%s: specversion %q", typ, ex.Name, evt.SpecVersion())
	}
	if evt.ID() == "" || evt.Subject() == "" || evt.Source() == "" || evt.Time().IsZero() {
		t.Errorf("%s/%s: id/source/subject/time must all be present", typ, ex.Name)
	}
	if evt.Source() != cloudevents.Source {
		t.Errorf("%s/%s: source %q, want %q", typ, ex.Name, evt.Source(), cloudevents.Source)
	}
	if evt.DataMediaType() != cloudevents.DataContentType {
		t.Errorf("%s/%s: datacontenttype %q", typ, ex.Name, evt.DataMediaType())
	}
	if want := "urn:warehouse:fulfillment-execution:analytics:" + evtName(typ) + ":v1"; evt.DataSchema() != want {
		t.Errorf("%s/%s: dataschema %q, want %q", typ, ex.Name, evt.DataSchema(), want)
	}
	if got := ex.Headers["content-type"]; got != cloudevents.MediaType {
		t.Errorf("%s/%s: Kafka content-type header %q, want this repo's constant %q", typ, ex.Name, got, cloudevents.MediaType)
	}
}

// E4: the projector is a Conformist on its own analytics stream — unknown
// ADDITIVE data attributes must not fail the decode or the projection,
// an analytics type outside the five routed ones (the channel documents
// AnalyticsItemPicked et al.) is acknowledged without touching the read
// model or the processed set, and a message that is not a CloudEvent at
// all is a deterministic decode error wrapping ErrNotCloudEvent.
func TestUnknownAttributesTolerated_OwnAnalytics(t *testing.T) {
	golden := clonePayload(ownAnalyticsExamples(t, kafka.TypeTaskClaimed)[0].Payload)

	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	proj := &fakeProjection{}
	c := &kafka.AnalyticsConsumer{Projection: proj, Processed: newFakeProcessed(), Logger: contractLogger}
	if err := c.HandleMessage(context.Background(), goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if len(proj.calls) != 1 {
		t.Errorf("unknown data attributes changed the outcome: %d calls", len(proj.calls))
	}

	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wes.fulfillment-execution.task.ItemPicked"
	if err := c.HandleMessage(context.Background(), goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unrouted analytics type must be skipped, not an error: %v", err)
	}
	if len(proj.calls) != 1 {
		t.Errorf("unrouted analytics type was projected (%d calls)", len(proj.calls))
	}

	_, err := cloudevents.Decode(goldenJSON(t, map[string]any{"task_id": "t-legacy", "task_type": "PICK"}))
	if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("retired flat envelope decoded as %v, want ErrNotCloudEvent", err)
	}
}
