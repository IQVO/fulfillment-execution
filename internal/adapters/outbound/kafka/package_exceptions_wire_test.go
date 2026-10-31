package kafka_test

import (
	"context"
	"encoding/json"
	"testing"

	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

// PackageDiverted and WeightDiscrepancyDetected are promoted from the
// analytics topic to the integration topic for exception-management
// (ADR 0053): same `type`, same `subject`/key (package id), same `data`,
// integration `dataschema`. Golden exact-JSON tests mirror the shape of
// TestGolden_Integration_TaskCPTMissed, and a byte-identity test pins the
// "same data" promise against the analytics encoder.

func TestGolden_Integration_PackageDiverted(t *testing.T) {
	w := &fakeWriter{}
	p := outboundkafka.NewPublisherWithWriter(w, nil, nil, goldenID1)
	if err := p.Publish(context.Background(), shared.NewPackageDiverted("p1", goldenAt)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(w.msgs))
	}
	assertGolden(t, w.msgs[0], "p1", `{
		"specversion":"1.0",
		"id":"`+goldenID+`",
		"source":"/warehouse/fulfillment-execution",
		"type":"com.warehouse.wes.fulfillment-execution.package.PackageDiverted",
		"subject":"p1",
		"time":"2026-09-30T15:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:fulfillment-execution:events:PackageDiverted:v1",
		"data":{"package_id":"p1"}
	}`)
}

func TestGolden_Integration_WeightDiscrepancyDetected(t *testing.T) {
	w := &fakeWriter{}
	p := outboundkafka.NewPublisherWithWriter(w, nil, nil, goldenID1)
	if err := p.Publish(context.Background(), shared.NewWeightDiscrepancyDetected("p1", 1000, 1200, goldenAt)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(w.msgs))
	}
	assertGolden(t, w.msgs[0], "p1", `{
		"specversion":"1.0",
		"id":"`+goldenID+`",
		"source":"/warehouse/fulfillment-execution",
		"type":"com.warehouse.wes.fulfillment-execution.package.WeightDiscrepancyDetected",
		"subject":"p1",
		"time":"2026-09-30T15:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:fulfillment-execution:events:WeightDiscrepancyDetected:v1",
		"data":{"package_id":"p1","expected_g":1000,"actual_g":1200}
	}`)
}

// rawData returns the `data` member of a structured-mode CloudEvent
// exactly as it sits on the wire (no re-marshalling), so two encoders can
// be compared byte for byte.
func rawData(t *testing.T, value []byte) []byte {
	t.Helper()
	var env struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(value, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v (%s)", err, value)
	}
	if len(env.Data) == 0 {
		t.Fatalf("envelope has no data member: %s", value)
	}
	return env.Data
}

// promotedCase is one event checked on both topics.
type promotedCase struct {
	name  string
	event shared.DomainEvent
	typ   string
}

// checkPromotedPair asserts one event's integration and analytics encodings
// agree on topic pair, type, key and data bytes.
func checkPromotedPair(t *testing.T, tc promotedCase) {
	t.Helper()
	ctx := context.Background()
	integration := outboundkafka.NewPublisherWithWriter(nil, nil, nil, goldenID1)
	analytics := outboundkafka.NewAnalyticsPublisherWithWriter(nil, nil, goldenID1)

	ie, err := integration.Encode(ctx, tc.event)
	if err != nil {
		t.Fatalf("integration Encode: %v", err)
	}
	ae, err := analytics.Encode(ctx, tc.event)
	if err != nil {
		t.Fatalf("analytics Encode: %v", err)
	}
	if len(ie) != 1 || len(ae) != 1 {
		t.Fatalf("expected 1 encoded message per encoder, got integration=%d analytics=%d", len(ie), len(ae))
	}
	if ie[0].Topic != outboundkafka.Topic || ae[0].Topic != outboundkafka.AnalyticsTopic {
		t.Fatalf("topics = %q / %q, want %q / %q", ie[0].Topic, ae[0].Topic, outboundkafka.Topic, outboundkafka.AnalyticsTopic)
	}
	if ie[0].EventType != tc.typ || ae[0].EventType != tc.typ {
		t.Fatalf("types = %q / %q, want %q on both", ie[0].EventType, ae[0].EventType, tc.typ)
	}
	if string(ie[0].Key) != string(ae[0].Key) {
		t.Fatalf("keys differ: integration %q, analytics %q", ie[0].Key, ae[0].Key)
	}
	id, ad := rawData(t, ie[0].Value), rawData(t, ae[0].Value)
	if string(id) != string(ad) {
		t.Fatalf("data bytes differ\n integration: %s\n   analytics: %s", id, ad)
	}
}

// The promoted events carry the SAME data bytes on both topics: the
// integration encoder (typed structs) and the analytics encoder (maps) must
// agree byte for byte, not just structurally — a consumer may hash or
// store the payload verbatim.
func TestPromotedPackageEvents_DataBytesIdenticalOnBothTopics(t *testing.T) {
	cases := []promotedCase{
		{"PackageDiverted", shared.NewPackageDiverted("p1", goldenAt), "com.warehouse.wes.fulfillment-execution.package.PackageDiverted"},
		{"WeightDiscrepancyDetected", shared.NewWeightDiscrepancyDetected("p1", 1000, 1200, goldenAt), "com.warehouse.wes.fulfillment-execution.package.WeightDiscrepancyDetected"},
		{"WeightDiscrepancyDetected fractional grams", shared.NewWeightDiscrepancyDetected("pkg-77", 1234.5, 987.25, goldenAt), "com.warehouse.wes.fulfillment-execution.package.WeightDiscrepancyDetected"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			checkPromotedPair(t, tc)
		})
	}
}

// Publish writes exactly the two promoted messages for a divert's event
// pair (the use case publishes both in one call) and skips nothing else
// on that pair — the analytics-only events stay off the integration topic.
func TestPublish_DivertPairReachesIntegrationTopic_LabelAppliedDoesNot(t *testing.T) {
	w := &fakeWriter{}
	p := outboundkafka.NewPublisherWithWriter(w, nil, nil, goldenID1)
	err := p.Publish(context.Background(),
		shared.NewWeightDiscrepancyDetected("p1", 1000, 1200, goldenAt),
		shared.NewPackageDiverted("p1", goldenAt),
		shared.NewLabelApplied("p1", goldenAt),
		shared.NewPackageSealed("p1", goldenAt),
	)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 2 {
		t.Fatalf("expected exactly the 2 promoted messages, got %d", len(w.msgs))
	}
	e0, _ := decodeCE[outboundkafka.WeightDiscrepancyDetectedData](t, w.msgs[0].Value)
	e1, d1 := decodeCE[outboundkafka.PackageDivertedData](t, w.msgs[1].Value)
	if e0.Type() != "com.warehouse.wes.fulfillment-execution.package.WeightDiscrepancyDetected" {
		t.Errorf("first type = %q", e0.Type())
	}
	if e1.Type() != "com.warehouse.wes.fulfillment-execution.package.PackageDiverted" {
		t.Errorf("second type = %q", e1.Type())
	}
	if d1.PackageId != "p1" || string(w.msgs[1].Key) != "p1" {
		t.Errorf("PackageDiverted data/key = %+v / %q, want p1", d1, w.msgs[1].Key)
	}
}
