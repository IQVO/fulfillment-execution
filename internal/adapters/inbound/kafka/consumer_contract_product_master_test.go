package kafka_test

// Event consumer contract tests (E1-E4) for the product-master edge: this
// repo keeps two local copies current off product-master's shared,
// fan-out topic warehouse.product-master.events (kafka.ProductMasterTopic,
// wired by cmd/execution/classification.go and packview wiring) — the
// classification copy (ADR-0039) from ProductClassified, and the pack
// view from ProductPackLevelDeclared / ProductPackLevelRetired. The
// contract both consumers validate against is the PINNED copy of
// product-master's apis/asyncapi.yaml (testdata/product-master.asyncapi.yaml,
// registered in contracts/pinned.json; the pinned-contracts CI job fails
// when the copy drifts from IQVO/product-master@develop).
//
// Golden fixtures come from the producer spec's OWN examples.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/packview"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

var contractLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// productMasterRoutedTypes are the CloudEvents types this repo's two
// ProductMasterTopic consumers decode (the rest of the channel's catalog
// is skipped — E4).
var productMasterRoutedTypes = []string{
	kafka.TypeProductClassified,
	kafka.TypeProductPackLevelDeclared,
	kafka.TypeProductPackLevelRetired,
}

func productMasterMessageNames() map[string]string {
	return map[string]string{
		kafka.TypeProductClassified:        "ProductClassified",
		kafka.TypeProductPackLevelDeclared: "ProductPackLevelDeclared",
		kafka.TypeProductPackLevelRetired:  "ProductPackLevelRetired",
	}
}

// E1: the topic both readers subscribe to (kafka.ProductMasterTopic,
// passed by cmd/execution's classification and pack-view wiring) is
// documented by the producer's pinned spec as a 'subscribe:' channel,
// and the message list on that channel documents every type this repo
// decodes. The spec pins no per-message `type` const: the channel's own
// description states the routing convention
// com.warehouse.wms.product-master.product.<EventName>, so the routed
// types are asserted by their EventName message entries.
func TestPinnedAsyncAPICoversSubscriptions_ProductMasterEvents(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedProductMasterSpec)

	documented := map[string]bool{}
	for _, name := range doc.subscribeMessageRefs(t, kafka.ProductMasterTopic) {
		documented[name] = true
	}

	for _, typ := range productMasterRoutedTypes {
		if !documented[evtName(typ)] {
			t.Errorf("pinned product-master spec does not document message %s for %s on the %s subscribe channel (documented: %v)",
				evtName(typ), typ, kafka.ProductMasterTopic, documented)
		}
	}
	// Shared fan-out topic: the five other documented product messages are
	// exactly what these consumers must ignore.
	if len(documented) < 6 {
		t.Logf("note: pinned spec documents %d message(s) on the shared topic", len(documented))
	}
}

// productMasterExamples returns the pinned spec's own examples of one
// routed message type.
func productMasterExamples(t *testing.T, typ string) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedProductMasterSpec)
	msg := doc.message(t, productMasterMessageNames()[typ])
	if len(msg.Examples) == 0 {
		t.Fatalf("pinned product-master spec documents %s without examples", typ)
	}
	return msg.Examples
}

// E2 (classification copy): every ProductClassified example decodes
// through the REAL decode → ApplyProductClassified path Handle drives and
// lands in the local copy with the wire's own facts; a replayed
// CloudEvents id applies nothing.
func TestGoldenEventsDecode_ProductClassified(t *testing.T) {
	for _, ex := range productMasterExamples(t, kafka.TypeProductClassified) {
		t.Run(ex.Name, func(t *testing.T) {
			store := productclassificationcopy.NewMemoryStore()
			c := kafka.NewProductClassifiedConsumerFromReader(nil, kafka.ProductMasterTopic,
				&usecases.ApplyProductClassified{Processed: memory.NewProcessedEventsRepo(), Copy: store}, contractLogger)
			raw := goldenJSON(t, ex.Payload)
			msg := kafkago.Message{Value: raw}

			if err := c.Handle(context.Background(), msg); err != nil {
				t.Fatalf("handle golden example: %v", err)
			}
			assertClassificationCopied(t, ex, store)

			if err := c.Handle(context.Background(), msg); err != nil {
				t.Fatalf("redelivery: %v", err)
			}
			assertClassificationCopied(t, ex, store) // unchanged by the replay
		})
	}
}

func assertClassificationCopied(t *testing.T, ex pinnedExample, store *productclassificationcopy.MemoryStore) {
	t.Helper()
	data := ex.Payload["data"].(map[string]any)
	sku, _ := data["sku"].(string)
	info, err := store.GetClassification(context.Background(), sku)
	if err != nil {
		t.Fatalf("classification copy for %s: %v", sku, err)
	}
	if !info.Known {
		t.Fatalf("golden ProductClassified for %s left the copy Unknown", sku)
	}
	hazmat := false
	for _, tag := range data["handling_tags"].([]any) {
		if tag == "Hazmat" {
			hazmat = true
		}
	}
	if info.Hazmat != hazmat {
		t.Errorf("copied Hazmat=%v, wire handling_tags say Hazmat=%v", info.Hazmat, hazmat)
	}
	if hazmat {
		if got, ok := data["dot_hazard_class"].(int); ok && info.DOTHazardClass != got {
			t.Errorf("copied DOT hazard class %d, wire says %d", info.DOTHazardClass, got)
		}
	}
}

// E2 (pack view): every ProductPackLevelDeclared example decodes through
// the REAL decode → ApplyPackLevel path and lands in the pack view; a
// ProductPackLevelRetired example then retires it by its wire
// effective_to.
func TestGoldenEventsDecode_ProductPackLevels(t *testing.T) {
	view := packview.NewMemoryStore()
	c := kafka.NewPackLevelConsumerFromReader(nil, kafka.ProductMasterTopic,
		&usecases.ApplyPackLevel{Processed: memory.NewProcessedEventsRepo(), View: view}, contractLogger)

	for _, ex := range productMasterExamples(t, kafka.TypeProductPackLevelDeclared) {
		t.Run(ex.Name, func(t *testing.T) { runPackLevelDeclaredExample(t, c, view, ex) })
	}

	for _, ex := range productMasterExamples(t, kafka.TypeProductPackLevelRetired) {
		t.Run(ex.Name, func(t *testing.T) { runPackLevelRetiredExample(t, c, view, ex) })
	}
}

// runPackLevelDeclaredExample applies a golden ProductPackLevelDeclared and
// asserts the pack view carries the wire's sku/level/units/version.
func runPackLevelDeclaredExample(t *testing.T, c *kafka.PackLevelConsumer, view *packview.MemoryStore, ex pinnedExample) {
	t.Helper()
	if err := c.Handle(context.Background(), kafkago.Message{Value: goldenJSON(t, ex.Payload)}); err != nil {
		t.Fatalf("handle declared example: %v", err)
	}
	data := ex.Payload["data"].(map[string]any)
	rec, ok, err := view.FindByGTIN(context.Background(), data["gtin"].(string))
	if err != nil || !ok {
		t.Fatalf("pack view lacks GTIN %v after the golden event: %v", data["gtin"], err)
	}
	if rec.SKU != data["sku"] || string(rec.Level) != data["level"].(string) {
		t.Errorf("pack record %+v does not carry the wire's sku/level", rec)
	}
	if want := data["units_per_pack"].(int); rec.UnitsPerPack != want {
		t.Errorf("pack record units_per_pack %d, wire says %d", rec.UnitsPerPack, want)
	}
	if want := data["version"].(int); rec.Version != int64(want) {
		t.Errorf("pack record version %d, wire says %d", rec.Version, want)
	}
}

// runPackLevelRetiredExample applies a golden ProductPackLevelRetired and
// asserts the record retired at the wire's own effective_to.
func runPackLevelRetiredExample(t *testing.T, c *kafka.PackLevelConsumer, view *packview.MemoryStore, ex pinnedExample) {
	t.Helper()
	if err := c.Handle(context.Background(), kafkago.Message{Value: goldenJSON(t, ex.Payload)}); err != nil {
		t.Fatalf("handle retired example: %v", err)
	}
	data := ex.Payload["data"].(map[string]any)
	rec, ok, err := view.FindByGTIN(context.Background(), data["gtin"].(string))
	if err != nil || !ok {
		t.Fatalf("pack view lacks the retired GTIN %v: %v", data["gtin"], err)
	}
	want, _ := time.Parse(time.RFC3339, data["effective_to"].(string))
	if rec.EffectiveTo == nil || !rec.EffectiveTo.Equal(want) {
		t.Errorf("retired record effective_to %v, wire says %v", rec.EffectiveTo, want)
	}
}

// assertProductMasterEnvelope holds one golden product-master example to
// the fleet's CloudEvents 1.0 bar with the producer's identity.
func assertProductMasterEnvelope(t *testing.T, typ string, ex pinnedExample, evt ce.Event) {
	t.Helper()
	if evt.SpecVersion() != cloudevents.SpecVersion {
		t.Errorf("%s/%s: specversion %q", typ, ex.Name, evt.SpecVersion())
	}
	if evt.ID() == "" || evt.Subject() == "" || evt.Source() == "" || evt.Time().IsZero() {
		t.Errorf("%s/%s: id/source/subject/time must all be present", typ, ex.Name)
	}
	if evt.Source() != "/warehouse/product-master" {
		t.Errorf("%s/%s: source %q, want /warehouse/product-master", typ, ex.Name, evt.Source())
	}
	if evt.DataMediaType() != cloudevents.DataContentType {
		t.Errorf("%s/%s: datacontenttype %q", typ, ex.Name, evt.DataMediaType())
	}
	if want := "urn:warehouse:product-master:events:" + productMasterMessageNames()[typ] + ":v1"; evt.DataSchema() != want {
		t.Errorf("%s/%s: dataschema %q, want %q", typ, ex.Name, evt.DataSchema(), want)
	}
}

// E3: every golden fixture of every routed type carries the
// fleet-mandatory CloudEvents 1.0 envelope, and the pinned spec's
// envelope schemas require those attributes.
func TestCloudEventsEnvelopeInvariants_ProductMaster(t *testing.T) {
	for _, typ := range productMasterRoutedTypes {
		for _, ex := range productMasterExamples(t, typ) {
			evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
			if err != nil {
				t.Fatalf("decode %s example %s: %v", typ, ex.Name, err)
			}
			assertProductMasterEnvelope(t, typ, ex, evt)
		}
	}

	doc := loadPinnedAsyncAPI(t, pinnedProductMasterSpec)
	if doc.DefaultContentType != "application/cloudevents+json" {
		t.Errorf("pinned spec defaultContentType %q, want application/cloudevents+json", doc.DefaultContentType)
	}
	for _, name := range productMasterMessageNames() {
		required := doc.envelopeRequired(t, doc.message(t, name).Payload)
		for _, attr := range []string{"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema", "data"} {
			if !required[attr] {
				t.Errorf("pinned spec's %s envelope does not require %q", name, attr)
			}
		}
	}
}

// E4: shared fan-out topic — unknown ADDITIVE data attributes must not
// fail the decode or the apply, an unknown event TYPE (the channel
// documents five this repo does not route on) is silently skipped, and a
// message that is not a CloudEvent at all is a deterministic decode
// error wrapping ErrNotCloudEvent (the poison-message signal both
// consumers WARN-log and commit past, never retry).
func TestUnknownAttributesTolerated_ProductMaster(t *testing.T) {
	golden := clonePayload(productMasterExamples(t, kafka.TypeProductClassified)[0].Payload)

	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	store := productclassificationcopy.NewMemoryStore()
	c := kafka.NewProductClassifiedConsumerFromReader(nil, kafka.ProductMasterTopic,
		&usecases.ApplyProductClassified{Processed: memory.NewProcessedEventsRepo(), Copy: store}, contractLogger)
	if err := c.Handle(context.Background(), kafkago.Message{Value: goldenJSON(t, tolerant)}); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if _, err := store.GetClassification(context.Background(), "SKU-1"); err != nil {
		t.Errorf("unknown data attributes changed the outcome: %v", err)
	}

	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wms.product-master.product.ProductRegistered"
	if err := c.Handle(context.Background(), kafkago.Message{Value: goldenJSON(t, otherType)}); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}

	_, err := cloudevents.Decode(goldenJSON(t, map[string]any{"sku": "SKU-legacy", "version": 1}))
	if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("retired flat envelope decoded as %v, want ErrNotCloudEvent", err)
	}
}
