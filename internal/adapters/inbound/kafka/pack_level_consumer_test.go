package kafka_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/packview"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

func packLevelCE(id, typ, sku string, data map[string]any) []byte {
	return cloudEvent(id, "/warehouse/product-master", typ, sku,
		time.Date(2026, 10, 6, 21, 0, 0, 0, time.UTC), "urn:warehouse:product-master:events:"+typ+":v1", data)
}

func declaredCE(id string, over map[string]any) []byte {
	data := map[string]any{
		"sku": "SKU-1", "level": "CASE", "gtin": "24006381333935", "child_gtin": "04006381333931", "qty_of_child": 12,
		"units_per_pack": 12, "is_orderable": true, "is_despatch_unit": true,
		"effective_from": "2026-10-06T20:00:00Z", "version": 3,
	}
	for k, v := range over {
		if v == nil {
			delete(data, k)
			continue
		}
		data[k] = v
	}
	return packLevelCE(id, kafka.TypeProductPackLevelDeclared, "SKU-1", data)
}

func newPackLevelConsumer(r *fakeFetchReader, view *packview.MemoryStore) *kafka.PackLevelConsumer {
	apply := &usecases.ApplyPackLevel{Processed: memory.NewProcessedEventsRepo(), View: view}
	return kafka.NewPackLevelConsumerFromReader(r, kafka.ProductMasterTopic, apply, slog.New(slog.NewTextHandler(io.Discard, nil))).
		WithRetryIntervals(time.Millisecond, 5*time.Millisecond)
}

func runPackLevelUntilCommitted(t *testing.T, c *kafka.PackLevelConsumer, r *fakeFetchReader, want int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for len(r.commits()) < want {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("committed %v, want %d commits", r.commits(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned %v", err)
	}
}

func TestPackLevelConsumer_DeclaredThenRetiredThenSupersededGTIN(t *testing.T) {
	view := packview.NewMemoryStore()
	r := &fakeFetchReader{msgs: []kafkago.Message{
		msgAt(0, declaredCE("ce-1", nil)),
		msgAt(1, packLevelCE("ce-2", kafka.TypeProductPackLevelRetired, "SKU-1", map[string]any{
			"sku": "SKU-1", "level": "CASE", "gtin": "24006381333935", "effective_to": "2026-10-06T21:01:00Z", "reason": "superseded", "version": 4,
		})),
		// the next GTIN at the level arrives in 13-digit form and is normalised to 14.
		msgAt(2, declaredCE("ce-3", map[string]any{"gtin": "4006381333948", "units_per_pack": 24, "qty_of_child": 24, "child_gtin": nil, "version": 5})),
	}}
	runPackLevelUntilCommitted(t, newPackLevelConsumer(r, view), r, 3)

	old, ok, _ := view.FindByGTIN(context.Background(), "24006381333935")
	if !ok || old.EffectiveTo == nil || old.UnitsPerPack != 12 || old.Level != task.PackLevelCase || old.ChildGTIN != "04006381333931" {
		t.Fatalf("retired row = %+v ok=%v", old, ok)
	}
	next, ok, _ := view.FindByGTIN(context.Background(), "04006381333948")
	if !ok || next.UnitsPerPack != 24 || next.EffectiveTo != nil {
		t.Fatalf("superseding row = %+v ok=%v", next, ok)
	}
}

func TestPackLevelConsumer_SkipsInvalidAndIgnoresOtherTypes(t *testing.T) {
	view := packview.NewMemoryStore()
	r := &fakeFetchReader{msgs: []kafkago.Message{
		msgAt(0, []byte(`{"flat":"envelope"}`)),
		msgAt(1, declaredCE("ce-bad-gtin", map[string]any{"gtin": "24006381333936"})),
		msgAt(2, declaredCE("ce-bad-level", map[string]any{"level": "BOX"})),
		msgAt(3, declaredCE("ce-bad-upp", map[string]any{"units_per_pack": 0})),
		msgAt(4, declaredCE("ce-bad-version", map[string]any{"version": 0})),
		msgAt(5, declaredCE("ce-bad-sku", map[string]any{"sku": " "})),
		msgAt(6, declaredCE("ce-bad-child", map[string]any{"child_gtin": "123"})),
		msgAt(7, packLevelCE("ce-other", "com.warehouse.wms.product-master.product.ProductRegistered", "SKU-1", map[string]any{"sku": "SKU-1"})),
		msgAt(8, packLevelCE("ce-bad-retire", kafka.TypeProductPackLevelRetired, "SKU-1", map[string]any{"sku": "SKU-1", "gtin": "24006381333935", "version": 4})),
		msgAt(9, declaredCE("ce-ok", nil)),
	}}
	runPackLevelUntilCommitted(t, newPackLevelConsumer(r, view), r, 10)

	if _, ok, _ := view.FindByGTIN(context.Background(), "24006381333935"); !ok {
		t.Fatal("the one valid event must be applied")
	}
}

func TestPackLevelConsumer_RequiresAGroup(t *testing.T) {
	if _, err := kafka.NewPackLevelConsumer([]string{"localhost:1"}, kafka.ProductMasterTopic, "", &usecases.ApplyPackLevel{}, nil); err == nil {
		t.Fatal("an empty consumer group must be refused (PACK_VIEW_CONSUMER_GROUP)")
	}
}
