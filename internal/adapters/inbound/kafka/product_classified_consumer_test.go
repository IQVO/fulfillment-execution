package kafka_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
)

// fakeFetchReader serves a fixed message list through FetchMessage, records
// every CommitMessages call, and blocks on ctx once the list is drained.
type fakeFetchReader struct {
	mu        sync.Mutex
	msgs      []kafkago.Message
	next      int
	committed []int64
	fetchErrs int
}

func (r *fakeFetchReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	r.mu.Lock()
	if r.fetchErrs > 0 {
		r.fetchErrs--
		r.mu.Unlock()
		return kafkago.Message{}, errors.New("fake: broker blip")
	}
	if r.next < len(r.msgs) {
		m := r.msgs[r.next]
		r.next++
		r.mu.Unlock()
		return m, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}

func (r *fakeFetchReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range msgs {
		r.committed = append(r.committed, m.Offset)
	}
	return nil
}

func (r *fakeFetchReader) commits() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.committed...)
}

// flakyCopy fails the first failN upserts, then delegates to a MemoryStore.
type flakyCopy struct {
	*productclassificationcopy.MemoryStore
	mu    sync.Mutex
	failN int
	calls int
}

func (f *flakyCopy) UpsertIfNewer(ctx context.Context, rec ports.ProductClassificationRecord) (bool, error) {
	f.mu.Lock()
	f.calls++
	fail := f.failN > 0
	if fail {
		f.failN--
	}
	f.mu.Unlock()
	if fail {
		return false, errors.New("fake: database unavailable")
	}
	return f.MemoryStore.UpsertIfNewer(ctx, rec)
}

func productClassifiedCE(id, sku string, data map[string]any) []byte {
	return cloudEvent(id, "/warehouse/product-master", kafka.TypeProductClassified, sku,
		time.Date(2026, 10, 6, 21, 2, 0, 0, time.UTC), "urn:warehouse:product-master:events:ProductClassified:v1", data)
}

func msgAt(offset int64, value []byte) kafkago.Message {
	return kafkago.Message{Topic: kafka.ProductMasterTopic, Partition: 0, Offset: offset, Value: value}
}

// runUntilCommitted runs the consumer until want commits were recorded.
func runUntilCommitted(t *testing.T, c *kafka.ProductClassifiedConsumer, r *fakeFetchReader, want int) {
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

func newClassifiedConsumer(r *fakeFetchReader, cp ports.ProductClassificationCopy) *kafka.ProductClassifiedConsumer {
	apply := &usecases.ApplyProductClassified{Processed: memory.NewProcessedEventsRepo(), Copy: cp}
	return kafka.NewProductClassifiedConsumerFromReader(r, kafka.ProductMasterTopic, apply, slog.New(slog.NewTextHandler(io.Discard, nil))).
		WithRetryIntervals(time.Millisecond, 5*time.Millisecond)
}

func TestProductClassifiedConsumer_AppliesAndCommits(t *testing.T) {
	store := productclassificationcopy.NewMemoryStore()
	r := &fakeFetchReader{msgs: []kafkago.Message{
		msgAt(0, productClassifiedCE("ce-1", "SKU-1", map[string]any{
			"sku": "SKU-1", "handling_tags": []string{"Hazmat", "TemperatureSensitive"},
			"temperature_class": "Frozen", "dot_hazard_class": 3, "classification_source": "native", "version": 3,
		})),
	}}
	runUntilCommitted(t, newClassifiedConsumer(r, store), r, 1)

	info, err := store.GetClassification(context.Background(), "SKU-1")
	if err != nil || info != (ports.ClassificationInfo{Known: true, Hazmat: true, DOTHazardClass: 3}) {
		t.Fatalf("lookup = %+v, %v", info, err)
	}
}

func TestProductClassifiedConsumer_StaleVersionIgnored(t *testing.T) {
	store := productclassificationcopy.NewMemoryStore()
	r := &fakeFetchReader{msgs: []kafkago.Message{
		msgAt(0, productClassifiedCE("ce-a", "SKU-2", map[string]any{"sku": "SKU-2", "handling_tags": []string{"Hazmat"}, "dot_hazard_class": 3, "version": 5})),
		msgAt(1, productClassifiedCE("ce-b", "SKU-2", map[string]any{"sku": "SKU-2", "handling_tags": []string{"Hazmat"}, "dot_hazard_class": 1, "version": 4})),
	}}
	runUntilCommitted(t, newClassifiedConsumer(r, store), r, 2)

	info, _ := store.GetClassification(context.Background(), "SKU-2")
	if info.DOTHazardClass != 3 {
		t.Fatalf("dot = %d: a stale version overwrote the copy", info.DOTHazardClass)
	}
}

func TestProductClassifiedConsumer_UnknownTypeIgnoredAndCommitted(t *testing.T) {
	store := productclassificationcopy.NewMemoryStore()
	registered := cloudEvent("ce-r", "/warehouse/product-master", "com.warehouse.wms.product-master.product.ProductRegistered", "SKU-3",
		time.Date(2026, 10, 6, 21, 0, 0, 0, time.UTC), "urn:warehouse:product-master:events:ProductRegistered:v1",
		map[string]any{"sku": "SKU-3", "description": "x", "version": 1})
	r := &fakeFetchReader{msgs: []kafkago.Message{msgAt(7, registered)}}
	runUntilCommitted(t, newClassifiedConsumer(r, store), r, 1)

	if got := r.commits(); got[0] != 7 {
		t.Fatalf("commits = %v, want offset 7 committed past", got)
	}
	if info, _ := store.GetClassification(context.Background(), "SKU-3"); info.Known {
		t.Fatal("a ProductRegistered must not create a classification")
	}
}

func TestProductClassifiedConsumer_InvalidMessagesSkippedAndCommitted(t *testing.T) {
	store := productclassificationcopy.NewMemoryStore()
	r := &fakeFetchReader{msgs: []kafkago.Message{
		msgAt(0, []byte(`{"event_type":"ProductClassified","sku":"SKU-X"}`)), // not a CloudEvent
		msgAt(1, []byte(`not json`)),
		msgAt(2, productClassifiedCE("ce-nosku", "SKU-X", map[string]any{"handling_tags": []string{"Hazmat"}, "version": 1})),
		msgAt(3, productClassifiedCE("ce-v0", "SKU-X", map[string]any{"sku": "SKU-X", "handling_tags": []string{"Hazmat"}, "version": 0})),
		msgAt(4, productClassifiedCE("ce-dot", "SKU-X", map[string]any{"sku": "SKU-X", "handling_tags": []string{"Hazmat"}, "dot_hazard_class": 12, "version": 1})),
		msgAt(5, productClassifiedCE("ce-type", "SKU-X", map[string]any{"sku": 42, "version": 1})),
		msgAt(6, productClassifiedCE("ce-ok", "SKU-OK", map[string]any{"sku": "SKU-OK", "handling_tags": []string{"Fragile"}, "version": 1})),
	}}
	runUntilCommitted(t, newClassifiedConsumer(r, store), r, 7)

	if info, _ := store.GetClassification(context.Background(), "SKU-X"); info.Known {
		t.Fatal("an invalid message must never reach the copy")
	}
	if info, _ := store.GetClassification(context.Background(), "SKU-OK"); !info.Known || !info.Fragile {
		t.Fatalf("the valid message after the invalid ones was not applied: %+v", info)
	}
}

// A transient failure is retried on the SAME message and the offset is
// committed exactly once, after the success.
func TestProductClassifiedConsumer_TransientFailureRetriedThenCommittedOnce(t *testing.T) {
	cp := &flakyCopy{MemoryStore: productclassificationcopy.NewMemoryStore(), failN: 2}
	r := &fakeFetchReader{
		fetchErrs: 1,
		msgs: []kafkago.Message{
			msgAt(0, productClassifiedCE("ce-flaky", "SKU-F", map[string]any{"sku": "SKU-F", "handling_tags": []string{"Hazmat"}, "dot_hazard_class": 2, "version": 1})),
		},
	}
	c := newClassifiedConsumer(r, cp)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	deadline := time.Now().Add(10 * time.Second)
	for len(r.commits()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	// Give a buggy loop the chance to commit twice before asserting.
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if got := r.commits(); len(got) != 1 || got[0] != 0 {
		t.Fatalf("commits = %v, want exactly one commit of offset 0", got)
	}
	cp.mu.Lock()
	calls := cp.calls
	cp.mu.Unlock()
	if calls != 3 {
		t.Fatalf("upsert attempts = %d, want 3 (2 failures + 1 success)", calls)
	}
	info, _ := cp.GetClassification(context.Background(), "SKU-F")
	if !info.Known || info.DOTHazardClass != 2 {
		t.Fatalf("lookup = %+v, want the retried classification", info)
	}
}

// Stopping the consumer while a message keeps failing must NOT commit it.
func TestProductClassifiedConsumer_StopDuringRetryDoesNotCommit(t *testing.T) {
	cp := &flakyCopy{MemoryStore: productclassificationcopy.NewMemoryStore(), failN: 1 << 30}
	r := &fakeFetchReader{msgs: []kafkago.Message{
		msgAt(0, productClassifiedCE("ce-down", "SKU-D", map[string]any{"sku": "SKU-D", "handling_tags": []string{"Hazmat"}, "version": 1})),
	}}
	c := newClassifiedConsumer(r, cp)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned %v", err)
	}
	if got := r.commits(); len(got) != 0 {
		t.Fatalf("commits = %v, a failing message must never be committed", got)
	}
}

func TestNewProductClassifiedConsumer_RequiresGroup(t *testing.T) {
	if _, err := kafka.NewProductClassifiedConsumer([]string{"broker:9092"}, kafka.ProductMasterTopic, "", nil, nil); err == nil {
		t.Fatal("an empty consumer group must be rejected")
	}
	c, err := kafka.NewProductClassifiedConsumer([]string{"broker:9092"}, kafka.ProductMasterTopic, "group-under-test", nil, nil)
	if err != nil {
		t.Fatalf("NewProductClassifiedConsumer: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestProductClassifiedConsumer_CloseWithInjectedReaderIsNoop(t *testing.T) {
	c := newClassifiedConsumer(&fakeFetchReader{}, productclassificationcopy.NewMemoryStore())
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
