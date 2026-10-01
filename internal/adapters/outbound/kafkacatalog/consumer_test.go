package kafkacatalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"

	"github.com/claudioed/fulfillment-execution/internal/domain/pathcatalog"
)

// fakeReader replays a fixed sequence of messages, then blocks until ctx
// is cancelled -- mirrors a real Kafka reader that has caught up and is
// now waiting for new messages.
type fakeReader struct {
	messages []kafkago.Message
	pos      int
	closed   bool
}

func (r *fakeReader) ReadMessage(ctx context.Context) (kafkago.Message, error) {
	if r.pos < len(r.messages) {
		m := r.messages[r.pos]
		r.pos++
		return m, nil
	}
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}

func (r *fakeReader) Close() error {
	r.closed = true
	return nil
}

// envelopeMsg builds a CloudEvents 1.0 structured-mode message exactly as
// process-path-management publishes it (ADR-0032), via the official SDK.
func envelopeMsg(t *testing.T, partition int, offset int64, eventType string, data any) kafkago.Message {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(fmt.Sprintf("evt-%d-%d", partition, offset))
	e.SetSource("/warehouse/process-path-management")
	e.SetType(eventType)
	e.SetSubject("path")
	e.SetTime(time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC))
	e.SetDataSchema("urn:warehouse:process-path-management:events:X:v1")
	if err := e.SetData(ce.ApplicationJSON, data); err != nil {
		t.Fatalf("set data: %v", err)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal cloudevent: %v", err)
	}
	return kafkago.Message{Partition: partition, Offset: offset, Value: raw}
}

func newTestConsumer(reader Reader, target targetOffsets) *Consumer {
	c := &Consumer{
		Reader:  reader,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		paths:   make(map[string]pathcatalog.PathDefinition),
		readyCh: make(chan struct{}),
		target:  target,
	}
	if len(target) == 0 {
		c.markReady()
	}
	return c
}

func TestConsumer_NoTargetOffsets_IsReadyImmediately(t *testing.T) {
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	if !c.Ready() {
		t.Fatal("expected a consumer with no readiness target to be ready immediately")
	}
}

func TestConsumer_Run_BecomesReadyAfterCatchingUpSinglePartition(t *testing.T) {
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, TypeProcessPathCreated, pathData{PathId: "PICK", MatchPrefix: "pick", Direct: true, RequiredCapabilities: []string{"pick"}}),
			envelopeMsg(t, 0, 1, TypeProcessPathCreated, pathData{PathId: "PACK", MatchPrefix: "pack", Direct: true, RequiredCapabilities: []string{"pack"}}),
		},
	}
	// target[0] = 2 means "caught up once offset 1 has been processed"
	// (last is exclusive: 2 messages at offsets 0 and 1).
	c := newTestConsumer(reader, targetOffsets{0: 2})

	if c.Ready() {
		t.Fatal("expected not ready before Run starts")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = c.Run(ctx)
		close(done)
	}()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout, got: %v", err)
	}
	cancel()
	<-done

	if _, err := c.Lookup("pick"); err != nil {
		t.Fatalf("expected PICK to be looked up successfully, got: %v", err)
	}
	if _, err := c.Lookup("pack"); err != nil {
		t.Fatalf("expected PACK to be looked up successfully, got: %v", err)
	}
}

func TestConsumer_MultiPartition_ReadyOnlyAfterBothCaughtUp(t *testing.T) {
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, TypeProcessPathCreated, pathData{PathId: "PICK", MatchPrefix: "pick", Direct: true, RequiredCapabilities: []string{"pick"}}),
			// Partition 1 not yet caught up (target[1]=1, need offset 0).
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 1, 1: 1})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err == nil {
		t.Fatal("expected WaitReady to time out -- partition 1 never caught up")
	}
}

func TestConsumer_Deactivated_RemovesPathFromCache(t *testing.T) {
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, TypeProcessPathCreated, pathData{PathId: "PICK", MatchPrefix: "pick", Direct: true, RequiredCapabilities: []string{"pick"}}),
			envelopeMsg(t, 0, 1, TypeProcessPathDeactivated, pathData{PathId: "PICK"}),
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 2})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout, got: %v", err)
	}

	if _, err := c.Lookup("pick"); err == nil {
		t.Fatal("expected PICK to be unknown after deactivation")
	}
}

func TestConsumer_Revised_UpdatesMatchPrefix(t *testing.T) {
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, TypeProcessPathCreated, pathData{PathId: "PICK", MatchPrefix: "pick", Direct: true, RequiredCapabilities: []string{"pick"}}),
			envelopeMsg(t, 0, 1, TypeProcessPathUpdated, pathData{PathId: "PICK", MatchPrefix: "pick-zone-a", Direct: true, RequiredCapabilities: []string{"pick", "hazmat"}}),
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 2})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout, got: %v", err)
	}

	def, err := c.Lookup("pick-zone-a")
	if err != nil {
		t.Fatalf("expected pick-zone-a to resolve after revision, got: %v", err)
	}
	if len(def.RequiredCapabilities) != 2 {
		t.Fatalf("expected 2 required capabilities after revision, got %d", len(def.RequiredCapabilities))
	}
}

// destination_location_role is process-path-management's optional field
// (that service's ADR 0006/0009); this consumer must decode it into
// PathDefinition.DestinationLocationRole so a future caller of Lookup
// can act on it — see the PathDefinition doc comment for why this is
// wiring only.
func TestConsumer_DecodesDestinationLocationRole(t *testing.T) {
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, TypeProcessPathCreated, pathData{PathId: "PACK", MatchPrefix: "pack", Direct: true, RequiredCapabilities: []string{"pack"}, DestinationLocationRole: "Drop"}),
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 1})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout, got: %v", err)
	}

	def, err := c.Lookup("pack")
	if err != nil {
		t.Fatalf("expected PACK to resolve, got: %v", err)
	}
	if def.DestinationLocationRole != "Drop" {
		t.Fatalf("expected DestinationLocationRole %q, got %q", "Drop", def.DestinationLocationRole)
	}
}

// A path with no declared destination role (the default, most common
// case — see process-path-management's own ProcessPathData doc comment)
// must decode to the empty string, not fail or panic.
func TestConsumer_NoDestinationLocationRole_DecodesToEmptyString(t *testing.T) {
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, TypeProcessPathCreated, pathData{PathId: "PICK", MatchPrefix: "pick", Direct: true, RequiredCapabilities: []string{"pick"}}),
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 1})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout, got: %v", err)
	}

	def, err := c.Lookup("pick")
	if err != nil {
		t.Fatalf("expected PICK to resolve, got: %v", err)
	}
	if def.DestinationLocationRole != "" {
		t.Fatalf("expected empty DestinationLocationRole when undeclared, got %q", def.DestinationLocationRole)
	}
}

func TestConsumer_UnknownEventType_IsIgnored(t *testing.T) {
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, "com.warehouse.wes.process-path-management.processpath.SomeFutureEvent", map[string]any{}),
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 1})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout even for an unrecognized event type, got: %v", err)
	}
}

// A short-name type (the retired flat envelope's event_type value) must
// not match: dispatch is on the FULL CloudEvents type string only.
func TestConsumer_ShortNameType_IsIgnored(t *testing.T) {
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	msg := envelopeMsg(t, 0, 0, "ProcessPathCreated", pathData{PathId: "PICK", MatchPrefix: "pick"})
	if err := c.handle(msg); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if _, err := c.Lookup("pick"); err == nil {
		t.Fatal("a short-name type must not be applied")
	}
}

// A legacy flat-envelope message is rejected as not-a-CloudEvent (logged
// and committed past by Run), never parsed into the catalogue — and it
// still counts toward readiness so it cannot wedge startup.
func TestConsumer_LegacyFlatEnvelope_IsRejectedNotParsed(t *testing.T) {
	flat := kafkago.Message{Partition: 0, Offset: 0, Value: []byte(`{"event_id":"e1","event_type":"ProcessPathCreated","occurred_at":"2026-01-01T00:00:00Z","source":"process-path-management","data":{"path_id":"PICK","match_prefix":"pick"}}`)}
	c := newTestConsumer(&fakeReader{messages: []kafkago.Message{flat}}, targetOffsets{0: 1})

	if err := c.handle(flat); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Fatalf("handle(flat) err = %v, want ErrNotCloudEvent", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready after skipping the legacy message, got: %v", err)
	}
	if _, err := c.Lookup("pick"); err == nil {
		t.Fatal("legacy flat message must not populate the catalogue")
	}
}
