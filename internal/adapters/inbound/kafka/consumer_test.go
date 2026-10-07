package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/ports"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/pathcatalog"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

var epoch = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func idSeq(prefix string) func() shared.TaskId {
	n := 0
	return func() shared.TaskId {
		n++
		return shared.TaskId(prefix + string(rune('0'+n)))
	}
}

// testCatalogue is the fixture catalogue every consumer test uses,
// mirroring warehouse-infra's real sortable-fc.yaml declared paths.
func testCatalogue() *pathcatalog.Catalogue {
	return pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", Direct: true, RequiredCapabilities: []string{"pick"}},
		{Id: "PACK", MatchPrefix: "pack", Direct: true, RequiredCapabilities: []string{"pack"}},
		{Id: "REBIN", MatchPrefix: "rebin", Direct: true, RequiredCapabilities: []string{"rebin"}},
		{Id: "SLAM", MatchPrefix: "slam", Direct: true, RequiredCapabilities: []string{"slam"}},
		{Id: "DISPATCH", MatchPrefix: "dispatch", Direct: true, RequiredCapabilities: []string{"dispatch"}},
		{Id: "ARRIVAL", MatchPrefix: "arrival", Direct: true, RequiredCapabilities: []string{"arrival"}},
	})
}

func newConsumer(t *testing.T) (*kafka.Consumer, *memory.TaskRepo) {
	t.Helper()
	tasks := memory.NewTaskRepo()
	apply := newApplyWorkReleased(tasks, memory.NewProcessedEventsRepo(), testCatalogue())
	c := &kafka.Consumer{
		Apply:  apply,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return c, tasks
}

// newApplyWorkReleased builds the production use-case shape the consumer
// now drives: CreateTask + processed-events claim + catalogue lookup
// inside one (here nil — in-memory) UnitOfWork.
func newApplyWorkReleased(tasks *memory.TaskRepo, processed ports.ProcessedEvents, catalogue ports.PathCatalogue) *usecases.ApplyWorkReleased {
	createTask := &usecases.CreateTask{
		Tasks:     tasks,
		Publisher: events.NewBufferedPublisher(),
		Clock:     memory.NewFixedClock(epoch),
		NewId:     idSeq("t"),
	}
	return &usecases.ApplyWorkReleased{
		CreateTask: createTask,
		Processed:  processed,
		Catalogue:  catalogue,
	}
}

// fakeDeadLetterSink records every message written to it, so a dead-letter
// test can assert on topic/key/value/headers without a live broker.
type fakeDeadLetterSink struct {
	sent []kafkago.Message
	err  error
}

func (f *fakeDeadLetterSink) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, msgs...)
	return nil
}

func headerValue(headers []kafkago.Header, key string) (string, bool) {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// sendToDeadLetter (invoked by Run on a Handle failure) publishes the
// original message to <topic>.dlq, preserving its key/value and adding
// failure-context headers.
func TestSendToDeadLetter_PublishesOriginalMessageWithFailureContext(t *testing.T) {
	c, _ := newConsumer(t)
	sink := &fakeDeadLetterSink{}
	c.DeadLetter = sink

	orig := kafkago.Message{
		Topic:     "warehouse.work-planning.events",
		Partition: 2,
		Offset:    42,
		Key:       []byte("poison-key"),
		Value:     workReleasedJSON("evt-dlq", "NOT-A-REAL-PATH", "wu-dlq"),
		Headers:   []kafkago.Header{{Key: "existing", Value: []byte("keep-me")}},
	}
	c.SendToDeadLetter(context.Background(), orig, fmt.Errorf("path_id not found in catalogue"))

	if len(sink.sent) != 1 {
		t.Fatalf("expected exactly 1 message sent to the dead-letter sink, got %d", len(sink.sent))
	}
	got := sink.sent[0]
	if got.Topic != "warehouse.work-planning.events.dlq" {
		t.Fatalf("expected dlq topic %q, got %q", "warehouse.work-planning.events.dlq", got.Topic)
	}
	if string(got.Key) != "poison-key" {
		t.Fatalf("expected original key preserved, got %q", got.Key)
	}
	if string(got.Value) != string(orig.Value) {
		t.Fatalf("expected original value preserved verbatim, got %q", got.Value)
	}
	if v, ok := headerValue(got.Headers, "existing"); !ok || v != "keep-me" {
		t.Fatalf("expected original header preserved, got headers %+v", got.Headers)
	}
	if v, ok := headerValue(got.Headers, "x-dlq-error"); !ok || v != "path_id not found in catalogue" {
		t.Fatalf("expected x-dlq-error header with the failure cause, got %+v", got.Headers)
	}
	if v, ok := headerValue(got.Headers, "x-dlq-original-offset"); !ok || v != "42" {
		t.Fatalf("expected x-dlq-original-offset=42, got %+v", got.Headers)
	}
}

// A nil DeadLetter (the default — every pre-existing deployment, and every
// other test in this file) must be a safe no-op, not a panic.
func TestSendToDeadLetter_NilSink_IsNoOp(t *testing.T) {
	c, _ := newConsumer(t)
	c.DeadLetter = nil

	c.SendToDeadLetter(context.Background(), kafkago.Message{Topic: "t"}, fmt.Errorf("boom"))
}

// A DeadLetter publish failure must be swallowed (logged, not propagated)
// so a broker blip on the DLQ write itself cannot wedge the main consumer
// loop — the entire point of routing failures to a DLQ instead of just
// logging is that the main loop keeps moving.
func TestSendToDeadLetter_SinkFailure_DoesNotPanicOrPropagate(t *testing.T) {
	c, _ := newConsumer(t)
	c.DeadLetter = &fakeDeadLetterSink{err: fmt.Errorf("dlq broker unreachable")}

	// No panic and no return value to check — sendToDeadLetter is void;
	// simply completing without panicking is the assertion.
	c.SendToDeadLetter(context.Background(), kafkago.Message{Topic: "t", Value: []byte("x")}, fmt.Errorf("original failure"))
}

func totalPending(t *testing.T, tasks *memory.TaskRepo) int {
	t.Helper()
	total := 0
	for _, tt := range []task.Type{task.Pick, task.Pack, task.Rebin, task.Slam} {
		n, err := tasks.CountByTypeAndStatus(context.Background(), tt, task.Pending)
		if err != nil {
			t.Fatalf("CountByTypeAndStatus: %v", err)
		}
		total += n
	}
	return total
}

func workReleasedJSON(eventId, pathId, workUnitId string) []byte {
	return workReleasedCE(eventId, kafka.TypeWorkReleased, workUnitId, map[string]any{
		"path_id":      pathId,
		"work_unit_id": workUnitId,
		"cpt":          "2026-08-21T23:00:00Z",
		"ref":          "release-1",
	})
}

func workReleasedJSONWithFragile(eventId, pathId, workUnitId string, fragile bool) []byte {
	return workReleasedCE(eventId, kafka.TypeWorkReleased, workUnitId, map[string]any{
		"path_id":      pathId,
		"work_unit_id": workUnitId,
		"cpt":          "2026-08-21T23:00:00Z",
		"ref":          "release-1",
		"fragile":      fragile,
	})
}

func workReleasedJSONWithGiftWrap(eventId, pathId, workUnitId string, giftWrap bool) []byte {
	return workReleasedCE(eventId, kafka.TypeWorkReleased, workUnitId, map[string]any{
		"path_id":      pathId,
		"work_unit_id": workUnitId,
		"cpt":          "2026-08-21T23:00:00Z",
		"ref":          "release-1",
		"gift_wrap":    giftWrap,
	})
}

// legacyFlatWorkReleasedJSON is the retired flat platform envelope
// (event_id/event_type/occurred_at) — used only to prove it is rejected.
func legacyFlatWorkReleasedJSON(eventId, pathId, workUnitId string) []byte {
	return []byte(`{
		"event_id": "` + eventId + `",
		"event_type": "WorkReleased",
		"occurred_at": "2026-08-21T22:00:00Z",
		"source": "wes-work-planning",
		"data": {"path_id": "` + pathId + `", "work_unit_id": "` + workUnitId + `", "cpt": "2026-08-21T23:00:00Z", "ref": "release-1"}
	}`)
}

func TestNewConsumerWithGroup_UsesSuppliedConsumerGroup(t *testing.T) {
	c := kafka.NewConsumerWithGroup([]string{"broker:9092"}, "work-released", "e2s-fulfillment", nil, nil)
	defer c.Close()

	config := c.Reader.Config()
	if got := config.GroupID; got != "e2s-fulfillment" {
		t.Fatalf("Reader group ID = %q, want supplied group", got)
	}
	if config.StartOffset != kafkago.LastOffset {
		t.Fatalf("Reader start offset = %d, want LastOffset for an isolated group", config.StartOffset)
	}
}

func TestHandleMessage_CreatesTaskFromWorkReleased(t *testing.T) {
	c, tasks := newConsumer(t)

	err := c.HandleMessage(context.Background(), workReleasedJSON("evt-1", "PICK", "wu-1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := totalPending(t, tasks); got != 1 {
		t.Fatalf("expected exactly 1 task, got %d", got)
	}
	n, _ := tasks.CountByTypeAndStatus(context.Background(), task.Pick, task.Pending)
	if n != 1 {
		t.Fatalf("expected the task to be a Pick task, got %d Pick tasks", n)
	}
}

func TestHandleMessage_IgnoresNonWorkReleasedEvents(t *testing.T) {
	c, tasks := newConsumer(t)

	for _, typ := range []string{
		"com.warehouse.wes.work-planning.workunit.SomethingElse",
		// The retired short name must not match: dispatch is on the FULL
		// CloudEvents type string only.
		"WorkReleased",
	} {
		other := workReleasedCE("evt-2-"+typ, typ, "wu-2", map[string]any{"path_id": "PICK", "work_unit_id": "wu-2", "cpt": "2026-08-21T23:00:00Z"})
		if err := c.HandleMessage(context.Background(), other); err != nil {
			t.Fatalf("unexpected error for type %q: %v", typ, err)
		}
	}

	if got := totalPending(t, tasks); got != 0 {
		t.Fatalf("expected no tasks created, got %d", got)
	}
}

func TestHandleMessage_DoubleDeliveryCreatesExactlyOneTask(t *testing.T) {
	c, tasks := newConsumer(t)

	msg := workReleasedJSON("evt-dup", "PACK", "wu-2")
	if err := c.HandleMessage(context.Background(), msg); err != nil {
		t.Fatalf("first delivery: unexpected error: %v", err)
	}
	if err := c.HandleMessage(context.Background(), msg); err != nil {
		t.Fatalf("second delivery: unexpected error: %v", err)
	}

	if got := totalPending(t, tasks); got != 1 {
		t.Fatalf("expected exactly 1 task after double delivery, got %d", got)
	}
}

// The path-catalogue lookup replaces the old path_id-prefix-guessing
// convention: every declared path resolves to its own real task type and
// required-capability set, sourced entirely from the catalogue.
func TestHandleMessage_DerivesTaskTypeFromCatalogue(t *testing.T) {
	c, tasks := newConsumer(t)

	cases := []struct {
		eventId  string
		pathId   string
		wantType task.Type
	}{
		{"evt-pick", "PICK", task.Pick},
		{"evt-pack", "PACK", task.Pack},
		{"evt-rebin", "REBIN", task.Rebin},
		{"evt-slam", "SLAM", task.Slam},
	}
	for _, tc := range cases {
		if err := c.HandleMessage(context.Background(), workReleasedJSON(tc.eventId, tc.pathId, "wu-derive")); err != nil {
			t.Fatalf("unexpected error for path_id %q: %v", tc.pathId, err)
		}
	}

	pick, _ := tasks.CountByTypeAndStatus(context.Background(), task.Pick, task.Pending)
	pack, _ := tasks.CountByTypeAndStatus(context.Background(), task.Pack, task.Pending)
	rebin, _ := tasks.CountByTypeAndStatus(context.Background(), task.Rebin, task.Pending)
	slam, _ := tasks.CountByTypeAndStatus(context.Background(), task.Slam, task.Pending)

	if pick != 1 {
		t.Fatalf("expected 1 Pick task, got %d", pick)
	}
	if pack != 1 {
		t.Fatalf("expected 1 Pack task, got %d", pack)
	}
	if rebin != 1 {
		t.Fatalf("expected 1 Rebin task, got %d", rebin)
	}
	if slam != 1 {
		t.Fatalf("expected 1 SLAM task, got %d", slam)
	}
}

// The regression this fix exists to prevent: real WorkReleased events
// across this fleet carry station/zone/scenario-qualified path_id
// values (order-management's default "pick", e2e fixtures' "pick-zone-a"
// and "pick-soak"), not the bare canonical catalogue id. Every one of
// these must resolve to the correct task type, not fail as unknown.
func TestHandleMessage_ResolvesRealFleetPathIdVariants(t *testing.T) {
	c, tasks := newConsumer(t)

	cases := []struct {
		eventId  string
		pathId   string
		wantType task.Type
	}{
		{"evt-default-pick", "pick", task.Pick},
		{"evt-zone-a", "pick-zone-a", task.Pick},
		{"evt-pick-soak", "pick-soak", task.Pick},
		{"evt-pack-soak", "pack-soak", task.Pack},
	}
	for _, tc := range cases {
		if err := c.HandleMessage(context.Background(), workReleasedJSON(tc.eventId, tc.pathId, "wu-variant")); err != nil {
			t.Fatalf("unexpected error for real-world path_id %q: %v", tc.pathId, err)
		}
	}

	pick, _ := tasks.CountByTypeAndStatus(context.Background(), task.Pick, task.Pending)
	pack, _ := tasks.CountByTypeAndStatus(context.Background(), task.Pack, task.Pending)
	if pick != 3 {
		t.Fatalf("expected 3 Pick tasks from the 3 pick-family path_ids, got %d", pick)
	}
	if pack != 1 {
		t.Fatalf("expected 1 Pack task from the pack-family path_id, got %d", pack)
	}
}

// The core behavior change this catalogue introduces: an unrecognized
// path_id is now a hard error, never a silent default to task.Pick. This
// is a deliberate breaking fix to a documented bug (see ADR on the
// process-path catalogue), not a regression.
func TestHandleMessage_UnknownPathId_ReturnsError(t *testing.T) {
	c, tasks := newConsumer(t)

	err := c.HandleMessage(context.Background(), workReleasedJSON("evt-unknown", "NOT-A-REAL-PATH", "wu-unknown"))
	if err == nil {
		t.Fatal("expected an error for an unrecognized path_id, got nil")
	}
	if got := totalPending(t, tasks); got != 0 {
		t.Fatalf("expected no task to be created for an unrecognized path_id, got %d", got)
	}
}

// data.fragile is optional on WorkReleased for backward compatibility with
// producers that predate the field — a known simplification documented in
// this repo's README/INTEGRATION notes, matching the existing path_id
// prefix convention. Present-and-true must thread through to the created
// Task's Fragile flag.
func TestHandleMessage_ReadsOptionalFragileFlag(t *testing.T) {
	c, tasks := newConsumer(t)

	err := c.HandleMessage(context.Background(), workReleasedJSONWithFragile("evt-fragile", "PACK", "wu-fragile", true))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	candidates, err := tasks.FindClaimableByType(context.Background(), task.Pack, epoch)
	if err != nil {
		t.Fatalf("FindClaimableByType: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("expected exactly 1 Pack task, got %d", len(candidates))
	}
	if !candidates[0].Fragile() {
		t.Fatalf("expected the created task to carry Fragile() == true")
	}
}

// Absent data.fragile (the shape produced by any already-documented
// producer that predates this field) must default to false rather than
// error or panic.
func TestHandleMessage_MissingFragileFieldDefaultsFalse(t *testing.T) {
	c, tasks := newConsumer(t)

	err := c.HandleMessage(context.Background(), workReleasedJSON("evt-no-fragile", "PICK", "wu-no-fragile"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	candidates, err := tasks.FindClaimableByType(context.Background(), task.Pick, epoch)
	if err != nil {
		t.Fatalf("FindClaimableByType: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("expected exactly 1 Pick task, got %d", len(candidates))
	}
	if candidates[0].Fragile() {
		t.Fatalf("expected Fragile() == false when data.fragile is absent from the envelope")
	}
}

// data.gift_wrap is optional on WorkReleased, omitted entirely (never
// published as explicit false) when gift wrap was not requested — see
// ADR-0011. Present-and-true must thread through to the created Task's
// GiftWrap flag.
func TestHandleMessage_ReadsOptionalGiftWrapFlag(t *testing.T) {
	c, tasks := newConsumer(t)

	err := c.HandleMessage(context.Background(), workReleasedJSONWithGiftWrap("evt-giftwrap", "PACK", "wu-giftwrap", true))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	candidates, err := tasks.FindClaimableByType(context.Background(), task.Pack, epoch)
	if err != nil {
		t.Fatalf("FindClaimableByType: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("expected exactly 1 Pack task, got %d", len(candidates))
	}
	if !candidates[0].GiftWrap() {
		t.Fatalf("expected the created task to carry GiftWrap() == true")
	}
}

// Absent data.gift_wrap (the common case: any WorkReleased that does not
// carry a gift-wrap request for the order) must default to false rather
// than error or panic.
func TestHandleMessage_MissingGiftWrapFieldDefaultsFalse(t *testing.T) {
	c, tasks := newConsumer(t)

	err := c.HandleMessage(context.Background(), workReleasedJSON("evt-no-giftwrap", "PICK", "wu-no-giftwrap"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	candidates, err := tasks.FindClaimableByType(context.Background(), task.Pick, epoch)
	if err != nil {
		t.Fatalf("FindClaimableByType: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("expected exactly 1 Pick task, got %d", len(candidates))
	}
	if candidates[0].GiftWrap() {
		t.Fatalf("expected GiftWrap() == false when data.gift_wrap is absent from the envelope")
	}
}

// A WorkReleased CloudEvent's data.work_unit_id becomes the created
// Task's OrderRef (payload read via DataAs).
func TestHandleMessage_CloudEvent_SetsOrderRefFromData(t *testing.T) {
	c, tasks := newConsumer(t)

	if err := c.HandleMessage(context.Background(), workReleasedJSON("evt-ce", "PICK", "wu-ce")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	candidates, err := tasks.FindClaimableByType(context.Background(), task.Pick, epoch)
	if err != nil {
		t.Fatalf("FindClaimableByType: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("expected exactly 1 Pick task, got %d", len(candidates))
	}
	if got := candidates[0].OrderRef(); got != shared.OrderRef("wu-ce") {
		t.Fatalf("expected OrderRef %q (from data.work_unit_id), got %q", "wu-ce", got)
	}
}

// The retired flat envelope is a deterministic poison message (ADR-0032):
// rejected with cloudevents.ErrNotCloudEvent, never parsed, never creates
// a Task — Run dead-letters it.
func TestHandleMessage_LegacyFlatEnvelope_IsRejectedNotParsed(t *testing.T) {
	c, tasks := newConsumer(t)

	err := c.HandleMessage(context.Background(), legacyFlatWorkReleasedJSON("evt-flat", "PICK", "wu-flat"))
	if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Fatalf("err = %v, want ErrNotCloudEvent", err)
	}
	if got := totalPending(t, tasks); got != 0 {
		t.Fatalf("expected no task from a legacy flat message, got %d", got)
	}

	// Handle (the Run path) must fail the same way, without retrying, so
	// Run routes it to the dead-letter sink.
	msg := kafkago.Message{Topic: "warehouse.work-planning.events", Value: legacyFlatWorkReleasedJSON("evt-flat-2", "PICK", "wu-flat")}
	if err := c.Handle(context.Background(), msg); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Fatalf("Handle err = %v, want ErrNotCloudEvent", err)
	}
	if got := totalPending(t, tasks); got != 0 {
		t.Fatalf("expected no task from a legacy flat message, got %d", got)
	}
}

// An unrecognized specversion value must fail soft — return an error
// wrapping cloudevents.ErrNotCloudEvent (dead-lettered by Run) — never
// crash the process and never silently create a Task from an event this
// consumer cannot interpret.
func TestHandleMessage_MalformedSpecversion_FailsSoftWithoutCreatingTask(t *testing.T) {
	c, tasks := newConsumer(t)

	malformed := []byte(`{
		"specversion": "2.0",
		"id": "evt-bad-specversion",
		"type": "com.warehouse.wes.work-planning.workunit.WorkReleased",
		"source": "/warehouse/wes-work-planning",
		"subject": "wu-bad",
		"time": "2026-08-21T22:00:00Z",
		"datacontenttype": "application/json",
		"data": {"path_id": "PICK", "work_unit_id": "wu-bad", "cpt": "2026-08-21T23:00:00Z", "ref": "release-1"}
	}`)

	err := c.HandleMessage(context.Background(), malformed)
	if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Fatalf("err = %v, want ErrNotCloudEvent for an unsupported specversion", err)
	}
	if got := totalPending(t, tasks); got != 0 {
		t.Fatalf("expected no task to be created for an unsupported specversion, got %d", got)
	}
}
