package kafka_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"

	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// Encode is the half of Publish the transactional outbox persists (ADR
// 0020): it must produce exactly the bytes Publish would have written —
// topic, key, CloudEvents type and the enriched CloudEvent — without touching the
// writer at all.

func TestPublisher_Encode_ProducesIntegrationMessageWithoutWriting(t *testing.T) {
	tasks := memory.NewTaskRepo()
	stations := memory.NewStationRepo()
	tk := task.New("task-1", task.Pick, shared.NewCPT(epoch.Add(time.Hour)), "wu-1", shared.NewCapabilitySet("pick"), false, false)
	_ = tk.Claim("station-1", shared.NewCapabilitySet("pick"), epoch, 5*time.Minute)
	completedAt := epoch.Add(30 * time.Second)
	_ = tk.Complete("station-1", completedAt)
	_ = tasks.Save(context.Background(), tk)
	st := station.New("station-1", shared.NewCapabilitySet("pick"))
	_ = st.CheckIn("worker-7")
	_ = stations.Save(context.Background(), st)

	// A nil Writer proves Encode never writes.
	p := outboundkafka.NewPublisherWithWriter(nil, tasks, stations, func() string { return "evt-1" })

	encoded, err := p.Encode(context.Background(),
		shared.NewTaskCreated("task-1", epoch), // not in the integration contract: skipped
		shared.NewTaskCompleted("task-1", "station-1", completedAt),
	)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("expected 1 encoded message (TaskCompleted only), got %d", len(encoded))
	}
	enc := encoded[0]
	if enc.Topic != outboundkafka.Topic {
		t.Errorf("Topic = %q, want %q", enc.Topic, outboundkafka.Topic)
	}
	if enc.EventType != "com.warehouse.wes.fulfillment-execution.task.TaskCompleted" {
		t.Errorf("EventType = %q", enc.EventType)
	}
	if string(enc.Key) != "task-1" {
		t.Errorf("Key = %q, want task-1", enc.Key)
	}
	env, data := decodeCE[outboundkafka.TaskCompletedData](t, enc.Value)
	if env.ID() != "evt-1" || env.Source() != "/warehouse/fulfillment-execution" || !env.Time().Equal(completedAt) {
		t.Errorf("envelope = %+v", env)
	}
	if data.WorkUnitId != "wu-1" || data.AssociateId != "worker-7" || data.DurationSeconds != 30 {
		t.Errorf("enrichment: work_unit_id=%q associate_id=%q duration=%d", data.WorkUnitId, data.AssociateId, data.DurationSeconds)
	}
	// No span is active on ctx, so the only header is the CloudEvents
	// structured-mode content-type — no trace headers.
	if len(enc.Headers) != 1 || !hasContentTypeHeader(enc.Headers) {
		t.Errorf("expected only the content-type header, got %v", enc.Headers)
	}
}

func TestPublisher_Encode_InjectsTraceHeadersWhenSpanActive(t *testing.T) {
	installRecorder(t)
	tasks := memory.NewTaskRepo()
	newTestTask(t, tasks, "wu-1")
	p := outboundkafka.NewPublisherWithWriter(nil, tasks, nil, func() string { return "evt-1" })

	ctx, span := otel.Tracer("caller").Start(context.Background(), "CompleteTask")
	defer span.End()

	encoded, err := p.Encode(ctx, shared.NewTaskCompleted("task-1", "station-1", epoch))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("expected 1 encoded message, got %d", len(encoded))
	}
	headers := encoded[0].Headers
	var traceparent string
	for _, h := range headers {
		if h.Key == "traceparent" {
			traceparent = string(h.Value)
		}
	}
	if traceparent == "" {
		t.Fatalf("expected a traceparent header, got %v", headers)
	}
}

func TestPublisher_Encode_PropagatesEnrichmentLookupError(t *testing.T) {
	p := outboundkafka.NewPublisherWithWriter(nil, failingTaskRepo{}, nil, func() string { return "evt-1" })
	if _, err := p.Encode(context.Background(), shared.NewTaskCompleted("task-1", "station-1", epoch)); err == nil {
		t.Fatal("expected the task lookup error to propagate")
	}
}

// TaskCPTMissed (ADR-0025) needs no repo enrichment: every wire field
// comes straight off the domain event.
func TestPublisher_Encode_ProducesTaskCPTMissedMessageWithoutWriting(t *testing.T) {
	p := outboundkafka.NewPublisherWithWriter(nil, nil, nil, func() string { return "evt-cpt" })
	cpt := epoch.Add(-30 * time.Minute)

	encoded, err := p.Encode(context.Background(), shared.NewTaskCPTMissed("task-1", "order-1", "PICK", cpt, epoch))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("expected 1 encoded message, got %d", len(encoded))
	}
	enc := encoded[0]
	if enc.Topic != outboundkafka.Topic {
		t.Errorf("Topic = %q, want %q", enc.Topic, outboundkafka.Topic)
	}
	if enc.EventType != "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" {
		t.Errorf("EventType = %q", enc.EventType)
	}
	if string(enc.Key) != "task-1" {
		t.Errorf("Key = %q, want task-1", enc.Key)
	}
	env, data := decodeCE[outboundkafka.TaskCPTMissedData](t, enc.Value)
	if env.ID() != "evt-cpt" || env.Source() != "/warehouse/fulfillment-execution" || !env.Time().Equal(epoch) {
		t.Errorf("envelope = %+v", env)
	}
	if data.TaskId != "task-1" || data.OrderRef != "order-1" || data.TaskType != "PICK" || !data.Cpt.Equal(cpt) {
		t.Errorf("data = %+v", data)
	}
}

// PackageManifested (ADR-0025) needs no repo enrichment either.
func TestPublisher_Encode_ProducesPackageManifestedMessageWithoutWriting(t *testing.T) {
	p := outboundkafka.NewPublisherWithWriter(nil, nil, nil, func() string { return "evt-manifest" })

	encoded, err := p.Encode(context.Background(), shared.NewPackageManifested("pkg-1", "order-1", epoch))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("expected 1 encoded message, got %d", len(encoded))
	}
	enc := encoded[0]
	if enc.Topic != outboundkafka.Topic {
		t.Errorf("Topic = %q, want %q", enc.Topic, outboundkafka.Topic)
	}
	if enc.EventType != "com.warehouse.wes.fulfillment-execution.package.PackageManifested" {
		t.Errorf("EventType = %q", enc.EventType)
	}
	if string(enc.Key) != "pkg-1" {
		t.Errorf("Key = %q, want pkg-1", enc.Key)
	}
	env, data := decodeCE[outboundkafka.PackageManifestedData](t, enc.Value)
	if env.ID() != "evt-manifest" || env.Source() != "/warehouse/fulfillment-execution" || !env.Time().Equal(epoch) {
		t.Errorf("envelope = %+v", env)
	}
	if data.PackageId != "pkg-1" || data.OrderRef != "order-1" {
		t.Errorf("data = %+v", data)
	}
}

// A LabelApplied event (in the analytics contract but not the integration
// one) is still skipped by the integration Publisher/Encode, unaffected by
// the TaskCPTMissed/PackageManifested widening.
func TestPublisher_Encode_StillSkipsEventsOutsideTheIntegrationContract(t *testing.T) {
	p := outboundkafka.NewPublisherWithWriter(nil, nil, nil, func() string { return "evt-1" })
	encoded, err := p.Encode(context.Background(), shared.NewLabelApplied("pkg-1", epoch))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 0 {
		t.Fatalf("expected 0 encoded messages, got %d", len(encoded))
	}
}

func TestAnalyticsPublisher_Encode_ProducesOneMessagePerContractEvent(t *testing.T) {
	p := outboundkafka.NewAnalyticsPublisherWithWriter(nil, fakeTaskRepo{taskType: task.Pack, found: true}, func() string { return "evt-a" })

	encoded, err := p.Encode(context.Background(),
		shared.NewTaskCreated("t1", epoch),
		unknownEvent{},
		shared.NewPackageSealed("p1", epoch),
	)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) != 2 {
		t.Fatalf("expected 2 encoded messages (unknown event skipped), got %d", len(encoded))
	}
	if encoded[0].Topic != outboundkafka.AnalyticsTopic || encoded[1].Topic != outboundkafka.AnalyticsTopic {
		t.Errorf("topics = %q, %q; want %q", encoded[0].Topic, encoded[1].Topic, outboundkafka.AnalyticsTopic)
	}
	if encoded[0].EventType != "com.warehouse.wes.fulfillment-execution.task.TaskCreated" || string(encoded[0].Key) != "t1" {
		t.Errorf("first = %s/%s, want TaskCreated/t1", encoded[0].EventType, encoded[0].Key)
	}
	if encoded[1].EventType != "com.warehouse.wes.fulfillment-execution.package.PackageSealed" || string(encoded[1].Key) != "p1" {
		t.Errorf("second = %s/%s, want PackageSealed/p1", encoded[1].EventType, encoded[1].Key)
	}
	env, data := decodeCE[map[string]any](t, encoded[0].Value)
	if env.ID() != "evt-a" || env.DataSchema() != "urn:warehouse:fulfillment-execution:analytics:TaskCreated:v1" || env.Source() != "/warehouse/fulfillment-execution" {
		t.Errorf("envelope = %+v", env)
	}
	if data["task_type"] != "PACK" {
		t.Errorf("task_type enrichment = %v, want PACK", data["task_type"])
	}
}

// RelaySink is the wire half of the outbox relay: each Encoded is routed
// to its own topic through one topic-less writer, in one write.

func TestRelaySink_SendsEachMessageOnItsOwnTopicInOneWrite(t *testing.T) {
	w := &batchRecordingWriter{}
	sink := outboundkafka.NewRelaySinkWithWriter(w)

	err := sink.Send(context.Background(),
		outboundkafka.Encoded{Topic: outboundkafka.Topic, EventType: "TaskCompleted", Key: []byte("t1"), Value: []byte("a"), Headers: []kafkago.Header{{Key: "traceparent", Value: []byte("00-x")}}},
		outboundkafka.Encoded{Topic: outboundkafka.AnalyticsTopic, EventType: "TaskCompleted", Key: []byte("t1"), Value: []byte("b")},
	)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if w.calls != 1 {
		t.Fatalf("expected one WriteMessages call, got %d", w.calls)
	}
	if len(w.msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(w.msgs))
	}
	if w.msgs[0].Topic != outboundkafka.Topic || w.msgs[1].Topic != outboundkafka.AnalyticsTopic {
		t.Errorf("topics = %q, %q", w.msgs[0].Topic, w.msgs[1].Topic)
	}
	if string(w.msgs[0].Key) != "t1" || string(w.msgs[0].Value) != "a" || len(w.msgs[0].Headers) != 1 {
		t.Errorf("message 0 = %+v", w.msgs[0])
	}
}

func TestRelaySink_RejectsTopiclessMessageBeforeWriting(t *testing.T) {
	w := &batchRecordingWriter{}
	sink := outboundkafka.NewRelaySinkWithWriter(w)
	if err := sink.Send(context.Background(), outboundkafka.Encoded{EventType: "X", Value: []byte("v")}); err == nil {
		t.Fatal("expected an error for a message without a topic")
	}
	if w.calls != 0 {
		t.Fatalf("nothing should have been written, got %d calls", w.calls)
	}
}

func TestRelaySink_EmptySendIsNoop(t *testing.T) {
	w := &batchRecordingWriter{}
	if err := outboundkafka.NewRelaySinkWithWriter(w).Send(context.Background()); err != nil || w.calls != 0 {
		t.Fatalf("err=%v calls=%d", err, w.calls)
	}
}

func TestRelaySink_WrapsWriterError(t *testing.T) {
	sink := outboundkafka.NewRelaySinkWithWriter(failingWriter{})
	err := sink.Send(context.Background(), outboundkafka.Encoded{Topic: "t", Value: []byte("v")})
	if err == nil || !strings.Contains(err.Error(), "broker unavailable") {
		t.Fatalf("expected the writer error to be wrapped, got %v", err)
	}
}

// batchRecordingWriter counts WriteMessages calls so the "one write per
// Send" guarantee can be asserted.
type batchRecordingWriter struct {
	calls int
	msgs  []kafkago.Message
}

func (w *batchRecordingWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	w.calls++
	w.msgs = append(w.msgs, msgs...)
	return nil
}

// failingTaskRepo errors on FindById so Encode's enrichment error path can
// be exercised.
type failingTaskRepo struct{ fakeTaskRepo }

func (failingTaskRepo) FindById(context.Context, shared.TaskId) (*task.Task, error) {
	return nil, errors.New("db down")
}
