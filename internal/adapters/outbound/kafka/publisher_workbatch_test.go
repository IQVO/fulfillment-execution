package kafka

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
	"github.com/claudioed/fulfillment-execution/internal/domain/workbatch"
)

// wbNow is the fixed instant every workbatch wire test uses.
var wbNow = time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC)

// wbFormedEvent builds a WorkBatchFormed with two members, stop order 1, 2.
func wbFormedEvent() shared.WorkBatchFormed {
	return shared.NewWorkBatchFormed("wb-1", "stn-1", wbNow,
		[]shared.WorkBatchMemberFact{
			{TaskId: "t-1", StopSeq: 1, Compartment: 1},
			{TaskId: "t-2", StopSeq: 2, Compartment: 2},
		}, 1, wbNow)
}

// TestEncode_WorkBatchFormed_BothTopics_ByteIdenticalData: the event lands
// on the integration topic AND the analytics topic, and the `data` bytes
// are identical on both (ADR 0063; same convention as the promoted
// exception events).
func TestEncode_WorkBatchFormed_BothTopics_ByteIdenticalData(t *testing.T) {
	integration := &Publisher{NewId: func() string { return "id-1" }}
	analytics := &AnalyticsPublisher{NewId: func() string { return "id-2" }}
	ev := wbFormedEvent()

	encs, err := integration.Encode(context.Background(), ev)
	if err != nil {
		t.Fatalf("integration encode: %v", err)
	}
	if len(encs) != 1 || encs[0].Topic != Topic {
		t.Fatalf("integration encodings = %+v, want one on %s", encs, Topic)
	}
	if encs[0].EventType != "com.warehouse.wes.fulfillment-execution.workbatch.WorkBatchFormed" {
		t.Fatalf("type = %s", encs[0].EventType)
	}
	if string(encs[0].Key) != "wb-1" {
		t.Fatalf("key = %s, want the batch id", encs[0].Key)
	}

	aencs, err := analytics.Encode(context.Background(), ev)
	if err != nil {
		t.Fatalf("analytics encode: %v", err)
	}
	if len(aencs) != 1 || aencs[0].Topic != AnalyticsTopic {
		t.Fatalf("analytics encodings = %+v, want one on %s", aencs, AnalyticsTopic)
	}
	if integrationData, analyticsData := dataOf(t, encs[0].Value), dataOf(t, aencs[0].Value); string(integrationData) != string(analyticsData) {
		t.Fatalf("data bytes differ:\n integration %s\n analytics   %s", integrationData, analyticsData)
	}
}

// TestEncode_WorkBatchCompletedAndAbandoned_BothTopics: the terminal events
// land on both topics with their walk_seconds / member_count facts.
func TestEncode_WorkBatchCompletedAndAbandoned_BothTopics(t *testing.T) {
	integration := &Publisher{NewId: func() string { return "id-1" }}
	analytics := &AnalyticsPublisher{NewId: func() string { return "id-2" }}
	completed := shared.NewWorkBatchCompleted("wb-1", "stn-1", 2, 1200, 3, wbNow.Add(20*time.Minute))
	abandoned := shared.NewWorkBatchAbandoned("wb-2", "stn-2", 1, 2, wbNow.Add(6*time.Minute))

	for _, ev := range []shared.DomainEvent{completed, abandoned} {
		encs, err := integration.Encode(context.Background(), ev)
		if err != nil || len(encs) != 1 {
			t.Fatalf("integration encode %s: %v, %d encodings", ev.EventName(), err, len(encs))
		}
		aencs, err := analytics.Encode(context.Background(), ev)
		if err != nil || len(aencs) != 1 {
			t.Fatalf("analytics encode %s: %v, %d encodings", ev.EventName(), err, len(aencs))
		}
		if string(dataOf(t, encs[0].Value)) != string(dataOf(t, aencs[0].Value)) {
			t.Fatalf("%s data bytes differ between topics", ev.EventName())
		}
	}
	var formed struct {
		WalkSeconds int64 `json:"walk_seconds"`
		MemberCount int   `json:"member_count"`
	}
	if err := json.Unmarshal(dataOf(t, mustEncodeOne(t, integration, completed)), &formed); err != nil {
		t.Fatalf("decode completed: %v", err)
	}
	if formed.WalkSeconds != 1200 || formed.MemberCount != 2 {
		t.Fatalf("completed payload = %+v", formed)
	}
}

// TestEncode_TaskCompleted_BatchFieldsOmittedWithoutBatchRepo: with no batch
// repo wired (the pre-batching configuration) the payload bytes carry no
// batch_id/batch_size at all.
func TestEncode_TaskCompleted_BatchFieldsOmittedWithoutBatchRepo(t *testing.T) {
	tasks := memory.NewTaskRepo()
	tk := task.New("t-1", task.Pick, shared.NewCPT(wbNow.Add(time.Hour)), "wu-1", shared.NewCapabilitySet(), false, false)
	if err := tasks.Save(context.Background(), tk); err != nil {
		t.Fatalf("seed: %v", err)
	}
	p := &Publisher{Tasks: tasks, NewId: func() string { return "id-1" }}
	raw := mustEncodeOne(t, p, shared.NewTaskCompleted("t-1", "stn-1", wbNow))
	var payload map[string]any
	if err := json.Unmarshal(dataOf(t, raw), &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, present := payload["batch_id"]; present {
		t.Fatal("batch_id present without a batch repo")
	}
	if _, present := payload["batch_size"]; present {
		t.Fatal("batch_size present without a batch repo")
	}
}

// TestEncode_TaskCompleted_CarriesBatchFields: with the batch repo wired and
// the completed task a member of a batch, TaskCompleted carries batch_id
// and batch_size (ADR 0063), including for the LAST member — whose batch
// has just flipped to COMPLETED in the same transaction.
func TestEncode_TaskCompleted_CarriesBatchFields(t *testing.T) {
	tasks := memory.NewTaskRepo()
	batches := memory.NewWorkBatchRepo()
	tk := task.New("t-1", task.Pick, shared.NewCPT(wbNow.Add(time.Hour)), "wu-1", shared.NewCapabilitySet(), false, false)
	if err := tasks.Save(context.Background(), tk); err != nil {
		t.Fatalf("seed task: %v", err)
	}
	b, err := workbatch.Form("wb-1", "stn-1", 4, tk.CPT().Time(), []workbatch.Member{{TaskId: "t-1", Cpt: tk.CPT().Time(), FromBin: "A-01"}}, wbNow)
	if err != nil {
		t.Fatalf("form: %v", err)
	}
	if err := b.CompleteMember("t-1", wbNow.Add(3*time.Minute)); err != nil {
		t.Fatalf("complete member: %v", err)
	}
	if err := batches.Save(context.Background(), b); err != nil {
		t.Fatalf("seed batch: %v", err)
	}

	p := &Publisher{Tasks: tasks, Batches: batches, NewId: func() string { return "id-1" }}
	raw := mustEncodeOne(t, p, shared.NewTaskCompleted("t-1", "stn-1", wbNow.Add(3*time.Minute)))
	var payload struct {
		BatchId   string `json:"batch_id"`
		BatchSize int    `json:"batch_size"`
	}
	if err := json.Unmarshal(dataOf(t, raw), &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if payload.BatchId != "wb-1" || payload.BatchSize != 1 {
		t.Fatalf("batch fields = %+v, want wb-1/1", payload)
	}
}

// dataOf extracts the CloudEvents data object from a structured-mode value.
func dataOf(t *testing.T, raw []byte) json.RawMessage {
	t.Helper()
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	return envelope.Data
}

func mustEncodeOne(t *testing.T, e interface {
	Encode(context.Context, ...shared.DomainEvent) ([]Encoded, error)
}, ev shared.DomainEvent) []byte {
	t.Helper()
	encs, err := e.Encode(context.Background(), ev)
	if err != nil || len(encs) != 1 {
		t.Fatalf("encode %s: %v (%d)", ev.EventName(), err, len(encs))
	}
	return encs[0].Value
}
