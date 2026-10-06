package kafka_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

var transferGoldenAt = time.Date(2026, 10, 6, 15, 4, 5, 0, time.UTC)

func transferGoldenDetails() shared.TaskTransferDetails {
	return shared.TaskTransferDetails{
		TransferRef: "tr-9f2a",
		DemandId:    "demand-77",
		WorkUnitId:  "wu-tr-8a1f",
		WorkKind:    "TRANSFER_DISPATCH",
		SiteId:      "site-north",
		SKU:         "SKU-0042",
		Quantity:    12,
	}
}

func transferFactTask(t *testing.T, kind task.WorkKind, taskType task.Type) *task.Task {
	t.Helper()
	tr := transferGoldenDetails()
	details := &task.TransferDetails{
		TransferRef: tr.TransferRef,
		DemandId:    tr.DemandId,
		WorkKind:    kind,
		SiteId:      tr.SiteId,
		SKU:         tr.SKU,
		Quantity:    tr.Quantity,
	}
	tk := task.NewTransferTask("task-tr-8a1f", taskType, shared.NewCPT(transferGoldenAt.Add(time.Hour)), "wu-tr-8a1f", shared.NewCapabilitySet(shared.Capability(strings.ToLower(string(taskType)))), false, false, *details)
	if err := tk.Claim("station-07", shared.NewCapabilitySet(shared.Capability(strings.ToLower(string(taskType)))), transferGoldenAt.Add(-time.Minute), 10*time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	return tk
}

// assertTransferGolden publishes the fact for a claimed transfer task and
// asserts the exact envelope: type, dataschema, subject/key = task_id,
// time = completion time, and the full snake_case payload.
func assertTransferGolden(t *testing.T, tk *task.Task, fact shared.DomainEvent, wantType, wantSchema string) {
	t.Helper()
	tasks := memory.NewTaskRepo()
	if err := tasks.Save(context.Background(), tk); err != nil {
		t.Fatalf("save task: %v", err)
	}
	w := &fakeWriter{}
	p := outboundkafka.NewPublisherWithWriter(w, tasks, memory.NewStationRepo(), goldenID1)
	if err := p.Publish(context.Background(), fact); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 1 {
		t.Fatalf("expected exactly 1 wire message, got %d", len(w.msgs))
	}
	msg := w.msgs[0]
	if string(msg.Key) != "task-tr-8a1f" {
		t.Fatalf("key = %q, want the task id", msg.Key)
	}
	if !hasContentTypeHeader(msg.Headers) {
		t.Fatalf("missing content-type header, got %v", msg.Headers)
	}
	env := decodeTransferEnvelope(t, msg.Value)
	if env.Type != wantType {
		t.Fatalf("type = %q, want %q", env.Type, wantType)
	}
	if env.DataSchema != wantSchema {
		t.Fatalf("dataschema = %q, want %q", env.DataSchema, wantSchema)
	}
	if env.Subject != "task-tr-8a1f" {
		t.Fatalf("subject = %q, want the task id", env.Subject)
	}
	if env.Time != "2026-10-06T15:04:05Z" {
		t.Fatalf("time = %q, want the completion time", env.Time)
	}
	assertTransferPayload(t, env.Data)
}

type transferEnvelope struct {
	Type       string          `json:"type"`
	DataSchema string          `json:"dataschema"`
	Subject    string          `json:"subject"`
	Time       string          `json:"time"`
	ID         string          `json:"id"`
	Data       json.RawMessage `json:"data"`
}

func decodeTransferEnvelope(t *testing.T, value []byte) transferEnvelope {
	t.Helper()
	var env transferEnvelope
	if err := json.Unmarshal(value, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v (%s)", err, value)
	}
	return env
}

func assertTransferPayload(t *testing.T, raw json.RawMessage) {
	t.Helper()
	var data struct {
		TransferRef string `json:"transfer_ref"`
		DemandId    string `json:"demand_id"`
		WorkUnitId  string `json:"work_unit_id"`
		TaskId      string `json:"task_id"`
		WorkKind    string `json:"work_kind"`
		SiteId      string `json:"site_id"`
		SKU         string `json:"sku"`
		Quantity    int    `json:"quantity"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatalf("unmarshal data: %v (%s)", err, raw)
	}
	if data.TransferRef != "tr-9f2a" || data.DemandId != "demand-77" || data.WorkUnitId != "wu-tr-8a1f" ||
		data.TaskId != "task-tr-8a1f" || data.WorkKind != "TRANSFER_DISPATCH" || data.SiteId != "site-north" ||
		data.SKU != "SKU-0042" || data.Quantity != 12 {
		t.Fatalf("data payload = %+v", data)
	}
}

func TestGolden_Integration_TransferPicked(t *testing.T) {
	tk := transferFactTask(t, task.WorkKindTransferPick, task.Pick)
	assertTransferGolden(t, tk, shared.NewTransferPicked("task-tr-8a1f", transferGoldenDetails(), transferGoldenAt),
		"com.warehouse.wes.fulfillment-execution.transfer.TransferPicked",
		"urn:warehouse:fulfillment-execution:events:TransferPicked:v1")
}

func TestGolden_Integration_TransferDispatched(t *testing.T) {
	tk := transferFactTask(t, task.WorkKindTransferDispatch, task.Dispatch)
	assertTransferGolden(t, tk, shared.NewTransferDispatched("task-tr-8a1f", transferGoldenDetails(), transferGoldenAt),
		"com.warehouse.wes.fulfillment-execution.transfer.TransferDispatched",
		"urn:warehouse:fulfillment-execution:events:TransferDispatched:v1")
}

func TestGolden_Integration_TransferArrived(t *testing.T) {
	tk := transferFactTask(t, task.WorkKindTransferArrival, task.Arrival)
	assertTransferGolden(t, tk, shared.NewTransferArrived("task-tr-8a1f", transferGoldenDetails(), transferGoldenAt),
		"com.warehouse.wes.fulfillment-execution.transfer.TransferArrived",
		"urn:warehouse:fulfillment-execution:events:TransferArrived:v1")
}
