package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// relocationGoldenFixture saves a claimed RELOCATE task (a Vacate whose
// destination was scanned) and returns the publisher over the repos.
func relocationGoldenFixture(t *testing.T) (*fakeWriter, *outboundkafka.Publisher) {
	t.Helper()
	tasks := memory.NewTaskRepo()
	move, err := task.NewRelocationMove("plan-7", 3, "Vacate", "SKU-1", "FWD-A", "")
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	tk := task.NewRelocationTask("task-rel-1", shared.NewCPT(goldenAt.Add(time.Hour)), "slot-plan-7-move-3", shared.NewCapabilitySet("relocate"), move)
	if err := tk.Claim("station-07", shared.NewCapabilitySet("relocate"), goldenAt.Add(-90*time.Second), 10*time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	_ = tasks.Save(context.Background(), tk)
	w := &fakeWriter{}
	return w, outboundkafka.NewPublisherWithWriter(w, tasks, memory.NewStationRepo(), goldenID1)
}

// TestGolden_Integration_TaskCompleted_Relocate pins the additive RELOCATE
// fields of TaskCompleted v1 (ADR 0047) byte-for-byte: sku, from_bin,
// to_bin, quantity and relocation_ref, with EXACTLY the names and JSON types
// inventory-storage's TaskCompleted consumer decodes (strings, an integer
// quantity, ref "<plan_id>:<move_seq>"). No order_ref / line_no: relocation
// work is not order work.
func TestGolden_Integration_TaskCompleted_Relocate(t *testing.T) {
	w, p := relocationGoldenFixture(t)
	ev := shared.NewRelocationTaskCompleted("task-rel-1", "station-07", goldenAt, shared.TaskRelocation{
		SKU: "SKU-1", FromBin: "FWD-A", ToBin: "RSV-2", Quantity: 12, Ref: "plan-7:3",
	})
	if err := p.Publish(context.Background(), ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(w.msgs))
	}
	assertGolden(t, w.msgs[0], "task-rel-1", `{
		"specversion":"1.0",
		"id":"`+goldenID+`",
		"source":"/warehouse/fulfillment-execution",
		"type":"com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
		"subject":"task-rel-1",
		"time":"2026-09-30T15:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:fulfillment-execution:events:TaskCompleted:v1",
		"data":{"task_id":"task-rel-1","station_id":"station-07","work_unit_id":"slot-plan-7-move-3","duration_seconds":90,"task_type":"RELOCATE",
			"sku":"SKU-1","from_bin":"FWD-A","to_bin":"RSV-2","quantity":12,"relocation_ref":"plan-7:3"}
	}`)
}

// inventoryStorageTaskCompleted is a verbatim copy of inventory-storage's
// taskCompletedData (internal/adapters/inbound/kafka/task_completed_consumer.go
// on its develop): the consumer side of the contract this encoder must meet.
type inventoryStorageTaskCompleted struct {
	TaskID        string `json:"task_id"`
	TaskType      string `json:"task_type"`
	OrderRef      string `json:"order_ref"`
	LineNo        *int   `json:"line_no"`
	SKU           string `json:"sku"`
	FromBin       string `json:"from_bin"`
	ToBin         string `json:"to_bin"`
	Quantity      *int   `json:"quantity"`
	RelocationRef string `json:"relocation_ref"`
}

// TestTaskCompleted_RelocateDecodesAsInventoryStorageReadsIt round-trips
// the encoded data through the consumer's own struct shape.
func TestTaskCompleted_RelocateDecodesAsInventoryStorageReadsIt(t *testing.T) {
	w, p := relocationGoldenFixture(t)
	ev := shared.NewRelocationTaskCompleted("task-rel-1", "station-07", goldenAt, shared.TaskRelocation{
		SKU: "SKU-1", FromBin: "FWD-A", ToBin: "RSV-2", Quantity: 12, Ref: "plan-7:3",
	})
	if err := p.Publish(context.Background(), ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	var envelope struct {
		Data inventoryStorageTaskCompleted `json:"data"`
	}
	if err := json.Unmarshal(w.msgs[0].Value, &envelope); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := envelope.Data
	if got.TaskType != "RELOCATE" || got.SKU != "SKU-1" || got.FromBin != "FWD-A" || got.ToBin != "RSV-2" || got.RelocationRef != "plan-7:3" {
		t.Fatalf("decoded = %+v", got)
	}
	if got.Quantity == nil || *got.Quantity != 12 {
		t.Fatalf("quantity = %v, want 12", got.Quantity)
	}
	if got.OrderRef != "" || got.LineNo != nil {
		t.Fatalf("relocation work carries no order_ref/line_no, got %q / %v", got.OrderRef, got.LineNo)
	}
}

// TestTaskCompleted_NonRelocationOmitsTheRelocationFields: the additive
// fields are absent (not "" / 0) on every other task's TaskCompleted, so
// the existing goldens stay byte-identical.
func TestTaskCompleted_NonRelocationOmitsTheRelocationFields(t *testing.T) {
	tasks := memory.NewTaskRepo()
	tk := task.New("task-p", task.Pick, shared.NewCPT(goldenAt.Add(time.Hour)), "wu-p", shared.NewCapabilitySet("pick"), false, false)
	_ = tasks.Save(context.Background(), tk)
	w := &fakeWriter{}
	p := outboundkafka.NewPublisherWithWriter(w, tasks, nil, goldenID1)
	if err := p.Publish(context.Background(), shared.NewTaskCompleted("task-p", "s1", goldenAt)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	var envelope struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(w.msgs[0].Value, &envelope); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, k := range []string{"sku", "from_bin", "to_bin", "quantity", "relocation_ref"} {
		if _, ok := envelope.Data[k]; ok {
			t.Errorf("non-RELOCATE TaskCompleted must omit %q, data = %v", k, envelope.Data)
		}
	}
}
