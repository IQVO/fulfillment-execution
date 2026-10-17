package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/fulfillment-execution/internal/adapters/outbound/kafka"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

const (
	replSourceBin = "WH1-RSV-AMB-B02-03-02-C"
	replPickFace  = "WH1-FWD-AMB-A01-01-01-A"
)

// replenishTask builds a Pending REPLENISH task (24 units, NORMAL).
func replenishTask(t *testing.T, id shared.TaskId, from, to, priority string, cpt shared.CPT, caps shared.CapabilitySet) *task.Task {
	t.Helper()
	move, err := task.NewReplenishmentMove("req-9", "SKU-1", from, to, 24, priority)
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	return task.NewReplenishmentTask(id, cpt, "req-9", caps, move)
}

// replenishmentGoldenFixture saves a claimed REPLENISH task and returns the
// publisher over the repos.
func replenishmentGoldenFixture(t *testing.T) (*fakeWriter, *outboundkafka.Publisher) {
	t.Helper()
	tk := replenishTask(t, "task-rep-1", replSourceBin, replPickFace, "URGENT", shared.NewCPT(goldenAt.Add(time.Hour)), shared.NewCapabilitySet("replenish"))
	if err := tk.Claim("station-07", shared.NewCapabilitySet("replenish"), goldenAt.Add(-90*time.Second), 10*time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	return poolFixture(t, tk)
}

func replenishmentCompleted() shared.TaskCompleted {
	return shared.NewReplenishmentTaskCompleted("task-rep-1", "station-07", goldenAt, shared.TaskReplenishment{
		SKU: "SKU-1", FromBin: replSourceBin, ToBin: replPickFace, Quantity: 20, RequestRef: "req-9",
	})
}

// TestGolden_Integration_TaskCompleted_Replenish pins the REPLENISH shape of
// TaskCompleted v1 (ADR 0050) byte-for-byte: sku, from_bin, to_bin, quantity
// (the operator-confirmed 20, not the planned 24) and the additive
// request_ref, with the names and JSON types inventory-storage's consumer
// decodes. No order_ref / line_no / relocation_ref.
func TestGolden_Integration_TaskCompleted_Replenish(t *testing.T) {
	w, p := replenishmentGoldenFixture(t)
	publishOne(t, w, p, replenishmentCompleted())
	assertGolden(t, w.msgs[0], "task-rep-1", `{
		"specversion":"1.0",
		"id":"`+goldenID+`",
		"source":"/warehouse/fulfillment-execution",
		"type":"com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
		"subject":"task-rep-1",
		"time":"2026-09-30T15:00:00Z",
		"datacontenttype":"application/json",
		"dataschema":"urn:warehouse:fulfillment-execution:events:TaskCompleted:v1",
		"data":{"task_id":"task-rep-1","station_id":"station-07","work_unit_id":"req-9","duration_seconds":90,"task_type":"REPLENISH",
			"sku":"SKU-1","from_bin":"`+replSourceBin+`","to_bin":"`+replPickFace+`","quantity":20,"request_ref":"req-9"}
	}`)
}

// inventoryStorageReplenishCompleted is a field-for-field copy of
// inventory-storage's taskCompletedData (internal/adapters/inbound/kafka/
// task_completed_consumer.go on its feature/apply-replenish-task branch, ADR
// 0041): the consumer side of the contract this encoder must meet. It is
// restated here, never imported: if this test needs an edit, the consumer's
// contract changed.
type inventoryStorageReplenishCompleted struct {
	TaskID        string `json:"task_id"`
	TaskType      string `json:"task_type"`
	OrderRef      string `json:"order_ref"`
	LineNo        *int   `json:"line_no"`
	SKU           string `json:"sku"`
	FromBin       string `json:"from_bin"`
	ToBin         string `json:"to_bin"`
	Quantity      *int   `json:"quantity"`
	RelocationRef string `json:"relocation_ref"`
	RequestRef    string `json:"request_ref"`
}

var errMalformedReplenish = errors.New("malformed replenish task")

// inventoryStorageValidates restates the REQUIRED-field rules of
// inventory-storage's ApplyReplenishTask.replenishCommand that the encoder
// must satisfy (task_type REPLENISH; non-blank sku, from_bin, to_bin and
// request_ref; a quantity of at least 1; distinct bins).
func inventoryStorageValidates(d inventoryStorageReplenishCompleted) error {
	switch {
	case d.TaskType != "REPLENISH":
		return errMalformedReplenish
	case strings.TrimSpace(d.RequestRef) == "":
		return errMalformedReplenish
	case strings.TrimSpace(d.SKU) == "", strings.TrimSpace(d.FromBin) == "", strings.TrimSpace(d.ToBin) == "":
		return errMalformedReplenish
	case d.Quantity == nil || *d.Quantity < 1:
		return errMalformedReplenish
	case d.FromBin == d.ToBin:
		return errMalformedReplenish
	}
	return nil
}

// TestTaskCompleted_ReplenishDecodesAsInventoryStorageReadsIt round-trips the
// encoded data through the consumer's own struct shape and its required-field
// rules.
func TestTaskCompleted_ReplenishDecodesAsInventoryStorageReadsIt(t *testing.T) {
	w, p := replenishmentGoldenFixture(t)
	publishOne(t, w, p, replenishmentCompleted())
	var envelope struct {
		Type string                             `json:"type"`
		Data inventoryStorageReplenishCompleted `json:"data"`
	}
	if err := json.Unmarshal(w.msgs[0].Value, &envelope); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if envelope.Type != "com.warehouse.wes.fulfillment-execution.task.TaskCompleted" {
		t.Fatalf("type = %q", envelope.Type)
	}
	got := envelope.Data
	if got.TaskType != "REPLENISH" || got.SKU != "SKU-1" || got.FromBin != replSourceBin || got.ToBin != replPickFace || got.RequestRef != "req-9" {
		t.Fatalf("decoded = %+v", got)
	}
	if got.Quantity == nil || *got.Quantity != 20 {
		t.Fatalf("quantity = %v, want the confirmed 20", got.Quantity)
	}
	if got.OrderRef != "" || got.LineNo != nil || got.RelocationRef != "" {
		t.Fatalf("replenishment work carries no order_ref/line_no/relocation_ref, got %q / %v / %q", got.OrderRef, got.LineNo, got.RelocationRef)
	}
	if err := inventoryStorageValidates(got); err != nil {
		t.Fatalf("inventory-storage would reject the payload: %v", err)
	}
}

// TestTaskCompleted_NonReplenishOmitsRequestRef: request_ref is absent (not
// "") on every other task's TaskCompleted, including RELOCATE, so the
// existing goldens stay byte-identical.
func TestTaskCompleted_NonReplenishOmitsRequestRef(t *testing.T) {
	w, p := relocationGoldenFixture(t)
	ev := shared.NewRelocationTaskCompleted("task-rel-1", "station-07", goldenAt, shared.TaskRelocation{
		SKU: "SKU-1", FromBin: "FWD-A", ToBin: "RSV-2", Quantity: 12, Ref: "plan-7:3",
	})
	data := availableData(t, publishOne(t, w, p, ev))
	if _, ok := data["request_ref"]; ok {
		t.Fatalf("a RELOCATE TaskCompleted must omit request_ref, data = %v", data)
	}
	if data["relocation_ref"] != "plan-7:3" {
		t.Fatalf("relocation_ref lost: %v", data)
	}
}

// The invalid completions an inventory-storage consumer dead-letters can not
// be produced by this encoder's inputs: the validator catches each.
func TestInventoryStorageValidates_RestatesTheRequiredFields(t *testing.T) {
	q := 5
	good := inventoryStorageReplenishCompleted{TaskType: "REPLENISH", SKU: "S", FromBin: "A", ToBin: "B", Quantity: &q, RequestRef: "r"}
	if err := inventoryStorageValidates(good); err != nil {
		t.Fatalf("good payload rejected: %v", err)
	}
	zero := 0
	for name, mut := range map[string]func(*inventoryStorageReplenishCompleted){
		"no request_ref": func(d *inventoryStorageReplenishCompleted) { d.RequestRef = " " },
		"no sku":         func(d *inventoryStorageReplenishCompleted) { d.SKU = "" },
		"no from_bin":    func(d *inventoryStorageReplenishCompleted) { d.FromBin = "" },
		"no to_bin":      func(d *inventoryStorageReplenishCompleted) { d.ToBin = "" },
		"no quantity":    func(d *inventoryStorageReplenishCompleted) { d.Quantity = nil },
		"zero quantity":  func(d *inventoryStorageReplenishCompleted) { d.Quantity = &zero },
		"same bins":      func(d *inventoryStorageReplenishCompleted) { d.ToBin = d.FromBin },
		"other type":     func(d *inventoryStorageReplenishCompleted) { d.TaskType = "RELOCATE" },
	} {
		d := good
		mut(&d)
		if inventoryStorageValidates(d) == nil {
			t.Errorf("%s: want rejection", name)
		}
	}
}

// goldenReplenishTaskAvailable is the exact TaskAvailable of a REPLENISH
// task (ADR 0050): both bins are known, so source bin first, destination bin
// last. priority_band is the P2 derivation (type + CPT instant); the
// replenishment priority does not enter it.
const goldenReplenishTaskAvailable = `{
	"specversion":"1.0",
	"id":"` + goldenID + `",
	"source":"/warehouse/fulfillment-execution",
	"type":"` + typeTaskAvailable + `",
	"subject":"task-rep-1",
	"time":"2026-09-30T15:00:00Z",
	"datacontenttype":"application/json",
	"dataschema":"urn:warehouse:fulfillment-execution:events:TaskAvailable:v1",
	"data":{"task_id":"task-rep-1","task_type":"REPLENISH","site_id":"WH1",
		"from_location":"` + replSourceBin + `","to_location":"` + replPickFace + `",
		"required_capabilities":["forklift","replenish"],
		"cpt":"2026-09-30T16:00:00Z","priority_band":"REPLENISH:2026-09-30T16:00:00Z",
		"available_at":"2026-09-30T15:00:00Z"}
}`

func goldenReplenish(t *testing.T) *task.Task {
	t.Helper()
	return replenishTask(t, "task-rep-1", replSourceBin, replPickFace, "URGENT",
		shared.NewCPT(goldenAt.Add(time.Hour)), shared.NewCapabilitySet("replenish", "forklift"))
}

func TestGolden_Integration_TaskAvailable_Replenish(t *testing.T) {
	w, p := poolFixture(t, goldenReplenish(t))
	publishOne(t, w, p, shared.NewTaskAvailable("task-rep-1", goldenAt))
	assertGolden(t, w.msgs[0], "task-rep-1", goldenReplenishTaskAvailable)
}

func TestGolden_TaskAvailable_Replenish_DecodesAsTaskInterleavingExpects(t *testing.T) {
	e, got := decodeCE[consumerTaskAvailable](t, []byte(goldenReplenishTaskAvailable))
	if e.Type() != typeTaskAvailable {
		t.Fatalf("type = %q", e.Type())
	}
	if got.TaskID != "task-rep-1" || got.TaskType != "REPLENISH" || got.SiteID != "WH1" ||
		got.FromLocation != replSourceBin || got.ToLocation != replPickFace ||
		got.PriorityBand != "REPLENISH:2026-09-30T16:00:00Z" || len(got.RequiredCapabilities) != 2 {
		t.Fatalf("decoded = %+v", got)
	}
}

// NORMAL and URGENT requests with the same CPT are claimNext peers, so they
// publish the same band (ADR 0050: claimNext orders by CPT only).
func TestGolden_TaskAvailable_Replenish_PriorityDoesNotEnterTheBand(t *testing.T) {
	bands := map[string]string{}
	for _, priority := range []string{"NORMAL", "URGENT"} {
		tk := replenishTask(t, "task-rep-1", replSourceBin, replPickFace, priority, shared.NewCPT(goldenAt.Add(time.Hour)), shared.NewCapabilitySet("replenish"))
		w, p := poolFixture(t, tk)
		bands[priority] = availableData(t, publishOne(t, w, p, shared.NewTaskAvailable("task-rep-1", goldenAt)))["priority_band"].(string)
	}
	if bands["NORMAL"] != bands["URGENT"] {
		t.Fatalf("bands differ: %v", bands)
	}
}

// A REPLENISH task whose bins carry no determinable site is not announced.
func TestPublish_TaskAvailable_Replenish_NotAnnouncedWithoutSite(t *testing.T) {
	tk := replenishTask(t, "task-nosite", "BIN-A", "BIN-B", "NORMAL", shared.NewCPT(goldenAt), shared.NewCapabilitySet("replenish"))
	w, p := poolFixture(t, tk)
	if err := p.Publish(context.Background(), shared.NewTaskAvailable("task-nosite", goldenAt)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(w.msgs) != 0 {
		t.Fatalf("expected no TaskAvailable without a site, got %d", len(w.msgs))
	}
}

// TaskClaimed carries task_type REPLENISH like any other type.
func TestPublish_TaskClaimed_Replenish(t *testing.T) {
	w, p := poolFixture(t, goldenReplenish(t))
	data := availableData(t, publishOne(t, w, p, shared.NewTaskClaimed("task-rep-1", "s1", goldenAt)))
	if data["task_type"] != "REPLENISH" || data["station_id"] != "s1" {
		t.Fatalf("TaskClaimed data = %v", data)
	}
}

// Encode of a REPLENISH completion for a task that was never saved still
// yields the move fields (they come from the event), only the enrichment is
// absent.
func TestEncode_ReplenishCompletionWithoutTaskRow(t *testing.T) {
	w := &fakeWriter{}
	p := outboundkafka.NewPublisherWithWriter(w, memory.NewTaskRepo(), memory.NewStationRepo(), goldenID1)
	data := availableData(t, publishOne(t, w, p, replenishmentCompleted()))
	if data["request_ref"] != "req-9" || data["sku"] != "SKU-1" || data["quantity"] != float64(20) {
		t.Fatalf("data = %v", data)
	}
}
