package http_test

import (
	"context"
	"encoding/json"
	stdhttp "net/http"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

const problemBase = "https://errors.fulfillment-execution.warehouse-systems.dev/"

// seedRelocation stores a Pending RELOCATE task "rel-1" (as the WorkReleased
// consumer would) and registers station s1 with the relocate capability.
func seedRelocation(t *testing.T, tasks *memory.TaskRepo, stations *memory.StationRepo, clock *memory.FixedClock, kind, from, to string) {
	t.Helper()
	move, err := task.NewRelocationMove("plan-7", 3, kind, "SKU-1", from, to)
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	tk := task.NewRelocationTask("rel-1", shared.NewCPT(clock.Now().Add(time.Hour)), "slot-plan-7-move-3", shared.NewCapabilitySet("relocate"), move)
	if err := tasks.Save(context.TODO(), tk); err != nil {
		t.Fatalf("save: %v", err)
	}
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("relocate", "pick")))
}

func claimRelocate(t *testing.T, srv stdhttp.Handler) map[string]any {
	t.Helper()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{"taskType": "RELOCATE"})
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("claim-next RELOCATE: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return body
}

// TestRelocate_ClaimShowsTheMove: a station claims RELOCATE work (taskType
// RELOCATE is claimable) and the response shows the planned move, the open
// side omitted.
func TestRelocate_ClaimShowsTheMove(t *testing.T) {
	srv, tasks, stations, _, clock := newTestServer()
	seedRelocation(t, tasks, stations, clock, "Vacate", "FWD-A", "")

	body := claimRelocate(t, srv)
	if body["type"] != "RELOCATE" {
		t.Fatalf("type = %v, want RELOCATE", body["type"])
	}
	rel, ok := body["relocation"].(map[string]any)
	if !ok {
		t.Fatalf("claim response has no relocation object: %v", body)
	}
	want := map[string]any{"planId": "plan-7", "moveSeq": float64(3), "kind": "Vacate", "sku": "SKU-1", "fromBin": "FWD-A", "ref": "plan-7:3"}
	for k, v := range want {
		if rel[k] != v {
			t.Errorf("relocation.%s = %v, want %v", k, rel[k], v)
		}
	}
	if _, has := rel["toBin"]; has {
		t.Fatalf("the open side is omitted, got toBin %v", rel["toBin"])
	}
}

// TestRelocate_CompletionRecordsTheExecutedMove: completing with the
// confirmed quantity and the scanned open bin records the executed move,
// visible on the task read model.
func TestRelocate_CompletionRecordsTheExecutedMove(t *testing.T) {
	srv, tasks, stations, _, clock := newTestServer()
	seedRelocation(t, tasks, stations, clock, "Vacate", "FWD-A", "")
	claimRelocate(t, srv)

	rec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/rel-1/complete", map[string]any{"stationId": "s1", "quantity": 12, "toBin": "RSV-2"})
	if rec.Code != stdhttp.StatusNoContent {
		t.Fatalf("complete: expected 204, got %d: %s", rec.Code, rec.Body.String())
	}
	saved, _ := tasks.FindById(context.TODO(), "rel-1")
	want := task.RelocationResult{Quantity: 12, FromBin: "FWD-A", ToBin: "RSV-2"}
	if saved.Status() != task.Completed || saved.RelocationResult() == nil || *saved.RelocationResult() != want {
		t.Fatalf("saved = %s %+v, want COMPLETED %+v", saved.Status(), saved.RelocationResult(), want)
	}

	list := doJSON(t, srv, stdhttp.MethodGet, "/tasks?orderRef=slot-plan-7-move-3", nil)
	var tasksResp []struct {
		Relocation struct {
			Executed map[string]any `json:"executed"`
		} `json:"relocation"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &tasksResp); err != nil || len(tasksResp) != 1 {
		t.Fatalf("GET /tasks: %v %s", err, list.Body.String())
	}
	executed := tasksResp[0].Relocation.Executed
	if executed["quantity"] != float64(12) || executed["fromBin"] != "FWD-A" || executed["toBin"] != "RSV-2" {
		t.Fatalf("executed = %v", executed)
	}
}

// TestRelocate_CompletionRejections maps every relocation-confirmation error
// to a 422 problem and leaves the task Claimed.
func TestRelocate_CompletionRejections(t *testing.T) {
	cases := []struct {
		name string
		body map[string]any
		slug string
	}{
		{"no inputs", map[string]any{"stationId": "s1"}, "relocation-confirmation-required"},
		{"open source not scanned", map[string]any{"stationId": "s1", "quantity": 4}, "invalid-relocation-confirmation"},
		{"scan differs from the plan", map[string]any{"stationId": "s1", "quantity": 4, "fromBin": "RSV-1", "toBin": "OTHER"}, "invalid-relocation-confirmation"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, tasks, stations, _, clock := newTestServer()
			seedRelocation(t, tasks, stations, clock, "Assign", "", "FWD-B")
			claimRelocate(t, srv)
			rec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/rel-1/complete", tc.body)
			assertProblemDetails(t, rec, stdhttp.StatusUnprocessableEntity, problemBase+tc.slug, "/tasks/rel-1/complete")
			saved, _ := tasks.FindById(context.TODO(), "rel-1")
			if saved.Status() != task.Claimed {
				t.Fatalf("status = %s, want CLAIMED", saved.Status())
			}
		})
	}
}

// TestRelocate_InputsOnANonRelocateTaskAre422 and a PICK still completes
// without them.
func TestRelocate_InputsOnANonRelocateTaskAre422(t *testing.T) {
	srv, _, stations, _, clock := newTestServer()
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("pick")))
	doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "PICK", "cpt": clock.Now().Add(time.Hour), "orderRef": "order-1", "requiredCapabilities": []string{"pick"},
	})
	claim := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/claim-next", map[string]any{"taskType": "PICK"})
	var claimed struct {
		Id         string         `json:"id"`
		Relocation map[string]any `json:"relocation"`
	}
	_ = json.Unmarshal(claim.Body.Bytes(), &claimed)
	if claimed.Relocation != nil {
		t.Fatalf("a PICK task has no relocation object, got %v", claimed.Relocation)
	}
	rec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/"+claimed.Id+"/complete", map[string]any{"stationId": "s1", "quantity": 3})
	assertProblemDetails(t, rec, stdhttp.StatusUnprocessableEntity, problemBase+"not-relocation-task", "/tasks/"+claimed.Id+"/complete")
	ok := doJSON(t, srv, stdhttp.MethodPost, "/tasks/"+claimed.Id+"/complete", map[string]any{"stationId": "s1"})
	if ok.Code != stdhttp.StatusNoContent {
		t.Fatalf("PICK without inputs: expected 204, got %d", ok.Code)
	}
}

// TestCompleteTask_MalformedRelocationInputsAre400: values the contract
// forbids (quantity below 1, blank bins, wrong JSON types) never reach the
// use case.
func TestCompleteTask_MalformedRelocationInputsAre400(t *testing.T) {
	cases := map[string]map[string]any{
		"zero quantity":     {"stationId": "s1", "quantity": 0},
		"negative quantity": {"stationId": "s1", "quantity": -1},
		"blank fromBin":     {"stationId": "s1", "fromBin": " "},
		"blank toBin":       {"stationId": "s1", "toBin": ""},
		"string quantity":   {"stationId": "s1", "quantity": "12"},
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			srv, _, _, _, _ := newTestServer()
			rec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/any/complete", body)
			assertProblemDetails(t, rec, stdhttp.StatusBadRequest, problemBase+"invalid-request", "/tasks/any/complete")
		})
	}
}

// TestPostTask_RelocateIsNotCreatable: RELOCATE work needs a planned move,
// so POST /tasks keeps rejecting it (400), while claim-next accepts it.
func TestPostTask_RelocateIsNotCreatable(t *testing.T) {
	srv, _, _, _, clock := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "RELOCATE", "cpt": clock.Now().Add(time.Hour), "orderRef": "o", "requiredCapabilities": []string{"relocate"},
	})
	assertProblemDetails(t, rec, stdhttp.StatusBadRequest, problemBase+"invalid-request", "")
}
