package http_test

import (
	"context"
	"encoding/json"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

const (
	replSrc = "WH1-RSV-AMB-B02-03-02-C"
	replDst = "WH1-FWD-AMB-A01-01-01-A"
)

// seedReplenishment stores a Pending REPLENISH task "rep-1" (as the
// WorkReleased consumer would) and registers station s1 with the replenish
// capability.
func seedReplenishment(t *testing.T, tasks *memory.TaskRepo, stations *memory.StationRepo, clock *memory.FixedClock) {
	t.Helper()
	move, err := task.NewReplenishmentMove("req-9", "SKU-1", replSrc, replDst, 24, "URGENT")
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	tk := task.NewReplenishmentTask("rep-1", shared.NewCPT(clock.Now().Add(time.Hour)), "replenish-req-9", shared.NewCapabilitySet("replenish"), move)
	if err := tasks.Save(context.TODO(), tk); err != nil {
		t.Fatalf("save: %v", err)
	}
	_ = stations.Save(context.TODO(), station.New("s1", shared.NewCapabilitySet("replenish", "pick")))
}

func claimReplenish(t *testing.T, srv stdhttp.Handler) map[string]any {
	t.Helper()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/task-claims", map[string]any{"taskType": "REPLENISH"})
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("claim-next REPLENISH: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return body
}

// TestReplenish_ClaimShowsTheMove: a station claims REPLENISH work (taskType
// REPLENISH is claimable) and the response shows the planned move.
func TestReplenish_ClaimShowsTheMove(t *testing.T) {
	srv, tasks, stations, _, clock := newTestServer()
	seedReplenishment(t, tasks, stations, clock)

	body := claimReplenish(t, srv)
	if body["type"] != "REPLENISH" {
		t.Fatalf("type = %v, want REPLENISH", body["type"])
	}
	if _, has := body["relocation"]; has {
		t.Fatalf("a REPLENISH task has no relocation object: %v", body)
	}
	rep, ok := body["replenishment"].(map[string]any)
	if !ok {
		t.Fatalf("claim response has no replenishment object: %v", body)
	}
	want := map[string]any{"requestId": "req-9", "sku": "SKU-1", "fromBin": replSrc, "toBin": replDst, "quantity": float64(24), "priority": "URGENT"}
	if len(rep) != len(want) {
		t.Fatalf("replenishment = %v, want exactly %v (no executed block before completion)", rep, want)
	}
	for k, v := range want {
		if rep[k] != v {
			t.Errorf("replenishment.%s = %v, want %v", k, rep[k], v)
		}
	}
}

// TestReplenish_CompletionRecordsTheExecutedMove: completing with the
// confirmed quantity records the executed move, visible on the read model.
func TestReplenish_CompletionRecordsTheExecutedMove(t *testing.T) {
	srv, tasks, stations, _, clock := newTestServer()
	seedReplenishment(t, tasks, stations, clock)
	claimReplenish(t, srv)

	rec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/rep-1/completions", map[string]any{"stationId": "s1", "quantity": 20, "fromBin": replSrc, "toBin": replDst})
	if rec.Code != stdhttp.StatusNoContent {
		t.Fatalf("complete: expected 204, got %d: %s", rec.Code, rec.Body.String())
	}
	saved, _ := tasks.FindById(context.TODO(), "rep-1")
	want := task.ReplenishmentResult{Quantity: 20, FromBin: replSrc, ToBin: replDst}
	if saved.Status() != task.Completed || saved.ReplenishmentResult() == nil || *saved.ReplenishmentResult() != want {
		t.Fatalf("saved = %s %+v, want COMPLETED %+v", saved.Status(), saved.ReplenishmentResult(), want)
	}

	list := doJSON(t, srv, stdhttp.MethodGet, "/tasks?orderRef=replenish-req-9", nil)
	var tasksPage struct {
		Items []struct {
			Replenishment struct {
				Executed map[string]any `json:"executed"`
			} `json:"replenishment"`
		} `json:"items"`
	}
	err := json.Unmarshal(list.Body.Bytes(), &tasksPage)
	tasksResp := tasksPage.Items
	if err != nil || len(tasksResp) != 1 {
		t.Fatalf("GET /tasks: %v %s", err, list.Body.String())
	}
	executed := tasksResp[0].Replenishment.Executed
	if executed["quantity"] != float64(20) || executed["fromBin"] != replSrc || executed["toBin"] != replDst {
		t.Fatalf("executed = %v", executed)
	}
}

// TestReplenish_CompletionRejections maps every replenishment-confirmation
// error to a 422 problem and leaves the task Claimed.
func TestReplenish_CompletionRejections(t *testing.T) {
	cases := []struct {
		name string
		body map[string]any
		slug string
	}{
		{"no inputs", map[string]any{"stationId": "s1"}, "replenishment-confirmation-required"},
		{"scan of the source differs", map[string]any{"stationId": "s1", "quantity": 4, "fromBin": "WH1-RSV-AMB-Z99-01-01-A"}, "invalid-replenishment-confirmation"},
		{"scan of the destination differs", map[string]any{"stationId": "s1", "quantity": 4, "toBin": "WH1-FWD-AMB-Z99-01-01-A"}, "invalid-replenishment-confirmation"},
		{"bins without a quantity", map[string]any{"stationId": "s1", "fromBin": replSrc}, "invalid-replenishment-confirmation"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, tasks, stations, _, clock := newTestServer()
			seedReplenishment(t, tasks, stations, clock)
			claimReplenish(t, srv)
			rec := doJSON(t, srv, stdhttp.MethodPost, "/tasks/rep-1/completions", tc.body)
			assertProblemDetails(t, rec, stdhttp.StatusUnprocessableEntity, problemBase+tc.slug, "/tasks/rep-1/completions")
			saved, _ := tasks.FindById(context.TODO(), "rep-1")
			if saved.Status() != task.Claimed {
				t.Fatalf("status = %s, want CLAIMED", saved.Status())
			}
		})
	}
}

// A station without the replenish capability never gets REPLENISH work.
func TestReplenish_ClaimNeedsTheCapability(t *testing.T) {
	srv, tasks, stations, _, clock := newTestServer()
	seedReplenishment(t, tasks, stations, clock)
	_ = stations.Save(context.TODO(), station.New("s2", shared.NewCapabilitySet("pick", "relocate")))
	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s2/task-claims", map[string]any{"taskType": "REPLENISH"})
	if rec.Code != stdhttp.StatusConflict {
		t.Fatalf("a station without replenish: expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
	saved, _ := tasks.FindById(context.TODO(), "rep-1")
	if saved.Status() != task.Pending {
		t.Fatalf("status = %s, want PENDING", saved.Status())
	}
}

// POST /tasks keeps its creatable enum: REPLENISH work needs a planned move,
// so it is rejected (400), while claim-next accepts it.
func TestPostTask_ReplenishIsNotCreatable(t *testing.T) {
	srv, _, _, _, clock := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/tasks", map[string]any{
		"type": "REPLENISH", "cpt": clock.Now().Add(time.Hour), "orderRef": "o", "requiredCapabilities": []string{"replenish"},
	})
	assertProblemDetails(t, rec, stdhttp.StatusBadRequest, problemBase+"invalid-request", "")
}

// An unknown claim-next taskType still lists the full set, REPLENISH included.
func TestClaimNext_UnknownTaskTypeNamesReplenish(t *testing.T) {
	srv, _, _, _, _ := newTestServer()
	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/s1/task-claims", map[string]any{"taskType": "NOPE"})
	if rec.Code != stdhttp.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	var p struct {
		Detail string `json:"detail"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &p)
	if !strings.HasSuffix(p.Detail, "REPLENISH, BREAK_CASE") {
		t.Fatalf("detail = %q, want it to end with REPLENISH, BREAK_CASE", p.Detail)
	}
}
