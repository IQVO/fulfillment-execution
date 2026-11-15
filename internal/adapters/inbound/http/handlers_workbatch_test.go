package http_test

import (
	"encoding/json"
	stdhttp "net/http"
	"testing"
	"time"

	"github.com/claudioed/fulfillment-execution/internal/adapters/inbound/http"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/events"
	"github.com/claudioed/fulfillment-execution/internal/adapters/outbound/memory"
	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// newWorkBatchServer builds the OLTP handler set with the work-batch use
// cases wired, mode on — the composition root's wiring in miniature. It
// returns the handler set so a test can flip the mode gate.
func newWorkBatchServer(t *testing.T) (stdhttp.Handler, *usecases.FormWorkBatch, *memory.TaskRepo, *memory.FixedClock) {
	t.Helper()
	tasks := memory.NewTaskRepo()
	stations := memory.NewStationRepo()
	batches := memory.NewWorkBatchRepo()
	publisher := events.NewBufferedPublisher()
	clock := memory.NewFixedClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	if err := stations.Save(t.Context(), station.New(shared.StationId("stn-1"), shared.NewCapabilitySet(shared.Capability("pick")))); err != nil {
		t.Fatalf("seed station: %v", err)
	}
	form := &usecases.FormWorkBatch{
		Tasks: tasks, Stations: stations, Batches: batches, Publisher: publisher,
		Clock: clock, NewBatchId: func() shared.BatchId { return "wb-1" },
		Mode: usecases.BatchModeOn,
	}
	h, _, _, _, _ := newTestHandlers()
	h.FormWorkBatch = form
	h.RenewWorkBatchLease = &usecases.RenewWorkBatchLease{Tasks: tasks, Batches: batches, Clock: clock}
	h.GetWorkBatch = &usecases.GetWorkBatch{Batches: batches}
	return http.NewRouter(h, nil), form, tasks, clock
}

// seedPick stores one claimable PICK with a pick face.
func seedPick(t *testing.T, tasks *memory.TaskRepo, id string, cpt time.Time) {
	t.Helper()
	plan, err := task.NewPickPlan("SKU-1", 1, "EACH", "WH1", "A-01")
	if err != nil {
		t.Fatalf("pick plan: %v", err)
	}
	tk := task.New(shared.TaskId(id), task.Pick, shared.NewCPT(cpt), shared.OrderRef("o-"+id), shared.NewCapabilitySet(shared.Capability("pick")), false, false).WithPickPlan(&plan, 0)
	if err := tasks.Save(t.Context(), tk); err != nil {
		t.Fatalf("seed %s: %v", id, err)
	}
}

// TestPostFormWorkBatch_ModeOffAnswers503 pins the D5 gate at the HTTP
// edge: with PICK_BATCH_MODE not `on` the surface answers 503
// batch-mode-off and the ordinary claim surface is unchanged.
func TestPostFormWorkBatch_ModeOffAnswers503(t *testing.T) {
	srv, form, tasks, _ := newWorkBatchServer(t)
	seedPick(t, tasks, "t1", time.Date(2026, 1, 1, 18, 0, 0, 0, time.UTC))
	form.Mode = usecases.BatchModeOff

	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/stn-1/work-batches", nil)
	if rec.Code != stdhttp.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503: %s", rec.Code, rec.Body.String())
	}
	var p struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if p.Type != "https://errors.fulfillment-execution.warehouse-systems.dev/batch-mode-off" {
		t.Fatalf("type = %s", p.Type)
	}
}

func TestPostFormWorkBatch_FormsAndReadsBack(t *testing.T) {
	srv, _, tasks, _ := newWorkBatchServer(t)
	cpt := time.Date(2026, 1, 1, 18, 0, 0, 0, time.UTC)
	seedPick(t, tasks, "t1", cpt)

	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/stn-1/work-batches", nil)
	if rec.Code != stdhttp.StatusCreated {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/work-batches/wb-1" {
		t.Fatalf("Location = %q", loc)
	}
	var created struct {
		Batch struct {
			BatchId string `json:"batch_id"`
			Status  string `json:"status"`
			Members []struct {
				TaskId  string `json:"task_id"`
				StopSeq int    `json:"stop_seq"`
				State   string `json:"state"`
			} `json:"members"`
		} `json:"batch"`
		AnchorTaskId string `json:"anchor_task_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if created.AnchorTaskId != "t1" || created.Batch.BatchId != "wb-1" || created.Batch.Status != "OPEN" {
		t.Fatalf("body = %+v", created)
	}
	if len(created.Batch.Members) != 1 || created.Batch.Members[0].TaskId != "t1" || created.Batch.Members[0].StopSeq != 1 {
		t.Fatalf("members = %+v", created.Batch.Members)
	}

	// GET reads it back.
	get := doJSON(t, srv, stdhttp.MethodGet, "/work-batches/wb-1", nil)
	if get.Code != stdhttp.StatusOK {
		t.Fatalf("GET status = %d: %s", get.Code, get.Body.String())
	}

	// A second formation while the first is OPEN is 409 station-has-open-batch.
	again := doJSON(t, srv, stdhttp.MethodPost, "/stations/stn-1/work-batches", nil)
	if again.Code != stdhttp.StatusConflict {
		t.Fatalf("second formation status = %d: %s", again.Code, again.Body.String())
	}
}

func TestWorkBatchReadsAndRenewals_NotFoundAndNoContent(t *testing.T) {
	srv, _, tasks, _ := newWorkBatchServer(t)
	seedPick(t, tasks, "t1", time.Date(2026, 1, 1, 18, 0, 0, 0, time.UTC))

	if rec := doJSON(t, srv, stdhttp.MethodGet, "/work-batches/nope", nil); rec.Code != stdhttp.StatusNotFound {
		t.Fatalf("GET unknown = %d", rec.Code)
	}
	if rec := doJSON(t, srv, stdhttp.MethodPost, "/work-batches/nope/lease-renewals", nil); rec.Code != stdhttp.StatusNotFound {
		t.Fatalf("renew unknown = %d", rec.Code)
	}

	if rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/stn-1/work-batches", nil); rec.Code != stdhttp.StatusCreated {
		t.Fatalf("form = %d: %s", rec.Code, rec.Body.String())
	}
	rn := doJSON(t, srv, stdhttp.MethodPost, "/work-batches/wb-1/lease-renewals", nil)
	if rn.Code != stdhttp.StatusNoContent {
		t.Fatalf("renew = %d: %s", rn.Code, rn.Body.String())
	}
}

// TestPostFormWorkBatch_AnchorWithoutPickFaceIsPlainClaim pins the ADR 0062
// rule at the edge: an anchor with no pick face is claimed like task-claims
// would and the answer carries batch: null with 200, not a 201.
func TestPostFormWorkBatch_AnchorWithoutPickFaceIsPlainClaim(t *testing.T) {
	srv, _, tasks, _ := newWorkBatchServer(t)
	tk := task.New(shared.TaskId("t-noface"), task.Pick, shared.NewCPT(time.Date(2026, 1, 1, 18, 0, 0, 0, time.UTC)), shared.OrderRef("o-1"), shared.NewCapabilitySet(shared.Capability("pick")), false, false)
	if err := tasks.Save(t.Context(), tk); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rec := doJSON(t, srv, stdhttp.MethodPost, "/stations/stn-1/work-batches", nil)
	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Batch        any    `json:"batch"`
		AnchorTaskId string `json:"anchor_task_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Batch != nil || body.AnchorTaskId != "t-noface" {
		t.Fatalf("body = %+v", body)
	}
}
