package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/workbatch"
)

// workBatchMemberResponse is one stop of the walk.
type workBatchMemberResponse struct {
	TaskId      string `json:"task_id"`
	StopSeq     int    `json:"stop_seq"`
	Compartment int    `json:"compartment"`
	State       string `json:"state"`
}

// workBatchResponse is the WorkBatch read shape of GET /work-batches/{id}
// and the creation answer of POST /stations/{stationId}/work-batches.
type workBatchResponse struct {
	Id        shared.BatchId            `json:"batch_id"`
	StationId shared.StationId          `json:"station_id"`
	Status    workbatch.Status          `json:"status"`
	CPT       string                    `json:"cpt"`
	FormedAt  string                    `json:"formed_at"`
	Members   []workBatchMemberResponse `json:"members"`
}

func toWorkBatchResponse(b *workbatch.WorkBatch) workBatchResponse {
	members := make([]workBatchMemberResponse, len(b.Members()))
	for i, m := range b.Members() {
		members[i] = workBatchMemberResponse{
			TaskId:      string(m.TaskId),
			StopSeq:     m.StopSeq,
			Compartment: m.Compartment,
			State:       string(m.State),
		}
	}
	return workBatchResponse{
		Id:        b.Id(),
		StationId: b.StationId(),
		Status:    b.Status(),
		CPT:       b.CPT().UTC().Format("2006-01-02T15:04:05Z07:00"),
		FormedAt:  b.FormedAt().UTC().Format("2006-01-02T15:04:05Z07:00"),
		Members:   members,
	}
}

// formWorkBatchResponse answers POST /stations/{stationId}/work-batches.
// batch is null when the anchor was claimed as an ordinary single task
// because it has no pick face (ADR 0062): the claim stands, exactly as
// POST /stations/{stationId}/task-claims would have left it, and no walk
// was formed.
type formWorkBatchResponse struct {
	Batch        *workBatchResponse `json:"batch"`
	AnchorTaskId shared.TaskId      `json:"anchor_task_id"`
}

// PostFormWorkBatch handles POST /stations/{stationId}/work-batches
// (ADR 0063): form this station's walk. Only serves when PICK_BATCH_MODE is
// `on`; off/shadow answer 503 batch-mode-off.
func (h *Handlers) PostFormWorkBatch(w http.ResponseWriter, r *http.Request) {
	if h.FormWorkBatch == nil {
		// An unwired surface is an off surface (the unit-test harness
		// builds handlers without it; the composition root always wires
		// it): answer the mode-off problem, never a panic.
		writeError(w, r, usecases.ErrBatchModeOff)
		return
	}
	stationId := chi.URLParam(r, "stationId")
	out, err := h.FormWorkBatch.Execute(r.Context(), shared.StationId(stationId))
	if err != nil {
		writeError(w, r, err)
		return
	}
	resp := formWorkBatchResponse{AnchorTaskId: out.AnchorTaskId}
	if out.Batch != nil {
		resp.Batch = newWorkBatchResponsePtr(toWorkBatchResponse(out.Batch))
		writeCreated(w, "/work-batches/"+string(out.Batch.Id()), resp)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func newWorkBatchResponsePtr(r workBatchResponse) *workBatchResponse { return &r }

// PostWorkBatchLeaseRenewal handles POST /work-batches/{batchId}/
// lease-renewals (ADR 0063's LeaseTogether policy): renew every open
// member's task lease.
func (h *Handlers) PostWorkBatchLeaseRenewal(w http.ResponseWriter, r *http.Request) {
	if h.RenewWorkBatchLease == nil {
		writeError(w, r, usecases.ErrBatchModeOff)
		return
	}
	batchId := chi.URLParam(r, "batchId")
	if err := h.RenewWorkBatchLease.Execute(r.Context(), shared.BatchId(batchId)); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GetWorkBatchHandler handles GET /work-batches/{batchId}. The Handler
// suffix avoids the field/method name collision with the GetWorkBatch use
// case, like GetTasksHandler.
func (h *Handlers) GetWorkBatchHandler(w http.ResponseWriter, r *http.Request) {
	if h.GetWorkBatch == nil {
		writeError(w, r, usecases.ErrBatchNotFound)
		return
	}
	batchId := chi.URLParam(r, "batchId")
	b, err := h.GetWorkBatch.Execute(r.Context(), shared.BatchId(batchId))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toWorkBatchResponse(b))
}
