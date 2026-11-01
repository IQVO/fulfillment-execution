package http

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
)

func toPackageResponse(p *pack.Package) packageResponse {
	resp := ordinaryPackageResponse(p)
	if oc := p.OwnContainer(); oc != nil {
		resp.ShipInOwnContainer = true
		resp.ContainerSource = string(oc.Source)
		resp.Container = &containerResponse{
			Gtin: oc.GTIN, LengthMm: oc.Dims.LengthMM, WidthMm: oc.Dims.WidthMM, HeightMm: oc.Dims.HeightMM, WeightG: oc.Dims.WeightG,
		}
	}
	return resp
}

func ordinaryPackageResponse(p *pack.Package) packageResponse {
	return packageResponse{
		Id:                string(p.Id()),
		OrderRef:          string(p.OrderRef()),
		Status:            string(p.Status()),
		ScannedContents:   p.ScannedContents(),
		FragileHandling:   p.FragileHandling(),
		GiftWrapRequested: p.GiftWrapRequested(),
		SortLane:          p.SortLane(),
	}
}

// PostSealPackage handles POST /tasks/{id}/seal-package.
func (h *Handlers) PostSealPackage(w http.ResponseWriter, r *http.Request) {
	taskId := chi.URLParam(r, "id")
	var req sealPackageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBadRequest(w, r, "invalid request body")
		return
	}
	if msg := req.validate(); msg != "" {
		writeBadRequest(w, r, msg)
		return
	}

	var (
		p   *pack.Package
		err error
	)
	if req.ShipInOwnContainer {
		p, err = h.SealPackage.ExecuteInOwnContainer(r.Context(), shared.TaskId(taskId), shared.StationId(req.StationId), req.Contents, *req.LineQuantity)
	} else {
		p, err = h.SealPackage.Execute(r.Context(), shared.TaskId(taskId), shared.StationId(req.StationId), req.Contents)
	}
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeCreated(w, "/packages/"+string(p.Id()), toPackageResponse(p))
}

// PostRunSlam handles POST /packages/{id}/slam.
func (h *Handlers) PostRunSlam(w http.ResponseWriter, r *http.Request) {
	packageId := chi.URLParam(r, "id")
	var req runSlamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBadRequest(w, r, "invalid request body")
		return
	}
	if msg := req.validate(); msg != "" {
		writeBadRequest(w, r, msg)
		return
	}

	if err := h.RunSlam.Execute(r.Context(), shared.PackageId(packageId), req.ActualWeight, req.ExpectedWeight); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GetPackageHandler handles GET /packages/{id} (ADR-0033): the read side of
// the Package aggregate, returning the same Package shape seal-package's
// 201 returns. This is how a caller learns the SLAM outcome — LABELED vs
// DIVERTED — after POST /packages/{id}/slam's outcome-agnostic 204. An
// unknown id is 404 package-not-found.
func (h *Handlers) GetPackageHandler(w http.ResponseWriter, r *http.Request) {
	packageId := chi.URLParam(r, "id")
	p, err := h.GetPackage.Execute(r.Context(), shared.PackageId(packageId))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toPackageResponse(p))
}

// GetPackagesHandler handles GET /packages?orderRef=<ref> (ADR-0033),
// mirroring GetTasksHandler: orderRef is required (400 invalid-request if
// missing or empty) and an unknown orderRef returns an empty array.
func (h *Handlers) GetPackagesHandler(w http.ResponseWriter, r *http.Request) {
	orderRef := r.URL.Query().Get("orderRef")
	if orderRef == "" {
		writeBadRequest(w, r, "orderRef is required")
		return
	}

	packages, err := h.GetPackagesByOrderRef.Execute(r.Context(), shared.OrderRef(orderRef))
	if err != nil {
		writeError(w, r, err)
		return
	}

	resp := make([]packageResponse, 0, len(packages))
	for _, p := range packages {
		resp = append(resp, toPackageResponse(p))
	}
	writeJSON(w, http.StatusOK, resp)
}
