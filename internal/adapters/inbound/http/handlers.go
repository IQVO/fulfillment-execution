// Package http is the inbound REST adapter: chi handlers, DTOs, and
// domain-error-to-HTTP-status mapping. Domain structs never leak through it.
package http

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// Handlers wires REST endpoints to use cases.
type Handlers struct {
	CreateTask           *usecases.CreateTask
	ClaimNext            *usecases.ClaimNext
	RenewLease           *usecases.RenewLease
	CompleteTask         *usecases.CompleteTask
	SealPackage          *usecases.SealPackage
	RunSlam              *usecases.RunSlam
	GetQueueDepth        *usecases.GetQueueDepth
	ExpireLeases         *usecases.ExpireLeases
	RegisterStation      *usecases.RegisterStation
	GetTasksByOrderRef   *usecases.GetTasksByOrderRef
	CheckInStation       *usecases.CheckInStation
	CheckOutStation      *usecases.CheckOutStation
	ArriveAtRebin        *usecases.ArriveAtRebin
	GetInstalledCapacity *usecases.GetInstalledCapacity
	SweepCPTMisses       *usecases.SweepCPTMisses
	// GetPackage and GetPackagesByOrderRef back the Package read model
	// (ADR-0033): GET /packages/{id} and GET /packages?orderRef=.
	GetPackage            *usecases.GetPackage
	GetPackagesByOrderRef *usecases.GetPackagesByOrderRef
	// Readiness backs GET /readyz (ADR-0029 §graceful shutdown): flipped
	// to not-ready as the FIRST step of shutdown in cmd/execution. Nil is
	// a documented "always ready" no-op — see Readiness.Ready — so every
	// pre-existing test/caller that does not wire one is unaffected.
	Readiness *Readiness
}

func toTaskResponse(t *task.Task) taskResponse {
	caps := make([]string, 0, len(t.RequiredCapabilities()))
	for c := range t.RequiredCapabilities() {
		caps = append(caps, string(c))
	}
	resp := taskResponse{
		Id:                   string(t.Id()),
		Type:                 string(t.Type()),
		Status:               string(t.Status()),
		CPT:                  t.CPT().Time(),
		OrderRef:             string(t.OrderRef()),
		RequiredCapabilities: caps,
		Fragile:              t.Fragile(),
		GiftWrap:             t.GiftWrap(),
	}
	if lease := t.Lease(); lease != nil {
		stationId := string(lease.StationId)
		resp.LeaseStationId = &stationId
		expiry := lease.Expiry
		resp.LeaseExpiry = &expiry
	}
	return resp
}

func toPackageResponse(p *pack.Package) packageResponse {
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

func toStationResponse(s *station.Station) stationResponse {
	caps := make([]string, 0, len(s.Capabilities()))
	for c := range s.Capabilities() {
		caps = append(caps, string(c))
	}
	return stationResponse{
		Id:           string(s.Id()),
		Capabilities: caps,
		Occupied:     s.IsOccupied(),
		LocationCode: s.LocationCode(),
	}
}

// PostTask handles POST /tasks.
func (h *Handlers) PostTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBadRequestNoInstance(w, "invalid request body")
		return
	}
	if msg := req.validate(); msg != "" {
		writeBadRequestNoInstance(w, msg)
		return
	}

	caps := make([]shared.Capability, len(req.RequiredCapabilities))
	for i, c := range req.RequiredCapabilities {
		caps[i] = shared.Capability(c)
	}

	// giftWrap is deliberately not accepted from createTaskRequest: unlike
	// fragile, it has exactly one ingestion path (WorkReleased.data.gift_wrap
	// via the Kafka consumer — see ADR-0011), so a direct HTTP POST /tasks
	// caller always gets false here; the taskResponse field is read-only.
	t, err := h.CreateTask.Execute(r.Context(), task.Type(req.Type), shared.NewCPT(req.CPT), shared.OrderRef(req.OrderRef), shared.NewCapabilitySet(caps...), req.Fragile, false)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeCreated(w, "/tasks/"+string(t.Id()), toTaskResponse(t))
}

// PostClaimNext handles POST /stations/{stationId}/claim-next.
func (h *Handlers) PostClaimNext(w http.ResponseWriter, r *http.Request) {
	stationId := chi.URLParam(r, "stationId")
	var req claimNextRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBadRequest(w, r, "invalid request body")
		return
	}
	if msg := req.validate(); msg != "" {
		writeBadRequest(w, r, msg)
		return
	}

	t, err := h.ClaimNext.Execute(r.Context(), shared.StationId(stationId), task.Type(req.TaskType))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTaskResponse(t))
}

// PostRenewLease handles POST /tasks/{id}/renew-lease.
func (h *Handlers) PostRenewLease(w http.ResponseWriter, r *http.Request) {
	taskId := chi.URLParam(r, "id")
	var req renewLeaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBadRequest(w, r, "invalid request body")
		return
	}
	if msg := req.validate(); msg != "" {
		writeBadRequest(w, r, msg)
		return
	}

	if err := h.RenewLease.Execute(r.Context(), shared.TaskId(taskId), shared.StationId(req.StationId)); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// PostCompleteTask handles POST /tasks/{id}/complete.
func (h *Handlers) PostCompleteTask(w http.ResponseWriter, r *http.Request) {
	taskId := chi.URLParam(r, "id")
	var req completeTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBadRequest(w, r, "invalid request body")
		return
	}
	if msg := req.validate(); msg != "" {
		writeBadRequest(w, r, msg)
		return
	}

	if err := h.CompleteTask.Execute(r.Context(), shared.TaskId(taskId), shared.StationId(req.StationId)); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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

	p, err := h.SealPackage.Execute(r.Context(), shared.TaskId(taskId), shared.StationId(req.StationId), req.Contents)
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

// GetQueueDepthHandler handles GET /queues/{taskType}/depth.
func (h *Handlers) GetQueueDepthHandler(w http.ResponseWriter, r *http.Request) {
	taskType := chi.URLParam(r, "taskType")
	depth, err := h.GetQueueDepth.Execute(r.Context(), task.Type(taskType))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, queueDepthResponse{TaskType: taskType, Depth: depth})
}

// GetInstalledCapacityHandler handles GET /capacity/{capability}. This is
// the read endpoint workforce-management's CommitShiftPlan calls to
// enforce plannedHeads against the REAL Station registry, live, rather
// than trusting a caller-supplied number alone — see ADR-0018. A
// capability with zero registered stations is not an error: it returns
// installed: 0, exactly like GetQueueDepthHandler treats an unrecognized
// taskType as depth 0 rather than a 404.
func (h *Handlers) GetInstalledCapacityHandler(w http.ResponseWriter, r *http.Request) {
	capability := chi.URLParam(r, "capability")
	installed, err := h.GetInstalledCapacity.Execute(r.Context(), shared.Capability(capability))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, installedCapacityResponse{Capability: capability, Installed: installed})
}

// GetTasksHandler handles GET /tasks?orderRef=<ref>. It requires the
// orderRef query parameter (400 if missing) and returns every task
// recorded for that order — normally one PICK/PACK/SLAM leg each, more if
// a leg was retried after a lease expired — as a JSON array. An unknown
// orderRef is not an error: it returns an empty array.
func (h *Handlers) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	orderRef := r.URL.Query().Get("orderRef")
	if orderRef == "" {
		writeBadRequest(w, r, "orderRef is required")
		return
	}

	tasks, err := h.GetTasksByOrderRef.Execute(r.Context(), shared.OrderRef(orderRef))
	if err != nil {
		writeError(w, r, err)
		return
	}

	resp := make([]taskResponse, 0, len(tasks))
	for _, t := range tasks {
		resp = append(resp, toTaskResponse(t))
	}
	writeJSON(w, http.StatusOK, resp)
}

// PostExpireLeases handles POST /tasks/expire-leases.
func (h *Handlers) PostExpireLeases(w http.ResponseWriter, r *http.Request) {
	freed, err := h.ExpireLeases.Execute(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, expireLeasesResponse{Freed: freed})
}

// PostSweepCPTMisses handles POST /tasks/sweep-cpt-misses: the Clock-driven
// sweep that detects tasks still open past their CPT and raises
// TaskCPTMissed for each (ADR-0025). Mirrors PostExpireLeases's own
// externally-triggered convention exactly — a collection-level action on
// the tasks resource, not scoped to a single task id.
func (h *Handlers) PostSweepCPTMisses(w http.ResponseWriter, r *http.Request) {
	reported, err := h.SweepCPTMisses.Execute(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, sweepCPTMissesResponse{Reported: reported})
}

// PostRegisterStation handles POST /stations.
func (h *Handlers) PostRegisterStation(w http.ResponseWriter, r *http.Request) {
	var req registerStationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBadRequestNoInstance(w, "invalid request body")
		return
	}
	if msg := req.validate(); msg != "" {
		writeBadRequestNoInstance(w, msg)
		return
	}

	s, err := h.RegisterStation.Execute(r.Context(), req.StationId, req.Capabilities, req.LocationCode)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeCreated(w, "/stations/"+string(s.Id()), toStationResponse(s))
}

// GetHealthz handles GET /healthz.
func (h *Handlers) GetHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// PostCheckInStation handles POST /stations/{stationId}/check-in.
func (h *Handlers) PostCheckInStation(w http.ResponseWriter, r *http.Request) {
	stationId := chi.URLParam(r, "stationId")
	var req checkInStationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBadRequest(w, r, "invalid request body")
		return
	}
	if msg := req.validate(); msg != "" {
		writeBadRequest(w, r, msg)
		return
	}

	s, err := h.CheckInStation.Execute(r.Context(), shared.StationId(stationId), station.OccupantId(req.OccupantId))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toStationResponse(s))
}

// PostCheckOutStation handles POST /stations/{stationId}/check-out.
func (h *Handlers) PostCheckOutStation(w http.ResponseWriter, r *http.Request) {
	stationId := chi.URLParam(r, "stationId")

	s, err := h.CheckOutStation.Execute(r.Context(), shared.StationId(stationId))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toStationResponse(s))
}

// PostArriveAtRebin handles POST /rebin/arrivals: one required line of an
// order has reached the Rebin path. Once every required line for the
// order has arrived, this creates the order's PACK task — see
// usecases.ArriveAtRebin.
func (h *Handlers) PostArriveAtRebin(w http.ResponseWriter, r *http.Request) {
	var req arriveAtRebinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBadRequestNoInstance(w, "invalid request body")
		return
	}
	if msg := req.validate(); msg != "" {
		writeBadRequestNoInstance(w, msg)
		return
	}

	caps := make([]shared.Capability, len(req.PackRequiredCapabilities))
	for i, c := range req.PackRequiredCapabilities {
		caps[i] = shared.Capability(c)
	}

	err := h.ArriveAtRebin.Execute(
		r.Context(),
		shared.OrderRef(req.OrderRef),
		req.LineId,
		req.RequiredLineIds,
		shared.NewCPT(req.PackCPT),
		shared.NewCapabilitySet(caps...),
		req.PackFragile,
		req.PackGiftWrap,
	)
	if err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
