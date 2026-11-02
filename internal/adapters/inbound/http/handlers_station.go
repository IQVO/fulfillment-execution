package http

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/fulfillment-execution/internal/domain/shared"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
)

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

// PostCheckInStation handles POST /stations/{stationId}/check-ins.
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

// PostCheckOutStation handles POST /stations/{stationId}/check-outs.
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
	if req.PackCPT.IsZero() {
		writeZeroDeadline(w, "packCpt")
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
		shared.NewCPT(*req.PackCPT),
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
