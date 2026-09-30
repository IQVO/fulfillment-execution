package http

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/claudioed/fulfillment-execution/internal/application/usecases"
	"github.com/claudioed/fulfillment-execution/internal/domain/consolidation"
	pack "github.com/claudioed/fulfillment-execution/internal/domain/package"
	"github.com/claudioed/fulfillment-execution/internal/domain/station"
	"github.com/claudioed/fulfillment-execution/internal/domain/task"
)

// problemBaseURI namespaces every RFC 7807 "type" URI this service mints.
// These URIs are identifiers, not fetchable documents (RFC 7807 §3.1).
const problemBaseURI = "https://errors.fulfillment-execution.warehouse-systems.dev/"

// problem is the RFC 7807 (Problem Details for HTTP APIs) response body
// used for every error response.
type problem struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance,omitempty"`
}

// statusFor maps a typed domain/application error to an HTTP status code.
func statusFor(err error) int {
	switch {
	case errors.Is(err, usecases.ErrTaskNotFound),
		errors.Is(err, usecases.ErrStationNotFound),
		errors.Is(err, usecases.ErrPackageNotFound):
		return http.StatusNotFound

	case errors.Is(err, task.ErrAlreadyClaimed),
		errors.Is(err, task.ErrAlreadyCompleted),
		errors.Is(err, task.ErrNotClaimed),
		errors.Is(err, task.ErrNotOwner),
		errors.Is(err, station.ErrOccupied),
		errors.Is(err, station.ErrNotOccupied),
		errors.Is(err, pack.ErrAlreadySealed),
		errors.Is(err, pack.ErrAlreadyProcessed),
		errors.Is(err, pack.ErrNotSealed),
		errors.Is(err, pack.ErrPackageSegregationViolation),
		errors.Is(err, usecases.ErrNoClaimableTask):
		return http.StatusConflict

	case errors.Is(err, task.ErrCapabilityMismatch),
		errors.Is(err, station.ErrCapabilityMismatch),
		errors.Is(err, pack.ErrNoScannedContents),
		errors.Is(err, consolidation.ErrUnknownLine),
		errors.Is(err, usecases.ErrWrongTaskType),
		errors.Is(err, usecases.ErrStationLocationNotWorkCenter):
		return http.StatusUnprocessableEntity

	default:
		return http.StatusInternalServerError
	}
}

// problemCatalog maps each typed domain/application error to its RFC 7807
// "type" slug and category-level "title". One entry per distinct error
// CATEGORY, mirroring statusFor's error set exactly (see REST_AUDIT.md).
// Entries are probed with errors.Is in catalog order, which matches the
// arm order of the switch this table replaced, so the mapping is exactly
// equivalent.
var problemCatalog = []struct {
	err   error
	slug  string
	title string
}{
	{usecases.ErrTaskNotFound, "task-not-found", "Task not found"},
	{usecases.ErrStationNotFound, "station-not-found", "Station not found"},
	{usecases.ErrPackageNotFound, "package-not-found", "Package not found"},

	{task.ErrAlreadyClaimed, "task-already-claimed", "Task already claimed by another station"},
	{task.ErrAlreadyCompleted, "task-already-completed", "Task already completed"},
	{task.ErrNotClaimed, "task-not-claimed", "Task is not currently claimed"},
	{task.ErrNotOwner, "task-not-owner", "Station does not own the active claim on this task"},
	{station.ErrOccupied, "station-occupied", "Station is already occupied"},
	{station.ErrNotOccupied, "station-not-occupied", "Station is not occupied"},
	{pack.ErrAlreadySealed, "package-already-sealed", "Package already sealed"},
	{pack.ErrAlreadyProcessed, "package-already-processed", "Package SLAM already processed"},
	{pack.ErrNotSealed, "package-not-sealed", "Package must be sealed before SLAM"},
	{pack.ErrPackageSegregationViolation, "package-segregation-violation", "Scanned item's DOT hazard class is incompatible with an already-scanned item"},
	{usecases.ErrNoClaimableTask, "no-claimable-task", "No claimable task for station capabilities"},

	{task.ErrCapabilityMismatch, "task-capability-mismatch", "Station capabilities do not match task requirements"},
	{station.ErrCapabilityMismatch, "station-capability-mismatch", "Capabilities do not match"},
	{pack.ErrNoScannedContents, "package-no-scanned-contents", "Cannot seal a package without scanned contents"},
	{consolidation.ErrUnknownLine, "rebin-unknown-line", "Line is not part of this order's required consolidation set"},
	{usecases.ErrWrongTaskType, "wrong-task-type", "Wrong task type for this operation"},
	{usecases.ErrStationLocationNotWorkCenter, "station-location-not-workcenter", "Station's locationCode does not resolve to a facility-layout WorkCenter"},
}

// problemTypeAndTitle maps a typed domain/application error to its RFC 7807
// "type" slug and category-level "title" by probing problemCatalog in
// order, falling back to the internal-error pair exactly like statusFor's
// default arm.
func problemTypeAndTitle(err error) (slug, title string) {
	for _, entry := range problemCatalog {
		if errors.Is(err, entry.err) {
			return entry.slug, entry.title
		}
	}
	return "internal-error", "Internal server error"
}

// writeError maps a typed domain/application error to an RFC 7807 response,
// using statusFor for the HTTP status (unchanged from before Stage 2) and
// problemTypeAndTitle for the type/title pair. instance is the request path
// that produced the error.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	status := statusFor(err)
	slug, title := problemTypeAndTitle(err)
	writeProblem(w, status, slug, title, err.Error(), r.URL.Path)
}

// writeProblem writes an RFC 7807 (application/problem+json) response body.
func writeProblem(w http.ResponseWriter, status int, slug, title, detail, instance string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problem{
		Type:     problemBaseURI + slug,
		Title:    title,
		Status:   status,
		Detail:   detail,
		Instance: instance,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeCreated writes a 201 Created response with a Location header
// pointing at the newly created resource.
func writeCreated(w http.ResponseWriter, location string, v any) {
	w.Header().Set("Location", location)
	writeJSON(w, http.StatusCreated, v)
}

// writeBadRequest writes an RFC 7807 400 for a malformed or incomplete
// request body (invalid JSON, or a decoded DTO missing a required field),
// on an endpoint whose path identifies a resource (e.g. /tasks/{id}/...).
func writeBadRequest(w http.ResponseWriter, r *http.Request, detail string) {
	writeProblem(w, http.StatusBadRequest, "invalid-request", "The request is malformed or missing a required field", detail, r.URL.Path)
}

// writeBadRequestNoInstance is writeBadRequest for a bare collection-create
// endpoint (POST /tasks, POST /stations) where the path has no segment
// identifying a specific resource instance — RFC 7807 permits omitting
// "instance" in that case (see REST_API_TASK.md Stage 2).
func writeBadRequestNoInstance(w http.ResponseWriter, detail string) {
	writeProblem(w, http.StatusBadRequest, "invalid-request", "The request is malformed or missing a required field", detail, "")
}
