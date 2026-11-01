package http

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/claudioed/fulfillment-execution/internal/application/ports"
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
		errors.Is(err, usecases.ErrNoClaimableTask),
		// The scan or break does not fit the task's current plan: a conflict
		// with the task's state, not a malformed request.
		// A scanned code that is not a valid GTIN, or that no pack level
		// carries, matches nothing the task can be confirmed against. It is
		// a 409, not a 404 (the task exists) and not a 400 (the body schema
		// already bounds the code's shape, so it is a schema-valid request).
		errors.Is(err, usecases.ErrInvalidScannedGTIN),
		errors.Is(err, usecases.ErrUnknownGTIN),
		errors.Is(err, task.ErrScanSkuMismatch),
		errors.Is(err, task.ErrScanLevelMismatch),
		errors.Is(err, task.ErrNoPickPlan),
		errors.Is(err, task.ErrPickOverQuantity),
		errors.Is(err, task.ErrPickIncomplete),
		errors.Is(err, task.ErrNotCaseBreakTask),
		errors.Is(err, task.ErrCaseBreakCompletionRequired):
		return http.StatusConflict

	case errors.Is(err, task.ErrInvalidScanCount):
		return http.StatusBadRequest

	case errors.Is(err, ports.ErrCaseBreakUnavailable):
		return http.StatusServiceUnavailable

	// Work-planning delegation (ADR-0058): the peer's business rejections are
	// conflicts, its unknown path is a 404, an unreachable/off-contract peer
	// is a bad gateway, an unset delegation URL is explicitly off (503), and
	// an empty path id is a client error.
	case errors.Is(err, ports.ErrNoPendingWork):
		return http.StatusConflict
	case errors.Is(err, ports.ErrUnknownWorkPlanningPath):
		return http.StatusNotFound
	case errors.Is(err, ports.ErrWorkPlanningUnreachable):
		return http.StatusBadGateway
	case errors.Is(err, usecases.ErrWorkPlanningNotConfigured):
		return http.StatusServiceUnavailable
	case errors.Is(err, usecases.ErrPathIdRequired):
		return http.StatusBadRequest

	case errors.Is(err, ports.ErrCaseBreakRejected):
		return http.StatusConflict

	case errors.Is(err, task.ErrCapabilityMismatch),
		errors.Is(err, station.ErrCapabilityMismatch),
		errors.Is(err, pack.ErrNoScannedContents),
		errors.Is(err, pack.ErrNotSingleLine),
		errors.Is(err, pack.ErrOwnContainerQuantityMismatch),
		errors.Is(err, pack.ErrNoCaseLevel),
		errors.Is(err, pack.ErrCaseLevelRetired),
		errors.Is(err, pack.ErrCaseDimensionsUnknown),
		errors.Is(err, consolidation.ErrUnknownLine),
		errors.Is(err, usecases.ErrWrongTaskType),
		errors.Is(err, usecases.ErrStationLocationNotWorkCenter),
		errors.Is(err, task.ErrRelocationConfirmationRequired),
		errors.Is(err, task.ErrInvalidRelocationConfirmation),
		errors.Is(err, task.ErrReplenishmentConfirmationRequired),
		errors.Is(err, task.ErrInvalidReplenishmentConfirmation),
		errors.Is(err, task.ErrNotRelocationTask):
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

	// Work-planning delegation outcomes (ADR-0058): typed errors raised by
	// the outbound A2A port mapping, plus the two local request-shape /
	// configuration states.
	{ports.ErrNoPendingWork, "work-planning-no-pending-work", "Work planning has no pending work for this path"},
	{ports.ErrUnknownWorkPlanningPath, "work-planning-unknown-path", "Work planning does not know this process path"},
	{ports.ErrWorkPlanningUnreachable, "work-planning-unreachable", "Work planning agent unreachable or answered off-contract"},
	{usecases.ErrWorkPlanningNotConfigured, "work-planning-not-configured", "Work-planning delegation not configured"},
	{usecases.ErrPathIdRequired, "path-id-required", "A process path id is required"},

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
	{pack.ErrNotSingleLine, "ship-in-own-container-not-single-line", "Ship-in-own-container needs exactly one order line in the package"},
	{pack.ErrOwnContainerQuantityMismatch, "ship-in-own-container-quantity-mismatch", "The order line is not exactly one full case"},
	{pack.ErrNoCaseLevel, "ship-in-own-container-no-case-level", "The SKU has no active CASE level"},
	{pack.ErrCaseLevelRetired, "ship-in-own-container-case-retired", "The SKU's CASE level is retired"},
	{pack.ErrCaseDimensionsUnknown, "ship-in-own-container-case-dimensions-unknown", "The active CASE level has no dimensions and weight"},
	{consolidation.ErrUnknownLine, "rebin-unknown-line", "Line is not part of this order's required consolidation set"},
	{usecases.ErrWrongTaskType, "wrong-task-type", "Wrong task type for this operation"},
	{usecases.ErrStationLocationNotWorkCenter, "station-location-not-workcenter", "Station's locationCode does not resolve to a facility-layout WorkCenter"},
	{task.ErrRelocationConfirmationRequired, "relocation-confirmation-required", "A RELOCATE task completes only with the confirmed quantity and the bins the move left open"},
	{task.ErrInvalidRelocationConfirmation, "invalid-relocation-confirmation", "The relocation confirmation does not match the planned move"},
	{task.ErrNotRelocationTask, "not-relocation-task", "quantity, fromBin and toBin apply only to RELOCATE and REPLENISH tasks"},
	{task.ErrReplenishmentConfirmationRequired, "replenishment-confirmation-required", "A REPLENISH task completes only with the confirmed quantity"},
	{task.ErrInvalidReplenishmentConfirmation, "invalid-replenishment-confirmation", "The replenishment confirmation does not match the planned move"},

	{usecases.ErrInvalidScannedGTIN, "invalid-scanned-gtin", "The scanned text is not a valid GTIN"},
	{task.ErrInvalidScanCount, "invalid-scan-count", "The scan count must be at least 1"},
	{usecases.ErrUnknownGTIN, "unknown-gtin", "The scanned GTIN is not a known pack level"},
	{task.ErrScanSkuMismatch, "scan-sku-mismatch", "The scanned GTIN is not the SKU of this pick"},
	{task.ErrScanLevelMismatch, "scan-level-mismatch", "The scanned pack level does not match the pick unit"},
	{task.ErrNoPickPlan, "no-pick-plan", "The task carries no pick plan to scan against"},
	{task.ErrPickOverQuantity, "pick-over-quantity", "The scan would pick more than the planned quantity"},
	{task.ErrPickIncomplete, "pick-incomplete", "The scanned eaches do not add up to the planned quantity"},
	{task.ErrNotCaseBreakTask, "not-case-break-task", "The task is not a BREAK_CASE task"},
	{task.ErrCaseBreakCompletionRequired, "case-break-completion-required", "A BREAK_CASE task completes only through POST /tasks/{id}/break-case"},
	{ports.ErrCaseBreakUnavailable, "case-break-unavailable", "inventory-storage could not be reached for the case break"},
	{ports.ErrCaseBreakRejected, "case-break-rejected", "inventory-storage rejected the case break"},
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
// writeZeroDeadline answers a deadline that is present but the zero time (0001-01-01T00:00:00Z).
// It is a valid RFC 3339 date-time, so the request is well-formed (not a 400 "is required"), but
// no task can be due at the zero instant: 422, problem slug invalid-deadline.
func writeZeroDeadline(w http.ResponseWriter, field string) {
	writeProblem(w, http.StatusUnprocessableEntity, "invalid-deadline", "The deadline is not a real instant",
		field+" must be a real deadline, not the zero time 0001-01-01T00:00:00Z", "")
}

func writeBadRequestNoInstance(w http.ResponseWriter, detail string) {
	writeProblem(w, http.StatusBadRequest, "invalid-request", "The request is malformed or missing a required field", detail, "")
}
