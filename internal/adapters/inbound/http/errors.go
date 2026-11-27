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
	"github.com/claudioed/fulfillment-execution/internal/domain/workbatch"
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
		errors.Is(err, usecases.ErrPackageNotFound),
		errors.Is(err, usecases.ErrBatchNotFound):
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

	// Pick batching (ADR 0063). ErrBatchModeOff is a configured state of
	// the surface, not a fault: PICK_BATCH_MODE off/shadow answers 503
	// batch-mode-off, exactly like an unset delegation URL (ADR-0058).
	case errors.Is(err, usecases.ErrBatchModeOff):
		return http.StatusServiceUnavailable
	// A station forming while holding an OPEN batch is a conflict on the
	// station's state, mirroring station.ErrOccupied; a lifecycle command
	// on a COMPLETED/ABANDONED batch is the same class of conflict.
	case errors.Is(err, workbatch.ErrStationHasOpenBatch),
		errors.Is(err, workbatch.ErrBatchNotOpen):
		return http.StatusConflict

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
// catalogue entry: the "type" slug, the category-level "title" and the
// replay class (ADR 0066 — FINAL: a function of the request alone or
// provably irreversible for the identical request, stored and replayed;
// RETRYABLE: depends on state that can change, released). One entry per
// distinct error CATEGORY, mirroring statusFor's error set exactly (see
// REST_AUDIT.md). Entries are probed with errors.Is in catalog order, which
// matches the arm order of the switch this table replaced, so the mapping
// is exactly equivalent.
var problemCatalog = []struct {
	err  error
	info problemInfo
}{
	// A task can also arrive later, created by a consumed WorkReleased
	// event (consume_work_released.go), so a 404 can be "not known yet".
	{usecases.ErrTaskNotFound, problemInfo{"task-not-found", "Task not found", classRetryable}},
	// Station ids are caller-supplied (register_station.go): another
	// request (POST /stations) can deliver the station later.
	{usecases.ErrStationNotFound, problemInfo{"station-not-found", "Station not found", classRetryable}},
	// Package ids are minted by this service on the seal (seal_package.go:
	// pack.New(uc.NewId(), ...)) and packages are never deleted: a 404 is
	// irreversible for the identical request.
	{usecases.ErrPackageNotFound, problemInfo{"package-not-found", "Package not found", classFinal}},

	// Work-planning delegation outcomes (ADR-0058): typed errors raised by
	// the outbound A2A port mapping, plus the two local request-shape /
	// configuration states.
	{ports.ErrNoPendingWork, problemInfo{"work-planning-no-pending-work", "Work planning has no pending work for this path", classRetryable}},
	{ports.ErrUnknownWorkPlanningPath, problemInfo{"work-planning-unknown-path", "Work planning does not know this process path", classRetryable}},
	{ports.ErrWorkPlanningUnreachable, problemInfo{"work-planning-unreachable", "Work planning agent unreachable or answered off-contract", classRetryable}},
	{usecases.ErrWorkPlanningNotConfigured, problemInfo{"work-planning-not-configured", "Work-planning delegation not configured", classRetryable}},
	{usecases.ErrPathIdRequired, problemInfo{"path-id-required", "A process path id is required", classFinal}},

	{task.ErrAlreadyClaimed, problemInfo{"task-already-claimed", "Task already claimed by another station", classRetryable}},
	// COMPLETED is terminal (task.go: "already completed" on ANY attempt to
	// act): no state change can make the identical request succeed.
	{task.ErrAlreadyCompleted, problemInfo{"task-already-completed", "Task already completed", classFinal}},
	{task.ErrNotClaimed, problemInfo{"task-not-claimed", "Task is not currently claimed", classRetryable}},
	{task.ErrNotOwner, problemInfo{"task-not-owner", "Station does not own the active claim on this task", classRetryable}},
	{station.ErrOccupied, problemInfo{"station-occupied", "Station is already occupied", classRetryable}},
	{station.ErrNotOccupied, problemInfo{"station-not-occupied", "Station is not occupied", classRetryable}},
	// Sealed, like completed, is a terminal package state.
	{pack.ErrAlreadySealed, problemInfo{"package-already-sealed", "Package already sealed", classFinal}},
	{pack.ErrAlreadyProcessed, problemInfo{"package-already-processed", "Package SLAM already processed", classFinal}},
	{pack.ErrNotSealed, problemInfo{"package-not-sealed", "Package must be sealed before SLAM", classRetryable}},
	{pack.ErrPackageSegregationViolation, problemInfo{"package-segregation-violation", "Scanned item's DOT hazard class is incompatible with an already-scanned item", classRetryable}},
	{usecases.ErrNoClaimableTask, problemInfo{"no-claimable-task", "No claimable task for station capabilities", classRetryable}},

	// Pick batching (ADR 0063): the mode gate (503, a configured state),
	// the open-batch formation guard, the lifecycle conflict and the
	// read-side miss.
	{usecases.ErrBatchModeOff, problemInfo{"batch-mode-off", "Pick batching is off (PICK_BATCH_MODE)", classRetryable}},
	// Batch ids are minted by this service when the batch is formed
	// (form_work_batch.go) and batches are never deleted: a 404 is
	// irreversible for the identical request.
	{usecases.ErrBatchNotFound, problemInfo{"batch-not-found", "Work batch not found", classFinal}},
	{workbatch.ErrStationHasOpenBatch, problemInfo{"station-has-open-batch", "Station already holds an open batch", classRetryable}},
	{workbatch.ErrBatchNotOpen, problemInfo{"batch-not-open", "Batch is not open", classRetryable}},

	{task.ErrCapabilityMismatch, problemInfo{"task-capability-mismatch", "Station capabilities do not match task requirements", classRetryable}},
	{station.ErrCapabilityMismatch, problemInfo{"station-capability-mismatch", "Capabilities do not match", classRetryable}},
	{pack.ErrNoScannedContents, problemInfo{"package-no-scanned-contents", "Cannot seal a package without scanned contents", classRetryable}},
	{pack.ErrNotSingleLine, problemInfo{"ship-in-own-container-not-single-line", "Ship-in-own-container needs exactly one order line in the package", classRetryable}},
	{pack.ErrOwnContainerQuantityMismatch, problemInfo{"ship-in-own-container-quantity-mismatch", "The order line is not exactly one full case", classRetryable}},
	{pack.ErrNoCaseLevel, problemInfo{"ship-in-own-container-no-case-level", "The SKU has no active CASE level", classRetryable}},
	{pack.ErrCaseLevelRetired, problemInfo{"ship-in-own-container-case-retired", "The SKU's CASE level is retired", classRetryable}},
	{pack.ErrCaseDimensionsUnknown, problemInfo{"ship-in-own-container-case-dimensions-unknown", "The active CASE level has no dimensions and weight", classRetryable}},
	{consolidation.ErrUnknownLine, problemInfo{"rebin-unknown-line", "Line is not part of this order's required consolidation set", classRetryable}},
	// The task's type is stamped at creation and never changes.
	{usecases.ErrWrongTaskType, problemInfo{"wrong-task-type", "Wrong task type for this operation", classFinal}},
	{usecases.ErrStationLocationNotWorkCenter, problemInfo{"station-location-not-workcenter", "Station's locationCode does not resolve to a facility-layout WorkCenter", classRetryable}},
	// The planned move is stamped on the task at release and never changes:
	// for the identical request these answers are a function of the request
	// against an immutable plan.
	{task.ErrRelocationConfirmationRequired, problemInfo{"relocation-confirmation-required", "A RELOCATE task completes only with the confirmed quantity and the bins the move left open", classFinal}},
	{task.ErrInvalidRelocationConfirmation, problemInfo{"invalid-relocation-confirmation", "The relocation confirmation does not match the planned move", classFinal}},
	{task.ErrNotRelocationTask, problemInfo{"not-relocation-task", "quantity, fromBin and toBin apply only to RELOCATE and REPLENISH tasks", classFinal}},
	{task.ErrReplenishmentConfirmationRequired, problemInfo{"replenishment-confirmation-required", "A REPLENISH task completes only with the confirmed quantity", classFinal}},
	{task.ErrInvalidReplenishmentConfirmation, problemInfo{"invalid-replenishment-confirmation", "The replenishment confirmation does not match the planned move", classFinal}},

	// A GTIN is validated from the scanned bytes alone (pick_scan.go:
	// gs1.ParseGTIN, RejectVariableMeasure): FINAL. Whether a valid GTIN is
	// KNOWN is a PackView fact product-master can declare later: RETRYABLE.
	{usecases.ErrInvalidScannedGTIN, problemInfo{"invalid-scanned-gtin", "The scanned text is not a valid GTIN", classFinal}},
	{task.ErrInvalidScanCount, problemInfo{"invalid-scan-count", "The scan count must be at least 1", classFinal}},
	{usecases.ErrUnknownGTIN, problemInfo{"unknown-gtin", "The scanned GTIN is not a known pack level", classRetryable}},
	// The SKU and the level of a scanned GTIN come from the PackView, a
	// local copy product-master events can correct later.
	{task.ErrScanSkuMismatch, problemInfo{"scan-sku-mismatch", "The scanned GTIN is not the SKU of this pick", classRetryable}},
	{task.ErrScanLevelMismatch, problemInfo{"scan-level-mismatch", "The scanned pack level does not match the pick unit", classRetryable}},
	{task.ErrNoPickPlan, problemInfo{"no-pick-plan", "The task carries no pick plan to scan against", classRetryable}},
	{task.ErrPickOverQuantity, problemInfo{"pick-over-quantity", "The scan would pick more than the planned quantity", classRetryable}},
	{task.ErrPickIncomplete, problemInfo{"pick-incomplete", "The scanned eaches do not add up to the planned quantity", classRetryable}},
	{task.ErrNotCaseBreakTask, problemInfo{"not-case-break-task", "The task is not a BREAK_CASE task", classFinal}},
	{task.ErrCaseBreakCompletionRequired, problemInfo{"case-break-completion-required", "A BREAK_CASE task completes only through POST /tasks/{id}/case-breaks", classFinal}},
	{ports.ErrCaseBreakUnavailable, problemInfo{"case-break-unavailable", "inventory-storage could not be reached for the case break", classRetryable}},
	{ports.ErrCaseBreakRejected, problemInfo{"case-break-rejected", "inventory-storage rejected the case break", classRetryable}},
}

// problemInfoFor maps a typed domain/application error to its catalogue
// entry by probing problemCatalog in order, falling back to the registered
// internal-error pair exactly like statusFor's default arm.
func problemInfoFor(err error) problemInfo {
	for _, entry := range problemCatalog {
		if errors.Is(err, entry.err) {
			return entry.info
		}
	}
	return problemInternalError
}

// writeError maps a typed domain/application error to an RFC 7807 response,
// using statusFor for the HTTP status (unchanged from before Stage 2) and
// problemInfoFor for the type/title/class triple. instance is the request
// path that produced the error.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	status := statusFor(err)
	writeProblem(w, status, problemInfoFor(err), err.Error(), r.URL.Path)
}

// writeProblem writes an RFC 7807 (application/problem+json) response body
// and marks the idempotency middleware's capturing writer with the class of
// the problem it answers (replay_class.go); on any other writer the mark is
// a no-op.
func writeProblem(w http.ResponseWriter, status int, info problemInfo, detail, instance string) {
	markReplayClass(w, info.class)
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problem{
		Type:     problemBaseURI + info.slug,
		Title:    info.title,
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
	writeProblem(w, http.StatusBadRequest, problemInvalidRequest, detail, r.URL.Path)
}

// writeBadRequestNoInstance is writeBadRequest for a bare collection-create
// endpoint (POST /tasks, POST /stations) where the path has no segment
// identifying a specific resource instance — RFC 7807 permits omitting
// "instance" in that case (see REST_API_TASK.md Stage 2).
// writeZeroDeadline answers a deadline that is present but the zero time (0001-01-01T00:00:00Z).
// It is a valid RFC 3339 date-time, so the request is well-formed (not a 400 "is required"), but
// no task can be due at the zero instant: 422, problem slug invalid-deadline.
func writeZeroDeadline(w http.ResponseWriter, field string) {
	writeProblem(w, http.StatusUnprocessableEntity, problemInvalidDeadline,
		field+" must be a real deadline, not the zero time 0001-01-01T00:00:00Z", "")
}

func writeBadRequestNoInstance(w http.ResponseWriter, detail string) {
	writeProblem(w, http.StatusBadRequest, problemInvalidRequest, detail, "")
}
