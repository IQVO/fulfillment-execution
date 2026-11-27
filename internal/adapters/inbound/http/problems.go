package http

import "net/http"

// registered collects the problem types declared with register below. The
// typed domain errors are not here: they are the entries of problemCatalog
// (errors.go), their status is statusFor's.
var registered []problemType

// register declares a problem type that is not a typed domain error: it
// records the entry for allProblemTypes and returns the problemInfo to
// answer it with. The status is the one the call sites answer;
// allProblemTypes' tests use it to drive the real error writer.
func register(slug, title string, status int, class replayClass) problemInfo {
	info := problemInfo{slug, title, class}
	registered = append(registered, problemType{Slug: slug, Status: status, Class: class, info: info})
	return info
}

// The problem types that are not a typed domain error (those are the entries
// of problemCatalog in errors.go). Each is declared with register, which
// takes the replay class, so none can exist without one
// (replay_class_test.go scans for a problemInfo literal anywhere else).
//
// Request validation is FINAL: the answer is a function of the request
// alone. A refusal that depends on state, a dependency or the server itself
// is RETRYABLE. This service has no OUTCOME: every refusal rolls its writes
// back with the request transaction (see
// docs/docs/adr/0066-idempotency-replay-classes.md).
var (
	problemInvalidRequest  = register("invalid-request", "The request is malformed or missing a required field", http.StatusBadRequest, classFinal)
	problemInvalidDeadline = register("invalid-deadline", "The deadline is not a real instant", http.StatusUnprocessableEntity, classFinal)

	problemBodyUnreadable = register("malformed-request-body", "The request body could not be read", http.StatusBadRequest, classFinal)
	problemBodyHasNUL     = register("malformed-request-body", "The request body contains a NUL character", http.StatusBadRequest, classFinal)

	problemIdempotencyKeyRequired = register("idempotency-key-required", "Idempotency-Key header is required", http.StatusBadRequest, classFinal)
	problemIdempotencyKeyInvalid  = register("idempotency-key-invalid", "Idempotency-Key header is invalid", http.StatusBadRequest, classFinal)
	// problemIdempotencyKeyReused is the store's own answer to a key seen
	// with another request; it is never stored (no handler ran, no key row
	// is written by the request that gets it).
	problemIdempotencyKeyReused = register("idempotency-key-reused", "Idempotency-Key was already used with a different request", http.StatusConflict, classFinal)

	problemResourceNotFound = register("resource-not-found", "Resource not found", http.StatusNotFound, classFinal)

	// The 400 problems of the read side (collections, reports): no keyed
	// route answers them, they carry the class of the question "is it a
	// function of the request alone".
	problemUnknownQueryParameter = register("unknown-query-parameter", "The query parameter is not declared by this route", http.StatusBadRequest, classFinal)
	problemInvalidLimit          = register("invalid-limit", "The limit is not valid", http.StatusBadRequest, classFinal)
	problemInvalidCursor         = register("invalid-cursor", "The cursor is not valid", http.StatusBadRequest, classFinal)
	problemInvalidReportQuery    = register("invalid-report-query", "The report query is malformed or missing a required parameter", http.StatusBadRequest, classFinal)

	// Server-side answers: never stored (a 5xx), so RETRYABLE.
	problemInternalError         = register("internal-error", "Internal server error", http.StatusInternalServerError, classRetryable)
	problemReportStoreError      = register("report-store-error", "The report could not be served", http.StatusInternalServerError, classRetryable)
	problemDependencyUnavailable = register("dependency-unavailable", "A runtime dependency is unavailable", http.StatusServiceUnavailable, classRetryable)
)
