package inventorystorage

// The anti-corruption layer's reading of inventory-storage's refusals (ADR 0066).
//
// inventory-storage adopted the fleet idempotency rule (its ADR 0053): every problem type
// it answers on an idempotent route declares a replay class. A RETRYABLE refusal is
// released: nothing was committed, the key is free, and the same request under the SAME
// Idempotency-Key executes again and may pass. FINAL and OUTCOME refusals are stored and
// replayed, so repeating them only repeats the refusal. The case break is called with the
// deterministic key break-case:<taskId> (usecases.CaseBreakIdempotencyKey, no attempt
// part), so this adapter must know which refusals are worth another attempt. That
// knowledge is the callee's and lives here, one visible table; the use case and the domain
// only see ports.ErrCaseBreakRefusalReleased.
//
// A slug that is not in the table is TERMINAL, and so is a 4xx without a problem body:
// an unknown refusal never loops. refusal_classes_test.go holds the table against the
// pinned callee OpenAPI and against a copy of the callee's ADR 0053 class table.

// refusalClass is how this context treats one problem slug of inventory-storage.
type refusalClass uint8

const (
	// classUnset is the zero value: the slug is not in the table, which reads as terminal.
	classUnset refusalClass = iota
	// classTerminal is a FINAL or OUTCOME refusal of the callee: the break is refused for good.
	classTerminal
	// classRetryable is a RETRYABLE refusal of the callee: the break stays retryable.
	classRetryable
)

// caseBreakRefusals maps the problem slugs POST /containers/{lpn}/case-breaks can answer
// (documented in the pinned OpenAPI, or raised by the break and its placement checks in the
// callee) to their class here, from the callee's ADR 0053 table. The statuses are not read:
// location-classification-unavailable is a 409 although it is a dependency outage.
var caseBreakRefusals = map[string]refusalClass{
	// RETRYABLE: nothing committed, the answer depends on state that can change.
	"container-not-found":                 classRetryable, // the LPN is minted upstream; an event may deliver it later
	"bin-not-found":                       classRetryable, // bins are registered by PUT /bins and facility-layout events
	"stock-unit-not-found":                classRetryable,
	"container-not-sealed":                classRetryable,
	"container-not-stowed":                classRetryable,
	"container-reserved":                  classRetryable, // released by revoke, confirm or expiry
	"break-not-conserving":                classRetryable, // a cycle count can repair the discrepancy
	"container-not-at-site":               classRetryable,
	"bin-handling-unit-mismatch":          classRetryable,
	"bin-full":                            classRetryable,
	"release-exceeds-occupancy":           classRetryable,
	"concurrent-modification":             classRetryable, // another writer's commit; the retry reads the new version
	"hazmat-zone-required":                classRetryable,
	"hazmat-class-incompatible":           classRetryable,
	"temperature-class-mismatch":          classRetryable,
	"location-classification-unavailable": classRetryable, // a dependency outage answered 409
	"product-classification-not-found":    classRetryable, // classifications arrive by product-master events
	"held-unit-closed":                    classRetryable, // named by the brief; the break does not answer it today
	"transfer-receipt-conflict":           classRetryable, // named by the brief; the break does not answer it today
	// FINAL (and OUTCOME, none on this route): a function of the request, or stored.
	"empty-bin-id":             classTerminal,
	"malformed-request-body":   classTerminal,
	"not-found":                classTerminal,
	"idempotency-key-required": classTerminal,
	"idempotency-key-invalid":  classTerminal,
	"idempotency-key-reused":   classTerminal, // this caller repeats the same body: repeating it cannot pass
}

// releasedRefusal reports whether the callee releases the slug under the key.
func releasedRefusal(slug string) bool { return caseBreakRefusals[slug] == classRetryable }
