---
id: 0066-retry-the-case-break-refusals-inventory-storage-releases
slug: /adr/0066-retry-the-case-break-refusals-inventory-storage-releases
title: 66. Retry the case-break refusals inventory-storage releases (replay class RETRYABLE)
sidebar_label: "66. Retry released case-break refusals"
description: "ADR 0066 — the consumer half of the fleet idempotency rule on the case-break call: the inventory-storage adapter maps the callee's problem slug to retryable or terminal from the callee's ADR 0053 class table; a RETRYABLE refusal leaves the BREAK_CASE task claimed and answers 503 so the floor client retries under the same Idempotency-Key break-case:<taskId>; FINAL, OUTCOME, unknown slugs and 4xx without a problem body stay 409 case-break-rejected."
---

# 66. Retry the case-break refusals inventory-storage releases (replay class RETRYABLE)

## Status

Proposed (2026-11-19). Adopts the consumer side of the fleet idempotency rule decided on 2026-11-18
(`.claude/rules/fleet/idempotency-and-outbox.md`; standard in warehouse-harness-template
`docs/idempotency-replay-classes.md`). The provider side is merged: inventory-storage ADR 0053 (PR #329).
Amends [ADR 0054](./0054-case-and-each-pick-scans-and-break-case-task.md), the BREAK_CASE call, in the one
sentence that treated every 4xx of inventory-storage as permanent. It does **not** touch this service's own
idempotency middleware or what it stores for its own routes ([ADR 0055](./0055-scan-idempotency-and-claimable-break-case.md)):
that is this service as a provider. No route, status code, problem slug, field, event or migration of this
service is added or changed.

## Context

Completing a BREAK_CASE task (`POST /tasks/{id}/case-breaks`) calls inventory-storage
`POST /containers/{lpn}/case-breaks` with the key `break-case:<taskId>`: stable per task, no attempt part
(`usecases.CaseBreakIdempotencyKey`). Before the rule inventory-storage stored every refusal below 500 for 24 hours,
so a retry under that key replayed it and treating every 4xx as `ErrCaseBreakRejected` (409 `case-break-rejected`)
lost nothing. After the rule it does NOT store a refusal that depends on state another request can change (replay
class RETRYABLE): nothing was committed, the key is released, and the same request under the SAME key executes
again and may pass. A `bin-not-found` or `container-not-found` (registered or delivered by an event a moment
later), a `concurrent-modification`, a `container-reserved` released by a revoke could therefore succeed on the
next attempt, but this service answered 409 `case-break-rejected` on the first one, and the floor client
(warehouse-floor-client, `RETRYABLE_STATUS` = 429, 500, 502, 503, 504) treats a 4xx as a final refusal shown to
the operator while it re-sends a 503 from its outbox with the same key.

Facts read from the code (2026-11-19):

- `inventorystorage.Client.BreakCase` mapped 408, 429 and 5xx to `ErrCaseBreakUnavailable` and every other
  non-2xx to `ErrCaseBreakRejected`, by status alone. `BreakerClient` retries `ErrCaseBreakUnavailable` 3 times
  (jittered 50-500 ms) inside one call, always with the same key, and excludes `ErrCaseBreakRejected` from the
  circuit breaker's failure count.
- `CompleteCaseBreak` leaves the task `Claimed` and unchanged on any error from the call; there is no pending or
  failed state for a case break and no attempt counter. The retry mechanism is the claim's lease (5 minutes,
  `DefaultLeaseDuration`) plus the caller re-issuing the command: the floor client's outbox on a 5xx.
  `ErrCaseBreakUnavailable` answers `503 case-break-unavailable`, which this service's middleware never stores
  (a 5xx rolls the key row back), so a re-send executes; `ErrCaseBreakRejected` answers `409 case-break-rejected`,
  which the middleware stores and replays under the floor client's key.
- `location-classification-unavailable` is answered **409** by inventory-storage although it is a dependency
  outage (its ADR 0053 open point: it should be 503). Read by status, this caller classified that outage as a
  business rejection; read by problem slug, as below, it is retryable. The status is not trusted.

## Decision

1. **The outbound adapter reads the callee's class.** `internal/adapters/outbound/inventorystorage/refusal_classes.go`
   holds one small visible table from problem slug to `retryable` (callee RETRYABLE) or `terminal` (FINAL or OUTCOME),
   derived from the callee's merged class table for the case-breaks operation. The class is the callee's knowledge and
   lives in the anti-corruption layer, never in the domain; the use case and the domain only see the typed error.
2. **A released refusal is `ports.ErrCaseBreakRefusalReleased`**, which always wraps `ports.ErrCaseBreakUnavailable`.
   The task stays `Claimed`, nothing is published, and the answer is the existing `503 case-break-unavailable`
   (documented, never stored), whose `detail` carries the callee's status and problem body, hence the slug. The retry is the
   floor client's re-send under the SAME `Idempotency-Key`; this service calls inventory-storage again with the SAME
   `break-case:<taskId>`. **No attempt part is added to the key**: the whole point is that the callee released it.
3. **Terminal stays terminal.** A FINAL or OUTCOME slug, a slug that is not in the table, a 4xx without a problem body
   (or whose body is not a problem document) is `ErrCaseBreakRejected`, 409 `case-break-rejected`, exactly as before.
   Unknown is terminal: an unknown refusal never loops. The status is not read for the class (a 404 and a 409 are both
   retryable when the slug is).
4. **Bounds: the existing ones, nothing new.** Inside one call, the 3 attempts of `BreakerClient` (same key). Across
   calls, the claim's lease: while it lasts each re-send executes once more against inventory-storage; when it lapses the
   command ends where it always did, `409 task-not-claimed`, and the task returns to the pool. A refusal is not the
   dependency failing: `ErrCaseBreakRefusalReleased` is excluded from the circuit breaker, like a rejection, so a bin
   that is not registered yet never opens the circuit for case breaks that would succeed.
5. **The reason is surfaced without a contract change.** The pending reason is the `detail` text of the 503. A
   machine-readable reason (a problem slug or field of its own) would be a contract change and is not made here (see Open
   points).

### The table

RETRYABLE (retried): `container-not-found`, `bin-not-found`, `stock-unit-not-found`, `container-not-sealed`,
`container-not-stowed`, `container-reserved`, `break-not-conserving`, `container-not-at-site`,
`bin-handling-unit-mismatch`, `bin-full`, `release-exceeds-occupancy`, `concurrent-modification`,
`hazmat-zone-required`, `hazmat-class-incompatible`, `temperature-class-mismatch`,
`location-classification-unavailable` (a 409 that is a dependency outage), `product-classification-not-found`;
and `held-unit-closed`, `transfer-receipt-conflict`, which the case break does not answer today and are listed
because the callee's table says so.

TERMINAL, explicit: `empty-bin-id`, `malformed-request-body`, `not-found`, `idempotency-key-required`,
`idempotency-key-invalid`, `idempotency-key-reused` (this caller sends a valid key and the same body every time:
repeating it cannot pass). Every other slug is unknown and therefore terminal.

### Keeping the table honest

`refusal_classes_test.go` holds the table against the byte-pinned callee OpenAPI (`contracts/pinned.json`, already
pinned): every problem type the case-breaks operation documents must be classified, every documented slug the
Idempotency-Key text says is not replayed must be retryable, and the audit is proved to fail on a drifted table. A
copy of the callee's ADR 0053 class table (`testdata/inventory-storage-adr-0053-classes.tsv`, 81 problem types, 44
FINAL, 2 OUTCOME, 35 RETRYABLE) is compared entry by entry. The pin cannot enumerate the callee's catalogue, so a
refusal that no response text names is held by the table's own entries and the ADR copy.

## Consequences

- A refusal that can pass later is no longer shown to the operator as a final refusal: the floor client keeps the
  command and re-sends it with the same key until it passes or the claim lapses. A 409 `case-break-rejected` now
  means a refusal inventory-storage stores.
- The status of an operator-visible answer changes from 409 to 503 for a released refusal. Both are documented on
  the operation; clients that branch on the status see a 5xx where they used to see a 4xx (the descriptions are
  updated; no schema changes).
- `circuit_breaker_state{dependency="inventory-storage"}` no longer counts refusals as failures.
- Tests: `refusal_classes_test.go` (table against the pin and the ADR copy), `client_refusal_test.go` (slug reading,
  retry under the same key, bound, circuit stays closed), `usecases/complete_case_break_test.go` (a released refusal
  keeps the task claimed and a later success completes with the same key; a final refusal stays a rejection; the lease
  ends a refusal that never passes).

## Open points (reported, not changed)

- A persisted attempt count or last-refusal record per case break (as vendor-connectivity ADR 0016 keeps
  `submissions.last_refusal_*`) would need a migration on `tasks`; not made. The lease is the only bound across calls.
- A machine-readable pending reason on `case-break-unavailable` (the callee's slug as a field, or its own problem type
  such as `case-break-refused-retryable`) would be a contract change of `apis/openapi.yaml`'s `Problem` or catalogue.
- The 503 title still reads "inventory-storage could not be reached for the case break"; the title is part of the
  catalogue and is left as is.
- `location-classification-unavailable` should be a 503 in inventory-storage; this caller no longer depends on it.
