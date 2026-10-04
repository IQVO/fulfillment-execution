---
id: 0005-rfc-7807-problem-details
title: 5. RFC 7807 problem+json for every error response
sidebar_label: 5. RFC 7807 error format
sidebar_position: 5
description: Replace the bespoke {"error":"..."} error body with RFC 7807 Problem Details, giving clients a stable machine-readable type to branch on.
---

# 5. RFC 7807 `application/problem+json` for every error response

## Status

**Accepted.** Delivered as Stage 2 of the REST API hardening work
(`REST_API_TASK.md`), which also brought the API to Richardson Maturity
Level 2, added request validation, and produced `apis/openapi.yaml`.

## Context

The HTTP adapter originally returned a bespoke body:

```json
{"error": "task: already claimed"}
```

with the status code carrying all the machine-readable meaning. Two problems
with that, both concrete for this particular API.

**First, the status code is not specific enough to act on.** This service maps
**eleven distinct typed errors to `409`** (ten when this ADR was written; the
pack segregation check, ADR-0010, added `ErrPackageSegregationViolation`) —
`ErrAlreadyClaimed`,
`ErrAlreadyCompleted`, `ErrNotClaimed`, `ErrNotOwner`, `ErrOccupied`,
`ErrNotOccupied`, `ErrAlreadySealed`, `ErrAlreadyProcessed`, `ErrNotSealed`,
`ErrPackageSegregationViolation`,
`ErrNoClaimableTask`. A station client genuinely needs to distinguish them:

- `no-claimable-task` → the pool is momentarily empty for me; **poll again**.
- `task-already-claimed` → another station beat me; **ask for a different
  task**.
- `task-not-owner` → my lease lapsed and someone else has it; **stop working
  and re-claim**.

Three completely different client behaviours behind one status code. With only
`{"error": "..."}` to go on, the client's only option is substring-matching an
English message — which breaks the moment anyone rewords it.

**Second, a bespoke shape is one more thing to document and learn.** The API
already had four distinct error status codes and nineteen error conditions
(counted when this ADR was written).

Forces:

- Clients are station devices and sibling services, not browsers — they need
  to branch programmatically.
- Error text is prose and will be reworded; clients must not depend on it.
- The API is documented in OpenAPI, so the error body needs a schema.
- A standard, if one fits, beats a local invention.

## Decision

**We will return an RFC 7807 `application/problem+json` document for every
error response, without exception.**

```json
{
  "type": "https://errors.fulfillment-execution.warehouse-systems.dev/task-already-claimed",
  "title": "Task already claimed by another station",
  "status": 409,
  "detail": "task: already claimed",
  "instance": "/tasks/task-8a1f/complete"
}
```

- **`type`** — the stable machine-readable identifier, and the field clients
  branch on. A URI under
  `https://errors.fulfillment-execution.warehouse-systems.dev/`, which is an
  *identifier, not a fetchable document* — RFC 7807 §3.1 explicitly permits
  this.
- **`title`** — the category-level human summary. Stable per type.
- **`status`** — mirrors the HTTP status.
- **`detail`** — the specific error text. Prose; clients must not parse it.
- **`instance`** — the request path that produced the error. **Omitted** on
  bare collection-create endpoints (`POST /tasks`, `POST /stations`) where the
  path identifies no specific resource instance.

`Content-Type: application/problem+json` on every error response.

The mapping lives in `internal/adapters/inbound/http/errors.go`: an exhaustive
`statusFor(err)` switch for the status and a `problemCatalog` table (probed by
`problemTypeAndTitle(err)`) for the type/title pair — both keyed on
`errors.Is` against the typed domain and application errors. (The ADR was
written when both were switches; the table replaced the second one with
identical behaviour.) **Twenty typed errors are catalogued** today (3 × `404`,
11 × `409`, 6 × `422`); `internal-error`, `invalid-request` and the
Idempotency-Key types of ADR-0028 are emitted on top of those. The full table
is in the [API Reference](../api-reference/index.md).

### The status-code semantics this pinned down

Writing the mapping out exhaustively forced two distinctions to be made
explicit rather than case-by-case:

**`409` versus `422`.** `409` means *"try again later, or against different
state"* — the task is claimed **now**; its lease may lapse and free it. `422`
means *"this will never work as asked"* — the station lacks the `hazmat`
certification and retrying changes nothing. State conflicts get `409`;
content violations get `422`.

**`400` versus `422`.** DTO validation failures (missing `stationId`, empty
`type`, malformed JSON) are `400` — the *request* is wrong. Domain-invariant
violations are `422` — the request was well-formed, the *operation* was not
allowed. `sealPackageRequest.validate()` shows the line precisely: it requires
`stationId` but deliberately does **not** require non-empty `contents`,
because "you cannot seal an empty carton" is `pack.ErrNoScannedContents`, a
domain rule worth a `422`, not a syntax error worth a `400`.

**`ErrNoClaimableTask` is `409`, not `404`.** The pool is a valid resource that
is momentarily empty of work this station can do — not a missing resource. An
idle station polls; it does not treat the answer as permanent failure.

## Consequences

### Easier

- **Clients can branch reliably.** `type` is stable; the eleven `409` conditions
  are now distinguishable without reading English.
- **The error contract is documentable and lintable.** A single `Problem`
  schema in `apis/openapi.yaml`, referenced by every error response, checked
  by Spectral in CI.
- **Adding an error type is a two-line, obvious change.** One case in
  `statusFor`, one in `problemTypeAndTitle`. The two switches mirroring each
  other makes an omission visible in review.
- **No local invention to explain.** RFC 7807 is a standard with existing
  client-side support.
- **The status-code discipline is now written down.** The `409`/`422`/`400`
  rules above are consequences of doing this exhaustively, and they outlast
  the format decision itself.

### Harder

- **It was a breaking change.** Any client parsing `{"error": "..."}` had to
  change. Acceptable here because consumers are internal and the migration was
  done in one pass, but it is a real cost.
- **A switch and a table must be kept in agreement.** `statusFor` and
  `problemCatalog` (via `problemTypeAndTitle`) are separate structures over the
  same error set. They
  agree today; nothing in the compiler enforces it. A single table keyed by
  error would be safer, and the current shape is the readability trade-off
  that was chosen.
- **Type URIs are a public commitment.** Once a client branches on
  `…/no-claimable-task`, renaming that slug is a breaking change even though
  it looks like a cosmetic string.
- **A wrong `500` is now indistinguishable in shape.** Any unmapped error
  falls through to `internal-error`, which looks like a deliberate problem type
  to a client. `statusFor` being exhaustive over the typed errors, and
  `problemCatalog` mirroring it, is what
  keeps that branch rare.
- **The `instance` rule has an exception.** Omitting it on bare
  collection-create endpoints is defensible under the RFC but is a special
  case that has to be remembered when adding a new collection endpoint.
