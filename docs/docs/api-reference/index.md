---
id: index
title: API Reference
sidebar_label: Overview
sidebar_position: 1
slug: /api-reference/
description: Every REST endpoint and the event contract — conventions, status-code mapping, and RFC 7807 error shapes.
---

# API Reference

This service exposes **two contracts**, both of which live in `apis/` in the
repository and are linted by Spectral on every push:

| Contract | File | Rendered here |
| --- | --- | --- |
| Synchronous REST | `apis/openapi.yaml` (OpenAPI 3.0.3) | [REST API](./rest/fulfillment-execution-api.info.mdx) — generated directly from the spec |
| Asynchronous events | `apis/asyncapi.yaml` (AsyncAPI 2.6.0) | [Events](./events.md) — hand-authored from the spec |

The REST pages under **REST API** are generated from the real spec file at
build time by `docusaurus-plugin-openapi-docs`. They are not transcribed by
hand, so they cannot drift from the contract that CI validates.

## Endpoint coverage: 17 / 17

Cross-checked against `internal/adapters/inbound/http/router.go`:

| Method | Path | Use case | In `openapi.yaml`? |
| --- | --- | --- | --- |
| `POST` | `/tasks` | `CreateTask` | Yes |
| `GET` | `/tasks?orderRef=` | `GetTasksByOrderRef` ([ADR-0013](../adr/0013-fulfillment-mfe-console-adoption.md)) | Yes |
| `POST` | `/stations` | `RegisterStation` (optional `locationCode`, [ADR-0024](../adr/0024-station-location-code-and-workcenter-role-check.md)) | Yes |
| `POST` | `/stations/{stationId}/claim-next` | `ClaimNext` | Yes |
| `POST` | `/stations/{stationId}/check-in` | `CheckInStation` | Yes |
| `POST` | `/stations/{stationId}/check-out` | `CheckOutStation` | Yes |
| `POST` | `/tasks/{id}/renew-lease` | `RenewLease` | Yes |
| `POST` | `/tasks/{id}/complete` | `CompleteTask` | Yes |
| `POST` | `/tasks/{id}/seal-package` | `SealPackage` | Yes |
| `POST` | `/packages/{id}/slam` | `RunSlam` | Yes |
| `GET` | `/queues/{taskType}/depth` | `GetQueueDepth` | Yes |
| `GET` | `/capacity/{capability}` | `GetInstalledCapacity` ([ADR-0018](../adr/0018-installed-capacity-read-endpoint.md)) | Yes |
| `POST` | `/tasks/expire-leases` | `ExpireLeases` | Yes |
| `POST` | `/tasks/sweep-cpt-misses` | `SweepCPTMisses` ([ADR-0025](../adr/0025-cpt-missed-sweep-and-package-manifested.md)) | Yes |
| `POST` | `/rebin/arrivals` | `ArriveAtRebin` ([ADR-0016](../adr/0016-rebin-and-order-consolidation.md)) | Yes |
| `GET` | `/healthz` | *(liveness)* | Yes |
| `GET` | `/readyz` | *(readiness, [ADR-0029](../adr/0029-resilience-circuit-breakers-retry-dlq-shutdown.md))* | Yes |

Every route on the router is declared in the spec and every spec operation
is a real route. That two-way match is enforced by
`internal/adapters/inbound/http/openapi_routes_test.go` (it walks the chi
router and diffs it against `paths:` in `apis/openapi.yaml`), which covers the
one kind of drift the PR-time `docs-api-drift` check cannot see: a live route
the spec omits. `POST /rebin/arrivals` was that kind of drift until it was
added to the spec under its own **Rebin** tag.

The read-only analytics endpoints (`GET /reports/throughput`,
`GET /reports/throughput/freshness`) are served by the separate
`cmd/fulfillment-reports` process — see
[Throughput report](../analytics/throughput-report.md).

No route or MCP tool is authenticated:
[ADR-0022](../adr/0022-remove-rest-mcp-auth.md) removed the static-bearer
layer and supersedes ADR-0021.

## Conventions

**Richardson Maturity Level 2, deliberately not Level 3.** Resource nouns,
correct verbs, correct status codes, `Location` headers on creation — but no
HATEOAS. Clients are expected to know the URL structure rather than discover
it from response links, and the spec says so explicitly.

**Actions are POSTs on sub-resources.** `claim-next`, `renew-lease`,
`complete`, `seal-package`, `slam` and `expire-leases` are state transitions
with side effects, not resource replacements — so they are `POST` on a
sub-path, not `PUT` on a field. `POST /stations/{stationId}/claim-next` in
particular reads correctly as "this station asks for its next task," which is
the pull semantics made visible in the URL.

**Domain structs never leak.** Every request and response body is a DTO
defined in `internal/adapters/inbound/http/dto.go`. Aggregates are unexported
internally and are mapped explicitly on the way out.

**Validation happens twice, for different reasons.** DTO-level validation
catches malformed requests (missing `stationId`, empty `type`) and returns
`400`. Domain-level invariants (`ErrNoScannedContents`, `ErrCapabilityMismatch`)
return `422` — the request was well-formed, the *operation* was not allowed.
`sealPackageRequest.validate()` shows the line clearly: it checks `stationId`
is present but deliberately does **not** check that `contents` is non-empty,
because "you cannot seal an empty carton" is a domain rule, not a syntax
error.

## Errors: RFC 7807 `application/problem+json`

Every error response — without exception — is an RFC 7807 problem document:

```json
{
  "type": "https://errors.fulfillment-execution.warehouse-systems.dev/task-already-claimed",
  "title": "Task already claimed by another station",
  "status": 409,
  "detail": "task: already claimed",
  "instance": "/tasks/task-8a1f/complete"
}
```

`type` is a stable identifier a client can branch on — a URI, but not a
fetchable document (RFC 7807 §3.1 explicitly permits this). `title` is the
category-level human summary; `detail` is the specific error text; `instance`
is the request path that produced it. On bare collection-create endpoints
(`POST /tasks`, `POST /stations`) `instance` is omitted, because the path
identifies no specific resource.

Rationale is in [ADR-0005](../adr/0005-rfc-7807-problem-details.md).

### Status-code mapping

The mapping lives in `internal/adapters/inbound/http/errors.go` and is
exhaustive over the typed error set.

| Status | Meaning here | Errors mapped |
| --- | --- | --- |
| `400` | Malformed or incomplete request body | invalid JSON, missing required DTO field (`invalid-request`) |
| `404` | Referenced aggregate does not exist | `ErrTaskNotFound`, `ErrStationNotFound`, `ErrPackageNotFound` |
| `409` | Well-formed request, but the aggregate's **state** forbids it | `ErrAlreadyClaimed`, `ErrAlreadyCompleted`, `ErrNotClaimed`, `ErrNotOwner`, `ErrOccupied`, `ErrNotOccupied`, `ErrAlreadySealed`, `ErrAlreadyProcessed`, `ErrNotSealed`, `ErrPackageSegregationViolation`, `ErrNoClaimableTask` |
| `422` | Well-formed request, but the **content** violates a domain rule | `ErrCapabilityMismatch` (task and station), `ErrNoScannedContents`, `consolidation.ErrUnknownLine`, `ErrWrongTaskType`, `ErrStationLocationNotWorkCenter` |
| `500` | Anything unmapped | fallback (`internal-error`) |

The `409` versus `422` split is the one worth internalising: `409` means *"try
again later or against different state"* — the task is claimed **now**, the
lease may lapse and free it. `422` means *"this will never work as asked"* —
the station lacks `hazmat` certification and retrying changes nothing.

`ErrNoClaimableTask` is mapped to `409` rather than `404` on purpose: the pool
is a valid resource that is momentarily empty of work this station can do, not
a missing resource. An idle station polls again; it does not treat the answer
as a permanent failure.

### Every problem type

| Slug | Status | Title |
| --- | --- | --- |
| `invalid-request` | 400 | The request is malformed or missing a required field |
| `task-not-found` | 404 | Task not found |
| `station-not-found` | 404 | Station not found |
| `package-not-found` | 404 | Package not found |
| `task-already-claimed` | 409 | Task already claimed by another station |
| `task-already-completed` | 409 | Task already completed |
| `task-not-claimed` | 409 | Task is not currently claimed |
| `task-not-owner` | 409 | Station does not own the active claim on this task |
| `station-occupied` | 409 | Station is already occupied |
| `station-not-occupied` | 409 | Station is not occupied |
| `package-already-sealed` | 409 | Package already sealed |
| `package-already-processed` | 409 | Package SLAM already processed |
| `package-not-sealed` | 409 | Package must be sealed before SLAM |
| `package-segregation-violation` | 409 | Scanned item's DOT hazard class is incompatible with an already-scanned item |
| `no-claimable-task` | 409 | No claimable task for station capabilities |
| `task-capability-mismatch` | 422 | Station capabilities do not match task requirements |
| `station-capability-mismatch` | 422 | Capabilities do not match |
| `package-no-scanned-contents` | 422 | Cannot seal a package without scanned contents |
| `rebin-unknown-line` | 422 | Line is not part of this order's required consolidation set |
| `wrong-task-type` | 422 | Wrong task type for this operation |
| `station-location-not-workcenter` | 422 | Station's locationCode does not resolve to a facility-layout WorkCenter |
| `internal-error` | 500 | Internal server error |

All types share the base URI
`https://errors.fulfillment-execution.warehouse-systems.dev/`.

## Not on this API

- **Work release.** `wes-work-planning` decides what to release; it arrives
  over Kafka, not HTTP.
- **Inventory reservations.** `inventory-storage` owns stock truth; this
  service only reads per-SKU hazard classification from it (opt-in).
- **WCS / equipment commands.** A separate command channel, not built.
