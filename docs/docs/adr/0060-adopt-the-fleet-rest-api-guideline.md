---
id: 0060-adopt-the-fleet-rest-api-guideline
slug: /adr/0060-adopt-the-fleet-rest-api-guideline
title: 0060. Adopt the fleet REST API guideline
sidebar_label: 0060. Fleet REST API guideline
description: "ADR 0060 - fulfillment-execution adopts the mandatory fleet REST API guideline as a hard cutover: noun routes (no verb segments), every POST idempotent behind one global Idempotency-Key middleware (409 idempotency-key-reused, Idempotency-Replayed), and keyset pagination with the {items,nextCursor} envelope on the two collections."
---

# 0060. Adopt the fleet REST API guideline

## Status

**Accepted**. Amends [ADR-0028](./0028-idempotency-key-middleware.md) (key reuse
is `409`, not `422`; the middleware now wraps every `POST`), [ADR-0033](./0033-package-read-model.md)
(`GET /packages` is paged) and [ADR-0055](./0055-scan-idempotency-and-claimable-break-case.md).
Route names in older ADRs are rewritten to the new routes; their decisions are unchanged.

## Context

The fleet-wide REST guideline (warehouse-docs, `STANDARD.md`) is mandatory for every
`apis/openapi.yaml`. This service diverged from it in three ways: ten commands were verb routes
(`/claim-next`, `/expire-leases`, ...), only `POST /tasks` and `POST /tasks/{id}/scans` were
idempotent (13 other POSTs were unkeyed), key reuse answered `422`, and both collections
(`GET /tasks?orderRef=`, `GET /packages?orderRef=`) returned a bare array with no paging.

## Decision

A hard cutover: no aliases, no dual routes, no toggles. Enforced by the `rest-guideline` Spectral
rule in `.spectral.yaml` (R1-R8) in the `api-lint` job, and by integration tests.

**Noun routes (R1, R2).** A command is a thing the domain records or a lifecycle-transition
resource. `operationId`s are unchanged, so MCP/A2A mappings do not move; status codes and bodies
are unchanged.

| Old | New |
|---|---|
| `POST /stations/{stationId}/task-claims` | `POST /stations/{stationId}/task-claims` |
| `POST /stations/{stationId}/check-ins` | `POST /stations/{stationId}/check-ins` |
| `POST /stations/{stationId}/check-outs` | `POST /stations/{stationId}/check-outs` |
| `POST /tasks/{id}/lease-renewals` | `POST /tasks/{id}/lease-renewals` |
| `POST /tasks/{id}/completions` | `POST /tasks/{id}/completions` |
| `POST /tasks/{id}/case-breaks` | `POST /tasks/{id}/case-breaks` |
| `POST /tasks/{id}/package-seals` | `POST /tasks/{id}/package-seals` |
| `POST /packages/{id}/slam-runs` | `POST /packages/{id}/slam-runs` |
| `POST /tasks/expire-leases` | `POST /task-lease-expirations` |
| `POST /tasks/sweep-cpt-misses` | `POST /cpt-miss-sweeps` |

**Idempotency (R3-R6, R8).** Every `POST` (there is no `PATCH`; `PUT`/`DELETE` do not exist
here) is route-scoped behind `RequireIdempotencyKey`, including the sweeps and
`POST /paths/{pathId}/work-requests` (one logical pull, one key; a second pull mints a new key).

- `Idempotency-Key` is 1-128 printable ASCII: missing is `400 idempotency-key-required`,
  malformed (including over 128 characters) is `400 idempotency-key-invalid`.
- The key is global: the store row is looked up by key alone and compared by fingerprint =
  method + path + SHA-256(body). Different fingerprint: `409 idempotency-key-reused`.
- A replay returns the stored status, headers (`Location`) and body plus `Idempotency-Replayed: true`;
  the first execution never carries it. `Idempotent-Replayed` is retired.
- Persistence is unchanged and already correct: the `idempotency_keys` row is written in the
  same transaction as the aggregate change and the outbox rows (the request context carries the
  transaction, `UnitOfWork.Execute` joins it). 2xx and 4xx are stored and replayed; a 5xx now
  rolls the whole unit back and is **never stored**, so the retry re-executes. Concurrent
  same-key requests serialise on the primary-key index.
- The sweep CronJob (`cmd/sweep`) mints one key per pass and reuses it across that pass's retries.

**Collections (R7).** `GET /tasks?orderRef=` and `GET /packages?orderRef=` take `limit` (1-500,
default 100) and `cursor` and answer `{items,nextCursor}`. Both are ordered by the primary key
`id`, which is unique, so the keyset position is `(id, id)` and ties cannot occur. Invalid
`limit` is `400 invalid-limit`, an undecodable `cursor` `400 invalid-cursor`, and any other query
parameter (the retired `after`, `offset`, `nextToken`) `400 unknown-query-parameter`.
The helper is the fleet `page` package (`internal/adapters/inbound/http/page`).

**Exemptions:** none. `GET /queues/{taskType}/depth` and `GET /capacity/{capability}` return one
object and are not collections.

## Consequences

- Breaking for every client: new URLs, list envelope, key required. Deployed together with the
  rest of the REST cutover set.
- Consumers changed in this repo: the sweep CronJob (`cmd/sweep`, chart), the inventory-storage
  case-break client (`POST /containers/{lpn}/case-breaks`), and the `web/` remote (follows
  `nextCursor`).
- A 5xx stored by the old middleware could be replayed forever; that can no longer happen.
