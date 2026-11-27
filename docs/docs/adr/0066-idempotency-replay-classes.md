---
id: 0066-idempotency-replay-classes
slug: /adr/0066-idempotency-replay-classes
title: 66. Every problem type has a replay class; a retryable refusal is not stored
sidebar_label: "66. Idempotency replay classes"
description: "ADR 0066 — supersedes the stored-refusal design of ADR 0055 (owner decision 2026-11-23): each problem type declares FINAL or RETRYABLE on its catalogue entry; a 2xx and a FINAL refusal are stored and replayed, a RETRYABLE refusal rolls the request back, key row included, so the retry under the same Idempotency-Key executes again. No OUTCOME exists in this service."
---

# 66. Every problem type has a replay class; a retryable refusal is not stored

## Status

Accepted (2026-11-27). Supersedes the stored-refusal design of
[ADR-0055](./0055-scan-idempotency-and-claimable-break-case.md) ("a stored
4xx is replayed too") by the owner decision of **2026-11-23**: the fleet
replay-class rule (owner decision 2026-11-18,
`.claude/rules/fleet/idempotency-and-outbox.md`, standard in
warehouse-harness-template `docs/idempotency-replay-classes.md`) applies to
this service. ADR-0055's Decision text is unchanged; its Consequences and
the floor-client reliance on replayed refusals are superseded as recorded in
the dated addendum on that ADR.

## Context

ADR-0055 kept idempotency keys forever and replayed the stored answer of ANY
non-5xx response, including a business refusal ("a stored 4xx is replayed
too"). That design pins a stale refusal: a scan rejected with
`pick-over-quantity` or `unknown-gtin` — answers that depend on state that
can change — was replayed to a client that did the right thing and retried,
and warehouse-floor-client came to rely on the replay.

The fleet rule: a request retried under the same `Idempotency-Key` is
answered from the store only when its first answer is **final for that
request**. Each problem type a service can answer on an idempotent route has
exactly one **replay class**, declared ON THE CATALOGUE ENTRY:

| Class | Meaning | Stored and replayed |
|---|---|---|
| `FINAL` | the answer is a function of the request alone, or provably irreversible for the identical request | yes |
| `OUTCOME` | the command COMMITTED state and then answered an error that must be remembered | yes, and its writes commit with the stored answer |
| `RETRYABLE` | nothing committed and the answer depends on state that can change | no — the key is released and a retry executes again |

## Decision

- The class lives on the catalogue entry: `problemInfo.class` in
  `internal/adapters/inbound/http/replay_class.go`, declared in
  `problemCatalog` (errors.go, typed domain errors) and in `register(...)`
  (problems.go, every other problem type). A problem type without a class
  fails `TestEveryProblemTypeDeclaresAReplayClass`; a test holds this ADR's
  table equal to the catalogue.
- `writeProblem` marks the middleware's capturing writer with the class of
  the problem it answers. The middleware reads the mark and holds no list of
  slugs and no status ranges beyond 2xx and 5xx: a 2xx is stored, a 5xx or a
  panic never is, a FINAL or OUTCOME refusal is stored, and every other
  answer (a RETRYABLE refusal, or a runtime refusal with no class — the safe
  fallback) rolls the whole transaction back, key row included, exactly like
  the 5xx branch. A replay still carries `Idempotency-Replayed: true`.
- Releasing a RETRYABLE key means a different body under the same key
  afterwards is a NEW request, not `409 idempotency-key-reused`.
- A concurrent duplicate of a keyed request is unchanged: this design claims
  the key row inside the request transaction BEFORE the handler runs, so the
  duplicate blocks on Postgres' unique-index lock until the original
  resolves and then replays the original's stored answer (ADR-0028's
  concurrency argument); a duplicate never answers its own unstored refusal.
- **There is no OUTCOME in this service.** Every use case writes through
  `atomically(...)` (`unit_of_work.go`): an error return rolls the writes
  back with the request transaction, and the middleware's savepoint
  (`handler_run`) discards a handler's partial writes on any failed
  statement. No refusal provably commits state, so no problem type can be
  OUTCOME here. The middleware still implements the OUTCOME branch (an
  OUTCOME refusal commits its writes with the key row; one whose writes were
  lost behind a failed release is a 500, never stored) so the rule's
  vocabulary is complete if a committing refusal is ever added.
- Scan refusals are classified BY CAUSE, not by slug alone: `unknown-gtin`,
  `scan-sku-mismatch`, `scan-level-mismatch` are PackView facts
  product-master events can deliver or correct later (RETRYABLE);
  `pick-over-quantity`, `pick-incomplete`, `no-pick-plan` are task state
  (RETRYABLE); `invalid-scanned-gtin` and `invalid-scan-count` are functions
  of the scanned bytes alone (FINAL).

### Class table (every problem type; held equal to the catalogue by test)

| Slug | Status | Class | Why (code-cited for the non-FINAL ones) |
|---|---|---|---|
| `task-not-found` | 404 | RETRYABLE | tasks are also created by consumed WorkReleased events (`consume_work_released.go`): "not known yet" is possible |
| `station-not-found` | 404 | RETRYABLE | station ids are caller-supplied (`register_station.go`): POST /stations can deliver the station later |
| `package-not-found` | 404 | FINAL | |
| `work-planning-no-pending-work` | 409 | RETRYABLE | peer state: work can be released later |
| `work-planning-unknown-path` | 404 | RETRYABLE | peer state: the peer's path catalogue can learn the path |
| `work-planning-unreachable` | 502 | RETRYABLE | a 5xx is never stored; the peer can come back |
| `work-planning-not-configured` | 503 | RETRYABLE | a 5xx is never stored; configuration can change |
| `path-id-required` | 400 | FINAL | |
| `task-already-claimed` | 409 | RETRYABLE | the claim is a lease: it expires and frees the task |
| `task-already-completed` | 409 | FINAL | |
| `task-not-claimed` | 409 | RETRYABLE | claim state changes (claim, lease expiry) |
| `task-not-owner` | 409 | RETRYABLE | the active claim changes hands |
| `station-occupied` | 409 | RETRYABLE | occupancy changes on check-out |
| `station-not-occupied` | 409 | RETRYABLE | occupancy changes on check-in |
| `package-already-sealed` | 409 | FINAL | |
| `package-already-processed` | 409 | FINAL | |
| `package-not-sealed` | 409 | RETRYABLE | the seal can happen later |
| `package-segregation-violation` | 409 | RETRYABLE | depends on the package's already-scanned contents (state) |
| `no-claimable-task` | 409 | RETRYABLE | the pool changes; the claim-race loser of this service's concurrent-update case |
| `batch-mode-off` | 503 | RETRYABLE | a 5xx is never stored; a configured state, not a fault |
| `batch-not-found` | 404 | FINAL | |
| `station-has-open-batch` | 409 | RETRYABLE | the open batch completes or is abandoned |
| `batch-not-open` | 409 | RETRYABLE | batch lifecycle state |
| `task-capability-mismatch` | 422 | RETRYABLE | station capabilities can be re-registered |
| `station-capability-mismatch` | 422 | RETRYABLE | station capabilities can be re-registered |
| `package-no-scanned-contents` | 422 | RETRYABLE | scans can arrive before the seal |
| `ship-in-own-container-not-single-line` | 422 | RETRYABLE | package contents / order-line facts |
| `ship-in-own-container-quantity-mismatch` | 422 | RETRYABLE | package contents / order-line facts |
| `ship-in-own-container-no-case-level` | 422 | RETRYABLE | a PackView fact product-master can declare later |
| `ship-in-own-container-case-retired` | 422 | RETRYABLE | a PackView fact; product-master can activate another CASE level |
| `ship-in-own-container-case-dimensions-unknown` | 422 | RETRYABLE | a PackView fact product-master can deliver later |
| `rebin-unknown-line` | 422 | RETRYABLE | the consolidation set is built from facts that can arrive later |
| `wrong-task-type` | 422 | FINAL | |
| `station-location-not-workcenter` | 422 | RETRYABLE | facility-layout's role data can change (the lookup fails open on UNKNOWN) |
| `relocation-confirmation-required` | 422 | FINAL | |
| `invalid-relocation-confirmation` | 422 | FINAL | |
| `not-relocation-task` | 422 | FINAL | |
| `replenishment-confirmation-required` | 422 | FINAL | |
| `invalid-replenishment-confirmation` | 422 | FINAL | |
| `invalid-scanned-gtin` | 409 | FINAL | |
| `invalid-scan-count` | 400 | FINAL | |
| `unknown-gtin` | 409 | RETRYABLE | a referenced fact not known yet: the PackView is a local copy product-master events extend |
| `scan-sku-mismatch` | 409 | RETRYABLE | the GTIN→SKU mapping is a PackView fact that can be corrected |
| `scan-level-mismatch` | 409 | RETRYABLE | the GTIN's pack level is a PackView fact that can be corrected |
| `no-pick-plan` | 409 | RETRYABLE | aggregate state not reached yet: the plan can be stamped later |
| `pick-over-quantity` | 409 | RETRYABLE | task state (picked vs planned); a capacity-style refusal |
| `pick-incomplete` | 409 | RETRYABLE | task state |
| `not-case-break-task` | 409 | FINAL | |
| `case-break-completion-required` | 409 | FINAL | |
| `case-break-unavailable` | 503 | RETRYABLE | a 5xx is never stored; inventory-storage can come back |
| `case-break-rejected` | 409 | RETRYABLE | the peer's rejection causes are not knowable locally; unsure → RETRYABLE |
| `invalid-request` | 400 | FINAL | |
| `invalid-deadline` | 422 | FINAL | |
| `malformed-request-body` | 400 | FINAL | |
| `idempotency-key-required` | 400 | FINAL | |
| `idempotency-key-invalid` | 400 | FINAL | |
| `idempotency-key-reused` | 409 | FINAL | |
| `resource-not-found` | 404 | FINAL | |
| `unknown-query-parameter` | 400 | FINAL | |
| `invalid-limit` | 400 | FINAL | |
| `invalid-cursor` | 400 | FINAL | |
| `invalid-report-query` | 400 | FINAL | |
| `internal-error` | 500 | RETRYABLE | a 5xx is never stored |
| `report-store-error` | 500 | RETRYABLE | a 5xx is never stored |
| `dependency-unavailable` | 503 | RETRYABLE | a 5xx is never stored |

### Judgement calls (most consequential first)

1. **`pick-over-quantity` and the other scan refusals are RETRYABLE, so
   ADR-0055's replayed-scan-refusal is gone on purpose.** The floor client
   settles a refused command and never auto-resends a 4xx with the same key
   (verified read-only: `src/lib/commands.ts` — automatic resends happen only
   for no-answer and 429/500/502/503/504), so it needs no change; the
   operator's own next scan under the same key now executes instead of
   replaying the stored refusal. warehouse-floor-client PR #321 (client-side
   retry) stays closed: no retry behaviour is added anywhere.
2. **The 404s split by who mints the id.** `task-not-found` and
   `station-not-found` are RETRYABLE (events or another request can deliver
   the fact); `package-not-found` and `batch-not-found` are FINAL — package
   ids are minted by this service on the seal (`seal_package.go`), batch ids
   on the form (`form_work_batch.go`), and neither aggregate is ever
   deleted, so a 404 on the identical request is irreversible.
3. **The terminal states are FINAL** (`task-already-completed`,
   `package-already-sealed`, `package-already-processed`): no state change
   can make the identical request succeed.
4. **The immutable-fact 4xx are FINAL** (`wrong-task-type`,
   `not-relocation-task`, `not-case-break-task`,
   `case-break-completion-required`): the task's type is stamped at
   creation. The confirmation problems
   (`relocation-confirmation-required`, `invalid-relocation-confirmation`,
   `replenishment-confirmation-required`,
   `invalid-replenishment-confirmation`) are FINAL: the planned move is
   stamped at release, so for the identical request these answers never
   change — the confirmation flow re-sends with a DIFFERENT body under a new
   key.
5. **`case-break-rejected` is RETRYABLE** although inventory-storage's own
   break-case refusals have their own classes there (PR #329): the cause is
   not knowable locally, and the rule's default when unsure is RETRYABLE.
6. **`batch-not-open` is RETRYABLE** although a completed/abandoned batch
   never reopens: the refusal can also fire while the batch is being formed
   or the mode flips, so the mixed-cause rule applies. Splitting it is a
   recorded follow-up.

## Consequences

- A client retrying a refused request under the same key now gets the
  CURRENT answer, not the stored refusal. Callers that reused a key after a
  refusal on purpose get their retry executed.
- `warehouse-floor-client` needs no change (verified read-only, judgement
  call 1). The `break-case:<taskId>` idempotency key this service sends to
  inventory-storage is covered by inventory-storage PR #329; no action here.
- Retention: nothing purges expired `idempotency_keys` rows (migration 0012
  deferred a cleanup job explicitly). Unchanged by this ADR; recorded as an
  open point on the adopting PR.
- The `apis/openapi.yaml` Idempotency text, the README, the REST rule and
  the troubleshooting docs say which answers are replayed (2xx, FINAL) and
  which release the key (RETRYABLE).
