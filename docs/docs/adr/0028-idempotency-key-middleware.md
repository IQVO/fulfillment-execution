---
id: 0028-idempotency-key-middleware
slug: /adr/0028-idempotency-key-middleware
title: 0028. Transactional Idempotency-Key middleware for POST /tasks
sidebar_label: 0028. Idempotency-Key middleware
description: "ADR 0028 — POST /tasks requires a caller-supplied Idempotency-Key header. A route-scoped middleware begins the outer Postgres transaction, joins it with CreateTask's own UnitOfWork via the existing tx-in-context mechanism (internal/pgtx), and lets the database's own unique-index lock do request de-duplication with no polling, no timeout, and no in-progress state. Direct port of order-management ADR-0023, the fleet reference implementation."
---

# 0028. Transactional Idempotency-Key middleware for POST /tasks

## Status

Accepted — implemented in the same change that introduced this record.
Direct port of order-management's ADR-0023 (`POST /orders`) — the fleet
reference implementation this middleware and its tests copy
scenario-for-scenario, applied here to `POST /tasks`.

## Context

Of every mutating route this service exposes —

```
POST /stations
POST /tasks
POST /stations/{stationId}/claim-next
POST /stations/{stationId}/check-in
POST /stations/{stationId}/check-out
POST /tasks/{id}/renew-lease
POST /tasks/{id}/complete
POST /tasks/{id}/seal-package
POST /packages/{id}/slam
POST /tasks/expire-leases
POST /tasks/sweep-cpt-misses
POST /rebin/arrivals
```

— exactly one, `POST /tasks` (`CreateTask`), creates a genuinely NEW
resource with a server-generated id and has no other safety net against
a duplicate on retry. `POST /stations` (`RegisterStation`) takes a
**caller-supplied** `stationId` and is already documented and
implemented as idempotent by construction — "re-registering an existing
stationId... overwrites the capability set rather than erroring" (see
`RegisterStation.Execute`'s own doc comment) — so a retried
`POST /stations` with the same body is already safe without this
middleware. Every other route above either acts on a caller-supplied
`{id}`/`{stationId}` (at worst a redundant no-op or a domain-rejected
double-transition — a separate, narrower lost-update concern, out of
scope here) or is a sweep/batch operation (`expire-leases`,
`sweep-cpt-misses`, `rebin/arrivals`) with different retry semantics
that this ADR deliberately does not force-fit into a per-key contract.

A client that never receives the response to a successful
`POST /tasks` call — a dropped connection, a load-balancer timeout, a
client-side retry policy — has no safe way to tell "my request never
arrived" apart from "my request arrived and succeeded but I never saw
the response," and a naive retry duplicates the task.

ADR-0020 (transactional outbox) already left this service with the
transactional infrastructure this problem needs: `ports.UnitOfWork` +
`postgres.UnitOfWork.Execute`, and a context-based transaction-join
mechanism (`querierFrom`, driven by `withTx`/`txFrom`) that lets a
repo's own SQL detect and join an ALREADY-OPEN outer transaction rather
than starting a second, invisible one. This ADR's core design choice,
identical to order-management's, is to make idempotency bookkeeping
join that SAME transaction rather than build a second, parallel
transactional mechanism — so the whole HTTP-request-to-response cycle
(idempotency bookkeeping, the `Task` aggregate write, the outbox
insert) commits or rolls back as one atomic unit.

`CreateTask.Execute` already wraps its `Tasks.Save` + `Publisher.Publish`
in `atomically(ctx, uc.UnitOfWork, ...)` from the outbox rollout — no
change to the use case itself was needed for this ADR; the join is
proven by `TestIdempotency_FreshKey_CreatesTaskAndRecordsOutcome` and
its siblings, which assert both the `idempotency_keys` row and the
`tasks` row exist/don't-exist exactly as the atomic-commit argument
predicts.

## Decision

### 1. `Idempotency-Key` header, required on `POST /tasks`

A request to `POST /tasks` without an `Idempotency-Key` header gets
`400 application/problem+json` (`idempotency-key-required`). Deliberate
v1 choice, same as order-management: require the header on true
resource-creation endpoints rather than making it optional — an
optional header is trivial for a client to forget to set on exactly the
retry path where it matters most.

### 2. `idempotency_keys` table (migration `0012_idempotency_keys`)

```sql
CREATE TABLE idempotency_keys (
    key              TEXT PRIMARY KEY,
    method           TEXT NOT NULL,
    path             TEXT NOT NULL,
    request_hash     TEXT NOT NULL,
    status_code      INTEGER,
    response_body    BYTEA,
    response_headers JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ
);
CREATE INDEX idx_idempotency_keys_created_at ON idempotency_keys (created_at);
```

Same shape, byte-for-byte, as order-management's. `request_hash` is
`hex(sha256(request body))`. `status_code`, `response_body`, and
`response_headers` start `NULL` and are populated by exactly one
`UPDATE`, in the same transaction that inserted the row, immediately
before that transaction commits (§4).

### 3. `RequireIdempotencyKey` middleware (`internal/adapters/inbound/http/idempotency.go`)

Route-scoped only: `r.With(RequireIdempotencyKey(pool)).Post("/tasks",
h.PostTask)` in `router.go`, gated behind a new `WithIdempotencyPool`
`RouterOption` (nil pool — the in-memory dev/test composition root —
leaves `POST /tasks` unprotected, mirroring every other optional
Postgres-backed capability's nil convention in this repo). Never
`r.Use(...)` on the whole router.

The request body is read fully into memory once (needed both for
hashing and for replaying to the real handler), hashed, and restored via
`io.NopCloser(bytes.NewReader(...))` so downstream JSON decoding in
`PostTask` is completely unaffected.

The middleware begins a `pgxpool` transaction directly (no import of
`internal/adapters/outbound/postgres` needed) and binds it into the
request's `context.Context` via the same mechanism
`postgres.UnitOfWork` uses — see §5 for why this had to be factored
out.

`INSERT INTO idempotency_keys (key, method, path, request_hash) VALUES
($1,$2,$3,$4) ON CONFLICT (key) DO NOTHING` runs inside that
transaction:

- **1 row inserted** (genuinely new key): call the real handler with
  the tx-carrying context, using an `httptest.ResponseRecorder` to
  CAPTURE its status/headers/body rather than writing to the real
  `http.ResponseWriter` yet (§4).
- **0 rows** (a row for this key already exists): roll back this new,
  empty transaction and fall through to a plain, non-transactional
  `SELECT` (§5 explains why this is safe without additional locking):
  - `request_hash` mismatch → `422` (`idempotency-key-reused`).
  - `request_hash` match → a genuine retry: write
    `response_headers`/`status_code`/`response_body` VERBATIM to the
    real `http.ResponseWriter`, without calling the real handler at
    all.

### 4. The no-null-status-code-ever-committed invariant

On the fresh-key path, after the wrapped handler runs against the
recorder:

- **Normal completion (no panic):** within the SAME transaction that
  inserted the bare row, `UPDATE idempotency_keys SET status_code=$1,
  response_body=$2, response_headers=$3, completed_at=now() WHERE
  key=$4`, then `COMMIT`. Only AFTER a successful commit does the
  middleware copy the recorder's headers/status/body onto the real
  `http.ResponseWriter`.
- **Panic:** the middleware recovers it, `ROLLBACK`s the transaction (so
  neither the idempotency row nor any domain write the handler made
  ever becomes visible), and re-panics so the outer chi `Recoverer`
  still produces the service's normal `500`. A panic's outcome is never
  cached.

Same invariant order-management's ADR-0023 §4 documents in full: a
transaction that reads a COMMITTED `idempotency_keys` row can never
observe a `NULL` `status_code` — no in-progress marker, no
client-facing retry-after/409, no polling loop or timeout anywhere in
this design.

### 5. Concurrency: Postgres' own unique-index lock does the serialization

Two concurrent requests carrying the SAME key race on the `INSERT ...
ON CONFLICT (key) DO NOTHING` above. Postgres serializes them at the
primary-key unique index: the SECOND (and every later) inserter's
statement BLOCKS until the FIRST inserter's transaction resolves —
commits or rolls back. So by the time ANY transaction observes
`rowsAffected() == 0`, the ORIGINAL inserting transaction has
unconditionally finished, and combined with §4's invariant its outcome
is either fully populated (committed) or the row doesn't exist at all
(rolled back, and this caller's own blocked insert instead succeeds).
No polling loop, no lock-retry budget, and no timeout are needed
anywhere in this code. Proven with a real test, not asserted from
theory: `TestIdempotency_Concurrent_SameKeySameBody_ExactlyOneTaskCreated`
fires five real goroutines at the real chi router with the same key and
body and asserts, via a direct DB count, that exactly one `tasks` row
exists afterward.

### 6. Reusing, not duplicating, the transaction-join mechanism (`internal/pgtx`)

The mechanism the middleware needs to join
(`withTx`/`txFrom`/`querierFrom`, `postgres.UnitOfWork.Execute`)
pre-existed as unexported symbols inside
`internal/adapters/outbound/postgres/unit_of_work.go`. The middleware
lives in `internal/adapters/inbound/http`, and this repo's architecture
fitness tests (`internal/architecture`) forbid the inbound HTTP adapter
from importing the outbound Postgres adapter (and vice versa). Reusing
the mechanism unchanged was therefore impossible without either
violating that boundary or inventing a second, parallel mechanism.

The fix, identical to order-management's: extract the bare
key-type-plus-`WithTx`/`TxFrom` pair into a new, tiny, dependency-free
package, `internal/pgtx`, that both adapter packages import.
`postgres.withTx`/`postgres.txFrom` now delegate to
`pgtx.WithTx`/`pgtx.TxFrom` — an internal refactor with no change in
observable behaviour for any existing caller. The idempotency
middleware calls the exact same `pgtx.WithTx`/`pgtx.TxFrom` functions.
This means `CreateTask`'s own `atomically()`/`UnitOfWork.Execute` call
needed **zero changes** to pick up the middleware's transaction —
`Execute`'s existing "already in a transaction? just run `fn(ctx)`"
branch already does the right thing once fed a `pgtx`-bound context
from any source, not just its own.

### 7. Response caching scope: every normal response, including business errors

The middleware caches every NORMAL (non-panic) response the wrapped
handler produces — including a `4xx` business-logic/validation error
(e.g. an invalid `type`). A client retrying the exact same key + body
deterministically gets the exact same answer, including a validation
error, rather than re-running the same validation. Proven by
`TestIdempotency_BusinessErrorResponse_IsCachedAndReplayed`.

## Consequences

- `POST /tasks` now requires an `Idempotency-Key` header; every existing
  client/BDD/contract-test caller of that route needs one on every call.
- The whole request cycle — idempotency bookkeeping, `Task` aggregate
  write, outbox insert — is one Postgres transaction; a failure
  anywhere in that cycle after the idempotency row's `INSERT` rolls
  back the ENTIRE cycle, including the idempotency row itself. A
  retried request after such a failure re-attempts the real work from
  scratch.
- `internal/pgtx` is a new, tiny shared package in this repo, mirroring
  order-management's package of the same name — any future
  cross-cutting-transaction feature here should extend it rather than
  re-invent a parallel tx-in-context mechanism.
- `POST /stations` is deliberately NOT wired behind this middleware —
  it is already idempotent on a caller-supplied `stationId` (see
  Context). If a future change makes station ids server-generated, this
  middleware should be reconsidered for that route too.
- **Known follow-up, explicitly deferred:** no TTL/cleanup job exists
  yet for old `idempotency_keys` rows. `idx_idempotency_keys_created_at`
  exists specifically so a future scheduled job can find old rows
  without a full table scan — building that job is out of scope here,
  same deferral order-management's ADR-0023 recorded.
