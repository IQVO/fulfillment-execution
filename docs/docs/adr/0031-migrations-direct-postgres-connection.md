---
id: 0031-migrations-direct-postgres-connection
slug: /adr/0031-migrations-direct-postgres-connection
title: 31. Run golang-migrate against a direct Postgres connection, not PgBouncer
sidebar_label: 31. Migrations bypass PgBouncer
description: "ADR 0031 — direct port of order-management ADR-0029 (PR #115): golang-migrate's postgres driver takes a session-scoped pg_advisory_lock to serialize concurrent migration runs, which is incompatible with PgBouncer's transaction-pooling mode (warehouse-infra PR #43). Two or more fulfillment-execution replicas (cmd/execution or cmd/mcp) starting concurrently — HPA scale-out (ADR-0030) or an ordinary rolling deploy — crash-loop until one wins the advisory-lock race. Fix: a second env var, MIGRATIONS_DATABASE_URL, carries a direct (non-pooled) connection string used ONLY for the migration step; DATABASE_URL/the runtime pgxpool is untouched and keeps going through PgBouncer."
---

# 31. Run golang-migrate against a direct Postgres connection, not PgBouncer

## Status

Accepted — implemented in the same change that introduces this record. This
is a direct port of
[order-management ADR-0029](https://github.com/claudioed/order-management/blob/develop/docs/docs/adr/0029-migrations-direct-postgres-connection.md)
(PR [#115](https://github.com/claudioed/order-management/pull/115)), the
reference implementation for this fleet-wide fix, applied to
`fulfillment-execution` as part of the Phase 4 fan-out. The companion
`warehouse-infra` PR (#44) already provisions the `MIGRATIONS_DATABASE_URL`
secret key for all 9 OLTP services, including this one — no further
`warehouse-infra` work is needed for this repo.

## Context

`warehouse-infra`'s PgBouncer rollout (PR #43, Phase 3) repointed every one
of the fleet's 9 OLTP services' `DATABASE_URL` secret at PgBouncer, in
**transaction-pooling** mode (`pool_mode = "transaction"`). That's the
correct mode for this fleet's steady-state traffic — application code never
holds session state across statements.

What PR #43 did not carve out: **migrations**. Both of this service's
Postgres-backed binaries run golang-migrate's postgres driver
(`github.com/golang-migrate/migrate/v4/database/postgres`) against the same
`DATABASE_URL` at process startup, before serving any traffic:

- `cmd/execution/main.go`'s `openStorage` (the OLTP API + WorkReleased
  consumer composition root)
- `cmd/mcp/main.go`'s `buildTaskRepo` (the MCP server composition root)

golang-migrate's postgres driver calls `SELECT pg_advisory_lock($1)` to
serialize concurrent migration runs — by design, so that if two processes
start at once and both try to run the same migration, whichever loses the
lock blocks rather than races.

`pg_advisory_lock` is **session-scoped**: the lock is held by whichever
physical backend connection issued it. PgBouncer's transaction-pooling mode
does not preserve that mapping — each statement in a client's logical
session can be routed to a different physical backend connection, because
the client's backend connection is returned to the pool the instant its
transaction commits. So two replicas (`cmd/execution` or `cmd/mcp`)
starting concurrently — this service's own HPA scale-out event, ADR-0030,
or an ordinary rolling ArgoCD deploy with `replicas > 1` — can each land
their migration-lock statements on different physical backends, so the
advisory lock never behaves as a real mutex. The losing replica crash-loops
with errors like `pq: unnamed prepared statement does not exist` or `pq:
canceling statement due to statement timeout` for roughly 1-2 minutes until
the race resolves.

This is the exact fleet-wide bug order-management's ADR-0029 documents and
fixes (found and live-verified there during Phase 4 cleanup); this ADR
ports that same fix to `fulfillment-execution`'s two migration call sites.
It blocks safely enabling `mcp`-excluded caveats aside, this service's own
`api` HPA (ADR-0030) for any workload whose replica count can exceed 1.

## Decision

Give this service a **second** connection string, `MIGRATIONS_DATABASE_URL`
— a direct (non-pooled, session-mode) Postgres connection string, same
user/password/dbname as `DATABASE_URL`, pointed at Postgres itself rather
than PgBouncer — used **only** for the golang-migrate startup step in both
`cmd/execution` and `cmd/mcp`. `DATABASE_URL` and the pgxpool built from it
are completely unchanged: every request this service serves still goes
through PgBouncer in transaction-pooling mode, exactly as PR #43 set up.

`warehouse-infra` PR #44 already provisions `MIGRATIONS_DATABASE_URL` as a
new key alongside the existing `DATABASE_URL` key in each of the 9
services' database Secrets, including this one's
`fulfillment-execution-db`. This repo's `cmd/execution/main.go` and
`cmd/mcp/main.go` (both run migrations) now read `MIGRATIONS_DATABASE_URL`
for the migration step only:

```go
migrationsDatabaseURL := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
...
openStorage(rootCtx, logger, databaseURL, migrationsDatabaseURL, "migrations")   // cmd/execution
buildTaskRepo(rootCtx, databaseURL, migrationsDatabaseURL, "migrations", logger) // cmd/mcp
...
postgres.Migrate(migrationsDatabaseURL, migrationsPath)  // migrations only
// the pgxpool opened right after this still uses databaseURL, unchanged
```

The fallback to `databaseURL` when `MIGRATIONS_DATABASE_URL` is unset keeps
every environment that doesn't provision the split — local dev, CI
integration tests, or any cluster whose Terraform predates this fix —
working exactly as before, byte-for-byte.

`charts/fulfillment-execution`: a new `database.migrationsExistingSecretKey`
value (default `"MIGRATIONS_DATABASE_URL"`) renders the env var in both the
`api` (`templates/deployment.yaml`) and `mcp` (`templates/mcp-deployment.yaml`)
Deployments — the two Deployments whose binaries run migrations — sourced
from the same `database.existingSecret`, with `optional: true` on the
`secretKeyRef` so a secret that predates this chart version still starts
the pod. `templates/projector-deployment.yaml` and
`templates/reports-deployment.yaml` are intentionally untouched: neither
`cmd/fulfillment-projector` nor `cmd/fulfillment-reports` runs
golang-migrate against the OLTP `DATABASE_URL` (the projector migrates
`ANALYTICS_DATABASE_URL`, a DSN this fleet's PR #43 already left unpooled;
the reports binary is read-only and runs no migrations at all).

### Why the analytics-projector's migration is out of scope here

`cmd/fulfillment-projector` also runs golang-migrate (against
`ANALYTICS_DATABASE_URL` and `migrations/analytics`), but PR #43 already
carved analytics DSNs (`analytics_database_urls` in
`warehouse-infra`'s `terraform/locals.tf`) out of PgBouncer entirely — they
are direct, non-pooled connections today. The `pg_advisory_lock`/
transaction-pooling mismatch this ADR fixes does not apply there, so no
`MIGRATIONS_DATABASE_URL` split is needed for the projector.

### Why not just make PgBouncer's pool_mode session for this fleet?

Rejected, for the same reason order-management ADR-0029 rejects it: session
pooling would fix the advisory-lock problem but throws away the entire
point of PgBouncer for this fleet's traffic.

### Why not just remove the advisory lock / skip migrations on non-leader replicas?

Rejected, for the same reason order-management ADR-0029 rejects it: the
advisory lock is the right mechanism given a session-scoped connection —
the bug is the pooling-mode mismatch, not the mechanism. An init-container
Job was considered and rejected as a bigger architectural change for the
same outcome this two-line env-var fallback already achieves.

## Consequences

- **Fixes** the fleet-wide crash-loop bug for `fulfillment-execution`'s two
  Postgres-backed binaries (`cmd/execution`, `cmd/mcp`).
- **No runtime behavior change**: `DATABASE_URL` is untouched, so
  request-serving connection pooling, `pool_mode`, and PgBouncer's own
  configuration are all unaffected by this change.
- **No behavior change for environments without the split**: the
  `getenv("MIGRATIONS_DATABASE_URL", databaseURL)` fallback means local dev
  and CI integration tests keep using `DATABASE_URL` for everything,
  exactly as before.
- **Unblocks the `api` HPA (ADR-0030)** for any scale-out beyond 1 replica:
  this was a latent blocker on the same class of event ADR-0030's
  autoscaler triggers.
- One more secret key to keep in sync per service going forward;
  mechanically generated by Terraform from the same `local.services` map as
  `DATABASE_URL` already is (`warehouse-infra` PR #44), so there's no new
  per-service manual step.

## Verification

- `go build ./...`, `go vet ./...` clean.
- New regression tests (mirroring order-management PR #115's test names 1:1):
  - `cmd/execution/migrations_test.go`:
    `TestMigrationsDatabaseURLFallback` (both directions of the `getenv`
    fallback) and
    `TestOpenStorage_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations`
    (proves `openStorage` itself threads `migrationsDatabaseURL` into the
    migration step, not `databaseURL`, via a distinctive parse-error
    assertion), plus the pre-existing-pattern
    `TestOpenStorage_RetriesTheDatabaseNotJustOnce` and
    `TestOpenStorage_NoDatabaseURLUsesMemoryImmediately` updated for the
    new signature.
  - `cmd/mcp/migrations_test.go`: the same two new tests
    (`TestMigrationsDatabaseURLFallback`,
    `TestBuildTaskRepo_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations`)
    against the extracted `buildTaskRepo` function, plus
    `TestBuildTaskRepo_NoDatabaseURLUsesMemoryImmediately`.
- `helm lint charts/fulfillment-execution` passes; `helm template` with
  `database.existingSecret` set confirms `MIGRATIONS_DATABASE_URL` renders
  with `optional: true` in both the `api` and `mcp` Deployments and is
  absent from the `analytics-projector`/`analytics-reports` Deployments.
- Full local quality gate (`make check`, `make check-all`, `make
  integration`) run and green — see this ADR's companion PR description for
  the exact run.
