---
id: 0030-horizontal-autoscaling-and-pgxpool-tuning
slug: /adr/0030-horizontal-autoscaling-and-pgxpool-tuning
title: 30. Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning
sidebar_label: 30. HPA + pgxpool tuning
description: "ADR 0030 — Phase 3 (scalability) for fulfillment-execution: an autoscaling/v2 HorizontalPodAutoscaler per independently-assessed workload (api max 4, analytics-projector max 2, analytics-reports max 3, frontend max 3; mcp explicitly excluded for a real in-memory-session reason), all default-disabled via values.yaml so this PR changes nothing on merge; plus explicit pgxpool.Config MaxConns caps and per-pool statement_timeout values, sized against the shared Postgres instance now fronted by PgBouncer (warehouse-infra PR #43). Direct port of order-management ADR-0026."
---

# 30. Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning

## Status

Accepted — implemented in the same change that introduces this record. This
is Phase 3 (scalability) of the fleet production-readiness plan, applied to
`fulfillment-execution` as a direct port of
[order-management ADR-0026](https://github.com/claudioed/order-management/blob/develop/docs/docs/adr/0026-horizontal-autoscaling-and-pgxpool-tuning.md)
(PR [#110](https://github.com/claudioed/order-management/pull/110)), the
reference the rest of the fleet's Phase 3 PRs copy. This service's own
Deployment-selector scoping already passes `warehouse-infra`'s
`check-chart-selectors.py` conformance check (verified before starting this
work), so the HPA prerequisite from the fleet plan is already satisfied.

## Context

Before this change, this chart had exactly one `HorizontalPodAutoscaler`
template, unconditionally targeting the `api` (`cmd/execution`) Deployment
only, wired to a flat `autoscaling.enabled/minReplicas/maxReplicas/
targetCPUUtilizationPercentage` block — untested against the other four
Deployments this chart renders (`mcp`, `frontend`, `analytics-projector`,
`analytics-reports`), and never assessed for whether scaling
`analytics-projector` past 1 replica was even safe given its Kafka
consumer-group membership. `replicaCount` was hardcoded to 1 everywhere,
same as the rest of the fleet before its own Phase 3 work landed.

Separately, no pool in this codebase set an explicit `pgxpool.Config.
MaxConns`, so every pool — the OLTP pool
(`internal/adapters/outbound/postgres/pool.go`, used by `cmd/execution` and
`cmd/mcp`) and the two analytics pools
(`internal/adapters/outbound/analyticsstore/pool.go`'s `NewPool` used by
`cmd/fulfillment-projector`, and `NewReadOnlyPool` used by
`cmd/fulfillment-reports`) — ran on pgx's library default, `max(4,
runtime.NumCPU())` connections per process. No pool set a
`statement_timeout` either, so a single runaway query (a bad index, a lock
wait, an unbounded date-range report query) could hold a pooled connection
indefinitely, with nothing to cancel it.

This matters together, not separately: turning on HPA for a
Postgres-backed workload without an explicit, bounded `MaxConns` means the
service's real connection ceiling becomes "however many CPUs the node
happens to have, times however many replicas the HPA happens to have
scaled to" — an unbounded, indirect function of cluster autoscaling and CPU
load, not a number anyone chose. This PR does both together on purpose:
HPA without a bounded `MaxConns` would have been the actual
production-readiness gap.

### Finding: shared Postgres, PgBouncer already in front of it, and the real max_connections

Checked before picking any number, not assumed: `warehouse-infra/
terraform/postgres.tf` provisions a single Postgres server per environment
— there is no per-service Postgres instance. All 10 fleet backend services
(including `fulfillment-execution`) point at the SAME running Postgres
server, each with its own logical database/role, sharing one unmodified
`max_connections=100` (the Bitnami chart default — `warehouse-infra`'s
Terraform has never explicitly overridden it).

Since order-management's ADR-0026 landed, `warehouse-infra` PR #43
(merged) put **PgBouncer** in front of that shared Postgres instance in
transaction-pooling mode, `pool_size=12` per service database. Every
service's OLTP `DATABASE_URL` Secret — including this service's — is
**already re-pointed** at PgBouncer; no chart or code change was needed
for that part, it is already live. Per PgBouncer PR #43's own documented
reasoning, analytics DSNs deliberately stay **direct** to Postgres (low
QPS, a single Kafka-consumer connection each, no pooling benefit, and
transaction-pooling mode is incompatible with any session-level feature —
verified this service's Postgres adapters use no `pg_advisory`, `LISTEN`,
or `NOTIFY`, so nothing here would have blocked pooling anyway). This PR
mirrors that same split: the OLTP pool's connections flow through
PgBouncer (transparent to the Go code — `pgxpool` just points at a
different `host:port`, same DSN shape); the two analytics pools stay
pointed directly at Postgres, unchanged.

This is why the OLTP `MaxConns` chosen below (10, matching
order-management) can stay reasonably generous on the *client* side without
blowing the real Postgres ceiling: PgBouncer absorbs the actual
server-side connection multiplexing across every replica of every
service. `MaxConns` still matters — it bounds how many concurrent queries
one process can have in flight against its own pool at once (and how many
connections PgBouncer's own `pool_size=12` gets contended for by this
service's own replicas) — but it is no longer the direct multiplier
against Postgres's 100-connection ceiling that it would be without
PgBouncer in the path.

## Decision

### 1. HorizontalPodAutoscaler — one per independently-assessed workload

Assessed each of the five Deployments this chart renders on its own
merits — statefulness, and (for Kafka consumers) consumer-group-id
convention — rather than blanket-enabling HPA everywhere:

| Deployment | HPA? | min | max | target CPU | Why |
|---|---|---|---|---|---|
| `api` (`cmd/execution`) | Yes | 1 | 4 | 70% | Stateless OLTP HTTP + pull-based `claimNext`. Its inbound `WorkReleased` consumer (`internal/adapters/inbound/kafka/consumer.go`, `WORK_RELEASED_CONSUMER_GROUP` defaulting to `"fulfillment-execution"`) uses a **stable, shared** consumer group — the fleet's normal horizontally-scalable pattern, N replicas share partitions via ordinary Kafka group rebalancing, never a per-process-unique group. No in-memory session or per-request state anywhere in this Deployment. Nothing here breaks at N>1. |
| `analytics-projector` (`cmd/fulfillment-projector`) | Yes, capped at **2**, not api's 4 | 1 | 2 | 70% | Its analytics Kafka consumer (`kafka.AnalyticsConsumerGroup = "fulfillment-analytics"`, `internal/adapters/inbound/kafka/analytics_consumer.go`) is also a **stable, shared** group with no per-instance uniqueness — verified directly in the source, not assumed; the publisher (`internal/adapters/outbound/kafka/analytics_publisher.go`'s `marshalData`) always sets a `Key` on the analytics topic's messages (confirmed via its own table-driven test, `analytics_publisher_test.go`), so 2 replicas legitimately share the topic's partitions via normal rebalancing — the same safe shape as `api`'s `WorkReleased` consumer. Capped at 2 rather than left at 4 for two additional reasons, not correctness: (a) key-based partitioning means ordering is only guaranteed per-aggregate, so a wide fan-out buys little extra throughput for what is a lightweight idempotent-upsert workload; (b) every write is already idempotent on `event_id` via `PostgresProjection`'s own `claim()` (`analytics_processed_events`, `ON CONFLICT (event_id) DO NOTHING`), so correctness does not regress at 2, but there is no throughput case yet that justifies more. |
| `analytics-reports` (`cmd/fulfillment-reports`) | Yes | 1 | 3 | 70% | Stateless read-only REST reader over its own read-only pgxpool (`analyticsstore.NewReadOnlyPool`) — no in-memory state, no Kafka consumption. Same treatment as `api`. |
| `frontend` (nginx-unprivileged serving the built `fulfillment-mfe` SPA bundle) | Yes | 1 | 3 | 70% | Pure static-asset serving. No server-side session, no per-request state. The most trivially horizontally-scalable workload in this chart. |
| `mcp` (`cmd/mcp`) | **No — deliberately excluded, not just disabled** | — | — | — | Verified directly in `internal/adapters/inbound/mcp/server.go`: `mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)` wraps the `github.com/modelcontextprotocol/go-sdk` `StreamableHTTPHandler`, which keeps **per-process, in-memory session state** keyed by the MCP protocol's own `Mcp-Session-Id` header (a real multi-request session, not just a TCP/HTTP connection). `charts/fulfillment-execution/templates/mcp-service.yaml` is a plain `ClusterIP` Service with no `sessionAffinity` configured, so under >1 replica a second request carrying the same `Mcp-Session-Id` could land on a different pod than the one that created the session, which has never heard of it and would reject or silently start a new one. Fixing it for real needs either `sessionAffinity: ClientIP` (a partial mitigation only) or wiring the SDK's `StreamableHTTPOptions.EventStore` to a shared/external session store — a real code change, out of scope for this chart-and-pool-tuning PR. `mcp.replicaCount` stays a plain, manually-set value; no `autoscaling.mcp` block exists in `values.yaml` at all. Revisit if/when `cmd/mcp` adopts an external session store or the SDK's stateless mode. |

Every enabled block is namespaced under a single top-level `autoscaling:`
key in `values.yaml`
(`autoscaling.<api|projector|reports|frontend>.{enabled,minReplicas,
maxReplicas,targetCPUUtilizationPercentage}`), **every `enabled` value
defaults to `false`**. This PR makes per-workload HPA possible and
verified-correct; it deliberately does not turn any of it on — the fleet
enables each workload's HPA later, once, as a conscious rollout decision.

**No replicas-vs-HPA fight.** Each Deployment template guards its
`spec.replicas` field with `{{- if not .Values.autoscaling.<x>.enabled
}}` — when a workload's HPA is enabled, its Deployment renders with NO
`replicas` field at all (a hardcoded `replicas:` next to an active HPA
would otherwise fight it on every reconcile, most visibly right after a
`helm upgrade` resets it back to the chart's static value). Verified
directly with `helm template`:

- Default values → 0 `HorizontalPodAutoscaler` resources render, every
  Deployment keeps its static `replicas:` field.
- All four `autoscaling.*.enabled=true` (with `analytics.enabled=true`,
  `frontend.enabled=true`, `mcp.enabled=true`) → exactly 4
  `HorizontalPodAutoscaler` resources render (one per scalable workload,
  `mcp` has none by design), and none of those four Deployments has a
  `replicas:` field — `mcp`'s Deployment still does (confirmed: only its
  Deployment carries `replicas: 1` in the rendered manifest).
- Only `autoscaling.api.enabled=true` → exactly 1 HPA renders, only the
  `api` Deployment loses its `replicas:` field; `projector`/`reports`/
  `frontend` keep theirs untouched (confirmed by direct `helm template`
  output diff). Mixed enablement is safe and independent per workload, as
  designed.

`helm lint` passes both with and without every `autoscaling.*.enabled`
flag set; `go vet`/`gofmt`/`make check`/`make arch-test` all pass unchanged
(no Go code path is affected by the chart changes).

### 2. pgxpool MaxConns

All three pools now set an explicit `pgxpool.Config.MaxConns` instead of
inheriting the CPU-derived library default (`max(4, runtime.NumCPU())`):

| Pool | Used by | `MaxConns` | Reasoning |
|---|---|---|---|
| OLTP (`postgres.NewPool`) | `cmd/execution` (`api`), `cmd/mcp` (`mcp`) | **10** | Matches order-management ADR-0026's OLTP number exactly — same chart shape (`api` HPA max 4). At the `api` Deployment's HPA ceiling of 4 replicas, 4 * 10 = 40 client-side connections requested against PgBouncer, which pools them down through its own `pool_size=12` per-database server-side ceiling (warehouse-infra PR #43) — the client-side number no longer directly multiplies against Postgres's shared `max_connections=100` the way it would without PgBouncer in the path. |
| Analytics writer (`analyticsstore.NewPool`) | `cmd/fulfillment-projector` | **5** | The projector has no HPA (fixed at 1 replica — see the table above) and does single-row idempotent upserts driven by one Kafka message at a time; a small, flat pool is enough. This pool stays **direct to Postgres** (no PgBouncer), per PgBouncer PR #43's documented analytics-DSN exception — low QPS, a single consumer connection, no pooling benefit. |
| Analytics reader (`analyticsstore.NewReadOnlyPool`) | `cmd/fulfillment-reports` | **5** (`ReportsMaxConns`) | `reports` IS HPA-scalable (max 3); at that ceiling, 3 × 5 = 15 connections, also **direct to Postgres** (same analytics exception). |

Worst case across every workload simultaneously at its proposed HPA
maximum, on the analytics side that talks directly to Postgres (`api`/`mcp`
go through PgBouncer and are bounded by its `pool_size=12` instead):
`projector` fixed at 1 × 5 = 5, `reports` 3 × 5 = 15 → **20** direct
analytics connections for this ONE service alone, out of the shared
instance's 100. That is a small, easily-affordable slice even before
accounting for PgBouncer's absorption of the much larger OLTP number — the
same conservative-by-construction shape order-management's ADR-0026
documented for its own analytics pools.

If/when the fleet turns on multiple workloads' HPA simultaneously and
PgBouncer's own `pool_size=12` per service becomes the binding constraint
in practice, the next lever is PgBouncer's `pool_size` itself — a
`warehouse-infra` Terraform change, not a per-service one — out of scope
here.

### 3. statement_timeout

All three pools set `statement_timeout` via `pgxpool.Config.AfterConnect`,
running `SET statement_timeout = '<value>'` on every new physical
connection as it's established (not per-query, so it survives connection
reuse across pooled acquisitions). Values differ per pool because their
query shapes differ:

| Pool | `statement_timeout` | Reasoning |
|---|---|---|
| OLTP (`postgres.StatementTimeout`) | **5s** | Every OLTP query (`claimNext`, task/station/package reads and writes) is a single-aggregate operation keyed by id, normally low-single-digit milliseconds. 5s is generous headroom for real transient contention (a lock wait behind a concurrent writer) without ever being a normal-path concern, while bounding the absolute worst case tightly since this is the interactive, latency-sensitive path and also the pool with the most connections (40 at max HPA scale) to protect. Matches order-management's OLTP value exactly. |
| Analytics writer (`analyticsstore.StatementTimeout`) | **10s** | A Kafka consumer replaying a backlog after a redeploy issues its idempotent upserts in a tight loop; a transient lock wait here doesn't need to be as tight as an interactive OLTP request. Still bounded — the projector has no replica to fail over to (fixed at 1), so an unbounded query here would stall the entire analytics pipeline, not just one of several `api` pods. |
| Analytics reader (`analyticsstore.ReportsStatementTimeout`) | **15s** | The throughput report aggregates rows across a caller-chosen `[From, To)` time range — wider than the OLTP side's always-single-aggregate-by-id shape — so it gets more headroom, but still a hard ceiling: a caller-supplied wide date range must not be able to hold a reports connection forever. |

Verified with a real Postgres via testcontainers
(`internal/adapters/outbound/postgres/pool_limits_integration_test.go`,
`-tags=integration`), not a mock and not just reading `pg_settings`:

- `TestNewPool_AppliesStatementTimeoutToNewConnections` — opens a pool
  against a real `postgres:16-alpine` container with a short test-only
  timeout (200ms, via the shared `NewPoolWithLimits` the production
  `NewPool` wraps), confirms `SHOW statement_timeout` reads back `200ms`
  on a freshly acquired connection, then runs `SELECT pg_sleep(2)` and
  asserts Postgres itself cancels it (SQLSTATE 57014, "canceling
  statement due to statement timeout") rather than letting it run the
  full 2s — proving the setting is genuinely enforced server-side, not
  merely set and ignored — and finally confirms the pool is still usable
  afterward (the cancelled statement doesn't poison the connection).
- `TestNewPool_AppliesMaxConns` — acquires exactly `maxConns` connections
  from a pool configured with `MaxConns=2`, then asserts a further
  `Acquire` blocks until `context.DeadlineExceeded`, proving `MaxConns`
  is the pool's real, enforced ceiling rather than advisory.

Both tests pass locally against a real Postgres container (direct port of
order-management ADR-0026's test file).

## Consequences

- HPA is now possible, correct, and independently verified per workload
  for four of this chart's five Deployments — but **off by default
  everywhere**. Merging this PR changes nothing about production replica
  counts; `MaxConns`/`statement_timeout` are the only behavior change that
  takes effect on deploy, and both are conservative relative to today's
  unbounded defaults (they can only reduce, never increase, worst-case
  connection usage and hung-query duration).
- `mcp` remains explicitly un-autoscaled, with the exact reason (in-memory
  session state, no sticky routing) recorded here and in `values.yaml`'s
  comments, so a future contributor doesn't mechanically copy `api`'s HPA
  block onto it without re-solving the session-affinity problem first.
- Because PgBouncer (warehouse-infra PR #43) is already live and already
  fronting this service's OLTP `DATABASE_URL`, the OLTP `MaxConns=10` at
  `api`'s HPA ceiling (40 client-side connections at 4 replicas) is
  bounded by PgBouncer's own `pool_size=12` server-side, not directly by
  Postgres's shared `max_connections=100` — a materially safer position
  than order-management's own ADR-0026 was in at the time it was written
  (before PgBouncer existed). The analytics pools intentionally stay
  direct to Postgres per PgBouncer PR #43's own documented reasoning; their
  worst-case direct usage (20 connections at full HPA scale) is a small,
  easily-affordable slice of the shared ceiling.
- A read replica for the analytics/reports read path is explicitly OUT OF
  SCOPE for this PR — not evaluated, not designed, not decided. Revisit
  only once actually needed and confirmed by the person requesting it.
- `max_connections=100` itself is an unexamined Bitnami chart default, not
  a value anyone has deliberately sized for this fleet's real demand. This
  ADR treats it as a hard external constraint to work within, not
  something in scope to change; PgBouncer's `pool_size` is the more
  relevant lever now and is also out of scope here (a `warehouse-infra`
  Terraform change).
