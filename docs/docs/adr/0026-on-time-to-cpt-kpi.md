---
id: 0026-on-time-to-cpt-kpi
slug: /adr/0026-on-time-to-cpt-kpi
title: 0026. On-time-to-CPT KPI on the throughput analytics data product (companion to order-management ADR 0014 §6)
sidebar_label: 0026. On-time-to-CPT KPI
description: "ADR 0026 — this service's half of order-management ADR 0014 §6: on-time-to-CPT measured as PackageManifested's occurred_at against the originating SLAM task's CPT, added to the throughput analytics data product (ADR-0012) as new raw counts on throughput_rollup, plus a new read-only MCP tool get_on_time_to_cpt."
---

# 0026. On-time-to-CPT KPI on the throughput analytics data product

## Status

Accepted — implemented in the same change that introduced this record.
**Companion to order-management ADR 0014, §6 "Promise KPIs become part
of the analytical data product"** — the same kind of split ADR-0025 has
with order-management ADR 0014 §5. order-management ADR 0014 §6 states
the boundary explicitly:

> ADR 0006's Order Funnel report gains: promise basis distribution,
> re-promise rate, split-shipment rate, and promise-to-cutoff gap
> (`cutoffAt - allocatedAt`). On-time-to-CPT itself is measured where
> the evidence is, in fulfillment-execution's analytics (`cpt` vs
> `manifested_at`), not here. Both are exposed as read-only MCP tools
> per ADR 0010 so `warehouse-ops-agent` can reason about promise health
> without a REST detour.

This ADR is the fulfillment-execution half only: on-time-to-CPT. The
promise-basis-distribution/re-promise-rate/split-shipment-rate/promise-
to-cutoff-gap KPIs are a separate, parallel PR in order-management,
against that service's own Order Funnel report — this record has no
ground truth on their real shipped shape and does not describe them.

## Context

order-management ADR 0014 replaced `now + leadTime` promise-date
arithmetic with a CPT window derived from real fulfillment capability,
and ADR-0025 (this service) closed the feedback loop by raising
`TaskCPTMissed` and `PackageManifested` onto the integration topic. What
is still missing is the measurement itself: nobody yet asks "of the
packages that shipped, what fraction made their CPT?" — the fact exists
on the wire (`PackageManifested`'s occurred-at instant versus the originating
task's `CPT`) but nothing aggregates it into a KPI.

This service's throughput analytics data product (ADR-0012) is exactly
where that evidence already lives structurally: it is a per-service
report built from this service's own domain events, keyed by
`(task_type, station_id, hour_bucket)`, with `TaskCompleted`,
`LeaseExpired`, and `WeightDiscrepancyDetected` already rolled up the
same way. `PackageManifested` (ADR-0025, shipped) was a real gap in the
analytics publisher's switch — it marshals nine other event types onto
`warehouse.fulfillment.analytics` but not this one — so on-time-to-CPT
could not have been measured even in principle before this change.

The one piece of real design work is correlation: `PackageManifested`
carries `{PackageId, OrderRef}` and no CPT (ADR-0025 deliberately kept it
thin — see that ADR's §5, "Neither needs the repo-lookup enrichment
`TaskCompleted` requires"). But it *is* the correct event to measure
"was it on time" from, and it needs the CPT of a task it doesn't
reference by id. The CPT has to be resolved via the package's
originating SLAM task.

## Decision

### 1. Correlate `PackageManifested` to its originating SLAM task via `OrderRef`

`pack.Package` carries no `TaskId` field (verified by reading
`internal/domain/package/package.go` in full — its fields are `id`,
`orderRef`, `status`, `scannedContents`, `fragileHandling`,
`giftWrapRequested`, `scannedHazardClasses`; no task reference). The
correlation therefore goes through `OrderRef`, using
`ports.TaskRepo.FindByOrderRef(ctx, orderRef) ([]*task.Task, error)` —
already a real, shipped method on this repo's `TaskRepo` (added
alongside the process-path work, used today by
`GetTasksByOrderRef`/`ArriveAtRebin`). Among the tasks returned for an
order, the one with `Type() == task.Slam` is the one whose CPT a
manifest event should be compared against — a Pick/Pack/Rebin leg for
the same order is a different promise-risk window entirely. This is
exactly the same "repo-lookup-enrichment for an event that doesn't
itself carry every needed field" pattern
`AnalyticsPublisher.taskType`/`Publisher.encodeTaskCompleted` already use
for `task_type`/`OrderRef`/`AssociateId`.

`AnalyticsPublisher.onTimeToCPTFields` (new,
`internal/adapters/outbound/kafka/analytics_publisher.go`) performs this
lookup and returns the SLAM task's `task_type`/`station_id` (the
station of its lease, if any — a task not yet claimed simply has an
empty `station_id`, same convention `TaskClaimed`'s enrichment already
tolerates), the on-time verdict, and whether resolution succeeded.

### 2. The on-time boundary: `manifestedAt <= cpt` counts as on time

`task.Task.IsCPTMissed` (ADR-0025) treats `now >= cpt` as missed — the
instant a task's CPT arrives, it counts as overdue if still open. For
on-time-to-CPT to be the honest complement of that predicate rather than
an independently-invented boundary, this ADR is deliberate about the
symmetric choice: **`manifestedAt <= cpt` counts as on time.** The
boundary instant (manifested exactly at CPT) is ON TIME for the
manifesting side and MISSED for the sweep side — these are not
contradictory, because they are different events measuring different
things:

- `IsCPTMissed(now)` asks "is this task *still open* at/after its CPT" —
  a task that hasn't finished by the deadline is a live promise risk the
  instant the deadline arrives, so `now == cpt` is correctly treated as
  already late for an UNFINISHED task.
- On-time-to-CPT asks "did the FINISHING event (`PackageManifested`)
  happen at or before the CPT" — a package that manifests in the same
  instant the CPT arrives made its promise; the customer's package
  shipped on the deadline it was promised, not after it.

Both predicates use the same `>=`/`<=` convention (the boundary instant
belongs to the earlier side of the inequality — "not yet late" for the
sweep, "still on time" for the manifest), so the two boundaries are
consistent readings of the same instant from two different questions,
not a coincidence and not an inconsistency. `onTime :=
!manifestedAt.After(cpt)` is the literal Go expression (mirroring
`IsCPTMissed`'s own `!now.Before(cpt)` construction), pinned by an
explicit boundary test asserting exactly-at-CPT counts as on time
(`TestAnalyticsPublisher_PackageManifested_OnTimeAndLateAndBoundary`,
`TestAnalyticsConsumer_PackageManifested_RoutesOnTimeVerdict`).

### 3. Fail-soft when no SLAM task resolves

By construction, every `Package` descends from a SLAM task — SLAM is the
Pack path's own terminal task type, and `RunSlam` cannot run on a
package with no owning order/task. Still, per this fleet's convention
for a best-effort enrichment lookup (the same convention
`AnalyticsPublisher.taskType` already uses — "a missing task_type leaves
the report's dimension unspecified rather than failing the publish"),
`onTimeToCPTFields` returns `found=false` rather than erroring when
`FindByOrderRef` returns no SLAM task, and the marshalled analytics
payload carries `resolved: false`. The analytics consumer
(`internal/adapters/inbound/kafka/analytics_consumer.go`) checks this
flag and skips the `ApplyPackageManifested` call entirely when
`resolved=false` — no wrong dimension is ever recorded — while still
marking the event processed, so a redelivery of the same `event_id`
stays a no-op rather than being retried forever.

### 4. Grain: the existing `(task_type, station_id, hour_bucket)` rollup, not a new dimension

A manifested package has no task-type/station identity of its own. The
brief for this ADR raised the question explicitly — does SLAM already
have a natural home in `throughput_rollup`? — and the answer is yes:
`ApplyTaskCompleted` already keys SLAM completions by
`(taskType="SLAM", stationId, hourBucket)` today. Reusing that grain
means `packages_manifested`/`packages_on_time_cpt`/`packages_late_cpt`
land on the same SLAM rows the throughput report already produces, so a
caller filtering `taskType=SLAM` sees completions, avg claim-to-complete,
and on-time-to-CPT together in one row family — a second, parallel key
space for "really the same process path" was considered and rejected as
unnecessary complexity.

### 5. Raw counts, not a stored rate

`report.Row` gains three new `int` fields: `PackagesManifested`,
`PackagesOnTimeToCPT`, `PackagesLateToCPT`. No `OnTimeRate float64`
field is added to the read model — this matches the file's existing
convention exactly (`Completions`, `LeaseExpiries`, `WeighCheckDiverts`
are all raw counts; `AvgClaimToCompleteSeconds` is the one derived value
already stored, and it is derived in SQL from a sum+count pair the same
way `claim_to_complete_seconds`/`completions_with_claim` already are).
The rate is computed by the caller — the REST DTO ships the three raw
counts additively, and the new MCP tool (`get_on_time_to_cpt`) derives
`onTimeRate = PackagesOnTimeToCPT / PackagesManifested` (0 when
`PackagesManifested` is 0, not a spurious 0% rate) after summing across
every hour bucket its underlying throughput query returns, so a caller
does not have to do that arithmetic itself.

### 6. Widened surfaces (additive throughout)

- `internal/analytics/report/throughput.go` — `Row` gains the three
  fields above, documented at the same density as the existing ones.
  Still depends on nothing else in the module (verified: `go list
  -f '{{join .Imports "\n"}}' ./internal/analytics/report/` shows no
  internal import), so `arch-test`'s ADR-0012 rule is unaffected.
- `internal/analytics/report/ports.go` — `ProjectionStore` gains
  `ApplyPackageManifested(ctx, eventId, taskType, stationId string, at
  time.Time, onTime bool) error`. The port stays free of any CPT
  comparison of its own — it only records the caller's verdict,
  matching every other `Apply*` method's shape (raw fields in, no
  domain logic).
- `internal/adapters/outbound/analyticsstore/postgres_projection.go` —
  new `ApplyPackageManifested`, same claim-then-upsert transaction shape
  every other `Apply*` uses; `rollupDelta`/`upsertRollup` widened
  additively, mirroring the existing `claim_to_complete_seconds`/
  `completions_with_claim` sum+count precedent exactly (one `+1` per
  event into the counter that matches the caller's verdict).
- `migrations/analytics/0002_on_time_to_cpt.{up,down}.sql` — three new
  `BIGINT NOT NULL DEFAULT 0` columns on `throughput_rollup`. Purely
  additive: every pre-existing row defaults to zero on the new columns,
  and no existing column changes shape.
- `internal/adapters/outbound/analyticsstore/postgres_report.go` /
  `memory_store.go` — `Query` widened to select/return the three new
  fields; the in-memory test double gets the matching `Apply*` and
  counter fields, same style as the Postgres implementation.
- `internal/adapters/outbound/kafka/analytics_publisher.go` —
  `PackageManifested` added to `inAnalyticsContract` and `marshalData`;
  the marshalled payload gains `task_type`, `station_id`, `on_time`, and
  `resolved` alongside the existing `package_id`/`order_ref`.
- `internal/adapters/inbound/kafka/analytics_consumer.go` — routes
  `PackageManifested` to `ApplyPackageManifested` when `resolved=true`,
  skips (but still marks processed) when `resolved=false`.
- `internal/adapters/inbound/http/reports_handler.go` — the
  `throughputRowDTO` gains the three fields, additively, on the existing
  `GET /reports/throughput` response.
- `internal/adapters/inbound/mcp/report_tool.go` — `ThroughputRowView`
  (the MCP-side mirror of the REST DTO) gains the same three fields, so
  `get_fulfillment_throughput_report` also surfaces the raw counts.
- `internal/adapters/inbound/mcp/on_time_to_cpt_tool.go` (new) — the
  read-only `get_on_time_to_cpt` tool: takes `from`/`to` (required,
  RFC3339) plus optional `taskType`/`stationId` filters, calls the
  reports REST client's existing `GetThroughput`, sums the three raw
  counts across every returned bucket, and derives `onTimeRate`.
  `ReadOnlyHint: true`; registered only when a reports client is
  configured, matching `get_fulfillment_throughput_report`'s own
  registration convention exactly (`registerOnTimeToCPTTool`, called
  from `registerTools` right after `registerReportTool`).

No new use case was added — the tool calls the reports REST client
directly (the same shape `get_fulfillment_throughput_report` already
uses), since the aggregation-and-rate-derivation logic is a pure,
stateless transform of an existing query's result, not a new
application-layer concern with its own port or invariant.

### 7. `apis/asyncapi.yaml` — no change needed

The analytics topic (`warehouse.fulfillment.analytics`) has never been
part of this repo's AsyncAPI catalogue — only the integration topic
(`warehouse.fulfillment-execution.events`, documenting the
`warehouse.fulfillment.events` wire topic) is, and `PackageManifested`
is already documented there from ADR-0025 with its existing
`{PackageId, OrderRef}` payload — this ADR does not change what rides on
the integration topic. Verified by reading `apis/asyncapi.yaml` in full
before deciding: no `analytics` channel/section exists to extend.

## Alternatives considered

- **A new dimension/table for on-time-to-CPT instead of reusing the SLAM
  rollup grain.** Rejected (§4): SLAM already has a natural home in
  `throughput_rollup` via `ApplyTaskCompleted`, and a package inherits
  its SLAM task's dimensions rather than having its own — a second
  parallel key space would duplicate the grain for no real gain.
- **Store a precomputed `on_time_rate` column.** Rejected (§5): breaks
  this file's established raw-counts convention and creates a value that
  must be kept consistent with its own inputs on every upsert, for no
  benefit over deriving it at read time from two integers.
- **Resolve the SLAM task via a new `Package.TaskId` field instead of
  `FindByOrderRef`.** Considered but rejected: it would widen the
  `Package` aggregate's own persisted shape (a new migration on the OLTP
  schema, not just analytics) for a correlation the analytics layer
  already has a real, working mechanism for
  (`FindByOrderRef`) — the smaller, already-proven tool was preferred.
- **Fail the publish when no SLAM task resolves**, instead of
  `resolved=false` fail-soft. Rejected: mirrors this fleet's established
  best-effort-enrichment convention (`taskType`'s own doc comment) and
  avoids making a defensive edge case (one that should not happen by
  construction) capable of blocking the whole outbox relay.

## Consequences

**Easier**

- On-time-to-CPT is now a real, queryable fact — both over REST
  (`GET /reports/throughput`'s widened rows) and MCP
  (`get_on_time_to_cpt`), without a new database, a new topic, or a new
  process. It reuses every mechanism ADR-0012 already established.
- The boundary convention is documented and pinned by an explicit test
  at the exact instant a mutation-testing tool would target
  (`manifestedAt == cpt`), not left to accidental behaviour of a
  `<`/`<=` choice made in passing.
- `warehouse-ops-agent` (or any MCP client) can now ask "what fraction of
  SLAM passes made their CPT this shift" in one tool call, matching
  order-management ADR 0014 §6's stated intent.

**Harder**

- The correlation lookup (`FindByOrderRef` + a `Type() == task.Slam`
  scan) is a second repo round trip per `PackageManifested` publish,
  same cost class as `TaskCompleted`'s existing enrichment lookups — not
  free, but bounded and already the accepted pattern in this file.
- `resolved=false` is a real possibility the consumer must handle
  correctly (skip, but still mark processed) — one more branch in
  `HandleMessage`'s switch, covered by
  `TestAnalyticsConsumer_PackageManifested_SkipsWhenUnresolved`.
- The three new rollup columns exist on every `(task_type, station_id,
  hour_bucket)` row, not just SLAM ones — harmless (they default to
  zero and are simply never populated for PICK/PACK/REBIN rows), but a
  future reader of `throughput_rollup` should know these columns are
  meaningful only where `task_type = 'SLAM'` in practice.

## Verification performed

All run locally in the worktree before pushing; every command below
produced the quoted result.

- `gofmt -l .`: no output (clean).
- `go build ./...`, `go vet ./...`: clean.
- `go build -tags=integration ./...`, `go vet -tags=integration ./...`:
  clean.
- `golangci-lint run ./...`: `0 issues.`
- `go test ./... -race`: all packages pass, including new boundary
  coverage:
  `TestAnalyticsPublisher_PackageManifested_OnTimeAndLateAndBoundary`
  (before/exactly-at/after CPT, three subtests),
  `TestAnalyticsPublisher_PackageManifested_UnresolvedWhenNoSLAMTask`,
  `TestAnalyticsConsumer_PackageManifested_RoutesOnTimeVerdict`,
  `TestAnalyticsConsumer_PackageManifested_SkipsWhenUnresolved`,
  `TestMemoryStore_ApplyPackageManifested_OnTimeAndLateCounters`,
  `TestMemoryStore_ApplyPackageManifested_Idempotent`,
  `TestOnTimeToCPTTool_AggregatesAcrossBucketsAndDerivesRate`,
  `TestOnTimeToCPTTool_ZeroManifestedYieldsZeroRateNotDivideByZero`,
  `TestOnTimeToCPTTool_RequiresFromTo`,
  `TestOnTimeToCPTTool_NilClientErrors`.
- `make coverage`: **97.6%** (gate: 90%).
- `make arch-test`: all fitness tests pass, including the "report
  package depends on nothing else in the module" rule (verified
  independently via `go list -f '{{join .Imports "\n"}}'
  ./internal/analytics/report/`, empty output for internal imports).
- `make mutation-fast` (gremlins on `./internal/domain/task`, unaffected
  by this change — no domain-layer edit was made): Killed 16, Lived 0,
  efficacy 100.00%, mutator coverage 100.00% — unchanged from ADR-0025's
  baseline, confirming this ADR touched no domain-layer file.
- `make integration` (real `postgres:16` via `docker compose up -d`,
  `DATABASE_URL` + `ANALYTICS_DATABASE_URL` set, `ANALYTICS_DATABASE_URL`
  pointed at a freshly created `fulfillment_execution_analytics`
  database): all packages pass, including the new
  `TestPostgresProjectionAndReport_OnTimeToCPT_RoundTrip` — two on-time
  and one late `PackageManifested` applied twice each with the same
  `event_id`s (duplicate delivery), asserting `packages_manifested=3`,
  `packages_on_time_cpt=2`, `packages_late_cpt=1` (idempotent), and that
  unrelated existing columns (`completions`) stay at their zero default
  — proving the additive migration does not cross-contaminate existing
  rollup fields. Re-ran three consecutive times to confirm stability
  (unique per-run `station_id`/`event_id` suffixes, since the projection
  is idempotent on `event_id` and a fixed id would make the second run's
  assertions read as if nothing new landed — the first version of this
  test hit exactly that bug and was fixed before commit).
- Every existing test in the touched packages still passes unmodified —
  in particular
  `TestPostgresProjectionAndReport_RoundTrip`/`TestMemoryStore_ClaimCompleteIdempotent`
  (the pre-existing `Completions`/`AvgClaimToCompleteSeconds` assertions)
  are byte-identical to before this change.

## References

- [ADR-0012 — Per-service analytical data product](./0012-analytical-data-product.md)
- [ADR-0025 — TaskCPTMissed sweep and PackageManifested on the outbox](./0025-cpt-missed-sweep-and-package-manifested.md)
- order-management ADR 0014 — The delivery promise is a CPT window
  derived from fulfillment capability, §6 (companion decision; read-only
  ground truth for this ADR, not owned by this repo)
