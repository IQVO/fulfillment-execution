---
id: 0012-analytical-data-product
title: 12. Per-service analytical data product (report) via a separate analytics topic
sidebar_label: 12. Analytical data product
sidebar_position: 12
description: An analytical read model (the "report") built from this service's own domain events on a dedicated warehouse.fulfillment.analytics topic, projected into a separate analytical database and served by a read-only reports binary over REST and MCP — a lightweight data mesh with no central data platform.
---

# 12. Per-service analytical data product (the "report")

## Status

**Accepted.**

## Context

The warehouse-systems estate needs a per-service **report** that supports
analytics while each service stays the **OLTP** system of record for its own
bounded context. The requirement, stated deliberately simply: *follow data-mesh
principles, but without standing up a whole data platform.* No central
warehouse, no lake, no shared ETL team.

Fulfillment Execution already has everything the analytical side needs as a
substrate:

- Past-tense **domain events** (`TaskCreated`, `TaskClaimed`, `LeaseExpired`,
  `TaskCompleted`, `ItemPicked`, `PackageSealed`, `WeightDiscrepancyDetected`,
  `LabelApplied`, `PackageDiverted`) raised by the aggregates.
- A Kafka **integration** path (`warehouse.fulfillment.events`) already carrying
  `TaskCompleted` to `wes-work-planning`, with the CloudEvents-like `Envelope`
  and OTel trace propagation established in
  [ADR-0004](./0004-kafka-integration-events-and-envelope.md).
- The `ProcessedEvents` port for idempotent at-least-once consumption.
- The dual inbound-adapter pattern (HTTP + MCP) from
  [ADR-0008](./0008-mcp-inbound-adapter.md).

So the event backbone exists; what is missing is the **analytical read side**.
The forces shaping the decision:

- **The integration contract must not become coupled to reporting.** The report
  needs many more event types than the integration topic exposes, and they
  change on a different cadence. Widening `warehouse.fulfillment.events` with
  analytics-only event types would risk surprising `wes-work-planning`'s
  existing consumer and entangle two contracts that should evolve separately.
- **Analytics must never contend with OLTP.** A report query load, a long
  aggregation, or a projection rebuild must not touch the transactional
  database that serves `claimNext` and `SealPackage`.
- **The service still owns its data as a product.** Data-mesh domain ownership
  means the read side lives in this repo, owned by the same team, with a
  contract, an owner, and a freshness SLA — not shipped off to a central team.
- **No new central platform.** Reuse what the estate already runs: Kafka,
  Postgres, chi, the MCP SDK, the Helm chart.

The governing cross-service contract for this decision lives in
`warehouse-infra/docs/analytics/` — the **Envelope v1** analytics event contract
and the **analytics data-product governance charter**.

## Decision

**Fulfillment Execution owns an analytical data product built solely from its
own domain events, delivered on a dedicated analytics topic, projected into a
separate analytical database, and served read-only over REST and MCP. Three
processes; one writer.**

### 1. Separate analytics topic

A new outbound adapter publishes the report-input event set to
**`warehouse.fulfillment.analytics`**, originally using the shared **Envelope v1** wrapper
(`event_id`, `event_type`, `occurred_at`, `source`, `schema_version`, `data`)
with a per-`event_type` `data` payload. *(Superseded for the envelope by
[ADR-0032](./0032-cloudevents-mandatory-envelope.md): every analytics message
is now a CloudEvents 1.0 event, `schema_version` is replaced by `dataschema`
`urn:warehouse:fulfillment-execution:analytics:<EventName>:v1`, and the
projector dispatches on the full CloudEvents `type`.)* The existing integration publisher and
`warehouse.fulfillment.events` are **left untouched**, so no existing consumer
is affected. Analytics consumers switch on `event_type` and ignore unknown
types.

### 2. Separate analytical database

Projections land in a **separate analytical database** with its own credentials
(`ANALYTICS_DATABASE_URL`), its own golang-migrate migration set, and a
**read-only role** for the reader. Baseline is a dedicated `*_analytics`
database in the existing Postgres release (fitting the estate's
`init-databases.sql.tftpl` pattern); the `ANALYTICS_DATABASE_URL` seam allows
promotion to a physically separate instance later without code changes. The OLTP
`DATABASE_URL` database is never opened by the analytical side.

### 3. Three processes, one writer

- **`cmd/execution`** — the OLTP binary. Unchanged, except its composition root
  additionally publishes domain events to the analytics topic.
- **`cmd/fulfillment-projector`** — the analytics **writer**. Consumes
  `warehouse.fulfillment.analytics` (consumer group `fulfillment-analytics`),
  applies idempotent projections (reusing `ProcessedEvents`), and is the **only**
  writer of the analytical database. Runs the analytical migrations on start.
- **`cmd/fulfillment-reports`** — the **read-only reader**. Opens the analytical
  database with the read-only role and serves `GET /reports/throughput` and
  `GET /reports/throughput/freshness`. Never writes, never migrates.

### 4. Served over REST and MCP

The reports binary serves the REST report resource. A curated, read-only MCP
tool (`get_fulfillment_throughput_report`) — following the intent-level tool
discipline of [ADR-0008](./0008-mcp-inbound-adapter.md) — calls the reports
REST rather than opening the analytical database itself, so no process touches a
datastore it does not own.

### 5. The report

A **Throughput & Lease-Health** read model, keyed per task-type × station ×
time-bucket: completions per interval, average claim→complete latency,
lease-expiry rate, and SLAM weigh-check divert rate. It is a **projection** from
events (consistent with the existing "read models are projections, not
aggregate state" rule), eventually consistent to a freshness SLA (p95
event-to-report lag < 30s), not real-time.

The analytical read model lives in a new `internal/analytics/` region that
depends on nothing; the consumer and store adapters depend on it. The OLTP
**domain and application layers are not modified**, and `arch-test` enforces
that they do not import the analytics store.

## Consequences

### Easier

- **The integration contract is untouched**, so widening what analytics
  consumes never risks an integration consumer. Analytics retention is tuned
  independently of the integration topic.
- **Analytics cannot contend with OLTP** — separate database, separate
  connection, read-only reader role. A runaway report query cannot touch
  transactional throughput.
- **The report is rebuilt purely from events** — no dual-write from OLTP, so the
  transactional write path gains no new failure mode. The read model can be
  rebuilt from scratch by replaying the topic.
- **No central platform.** Everything reuses the estate's existing Kafka,
  Postgres, chi, MCP SDK and Helm.
- **Least privilege by construction.** The read-only DB role makes "a report can
  never corrupt the analytical store" a hard guarantee, not a convention.

### Harder

- **One more topic, two more binaries, and a second database** to operate per
  service. Mitigated by reusing the OLTP Postgres chart pattern and the existing
  consumer/publisher scaffolding.
- **Eventual consistency.** The report lags the OLTP truth by the freshness SLA;
  it is not a real-time view. This is the correct data-mesh tradeoff but must be
  communicated to report consumers.
- **The analytics publisher is a second producer path** for the same domain
  events. It re-serializes them (now as CloudEvents 1.0, ADR-0032) for the analytics topic; the
  event set it publishes must be kept in step with the report's inputs.
- **First deploy has an empty report** until events flow; historical backfill
  requires replaying `warehouse.fulfillment.analytics` from earliest into a
  fresh projector, so Kafka retention must cover the desired backfill window.

## References

- Envelope v1 contract (historical; superseded by [ADR-0032](./0032-cloudevents-mandatory-envelope.md)): `warehouse-infra/docs/analytics/envelope-v1.md`
- Governance charter: `warehouse-infra/docs/analytics/governance-charter.md`
- [ADR-0004 — Kafka integration events and envelope](./0004-kafka-integration-events-and-envelope.md)
- [ADR-0008 — MCP inbound adapter](./0008-mcp-inbound-adapter.md)
