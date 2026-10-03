---
id: throughput-report
title: Throughput & Lease-Health Report
sidebar_label: Throughput report
description: The fulfillment-execution analytical data product — a Throughput & Lease-Health read model built from the service's own domain events, served read-only over REST and MCP. Contract, grain, inputs, freshness SLA, and versioning.
---

# Throughput & Lease-Health Report

The analytical **data product** owned by Fulfillment Execution. It is built
entirely from this service's own domain events (never another service's
database) and served read-only. See [ADR-0012](../adr/0012-analytical-data-product.md)
for the decision, [ADR-0032](../adr/0032-cloudevents-mandatory-envelope.md)
for the CloudEvents 1.0 envelope every analytics event uses, and the
`warehouse-infra/docs/analytics/` governance charter for the cross-service rules.

## Name & owner

- **Report:** Throughput & Lease-Health.
- **Owner:** the Fulfillment Execution service/team (the same team that owns the
  OLTP write model).

## Grain

One row per **(task type × station × hour bucket)**, where `hourBucket` is the
UTC hour the row aggregates. Metrics per row:

| Metric | Meaning |
|---|---|
| `completions` | Count of `TaskCompleted` in the bucket. |
| `avgClaimToCompleteSeconds` | Mean seconds from a task's claim to its completion, over completions that had a recorded claim. |
| `leaseExpiries` | Count of `LeaseExpired` (unconfirmed claims that timed out back to the pool). |
| `weighCheckDiverts` | Count of `WeightDiscrepancyDetected` (SLAM weigh-check diversions). |
| `packagesManifested` | Count of `PackageManifested` attributed to the bucket ([ADR-0026](../adr/0026-on-time-to-cpt-kpi.md)). |
| `packagesOnTimeToCPT` | Of those, manifested at or before the originating SLAM task's CPT (`manifestedAt <= cpt`). |
| `packagesLateToCPT` | Of those, manifested after the CPT. |

The three on-time-to-CPT counts are raw counts, never a stored rate, so they
sum correctly across rows. They are populated on **SLAM rows only**: a
`PackageManifested` is correlated to its order's SLAM task via
`TaskRepo.FindByOrderRef`, and the row is that task's `(SLAM, station,
manifest hour)`. When no SLAM task resolves, the event is marked processed
but not projected.

## Inputs (analytics topic events)

Consumed from **`warehouse.fulfillment.analytics`** (the dedicated analytics
topic, separate from the integration topic). Every message is a CloudEvents
1.0 event with `dataschema`
`urn:warehouse:fulfillment-execution:analytics:<EventName>:v1`; the projector
dispatches on the full `type`, takes the occurrence instant from `time`, and
dedupes on `id`. A message that is not a valid CloudEvent is logged at WARN
and skipped.

| `type` (`com.warehouse.wes.fulfillment-execution.` + …) | Contributes |
|---|---|
| `task.TaskClaimed` | claim timestamp (for claim→complete latency) |
| `task.TaskCompleted` | `completions`, claim→complete latency |
| `task.LeaseExpired` | `leaseExpiries` |
| `package.WeightDiscrepancyDetected` | `weighCheckDiverts` |
| `package.PackageManifested` | `packagesManifested`, `packagesOnTimeToCPT` / `packagesLateToCPT` (publisher enriches with the SLAM task's type, station and on-time verdict) |

`task_type` is enriched onto task-scoped events by the publisher via a `TaskRepo`
lookup (the domain events themselves stay thin). `TaskCreated`, `ItemPicked`,
`PackageSealed`, `LabelApplied`, and `PackageDiverted` are published to the topic
but do not currently move this report; the projector acknowledges them without
projecting. `TaskCPTMissed` and the two Rebin events are not published to the
analytics topic at all.

## Interface

### REST (served by `cmd/fulfillment-reports`, read-only)

```
GET /reports/throughput?from=<RFC3339>&to=<RFC3339>&taskType=&stationId=&granularity=hour
GET /reports/throughput/freshness
GET /healthz
```

- `from`, `to` — **required**, RFC3339, `[from, to)` compared against `hourBucket`.
- `taskType`, `stationId` — optional exact-match filters.
- `granularity` — optional, defaults to `hour`.

Response (`200`):

```json
{
  "rows": [
    {
      "taskType": "PICK",
      "stationId": "S-7",
      "hourBucket": "2026-08-26T14:00:00Z",
      "completions": 42,
      "avgClaimToCompleteSeconds": 73.5,
      "leaseExpiries": 1,
      "weighCheckDiverts": 0,
      "packagesManifested": 0,
      "packagesOnTimeToCPT": 0,
      "packagesLateToCPT": 0
    }
  ]
}
```

Freshness (`200`):

```json
{ "lagSeconds": 4.2 }
```

Errors use RFC 7807 `application/problem+json`, consistent with the OLTP API
([ADR-0005](../adr/0005-rfc-7807-problem-details.md)).

### MCP (curated, read-only)

Two read-only tools on the existing `cmd/mcp` server (Streamable HTTP),
consistent with [ADR-0008](../adr/0008-mcp-inbound-adapter.md). Both call the
reports REST rather than opening the analytical database, and both are
registered only when the MCP server has a reports client configured.

- **`get_fulfillment_throughput_report`** — same filters as the REST endpoint;
  returns the rows.
- **`get_on_time_to_cpt`** ([ADR-0026](../adr/0026-on-time-to-cpt-kpi.md)) — `from`/`to` (required),
  optional `taskType`/`stationId`; sums the three on-time counts across the
  window and returns `packagesManifested`, `packagesOnTimeToCPT`,
  `packagesLateToCPT` and `onTimeRate` (`0` when nothing manifested). The
  rate is computed at read time, never stored.

## Freshness SLA

- **Definition:** `lagSeconds` = now − age of the most recently applied event.
- **Target:** p95 event-to-report lag **< 30s** under normal load.
- **Exposed:** `GET /reports/throughput/freshness`.
- Breaching the SLA is an operational signal (projector lag / consumer down), not
  a correctness bug — the report catches up when the projector does.

## Versioning

- Additive fields (new optional row metric, new query filter) are non-breaking.
- A breaking change to a row's shape or meaning is a new endpoint/tool version.
- The analytics event contract versions independently via each event's
  CloudEvents `dataschema` (`...:analytics:<EventName>:v<N>`); a breaking
  payload change is a new `.v2` type with a new dataschema version.

## Runbook notes

- **Two processes, one writer.** `cmd/fulfillment-projector` is the only writer of
  the analytical DB; `cmd/fulfillment-reports` connects read-only. The OLTP
  `cmd/execution` never opens the analytical DB.
- **Empty on first deploy.** The report is empty until events flow. To backfill
  history, replay `warehouse.fulfillment.analytics` from earliest into a fresh
  projector; Kafka retention must cover the desired window.
- **Eventual consistency.** The report is a projection, not a real-time view; it
  meets the freshness SLA, not transactional consistency.
