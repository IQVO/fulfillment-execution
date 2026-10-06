---
id: bounded-context-canvas
title: Bounded context canvas
sidebar_label: Bounded context canvas
sidebar_position: 11
description: The ddd-crew Bounded Context Canvas v5 for Fulfillment Execution — purpose, strategic classification, domain roles, every inbound and outbound message mapped to a real route, MCP tool or Kafka topic, business decisions, assumptions and open questions.
---

# Bounded context canvas

[ddd-crew Bounded Context Canvas v5](https://github.com/ddd-crew/bounded-context-canvas).
Every message row maps to a real route in
`internal/adapters/inbound/http/router.go`, a real MCP tool in
`internal/adapters/inbound/mcp/`, or a real Kafka topic and CloudEvents type
in the consumers and publishers.

## Name

**Fulfillment Execution** (`fulfillment-execution`, CloudEvents subdomain
`wes`, `source` `/warehouse/fulfillment-execution`).

## Purpose

Turn released work into physically completed work on the warehouse floor.
Each released work unit becomes a `Task` (Pick, Rebin, Pack or SLAM) that a
station **pulls** when it is free; the context guarantees that a task is
held by at most one station at a time (a time-boxed lease), that it is done
by a station with the right capabilities, and that the earliest CPT is
served first. It also seals packages (with DOT hazard segregation), runs the
SLAM weigh-check, consolidates an order's lines at Rebin, and reports
completions and missed CPTs back to planning, labour and order management.

## Strategic Classification

| Dimension | Value | Why |
| --- | --- | --- |
| **Domain** | **Core** | Pull-based dispatch with lease semantics is the throughput differentiator; see [Subdomain classification](./subdomain-classification.md) and [Core domain chart](./core-domain-chart.md). Pack and SLAM slices are Supporting and kept thin. |
| **Business model** | **Cost reduction / operational efficiency** | It earns nothing directly; it raises units per hour and on-time-to-CPT shipment, measured by the throughput and on-time-to-CPT reports ([ADR-0026](../adr/0026-on-time-to-cpt-kpi.md)). |
| **Evolution** | **Custom-built** | Built in-house, still gaining behaviour (claim compare-and-set, Rebin, package read model are recent ADRs). |

## Domain Roles

- **Execution context** — owns the task lifecycle (Pending → Claimed →
  Completed) and the claim/lease rules.
- **Enforcer** — enforces at-most-once claiming, capability matching, DOT
  segregation and the SLAM weigh tolerance.
- **Gateway to the floor (strategic)** — the only context that would talk to
  equipment (WCS), behind a deliberately empty anti-corruption seam
  ([ADR-0015](../adr/0015-wcs-equipment-anti-corruption-seam.md)).
- **Analysis context (side role)** — owns its own throughput and
  on-time-to-CPT read model (`cmd/fulfillment-projector`,
  `cmd/fulfillment-reports`).

## Inbound Communication

| Collaborator | Message | Type | Channel | Relationship |
| --- | --- | --- | --- | --- |
| `wes-work-planning` | `WorkReleased` | Event | Kafka `warehouse.work-planning.events`, `com.warehouse.wes.work-planning.workunit.WorkReleased` | Customer/Supplier, ACL on this side |
| `process-path-management` (opt-in) | `ProcessPathCreated` / `ProcessPathUpdated` / `ProcessPathDeactivated` | Event | Kafka `warehouse.process-path-management.events`, `com.warehouse.wes.process-path-management.processpath.*` (only with `PATH_CATALOGUE_SOURCE=kafka`) | Conformist on a Published Language |
| Station client / console | Create task | Command | REST `POST /tasks` (`Idempotency-Key` with Postgres) | OHS |
| Station client / console | Register station | Command | REST `POST /stations` | OHS |
| Station client | Claim next task | Command | REST `POST /stations/{stationId}/claim-next` | OHS |
| Station client | Check in / check out | Command | REST `POST /stations/{stationId}/check-in`, `POST /stations/{stationId}/check-out` | OHS |
| Station client | Renew lease | Command | REST `POST /tasks/{id}/renew-lease` | OHS |
| Station client | Complete task | Command | REST `POST /tasks/{id}/complete` | OHS |
| Pack station | Seal package | Command | REST `POST /tasks/{id}/seal-package` | OHS |
| SLAM station | Run SLAM weigh-check | Command | REST `POST /packages/{id}/slam` | OHS |
| Rebin station | Record arrival at Rebin | Command | REST `POST /rebin/arrivals` | OHS |
| Scheduler (external) | Expire leases | Command | REST `POST /tasks/expire-leases` | OHS |
| Scheduler (external) | Sweep CPT misses | Command | REST `POST /tasks/sweep-cpt-misses` | OHS |
| `warehouse-ops-agent` / console BFF | Tasks for an order | Query | REST `GET /tasks?orderRef=` | OHS |
| SLAM client / console | Package by id, packages for an order | Query | REST `GET /packages/{id}`, `GET /packages?orderRef=` | OHS |
| Any client | Queue depth | Query | REST `GET /queues/{taskType}/depth` | OHS |
| `workforce-management` | Installed capacity | Query | REST `GET /capacity/{capability}` | OHS |
| `warehouse-ops-agent` | Queue status, claimable work, stuck tasks | Query | MCP `get_queue_status`, `find_claimable_work`, `diagnose_stuck_tasks`; resource `queue://fulfillment/{type}/status`; prompt `triage_backlog` | OHS |
| `warehouse-ops-agent` | Complete task | Command | MCP `complete_task` | OHS |
| `warehouse-ops-agent` | Throughput / on-time-to-CPT report | Query | MCP `get_fulfillment_throughput_report`, `get_on_time_to_cpt` (when `REPORTS_BASE_URL` is set), served by REST `GET /reports/throughput` on `cmd/fulfillment-reports` | OHS |

## Outbound Communication

| Collaborator | Message | Type | Channel | Relationship |
| --- | --- | --- | --- | --- |
| `wes-work-planning` | `TaskCompleted` | Event | Kafka `warehouse.fulfillment.events`, `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` | Customer/Supplier feedback edge |
| `labor-performance` | `TaskCompleted` (with `associate_id`, `duration_seconds`, `task_type`) | Event | Kafka `warehouse.fulfillment.events`, `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` | Published Language |
| `order-management` | `TaskCPTMissed` | Event | Kafka `warehouse.fulfillment.events`, `com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed` | Published Language |
| `order-management` | `PackageManifested` | Event | Kafka `warehouse.fulfillment.events`, `com.warehouse.wes.fulfillment-execution.package.PackageManifested` | Published Language |
| own analytics read side | 10 analytics events (`TaskCreated`, `TaskClaimed`, `LeaseExpired`, `TaskCompleted`, `ItemPicked`, `PackageSealed`, `WeightDiscrepancyDetected`, `PackageDiverted`, `LabelApplied`, `PackageManifested`) | Event | Kafka `warehouse.fulfillment.analytics`, `com.warehouse.wes.fulfillment-execution.<task or package>.<Event>` | internal (same context) |
| `inventory-storage` (opt-in) | Product classification lookup | Query | REST `GET /products/{sku}/classification` (`PRODUCT_CLASSIFICATION_MODE=http`) | Customer/Supplier, ACL on this side |
| `facility-layout` (opt-in) | Location role lookup | Query | REST `GET /locations/{locationCode}` (`LOCATION_ROLE_MODE=http`) | Conformist behind ACL |
| `wes-work-planning` (operators) | Poison or failed `WorkReleased` | Event | Kafka `warehouse.work-planning.events.dlq` (`EVENT_PUBLISHER=kafka`) | — |
| WCS / equipment | none | — | none — `ports.EquipmentCommandPort` has no methods | strategic only |

## Ubiquitous Language

The full glossary is on [Ubiquitous language](../business-context/ubiquitous-language.md).
Top terms:

- **Task** — a unit of physical work with a type, a CPT and required
  capabilities.
- **claimNext** — pull dispatch: the station asks, the context answers with
  the earliest-CPT task it can do.
- **Lease** — a time-boxed claim (5 minutes by default) that returns the task
  to the pool on expiry.
- **CPT** — Critical Pull Time, the deadline that sets priority.
- **Station / Capability** — a work position and what it is certified and
  equipped for.
- **Package / Seal / SLAM** — the Pack output, closing it, and the
  weigh-check that labels or diverts it.
- **Rebin / OrderConsolidation** — fan-in of an order's lines before Pack.

## Business Decisions

- Dispatch is **pull, never push** — there is no `assign(task, station)`
  ([ADR-0002](../adr/0002-pull-based-claimnext-dispatch.md),
  [Why pull, not push](../business-context/why-pull-not-push.md)).
- Priority is **earliest CPT first**, nothing else.
- A task is claimed **at most once at a time**; a lapsed lease silently
  returns it to the pool
  ([ADR-0003](../adr/0003-lease-based-at-most-once-claiming.md)).
- Capabilities are read from the **registered station**, not from the
  request.
- A weigh-check outside **±0.05** of the expected weight **diverts** the
  package — a domain outcome, not an error.
- Items with incompatible **DOT hazard classes** may not share a package
  ([ADR-0010](../adr/0010-package-segregation-and-sort-lane.md)).
- A missed CPT is **reported, not enforced**, and re-reported on every sweep
  ([ADR-0025](../adr/0025-cpt-missed-sweep-and-package-manifested.md)).
- The PACK task for a multi-line order is created **once, when the last
  line reaches Rebin** ([ADR-0016](../adr/0016-rebin-and-order-consolidation.md)).

## Assumptions

- `wes-work-planning` releases work at the right rate; this context does not
  throttle intake.
- An external scheduler calls `POST /tasks/expire-leases` and
  `POST /tasks/sweep-cpt-misses`; no code in this repository schedules them.
  The Helm chart ships opt-in `CronJob`s for both (`sweeps.enabled`, default
  off).
- `WorkReleased.data.work_unit_id` is a stable correlation key, reused as
  `orderRef` and returned as `work_unit_id` on `TaskCompleted`.
- Hazard classification and location roles fail open: if the lookup is
  disabled or unavailable, sealing and registration proceed.
- Downstream consumers deduplicate on the CloudEvents `id` (outbox retries
  reuse it) and on their own business key for `TaskCPTMissed`.

## Verification Metrics

| Metric | Where |
| --- | --- |
| Completions, lease expiries, weigh-check diverts, claim-to-complete seconds per task type, station and hour | `throughput_rollup` (analytics DB), `GET /reports/throughput` |
| On-time-to-CPT: packages manifested on time vs late | `throughput_rollup.packages_on_time_cpt` / `packages_late_cpt`, MCP `get_on_time_to_cpt` |
| Queue depth per task type | `GET /queues/{taskType}/depth`, MCP `get_queue_status` |
| Report freshness | `GET /reports/throughput/freshness` |
| OpenTelemetry metrics (task completions, HTTP, Kafka) | `internal/observability/metrics.go` ([ADR-0019](../adr/0019-standard-metrics-convention.md)) |

## Open Questions

- Should a station have to be **checked in** before it can claim? Today
  `ClaimNext` ignores occupancy.
- Should `ItemPicked` be raised (item-level Pick), or removed from the
  catalogue?
- Should `ItemArrivedAtRebin` and `OrderConsolidated` be published, and to
  whom?
- Who owns the **schedule** for the lease and CPT sweeps in production? (The
  chart can run them as opt-in `CronJob`s, `sweeps.enabled`; whether and how
  often to turn that on is a per-environment choice, and `TaskCPTMissed`
  volume scales with the CPT sweep's frequency.)
- When does the WCS seam get its first method, and which vendor protocol
  does it translate?
- MCP governance: authentication was removed fleet-wide
  ([ADR-0022](../adr/0022-remove-rest-mcp-auth.md)); the governance charter
  §7–§8 remain unmet.
