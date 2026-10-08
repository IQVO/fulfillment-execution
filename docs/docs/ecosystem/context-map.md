---
id: context-map
title: Context map
sidebar_label: Context map
sidebar_position: 1
description: The ddd-crew Context Mapping view of Fulfillment Execution — every upstream/downstream edge with its context-mapping patterns and technology, the code that proves it, and which edges are live, opt-in, or deliberately absent.
---

# Context map

`warehouse-systems` is a fleet of Go services, one bounded context each, plus
an external WCS tier that is not built. This page is this service's slice of
the fleet context map, drawn with the
[ddd-crew Context Mapping](https://github.com/ddd-crew/context-mapping)
vocabulary. It is honest about the difference between **live** (a real
topic or route, a real adapter, running code, on by default), **opt-in**
(the adapter exists but an env var switches it on), and **deliberately
absent** (a real relationship in the domain with no wire, by decision).

## Pattern legend

| Abbreviation | Pattern | Sits on |
| --- | --- | --- |
| **U / D** | Upstream / Downstream — who can change the contract and who must follow | each end of an edge |
| **OHS** | Open Host Service — a general-purpose protocol published for many clients | upstream end |
| **PL** | Published Language — a documented interchange format (here: CloudEvents types + `apis/asyncapi.yaml`, `apis/openapi.yaml`) | upstream end |
| **CF** | Conformist — downstream adopts the upstream model without translation | downstream end |
| **ACL** | Anti-Corruption Layer — downstream translates at its boundary | downstream end |
| **C/S** | Customer/Supplier — downstream's needs are a real input to upstream's planning | the edge |
| **P** | Partnership | the edge (none here) |
| **SK** | Shared Kernel | the edge (none here — no shared Go types or tables) |
| **Separate Ways** | No integration, by decision | — |

## The map

Every arrow points from **upstream to downstream** (the direction the model
flows), whatever the transport direction of the call.

```mermaid
flowchart LR
    subgraph WES["WES tier"]
        WP["wes-work-planning<br/>Core"]
        FE["fulfillment-execution<br/>Core, THIS CONTEXT"]
        PPM["process-path-management"]
        LP["labor-performance<br/>Supporting"]
        WFM["workforce-management<br/>Supporting"]
        OA["warehouse-ops-agent"]
    end
    subgraph WMS["WMS tier"]
        OM["order-management<br/>Generic/Supporting"]
        IS["inventory-storage<br/>Core"]
        FL["facility-layout<br/>Generic"]
        PM["product-master<br/>Supporting"]
    end
    subgraph WCSTIER["WCS tier, not built"]
        WCS["WCS / equipment<br/>Generic"]
    end

    WP ==>|"U: PL / D: ACL - C/S<br/>Kafka com.warehouse.wes.work-planning.workunit.WorkReleased"| FE
    FE ==>|"U: OHS+PL / D: CF - C/S feedback<br/>Kafka com.warehouse.wes.fulfillment-execution.task.TaskCompleted"| WP
    FE ==>|"U: OHS+PL / D: CF<br/>Kafka ...fulfillment-execution.task.TaskCompleted"| LP
    FE ==>|"U: OHS+PL / D: CF<br/>Kafka ...task.TaskCPTMissed and ...package.PackageManifested"| OM
    FE -->|"U: OHS / D: CF<br/>REST GET /capacity/capability"| WFM
    FE -->|"U: OHS / D: CF<br/>MCP tools and REST GET /tasks?orderRef="| OA
    PPM -.->|"opt-in - U: PL / D: CF<br/>Kafka ...process-path-management.processpath.ProcessPath*"| FE
    PM -.->|"opt-in - U: PL / D: ACL, local copy<br/>Kafka com.warehouse.wms.product-master.product.ProductClassified"| FE
    FL -.->|"opt-in - U: OHS / D: ACL<br/>REST GET /locations/locationCode"| FE
    FE -.-x|"deliberately absent - U / D: ACL seam<br/>ports.EquipmentCommandPort, no adapter"| WCS

    classDef this fill:#2b6cb0,stroke:#1a365d,stroke-width:3px,color:#fff
    classDef absent stroke-dasharray: 6 4
    class FE this
    class WCS absent
```

Source: `internal/adapters/inbound/kafka/consumer.go`,
`internal/adapters/outbound/kafka/publisher.go`,
`internal/adapters/outbound/kafkacatalog/consumer.go`,
`internal/adapters/inbound/kafka/product_classified_consumer.go`,
`internal/adapters/outbound/productclassificationcopy/`,
`internal/adapters/outbound/facilitylayout/client.go`,
`internal/adapters/inbound/http/router.go`, `cmd/mcp/router.go`,
`internal/application/ports/equipment.go`, `apis/asyncapi.yaml`.
Thick arrows are Kafka (live when `EVENT_PUBLISHER=kafka`); thin solid
arrows are synchronous calls into this service; dotted arrows are opt-in or
absent. Omitted: edges between *other* contexts (see each service's own
map), this service's internal analytics topic
(`warehouse.fulfillment.analytics`, produced and consumed only by this
context), and browser clients (`warehouse-console` / the `web/` MFE), which
are not bounded contexts. Path parameters are written without braces in the
diagram labels.

## Relationships with evidence

| # | Upstream → Downstream | Patterns | Technology | Status | Evidence (this repo) |
| --- | --- | --- | --- | --- | --- |
| 1 | `wes-work-planning` → **this** | C/S; U: PL; D: **ACL** | Kafka `warehouse.work-planning.events`, `com.warehouse.wes.work-planning.workunit.WorkReleased`; group `WORK_RELEASED_CONSUMER_GROUP` (default `fulfillment-execution`) | **Live** (consumer always starts) | `internal/adapters/inbound/kafka/consumer.go` (`TypeWorkReleased`, `WorkReleasedData`, `handleClaimedEvent` → `CreateTask`) |
| 2 | **this** → `wes-work-planning` | C/S (feedback edge); U: OHS + PL; D: CF | Kafka `warehouse.fulfillment.events`, `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` | **Live** with `EVENT_PUBLISHER=kafka` | `internal/adapters/outbound/kafka/publisher.go` (`encodeTaskCompleted`, `work_unit_id` enrichment) |
| 3 | **this** → `labor-performance` | U: OHS + PL; D: CF | Kafka `warehouse.fulfillment.events`, `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` (`associate_id`, `duration_seconds`, `task_type`) | **Live** with `EVENT_PUBLISHER=kafka` | same publisher; [ADR-0014](../adr/0014-labor-performance-integration-hooks.md), [ADR-0023](../adr/0023-task-type-on-wire.md) |
| 4 | **this** → `order-management` | U: OHS + PL; D: CF | Kafka `warehouse.fulfillment.events`, `com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed`, `com.warehouse.wes.fulfillment-execution.package.PackageManifested` | **Live** with `EVENT_PUBLISHER=kafka` | `publisher.go` (`encodeTaskCPTMissed`, `encodePackageManifested`); [ADR-0025](../adr/0025-cpt-missed-sweep-and-package-manifested.md) |
| 5 | **this** → `workforce-management` | U: OHS; D: CF | REST `GET /capacity/{capability}` (called by workforce-management) | **Live** | `internal/adapters/inbound/http/router.go`, `usecases.GetInstalledCapacity`; [ADR-0018](../adr/0018-installed-capacity-read-endpoint.md) |
| 6 | **this** → `warehouse-ops-agent` | U: OHS; D: CF | MCP Streamable HTTP (`cmd/mcp`, tools `get_queue_status`, `find_claimable_work`, `diagnose_stuck_tasks`, `complete_task`, plus `get_fulfillment_throughput_report` / `get_on_time_to_cpt` when `REPORTS_BASE_URL` is set); REST `GET /tasks?orderRef=` from the console BFF | **Live** | `cmd/mcp/router.go`, `internal/adapters/inbound/mcp/tools.go`; [ADR-0008](../adr/0008-mcp-inbound-adapter.md), [ADR-0013](../adr/0013-fulfillment-mfe-console-adoption.md) |
| 7 | `process-path-management` → **this** | U: PL; D: CF | Kafka `warehouse.process-path-management.events`, `com.warehouse.wes.process-path-management.processpath.ProcessPathCreated` / `ProcessPathUpdated` / `ProcessPathDeactivated`; per-process group | **Opt-in** (`PATH_CATALOGUE_SOURCE=kafka`; default `file` reads the same shape from YAML) | `internal/adapters/outbound/kafkacatalog/consumer.go`; [ADR-0017](../adr/0017-process-path-catalogue-as-configuration.md) |
| 8 | `product-master` → **this** | U: PL; D: **ACL** (`ports.ClassificationInfo`) over a local copy | Kafka `warehouse.product-master.events`, `com.warehouse.wms.product-master.product.ProductClassified` → table `product_classification_copy`, read per scanned SKU at seal time; group `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` | **Opt-in** (`PRODUCT_CLASSIFICATION_MODE=kafka`; default permissive no-op; `http`, the former inventory-storage REST lookup, fails the boot) | `internal/adapters/inbound/kafka/product_classified_consumer.go`, `internal/adapters/outbound/productclassificationcopy/`; [ADR-0039](../adr/0039-product-classification-local-copy.md) (supersedes the live lookup of [ADR-0010](../adr/0010-package-segregation-and-sort-lane.md)) |
| 9 | `facility-layout` → **this** | U: OHS; D: **ACL** (`ports.LocationRoleInfo`), conforming to the `LocationRole` vocabulary | REST `GET /locations/{locationCode}`, called once per `RegisterStation` with a `locationCode`, behind retry + circuit breaker | **Opt-in** (`LOCATION_ROLE_MODE=http` + `FACILITY_LAYOUT_BASE_URL`; default permissive no-op) | `internal/adapters/outbound/facilitylayout/`; [ADR-0024](../adr/0024-station-location-code-and-workcenter-role-check.md) |
| 10 | **this** → WCS / equipment | Strategically U: this; D: WCS; ACL seam on this side | none | **Deliberately absent** — the port declares no methods and has no adapter | `internal/application/ports/equipment.go`; [ADR-0015](../adr/0015-wcs-equipment-anti-corruption-seam.md) |
| 11 | `inventory-storage`, `facility-layout`, `order-management` events → **this** | Separate Ways | none | **Deliberately absent** — stock, layout and order facts reach this context only through what `wes-work-planning` releases (plus the opt-in facility-layout lookup and product-master copy above) | no consumer for those topics in `internal/adapters/inbound/kafka/` |

All REST and MCP surfaces are unauthenticated
([ADR-0022](../adr/0022-remove-rest-mcp-auth.md)). No Shared Kernel and no
Partnership exist: no Go type, table or library is shared with another
context — even the DOT segregation matrix is a deliberate, documented
duplicate of inventory-storage's (`internal/domain/package/segregation.go`).

## The control loop with Work Planning

Edges 1 and 2 form a closed loop: Work Planning releases, this service
executes, Work Planning learns that it landed — the **drum-buffer-rope
feedback edge: Execution → Orchestration**.

```mermaid
sequenceDiagram
    autonumber
    participant WP as wes-work-planning
    participant K1 as warehouse.work-planning.events
    participant FE as fulfillment-execution
    participant ST as Station (Pick)
    participant K2 as warehouse.fulfillment.events

    WP->>K1: evt: WorkReleased (path_id, work_unit_id, cpt, ref)
    K1->>FE: consume, filter on the full CloudEvents type
    FE->>FE: MarkProcessed(id), skip if already seen
    FE->>FE: CreateTask(PICK, cpt, orderRef = work_unit_id)
    ST->>FE: cmd: POST /stations/station-03/claim-next
    FE-->>ST: 200 earliest-CPT matching task, leased 5 min
    ST->>FE: cmd: POST /tasks/task-1/complete
    FE->>K2: evt: TaskCompleted (task_id, station_id, work_unit_id)
    K2->>WP: consume
    WP->>WP: RecordCompletion(work_unit_id)
```

Source: `internal/adapters/inbound/kafka/consumer.go`,
`internal/application/usecases/claim_next.go`,
`internal/application/usecases/complete_task.go`,
`internal/adapters/outbound/kafka/publisher.go`. Omits the outbox relay hop
and the analytics fan-out (see [Domain message flow](../ddd/domain-message-flow.md)).

## Why the WES tier is two services, not one

The reference model describes Work Orchestration and Task & Labor Management
as a **Partnership** — "they evolve together; sequencing and assignment are
two halves of one optimization loop."

This platform splits them anyway. Work Planning answers *how much* work
should be on the floor and when to release it — changing when flow-balancing
policy changes. Fulfillment Execution answers *how a released unit safely
reaches completion* — changing when dispatch or claim semantics change.
Those are different reasons to change, on different cadences. So the edge
is Customer/Supplier over a Published Language, not a Partnership.

The cost of the split is real: the two must agree on `work_unit_id` as a
correlation key and on the CPT semantics that drive priority. That agreement
is the published language on both topics, and it is exactly what the
`apis/asyncapi.yaml` catalogue documents.

The *strategic* reasoning behind each pattern is on
[Context relationships](../ddd/context-relationships.md); the operational
detail (envelopes, mappings, idempotency, configuration) is on
[Integration contracts](./integration-contracts.md).
