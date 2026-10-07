---
id: domain-message-flow
title: Domain message flow
sidebar_label: Domain message flow
sidebar_position: 13
description: ddd-crew Domain Message Flow Modelling for Fulfillment Execution — four business scenarios showing every command, event and query between people, contexts and systems, using only real routes, MCP tools and CloudEvents types.
---

# Domain message flow

[ddd-crew Domain Message Flow Modelling](https://github.com/ddd-crew/domain-message-flow-modelling)
shows how a business scenario moves between actors and bounded contexts.
Every arrow is prefixed **`cmd:`** (command — asks for a change),
**`evt:`** (event — states a fact) or **`qry:`** (query — asks for data),
and numbered. Replies are left out (they are implied by a `qry:` or a
synchronous `cmd:`); notes say what the receiving context decides. Only
real messages appear: a REST route from
`internal/adapters/inbound/http/router.go`, an MCP tool from
`internal/adapters/inbound/mcp/`, or a CloudEvents type from the consumers
and publishers. Kafka hops are drawn directly between contexts; the topic
is named in the message.

## Scenario 1 — single-line order: release to completion

A work unit is released, a Pick station pulls it, completes it, and
planning and labour learn that it landed.

```mermaid
sequenceDiagram
    autonumber
    actor Picker as Picker at station-03
    participant WP as wes-work-planning
    participant FE as fulfillment-execution
    participant LP as labor-performance
    WP->>FE: evt: com.warehouse.wes.work-planning.workunit.WorkReleased via warehouse.work-planning.events
    Note over FE: CreateTask - PICK task, orderRef = work_unit_id
    Picker->>FE: cmd: POST /stations/station-03/check-in
    Picker->>FE: cmd: POST /stations/station-03/claim-next with taskType PICK
    Note over FE: earliest-CPT PICK task leased to station-03 for 5 minutes
    Picker->>FE: cmd: POST /tasks/task-1/renew-lease
    Picker->>FE: cmd: POST /tasks/task-1/complete
    FE->>WP: evt: com.warehouse.wes.fulfillment-execution.task.TaskCompleted via warehouse.fulfillment.events
    FE->>LP: evt: com.warehouse.wes.fulfillment-execution.task.TaskCompleted with associate_id and task_type
    Note over WP: RecordCompletion of work_unit_id
```

Source: `internal/adapters/inbound/kafka/consumer.go`,
`internal/adapters/inbound/http/router.go`,
`internal/application/usecases/claim_next.go`, `complete_task.go`,
`internal/adapters/outbound/kafka/publisher.go`. Omits the analytics topic,
the outbox relay hop, and the optional lease renewal loop (shown once).

## Scenario 2 — multi-line order: Rebin, Pack and SLAM

Each picked line reaches Rebin; the last one creates the PACK task; the
packer seals the carton; the SLAM line weighs it and manifests it to order
management.

```mermaid
sequenceDiagram
    autonumber
    actor Rebinner as Rebin associate
    actor Packer as Packer at pack-01
    actor Slam as SLAM line
    participant FE as fulfillment-execution
    participant IS as inventory-storage
    participant OM as order-management
    Rebinner->>FE: cmd: POST /rebin/arrivals line-1 of order wu-42
    Note over FE: ItemArrivedAtRebin - in-process only
    Rebinner->>FE: cmd: POST /rebin/arrivals line-2 of order wu-42
    Note over FE: last line - CreateTask PACK and OrderConsolidated
    Packer->>FE: cmd: POST /stations/pack-01/claim-next with taskType PACK
    Packer->>FE: cmd: POST /tasks/task-9/seal-package with contents
    FE->>IS: qry: GET /products/sku-1/classification - opt-in, per SKU
    Note over FE: segregation check, Seal, PackageSealed - reply 201 with sortLane
    Packer->>FE: cmd: POST /tasks/task-9/complete
    Slam->>FE: cmd: POST /packages/pkg-7/slam with actualWeight and expectedWeight
    FE->>OM: evt: com.warehouse.wes.fulfillment-execution.package.PackageManifested via warehouse.fulfillment.events
    Slam->>FE: qry: GET /packages/pkg-7 - reads status LABELED
```

Source: `internal/application/usecases/arrive_at_rebin.go`, `seal_package.go`,
`run_slam.go`, `get_package.go`,
`internal/adapters/outbound/productclassificationcopy/`,
`internal/adapters/outbound/kafka/publisher.go`. Omits the divert branch
(outside tolerance the package becomes `DIVERTED`, raising
`WeightDiscrepancyDetected` and `PackageDiverted` on the analytics topic
only, and nothing reaches order-management), the `TaskCompleted` fan-out
already shown in scenario 1, and the Pick tasks that produced the lines.

## Scenario 3 — the sweeps: a lapsed lease and a missed CPT

An external scheduler drives both sweeps; no code in this repository runs
them on a timer (the Helm chart's `sweeps.enabled` CronJobs are that
external scheduler, on by default: leases every minute, CPT every 5 minutes —
[ADR-0037](../adr/0037-sweeps-scheduled-by-cronjob-default-on.md)).

```mermaid
sequenceDiagram
    autonumber
    participant SCH as Scheduler - external
    participant FE as fulfillment-execution
    participant OM as order-management
    participant AN as fulfillment-projector
    SCH->>FE: cmd: POST /tasks/expire-leases
    Note over FE: CLAIMED tasks with an expired lease go back to PENDING
    FE->>AN: evt: com.warehouse.wes.fulfillment-execution.task.LeaseExpired via warehouse.fulfillment.analytics
    SCH->>FE: cmd: POST /tasks/sweep-cpt-misses
    Note over FE: open tasks at or past CPT, no state change
    FE->>OM: evt: com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed via warehouse.fulfillment.events
    Note over OM: RepromiseOrder
```

Source: `internal/application/usecases/expire_leases.go`,
`sweep_cpt_misses.go`, `internal/adapters/outbound/kafka/publisher.go`,
`analytics_publisher.go`, `internal/analytics/`. Omits that the same
overdue task is reported again on every later sweep.

## Scenario 4 — an operator agent triages a backlog over MCP

```mermaid
sequenceDiagram
    autonumber
    actor Op as Shift lead
    participant OA as warehouse-ops-agent
    participant MCP as fulfillment-execution cmd/mcp
    participant RPT as fulfillment-execution cmd/fulfillment-reports
    Op->>OA: qry: why is the Pick queue growing
    OA->>MCP: qry: get_queue_status processPath PICK
    OA->>MCP: qry: diagnose_stuck_tasks withinSeconds 60
    OA->>MCP: qry: get_fulfillment_throughput_report
    MCP->>RPT: qry: GET /reports/throughput
    Op->>OA: cmd: complete task-5 for station-03
    OA->>MCP: cmd: complete_task taskId task-5, stationId station-03
    Note over MCP: CompleteTask use case, TaskCompleted via the outbox
```

Source: `internal/adapters/inbound/mcp/tools.go`, `report_tool.go`,
`cmd/mcp/main.go`, `internal/adapters/inbound/http/reports_handler.go`.
Omits the `triage_backlog` prompt and `queue://fulfillment/...` resources
(alternative ways to reach the same reads), and the report tools' absence
when `REPORTS_BASE_URL` is unset.
