---
id: eventstorming
title: EventStorming
sidebar_label: EventStorming
sidebar_position: 14
description: Design-level EventStorming of Fulfillment Execution in ddd-crew cheat-sheet notation — dispatch, Rebin-Pack-SLAM and the sweeps — with every sticky traced to code and hotspots taken from real known gaps.
---

# EventStorming

Design-level [EventStorming](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet)
of the three main processes, reconstructed from the code rather than from a
workshop. Read each board left to right: an **actor** issues a
**command**, an **aggregate** decides, a **domain event** records the
fact, a **policy** reacts ("whenever ... then ..."), **read models** inform
the next decision, and **external systems** sit on the edges. **Hotspots**
are real open issues from the ADRs and docs, not invented ones.

## Legend

```mermaid
flowchart LR
    A["Actor"]:::actor
    C["Command"]:::command
    AG["Aggregate"]:::aggregate
    E["Domain event"]:::event
    P["Policy"]:::policy
    R["Read model"]:::readmodel
    X["External system"]:::external
    H["Hotspot"]:::hotspot
    A --> C --> AG --> E --> P
    R -.-> A
    X --> C
    H -.- AG

    classDef actor fill:#fef9c3,stroke:#a16207,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6391,color:#000
    classDef aggregate fill:#f7d84a,stroke:#9a7d0a,color:#000
    classDef event fill:#f6a04d,stroke:#a04000,color:#000
    classDef policy fill:#c39bd3,stroke:#6c3483,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#922b21,color:#000
    classDef hotspot fill:#e74c3c,stroke:#7b241c,color:#fff
```

Source: [ddd-crew EventStorming cheat sheet](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet)
colours as fixed by the fleet documentation brief.

## Process 1 — release, dispatch and completion

```mermaid
flowchart LR
    WP["wes-work-planning"]:::external
    E0["WorkReleased"]:::event
    P0["Policy: whenever work is released, create a task from the path catalogue"]:::policy
    C1["CreateTask"]:::command
    T1["Task"]:::aggregate
    E1["TaskCreated"]:::event
    RM1["Queue depth"]:::readmodel
    ST["Station operator"]:::actor
    C2["CheckInStation"]:::command
    S1["Station"]:::aggregate
    C3["ClaimNext"]:::command
    T2["Task"]:::aggregate
    E3["TaskClaimed"]:::event
    C4["RenewLease"]:::command
    C5["CompleteTask"]:::command
    T3["Task"]:::aggregate
    E5["TaskCompleted"]:::event
    LP["labor-performance"]:::external
    WP2["wes-work-planning"]:::external
    H1["Hotspot: claim does not require check-in"]:::hotspot
    H2["Hotspot: ItemPicked defined but never raised"]:::hotspot

    WP --> E0 --> P0 --> C1 --> T1 --> E1
    E1 -.-> RM1
    ST --> C2 --> S1
    RM1 -.-> ST
    ST --> C3 --> T2 --> E3
    ST --> C4 --> T2
    ST --> C5 --> T3 --> E5
    E5 --> WP2
    E5 --> LP
    H1 -.- C3
    H2 -.- T3

    classDef actor fill:#fef9c3,stroke:#a16207,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6391,color:#000
    classDef aggregate fill:#f7d84a,stroke:#9a7d0a,color:#000
    classDef event fill:#f6a04d,stroke:#a04000,color:#000
    classDef policy fill:#c39bd3,stroke:#6c3483,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#922b21,color:#000
    classDef hotspot fill:#e74c3c,stroke:#7b241c,color:#fff
```

Source: `internal/adapters/inbound/kafka/consumer.go`,
`internal/application/usecases/create_task.go`, `check_in_station.go`,
`claim_next.go`, `renew_lease.go`, `complete_task.go`,
`internal/domain/task/task.go`, `internal/domain/station/station.go`.
Omits `RegisterStation` (setup, no event) and the MCP `complete_task` path
(same command).

## Process 2 — Rebin, Pack and SLAM

```mermaid
flowchart LR
    RB["Rebin associate"]:::actor
    C1["ArriveAtRebin"]:::command
    OC["OrderConsolidation"]:::aggregate
    E1["ItemArrivedAtRebin"]:::event
    E2["OrderConsolidated"]:::event
    P1["Policy: whenever the last required line arrives, create the PACK task once"]:::policy
    C2["CreateTask PACK"]:::command
    T1["Task"]:::aggregate
    PK["Packer"]:::actor
    C3["SealPackage"]:::command
    IS["inventory-storage classification"]:::external
    PG["Package"]:::aggregate
    E3["PackageSealed"]:::event
    SL["SLAM line"]:::actor
    C4["RunSlam"]:::command
    PG2["Package"]:::aggregate
    E4["LabelApplied"]:::event
    E5["PackageManifested"]:::event
    E6["WeightDiscrepancyDetected"]:::event
    E7["PackageDiverted"]:::event
    RM["Package read model"]:::readmodel
    OM["order-management"]:::external
    H1["Hotspot: Rebin events never leave the process"]:::hotspot
    H2["Hotspot: SortLane decided but no WCS to act on it"]:::hotspot

    RB --> C1 --> OC --> E1
    OC --> E2
    E2 --> P1 --> C2 --> T1
    PK --> C3 --> PG --> E3
    IS --> C3
    SL --> C4 --> PG2
    PG2 --> E4
    PG2 --> E5
    PG2 --> E6
    PG2 --> E7
    E5 --> OM
    E5 -.-> RM
    E7 -.-> RM
    RM -.-> SL
    H1 -.- E1
    H2 -.- PG

    classDef actor fill:#fef9c3,stroke:#a16207,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6391,color:#000
    classDef aggregate fill:#f7d84a,stroke:#9a7d0a,color:#000
    classDef event fill:#f6a04d,stroke:#a04000,color:#000
    classDef policy fill:#c39bd3,stroke:#6c3483,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#922b21,color:#000
    classDef hotspot fill:#e74c3c,stroke:#7b241c,color:#fff
```

Source: `internal/application/usecases/arrive_at_rebin.go`,
`seal_package.go`, `run_slam.go`, `get_package.go`,
`internal/domain/consolidation/order_consolidation.go`,
`internal/domain/package/package.go`. Omits the PACK task's claim and
completion (process 1 applies unchanged) and the policy ordering detail:
in code `CreateTask` runs before `OrderConsolidated` is published, in the
same transaction.

## Process 3 — sweeps

```mermaid
flowchart LR
    SCH["External scheduler"]:::actor
    C1["ExpireLeases"]:::command
    T1["Task"]:::aggregate
    E1["LeaseExpired"]:::event
    P1["Policy: whenever a lease lapses, the task is claimable again"]:::policy
    RM1["Queue depth"]:::readmodel
    C2["SweepCPTMisses"]:::command
    T2["Task"]:::aggregate
    E2["TaskCPTMissed"]:::event
    OM["order-management"]:::external
    P2["Policy: whenever a CPT is missed, re-promise the order"]:::policy
    PJ["Throughput rollup"]:::readmodel
    H1["Hotspot: nothing in this repo schedules the sweeps"]:::hotspot
    H2["Hotspot: TaskCPTMissed re-fires on every pass"]:::hotspot

    SCH --> C1 --> T1 --> E1 --> P1
    P1 -.-> RM1
    E1 -.-> PJ
    SCH --> C2 --> T2 --> E2 --> OM
    OM --> P2
    H1 -.- SCH
    H2 -.- E2

    classDef actor fill:#fef9c3,stroke:#a16207,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6391,color:#000
    classDef aggregate fill:#f7d84a,stroke:#9a7d0a,color:#000
    classDef event fill:#f6a04d,stroke:#a04000,color:#000
    classDef policy fill:#c39bd3,stroke:#6c3483,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#922b21,color:#000
    classDef hotspot fill:#e74c3c,stroke:#7b241c,color:#fff
```

Source: `internal/application/usecases/expire_leases.go`,
`sweep_cpt_misses.go`, `internal/analytics/`,
`internal/adapters/outbound/kafka/publisher.go`. Omits the lazy expiry
inside `Claim` / `RenewLease` / `Complete`, which frees a lapsed lease
without raising `LeaseExpired`. The re-promise policy lives in
`order-management`, not here.

## Sticky inventory

| Sticky | Kind | Code evidence |
| --- | --- | --- |
| Station operator, Packer, Rebin associate, SLAM line | Actor | REST callers of `router.go` |
| External scheduler | Actor | callers of `POST /tasks/expire-leases`, `POST /tasks/sweep-cpt-misses` |
| `CreateTask`, `CheckInStation`, `ClaimNext`, `RenewLease`, `CompleteTask`, `ArriveAtRebin`, `SealPackage`, `RunSlam`, `ExpireLeases`, `SweepCPTMisses` | Command | `internal/application/usecases/*.go` |
| `Task`, `Station`, `Package`, `OrderConsolidation` | Aggregate | `internal/domain/{task,station,package,consolidation}` |
| `WorkReleased` | Domain event (inbound) | `com.warehouse.wes.work-planning.workunit.WorkReleased`, `internal/adapters/inbound/kafka/consumer.go` |
| `TaskCreated`, `TaskClaimed`, `LeaseExpired`, `TaskCompleted`, `TaskCPTMissed`, `PackageSealed`, `LabelApplied`, `PackageManifested`, `WeightDiscrepancyDetected`, `PackageDiverted`, `ItemArrivedAtRebin`, `OrderConsolidated` | Domain event | `internal/domain/shared/events.go` |
| Create a task from the path catalogue | Policy | `consumer.go` → `PathCatalogue.Lookup` → `CreateTask` ([ADR-0017](../adr/0017-process-path-catalogue-as-configuration.md)) |
| Create the PACK task once | Policy | `arrive_at_rebin.go` (`wasAlreadyComplete`, `IsComplete`) |
| A lapsed lease is claimable again | Policy | `Task.IsAvailable`, `TaskRepo.FindClaimableByType` |
| Re-promise on CPT miss | Policy (downstream) | `order-management`, [ADR-0025](../adr/0025-cpt-missed-sweep-and-package-manifested.md) |
| Queue depth, Package read model, Throughput rollup | Read model | `get_queue_depth.go`; `get_package.go` ([ADR-0033](../adr/0033-package-read-model.md)); `throughput_rollup` ([ADR-0012](../adr/0012-analytical-data-product.md)) |
| `wes-work-planning`, `labor-performance`, `order-management`, `inventory-storage` | External system | `apis/asyncapi.yaml`; `productclassification/client.go` |
| Claim does not require check-in | Hotspot | [Aggregates & invariants](./aggregates-and-invariants.md) "Honest status"; `claim_next.go` never reads occupancy |
| `ItemPicked` never raised | Hotspot | [Domain events](./domain-events.md); no caller of `shared.NewItemPicked` outside tests |
| Rebin events never leave the process | Hotspot | not in `inIntegrationContract` / `inAnalyticsContract`, not in `apis/asyncapi.yaml` ([ADR-0016](../adr/0016-rebin-and-order-consolidation.md)) |
| SortLane with no WCS | Hotspot | `Package.SortLane`; `ports.EquipmentCommandPort` has no methods ([ADR-0010](../adr/0010-package-segregation-and-sort-lane.md), [ADR-0015](../adr/0015-wcs-equipment-anti-corruption-seam.md)) |
| Sweeps are not scheduled here | Hotspot | [Task lifecycle](../overview/task-lifecycle.md); no ticker in `cmd/execution/main.go` |
| `TaskCPTMissed` re-fires | Hotspot | `sweep_cpt_misses.go` changes no state ([ADR-0025](../adr/0025-cpt-missed-sweep-and-package-manifested.md)) |
