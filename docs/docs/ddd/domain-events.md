---
id: domain-events
title: Domain events
sidebar_label: Domain events
sidebar_position: 3
description: All thirteen past-tense domain events raised by this bounded context, which aggregate raises each, what triggers it, and which are actually published outside the process today.
---

# Domain events

Thirteen past-tense facts, all defined in `internal/domain/shared/events.go`.
Every one satisfies the same tiny interface:

```go
type DomainEvent interface {
    EventName() string
    OccurredAt() time.Time
}
```

Events are **deliberately thin** — most carry only the aggregate id. Enrichment
for the wire happens in the outbound adapter, never on the event itself. See
[ADR-0004](../adr/0004-kafka-integration-events-and-envelope.md) for why.

## The catalogue

| Event | Aggregate | Raised when | Payload fields | Published externally? |
| --- | --- | --- | --- | --- |
| `TaskCreated` | Task | `CreateTask` puts a new unit of work in the pool | `TaskId` | No — in-process only |
| `TaskClaimed` | Task | `ClaimNext` leases a task to a station | `TaskId`, `StationId` | No — in-process only |
| `LeaseExpired` | Task | `ExpireLeases` frees a task whose lease lapsed | `TaskId` | No — in-process only |
| `TaskCompleted` | Task | `CompleteTask` succeeds | `TaskId`, `StationId` | **Yes** — Kafka, `warehouse.fulfillment.events` |
| `TaskCPTMissed` | Task | `SweepCPTMisses` finds an open task at or past its CPT | `TaskId`, `OrderRef`, `TaskType`, `CPT` | **Yes** — Kafka, `warehouse.fulfillment.events` ([ADR-0025](../adr/0025-cpt-missed-sweep-and-package-manifested.md)) |
| `ItemPicked` | Task | *(defined; not raised today — see below)* | `TaskId` | No |
| `PackageSealed` | Package | `SealPackage` seals a carton | `PackageId` | No — in-process only |
| `WeightDiscrepancyDetected` | Package | SLAM finds actual weight outside tolerance | `PackageId`, `ExpectedWeight`, `ActualWeight` | No — in-process only |
| `LabelApplied` | Package | SLAM passes and the shipping label is applied | `PackageId` | No — in-process only |
| `PackageManifested` | Package | SLAM passes (raised alongside `LabelApplied`) | `PackageId`, `OrderRef` | **Yes** — Kafka, `warehouse.fulfillment.events` ([ADR-0025](../adr/0025-cpt-missed-sweep-and-package-manifested.md)) |
| `PackageDiverted` | Package | SLAM fails and the package is routed off the standard path | `PackageId` | No — in-process only |
| `ItemArrivedAtRebin` | OrderConsolidation | `ArriveAtRebin` records a line arrival | `OrderRef`, `LineId` | No — in-process only, not in `apis/asyncapi.yaml` |
| `OrderConsolidated` | OrderConsolidation | The last required line arrives and the PACK task is created | `OrderRef` | No — in-process only, not in `apis/asyncapi.yaml` |

:::caution Published ≠ defined
Only **`TaskCompleted`, `TaskCPTMissed` and `PackageManifested`** are carried
onto the integration topic — the allowlist in
`internal/adapters/outbound/kafka/publisher.go` is the source of truth. The
analytics publisher separately forwards a projection-relevant subset to
`warehouse.fulfillment.analytics` ([ADR-0012](../adr/0012-analytical-data-product.md)).
Everything else goes only to the in-process publisher. `apis/asyncapi.yaml`
documents eleven of the thirteen (all but the two Rebin events) and marks
publication status per message; this table is the same truth in one place.

`ItemPicked` is the one event that is defined and tested but never raised by
any use case. The Pick path is currently modelled at task granularity (claim →
complete) rather than item granularity. It stays in the catalogue because it
is part of the intended model, and removing it would lose that intent.
:::

## Which use case raises what

```mermaid
flowchart LR
    CT["CreateTask"] --> TC["TaskCreated"]
    CN["ClaimNext"] --> TCL["TaskClaimed"]
    EL["ExpireLeases"] --> LE["LeaseExpired"]
    CMP["CompleteTask"] --> TCP["TaskCompleted"]
    SP["SealPackage"] --> PS["PackageSealed"]
    RS["RunSlam"] -->|within tolerance| LA["LabelApplied"]
    RS -->|within tolerance| PM["PackageManifested"]
    RS -->|outside tolerance| WD["WeightDiscrepancyDetected"]
    RS -->|outside tolerance| PD["PackageDiverted"]
    SW["SweepCPTMisses"] --> CM["TaskCPTMissed"]
    AR["ArriveAtRebin"] --> IA["ItemArrivedAtRebin"]
    AR -->|last line| OC["OrderConsolidated"]
    TCP ==>|Kafka| K[("warehouse.fulfillment.events")]
    CM ==>|Kafka| K
    PM ==>|Kafka| K
```

`RegisterStation`, `CheckInStation`, `CheckOutStation`, `RenewLease` and the
three reads (`GetQueueDepth`, `GetTasksByOrderRef`, `GetInstalledCapacity`)
raise **nothing**. Registering or staffing a station is an operational action
with no existing fact that fits it, and the deliberate choice was not to
invent events just for symmetry — an event catalogue padded with facts nobody
consumes is worse than a short honest one.

## The one event with two facts

`RunSlam` publishes **two** events on the failure branch, in a single call:

```go
uc.Publisher.Publish(ctx,
    shared.NewWeightDiscrepancyDetected(packageId, expectedWeight, actualWeight, now),
    shared.NewPackageDiverted(packageId, now),
)
```

They are separate on purpose. `WeightDiscrepancyDetected` is the
**measurement** — it carries both weights and is what a quality or
loss-prevention consumer wants. `PackageDiverted` is the **routing decision** —
it is what a materials-handling consumer wants. Collapsing them into one event
would force every consumer to care about both concerns.

Note the argument order: the event is constructed as
`(packageId, expected, actual, now)` while the use case's own signature is
`Execute(ctx, packageId, actualWeight, expectedWeight)`. The API request body
names both fields explicitly (`actualWeight`, `expectedWeight`) so callers are
never relying on positional order.

## Naming convention on the wire

Externally published events use the CloudEvents `type` convention shared
across the platform:

```
com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>
```

For this context, for example:

```
com.warehouse.wes.fulfillment-execution.task.TaskCompleted
com.warehouse.wes.fulfillment-execution.package.PackageDiverted
```

The `type` attribute encodes the full DDD coordinates of the fact — subdomain,
bounded context, aggregate, event — so a consumer can route or filter purely
on `type` without opening `data`.

:::info CloudEvents 1.0 on the wire
Every Kafka message this service produces or consumes is a CloudEvents 1.0
event in structured content mode (`specversion` / `id` / `source` / `type` /
`subject` / `time` / `datacontenttype` / `dataschema` / `data`) — see
[ADR-0032](../adr/0032-cloudevents-mandatory-envelope.md). Integration
events go to **`warehouse.fulfillment.events`** (read by `wes-work-planning`,
`labor-performance` and `order-management`); analytics events to
`warehouse.fulfillment.analytics`. There is no other envelope. See the
[Events reference](../api-reference/events.md) for the full catalogue.
:::

## Why the events stay thin

`TaskCompleted` carries only `TaskId` and `StationId`. The wire format needs a
third field, `work_unit_id`, so Work Planning can correlate the completion
back to the unit it released.

That field is **not** added to the domain event. Instead the Kafka publisher
looks the task back up through `ports.TaskRepo` and reads `OrderRef()`:

```go
t, err := p.Tasks.FindById(ctx, tc.TaskId)
...
Data: TaskCompletedData{
    TaskId:     string(tc.TaskId),
    StationId:  string(tc.StationId),
    WorkUnitId: workUnitId,   // enriched here, in the adapter
}
```

The reason is that `work_unit_id` is an *integration* concern — it exists
because a particular downstream consumer needs a particular correlation key.
Pushing it into the domain event would make the domain model shaped by a
consumer's needs. The same repo-lookup-enrichment pattern is used in
`inventory-storage`'s publisher for `ReservationRevoked`, so it is a platform
convention rather than a local hack.

## Fragile/FragileHandling did not extend any event payload

The same "events stay thin" discipline applied when `Task.Fragile` and
`Package.FragileHandling` were added: `TaskCreated` still carries only
`TaskId`, and `PackageSealed` still carries only `PackageId`. Both flags are
already visible on their aggregate's own GET response
(`GET /tasks/{id}` — not yet a route today, but `TaskResponse` carries
`fragile`; the `POST /tasks` and `claim-next` responses already do — and the
`POST /tasks/{id}/seal-package` response carries `fragileHandling`), so
nothing consuming these events in-process needs the value pushed onto the
event itself. If a future external consumer needs `fragile` on the wire, the
same repo-lookup-enrichment pattern used for `TaskCompleted`'s
`work_unit_id` is the template to follow — enrich in the adapter, not the
domain event.

## `SortLane` did not extend any event payload either

[ADR-0010](../adr/0010-package-segregation-and-sort-lane.md) made the
identical judgement call for `Package.SortLane()`: `PackageSealed` and
`LabelApplied` still carry only `PackageId` (and `PackageManifested`, added
later by ADR-0025, carries only `PackageId` + `OrderRef`). `SortLane` is fully visible
on the `POST /tasks/{id}/seal-package` response's `sortLane` field, which
is where anything needing it reads it today — there is no in-process
consumer of `PackageSealed`/`LabelApplied` that needs the value on the
event payload. This is a WES-tier decision only: no WCS
device/conveyor/lane-controller integration exists in this repository, so
there is not yet a concrete external consumer whose needs would even
justify a payload change, the same honest gap already accepted for
`fragileHandling`.
