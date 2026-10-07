---
id: sequence-diagrams
title: Sequence diagrams
sidebar_label: Sequence diagrams
sidebar_position: 17
description: UML sequence diagrams for every command use case of Fulfillment Execution exposed over REST, MCP or the Kafka consumer — derived from the use-case function bodies, including the idempotency middleware, the claim compare-and-set, transaction boundaries and error branches.
---

# Sequence diagrams

One diagram per command use case (grouped where the flow is the same),
drawn from the function bodies in `internal/application/usecases/` and the
adapters that call them. Transaction boundaries are the `atomically(ctx,
UnitOfWork, ...)` blocks: with Postgres they are one database transaction,
and with `EVENT_PUBLISHER=kafka` the publisher writes `outbox_events` rows
inside it. Read-only use cases (`GetQueueDepth`, `GetTasksByOrderRef`,
`GetInstalledCapacity`, `GetPackage`, `GetPackagesByOrderRef`) are a single
repository call and are not drawn.

Common to every REST diagram: a domain or application error is mapped by
`writeError` (`internal/adapters/inbound/http/errors.go`) to an RFC 7807
problem with the status listed on [Aggregates & invariants](./aggregates-and-invariants.md).

## 1. Create a task over REST, with the Idempotency-Key middleware

```mermaid
sequenceDiagram
    autonumber
    actor C as Client
    participant MW as http idempotency middleware
    participant DB as Postgres
    participant H as http.Handlers PostTask
    participant UC as usecases.CreateTask
    participant T as task.Task
    participant R as TaskRepo
    participant P as EventPublisher - outbox
    C->>MW: POST /tasks with Idempotency-Key k1
    alt header missing
        MW-->>C: 400 idempotency-key-required
    end
    MW->>DB: BEGIN, INSERT idempotency_keys k1 ON CONFLICT DO NOTHING
    alt k1 already present
        MW->>DB: SELECT request_hash, status_code, response
        alt hash differs
            MW-->>C: 422 idempotency-key-reused
        else same request
            MW-->>C: replay stored status, headers and body
        end
    else fresh key
        MW->>H: request with the transaction in context
        H->>UC: Execute taskType, cpt, orderRef, required, fragile, giftWrap false
        UC->>T: task.New - status PENDING
        UC->>R: Save task - joins the transaction
        UC->>P: Publish TaskCreated - outbox row in the same transaction
        H-->>MW: 201 with Location /tasks/id, captured
        MW->>DB: UPDATE idempotency_keys set response, COMMIT
        MW-->>C: 201 task
    end
```

Source: `internal/adapters/inbound/http/idempotency.go`, `handlers.go`
(`PostTask`), `internal/application/usecases/create_task.go`,
`internal/pgtx/`, `internal/adapters/outbound/postgres/outbox_publisher.go`.
Omits the in-memory mode, where the middleware is not wired and the call
goes straight to the handler, and request validation (`400
invalid-request`). REST always passes `giftWrap = false`.

## 2. Create a task from WorkReleased

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka warehouse.work-planning.events
    participant CO as inbound/kafka Consumer
    participant PE as ProcessedEvents
    participant CAT as PathCatalogue
    participant UC as usecases.CreateTask
    participant R as TaskRepo
    participant P as EventPublisher
    participant DLQ as warehouse.work-planning.events.dlq
    K->>CO: message
    CO->>CO: decode CloudEvent, filter type WorkReleased
    alt not a valid CloudEvent
        CO->>DLQ: dead-letter immediately, no retry
    else other type
        CO->>CO: ignore and commit offset
    else WorkReleased
        CO->>PE: MarkProcessed event id
        alt already processed
            CO->>CO: skip
        else new
            CO->>CAT: Lookup path_id - longest matchPrefix
            CO->>UC: Execute type from path, cpt, orderRef = work_unit_id, fragile, gift_wrap
            UC->>R: Save task
            UC->>P: Publish TaskCreated
        end
    end
    alt handling failed after 3 attempts
        CO->>DLQ: publish with x-dlq headers when EVENT_PUBLISHER=kafka
    end
    CO->>K: commit offset
```

Source: `internal/adapters/inbound/kafka/consumer.go` (`HandleMessage`,
`handleMessageWithRetry`, `SendToDeadLetter`) and
`internal/application/usecases/consume_work_released.go`
(`ApplyWorkReleased`). Omits backoff timing and the `recover.go` panic
guard. Since ADR-0036 the `MarkProcessed` claim, the catalogue lookup,
the `CreateTask` save and the `TaskCreated` outbox publish all run inside
ONE UnitOfWork — a failed create rolls the claim back with it, so a
redelivery re-applies instead of being lost.

## 3. Claim the next task

```mermaid
sequenceDiagram
    autonumber
    actor S as Station client
    participant H as http.Handlers PostClaimNext
    participant UC as usecases.ClaimNext
    participant SR as StationRepo
    participant R as TaskRepo
    participant T as task.Task
    participant P as EventPublisher
    S->>H: POST /stations/station-03/claim-next with taskType
    H->>UC: Execute stationId, taskType
    UC->>SR: FindById station-03
    alt station not found
        UC-->>H: ErrStationNotFound - 404
    end
    UC->>R: FindClaimableByType taskType, now - earliest CPT first
    loop each candidate
        UC->>T: Claim stationId, station capabilities, now, 5 min
        alt ErrAlreadyClaimed or ErrCapabilityMismatch
            UC->>UC: next candidate
        else claimed in memory
            UC->>R: SaveClaim task - compare-and-set, inside UnitOfWork
            alt lost the race
                UC->>UC: next candidate, nothing published
            else won
                UC->>P: Publish TaskClaimed - same transaction
                UC-->>H: task
                H-->>S: 200 task with lease
            end
        end
    end
    UC-->>H: ErrNoClaimableTask
    H-->>S: 409 no-claimable-task
```

Source: `internal/application/usecases/claim_next.go`,
`internal/adapters/outbound/postgres/task_repo.go` (`SaveClaim`),
`internal/domain/task/task.go` (`Claim`). Omits the `Metrics.TaskClaimed`
counter. The compare-and-set writes only while the row is still `PENDING`
or `CLAIMED` with an expired lease ([ADR-0034](../adr/0034-concurrency-control-for-consolidation-and-claim.md)).

## 4. Renew a lease, and station setup

```mermaid
sequenceDiagram
    autonumber
    actor S as Station client
    participant H as http.Handlers
    participant RL as usecases.RenewLease
    participant RS as usecases.RegisterStation
    participant CI as usecases.CheckInStation
    participant FL as LocationRoleLookup
    participant T as task.Task
    participant ST as station.Station
    participant R as TaskRepo
    participant SR as StationRepo
    S->>H: POST /stations with id, capabilities, locationCode
    H->>RS: Execute
    opt locationCode set and LOCATION_ROLE_MODE=http
        RS->>FL: GetRole locationCode
        alt known and role is not WorkCenter
            RS-->>H: ErrStationLocationNotWorkCenter - 422
        end
    end
    RS->>SR: Save new station
    H-->>S: 201 station, occupied false
    S->>H: POST /stations/station-03/check-in with occupantId
    H->>CI: Execute
    CI->>SR: FindById
    CI->>ST: CheckIn occupant
    alt already occupied
        CI-->>H: ErrOccupied - 409
    end
    CI->>SR: Save
    H-->>S: 200 station
    S->>H: POST /tasks/task-1/renew-lease with stationId
    H->>RL: Execute taskId, stationId
    RL->>R: FindById
    RL->>T: RenewLease stationId, now, 5 min
    alt lease expired, not claimed, completed or not owner
        RL-->>H: ErrNotClaimed, ErrAlreadyCompleted or ErrNotOwner - 409
    end
    RL->>R: Save - no event, no UnitOfWork
    H-->>S: 204
```

Source: `internal/application/usecases/register_station.go`,
`check_in_station.go`, `check_out_station.go`, `renew_lease.go`,
`internal/adapters/outbound/facilitylayout/client.go`. Omits check-out
(same shape as check-in, `ErrNotOccupied` on an empty station). None of
these raise a domain event.

## 5. Complete a task — REST or MCP — and the outbox relay

```mermaid
sequenceDiagram
    autonumber
    actor S as Station client or MCP caller
    participant IN as http PostCompleteTask or mcp complete_task
    participant UC as usecases.CompleteTask
    participant R as TaskRepo
    participant T as task.Task
    participant ENC as kafka encoders
    participant OB as outbox_events
    participant RLY as OutboxRelay in cmd/execution
    participant K as Kafka
    S->>IN: POST /tasks/task-1/complete with stationId
    IN->>UC: Execute taskId, stationId
    UC->>R: FindById
    alt not found
        UC-->>IN: ErrTaskNotFound - 404
    end
    UC->>T: Complete stationId, now
    alt completed, not claimed, lease expired or not owner
        UC-->>IN: ErrAlreadyCompleted, ErrNotClaimed or ErrNotOwner - 409
    end
    UC->>R: Save - BEGIN UnitOfWork
    UC->>ENC: Publish TaskCompleted
    ENC->>R: FindById for work_unit_id, order_ref, task_type, claimedAt
    ENC->>OB: INSERT integration row and analytics row - COMMIT
    IN-->>S: 204
    loop relay poll
        RLY->>OB: unpublished rows in id order
        RLY->>K: write to warehouse.fulfillment.events and warehouse.fulfillment.analytics
        RLY->>OB: set published_at
    end
```

Source: `internal/application/usecases/complete_task.go`,
`internal/adapters/inbound/mcp/tools.go`,
`internal/adapters/outbound/kafka/publisher.go`, `analytics_publisher.go`,
`internal/adapters/outbound/postgres/outbox_publisher.go`, `outbox_relay.go`,
`internal/composition/publisher.go`. Omits the station lookup for
`associate_id`, the `Metrics.TaskCompleted` counter, and the non-Postgres
path where the encoders write to Kafka directly. `cmd/mcp` inserts outbox
rows but never runs the relay.

## 6. Seal a package

```mermaid
sequenceDiagram
    autonumber
    actor S as Pack station
    participant H as http PostSealPackage
    participant UC as usecases.SealPackage
    participant PR as PackageRepo
    participant R as TaskRepo
    participant CL as ProductClassificationLookup
    participant PK as pack.Package
    participant P as EventPublisher
    S->>H: POST /tasks/task-9/seal-package with stationId, contents
    H->>UC: Execute taskId, stationId, contents
    UC->>PR: FindByTaskId
    alt package already exists
        UC-->>H: existing package - idempotent retry
    end
    UC->>R: FindById
    alt not a PACK task
        UC-->>H: ErrWrongTaskType - 422
    else lease missing or expired
        UC-->>H: ErrNotClaimed - 409
    else lease held by another station
        UC-->>H: ErrNotOwner - 409
    end
    UC->>PK: pack.New with fragile and giftWrap from the task
    loop each sku
        UC->>CL: GetClassification sku - fail-open to class 0
        UC->>PK: ScanItemWithClass sku, hazardClass
        alt incompatible with an earlier item
            UC-->>H: ErrPackageSegregationViolation - 409
        end
    end
    UC->>PK: Seal
    alt no contents
        UC-->>H: ErrNoScannedContents - 422
    end
    UC->>PR: Save - inside UnitOfWork
    UC->>P: Publish PackageSealed
    H-->>S: 201 package with sortLane
```

Source: `internal/application/usecases/seal_package.go`,
`internal/domain/package/package.go`, `segregation.go`,
`internal/adapters/outbound/productclassificationcopy/` (local copy of
product-master's classifications, ADR-0039). The ownership check is
`Task.VerifyHeldBy(stationId, now)`: it requires a lease held by the caller
that has not expired at the `Clock`'s `now` (expiry is inclusive, as in
`Complete`), and it does not free the task, so an expired lease is rejected
with `ErrNotClaimed` (the same error `Complete` and `RenewLease` return for
that condition) even when no sweep has run yet; a missing lease is also
`ErrNotClaimed`, and an active lease held by another station stays
`ErrNotOwner` (decided 2026-10-06,
[ADR-0038](../adr/0038-seal-package-expired-lease-is-not-claimed.md)).

## 7. Run SLAM

```mermaid
sequenceDiagram
    autonumber
    actor S as SLAM line
    participant H as http PostRunSlam
    participant UC as usecases.RunSlam
    participant PR as PackageRepo
    participant PK as pack.Package
    participant P as EventPublisher
    S->>H: POST /packages/pkg-7/slam with actualWeight, expectedWeight
    H->>UC: Execute packageId, actual, expected
    UC->>PR: FindById
    alt not found
        UC-->>H: ErrPackageNotFound - 404
    end
    UC->>PK: Weigh expected, actual
    alt not sealed or already processed
        UC-->>H: ErrNotSealed or ErrAlreadyProcessed - 409
    end
    UC->>PR: Save - BEGIN UnitOfWork
    alt within tolerance 0.05
        UC->>P: Publish LabelApplied and PackageManifested
    else outside tolerance
        UC->>P: Publish WeightDiscrepancyDetected and PackageDiverted
    end
    H-->>S: 204 in both cases
```

Source: `internal/application/usecases/run_slam.go`,
`internal/domain/package/package.go` (`Weigh`). Omits the follow-up
`GET /packages/pkg-7` a caller uses to learn the outcome
([ADR-0033](../adr/0033-package-read-model.md)).

## 8. Arrive at Rebin

```mermaid
sequenceDiagram
    autonumber
    actor S as Rebin station
    participant H as http PostArriveAtRebin
    participant UC as usecases.ArriveAtRebin
    participant OR as OrderConsolidationRepo
    participant OC as consolidation.OrderConsolidation
    participant CT as usecases.CreateTask
    participant P as EventPublisher
    S->>H: POST /rebin/arrivals with orderRef, lineId, requiredLineIds, pack parameters
    H->>UC: Execute
    Note over UC,OR: one UnitOfWork for the whole call
    UC->>OR: FindByOrderRefForUpdate - advisory lock and FOR UPDATE
    alt first arrival for the order
        UC->>OC: consolidation.New orderRef, requiredLineIds
    end
    UC->>OC: RecordArrival lineId
    alt unknown line
        UC-->>H: ErrUnknownLine - 422
    else order was already complete
        UC-->>H: no-op
    else
        UC->>OR: Save
        UC->>P: Publish ItemArrivedAtRebin - in-process only
        opt this arrival completes the set
            UC->>CT: Execute PACK, packCPT, orderRef, packRequired, flags
            UC->>P: Publish OrderConsolidated - in-process only
        end
    end
    H-->>S: 204
```

Source: `internal/application/usecases/arrive_at_rebin.go`,
`internal/adapters/outbound/postgres/order_consolidation_repo.go`,
`internal/domain/consolidation/order_consolidation.go`. Omits `CreateTask`'s
inner save and `TaskCreated` (diagram 1). The requiredLineIds of later
arrivals are ignored: the set is fixed by the first arrival.

## 9. The sweeps

```mermaid
sequenceDiagram
    autonumber
    actor SCH as External scheduler
    participant H as http.Handlers
    participant EL as usecases.ExpireLeases
    participant SW as usecases.SweepCPTMisses
    participant R as TaskRepo
    participant T as task.Task
    participant P as EventPublisher
    SCH->>H: POST /tasks/expire-leases
    H->>EL: Execute
    EL->>R: FindAllClaimed
    loop each claimed task
        EL->>T: ExpireLeaseIfDue now
        opt lease expired
            EL->>R: Save - own UnitOfWork per task
            EL->>P: Publish LeaseExpired
        end
    end
    H-->>SCH: 200 freed count
    SCH->>H: POST /tasks/sweep-cpt-misses
    H->>SW: Execute
    SW->>R: FindOpenPastCPT now
    loop each overdue task
        SW->>P: Publish TaskCPTMissed - no state change
    end
    H-->>SCH: 200 reported count
```

Source: `internal/application/usecases/expire_leases.go`,
`sweep_cpt_misses.go`. Omits the partial-failure branch: on the first
error both sweeps stop and return the count so far.
