---
id: class-diagram
title: Class diagram
sidebar_label: Class diagram
sidebar_position: 15
description: UML class diagrams of the Fulfillment Execution domain model — aggregates, entities, value objects, enumerations and domain events from internal/domain — plus the hexagonal ports-and-adapters view.
---

# Class diagram

UML class diagrams drawn from `internal/domain/**` and
`internal/application/ports/`. Names are the real Go identifiers; fields
are the unexported struct fields (shown private, `-`), methods are the
state-changing ones and key queries (public, `+`). Go has no classes or
enums: an `<<Enumeration>>` is a named string type with a `const` block,
and a `<<Repository>>` is a port interface.

## 1. Aggregates, entities and value objects

```mermaid
classDiagram
    direction LR
    class Task {
        <<AggregateRoot>>
        -id TaskId
        -taskType Type
        -status Status
        -cpt CPT
        -orderRef OrderRef
        -requiredCapabilities CapabilitySet
        -lease Lease
        -claimedAt time.Time
        -fragile bool
        -giftWrap bool
        +Claim(stationId, stationCapabilities, now, leaseDuration) error
        +RenewLease(stationId, now, leaseDuration) error
        +Complete(stationId, now) error
        +ExpireLeaseIfDue(now) bool
        +IsAvailable(now) bool
        +IsCPTMissed(now) bool
    }
    class Lease {
        <<ValueObject>>
        +StationId StationId
        +Expiry time.Time
    }
    class TaskType {
        <<Enumeration>>
        PICK
        PACK
        SLAM
        REBIN
    }
    class TaskStatus {
        <<Enumeration>>
        PENDING
        CLAIMED
        COMPLETED
    }
    class Station {
        <<AggregateRoot>>
        -id StationId
        -capabilities CapabilitySet
        -occupant OccupantId
        -locationCode string
        +CheckIn(occupant) error
        +CheckOut() error
        +SetLocationCode(locationCode)
        +CanAccept(required) bool
        +ValidateAccept(required) error
    }
    class OccupantId {
        <<ValueObject>>
    }
    class Package {
        <<AggregateRoot>>
        -id PackageId
        -orderRef OrderRef
        -taskId TaskId
        -status Status
        -scannedContents string[]
        -scannedHazardClasses int[]
        -fragileHandling bool
        -giftWrapRequested bool
        +ScanItemWithClass(sku, hazardClass) error
        +Seal() error
        +Weigh(expectedWeight, actualWeight) bool, error
        +SortLane() string
    }
    class PackageStatus {
        <<Enumeration>>
        OPEN
        SEALED
        LABELED
        DIVERTED
    }
    class SortLane {
        <<Enumeration>>
        HAZMAT_LANE
        FRAGILE_NO_TILT
        STANDARD
    }
    class OrderConsolidation {
        <<AggregateRoot>>
        -orderRef string
        -requiredLines set of string
        -arrivedLines set of string
        +RecordArrival(lineId) error
        +IsComplete() bool
    }
    class CapabilitySet {
        <<ValueObject>>
        +HasAll(required) bool
        +Contains(c) bool
    }
    class Capability {
        <<ValueObject>>
    }
    class CPT {
        <<ValueObject>>
        -at time.Time
        +Before(other) bool
    }
    class TaskId {
        <<ValueObject>>
    }
    class StationId {
        <<ValueObject>>
    }
    class PackageId {
        <<ValueObject>>
    }
    class OrderRef {
        <<ValueObject>>
    }

    Task *-- "0..1" Lease : holds
    Task --> TaskType
    Task --> TaskStatus
    Task *-- CPT
    Task *-- CapabilitySet : requires
    Task --> OrderRef
    Task --> TaskId
    Lease --> StationId : owner
    Station *-- CapabilitySet : holds
    Station *-- "0..1" OccupantId
    Station --> StationId
    CapabilitySet o-- "*" Capability
    Package --> PackageStatus
    Package ..> SortLane : derives
    Package --> PackageId
    Package --> OrderRef
    Package --> TaskId : PACK task, by id
    OrderConsolidation ..> OrderRef : keyed by
```

Source: `internal/domain/task/task.go`, `internal/domain/station/station.go`,
`internal/domain/package/package.go`,
`internal/domain/consolidation/order_consolidation.go`,
`internal/domain/shared/{ids,capability,cpt}.go`. The two Go types named
`Status` are shown as `TaskStatus` and `PackageStatus`, and `task.Type` as
`TaskType`, to keep the names unique in one diagram. Omits constructors
(`New`, `Rehydrate`, `RehydrateWithLocation`), plain getters, and the
segregation matrix in `segregation.go` (`IsSegregationIncompatible`).
Aggregates reference each other **by id only** — `Package.taskId`,
`Lease.StationId` — never by object; no aggregate holds another.

## 2. Domain events and configuration types

```mermaid
classDiagram
    direction TB
    class DomainEvent {
        <<interface>>
        +EventName() string
        +OccurredAt() time.Time
    }
    class TaskCreated {
        <<DomainEvent>>
        +TaskId TaskId
    }
    class TaskClaimed {
        <<DomainEvent>>
        +TaskId TaskId
        +StationId StationId
    }
    class LeaseExpired {
        <<DomainEvent>>
        +TaskId TaskId
    }
    class TaskCompleted {
        <<DomainEvent>>
        +TaskId TaskId
        +StationId StationId
    }
    class TaskCPTMissed {
        <<DomainEvent>>
        +TaskId TaskId
        +OrderRef OrderRef
        +TaskType string
        +CPT time.Time
    }
    class ItemPicked {
        <<DomainEvent>>
        +TaskId TaskId
    }
    class PackageSealed {
        <<DomainEvent>>
        +PackageId PackageId
    }
    class WeightDiscrepancyDetected {
        <<DomainEvent>>
        +PackageId PackageId
        +ExpectedWeight float64
        +ActualWeight float64
    }
    class PackageDiverted {
        <<DomainEvent>>
        +PackageId PackageId
    }
    class LabelApplied {
        <<DomainEvent>>
        +PackageId PackageId
    }
    class PackageManifested {
        <<DomainEvent>>
        +PackageId PackageId
        +OrderRef OrderRef
    }
    class ItemArrivedAtRebin {
        <<DomainEvent>>
        +OrderRef OrderRef
        +LineId string
    }
    class OrderConsolidated {
        <<DomainEvent>>
        +OrderRef OrderRef
    }
    class Catalogue {
        <<ValueObject>>
        +Lookup(id) PathDefinition, error
        +Ids() string[]
    }
    class PathDefinition {
        <<ValueObject>>
        +Id string
        +MatchPrefix string
        +Direct bool
        +RequiredCapabilities string[]
        +DestinationLocationRole string
    }

    DomainEvent <|.. TaskCreated
    DomainEvent <|.. TaskClaimed
    DomainEvent <|.. LeaseExpired
    DomainEvent <|.. TaskCompleted
    DomainEvent <|.. TaskCPTMissed
    DomainEvent <|.. ItemPicked
    DomainEvent <|.. PackageSealed
    DomainEvent <|.. WeightDiscrepancyDetected
    DomainEvent <|.. PackageDiverted
    DomainEvent <|.. LabelApplied
    DomainEvent <|.. PackageManifested
    DomainEvent <|.. ItemArrivedAtRebin
    DomainEvent <|.. OrderConsolidated
    Catalogue *-- "*" PathDefinition
```

Source: `internal/domain/shared/events.go`,
`internal/domain/pathcatalog/path_definition.go`. Every event embeds an
unexported `base` struct (`Name`, `At`) that implements `DomainEvent`; it is
drawn as interface realization. Omits the `New...` constructors.

## 3. Hexagonal view: ports and adapters

```mermaid
classDiagram
    direction LR
    class TaskRepo {
        <<Repository>>
        Save · SaveClaim · FindById
        FindClaimableByType · FindAllClaimed
        FindOpenPastCPT · CountByTypeAndStatus · FindByOrderRef
    }
    class StationRepo {
        <<Repository>>
        Save · FindById · CountByCapability
    }
    class PackageRepo {
        <<Repository>>
        Save · FindById · FindByTaskId · FindByOrderRef
    }
    class OrderConsolidationRepo {
        <<Repository>>
        Save · FindByOrderRef · FindByOrderRefForUpdate
    }
    class EventPublisher {
        <<interface>>
        Publish
    }
    class UnitOfWork {
        <<interface>>
        Execute
    }
    class PathCatalogue {
        <<interface>>
        Lookup
    }
    class ProcessedEvents {
        <<interface>>
        MarkProcessed
    }
    class ProductClassificationLookup {
        <<interface>>
        GetClassification
    }
    class LocationRoleLookup {
        <<interface>>
        GetRole
    }
    class EquipmentCommandPort {
        <<interface>>
        no methods - ADR-0015
    }
    class Usecases {
        <<ApplicationService>>
        17 use cases
    }
    class HttpHandlers {
        <<InboundAdapter>>
        inbound/http
    }
    class McpServer {
        <<InboundAdapter>>
        inbound/mcp
    }
    class WorkReleasedConsumer {
        <<InboundAdapter>>
        inbound/kafka
    }
    class PostgresAdapters {
        <<OutboundAdapter>>
        outbound/postgres
    }
    class MemoryAdapters {
        <<OutboundAdapter>>
        outbound/memory
    }
    class KafkaEncoders {
        <<OutboundAdapter>>
        outbound/kafka + outbound/events
    }
    class OutboxPublisher {
        <<OutboundAdapter>>
        postgres.OutboxPublisher + OutboxRelay
    }
    class CatalogueLoaders {
        <<OutboundAdapter>>
        outbound/filecatalog + outbound/kafkacatalog
    }
    class ClassificationCopy {
        <<OutboundAdapter>>
        outbound/productclassificationcopy
    }
    class FacilityLayoutClient {
        <<OutboundAdapter>>
        outbound/facilitylayout
    }

    HttpHandlers --> Usecases
    McpServer --> Usecases
    WorkReleasedConsumer --> Usecases
    WorkReleasedConsumer --> PathCatalogue
    WorkReleasedConsumer --> ProcessedEvents
    Usecases --> TaskRepo
    Usecases --> StationRepo
    Usecases --> PackageRepo
    Usecases --> OrderConsolidationRepo
    Usecases --> EventPublisher
    Usecases --> UnitOfWork
    Usecases --> ProductClassificationLookup
    Usecases --> LocationRoleLookup
    PostgresAdapters ..|> TaskRepo
    PostgresAdapters ..|> StationRepo
    PostgresAdapters ..|> PackageRepo
    PostgresAdapters ..|> OrderConsolidationRepo
    PostgresAdapters ..|> UnitOfWork
    PostgresAdapters ..|> ProcessedEvents
    MemoryAdapters ..|> TaskRepo
    MemoryAdapters ..|> StationRepo
    MemoryAdapters ..|> PackageRepo
    MemoryAdapters ..|> OrderConsolidationRepo
    MemoryAdapters ..|> ProcessedEvents
    KafkaEncoders ..|> EventPublisher
    OutboxPublisher ..|> EventPublisher
    CatalogueLoaders ..|> PathCatalogue
    ClassificationCopy ..|> ProductClassificationLookup
    FacilityLayoutClient ..|> LocationRoleLookup
```

Source: `internal/application/ports/ports.go`, `ports/equipment.go`,
`internal/adapters/**`, `cmd/execution/main.go`,
`internal/composition/publisher.go`. Omits `Clock` and `Metrics` ports
(implemented by `memory` clock and `internal/observability`), the MCP
adapter's narrow `TaskQueries` read port, the analytics side
(`inbound/kafka/analytics_consumer.go`, `outbound/analyticsstore`), and the
idempotency and readiness middleware. The dependency direction is enforced
by arch-go in `internal/architecture/` (`make arch-test`).
