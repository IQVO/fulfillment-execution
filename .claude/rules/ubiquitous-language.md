# Ubiquitous Language, Aggregates, Domain Events, Use Cases

Full glossary and domain-layer contract for Fulfillment Execution. Read before
touching `internal/domain/` or `internal/application/usecases/`.

## Ubiquitous Language (use these exact names)

- **Task** — a unit of physical work: type Pick | Pack | SLAM | Rebin. States:
  Pending -> Claimed(leased) -> Completed, or lease-expires back to Pending.
  Carries CPT (priority derives from it). At most one active claim.
- **claimNext(stationId, capabilities)** — PULL dispatch. Returns the highest-
  priority (earliest CPT) pending task the station is certified/equipped for.
  Never name a person/station in advance (no push assign(task, station)).
- **Lease** — a claim has a timeout; if not confirmed/completed before expiry
  the task returns to the pool (prevents vanished work). Lease renewal allowed.
- **Station** — a work position with a capability set; one occupant at a time;
  capabilities must match the task handed to it.
- **Package** — pack output: an order becomes a sealed carton. **SLAM
  weigh-check**: actual weight must be within tolerance of expected, else the
  package is diverted.
- **Process path** — Pick / Pack / SLAM / Rebin as named task types (queues),
  not steps. The catalogue of declared paths (id, match prefix, required
  capabilities) is now **configuration**, not a hardcoded prefix guess
  (ADR-0017) — see `internal/domain/pathcatalog/` and the `filecatalog` /
  `kafkacatalog` outbound adapters that load it.
- **Fragile (packing hint)** — `Task.Fragile()` and the derived
  `Package.FragileHandling()`. Stamped onto the Task by `wes-work-planning`
  at release time (from `inventory-storage`'s `ProductClassification`, read
  once upstream — this service never looks the fragile flag up itself; the
  only direct inventory-storage call is the ADR-0010 hazard-class lookup
  below).
  `SealPackage` derives `FragileHandling` from the owning task's flag, not a
  separate caller input. Affects packing/downstream sortation only — does
  not gate claiming.
- **Hazmat (station capability)** — `"hazmat"` is a real, known value of the
  existing open `Capability`/`CapabilitySet` type (see `claimNext` above). A
  Task requiring hazmat handling sets it in `requiredCapabilities`; only a
  Station registered with that capability can claim it. Worked via the
  pre-existing generic capability-matching mechanism — no structural change
  needed (ADR-0009).
- **Package segregation & SortLane (ADR-0010)** — `SealPackage` performs a
  LIVE, synchronous per-scanned-SKU classification lookup (outbound port
  `ports.ProductClassificationLookup`, permissive-by-default HTTP adapter
  mirroring inventory-storage's own `facilitylayout` pattern:
  `PRODUCT_CLASSIFICATION_MODE=http|permissive`) — NOT a value stamped onto
  the Task at release time, because a Pack task's contents (which SKUs get
  scanned into it) are only known live at the scan station, not at release.
  `Package.ScanItemWithClass` rejects a scan whose DOT hazard class is
  incompatible (same 9×9 matrix as inventory-storage, duplicated by
  deliberate cross-repo convention) with an already-scanned item's class,
  raising `ErrPackageSegregationViolation`. `Package.SortLane()` derives
  `HAZMAT_LANE` > `FRAGILE_NO_TILT` > `STANDARD` (hazmat always wins) — a
  WES-tier routing DECISION only; no WCS device/conveyor execution exists or
  is planned in this workspace (see the WCS ACL seam note below).
- **Gift wrap (packing hint, ADR-0011)** — a further handling flag carried
  the same way as Fragile: stamped at release, affects packing/downstream
  only, does not gate claiming.
- **Rebin & OrderConsolidation (ADR-0016)** — REBIN is a task type/queue at
  the same cadence as Pick/Pack/SLAM (not a new bounded context). A small
  execution-scoped aggregate, `consolidation.OrderConsolidation`
  (`internal/domain/consolidation/`), tracks fan-in of an order's required
  lines arriving at Rebin; a Pack task is only created once every required
  line has arrived. It has no visibility outside this service and holds no
  reference to `Task` — lines are identified by string id only. This closes
  a gap this service's own docs previously flagged ("OrderConvergence...
  belongs upstream").
- **Installed capacity (ADR-0018)** — `GET /capacity/{capability}` is a
  read-model projection over the Station registry (how many currently
  registered stations hold a capability, regardless of occupancy) —
  workforce-management calls it to enforce `plannedHeads` against reality on
  `CommitShiftPlan`. An unrecognized capability has an installed count of 0
  (same "count over existing rows" contract as `getQueueDepth`), not an error.
- **WCS/equipment anti-corruption seam (ADR-0015)** — `ports.EquipmentCommandPort`
  in `internal/application/ports/equipment.go` is a deliberately UNIMPLEMENTED
  interface (declares no methods yet) marking where a future WCS/equipment
  integration would attach, without pulling that tier into this workspace.
- **Station location code (ADR-0024)** — `Station.LocationCode()` is an
  optional facility-layout `LocationCode`. With `LOCATION_ROLE_MODE=http`,
  `RegisterStation` looks the code up in facility-layout and rejects a
  KNOWN non-WorkCenter role (`ErrStationLocationNotWorkCenter`, 422);
  permissive by default (recorded unchecked).
- **CPT missed (ADR-0025)** — `Task.IsCPTMissed(now)`: a task still open
  (Pending or Claimed) at or past its CPT. Detected by the Clock-driven
  `SweepCPTMisses`, not on a timer inside the aggregate.

## Aggregates & invariants (enforce in domain, unit-tested)

- **Task**: at most one active claim (at-most-once); no double-complete; a
  claim requires matching capabilities; an expired lease frees the task.
- **Station**: one occupant at a time; claim rejected if capability mismatch.
- **Package**: cannot seal without scanned contents; SLAM diverts when
  |actual-expected| weight > tolerance; same-package DOT segregation
  violation raises `ErrPackageSegregationViolation`.
- **OrderConsolidation**: incomplete until every required line has arrived;
  `IsComplete()` only after all recorded.
- Read models (queue depth by task type, throughput, installed capacity) are
  PROJECTIONS from events/registry state, never source-of-truth state. The
  Package read model (ADR-0033) is different: a side-effect-free direct read
  of the Package aggregate's current state through `PackageRepo`.

## Domain events (past tense)

TaskCreated, TaskClaimed, LeaseExpired, TaskCompleted, ItemPicked,
PackageSealed, WeightDiscrepancyDetected, LabelApplied, PackageDiverted,
TaskCPTMissed, PackageManifested, ItemArrivedAtRebin, OrderConsolidated
(13, all in `internal/domain/shared/events.go`; the last two are ADR-0016's
Rebin events and are in-process only — not in `apis/asyncapi.yaml`).

Full AsyncAPI catalogue, publication status per event, and the CloudEvents
envelope are in `api-and-integration.md` — `TaskCompleted`, `TaskCPTMissed`,
and `PackageManifested` are on the wire today (ADR-0025 added the latter
two, closing the promise-feedback-loop half of order-management's ADR
0014 §5).

## Use cases (application layer)

1. CreateTask(type, cpt, ref, requiredCapabilities, fragile, giftWrap) -> Task in pool
2. ClaimNext(stationId, capabilities) -> leases + returns best-fit pending task
3. RenewLease(taskId, stationId) -> extends lease
4. CompleteTask(taskId, stationId) -> TaskCompleted (validates claim ownership)
5. SealPackage(taskId, contents) -> Package (Pack path); FragileHandling
   derived from the task's Fragile flag; performs a live per-SKU DOT hazard
   classification lookup and rejects on same-package segregation violation
   (ADR-0010)
6. RunSlam(packageId, actualWeight, expectedWeight) -> LabelApplied +
   PackageManifested (SLAM pass, ADR-0025), or WeightDiscrepancyDetected +
   PackageDiverted (SLAM fail — not manifested)
7. GetQueueDepth(taskType) -> read model
8. GetInstalledCapacity(capability) -> read model (ADR-0018)
9. ExpireLeases(now) -> sweeps expired claims back to Pending (Clock-driven)
10. CheckInStation / CheckOutStation(stationId, associateId) -> occupant
    tracking for labor-performance attribution (ADR-0014)
11. SweepCPTMisses(now) -> raises TaskCPTMissed for every still-open task
    past its CPT (Clock-driven, re-fires every pass while overdue; ADR-0025)
12. RegisterStation(stationId, capabilities, locationCode) -> creates or
    re-registers (idempotent) a Station; optional WorkCenter role check (ADR-0024)
13. GetTasksByOrderRef(orderRef) -> every task for one WorkUnit id, read-only
    (backs `GET /tasks?orderRef=`, ADR-0013)
14. ArriveAtRebin(orderRef, lineId, ...) -> records a line arrival on
    OrderConsolidation (ItemArrivedAtRebin); once complete, creates the
    order's PACK task via CreateTask and raises OrderConsolidated (ADR-0016)
15. GetPackage(packageId) -> one Package, read-only; ErrPackageNotFound when
    absent (backs `GET /packages/{id}`, ADR-0033 — how a caller learns the
    SLAM outcome after `POST /packages/{id}/slam`'s 204)
16. GetPackagesByOrderRef(orderRef) -> every Package for one order ref,
    read-only (backs `GET /packages?orderRef=`, ADR-0033)

`internal/application/usecases/` holds exactly these 17 structs (counting
CheckInStation and CheckOutStation separately).
