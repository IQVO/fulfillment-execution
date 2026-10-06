---
id: ubiquitous-language
title: Ubiquitous language
sidebar_label: Ubiquitous language
sidebar_position: 4
description: The exact vocabulary of the Fulfillment Execution bounded context — every term mapped to the code identifier that implements it, with the places where the code name differs flagged — plus the cross-context terms that look shared but are not.
---

# Ubiquitous language

These are the exact names this bounded context uses. They appear in the
domain code, the REST API, the event catalogue and this documentation. Every
term below maps to a real identifier; where the code (or the wire) uses a
different name than the language, the **Code name differs?** column says so
and why. If a term here does not match an identifier in `internal/domain/` or
`internal/application/`, one of the two is wrong.

This page is the glossary artifact of the
[ddd-crew pack](../ddd/ddd-artifacts.md).

## Core terms

| Term | Definition | Code identifier | Code name differs? |
| --- | --- | --- | --- |
| **Task** | A unit of physical work. Has a type, a CPT, an order reference, a set of required capabilities, and two packing hints (Fragile, Gift wrap). At most one active claim, ever. | `task.Task` (`internal/domain/task/task.go`) | No |
| **Task type / Process path** | Pick / Pack / Rebin / SLAM as **named task types (queues)**, not as steps in a workflow. See [Process paths](./process-paths.md). | `task.Type` = `task.Pick` `"PICK"`, `task.Pack` `"PACK"`, `task.Slam` `"SLAM"`, `task.Rebin` `"REBIN"` | **Yes** — called `taskType` in REST, `task_type` on events, `processPath` in MCP tool arguments (which accept only `PICK`, `PACK`, `SLAM` — not `REBIN`), and `PathDefinition.Id` in the catalogue |
| **Pending / Claimed / Completed** | The three task states. `Pending` is in the pool; `Claimed` is leased to exactly one station; `Completed` is terminal. | `task.Pending`, `task.Claimed`, `task.Completed` (`"PENDING"`, `"CLAIMED"`, `"COMPLETED"`) | No |
| **claimNext(stationId, capabilities)** | **PULL dispatch.** Returns the highest-priority (earliest CPT) pending task the station is certified and equipped for. The system never names a station in advance — there is deliberately no `assign(task, station)`. | `usecases.ClaimNext.Execute(ctx, stationId, taskType)`; `POST /stations/{stationId}/claim-next` | **Yes** — the code takes a `taskType`, not capabilities; capabilities are read from the persisted `Station` so a client cannot assert ones it lacks |
| **Claim** | The act of a station taking a task, enforcing at-most-once and capability match. | `task.Task.Claim`, persisted by `TaskRepo.SaveClaim` (compare-and-set) | No |
| **At-most-once** | The claim guarantee: two stations can never hold an active claim on the same task at the same time. | `task.ErrAlreadyClaimed`; `TaskRepo.SaveClaim` CAS ([ADR-0034](../adr/0034-concurrency-control-for-consolidation-and-claim.md)) | No |
| **Lease** | A time-boxed claim. If not renewed or completed before expiry, the task returns to the pool. Renewal is allowed and expected for long work. | `task.Lease{StationId, Expiry}`; `usecases.DefaultLeaseDuration` = 5 min | No |
| **Lease renewal** | Extending an active lease; only the owner may renew. | `task.Task.RenewLease`, `usecases.RenewLease`; `POST /tasks/{id}/renew-lease` | No |
| **Lease expiry** | The `Claimed → Pending` edge. Evaluated lazily inside every claim/renew/complete, and eagerly by the sweep. | `task.Task.ExpireLeaseIfDue`, `usecases.ExpireLeases`, event `LeaseExpired`; `POST /tasks/expire-leases` | No |
| **Completion** | Finishing a claimed task; only the owner, only while the lease is active, never twice. | `task.Task.Complete`, `usecases.CompleteTask`, event `TaskCompleted` | No |
| **Station** | A work position with a capability set. One occupant at a time. | `station.Station` (`internal/domain/station/station.go`) | No |
| **Occupant / check-in / check-out** | Whoever (associate or robot) is at a station right now. Operational state only — no event. Read at publish time to stamp `associate_id` on `TaskCompleted`. | `station.OccupantId`, `Station.CheckIn` / `CheckOut`, `usecases.CheckInStation` / `CheckOutStation` | **Yes** — `occupantId` in REST, `associate_id` on the `TaskCompleted` wire payload |
| **Station location code** | Optional facility-layout `LocationCode` where a station physically sits; checked against facility-layout's WorkCenter role when `LOCATION_ROLE_MODE=http`. | `Station.LocationCode()` / `SetLocationCode`, `ports.LocationRoleLookup` ([ADR-0024](../adr/0024-station-location-code-and-workcenter-role-check.md)) | No |
| **Capability** | An open string naming a certification or piece of equipment a station must have to accept a task. Known values: `pick`, `pack`, `rebin`, `slam`, `hazmat`. Compared as set containment. | `shared.Capability`, `shared.CapabilitySet.HasAll` | No |
| **Hazmat (capability)** | A capability like any other: a task requiring it can only be claimed by a station registered with it. No dedicated code path. | the string `"hazmat"` in `requiredCapabilities` ([ADR-0009](../adr/0009-fragile-and-hazmat-handling-flags.md)) | No |
| **Fragile** | A `Task`-level packing hint, stamped by `wes-work-planning` at release time from `inventory-storage`'s product classification. Does not gate claiming. | `task.Task.Fragile()`; wire `data.fragile` | No |
| **Gift wrap** | A `Task`-level packing hint from a caller-stated `WorkReleased.data.gift_wrap` request (not a product classification). Does not gate claiming; no HTTP ingestion path. | `task.Task.GiftWrap()` ([ADR-0011](../adr/0011-gift-wrap-handling-flag.md)) | **Yes** — `gift_wrap` on the inbound event, `giftWrap` on the REST task response |
| **Package** | Pack output: an order's contents becoming a sealed carton. | `pack.Package` in `internal/domain/package` | **Yes** — Go package name is `pack` because `package` is a keyword |
| **Scan** | Recording an item into an open package, checking DOT segregation against every already-scanned item. | `Package.ScanItem`, `Package.ScanItemWithClass` | No |
| **Seal** | Closing a package; impossible without scanned contents. | `Package.Seal`, `usecases.SealPackage`, event `PackageSealed`; `POST /tasks/{id}/seal-package` | No |
| **FragileHandling / GiftWrapRequested** | Package flags derived at construction from the owning PACK task's Fragile / GiftWrap. | `Package.FragileHandling()`, `Package.GiftWrapRequested()` | No |
| **DOT hazard class** | An integer 1-9 looked up live per scanned SKU at seal time; 0 means "no hazard class" (fail-open). | `ports.ClassificationInfo.DOTHazardClass`, `Package.ScannedHazardClasses()` | No |
| **Segregation** | The same-package rule that two scanned items' DOT hazard classes must be compatible per a class-level 49 CFR §177.848-derived matrix. | `pack.IsSegregationIncompatible`, `pack.ErrPackageSegregationViolation` ([ADR-0010](../adr/0010-package-segregation-and-sort-lane.md)) | No |
| **SortLane** | The derived sortation routing decision: `HAZMAT_LANE` beats `FRAGILE_NO_TILT` beats `STANDARD`. A decision only — no WCS integration. | `Package.SortLane()`, `pack.SortLaneHazmat` / `SortLaneFragileNoTilt` / `SortLaneStandard` | No |
| **SLAM weigh-check** | Scan, Label, Apply, Manifest. The actual weight must be within tolerance of the expected weight, otherwise the package is diverted instead of labelled. | `Package.Weigh(expected, actual)`, `pack.WeightTolerance = 0.05`, `usecases.RunSlam`; `POST /packages/{id}/slam` | **Yes** — "weigh-check" is `Weigh`; the analytics payload names the weights `expected_g` / `actual_g` (grams) while the domain is unit-agnostic and the API examples use kilograms |
| **Labeled / Diverted** | The two SLAM outcomes. Diverted is a domain outcome, not an error. | `pack.Labeled` `"LABELED"`, `pack.Diverted` `"DIVERTED"`; events `LabelApplied`, `WeightDiscrepancyDetected`, `PackageDiverted` | **Yes** — the state is `Labeled`, the event is `LabelApplied` |
| **Manifested** | A package that passed SLAM and is promised to a carrier; feeds order-management's promise loop. | event `PackageManifested` ([ADR-0025](../adr/0025-cpt-missed-sweep-and-package-manifested.md)) | **Yes** — no `Manifested` state exists; a manifested package is in state `LABELED` |
| **CPT** | **Critical Pull Time** — the deadline by which a task must be completed for its order to ship on schedule. Priority derives entirely from CPT. | `shared.CPT` (`internal/domain/shared/cpt.go`) | **Yes** — `apis/openapi.yaml` expands it as "Committed Processing Time" in three descriptions; same value, prose-only discrepancy |
| **CPT missed** | A task still open (Pending or Claimed) at or past its CPT. Reported, not enforced; re-reported on every sweep pass. | `task.Task.IsCPTMissed(now)`, `usecases.SweepCPTMisses`, event `TaskCPTMissed`; `POST /tasks/sweep-cpt-misses` | No |
| **OrderRef** | The reference back to the released work this task or package fulfils. | `shared.OrderRef` | **Yes** — populated from `WorkReleased.data.work_unit_id`, so it is wes-work-planning's **WorkUnit id** (`<orderId>-line-<lineNo>`), not an order-management order id; it is `work_unit_id` on `TaskCompleted`, `order_ref` on `TaskCPTMissed` / `PackageManifested`, `orderRef` in REST |
| **Queue depth** | How many `Pending` tasks of a given type sit in the pool. A projection computed on demand. | `usecases.GetQueueDepth`, `TaskRepo.CountByTypeAndStatus`; `GET /queues/{taskType}/depth`; MCP `get_queue_status` | No |
| **Installed capacity** | How many registered stations hold a capability, regardless of occupancy. A projection over the Station registry. | `usecases.GetInstalledCapacity`, `StationRepo.CountByCapability`; `GET /capacity/{capability}` ([ADR-0018](../adr/0018-installed-capacity-read-endpoint.md)) | **Yes** — response field is `installed` |
| **Rebin / OrderConsolidation** | The fan-in of an order's required lines at Rebin; once every line has arrived the order's PACK task is created exactly once. | `consolidation.OrderConsolidation`, `usecases.ArriveAtRebin`; `POST /rebin/arrivals` ([ADR-0016](../adr/0016-rebin-and-order-consolidation.md)) | **Yes** — the arrival command is `ArriveAtRebin` / `RecordArrival`, the event `ItemArrivedAtRebin` |
| **Required line / arrived line** | The line ids an order needs at Rebin, fixed by its first arrival; and the ones that have arrived. | `OrderConsolidation.RequiredLineIds()` / `ArrivedLineIds()`; REST `requiredLineIds`, `lineId` | No |
| **Process-path catalogue** | The declared set of process paths (id, match prefix, required capabilities) a `path_id` resolves against — configuration, not code. | `pathcatalog.Catalogue`, `pathcatalog.PathDefinition`, `ports.PathCatalogue` ([ADR-0017](../adr/0017-process-path-catalogue-as-configuration.md)) | No |
| **Package read model** | A side-effect-free read of a package's current state — how a caller learns the SLAM outcome after the outcome-agnostic `204`. | `usecases.GetPackage`, `usecases.GetPackagesByOrderRef` ([ADR-0033](../adr/0033-package-read-model.md)) | No |

Source: `internal/domain/**`, `internal/application/usecases/*.go`,
`internal/adapters/inbound/http/dto.go`,
`internal/adapters/outbound/kafka/*.go`, `internal/adapters/inbound/mcp/tools.go`.

## Terms borrowed from the wider reference model

These come from the fulfilment reference model and appear in this context's
prose, though not all are modelled here as types:

| Term | Meaning | Modelled here? |
| --- | --- | --- |
| **Tote** | The container that carries items from pick to pack, with a physical *and* virtual map of contents. | No — implicit in the Pick path narrative. |
| **Pod** | A mobile storage tower of coded bins; robots carry pods to stations. | No — belongs to WCS / `inventory-storage`. |
| **Bin** | A coded slot within a pod or shelf; the unit of location. | No — `facility-layout` and `inventory-storage` own location. |
| **Waveless release** | Continuous release of work rather than batching into waves. | No — that is `wes-work-planning`'s decision; this context only receives what was released. |
| **Item picked** | The item-level retrieval fact inside a Pick task. | Partly — event `ItemPicked` is defined but no use case raises it. |

## Terms that look shared across contexts but are not

The reference model warns that "same word, different model is allowed — and
expected," and that forcing a shared type across the boundary is the classic
DDD trap.

| Word | Here it means | Elsewhere it means |
| --- | --- | --- |
| **Task** | An execution-level unit bound to a claiming station at a specific moment; disposable once completed. | In a WMS, a *business-level demand signal* tied to an order's fulfilment lifecycle — it persists, has an SLA, has priority. Deliberately not a shared class. |
| **ShiftPlan** | Not modelled. | `workforce-management` owns a committed headcount split across paths; `wes-work-planning` has its *own* different `ShiftPlan`. |
| **WorkUnit** | Arrives as `orderRef` — an opaque correlation key, nothing more. | In `wes-work-planning` it is a first-class aggregate with release state. |
| **Station** | A work position with capabilities, claimable-from. | In `workforce-management` the equivalent concern stops at the **path** level — it never links an associate to a station or a task. |
| **Location** | Only an opaque optional `locationCode` on `Station`, role-checked at registration. A `Task` carries no location. | `facility-layout` owns the location hierarchy and roles; `inventory-storage` owns what is *in* a bin. |
| **Order** | Not modelled; `orderRef` is a WorkUnit id. | `order-management` owns the order, its promise and re-promise. |

Every one of these is translated at the boundary rather than shared. The
`WorkReleased` consumer is the concrete example: it decodes the CloudEvent,
reads `path_id`, `work_unit_id`, `cpt`, `fragile` and `gift_wrap`, resolves
the path through the catalogue, and constructs *this* context's types. No
upstream struct crosses the line.
