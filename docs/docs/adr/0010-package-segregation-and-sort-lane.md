---
id: 0010-package-segregation-and-sort-lane
title: 10. Live per-item DOT hazard classification, same-package segregation, and SortLane
sidebar_label: 10. Segregation & SortLane
sidebar_position: 10
description: SealPackage performs a live, synchronous per-scanned-SKU classification lookup via a new permissive-by-default outbound port, enforces same-package DOT hazard segregation, and Package derives a WES-tier SortLane routing decision (HAZMAT_LANE > FRAGILE_NO_TILT > STANDARD).
---

# 10. Live per-item DOT hazard classification, same-package segregation, and `SortLane`

## Status

**Accepted.** The live, synchronous lookup to inventory-storage (the
`productclassification` HTTP client, `PRODUCT_CLASSIFICATION_MODE=http`) is
**superseded by [ADR-0039](./0039-product-classification-local-copy.md)**:
the same port now reads a local copy of product-master's ProductClassified
events. The segregation matrix, `SortLane` and the fail-open semantics below
still hold.

## Context

`inventory-storage` now records an optional DOT hazard class (1-9) on a
SKU's `ProductClassification`, alongside the existing `Hazmat` handling tag
introduced for ADR-0009. That upstream addition creates two new concerns
this context did not previously need to model:

- **Same-package segregation.** Two hazmat-classified items with
  incompatible DOT hazard classes must not be sealed into the same
  physical carton — this is a real regulatory/safety constraint (49 CFR
  §177.848), not merely a packing preference like the existing Fragile
  flag.
- **Sortation routing.** A sealed package containing hazmat needs to route
  to a dedicated lane, distinct from — and taking priority over — the
  existing fragile "no tilt" routing hint from ADR-0009.

The obvious first instinct, matching ADR-0009's precedent exactly, is to
have `wes-work-planning` stamp a classification onto `Task` at release
time, the same way it stamps `fragile`. That precedent does not fit here,
and the mismatch was found before implementation started:

- **ADR-0009's `Fragile` flag is a single boolean, known and fixed at
  release time** — the whole Pack task is fragile or it is not, and that
  fact is knowable when `wes-work-planning` releases the task.
- **This concern is per-SKU, and a Pack task's actual contents are not
  known at release time.** A single Pack task's `seal-package` call can
  scan SKUs from multiple pick lines or even multiple orders that were
  never enumerated when the task was released — pack contents are
  discovered live at the scan station, one SKU at a time, as the associate
  physically scans each item into the carton. There is no single
  per-task boolean (or even a small fixed list) that `wes-work-planning`
  could stamp onto `Task` at release time that would capture "the DOT
  hazard class of whatever SKU gets scanned third, which nobody knows
  yet."
- **Stamping a per-SKU classification MAP onto `Task` at release time was
  considered and rejected** for the same reason: the set of SKUs that will
  actually be scanned is not fully known at release time either (an
  associate can scan more or different items than any pick-line manifest
  implies, subject to whatever validation exists elsewhere in the pack
  flow), so a pre-computed map keyed by "SKUs we expect to see" is either
  incomplete by construction or requires `wes-work-planning` to eagerly
  resolve classification for SKUs that may never actually appear in this
  particular carton.

The forces, several already established by ADR-0009 and this repository's
existing conventions:

- **Contexts must not share aggregates or call each other synchronously
  for domain facts** is ADR-0009's Context section's framing for *release-time*
  facts threaded through Kafka. It does not forbid a synchronous read at a
  different point in the lifecycle when the fact genuinely cannot be known
  earlier — `inventory-storage` itself already makes exactly this kind of
  live synchronous cross-context call, from `StowStock` to
  `facility-layout`'s location-classification endpoint, via a
  permissive-by-default outbound port
  (`internal/adapters/outbound/facilitylayout`, selected by
  `LOCATION_LOOKUP_MODE=http|permissive`). That pattern exists because
  `inventory-storage`'s placement check has the identical shape of problem
  this context now has: the fact needed (a bin's zone attributes) is not
  knowable at any earlier point the two aggregates could exchange it via
  an event.
- **`SealPackage` is the one place in this service that sees each scanned
  SKU individually**, at the exact moment it is added to the package —
  the natural (and only) point to run a per-item check.
- **Existing behaviour and tests must be unaffected unless a new feature
  is explicitly opted into.** Every prior cross-cutting addition in this
  repository (Kafka publishing via `EVENT_PUBLISHER`, and
  `inventory-storage`'s own `LOCATION_LOOKUP_MODE`) follows a
  permissive-by-default, env-var-selected adapter pattern for exactly this
  reason.
- **`wes-work-planning` needs zero changes for this work.** Because the
  fact this feature needs is discovered at scan time, not release time,
  nothing about the `WorkReleased` envelope, `CreateTask`, or `Task`
  itself needs to change — a clean signal that the release-time-stamping
  pattern genuinely does not apply here, rather than merely being
  inconvenient to extend.

## Decision

**`SealPackage.Execute` performs a live, synchronous classification lookup
per scanned SKU, through a new outbound port in this service
(`ports.ProductClassificationLookup`), mirroring `inventory-storage`'s
`facilitylayout` adapter pattern exactly: permissive-by-default (a no-op
lookup returning "unclassified" for every SKU), selected via
`PRODUCT_CLASSIFICATION_MODE=http|permissive` (default `permissive`).
`Package` enforces same-package DOT hazard segregation via a local,
documented duplication of the class-level segregation matrix, and derives
a new `SortLane()` routing decision with hazmat taking priority over
fragile. This is a WES-tier decision only — no WCS device/conveyor
integration exists or is planned in this repository.**

### The new port and adapter

```go
// internal/application/ports/ports.go
type ClassificationInfo struct {
    Hazmat         bool
    DOTHazardClass int
    Fragile        bool
    Known          bool
}

type ProductClassificationLookup interface {
    GetClassification(ctx context.Context, sku string) (ClassificationInfo, error)
}
```

`internal/adapters/outbound/productclassification/` provides two
implementations, structurally identical to `inventory-storage`'s
`facilitylayout` package:

- `PermissiveLookup` — the default. Never contacts `inventory-storage`,
  always returns `Known: false`.
- `Client` — a plain `net/http` client calling `inventory-storage`'s
  existing `GET /products/{sku}/classification` endpoint (which now also
  returns an optional `dotHazardClass` field, added there in a parallel
  change). A 404 is `Known: false` (fail-open: the SKU is not classified
  upstream). A transport error or unexpected status is returned as an
  error to the caller — see "Fail-open per item" below for how
  `SealPackage` handles that.

Selected in `cmd/execution/main.go` exactly like
`inventory-storage`'s `buildLocationLookup`:

```go
classificationLookup := buildClassificationLookup(
    getenv("PRODUCT_CLASSIFICATION_MODE", "permissive"),
    os.Getenv("INVENTORY_STORAGE_BASE_URL"),
    logger,
)
```

### `SealPackage` — a lookup per scanned SKU, not a stamped map

```go
p := pack.New(uc.NewId(), t.OrderRef(), t.Fragile())
for _, sku := range contents {
    hazardClass := uc.lookupHazardClass(ctx, sku)   // 0 if unclassified/unknown/lookup-error
    if err := p.ScanItemWithClass(sku, hazardClass); err != nil {
        return nil, err
    }
}
```

`ClassificationLookup` is a nil-safe field on `SealPackage`, exactly like
`inventory-storage`'s `StowStock.LocationLookup`: a `SealPackage` built
without it (every pre-existing test in this package) behaves exactly as
before this feature — `lookupHazardClass` returns `0` unconditionally, and
`ScanItemWithClass(sku, 0)` is equivalent to the old plain `ScanItem(sku)`.

### `Package` — segregation matrix and `ScanItemWithClass`

`internal/domain/package/segregation.go` carries a **local, documented
duplication** of the same class-level DOT segregation matrix
`inventory-storage` derives independently for its own SKU-level
segregation concern, from **49 CFR §177.848** collapsed by four
simplification rules (collapse divisions to parent class conservatively;
collapse the regulation's "X" and "O" markers both to incompatible, since
same-package storage has no equivalent of a transport vehicle's
intervening-bulkhead separation method; class 1 is maximally restrictive;
class 9 is broadly compatible except with class 1 — see the file's own doc
comment for the full derivation). This is the **same, already-accepted**
pattern as `TemperatureClass` being duplicated between `facility-layout`
and `inventory-storage`: bounded contexts do not share Go types or tables
across repository boundaries, so each side derives, names, and tests the
concept independently, translating at the integration edge instead.

`Package` gains:

```go
type Package struct {
    // ...existing fields...
    scannedHazardClasses []int
}

func (p *Package) ScanItemWithClass(sku string, hazardClass int) error
func (p *Package) ScannedHazardClasses() []int
func (p *Package) SortLane() string
```

`ScanItemWithClass` checks `hazardClass` (0 meaning "no hazard class")
against every already-scanned item's recorded hazard class via
`pack.IsSegregationIncompatible`, rejecting with the new
`pack.ErrPackageSegregationViolation` on the first incompatible pair,
**before** appending anything — the incompatible item is never recorded
into either `scannedContents` or `scannedHazardClasses`. `ScanItem(sku)`
is kept as a thin wrapper (`ScanItemWithClass(sku, 0)`) so it, and every
existing caller/test using it, is completely unaffected.

`hazardClass == 0` never triggers or blocks segregation and is never
itself recorded — fail-open for unclassified (or classified-but-not-Hazmat)
items, matching every other classification consumer in this whole system
(`inventory-storage`'s `SlotAttributes.Known` convention, and this
context's own pre-existing `Fragile`/hazmat-capability handling).

### `SortLane()` — hazmat beats fragile, computed lazily

```go
func (p *Package) SortLane() string {
    if len(p.scannedHazardClasses) > 0 {
        return SortLaneHazmat        // "HAZMAT_LANE"
    }
    if p.fragileHandling {
        return SortLaneFragileNoTilt // "FRAGILE_NO_TILT"
    }
    return SortLaneStandard          // "STANDARD"
}
```

Priority order, explicit and tested as a truth table: `HAZMAT_LANE` if any
scanned item carried a DOT hazard class, else `FRAGILE_NO_TILT` if
`FragileHandling()` (ADR-0009's existing flag) is true, else `STANDARD`.
Hazmat wins deliberately — a package containing regulated hazardous
material has a real physical/regulatory routing requirement that
supersedes a packing-care hint.

`SortLane()` is a **method, computed on every call**, not a stored field —
the same design-decision-worth-recording ADR-0009 made for
`FragileHandling` (stored, in that case, because it is fixed at
construction) but the opposite answer here, because `SortLane` is a pure
function of two already-present derived facts
(`scannedHazardClasses`/`fragileHandling`) with no independent state of
its own to lose or need to migrate. A stored field would need a migration
step and a place it could silently drift from its inputs; a method cannot
drift.

### Fail-open per item, not fail-closed like `StowStock`

`SealPackage`'s `lookupHazardClass` catches a transport/lookup error for a
single SKU and treats that SKU as unclassified (`hazardClass = 0`) —
**the whole seal proceeds**, rather than aborting on one lookup blip. This
is a deliberate asymmetry with `inventory-storage`'s `StowStock`, which
fails *closed* (`ErrLocationClassificationUnavailable`) when a
*classified* SKU's placement lookup genuinely errors:

- `StowStock`'s check is a **harder, at-rest safety gate** — stock is
  being placed into a bin that will sit there until picked; getting the
  placement rule wrong has a longer blast radius and a slower detection
  path.
- `SealPackage`'s check is a **live, active-station, soft-dependency
  hint** — an associate is standing at the scan station right now. Halting
  an entire pack station because `inventory-storage` is briefly slow or
  unreachable for one SKU's classification is a worse outcome than
  proceeding with that one item treated as unclassified (still fail-open,
  still consistent with "an item with no hazard class never blocks
  anything"). Any *other* scanned SKU in the same call whose lookup
  succeeds is still checked normally.

### Domain events stay thin — `SortLane` is queryable, not pushed

Following the exact same judgement call ADR-0009 already made for
`fragile`/`fragileHandling`: `PackageSealed` (and `LabelApplied`) still
carry only their aggregate id — see `internal/domain/shared/events.go`.
`SortLane` is not pushed onto either event's payload. It is fully visible
on the `POST /tasks/{id}/seal-package` response (`sortLane` field on
`PackageResponse`), which is sufficient for anything needing the value
today. If a future external consumer needs `sortLane` on the wire, the
existing repo-lookup-enrichment pattern (`TaskCompleted`'s
`work_unit_id`) is the template — enrich in the adapter, not the domain
event.

### HTTP surface

- `PackageResponse` gains `sortLane` (`HAZMAT_LANE` | `FRAGILE_NO_TILT` |
  `STANDARD`).
- `pack.ErrPackageSegregationViolation` maps to `409 Conflict` — a
  conflict between the incoming item and package state already
  accumulated, the same category as `ErrAlreadySealed`, not a
  request-validation failure.

## Consequences

### Easier

- **Real pack-time hazmat safety without a hard dependency on
  `inventory-storage`'s availability.** The permissive-by-default port
  means this feature ships and can be enabled per-deployment
  independently, exactly like `inventory-storage`'s own
  `LOCATION_LOOKUP_MODE` rollout.
- **`wes-work-planning` needed zero changes.** Because the classification
  lookup happens at scan time inside this service, no upstream producer,
  envelope, or `CreateTask` signature needed to change — a clean
  confirmation that this concern genuinely belongs at seal time, not
  release time.
- **No new aggregate, no new consistency boundary.** `Package` gained one
  field, one method, and one error — the same shape of change ADR-0009
  made to add `fragileHandling`.
- **`SortLane` cannot drift from its inputs.** Computed lazily from
  `scannedHazardClasses`/`fragileHandling`, so there is no third piece of
  state that could disagree with the two it is derived from.
- **The segregation check fails closed exactly where it matters (within
  one package) and fails open exactly where it should (unclassified items,
  a soft lookup dependency)** — the same fail-open/fail-closed discipline
  already established across this whole platform, applied consistently
  rather than reinvented.

### Harder

- **A second, independently-derived copy of the DOT segregation matrix now
  exists in this codebase**, alongside `inventory-storage`'s. This is a
  deliberate, documented duplication (see `segregation.go`'s own doc
  comment), consistent with `TemperatureClass` already being duplicated
  between `facility-layout` and `inventory-storage` — but it is still a
  second place a future correction to the regulation-derived table has to
  land, and the two copies are not mechanically kept in sync. **This
  repository's matrix was derived independently, not copied from
  `inventory-storage`'s parallel branch** (which had not merged at the
  time this was written) — the two are expected to be identical (same
  regulation, same four rules) but have not been diffed against each
  other. Reconciling them — diffing the two matrices once
  `inventory-storage`'s `feature/dot-hazard-class-segregation` branch
  merges, and fixing either side if they disagree — is a real follow-up,
  not done in this change.
- **A `SealPackage` call now makes up to N synchronous HTTP calls** (one
  per scanned SKU) when `PRODUCT_CLASSIFICATION_MODE=http` is enabled,
  where N is the size of `contents`. There is no batching or caching
  across SKUs in a single call. For a normal-sized carton this is a small,
  bounded fan-out, but it is a real new latency and failure-mode surface
  on the hot seal-package path that did not exist before.
- **The fail-open-per-item behaviour on lookup error is a real, accepted
  gap, not a hidden one.** A transient `inventory-storage` outage means
  hazmat items scanned during that window are silently treated as
  unclassified for segregation purposes — the safety check that matters
  most is exactly the one most likely to be skipped during a dependency
  outage. This is the explicit tradeoff documented above (a soft
  dependency should not halt an active pack station), but it is a real
  cost, not a free lunch.
- **WES-tier decision only — no WCS integration exists.** `SortLane()` is
  a routing *decision*, visible on the HTTP response and nothing else.
  There is no conveyor, no lane-controller, no physical sortation system
  in this workspace that consumes it — consistent with the same honest gap
  already accepted for `Package.FragileHandling()` in ADR-0009 ("no
  sortation ACL exists yet") and for `ItemPicked` in the domain-events
  catalogue. A real WCS system would consume `SortLane` as its own inbound
  fact (an integration event or a polling read), which this change does
  not build and does not attempt to build.
