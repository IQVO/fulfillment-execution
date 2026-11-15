---
id: 0065-the-orders-site-travels-from-work-released-to-package-manifested
slug: /adr/0065-the-orders-site-travels-from-work-released-to-package-manifested
title: 65. The order's site travels from WorkReleased to PackageManifested
sidebar_label: "65. Order site on PackageManifested"
description: "ADR 0065 — an order-work task keeps WorkReleased.site_id as its order site (tasks.order_site_id, migration 0028), the rebin PACK task inherits it from the order's PICK tasks, and PackageManifested publishes it as site_id and falls back to the PACK task's CPT when the order has no SLAM task, so outbound-shipping can plan a load."
---

# 0065. The order's site travels from WorkReleased to PackageManifested

## Status

Proposed (2026-11-14). Sources the `site_id` that
[ADR 0061](./0061-package-manifested-carries-ship-correlation.md) declared and
left always omitted, and changes ADR 0061's "No SLAM task, no `cpt`" rule
(see Decision, point 4). Requested by outbound-shipping (its ADR 0001,
decision D2; its ADR 0003 records the omitted site as a model gap):
outbound-shipping ignores a `PackageManifested` with an empty `site_id` or an
empty `cpt`, so until this lands no customer order's package is ever placed
on a load.

## Context

`PackageManifested` carries two optional fields outbound-shipping needs to
plan a load: `site_id`, the site the package ships from, and `cpt`, the
cutoff instant. It converts the instant to the site's schedule timezone and
matches it to a cutoff `local_time` of that site's CPT schedule. Both were
omitted for every customer order:

- **`site_id` had no source here.** order-management publishes the order's
  fulfillment site on `OrderAllocated` (top-level `site_id`, omitted when the
  order named none) and wes-work-planning copies it onto the work unit and
  onto `WorkReleased` (its ADR 0048: top-level `site_id` for order units).
  This service decodes it (`WorkReleasedData.SiteId`) but used it only for
  transfer work, as the transfer fact's site. For order work the task kept
  `sourceOrderId` and the line number only, and the PACK task, created at
  rebin by `ArriveAtRebin`, never saw a release at all.
- **`cpt` came only from a SLAM task**, found by order reference. Nothing
  creates a SLAM task for a customer order (order-management selects only
  the customer-order entry paths, the PACK task comes from rebin, and
  `RunSlam` acts on the package), so the lookup never resolved. ADR 0061 said
  so: "No SLAM task, no `cpt`".

The PACK task's CPT is already the right instant: it is the order's promise
date, which order-management builds from the same schedule outbound-shipping
reads. The rebin arrival posts it (`packCPT`), so the PACK task carries it
from the moment it exists.

Alternatives considered:

- **A `site_id` field on `POST /rebin/arrivals`.** Rejected: the site is
  already in this service, on the order's PICK tasks (released with the same
  order), so asking the rebin caller to repeat it adds a REST contract change,
  an OpenAPI regeneration and a way for the two to disagree. The FE-only
  route needs none of that.
- **Deriving the site from the pick plan's site** (`WorkReleased.data.pick.site_id`,
  ADR 0062). Rejected: it exists only on PICK releases that carry a plan, and
  it is the site a pick is *worked at*; the order's site is a different fact
  that happens to coincide today. Each keeps its own column.
- **Parsing a site out of a location code or an id.** Rejected, as in
  ADR 0061: nothing is ever inferred from string shapes.
- **Creating SLAM tasks for customer orders** so the existing `cpt` source
  resolves. Rejected: a much larger change to the task model for one instant
  the PACK task already holds.

## Decision

1. **The order's site is stored on the task.** `Task` gains an optional
   `orderSiteId` (`tasks.order_site_id`, nullable `TEXT`, migration 0028).
   Its value is validated like a facility-layout site code,
   `^[A-Z0-9]+$`, or empty. `ParseOrderSite` validates; `WithOrderSiteId`
   keeps a valid value and otherwise leaves the task without one, so the
   domain never holds a malformed code. It is a different field from the
   transfer block's `SiteId` and from the pick plan's `SiteId`. The in-memory
   repo stores copies of it like every other field.
2. **Order work keeps `WorkReleased.site_id`.** For a release that is not
   transfer work, the consumer maps `data.site_id` onto
   `WorkReleasedRequest.SiteId`, and `ApplyWorkReleased` creates the order
   task through `CreateTask.ExecuteReleaseWithPickAtSite`. Relocation,
   replenishment and case-break releases are not order work and keep no
   order site. An empty or malformed site is tolerated: the task is created
   without one and a WARN names the event (never an error, never a
   dead-letter), the same tolerance as an out-of-range `line_no`. Transfer
   behaviour is unchanged: its site stays inside the transfer block, the
   consumer passes nothing for it, and the use case would ignore it anyway.
3. **The rebin PACK task inherits the site from the order's PICK tasks.**
   `ArriveAtRebin` asks the new `TaskRepo.FindBySourceOrderId` for the
   tasks released for the posted order id and gives the PACK task the first
   non-empty order site of a PICK task, by task id. No PICK site means a
   PACK task without one, as before. Nothing is added to
   `POST /rebin/arrivals`. The lookup runs inside the arrival's unit of
   work and a failure fails the arrival (it is retried), because a PACK task
   created without its site would publish a manifest outbound-shipping
   ignores for good. Migration 0028 adds a partial index on
   `source_order_id` for this lookup (`order_ref` holds the per-line work unit
   id, so `idx_tasks_order_ref` does not serve it).
4. **`PackageManifested` publishes the site and the PACK task's CPT.** The
   existing best-effort correlation (fail-soft, by the manifest's order
   reference) now also resolves `site_id`: the first non-empty order site of
   the manifest's SLAM task, then of its PACK task. `cpt` stays the SLAM
   task's CPT when one exists (existing behaviour wins) and falls back to the
   PACK task's CPT otherwise, RFC 3339 in UTC; a zero CPT is not published.
   Among several PACK tasks (a retried leg) the one that was worked wins,
   then the most recently claimed, then the lowest id, the rule `pickSlamTask`
   already applies to SLAM. `site_id` and `cpt` stay `omitempty`: absent,
   never an empty string, and a payload for an order with no site is
   byte-identical to the pre-change one apart from the new `cpt` fallback.
   The AsyncAPI description of both fields and an example are updated; the
   dataschema stays `:v1` (additive).

## Consequences

- outbound-shipping can place every customer order that named a site on a
  load: its two required fields are now present, no change on its side.
  Its behaviour does change, by design: manifests it used to ignore now plan
  loads (and publish `LoadPlanned`) when the site's schedule copy is known and
  the PACK task's CPT matches a cutoff of it.
- The `cpt` of a customer order's manifest is now the order's promise date.
  A consumer that treated an absent `cpt` as "this was not a SLAM manifest"
  would see a change; none in the fleet does (order-management and
  returns-processing decode `package_id` and `order_ref` only,
  labor-performance and warehouse-planning ignore the type, none uses
  `DisallowUnknownFields` on this path).
- A task released before this change has no order site, and a PACK task
  created for an order whose PICK tasks predate it has none either; those
  manifests omit `site_id` as before. There is no backfill: the field is
  informational on the task and an order in flight at deploy time simply
  keeps the old behaviour.
- A site the producer sends that is not a valid code is dropped at the
  boundary. outbound-shipping would not have matched it anyway, and the WARN
  names the event so the producer's bad value is findable.
- The PACK task reads the site of the PICK tasks that were released with the
  order. If a PICK was released with a different site than another PICK of
  the same order, the lowest task id with a site wins; an order is released
  to one site, so this is a tie-break, not a business rule.
- Every `ports.TaskRepo` implementation gains `FindBySourceOrderId`
  (Postgres, in-memory, and the test fakes).
