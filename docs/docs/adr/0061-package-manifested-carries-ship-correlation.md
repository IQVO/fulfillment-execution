# 0061. PackageManifested carries the correlation ship-confirm needs

## Status

Proposed (2026-11-05). Verified by the wave-1 correlation spike
(`outbound-shipping-2026-11-05/spike-correlation.md`, verdict GO); this
record lands the producer half.

## Context

`PackageManifested` carries `package_id` and `order_ref`, and `order_ref`
is semantically ambiguous: on the release path it is the per-line work
unit id (`<order>-line-<n>`), on the rebin path — the path every customer
order actually takes — it is the bare order id, but only by caller
convention, never enforced here. `network-fulfillment` ADR 0014 records
that no table stores the hop from a work unit to the order-management
`OrderId`, and its hard rule 4 forbids string-convention joins; its ADR
0026 still hard-codes `singlePackage = 1` as a placeholder. Until the
fleet can say which order a manifested package ships for, ship-confirm
stays a manual POST and an order can never reach a real `Shipped`.

The wave-1 spike traced every value at SLAM time (commit `7816d25`):

- The order id is reachable by two unwired routes: the rebin path's
  `Package.orderRef` already IS the order id (the caller of
  `POST /rebin/arrivals` posts it; the fleet e2e simulator splits the
  PICK task's work-unit ref and posts the bare order id,
  `e2e-tests cmd/warehouse-day/floor.go:296,393`), and the release path
  keeps it on `Task.sourceOrderId` (`WorkReleased.ref`, stamped by
  `ApplyWorkReleased` via `CreateTask.ExecuteRelease`). But
  `ArriveAtRebin` created its PACK task through plain
  `CreateTask.Execute` (`arrive_at_rebin.go:104`), so no rebin-created
  PACK task carried the stamp — the two paths disagreed.
- The CPT is reachable: the analytics on-time-to-CPT KPI already resolves
  a manifest's SLAM task and its CPT through
  `TaskRepo.FindByOrderRef` + `pickSlamTask`
  (`analytics_publisher.go:173-243`, ADR-0026).
- The weight is a `RunSlam` parameter (`run_slam.go:25`), never published.
- The site is NOT reachable: no order-work data structure in this service
  or in wes-work-planning carries a site (transfer-only
  `Task.TransferDetails.SiteId`; spike Q3).

Additive tolerance was verified consumer by consumer (spike Q7): the only
real decoder of `PackageManifested`, order-management's
`RepromiseOrder` consumer, decodes a fixed two-field struct
(`packageManifestedData`, `repromise_consumer.go:96-100`) through the
CloudEvents SDK's `e.DataAs` (`:368`) — `encoding/json` into a fixed
struct, and the repo has no `DisallowUnknownFields` on any Kafka consumer
path, so unknown keys are silently ignored.
warehouse-planning's consumer on the same topic acts only on
`TaskCompleted` and names `PackageManifested` among the types it ignores;
exception-management decodes `PackageDiverted`, not `PackageManifested`.
No consumer breaks on additive fields.

## Decision

`PackageManifested`'s payload (`PackageManifestedData`, dataschema stays
`…:PackageManifested:v1`) gains four optional, `omitempty` fields —
`site_id` (string), `source_order_id` (string), `cpt` (string, RFC3339),
`weight_g` (float64, grams) — omitted entirely when unknown, never empty
strings or zero values on the wire, so the pre-ADR-0061 payload bytes are
unchanged (the fleet's additive precedent: `WorkReleased.line_no`,
ADR-0036).

- `source_order_id` is the order-management `OrderId`. It resolves from
  the correlated tasks' `Task.SourceOrderId` — the SLAM leg first (the
  manifest's own leg), then the PACK task the package was sealed for —
  and when the order's tasks are known but none is stamped (a PACK task
  from before this change), the rebin caller convention applies: the
  package's `order_ref` is the bare order id and a trailing `-line-N`
  work-unit suffix is stripped defensively. It is never re-interpreted
  from `order_ref` on the wire: `order_ref` keeps its existing bytes and
  meaning.
- Prerequisite, same change: `ArriveAtRebin` creates the order's PACK
  task through `CreateTask.ExecuteRelease`, stamping the posted
  `orderRef` as the task's `sourceOrderId` — so the release path
  (`WorkReleased.ref`) and the rebin path agree on `Task.SourceOrderId`
  and the dominant path fills `source_order_id` from the stamp, not the
  fallback.
- `cpt` is the manifest's SLAM task's CPT, RFC3339 — exactly the SLAM-leg
  correlation the analytics on-time-to-CPT KPI already performs
  (`pickSlamTask`, ADR-0026). No SLAM task, no `cpt`.
- `weight_g` is the actual SLAM weigh-check measurement `RunSlam` already
  receives, in grams, the same unit discipline as
  `WeightDiscrepancyDetected.actual_g`.
- `site_id` has no source anywhere in the order-work chain (spike Q3:
  `order-site-v1` is an order-management projection that never reaches
  wes-work-planning, and `WorkReleased.site_id` is transfer-only). It is
  declared and omitted in this change; it starts filling once a later
  wave lands a site source in the chain (order-management publishing the
  order site through to the task layer — the pick-batching wave 1.5
  hand-off recorded by the orchestrator). Hand-off noted; no string
  parsing of codes ever feeds it.

The enrichment is best-effort and fail-soft, mirroring the analytics
publisher's convention: a missing repo, lookup error or unresolvable
correlation leaves the optional fields omitted and the exact old payload
ships, rather than failing the publish. The downstream design
(outbound-shipping OS-0003) tolerates a manifest without
`source_order_id`: the package can still be loaded and shipped, it is
left out of `ShipmentConfirmed.orders[]` and counted in analytics.

## Consequences

- Additive, so the existing consumers are unaffected by construction:
  order-management ignores the unknown keys (spike Q7), and
  warehouse-planning / exception-management do not decode this event.
- The correlation gap that forced the manual shipment confirmation
  endpoint (network-fulfillment ADR 0014) closes for new manifests:
  `ShipmentConfirmed.orders[].order_id` will join
  `NetworkOrder.localOrderId` on an explicit field.
- What becomes harder: the publisher now performs a task lookup on the
  manifest path (inside the outbox transaction, like `TaskCompleted`'s
  enrichment), and the rebin fallback's `-line-N` strip is a convention
  guard, not a validated invariant — a site posting a work-unit-shaped
  ref that is NOT `<order>-line-N` would publish a wrong
  `source_order_id`. That risk is bounded: after the prerequisite stamp
  the fallback only serves tasks created before this change, and
  order-management's own consumer today skips work-unit-shaped refs it
  cannot parse, so no new failure mode appears there.
- `site_id` ships as a declared-but-always-omitted field until a later
  wave sources it; consumers must not assume its presence yet (the
  AsyncAPI marks it optional).
- Legacy manifests (published before this change) never carry the
  correlation fields; downstream backfills, if any, are the consumer's
  concern.
