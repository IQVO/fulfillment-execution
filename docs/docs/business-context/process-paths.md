---
id: process-paths
title: Process paths — Pick, Pack, Rebin, SLAM
sidebar_label: Process paths
sidebar_position: 2
description: What Pick, Pack, Rebin and SLAM actually are on a warehouse floor, and why they are modelled as named task types (queues) rather than as steps in a workflow.
---

# Process paths — Pick, Pack, Rebin, SLAM

A **process path** is one of Pick, Pack, Rebin, or SLAM. In this model they
are **named task types — that is, queues — not steps in a workflow**. That
distinction is small to write down and large in consequence, so it is worth
being precise about both what the paths physically are and why they are
modelled this way.

## What each path is, physically

### Pick

On order placement the system identifies the bin holding the item. Either a
robot brings the storage pod to a pick station, or the picker walks to the
bin. A shining light directs the associate to the exact slot; the item is
checked for damage and placed into a tote that carries both a physical and a
virtual map of its contents. When the tote is full — by weight or by
dimension — it goes onto a conveyor toward packing.

In this service: a `PICK` task, typically requiring the `pick` capability.
The domain event `ItemPicked` exists in the catalogue for the retrieval fact
itself.

:::note[Honest status]
`ItemPicked` is defined in `internal/domain/shared/events.go` and covered by
the event tests, but no use case raises it today — the current Pick path is
modelled at task granularity (claim → complete), not at item granularity. It
is documented here and in the [event catalogue](../ddd/domain-events.md)
because it is part of the intended model, not because it is currently emitted.
:::

### Pack

The tote's barcode is scanned. As each item is scanned the system suggests a
box or bag size from the product dimensions. The associate builds and tapes
the carton and affixes a barcode carrying everything needed to generate a
shipping label downstream.

In this service: a `PACK` task, whose completion path produces a `Package`
aggregate via `POST /tasks/{id}/seal-package`. The invariant that matters is
that **a carton cannot be sealed without scanned contents** — sealing an
unverified box is precisely how the wrong item ships.

For a multi-line order, this `PACK` task is not created directly by the
`WorkReleased` consumer — it is created by `ArriveAtRebin` once every
required line has converged at Rebin (see below). For a single-line order
there is nothing to converge, so its `PACK` task is created the moment its
one line arrives.

Note that **cartonization** (choosing the box size) is *not* implemented here.
The platform's strategic guidance is to model cartonization as its own Generic
Subdomain that both WMS and WES call into, rather than duplicating box-selection
logic in two places.

### Rebin — order consolidation

A multi-line order picked across different zones or pods produces
independent pick confirmations at different times. Rebin is the queue and
physical holding point (a bin, a chute) where those independently-picked
lines wait until every line required for the order has arrived — only then
can Pack begin, because Pack needs the whole order's contents together, not
one line at a time.

In this service: a `REBIN` task follows the identical claim/lease/complete
rules as every other task type — nothing about `Task` itself knows this is
a consolidation point. The fan-in logic lives in a separate small aggregate,
`internal/domain/consolidation.OrderConsolidation`, which tracks which of an
order's required lines have arrived. `POST /rebin/arrivals` records one
line's arrival; once `OrderConsolidation.IsComplete()` becomes true, the
`ArriveAtRebin` use case creates the order's `PACK` task by calling the
existing `CreateTask` use case — the same reuse principle documented on
`CreateTask` itself. See [ADR-0016](../adr/0016-rebin-and-order-consolidation.md)
for the full reasoning, including why this stayed a small aggregate inside
this bounded context rather than becoming a new service.

A single-line order (order-management's `FulfillmentClass.SINGLE`, per that
repository's ADR-0008) has exactly one required line, so it "completes"
immediately on that line's arrival — no special-casing needed, since
completeness is just `len(arrived) == len(required)` for any line count.

### SLAM — Scan, Label, Apply, Manifest

Packages are scanned and weighed on a scale that detects expected-versus-actual
weight discrepancies; a robotic arm prints and applies the shipping label with
a blast of air.

In this service: `POST /packages/{id}/slam`. The **weigh-check** is the
invariant — if `|actual − expected|` exceeds tolerance the package is
**diverted** rather than labelled, because a weight mismatch means the carton's
contents do not match what the system believes is inside it. Shipping it would
mean shipping the wrong thing, at the wrong postage, with a label that says
otherwise. The tolerance is `pack.WeightTolerance = 0.05` in the same unit as
the supplied weights.

A diverted package raises **two** events, `WeightDiscrepancyDetected` and
`PackageDiverted` — the measurement fact and the routing fact are separate
concerns and downstream consumers care about different ones.

## Why paths are queues, not steps

The tempting model is a workflow: a unit of work flows Pick → Pack → SLAM, and
each task "advances" to the next stage.

This model rejects that, and models each path as **its own independent pool of
tasks**. The reasons are concrete:

**1. A station pulls from a queue; it does not wait for a workflow.**
`claimNext` asks "what is the most urgent `PACK` work I am equipped to do?" A
pack station is idle or it is not, and that has nothing to do with which
particular pick finished last. Queues make the question answerable in one
repository call ordered by CPT. A workflow model would make it a graph
traversal.

**2. The queue depth per path is the operationally interesting number.**
`GET /queues/PACK/depth` answers "is packing starved or drowning?" — which is
exactly the buffer telemetry `wes-work-planning` flow-balances on. A per-path
queue depth *is* the read model; in a workflow model you would have to derive
it by counting instances parked at a particular node.

**3. The paths genuinely do not have a single fixed order at the unit level.**
A multi-line order picked across three zones by three associates produces
three independent pick confirmations at different times. Packing cannot start
until all required lines have arrived. That fan-in — `OrderConvergence` in the
reference model — is a *synchronisation* concern. It is now handled by the
Rebin path and the `OrderConsolidation` aggregate described above (see
[ADR-0016](../adr/0016-rebin-and-order-consolidation.md)); baking a linear
Pick→Pack ordering directly into `Task` itself would still have been wrong —
`OrderConsolidation` exists precisely so the fan-in guarantee is explicit,
observable state rather than an assumption baked into task sequencing.

**4. Independence is what makes each path's dispatch policy separately
tunable.** A change to how Pack tasks are prioritised should not require
reasoning about Pick.

## How a released work unit becomes a path

The `WorkReleased` consumer derives the task type from
`WorkReleased.data.path_id` via a real lookup against the fleet's declared
process-path catalogue, held in memory as
`internal/domain/pathcatalog.Catalogue`. By default
(`PATH_CATALOGUE_SOURCE=file`) it is loaded once at startup from
`warehouse-infra`'s `config/process-paths/sortable-fc.yaml`; with
`PATH_CATALOGUE_SOURCE=kafka` it is rebuilt by replaying
`process-path-management`'s `ProcessPath*` events and kept current from
them. An unrecognized `path_id` is a
hard error — the message handling fails outright rather than silently
defaulting to any particular task type. See
[ADR-0017](../adr/0017-process-path-catalogue-as-configuration.md) for the
full reasoning, including why this replaced an earlier
path_id-prefix-guessing convention that had exactly this silent-default
bug.

Required capabilities are read directly from the catalogue entry for the
matched path — the same capability vocabulary `workforce-management` uses
when it plans headcount per path. That shared catalogue is a **published
language**, not a shared Go type: neither service imports the other's code,
both simply agree on the same schema.
