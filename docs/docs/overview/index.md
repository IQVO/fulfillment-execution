---
id: index
title: Fulfillment Execution
sidebar_label: What this service is
sidebar_position: 1
slug: /overview/
description: The Pick/Pack/SLAM task lifecycle — the WES-tier Core bounded context that turns released work into completed physical operations.
---

# Fulfillment Execution

:::warning[Study project]
This documentation site is an educational Domain-Driven Design exercise. It
follows real industry-standard patterns and terminology, but it is **not a
production system** and is **not affiliated with, endorsed by, or
representative of any real-world company**.
:::

**Fulfillment Execution turns released work into completed physical
operations.** It owns the *task lifecycle* for the process paths of the
outbound value stream — **Pick**, **Rebin** (order consolidation),
**Pack**, and **SLAM** (Scan, Label, Apply, Manifest) — and nothing else.

It sits between its neighbours in the `warehouse-systems` platform:

- **Upstream:** [`wes-work-planning`](https://github.com/IQVO/wes-work-planning),
  the conductor, decides *what* work to release and *when*. Every unit it
  releases arrives here as a `WorkReleased` integration event and becomes a
  `Task` in this service's pool.
- **Downstream (live):** `wes-work-planning` (completion feedback),
  `labor-performance` and `order-management` consume the `TaskCompleted`,
  `TaskCPTMissed` and `PackageManifested` events this service publishes on
  `warehouse.fulfillment.events`.
- **Downstream (strategic):** WCS / equipment (conveyors, print-and-apply
  labellers, check-weighers) would execute the physical steps. That edge
  is modelled as a deliberately empty port and is **not wired** — see
  [Context Map](../ecosystem/context-map.md).

## The one design rule that shapes everything

> **Pull, not push.** A station claims the next task —
> `claimNext(stationId, capabilities)`. The system selects work, not workers.
> There is deliberately no `assign(task, station)` operation anywhere in this
> codebase.

Every other decision here follows from that rule. Because the system never
names a station in advance, a claim has to be **at-most-once** (two stations
must never end up walking to the same tote), and because a claiming station is
a physical human or robot that can drop a scanner, walk away, or lose network,
an unconfirmed claim must **return to the pool rather than vanish**. That is
what the **lease** is for.

Read the reasoning in full on
[Why pull, not push](../business-context/why-pull-not-push.md), and the two
architecture decisions that encode it:
[ADR-0002](../adr/0002-pull-based-claimnext-dispatch.md) and
[ADR-0003](../adr/0003-lease-based-at-most-once-claiming.md).

## What it owns, and what it deliberately does not

| Owns | Does **not** own | Who does |
| --- | --- | --- |
| The `Task` pool for Pick / Rebin / Pack / SLAM | Deciding *what* work to release and at what rate | `wes-work-planning` |
| Pull dispatch (`claimNext`) and lease lifecycle | Which associate stands at which station this shift | `workforce-management` |
| The `Station` capability set used to filter claimable work | Stock truth, reservations, bin-accurate location | `inventory-storage` |
| The `Package` aggregate: seal + SLAM weigh-check | Whether a coded location exists and is legal | `facility-layout` |
| Queue depth as a read-model projection | Driving PLCs, motors and sensors | WCS (external, generic subdomain) |

The boundary with `workforce-management` is the sharpest one and is deliberate
on both sides: Workforce Management stops at the **process-path boundary** — it
plans headcount per path and never links an associate to an individual task.
Dispatch of individual tasks to a claiming station belongs here. The two
contexts change at completely different cadences (shifts versus seconds), and
keeping them apart is what lets dispatch policy evolve without touching
workforce planning.

## Where to go next

- **[Task lifecycle](./task-lifecycle.md)** — the state machine, with the
  lease-expiry edge drawn explicitly.
- **[Architecture](./architecture.md)** — hexagonal layering, the real package
  map, and the fitness tests that keep it honest.
- **[Running locally](./running-locally.md)** — in-memory with just a
  process-path catalogue file;
  Postgres and Kafka when you want the real thing.
- **[API Reference](../api-reference/index.md)** — every REST endpoint,
  generated from the real `apis/openapi.yaml`.
- **[Architecture Decision Records](../adr/index.md)** — why it is built this
  way, reconstructed from the decisions actually made in this repo.
- **[DDD artifacts (ddd-crew)](../ddd/ddd-artifacts.md)** — core domain
  chart, bounded context canvas, aggregate design canvas, domain message
  flow, EventStorming, and UML class / ER / sequence diagrams, all derived
  from the code.
