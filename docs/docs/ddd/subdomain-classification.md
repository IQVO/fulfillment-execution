---
id: subdomain-classification
title: Subdomain classification
sidebar_label: Subdomain classification
sidebar_position: 1
description: Why Fulfillment Execution is classified as a Core subdomain, with the justification taken from the platform's DDD reference documents.
---

# Subdomain classification

## Classification: **Core**

Fulfillment Execution is a **Core subdomain** of the `warehouse-systems`
platform, sitting in the **WES tier**.

## The justification, from the reference model

DDD splits a domain into Core, Supporting and Generic subdomains **by
competitive differentiation** — not by size, difficulty, or how interesting
the code is. The platform's reference documents give two independent lines of
reasoning that land on Core for this context.

### Line 1 — the WES tier is Core when operational efficiency is your differentiator

`warehouse-systems-ddd.md` classifies the WES tier conditionally:

> **WES** — **Core Domain** *if* operational efficiency is your differentiator
> (you build/tune your own DC); **Supporting/Generic** if you consume a vendor
> WES and integrate at arm's length.

This platform builds and tunes its own execution layer — there is no vendor
WES behind an anti-corruption layer here. The same document is explicit about
what follows from that: "This is genuinely the **Core Domain** if your
differentiator is operational efficiency; treat it as Supporting/Generic if
you're consuming a vendor WES." Choosing Core is also what justifies the
investment in a bespoke dispatch policy at all — under a Supporting
classification the correct move would have been to integrate a product, not
to design `claimNext`.

### Line 2 — Picking is classified Core outright

`amazon-fulfillment-ddd.md`'s subdomain table classifies the pieces:

| Subdomain | Type | Why |
| --- | --- | --- |
| **Fulfillment Orchestration & Optimization** | **Core** | Continuous re-planning to the fastest/cheapest path is the differentiator. |
| **Picking** (task generation, pick-to-light, robot-to-picker) | **Core** | "Directly drives throughput and accuracy at scale." |
| Packing & Cartonization | Supporting | "Optimization matters but is a well-understood problem." |
| Shipping & Manifest (SLAM, sortation) | Supporting | "Critical path, mostly standardized MHE + carrier integration." |
| Labor & Workforce Management | Supporting | Important, industry-common. |
| Equipment / Automation Control (WCS) | Supporting / Generic | Device-agnostic control is a largely solved integration category. |

This context spans **Picking (Core)** plus the execution slices of **Packing**
and **Shipping/SLAM (both Supporting)**. It is classified by its most valuable
part — the task lifecycle and dispatch that drive throughput — which is Core.

The Supporting pieces it touches are held deliberately thin, which is exactly
what the classification predicts should happen:

- **Cartonization is absent.** Box-size selection is called out in the
  reference as a Generic Subdomain to be extracted rather than duplicated
  across WMS and WES. This service does not implement it.
- **SLAM is reduced to its invariant.** No carrier integration, no manifest
  generation, no chute assignment — only the weigh-check, because that is the
  part with a rule worth enforcing here.

## What being Core buys, and what it obliges

**Buys:** the licence to build rather than buy, and to invest in the dispatch
model itself. `claimNext` with lease semantics, at-most-once claiming, and
CPT-ordered candidate selection are all bespoke design in an area where a
Supporting classification would have said "integrate a product."

**Obliges:** the quality bar this repo actually holds itself to —

| Discipline | Where |
| --- | --- |
| Every invariant has a failing-path unit test | `internal/domain/**/*_test.go` |
| The dependency rule is executable | `internal/architecture/` (arch-go: 9 dependency subtests, plus fleet fitness tests), `arch-test` CI job |
| Business rules are readable by non-developers | `features/*.feature` (10 files), run by godog |
| Both published contracts are linted in CI | `apis/openapi.yaml` + `apis/asyncapi.yaml` via Spectral |
| Mutation testing on the domain | `gremlins`: blocking `mutation-fast` subset (`./internal/domain/task`) on every push and PR, full `mutation` run on schedule/dispatch |

The same classification is plotted on the ddd-crew
[Core Domain Chart](./core-domain-chart.md).

## Where the neighbours sit

| Context | Tier | Classification | Relationship to this context |
| --- | --- | --- | --- |
| `inventory-storage` | WMS | **Core** — chaotic stow + bin-accurate tracking is a real operational innovation | No call from this side; it can consume this context's `TaskCompleted` (`order_ref`) to confirm picks (off by default there). Stock reaches this context indirectly via Work Planning |
| `product-master` | WMS | **Supporting** (per product-master's own ADR 0001) — SKU product master data | **Upstream**: its `ProductClassified` feeds this context's local classification copy (ADR-0039) |
| `wes-work-planning` | WES | **Core** — "the conductor," continuous waveless release | **Upstream** supplier of released work; **downstream** consumer of completions |
| `workforce-management` | — | **Supporting** — "allocates workforce to workload; important, industry-common" | No technical edge; a deliberate boundary at the process path |
| **`fulfillment-execution`** | **WES** | **Core** | — |
| `facility-layout` | — | **Generic** — "same bucket as Cartonization and WCS… well-understood, not a competitive differentiator" | No edge today |
| WCS / equipment | WCS | **Generic** — "Buy, don't build — device orchestration is rarely a competitive advantage" | Strategically downstream; not wired |

The full relationship analysis, with the context-mapping patterns, is in
[Context relationships](./context-relationships.md), and the diagram is on the
[Context map](../ecosystem/context-map.md).
