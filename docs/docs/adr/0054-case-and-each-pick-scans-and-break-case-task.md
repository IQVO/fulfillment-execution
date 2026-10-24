---
id: 0054-case-and-each-pick-scans-and-break-case-task
slug: /adr/0054-case-and-each-pick-scans-and-break-case-task
title: 54. Case and each pick scans; BREAK_CASE task
sidebar_label: "54. Case and each pick scans; BREAK_CASE task"
description: "ADR 0054 — a PICK task confirms by scanning an EACH or CASE GTIN against a local copy of product-master's pack levels, with a SKU guard; a BREAK_CASE task asks inventory-storage to break a sealed case. Quantities stay in eaches."
---

# 54. Case and each pick scans; BREAK_CASE task

## Status

Proposed (2026-10-24). Additive. Part of the mastercase / GTIN adoption
(`.hermes/plans/mastercase-briefs/contracts.md` sections 0, 1, 3, 6, 8 of the warehouse-systems
workspace). Reverses the note in [ADR-0052](./0052-postgres-and-kafka-are-the-only-runtime.md)
that `INVENTORY_STORAGE_BASE_URL` is no longer read: it is read again, for the case break only.

## Decision

- A local `PackView` (migration 0022) is fed by a consumer of product-master's
  `ProductPackLevelDeclared` / `ProductPackLevelRetired` events, under its own stable group
  `PACK_VIEW_CONSUMER_GROUP` (it must differ from `PRODUCT_CLASSIFICATION_CONSUMER_GROUP`; both read
  `warehouse.product-master.events`). No scan calls product-master.
- A PICK task may carry a pick plan (SKU, quantity in eaches, pick unit `EACH` or `CASE`).
  `POST /tasks/{id}/scans` takes `{stationId, gtin, count}`: the GTIN is validated with the shared
  `gs1` package, resolved in the `PackView`, and must identify the task's SKU (`scan-sku-mismatch`
  otherwise, which is the guard against the each barcode printed on a case of something else) at the
  task's pick unit (`scan-level-mismatch`). An EACH scan adds `count` eaches; a CASE scan adds
  `count` x `units_per_pack` eaches. A rejected scan changes nothing.
- Quantities are eaches everywhere: `POST /tasks/{id}/complete`, `TaskCompleted` and the
  confirm-pick call to inventory-storage are unchanged.
- A BREAK_CASE task is released when an each pick finds only a sealed case at the source. Completing it
  with `POST /tasks/{id}/break-case` calls inventory-storage `POST /containers/{lpn}:break`
  with `Idempotency-Key: break-case:<taskId>` behind a retry and circuit breaker, and only on success
  completes the task and publishes `TaskCompleted`. An unreachable inventory-storage leaves the task
  claimed (503); a retry does not break twice. `POST /tasks/{id}/complete` on a BREAK_CASE task is
  refused (`case-break-completion-required`).

## Consequences

- The chart renders `PACK_VIEW_CONSUMER_GROUP` and `INVENTORY_STORAGE_BASE_URL`; warehouse-infra must
  set both.
- The pick plan and case break travel on `WorkReleased` as optional fields (`pick`, `case_break`).
