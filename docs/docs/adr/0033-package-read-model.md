---
id: 0033-package-read-model
slug: /adr/0033-package-read-model
title: 33. Package read model — GET /packages/{id} and GET /packages?orderRef=
sidebar_label: 33. Package read model
description: "ADR 0033 — add a side-effect-free read model for the Package aggregate (GET /packages/{id}, GET /packages?orderRef=) so the SLAM operator and outbound dock loader can tell a LABELED carton from a DIVERTED one, while POST /packages/{id}/slam keeps its outcome-agnostic 204."
---

# 33. Package read model

## Status

Accepted.

## Context

`POST /tasks/{id}/seal-package` creates a Package and returns it on `201`
(with a `Location: /packages/{id}` header), and `POST /packages/{id}/slam`
runs the SLAM weigh-check on it. SLAM returns `204` in **both** outcomes:

- label applied: status `LABELED`, `LabelApplied` + `PackageManifested`
  raised (ADR-0025);
- weight discrepancy: status `DIVERTED`, `WeightDiscrepancyDetected` +
  `PackageDiverted` raised.

The API had no read endpoint for a Package at all — the `Location` header
pointed at a resource nobody could `GET`. So an associate (or the
fleet's warehouse-day simulator, which drives the fleet purely through
published REST contracts) at the SLAM scale or the outbound dock could not
tell whether a carton goes to the truck by its `sortLane` or to the
problem-solve area. The only signal was the domain events, which are not on
the wire for `LabelApplied`/`PackageDiverted`.

## Decision

Add a read model for the Package aggregate, additive only:

- `GET /packages/{id}` returns `200` with the existing `PackageResponse`
  schema (the same DTO seal-package's `201` returns: `id`, `orderRef`,
  `status` `OPEN|SEALED|LABELED|DIVERTED`, `scannedContents`,
  `fragileHandling`, `giftWrapRequested`, `sortLane`). An unknown id is a
  `404` RFC 7807 `package-not-found` problem, reusing
  `usecases.ErrPackageNotFound` and its existing mapping.
- `GET /packages?orderRef=` returns `200` with an array of `PackageResponse`
  (empty when none), mirroring `GET /tasks?orderRef=` (ADR-0013): the
  same query-parameter validation and the same `400 invalid-request` for a
  missing or empty `orderRef`. Ordered by package id.
- One read use case per operation, `GetPackage` and
  `GetPackagesByOrderRef`, following `GetTasksByOrderRef`.
- `ports.PackageRepo` gains `FindByOrderRef`, implemented in the memory and
  postgres adapters. Migration `0013_package_order_ref_index` adds
  `idx_packages_order_ref` (non-unique: an order can produce several
  cartons), the same access path migration 0006 indexed for tasks.

**`POST /packages/{id}/slam` stays `204`.** Both outcomes are successful
completions of the weigh-check command. Returning a body (or a different
status per outcome) would change a published contract every existing caller
relies on, and would make a command double as a query. The REST-idiomatic
way to observe the result of a command that returns no body is to read the
resource it acted on, which this ADR makes possible.

## Consequences

- A caller learns the SLAM outcome with one extra `GET /packages/{id}` after
  the `204`. That costs one round trip and keeps commands and queries
  separate.
- The `Location` header seal-package already returned now resolves.
- No AsyncAPI change. No new domain events, no domain-layer change.
- `GET /packages?orderRef=` takes the same `orderRef` value `Task.OrderRef`
  carries: wes-work-planning's WorkUnit id, not order-management's order id.
  See the `orderRef` contract in ADR-0013.
