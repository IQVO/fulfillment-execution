---
id: 0056-ship-in-own-container
slug: /adr/0056-ship-in-own-container
title: 56. Ship in own container
sidebar_label: "56. Ship in own container"
description: "ADR 0056 — an order line that is exactly one full active CASE may be sealed in the case itself (the case's GTIN, dimensions and weight), with no cartonisation. Opt-in on POST /tasks/{id}/package-seals; additive field on the package and on PackageSealed."
---

# 56. Ship in own container

## Status

Proposed (2026-10-24). Builds on [ADR-0054](./0054-case-and-each-pick-scans-and-break-case-task.md)
(case picks, the local PackView) and [ADR-0055](./0055-scan-idempotency-and-claimable-break-case.md).
Contract P11-3 of the mastercase programme.

## Context

When a customer orders exactly one full case of a SKU, the picked sealed case is already a shippable
carton. Cartonising it into another box wastes a box and mis-states the parcel: the case has declared
dimensions and weight in product-master. Product-master publishes pack levels
(`ProductPackLevelDeclared/Retired`); this service already keeps a local PackView of them and must not
call product-master on the hot path.

## Decision

- **Opt-in.** `POST /tasks/{id}/package-seals` accepts two additive request fields: `shipInOwnContainer`
  (boolean) and `lineQuantity` (integer, EACHES, never cases). Absent or `false`: the ordinary seal,
  request and response byte-for-byte unchanged. `true` without a `lineQuantity >= 1` is `400`.
- **Rule.** The package ships in its own container only when `contents` names exactly one SKU and
  `lineQuantity` equals EXACTLY the `unitsPerPack` of the SKU's CASE level in effect now in the local
  PackView (`unitsPerPack - 1` and `unitsPerPack + 1` are both refused). Of overlapping levels the greater
  version wins. The decision reads only the local table.
- **Refusals** are `422` problems and seal nothing: `ship-in-own-container-not-single-line`,
  `ship-in-own-container-quantity-mismatch`, `ship-in-own-container-no-case-level`,
  `ship-in-own-container-case-retired`, `ship-in-own-container-case-dimensions-unknown`
  (product-master declares dims optionally, and there is nothing to ship by without them). There is no
  silent fallback to a carton: the caller chose the opt-in and learns why it cannot be honoured, then
  seals ordinarily.
- **Package fact.** A sealed package gains `shipInOwnContainer: true`, `containerSource: CASE` and
  `container {gtin, lengthMm, widthMm, heightMm, weightG}`, copied at seal time (a later re-declaration
  does not change a sealed package). Omitted from an ordinary carton. Persisted in nullable columns
  (migration 0024), which also add the optional case dimensions to the PackView.
- **Event.** `PackageSealed` (analytics, v1 stays v1) gains the optional additive `ship_in_own_container`,
  `container_source`, `container_gtin`, `container_dims{length_mm,width_mm,height_mm,weight_g}`, present
  only on the own-container variant; the ordinary payload is unchanged.
- **Idempotent** on the task as the ordinary seal is: a retry returns the package already sealed.

## Consequences

- No new consumer, topic, consumer group or environment value: the existing pack-level consumer also
  stores the case dimensions. Nothing is required of warehouse-infra.
- Rows of the PackView written before migration 0024 have no dimensions until product-master re-declares
  the level; own-container seals against them are refused `case-dimensions-unknown`.
- Quantities stay in eaches throughout.
