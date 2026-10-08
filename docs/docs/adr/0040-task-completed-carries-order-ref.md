---
id: 0040-task-completed-carries-order-ref
slug: /adr/0040-task-completed-carries-order-ref
title: 40. TaskCompleted carries an optional order_ref
sidebar_label: "40. TaskCompleted carries order_ref"
description: "ADR 0040 — TaskCompleted v1 (integration and analytics topics) gains an optional order_ref string (the upstream order id, stamped on the Task from WorkReleased.ref) so inventory-storage can confirm picks per ORDER. Additive, still v1; short picks are not modelled."
---

# 40. TaskCompleted carries an optional order_ref

## Status

Accepted. Decided 2026-10-06 (audit decision 17). Extends the published
`TaskCompleted` contract of [ADR-0004](./0004-kafka-integration-events-and-envelope.md),
[ADR-0014](./0014-labor-performance-integration-hooks.md) and
[ADR-0023](./0023-task-type-on-wire.md) additively; none of those bodies is
edited. Companion to inventory-storage's ADR 0032 (confirm-pick), whose plan
this replaces.

## Context

inventory-storage must confirm picked stock when a PICK task completes, so a
reservation does not stay `ACTIVE` forever. Its reservations are created by
order-management with `demand_ref = OrderId` (one reservation per order
line). The completion therefore has to say **which order** it belongs to.

What the code actually carries today (verified against `origin/develop` of
fulfillment-execution and wes-work-planning):

- wes-work-planning releases one work unit **per order line**:
  `work_unit_id = "<order_id>-line-<n>"`, `reference = <order_id>`
  (`ApplyOrderAllocated`), and publishes `WorkReleased{work_unit_id, ref}`
  with `ref = reference = <order_id>`.
- fulfillment-execution's `ApplyWorkReleased` stores
  `Task.orderRef = work_unit_id`, i.e. the **per-line** id
  `"<order_id>-line-<n>"`, and **dropped** `ref` (it was only used as the
  `demand_id` of transfer facts). `TaskCompleted.work_unit_id` is that
  `orderRef`.

So the order id is **not** `Task.orderRef` and not `work_unit_id`; it arrives
on `WorkReleased.ref` and was not kept. Publishing `orderRef` as `order_ref`
would emit `"<order>-line-<n>"`, which never equals a reservation's
`demand_ref`. A `Task` also has no SKU and no quantity, so the order is the
finest correlation a completion can offer.

## Decision

1. **Keep the order id on the Task.** `ApplyWorkReleased` stamps
   `WorkReleased.ref` onto the task as `sourceOrderId` (new nullable column
   `tasks.source_order_id`, migration `0016`, additive; `Task.SourceOrderId()`).
   It is stamped only for order-originated work: **never for transfer work**
   (its `ref` is a demand id), and left empty when a release carries no
   `ref`. `Task.orderRef` / `work_unit_id` are unchanged.
2. **Add `order_ref`** to the `data` of `TaskCompleted` on **both** topics
   (`warehouse.fulfillment.events`, dataschema
   `urn:warehouse:fulfillment-execution:events:TaskCompleted:v1`, and
   `warehouse.fulfillment.analytics`, dataschema
   `urn:warehouse:fulfillment-execution:analytics:TaskCompleted:v1`): an
   **optional string**, the task's `sourceOrderId`, **omitted when empty**
   (`omitempty`; the analytics payload only sets the key when non-empty).
   Every existing field is unchanged byte for byte.
3. **Stay v1.** Adding an optional field is additive under the fleet
   CloudEvents standard; the `type` and `dataschema` URN do not change.
4. **Source on the wire.** The domain event `TaskCompleted` stays thin
   (`TaskId`, `StationId`). Both encoders already enrich it from the
   just-saved `Task` through `TaskRepo.FindById` inside the unit of work
   (outbox path, ADR-0020); `order_ref` is read from the same loaded `Task`.
   The analytics encoder performs one more `FindById`, like its existing
   `task_type` lookup.
5. **Per-order granularity.** The consumer (inventory-storage) confirms every
   ACTIVE reservation whose `demand_ref` equals `order_ref`, for PICK tasks
   (`task_type = PICK`).
6. **Short picks are not modelled.** A `Task` carries no SKU or quantity, so a
   completion always means "a pick task for this order finished"; a
   partial/short pick cannot be expressed or detected from this event. This is
   an explicit, recorded limitation, not an oversight: modelling it needs a
   per-line pick fact and a business rule nobody has specified.

## Consequences

### Easier

- inventory-storage can correlate a pick completion to its reservations with
  one additive field and no REST call back into fulfillment-execution.
- Backward compatible: for a task without an order id the bytes on the wire
  are identical to before (golden tests on both topics).

### Harder

- **One order has several PICK tasks** (one per order line, because work units
  are per line). Every one of them publishes the same `order_ref`, so the
  **first** completion confirms all of the order's ACTIVE reservations,
  including lines not yet picked. The consumer is idempotent, so later
  completions are no-ops. Accepted together with the short-pick limitation;
  fixing it needs a per-line fact (see decision 6).
- **Tasks that predate migration 0016** (and tasks created through REST/MCP,
  which have no release `ref`) have no order id: their completion omits
  `order_ref` and is not confirmed by inventory-storage. Backfilling is not
  possible from this service's own data.
- `Task` gains one more persisted attribute and `CreateTask` one more entry
  point (`ExecuteRelease`); `Execute`/`ExecuteTransfer` keep their
  signatures.
- Consumers must tolerate unknown fields (a standing fleet rule). Ecosystem
  check (origin/develop of the other fleet repos): labor-performance and
  wes-work-planning decode `data` with the CloudEvents SDK `DataAs` into
  structs (no `DisallowUnknownFields`) and ignore the new field.
  order-management's RepromiseOrder consumer reads `TaskCPTMissed` /
  `PackageManifested`, not `TaskCompleted`. inventory-storage has no consumer
  yet (only its ADR 0032 plan). network-inventory-planning's transfer-fact
  consumer ignores `TaskCompleted`. e2e-tests only exercises the flow end to
  end.
- `ItemPicked` is still defined but never raised (see the event-storming
  hotspot); this ADR does not change that.
