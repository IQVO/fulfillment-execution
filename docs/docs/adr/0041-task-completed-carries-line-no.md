---
id: 0041-task-completed-carries-line-no
slug: /adr/0041-task-completed-carries-line-no
title: 41. TaskCompleted carries an optional line_no
sidebar_label: "41. TaskCompleted carries line_no"
description: "ADR 0041 — WorkReleased v1 (consumed) gains an optional line_no that is stamped on the Task as source_line_no (order work only), and TaskCompleted v1 (integration and analytics topics) gains an optional line_no next to order_ref, so inventory-storage confirms the pick of exactly the line that was picked. Additive, still v1."
---

# 41. TaskCompleted carries an optional line_no

## Status

Accepted. Decided 2026-10-06 (audit decision 18, per-line confirm-pick).
Extends [ADR 0040](./0040-task-completed-carries-order-ref.md) (`order_ref`)
and the published `TaskCompleted` contract of
[ADR-0004](./0004-kafka-integration-events-and-envelope.md),
[ADR-0014](./0014-labor-performance-integration-hooks.md) and
[ADR-0023](./0023-task-type-on-wire.md) additively; none of those bodies is
edited. This repo owns hops 5 and 6 of the cross-repo per-line contract
(order-management `lineNo` on the reservation request → inventory-storage
`Reservation.line_no` → wes-work-planning `WorkUnit.line_no` →
`WorkReleased.line_no` → **`Task.source_line_no` → `TaskCompleted.line_no`** →
inventory-storage per-line confirm).

## Context

ADR 0040 let `TaskCompleted` say which **order** it belongs to. Work units
are per **order line** (`work_unit_id = "<order>-line-<n>"`), so one order has
one PICK task per line and every one of them published the same `order_ref`.
ADR 0040 recorded the consequence: a consumer could not tell which line a
completion was for, so inventory-storage had to count completed PICK tasks per
order and confirm all reservations only on the last one.

The line number is known upstream (wes-work-planning receives it as
`OrderAllocatedLine.LineNo`) but today it only survives inside the work unit id
string. Parsing that string would couple this service to an id format it does
not own.

## Decision

1. **Read an optional `line_no` from `WorkReleased`** (integer ≥ 1, omitted by
   producers that predate it). `ApplyWorkReleased` stamps it on the Task as
   `sourceLineNo` (new nullable column `tasks.source_line_no`, migration
   `0017`, additive; `Task.SourceLineNo()`, `0` = unknown).
2. **Order work only.** Exactly like `source_order_id`, the line is stamped
   only for order-originated work; **never for transfer work**, even if a
   producer sent one. A non-positive value is treated as unknown.
3. **Never parse the work unit id.** If `line_no` is absent the line stays
   unknown even when the id ends in `-line-3`.
4. **Add `line_no`** to the `data` of `TaskCompleted` on **both** topics
   (`warehouse.fulfillment.events`, dataschema
   `urn:warehouse:fulfillment-execution:events:TaskCompleted:v1`, and
   `warehouse.fulfillment.analytics`, dataschema
   `urn:warehouse:fulfillment-execution:analytics:TaskCompleted:v1`): an
   **optional integer**, the task's `sourceLineNo`, **omitted when unknown**
   (never `0`). It sits next to `order_ref`; every existing field is unchanged
   byte for byte, so the golden JSON of a task without a line is identical to
   before.
5. **Stay v1.** Adding an optional field is additive under the fleet
   CloudEvents standard; `type` and the `dataschema` URN do not change.
6. **Source on the wire.** As for `order_ref`, both encoders read the value
   from the just-saved `Task` through `TaskRepo.FindById` inside the unit of
   work (outbox path, ADR-0020); the domain event `TaskCompleted` stays thin.
7. **Contract with inventory-storage.** For a PICK completion with `order_ref`
   and `line_no`, inventory-storage confirms the reservation(s) of that line;
   without `line_no` it keeps the last-pick counting of ADR 0040. This repo
   publishes the field; the consumer rule lives in inventory-storage.

## Consequences

### Easier

- The first completion of an order no longer implies the other lines were
  picked: a consumer can confirm exactly the line that finished.
- Fully backward compatible and order-independent across repos: every hop is
  optional, so wes-work-planning, this service and inventory-storage can be
  released in any order (until wes-work-planning sends `line_no`, nothing
  changes on the wire).

### Harder

- **Tasks that predate migration 0017** (and tasks created through REST/MCP,
  which have no release `line_no`) have no line: their completion omits
  `line_no` and consumers fall back to the order-level behaviour of ADR 0040.
  Backfilling is not possible from this service's own data (parsing the id was
  rejected, decision 3).
- `Task` gains one more persisted attribute; `CreateTask.ExecuteRelease` gains
  a `sourceLineNo` parameter (`Execute`/`ExecuteTransfer` keep their
  signatures).
- Short picks are still not modelled (ADR 0040, decision 6): a completion
  means "the pick task for this line finished".
- Consumers must tolerate unknown fields (a standing fleet rule). Ecosystem
  check (origin/develop of the other fleet repos): labor-performance and
  wes-work-planning decode `data` with the CloudEvents SDK `DataAs` into
  structs that do not declare `line_no` and have no `DisallowUnknownFields`, so
  they ignore it. inventory-storage's `TaskCompletedConsumer` decodes the same
  way (`order_ref` only today) and ignores it until its own per-line change
  lands. e2e-tests does not decode `TaskCompleted` data. This service's own
  analytics projector has no `DisallowUnknownFields` either.
- `ItemPicked` is still defined but never raised (see the event-storming
  hotspot); this ADR does not change that.
