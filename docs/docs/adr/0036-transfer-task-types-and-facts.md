---
id: 0036-transfer-task-types-and-facts
title: ADR-0036 — Transfer task types, atomic WorkReleased consumption, and transfer completion facts
sidebar_label: 0036 · Transfer task types & facts
sidebar_position: 36
description: Inter-warehouse-transfer work enters the pool as DISPATCH/ARRIVAL tasks carrying a transfer correlation block; completion publishes exactly one TransferPicked/TransferDispatched/TransferArrived fact; and the WorkReleased consumer's claim-then-create split is replaced by one atomic use case so a failed create can never permanently suppress redelivery. (Accepted)
---

# ADR-0036 — Transfer task types, atomic WorkReleased consumption, and transfer completion facts

## Status

**Accepted** (2026-10-06).

## Context

The fleet is building the inter-warehouse-transfer saga (see
warehouse-systems' network-inventory-transfers reference): planning
allocates origin stock, WES releases transfer-referenced work, and
execution must report the physical custody facts — picked at the origin,
dispatched from the origin, arrived at the destination — so planning can
advance the saga and destination receiving can correlate its receipt.
Until now fulfillment-execution only knew customer-order work: task types
PICK/PACK/SLAM/REBIN, `WorkReleased` payloads with no transfer
correlation, and a completion contract (`TaskCompleted`) that carries no
transfer identity at all.

Two facts forced the design:

1. **The transfer facts are execution's to report.** The physical acts
   (a picker picks the transfer's stock, a dispatcher loads it, a
   receiver receives it) happen against THIS service's tasks — the
   claim/lease/complete aggregate. No other context observes them
   first-hand; inventing them downstream from `TaskCompleted`'s
   order-shaped payload would be guesswork.

2. **The WorkReleased consumer had a pre-existing lost-event bug.** Its
   `handleMessageWithRetry` claimed the CloudEvents id
   (`processed_events` insert) BEFORE the catalogue lookup and the task
   creation, and OUTSIDE `CreateTask`'s UnitOfWork. A create that failed
   (transient DB blip, unknown path_id) left the marker committed with no
   Task in existence: the Kafka redelivery arrived, saw the marker, and
   acked — the WorkReleased was silently lost forever. Wiring transfer
   demands into that consumer before fixing it would have multiplied the
   blast radius (a lost transfer pick = a stuck saga).

## Decision

### 1. Transfer work enters through the existing single door

`WorkReleased` gains a strictly-additive, all-optional correlation block:
`transfer_ref`, `work_kind`, `site_id`, `sku`, `quantity` (plus
`demand_id` derived from the release's existing `ref`). A payload without
`transfer_ref` decodes exactly as before — every legacy producer is
unaffected. `work_unit_id` remains the Task's `OrderRef` (unchanged
contract); the transfer block rides alongside as correlation payload
that never affects claiming, capability matching or lease behaviour.

New task types `DISPATCH` and `ARRIVAL` join PICK/PACK/SLAM/REBIN (the
saga's dispatch/arrival legs are genuinely distinct pools with distinct
station capabilities, exactly the "queues not workflow steps" argument of
the process-paths model). They are catalogue-driven like every other
type: a `dispatch-*`/`arrival-*` path family must be declared in the
process-path catalogue (warehouse-infra's config or
process-path-management) before such work can be released.

### 2. One atomic use case replaces claim-then-create

A new `ApplyWorkReleased` use case claims the CloudEvents id, validates
the path_id against the catalogue, saves the Task and publishes
`TaskCreated` — all inside ONE UnitOfWork, with `CreateTask`'s nested
scope joining it (the same nested-join mechanism ArriveAtRebin already
uses). A failure anywhere rolls the claim back with the effect, so a
redelivery re-applies instead of being mistaken for an already-handled
event. This is a direct port of wes-work-planning's `onceAtomically`
(its ADR-0028), the fleet's reference fix for the identical bug shape.
Without a UnitOfWork (in-memory wiring) the claim is undone explicitly
via the new optional `ports.ProcessedEventReleaser` — the same fallback
the reference uses.

The in-process retry boundary simplifies as a result: retrying the whole
atomic call is always safe, because a failed attempt leaves no marker.

### 3. Completion selects exactly one transfer fact

When a task carrying a correlation block completes, `CompleteTask`
raises — alongside the unchanged `TaskCompleted` — exactly ONE fact
selected by `work_kind`:

| work_kind | fact |
| --- | --- |
| `TRANSFER_PICK` | `TransferPicked` |
| `TRANSFER_DISPATCH` | `TransferDispatched` |
| `TRANSFER_ARRIVAL` | `TransferArrived` |

Non-transfer tasks emit no transfer fact, ever. The task's Completed
state, `TaskCompleted` and the selected fact commit in the same
UnitOfWork (transactional outbox, ADR-0020), so a completed transfer task
and its custody fact are never observably apart.

Wire contract (CloudEvents 1.0, structured mode, on
`warehouse.fulfillment.events`):

- `type`: `com.warehouse.wes.fulfillment-execution.transfer.<Fact>`
- `dataschema`:
  `urn:warehouse:fulfillment-execution:events:<Fact>:v1`
- `subject` and Kafka key: the completing **task id** (per-aggregate
  ordering; the fact is the task's, not the transfer's)
- `time`: the completion time
- `data`: `{transfer_ref, demand_id?, work_unit_id, task_id, work_kind, site_id?, sku?, quantity?}`

`demand_id` is the release's own `ref` — the work-demand reference the
producer already carries, not a new producer obligation.

## Consequences

- **A failed WorkReleased create is now recoverable by redelivery.** The
  DLQ replay procedure in INTEGRATION.md no longer needs a manual
  `processed_events` delete for create failures (it remains for
  genuinely processed events).
- **The consumer's constructor surface changed.**
  `NewConsumer`/`NewConsumerWithGroup` take the assembled
  `*usecases.ApplyWorkReleased` instead of `CreateTask` + processed store
  + catalogue; the composition root builds it once and shares it.
- **Unknown work_kind is a hard error**, mirroring the path_id rule
  (ADR-0017): a `transfer_ref` whose `work_kind` is not one of the three
  declared kinds dead-letters rather than silently defaulting. Rehydrate
  fails the read on such a row (same discipline as PR #157's
  status/type validation).
- **Storage**: migration 0014 adds six nullable columns to `tasks`
  (`transfer_ref`, `demand_id`, `work_kind`, `site_id`, `sku`,
  `quantity`); NULL `transfer_ref` means "not transfer work" for both
  legacy and new rows.
- **The TaskCompleted → WES completion loop is untouched** —
  `work_unit_id` enrichment and RecordCompletion semantics are identical
  for transfer tasks.
- **What becomes harder**: task-type enumerations in consuming
  contracts (openapi's TaskType, console UIs) must learn DISPATCH and
  ARRIVAL; and the transfer fact set is per-completion single-shot — a
  short-picked transfer currently reports a clean pick (exception
  states are the saga's next increment, per the fleet reference).
