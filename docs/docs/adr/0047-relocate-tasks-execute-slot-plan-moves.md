---
id: 0047-relocate-tasks-execute-slot-plan-moves
slug: /adr/0047-relocate-tasks-execute-slot-plan-moves
title: 47. RELOCATE tasks execute approved slot-plan moves and report them on TaskCompleted
sidebar_label: "47. RELOCATE tasks for slot-plan moves"
description: "ADR 0047 — work released on the RELOCATE path carries one slot-plan move (WorkReleased.data.relocation); the RELOCATE task completes only with the operator-confirmed quantity and the scanned bins, and its TaskCompleted gains the additive sku, from_bin, to_bin, quantity and relocation_ref fields inventory-storage applies to its ledger. Implements slotting-optimization ADR 0010 on the execution side."
---

# 47. RELOCATE tasks execute approved slot-plan moves and report them on TaskCompleted

## Status

Accepted (2026-10-14). Implements the fulfillment-execution part of
slotting-optimization ADR 0010 ("Approved slot plans are executed as WES
relocation work"), final-gaps decision E1. Companion ADRs: inventory-storage
ADR 0039 (RelocateStock, StockRelocated, the TaskCompleted RELOCATE branch),
process-path-management's RELOCATE path (capability `relocate`) and
wes-work-planning's slot-plan intake (SlotPlanApproved -> one RELOCATE work
unit per move). No existing ADR is amended: every contract change below is
additive.

## Context

slotting-optimization approves a slot plan (`SlotPlanApproved`: moves of kind
`Relocate`, `Assign` or `Vacate`, each with a `sku` and an optional
`from_slot` / `to_slot`). Moving stock between bins is floor work. The fleet
already has one conductor for floor work: wes-work-planning plans it,
process-path-management routes it, and fulfillment-execution executes it with
operators. inventory-storage keeps the stock ledger, and fulfillment-execution's
`TaskCompleted` already drives it (confirm-pick, ADR 0040 / 0041). ADR 0010
keeps that shape. wes-work-planning plans one RELOCATE work unit per move.
This service turns the released work into a task, an operator executes it, and
the completion tells inventory-storage what moved.

Two facts are only known on the floor. The first is the quantity: v1 moves the
whole on-hand quantity of the SKU at the source bin, counted by the operator.
The second is the open side of an `Assign` or `Vacate` move: the operator
chooses and scans the source bin of an `Assign` (any non-forward bin holding
the SKU) and the reserve destination of a `Vacate`. So the completion must
carry them.

## Decision

### 1. WorkReleased: the `relocation` object (consumed contract)

wes-work-planning releases a slot-plan move on `warehouse.work-planning.events`
as an ordinary `com.warehouse.wes.work-planning.workunit.WorkReleased`
CloudEvent (v1, dataschema `urn:warehouse:wes-work-planning:events:WorkReleased:v1`).
The event has `path_id` = the RELOCATE path (`RELOCATE`, or any `relocate-*`
id the catalogue resolves to it) and ONE additive, nested object
`data.relocation`:

| field | type | required | meaning |
|---|---|---|---|
| `plan_id` | string, non-blank | yes | `SlotPlanApproved.data.plan_id` |
| `move_seq` | integer >= 0 | yes (an absent value is rejected, not read as 0) | the move's 0-based position in `SlotPlanApproved.data.moves` |
| `kind` | `Relocate` \| `Assign` \| `Vacate` (exact case) | yes | the SlotPlanApproved move kind |
| `sku` | string, non-blank | yes | the move's `sku` |
| `from_bin` | string | `Relocate`, `Vacate` | the move's `from_slot`; omit for an `Assign` (scanned at completion) |
| `to_bin` | string | `Relocate`, `Assign` | the move's `to_slot`; omit for a `Vacate` (scanned at completion) |

The other fields keep their meaning. `work_unit_id` becomes the task's order
reference (any unique id, e.g. `slot-plan-7-move-3`). `cpt` is the deadline
that orders the queue. `ref` should be the plan id; it is not stored, because
RELOCATE work is neither order work (no `order_ref` / `line_no`) nor transfer
work (no `transfer_ref`). `plan_id` plus `move_seq` must fit 200 characters:
inventory-storage limits `source_ref` to that.

```json
{"path_id": "RELOCATE", "work_unit_id": "slot-plan-7-move-3", "cpt": "2026-09-30T23:00:00Z", "ref": "plan-7",
 "relocation": {"plan_id": "plan-7", "move_seq": 3, "kind": "Assign", "sku": "SKU-1", "to_bin": "WH1-FWD-A01-01-01-A"}}
```

These releases are rejected and dead-lettered to
`warehouse.work-planning.events.dlq`, and no task is created:

- a `relocation` object that is not a valid move: missing `move_seq`, an
  unknown `kind`, a bin its kind requires missing, `from_bin` equal to
  `to_bin`, or a blank `plan_id` / `sku`;
- RELOCATE-path work without the object;
- the object on any other path.

The dedupe on the CloudEvent `id` is unchanged. A nested object, not
top-level fields, so it cannot collide with the transfer block's `sku`, and
its presence is one unambiguous marker.

### 2. The RELOCATE task

`task.Relocate` (`RELOCATE`) carries the planned `RelocationMove`. It is created
only from released work (never through `POST /tasks`, which keeps its
creatable type enum), requires the catalogue path's capabilities (`relocate`)
and is claimed like any other type (`POST /stations/{id}/task-claims` with
`taskType: RELOCATE`). The move is persisted with the task (migration 0019,
nullable `relocation_*` columns, NULL for every other task) and shown on the
task read model (`relocation` on `TaskResponse`, MCP `find_claimable_work`),
so the operator sees what to move.

### 3. Completing a RELOCATE task

`POST /tasks/{id}/completions` and the MCP `complete_task` tool take three
optional inputs next to `stationId`. Each is required for a RELOCATE task in
the cases below and rejected for any other task type:

- `quantity`: the counted, confirmed units, at least 1;
- `fromBin`: required when the move left the source open (`Assign`);
- `toBin`: required when the move left the destination open (`Vacate`).

A scan of a side the plan fixed must equal the planned bin. The claim rules
(owner, active lease, no double-complete) apply first and are unchanged.
Rejections, all `422` on REST (RFC 7807) and a tool error on MCP:

| error | slug |
|---|---|
| RELOCATE task completed without the inputs | `relocation-confirmation-required` |
| quantity below 1, an open side not scanned, a scan that differs from the plan, equal bins | `invalid-relocation-confirmation` |
| relocation inputs on a non-RELOCATE task | `not-relocation-task` |

### 4. TaskCompleted: the additive RELOCATE fields (published contract)

A RELOCATE task's `TaskCompleted` (still v1) carries five more `data` fields.
They are present only for `task_type` `RELOCATE` and omitted on every other
task, so every existing consumer and golden is unaffected. Names and JSON
types are exactly what inventory-storage's TaskCompleted consumer decodes:

| field | type | value |
|---|---|---|
| `sku` | string | the move's SKU |
| `from_bin` | string | the planned `from_bin`, or the scanned source of an `Assign` |
| `to_bin` | string | the planned `to_bin`, or the scanned destination of a `Vacate` |
| `quantity` | integer >= 1 | the operator-confirmed quantity |
| `relocation_ref` | string | `<plan_id>:<move_seq>` |

inventory-storage applies it as a RelocateStock (reason SLOTTING,
`source_ref` = `relocation_ref`) and raises StockRelocated.
slotting-optimization matches that back to the plan's move.
wes-work-planning's existing TaskCompleted handling (RecordCompletion by
`work_unit_id`) also tells it the move finished, which releases the moves
that depend on it (ADR 0010, ordering).

## Consequences

- One planner, one executor, one ledger: no new topic and no new event type
  in this service. `apis/asyncapi.yaml` documents the `relocation` object
  (consumed) and the five TaskCompleted fields (published), with an example
  of each.
- Tests: domain (`relocation_test.go`), use cases (`relocate_task_test.go`),
  WorkReleased decode (`consumer_relocation_test.go`), the golden wire shape
  and a round trip through inventory-storage's struct
  (`relocation_wire_test.go`), REST and MCP completion, Postgres round trip of
  the move and result, and an end-to-end Kafka + Postgres test (testcontainers)
  from a released RELOCATE work unit to the published TaskCompleted.
- A RELOCATE task can only be completed with operator input. The MCP
  `complete_task` description says so, and a model call without it is
  rejected without changing state.
- Out of scope in v1 (ADR 0010): partial quantities, min/max replenishment,
  and validating bins here. inventory-storage validates the bins against the
  site and dead-letters a move its ledger rejects.
