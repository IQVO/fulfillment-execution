---
id: 0050-replenish-tasks-execute-pick-face-replenishment
slug: /adr/0050-replenish-tasks-execute-pick-face-replenishment
title: 50. REPLENISH tasks execute pick-face replenishment moves and report them on TaskCompleted
sidebar_label: "50. REPLENISH tasks for pick-face replenishment"
description: "ADR 0050 — work released on the REPLENISH path carries one replenishment move (WorkReleased.data.replenishment); the REPLENISH task mirrors RELOCATE (ADR 0047): it completes only with the operator-confirmed quantity, announces both bins on TaskAvailable, and its TaskCompleted gains the additive request_ref next to sku, from_bin, to_bin and quantity, which inventory-storage applies as a RelocateStock with reason REPLENISHMENT."
---

# 50. REPLENISH tasks execute pick-face replenishment moves and report them on TaskCompleted

## Status

Accepted (2026-10-17). Additive: no existing event, endpoint or payload
changes. Consumer-first: inventory-storage's REPLENISH path (its ADR 0041,
`ApplyReplenishTask`) already decodes the `TaskCompleted` shape below;
`WorkReleased.data.replenishment` is produced by wes-work-planning after this
lands, so this service accepts it additively. Implements the fulfillment-execution
part of the pick-face-replenishment chain (decision 5 of its plan). Companion to
[ADR 0047](./0047-relocate-tasks-execute-slot-plan-moves.md), which it mirrors, and
[ADR 0049](./0049-task-pool-events-task-available-and-task-claimed.md), whose
placement logic it extends.

## Context

pick-face-replenishment watches the stock of each forward pick face and
requests a replenishment (`ReplenishmentRequested`: a SKU, a reserve source
bin, the pick face, a quantity and a `NORMAL` or `URGENT` priority). Moving the
stock is floor work, and the fleet already has the conductor for it: wes-work-planning
plans it as a `REPLENISH` work unit, process-path-management routes it,
fulfillment-execution runs it with operators, and inventory-storage keeps the
ledger. The same chain already runs slot-plan moves as `RELOCATE` work.
Replenishment is a sibling of RELOCATE with one difference: both bins and a
planned quantity are known when the work is released, so nothing is left for the
operator to choose, only to confirm.

## Decision

### 1. WorkReleased: the `replenishment` object (consumed contract)

wes-work-planning releases a request on `warehouse.work-planning.events` as an
ordinary `com.warehouse.wes.work-planning.workunit.WorkReleased` CloudEvent (v1).
The event has `path_id` = the REPLENISH path (`REPLENISH`, or any `replenish-*`
id the catalogue resolves to it; capability `replenish`) and ONE additive, nested
object `data.replenishment`, next to `relocation`:

| field | type | required | meaning |
|---|---|---|---|
| `request_id` | string, non-blank, at most 200 characters | yes | the replenishment request id |
| `sku` | string, non-blank | yes | the SKU to replenish |
| `from_bin` | string, non-blank | yes | the source (reserve) bin |
| `to_bin` | string, non-blank, not equal to `from_bin` | yes | the pick face |
| `quantity` | integer >= 1 (an absent value is rejected, not read as 0) | yes | the planned units |
| `priority` | `NORMAL` \| `URGENT` (exact case) | yes | the request urgency |

`work_unit_id` becomes the task's order reference, `cpt` orders the queue, and
`ref` should be the request id; it is not stored, because REPLENISH work is neither
order work nor transfer work.

```json
{"path_id": "REPLENISH", "work_unit_id": "replenish-req-9", "cpt": "2026-09-30T16:00:00Z", "ref": "req-9",
 "replenishment": {"request_id": "req-9", "sku": "SKU-1", "from_bin": "WH1-RSV-AMB-B02-03-02-C",
                   "to_bin": "WH1-FWD-AMB-A01-01-01-A", "quantity": 24, "priority": "URGENT"}}
```

### 2. The pairing rule (deterministic errors)

Exactly ADR 0047's rule, extended to the second object. These releases are
rejected and dead-lettered to `warehouse.work-planning.events.dlq` like the
RELOCATE ones, and no task is created:

| release | error |
|---|---|
| REPLENISH-path work without the `replenishment` object | `ErrReplenishmentMoveMissing` |
| a `replenishment` object on any other path (RELOCATE included) | `ErrReplenishmentMoveOnOtherPath` |
| a `relocation` object on the REPLENISH path | `ErrRelocationMoveOnOtherPath` (ADR 0047's existing error) |
| RELOCATE-path work with a `replenishment` object and no `relocation` object | `ErrRelocationMoveMissing` |
| a `replenishment` object that is not a valid move: a blank `request_id`, `sku` or bin, equal bins, a missing or non-positive `quantity`, an over-long `request_id`, a `priority` other than `NORMAL` / `URGENT` | wraps `task.ErrInvalidReplenishmentMove` or `task.ErrUnknownReplenishmentPriority` |

The dedupe on the CloudEvent `id` and the claim, task row and outbox rows
committing in one transaction are those of ADR 0046 and are unchanged: a rejected
release leaves no processed marker.

### 3. The REPLENISH task

`task.Replenish` (`REPLENISH`) carries the planned `ReplenishmentMove` (request id,
SKU, both bins, planned quantity, priority). It is created only from released
work (never through `POST /tasks`, whose creatable enum is unchanged), requires
the catalogue path's capabilities (`replenish`) and is claimed like any other type
(`POST /stations/{id}/claim-next` with `taskType: REPLENISH`); a station without
the `replenish` capability is never given one (the same capability matching as
`relocate`). The move is persisted with the task (migration 0020, nullable
`replenishment_*` columns, NULL for every other task; additive, with a tested
down) and shown on the read model (`replenishment` on `TaskResponse`, MCP
`find_claimable_work`).

### 4. Completing a REPLENISH task

`POST /tasks/{id}/complete` and the MCP `complete_task` tool take the same three
optional inputs as for RELOCATE, now meaningful for two task types:

- `quantity`: the operator-counted, confirmed units, at least 1. **Required** for
  a REPLENISH task.
- `fromBin`, `toBin`: **optional scans**. Both bins are planned, so a scan, when
  given, must equal the planned bin; an omitted scan is the planned bin.

The confirmed quantity is the operator's count and need not equal the planned
quantity (the source may hold less, or the pick face less room): like a RELOCATE
confirmation it is not capped here, and inventory-storage validates it against its
ledger and bins, dead-lettering a move the ledger rejects. The claim rules (owner,
active lease, no double-complete) apply first and are unchanged; a replayed
completion is the existing `409`. Rejections are `422` on REST (RFC 7807) and a
tool error on MCP, with new slugs because the RELOCATE ones name the task type:

| error | slug |
|---|---|
| REPLENISH task completed without the inputs | `replenishment-confirmation-required` |
| quantity below 1, or a scan that differs from the planned bin | `invalid-replenishment-confirmation` |
| inputs on a task that is neither RELOCATE nor REPLENISH | `not-relocation-task` (kept for compatibility; its title now says "RELOCATE and REPLENISH") |

### 5. TaskCompleted: the additive `request_ref` (published contract)

A REPLENISH task's `TaskCompleted` (still v1) carries `task_type: "REPLENISH"`,
`sku`, `from_bin`, `to_bin` and `quantity` (the confirmed quantity, as for
RELOCATE) and the additive `request_ref` = the `request_id`. It mirrors
`relocation_ref`; `relocation_ref`, `order_ref` and `line_no` are absent.
`request_ref` is omitted on every other task's event, so every existing consumer
and golden is unaffected. Names and JSON types are exactly what inventory-storage's
TaskCompleted consumer decodes (strings and an integer quantity); it applies the
completion as a `RelocateStock` with reason `REPLENISHMENT` and `source_ref` =
`request_ref`, which pick-face-replenishment matches back to close the request.
`work_unit_id` is the task's order reference, so wes-work-planning's existing
`RecordCompletion` also learns the work unit finished.

### 6. TaskAvailable and priority

A REPLENISH task knows both bins, so it is announced (ADR 0049): `from_location`
= the source bin, `to_location` = the destination bin, `site_id` from the bin's
LocationCode. No bin with a determinable site means no announcement, as for
RELOCATE.

`priority_band` follows the ADR 0049 derivation unchanged (task type + CPT
instant). claimNext orders by CPT only, so **`URGENT` does not sort before
`NORMAL` in this service**: the ordering key does not carry the priority, and
adding it would change `claimNext` for every task and the band's "equal for
exactly the tasks claimNext orders as peers" guarantee. The priority is recorded on
the task, shown to the operator, and `URGENT` is expected to arrive with an earlier
CPT: wes-work-planning places URGENT requests in the front band (its P3b).
Priority-aware ordering, and the interleaving tie-break inside a band, are
separate changes (P3c).

## Consequences

- One planner, one executor, one ledger: no new topic and no new event type in this
  service (so no change to the type catalogue). `apis/asyncapi.yaml` documents the
  `replenishment` object (consumed), the REPLENISH `TaskCompleted` and a
  `TaskAvailable` example; `apis/openapi.yaml` adds `REPLENISH` to `AnyTaskType`,
  the `replenishment` object on `TaskResponse`, and widens the complete-task body.
- RELOCATE behaviour and its tests are unchanged. The two move task types share
  the completion inputs (`task.ReplenishmentConfirmation` is an alias of
  `task.RelocationConfirmation`), the placement helper and the claim flow;
  everything type-specific is separate.
- Tests: domain (`replenishment_test.go`), use cases (`replenish_task_test.go`, with
  the one-unit-of-work and rollback checks), WorkReleased decode and the pairing rule
  on the wire (`consumer_replenishment_test.go`), the golden wire shapes of
  `TaskCompleted` and `TaskAvailable` and a decode through a field-for-field copy of
  inventory-storage's decoder and required-field rules (`replenishment_wire_test.go`),
  REST and MCP completion, and, against Postgres and Kafka (testcontainers), the
  migration up and down, the persistence round trip, the atomicity of completion and
  creation with their outbox rows, and the released-work-to-published-`TaskCompleted`
  path.
- Out of scope: consuming `InterleavingHintIssued` and the claimNext tie-break
  (P3c); producing the release (wes-work-planning, P3b); the process-path-management
  catalogue entry `{Id: "REPLENISH", MatchPrefix: "replenish", Direct: true,
  RequiredCapabilities: ["replenish"]}` (infrastructure configuration; this repo's
  catalogue fixtures know the path); priority-ordered claiming; partial-quantity
  follow-up work when the confirmed count is below the planned one (pick-face-replenishment
  re-detects the shortfall from the ledger).
