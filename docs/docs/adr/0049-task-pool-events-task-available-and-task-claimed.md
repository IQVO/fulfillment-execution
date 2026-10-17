---
id: 0049-task-pool-events-task-available-and-task-claimed
slug: /adr/0049-task-pool-events-task-available-and-task-claimed
title: 49. TaskAvailable and TaskClaimed tell consumers what is in the open-task pool
sidebar_label: "49. Task pool events"
description: "ADR 0049 — fulfillment-execution publishes TaskAvailable (a task entered or re-entered the pool) and TaskClaimed (it left) on warehouse.fulfillment.events, from the same transaction as the state change through the outbox; priority_band is derived from the real claimNext ordering key; only tasks with a determinable location are announced."
---

# 49. TaskAvailable and TaskClaimed tell consumers what is in the open-task pool

## Status

Accepted (2026-10-17). Additive: no existing event, endpoint or payload changes.
Consumer-first: task-interleaving (its ADR 0003) already decodes these two payloads.

## Context

task-interleaving keeps a local copy of the open-task pool, fed from
`warehouse.fulfillment.events`, and proposes the next task to a station that
has just finished one. Until now this service published no event when a task
entered the pool, and nothing it published carried a location, so the copy
stayed empty and no hint was ever issued. The only pool fact on the wire was the
analytics-topic `TaskClaimed` (`{task_id, task_type, station_id}`), which
consumers of the integration topic cannot read.

## Decision

Two integration events on `warehouse.fulfillment.events`, CloudEvents 1.0, Kafka
key = `task_id`, `source` `/warehouse/fulfillment-execution`, `dataschema`
`urn:warehouse:fulfillment-execution:events:<Type>:v1`, type prefix
`com.warehouse.wes.fulfillment-execution.task.`, built by the same Encoder as
`TaskCompleted` ([ADR-0032](./0032-cloudevents-mandatory-envelope.md)).

| event | `data` |
|-------|--------|
| `TaskAvailable` | `task_id`, `task_type`, `site_id`, `from_location`, `to_location`, `required_capabilities[]`, `cpt?`, `priority_band`, `available_at` |
| `TaskClaimed` | `task_id`, `task_type`, `station_id`, `claimed_at` |

### Which path raises what

| path | domain events raised | integration rows |
|------|----------------------|------------------|
| task created from `WorkReleased` (`ApplyWorkReleased` → `CreateTask.ExecuteRelease` / `ExecuteRelocation`) | `TaskCreated`, `TaskAvailable` at the creation instant | `TaskAvailable` (if the task has a location, see below) |
| task created through the API / MCP / the re-bin consolidation (`CreateTask.Execute`, `ExecuteTransfer`, the nested create in `ArriveAtRebin`) | `TaskCreated`, `TaskAvailable` | `TaskAvailable` (if located) |
| lease-expiry sweep (`ExpireLeases`) returns a claimed task to Pending | `LeaseExpired`, `TaskAvailable` at the sweep instant (a new `available_at`) | `TaskAvailable` (if located) |
| `ClaimNext` wins the compare-and-set | `TaskClaimed` at the claim instant | `TaskClaimed`, next to the unchanged analytics `TaskClaimed` |

Not raised: a replayed `WorkReleased` (the processed-event mark short-circuits
the whole unit of work), a rejected release, a failed create, an empty pool, a
capability mismatch, a claim that loses the race, `RenewLease`, any completion
(`TaskCompleted` is unchanged), the sweeps that do not free a task, and a second
sweep over a task already freed. A claim that re-claims a task whose lease
expired before the sweep ran raises only `TaskClaimed`: the task never
re-entered the pool through the sweep. The sweep cadence bounds how long a
stale claim stays in a consumer's copy; a later `TaskClaimed` still removes it.

### Same transaction

Both events go through the transactional outbox ([ADR-0020](./0020-transactional-outbox.md)),
in the use case's own unit of work, after the repository write. The Encoder
resolves the payload from the task row inside that transaction, so it sees the
just-saved state, and the task row, the analytics rows and the integration rows
commit or roll back together. There is no second publish path. An integration
test pins the rollback.

`TaskClaimed` has two rows because it is two Encoders over one domain event
(integration and analytics), each minting its own CloudEvents `id`, exactly as
`TaskCompleted` does today.

### `from_location`, `to_location`, `site_id`

This service holds a location for one kind of task only: a RELOCATE task
carries the planned bins of its slot-plan move ([ADR-0047](./0047-relocate-tasks-execute-slot-plan-moves.md)).
A PICK has no bin here (the release carries none; the reservation lives in
inventory-storage), and a PACK / SLAM / REBIN / transfer task is not tied to a
station until it is claimed. Calling another context for a bin is banned, so:

| task type | `from_location` | `to_location` | `site_id` |
|-----------|-----------------|---------------|-----------|
| RELOCATE, kind `Relocate` | `from_bin` | `to_bin` | site of the bin |
| RELOCATE, kind `Assign` (source scanned at completion) | `to_bin` | `to_bin` | site of the bin |
| RELOCATE, kind `Vacate` (destination scanned at completion) | `from_bin` | `from_bin` | site of the bin |
| PICK, PACK, SLAM, REBIN, DISPATCH, ARRIVAL | none known | none known | none |

The site is the first segment of the bin's facility-layout `LocationCode`
(`site-area-zone-aisle-bay-level-position`, seven hyphen-joined segments), the
same string rule inventory-storage applies to a bin id
([its ADR 0039](https://github.com/IQVO/inventory-storage)). A bin that is not
a well-formed code has no site.

**A task with no determinable location or site is not announced.** The consumer
rejects a `TaskAvailable` without `site_id` and `from_location`, so publishing
one would only add a poison message. The task is still created, claimed and
completed as before; it simply never enters task-interleaving's pool. When a
later change gives PICK a bin, or PACK / SLAM / REBIN a station location, the
event starts being published for them with no contract change.

### `required_capabilities`

The capabilities `claimNext(stationId, taskType)` matches a station against
(`Task.RequiredCapabilities`, `Capabilities.HasAll`), sorted, as a JSON array.
Empty array, never `null`, when the task requires none.

### `cpt`

The task's CPT as UTC RFC 3339; omitted when the task has none.

### `priority_band`

`claimNext` scans the claimable tasks **of one task type** ordered by CPT
ascending and nothing else (`ORDER BY cpt ASC`; ties are returned in no defined
order), then takes the first one the station's capabilities satisfy. The
ordering key is therefore (task type, CPT instant). Tasks with the same type and
the same instant are peers (either may be picked first); a different instant is
strictly ordered; a different type is never compared. The band is
`<task_type>:<CPT in UTC, RFC 3339 with nanoseconds as needed>`, for example
`RELOCATE:2026-09-30T16:00:00Z`. Two representations of one instant yield one
band. It is opaque: consumers compare it for equality and never parse it. If
`claimNext` ever orders by more than CPT, the band changes first, in a new
dataschema version if the shape of the key does.

### `available_at`

The instant the task became claimable: the creation time on release, the sweep
time on re-entry after a lease expiry.

## Consequences

- task-interleaving can fill its pool for RELOCATE work today. Pool coverage for
  PICK / PACK / SLAM / REBIN depends on this service gaining a location for
  them, which is deliberately not invented here.
- Every claim of any task type now writes one more outbox row on
  `warehouse.fulfillment.events`; consumers that dispatch on the full type
  ignore it ([ADR-0032](./0032-cloudevents-mandatory-envelope.md) §5).
- The `TaskAvailable` payload is resolved at encode time from the task row; a
  task that cannot be found yields no message.
- Out of scope: the REPLENISH task type, consuming `InterleavingHintIssued`, the
  hint tie-break inside `claimNext`, and any REST change.
