---
id: 0063-work-batches-formed-at-assignment-behind-a-mode-gate
slug: /adr/0063-work-batches-formed-at-assignment-behind-a-mode-gate
title: 63. Work batches form at assignment time, behind a mode gate
sidebar_label: "63. Work batches at assignment"
description: "ADR 0063 — a WorkBatch is the PICK tasks one station holds for one walk, formed at assignment time when the station asks for work: the anchor is exactly the task ClaimNext would return (shared claimer), companions are hinted same-CPT-band candidates in rank order filtered by capability and cart segregation, everything behind PICK_BATCH_MODE off|shadow|on with the fleet default off; TaskCompleted gains optional batch_id/batch_size and three WorkBatch* events go to both topics."
---

# 0063. Work batches form at assignment time, behind a mode gate

## Status

Proposed (2026-11-08). Pick-batching wave 3; consumes ADR 0062 (PICK
announcement) and task-interleaving's hints (ADR 0051).

## Context

ADR-0002 gave up travel batching on purpose and said it would have to happen
downstream — nothing has. Waveless release means nothing waits for a wave,
but a picker still walks one task at a time: claim, walk, pick, return. The
pick-batching plan (2026-11-05) settled the owner question (D1): assignment
happens in this context, so the batch forms here, at the moment a station
asks for work. A `WorkBatch` is not a Wave — nothing waits for a batch to
fill, a batch of one is valid, and `ClaimNext` / `POST .../task-claims`
stay exactly as they are.

The plan pinned the rest: same CPT instant only (D3, so a batch can never
delay an earlier cutoff), maximum 4 as a placeholder setting (D4), stops in
hint rank order (D6), and a rollout gate `PICK_BATCH_MODE` = off → shadow →
on (D5) because the benefit is unproven — no reliable saving figure exists
and the WIP limit may keep the pool too small to batch.

The load-bearing guarantee is **anchor equivalence**: whatever batching does,
it must never change *who goes first*. A station that would have received
task X from `ClaimNext` must receive a walk anchored on X.

## Decision

1. **`internal/domain/workbatch`** owns the aggregate. `Form` enforces the
   model's formation invariants — size in 1..maximum, every member a PICK
   with a pick face, every member in the anchor's CPT band, hazard classes
   compatible under the existing DOT segregation matrix — and assigns
   `stop_seq`/`compartment` in rank order. The aggregate is event-free; the
   application layer publishes. Member lifecycle: OPEN → COMPLETED
   (CompleteMember; the last one flips the batch to COMPLETED) and OPEN →
   DETACHED (DetachMember, idempotent, when the sweep returns an expired
   member to the pool; the last one ABANDONs the batch).

2. **`FormWorkBatch`** (application) shares the claimer with `ClaimNext`
   (`claim_core.go`): the anchor is selected by the *same code path*
   ClaimNext uses, so anchor equivalence is structural, not re-implemented.
   Companions come from the station's active interleaving hint, in rank
   order, kept only when still claimable, in the anchor's band, carrying a
   pick face, within the station's capabilities, and segregation-safe;
   each is claimed through the existing per-task compare-and-set (losing
   one to another station is a skip, never an error). An anchor with no
   pick face is claimed as an ordinary single task and no batch forms
   (ADR 0062). A station with an OPEN batch gets `ErrStationHasOpenBatch`.

3. **The gate comes first.** With `PICK_BATCH_MODE` not `on` (fleet default
   `off`), the use cases answer `ErrBatchModeOff` (503 at the edge), no
   batch row is written, no new event is published, and every existing
   suite runs unchanged. A typo in the value is a boot error, not a silent
   default. `shadow`'s compute-only path is wave 4.

4. **Leases stay per task; renewal is per batch.** `POST
   /work-batches/{batchId}/lease-renewals` renews every still-open member
   (LeaseTogether) so later stops of a long walk cannot expire mid-walk; an
   expired member detaches and the existing sweep returns the task to the
   pool unchanged.

5. **Contracts are additive.** REST: the three routes above (plus `GET
   /work-batches/{batchId}`). Events: `WorkBatchFormed`,
   `WorkBatchCompleted`, `WorkBatchAbandoned` on `warehouse.fulfillment.events`
   and their analytics occurrences on `warehouse.fulfillment.analytics`,
   type `com.warehouse.wes.fulfillment-execution.workbatch.<Event>`,
   CloudEvents 1.0 structured, keyed by batch id. `TaskCompleted` gains
   optional `batch_id`/`batch_size` (omit-when-not-batched, so the
   pre-batching payload bytes are identical); the publisher enriches them
   only when a task completed into a batch.

6. **Persistence** is two additive tables (`work_batches`,
   `work_batch_members`, migration 0027); the one-open-batch-per-station
   invariant is a partial unique index, so a lost formation race fails on
   the write exactly like the per-task claim compare-and-set.

## Consequences

- `labor-performance` can report batched and single picks apart from the
  `batch_id`/`batch_size` pair alone; standards stay frozen (ADR 0004).
- The walk's own length (`walk_seconds` on WorkBatchCompleted) is measured
  from formation; per-stop labor timing is ADR 0064's, not this one's.
- cmd/mcp gains the read-only `get_work_batch` tool over the same use case
  as the REST read (governance charter: tools call use cases).
- With mode `off` the rollback is nothing: the tables stay empty and the
  fleet behaves exactly as before this ADR.
- Wave 4 (shadow) decides whether `on` ever happens; if average batch size
  stays near 1, the plan says stop there.

## Addendum (2026-11-13): current state

Built and on `develop` (PR #280), behind `PICK_BATCH_MODE` (default `off`): the `WorkBatch` aggregate (`internal/domain/workbatch/workbatch.go:154`, `Form`), `FormWorkBatch` (`internal/application/usecases/form_work_batch.go:125`), tables from migration `0027_work_batch`, and `POST /stations/{stationId}/work-batches`. `WorkBatchFormed`, `WorkBatchCompleted` and `WorkBatchAbandoned` are published on both topics only with mode `on` (`internal/adapters/outbound/kafka/publisher.go:387`, `analytics_publisher.go:142`, `apis/asyncapi.yaml`). The `shadow` value parses, but `FormWorkBatch` answers `ErrBatchModeOff` for every mode except `on` (`form_work_batch.go:126`): the shadow compute-only path is not built. No sibling consumes the three events yet (`x-no-consumer-reason` in `apis/asyncapi.yaml`). The Status line above is unchanged.
