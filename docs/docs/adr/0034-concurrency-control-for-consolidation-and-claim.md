---
id: 0034-concurrency-control-for-consolidation-and-claim
slug: /adr/0034-concurrency-control-for-consolidation-and-claim
title: 34. Optimistic/pessimistic concurrency for consolidation and claim CAS
sidebar_label: 34. Concurrency control
description: "ADR 0034 — records the two database-level concurrency controls this service relies on: the compare-and-set UPDATE behind at-most-once claiming (SaveClaim, optimistic) and the per-order advisory lock + SELECT … FOR UPDATE behind Rebin order consolidation (FindByOrderRefForUpdate, pessimistic)."
---

# 34. Optimistic/pessimistic concurrency for consolidation and claim CAS

## Status

Accepted. Records `SaveClaim` (already shipped, previously without an ADR) and
adds the consolidation lock. Refines the "at-most-once" mechanics described in
[ADR-0003](./0003-lease-based-at-most-once-claiming.md) and the fan-in tracker
of [ADR-0016](./0016-rebin-and-order-consolidation.md).

## Context

Two read-modify-write paths run concurrently across replicas (HPA scale-out,
[ADR-0030](./0030-horizontal-autoscaling-and-pgxpool-tuning.md)), and the
domain methods alone cannot make either safe, because each replica works on
its own in-memory copy of the aggregate:

1. **Claiming.** `ClaimNext` reads claimable candidates with a plain `SELECT`
   (`FindClaimableByType`), so several stations can load the same `PENDING`
   task at the same instant. `Task.Claim` is correct per instance, but two
   instances can both succeed and both `Save`.
2. **Rebin consolidation.** `ArriveAtRebin` loaded the `OrderConsolidation`,
   recorded one line, and `Save` upserted the whole row
   (`ON CONFLICT … DO UPDATE SET arrived_lines = EXCLUDED.arrived_lines`) —
   last writer wins. Two lines of one order arriving together both read the
   same state; the second upsert erased the first's arrival, so the order
   could stay incomplete forever, and with a different interleaving the PACK
   task could be created more than once.

## Decision

Use the database as the arbiter, with the control matched to the shape of the
contention:

- **Claim — optimistic compare-and-set (`TaskRepo.SaveClaim`).** The `UPDATE
  tasks … WHERE id = $1 AND (status = 'PENDING' OR (status = 'CLAIMED' AND
  lease_expiry <= $now))` only matches while the row is still claimable.
  `RowsAffected() == 1` means this station won; `0` means another claim got
  there first and `ClaimNext` moves to the next candidate. No lock is held
  between the read and the write; losers simply retry on the next candidate.
  Contention is on *many tasks, few claimants each*, where retrying is cheap.
- **Consolidation — pessimistic per-order lock
  (`OrderConsolidationRepo.FindByOrderRefForUpdate`).** `ArriveAtRebin` now
  runs its whole read-modify-write inside the `UnitOfWork`
  ([ADR-0020](./0020-transactional-outbox.md)) and reads through
  `FindByOrderRefForUpdate`, which, inside a transaction, executes
  `pg_advisory_xact_lock(hashtextextended(order_ref, 0))` and then
  `SELECT … FOR UPDATE`. The advisory lock is what covers the *first* arrival
  (there is no row yet for `FOR UPDATE` to lock); the row lock covers any other
  writer afterwards. Both are released at commit/rollback, and under
  `READ COMMITTED` the read after the lock sees the previous holder's committed
  arrival. Contention is on *few orders, several lines each, and the
  completing arrival has side effects* (PACK task creation, events), where a
  CAS-and-retry would have to redo and undo those effects.

The in-memory adapters (`memory.OrderConsolidationRepo`, used with no
`UnitOfWork` for single-process dev and unit tests) implement
`FindByOrderRefForUpdate` as a plain read: they have no transaction scope to
hold a lock in.

Verified by testcontainers integration tests:
`TestOrderConsolidation_ConcurrentArrivals_NoLostLineAndSinglePackTask`
(8 goroutines arriving for one order → all 8 lines recorded, exactly one PACK
task) and the existing `claim_cas_integration_test.go` (N stations racing for
one task → exactly one winner).

## Consequences

### Easier

- Concurrent rebin arrivals cannot lose a line or double-create the PACK task,
  on any number of replicas.
- Claim stays lock-free and fast; the loser path is a normal control-flow
  branch, not an error.

### Harder

- Arrivals for the *same order* are serialized through one lock; a slow
  transaction (e.g. a slow outbox insert) delays that order's other lines.
  Different orders do not contend.
- The lock is only effective inside a `UnitOfWork`; calling
  `FindByOrderRefForUpdate` outside one degrades to a plain read. The use case
  always wraps it.
- A transaction holds a pooled connection while waiting for the lock, so a
  burst of arrivals for one order can occupy several connections for the
  duration of one transaction (bounded by the pool settings of
  [ADR-0030](./0030-horizontal-autoscaling-and-pgxpool-tuning.md)).
- The advisory-lock key is a 64-bit hash of `order_ref`; a hash collision only
  causes two orders to serialize against each other, never incorrect results.
