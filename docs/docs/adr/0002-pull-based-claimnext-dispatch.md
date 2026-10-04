---
id: 0002-pull-based-claimnext-dispatch
title: 2. Pull-based claimNext dispatch over push assignment
sidebar_label: 2. Pull-based claimNext dispatch
sidebar_position: 2
description: A station claims the next task; the system selects work, not workers. There is deliberately no assign(task, station) operation.
---

# 2. Pull-based `claimNext` dispatch over push assignment

## Status

**Accepted.** This is the defining design rule of the bounded context, stated
in `CLAUDE.md`: *"The defining design rule is **pull, not push**: a station
claims the next task (`claimNext(stationId, capabilities)`); the system selects
work, not workers."*

## Context

Something has to decide which unit of work happens at which station next. The
reference model (`warehouse-systems-ddd.md`) describes the conventional
answer in detail:

> Owns an **`Assignment` aggregate** — an *ephemeral* binding of
> `Task → Resource → Time`. Recomputed continuously based on: worker
> proximity, travel-path optimization, aisle congestion, equipment health,
> interleaving opportunities.

and further notes that the matching logic is best modelled as a **domain
service** rather than a method on `Assignment`, "because it needs a
cross-aggregate, near-real-time view of the whole `TaskPool` and
`ResourcePool` simultaneously."

That last sentence is the crux of the problem, and it names the cost precisely:
a push design requires this context to hold and continuously refresh a
near-real-time model of every station and every worker.

The forces:

- **Warehouse floor state is volatile at second granularity.** Associates take
  unscheduled breaks, scanners fail, totes jam, people are pulled onto hot
  orders. Any allocation computed from a snapshot is partly wrong before it is
  written.
- **Station availability is expensive to know and costly to get wrong.**
  Assign to a station that is not free and the task idles. Fail to assign to a
  station that is free and you pay someone to stand still.
- **A push optimiser needs worker knowledge.** Skills, certifications, current
  zone, shift window. Every one of those belongs to `workforce-management`,
  which is a *Supporting* subdomain and which explicitly "stops at the path
  boundary" and "never links an associate to a specific task."
- **Backpressure must be observable.** `wes-work-planning` flow-balances on
  live buffer telemetry, and this service's per-path queue depth is that
  telemetry.
- **Failure blast radius matters.** A dispatch mechanism that stops the whole
  floor when it fails is a different operational risk from one that affects a
  single task.
- **Global optimisation has real value.** Interleaving a putaway with a pick
  on one walk, batching by aisle, minimising travel — a push optimiser can, in
  principle, beat greedy dispatch on total travel distance. This is a genuine
  point in push's favour and is not dismissed.

## Decision

**We will dispatch by pull. A station asks for its next task; the system never
names a station in advance.**

The only dispatch operation is:

```
claimNext(stationId, capabilities) -> the highest-priority (earliest CPT)
                                      pending task this station is
                                      certified and equipped for
```

exposed as `POST /stations/{stationId}/claim-next`.

**There will be no `assign(task, station)` operation** — not as a use case, not
as an endpoint, not as a field on the aggregate. Specifically:

- `internal/application/usecases/` contains `claim_next.go` and no
  `assign_task.go`. No use case takes a task id and a station id as inputs to
  a *selection* decision. `RenewLease` and `CompleteTask` take both, but only
  to verify ownership of a claim that already exists.
- `task.Task` has no `assignedTo` field. It has a `*Lease`, set only by
  `Claim`.
- The URL shape reflects the semantics: the **station** is the resource being
  acted on, and the task is the *result*. A push design would have
  `PUT /tasks/{id}/assignee`.

Selection policy: `TaskRepo.FindClaimableByType` returns claimable tasks of the
requested type **ordered earliest-CPT-first**, and `ClaimNext` takes the first
one whose required capabilities the station satisfies — skipping past
non-matching candidates rather than failing on them. Priority derives entirely
from CPT; there is no separate priority field that could drift from the
deadline that actually matters.

Capabilities are resolved **server-side** from the persisted `Station`, not
taken from the request body. The client sends only `taskType`. A station
cannot claim work by asserting capabilities it does not have.

## Consequences

### Easier

- **No plan to invalidate.** The decision is made at the moment of demand, by
  the only party that knows for certain a station is free — the station
  itself. Volatile floor state stops being a correctness problem.
- **Availability is self-reporting.** A station that calls `claimNext` is, by
  construction, free. The model does not need to track availability at all,
  which is why `ClaimNext` does not consult `Station.occupant`.
- **The workforce boundary stays clean.** This context needs exactly one fact
  about the puller — its capability set — and never learns who is standing
  there. `Station.occupant` is an opaque `OccupantId` with no roster behind
  it. Labour policy can change without touching this service.
- **Backpressure is free and visible.** Saturated stations stop calling
  `claimNext`; queue depth rises; `GET /queues/{taskType}/depth` reports it
  immediately. Under push the backlog would hide *inside* the assignments.
- **Bounded failure.** A dead station affects exactly one task, returned to
  the pool by the lease. A dead push optimiser stops the floor.
- **Small, testable dispatch code.** `ClaimNext.Execute` is roughly twenty
  lines with no optimiser, no snapshot, and no scheduler.

### Harder

- **No global optimisation. Dispatch is greedy.** Each station takes the most
  urgent thing *it* can do. There is no interleaving, no travel-path
  optimisation across stations, no aisle batching. Total travel distance is
  accepted as suboptimal in exchange for having no stale plan to maintain.
  This is the real cost of the decision.
- **Selection quality lives entirely in one query.** With no optimiser, the
  ordering `FindClaimableByType` returns *is* the policy. Any future
  sophistication has to be expressed there — which is at least the right
  place, but it means the repository holds domain-significant behaviour.
- **Head-of-line effects are possible.** If the earliest-CPT task requires a
  rare capability, stations without it skip past it repeatedly. Nothing
  escalates or reserves it; it simply waits for a capable station to pull.
- **A station must be registered before it can pull.** `claimNext` resolves
  capabilities from the persisted `Station`, so a fresh server returns
  "station not found" for every claim. This was discovered during real
  end-to-end smoke testing and is exactly why `POST /stations` and the
  `RegisterStation` use case were added — the gap made the pull-dispatch flow
  impossible to exercise over HTTP at all.
- **Clients must poll.** There is no push notification to an idle station; it
  calls `claim-next` and may get `409 no-claimable-task`. That status is
  deliberate — the pool is a valid resource momentarily empty of work this
  station can do, not a missing one — but it does mean a polling client.
- **Concurrent claims race, and must.** Two stations calling `claimNext`
  simultaneously may both select the same earliest-CPT candidate. Correctness
  therefore depends on the at-most-once guarantee: the domain rule in
  `Task.Claim`, made atomic across processes by the compare-and-set
  `TaskRepo.SaveClaim` ([ADR-0003](./0003-lease-based-at-most-once-claiming.md),
  [ADR-0034](./0034-concurrency-control-for-consolidation-and-claim.md)).
