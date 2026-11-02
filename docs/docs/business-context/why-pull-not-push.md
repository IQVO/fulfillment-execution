---
id: why-pull-not-push
title: Why pull, not push
sidebar_label: Why pull, not push
sidebar_position: 3
description: The defining design rule of this bounded context — a station claims the next task, the system never names a station in advance — and the reasoning behind it.
---

# Why pull, not push

> **The defining design rule:** a station claims the next task —
> `claimNext(stationId, capabilities)`. The system selects work, not workers.
> There is no `assign(task, station)` operation.

This is the single most consequential decision in the context, and it is worth
separating the *what* from the *why*, because the mechanism is simple and the
reasoning is not.

## What push would look like

A push model has an optimiser that periodically scans the pool and the station
list, computes an allocation, and writes an assignment onto each task:
`assign(task-8a1f, station-03)`. Station 03's screen then displays task 8a1f.
This is the classic `Assignment` aggregate from the reference model — an
ephemeral binding of `Task → Resource → Time`, recomputed continuously.

It is a completely legitimate design, and the reference material describes it
in detail. This service does not use it.

## Why this context pulls instead

### 1. The plan is stale the moment it is written

Assignment decisions are made against a snapshot: which stations are free,
where people are, what the congestion looks like. On a warehouse floor that
snapshot is out of date within seconds. An associate takes an unscheduled
break, a tote jams, a station's scanner fails, someone is pulled to a hot
order. Every one of those events invalidates part of the allocation.

A push model responds by *recomputing continuously* — which means it needs a
near-real-time, cross-aggregate view of the entire task pool and the entire
resource pool simultaneously, plus congestion state. That is a genuinely hard
optimiser and, more importantly, a large amount of state this context would
have to own and keep fresh.

A pull model dissolves the problem. The decision is made at the *moment of
demand*, by the only party that knows for certain a station is free: the
station itself. There is no plan to invalidate because there is no plan.

### 2. Pull makes "who is free" self-reporting, not inferred

For a push model to be correct it must know station availability. Getting that
wrong in either direction is expensive: assign to a station that is not
actually free and the task sits idle; fail to assign to a station that *is*
free and you have paid an associate to stand still.

Under pull, availability is not modelled at all. A station that calls
`claimNext` is, by construction, free. This is why the endpoint is
`POST /stations/{stationId}/task-claims` and not a query — the call *is* the
declaration of availability.

### 3. It keeps this context out of the workforce business

A push optimiser needs to know about workers: skills, certifications, current
zone, shift window. The moment this context models those, it has absorbed a
Supporting subdomain (labour orchestration) into a Core one, and every labour
policy change forces a regression here.

Under pull, this context needs exactly one fact about the puller: its
**capability set**, held on the `Station` aggregate. It never learns who is
standing there. `Station.occupant` is an opaque `OccupantId` with no roster
behind it. That is what keeps the boundary with `workforce-management` — which
stops deliberately at the process-path boundary and never dispatches
individual tasks — clean in both directions.

### 4. Backpressure comes for free

If Pack stations are saturated, they stop calling `claimNext`, and Pack queue
depth rises. `GET /queues/PACK/depth` reports that rise immediately, and it is
exactly the buffer telemetry `wes-work-planning` flow-balances on.

Under push, a saturated station keeps receiving assignments; the backlog hides
*inside* the assignments rather than showing up in the queue. You would need a
separate mechanism to detect it.

### 5. The failure mode is bounded

If a pulling station dies, exactly one task is affected, and the lease returns
it to the pool within the lease window. If a push optimiser dies, nothing new
is assigned at all and the floor stops. Pull degrades gracefully; push has a
single point of failure with a floor-wide blast radius.

## What pull costs

Being honest about the trade-off, because it is real:

- **No global optimisation.** Pull is greedy: each station takes the most
  urgent thing *it* can do. Interleaving (combining a putaway and a pick on
  one walk) arrives only as an *advisory* signal: since
  [ADR-0051](../adr/0051-interleaving-hints-advise-claimnext.md) this context
  consumes task-interleaving's `InterleavingHintIssued` and lets it break
  ties **inside one priority band** — it never reorders across a CPT
  boundary, so the greedy policy stays intact and an absent hint costs
  nothing. There is still no travel-path optimisation across stations and no
  batching by aisle. A push optimiser can, in principle, beat greedy dispatch
  on total travel distance. This context accepts a locally-greedy allocation
  in exchange for having no stale plan to maintain.
- **Selection quality depends entirely on ordering.** With no optimiser, the
  ordering `TaskRepo.FindClaimableByType` returns *is* the policy. Today that
  is earliest-CPT-first, and any future sophistication has to live there.
- **A station must be registered before it can pull.** `claimNext` resolves
  the caller's capabilities from the persisted `Station`. This is why
  `POST /stations` exists — it was added specifically because a freshly
  started server returned "station not found" for every claim and the
  pull-dispatch flow could not be exercised over HTTP at all.

## How the rule shows up in the code

The rule is not a convention that reviewers have to remember — it is visible
in the shape of the API and the domain:

- `internal/application/usecases/` contains `claim_next.go`. There is no
  `assign_task.go`, and no use case takes both a task id and a station id as
  *inputs to a selection decision*. `RenewLease` and `CompleteTask` take both,
  but only to *verify ownership of an existing claim*.
- The `Task` aggregate has no `assignedTo` field. It has a `*Lease`, which is
  only ever set by `Claim`.
- The router has `POST /stations/{stationId}/task-claims` — the station is the
  resource being acted upon, and the task is the *result*. A push design would
  have `PUT /tasks/{id}/assignee`.
- `ClaimNext.Execute` loops over CPT-ordered candidates and takes the first
  one `Task.Claim` accepts. The station is never chosen — only the task is.

The full decision record, with the alternatives that were weighed, is
[ADR-0002](../adr/0002-pull-based-claimnext-dispatch.md).
