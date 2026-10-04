---
id: 0003-lease-based-at-most-once-claiming
title: 3. Lease-based at-most-once claiming over a hard lock
sidebar_label: 3. Lease-based claiming
sidebar_position: 3
description: A claim is a time-boxed lease. An unconfirmed claim returns to the pool rather than vanishing — chosen over a hard lock or an unbounded assignment.
---

# 3. Lease-based at-most-once claiming over a hard lock

## Status

**Accepted.** Stated in `CLAUDE.md`: *"Assignment is at-most-once with a
**lease** so an unconfirmed task returns to the pool rather than vanishing,"*
and *"**Lease** — a claim has a timeout; if not confirmed/completed before
expiry the task returns to the pool (prevents vanished work). Lease renewal is
allowed."*

## Context

[ADR-0002](./0002-pull-based-claimnext-dispatch.md) established that stations
pull. That immediately raises two questions this decision has to answer
together.

**First: what stops two stations claiming the same task?** Under pull,
concurrent `claimNext` calls will select the same earliest-CPT candidate. Two
associates walking to the same tote is a real, visible, expensive operational
failure — it wastes a walk and produces a duplicate pick that then has to be
unwound.

**Second: what happens when a claiming station never comes back?** This is the
harder question, and the reference model's own vocabulary hints at the answer
by describing an `Assignment` as carrying `AssignedAt, ExpiresAt/ReassignedAt`
— the binding is expected to be *temporary*.

The forces:

- **Claimants are physical and unreliable.** A station is a human or a robot
  with a scanner. Scanners die mid-pick. Associates go on break with a task
  open. Pick-to-light stations reboot. Network drops. None of these are
  exceptional; over a shift they are routine.
- **Silently stranded work is the worst outcome.** A task the system believes
  is in progress and that nobody is doing is invisible. It does not appear in
  queue depth, nothing escalates it, and nobody notices until the order misses
  its CPT. `CLAUDE.md` names this directly: the lease exists to *prevent
  vanished work*.
- **Long work is legitimate.** A deep-aisle pick, an awkward carton, a
  hazmat handling step. Any timeout short enough to detect abandonment
  quickly is also short enough to expire honest, in-progress work.
- **The service must run without Postgres.** In-memory adapters are a
  first-class configuration for local runs and tests, so the mechanism cannot
  depend on a database primitive.
- **Correctness must be testable without sleeping.** A test for expiry that
  waits five real minutes will not be run.

### Alternatives considered

**A database row lock (`SELECT … FOR UPDATE`) held for the duration of the
work.** Mutual exclusion is exact, but the lock's lifetime is a transaction's
lifetime, and the work here takes minutes of *physical* activity. Holding a
transaction open across a human walking down an aisle is not viable — and if
the connection drops, the lock releases silently with the task still marked as
in progress. It also fails outright under the in-memory adapter and couples a
domain guarantee to a storage engine.

**An unbounded claim with manual release.** Simple, and exactly the vanished-work
failure mode: an abandoned claim persists until a human notices and intervenes.

**A short timeout with no renewal.** Detects abandonment fast, but expires
honest long work, causing the same duplicate-pick problem it was meant to
prevent — from the opposite direction.

**A heartbeat/keepalive from the station.** Effectively renewal, but continuous
and implicit. It requires client-side background machinery and produces
constant traffic, and an idle-but-alive client keeps a task it is not working.

## Decision

**We will make a claim a time-boxed lease, renewable by its owner, and
automatically returned to the pool on expiry.**

- `task.Lease{StationId, Expiry}`, at most one per task, set only by
  `Task.Claim`.
- Default duration **5 minutes** (`usecases.DefaultLeaseDuration`), overridable
  per use case at the composition root.
- **Renewal is a first-class operation**, not an escape hatch:
  `POST /tasks/{id}/renew-lease`, owner-only, extending the expiry from *now*.
  This is how legitimately long work is handled — rather than by picking one
  timeout long enough for the worst case.
- **Only the owner may renew or complete.** Anyone else gets
  `task.ErrNotOwner`.
- **Expiry is evaluated in two complementary places**, and both are required:
  - **Lazily, at the point of use.** `Claim` calls `ExpireLeaseIfDue(now)`
    *before* checking whether the task is already claimed; `RenewLease` and
    `Complete` check `lease.expired(now)` and reject with `ErrNotClaimed`. A
    stale claim therefore cannot block a fresh claim even if no sweep has run.
  - **Eagerly, via a sweep.** `POST /tasks/expire-leases` runs the
    `ExpireLeases` use case over every `Claimed` task, frees the lapsed ones,
    publishes `LeaseExpired` for each, and returns the count freed. This is
    what makes freed work *visible* in the queue-depth read model rather than
    only becoming visible the next time somebody happens to pull.
- **`now` is always a parameter**, taken from `ports.Clock` at the application
  layer. No domain method calls `time.Now()`.

The ordering inside `Claim` is itself part of the decision:

```go
if t.status == Completed { return ErrAlreadyCompleted }
t.ExpireLeaseIfDue(now)                    // expiry BEFORE the claimed check
if t.status == Claimed   { return ErrAlreadyClaimed }
if !stationCapabilities.HasAll(t.requiredCapabilities) { return ErrCapabilityMismatch }
```

Reverse those two middle lines and a stale claim blocks the task permanently —
reintroducing exactly the failure the lease exists to prevent. This is pinned
by a failing-path test rather than left to code review.

## Consequences

### Easier

- **Work cannot vanish.** Every abandonment path — dead scanner, walked-away
  associate, crashed client, network partition — resolves itself within the
  lease window with no human intervention. This is the whole point.
- **At-most-once holds without a distributed lock.** The *rule* lives in
  `Task.Claim`, in the domain, so it behaves identically under Postgres and
  in-memory adapters; the *cross-process atomicity* comes from the
  compare-and-set `UPDATE` in `TaskRepo.SaveClaim` (`WHERE status = 'PENDING'
  OR (CLAIMED AND lease_expiry <= now)`), which only one of several racing
  stations can win — see
  [ADR-0034](./0034-concurrency-control-for-consolidation-and-claim.md).
- **Long work has an honest mechanism.** Renewal means the timeout is tuned
  for *detection latency*, not for the longest conceivable task.
- **Recovery is observable.** `LeaseExpired` is a real domain event and the
  sweep returns a count, so "how much work is being abandoned" is a
  measurable number rather than folklore.
- **Deterministically testable.** A fixed `Clock` advanced past the expiry
  proves the whole behaviour in microseconds.

### Harder

- **Abandonment detection is delayed by up to the lease duration.** With a
  5-minute default, a task dropped at second zero is unavailable for nearly
  five minutes. Shortening the lease shortens that delay and increases renewal
  traffic — a real, permanent tuning trade-off with no correct universal
  answer.
- **Clients must renew, or be short.** A station holding a task longer than
  the lease without renewing will lose it and get `ErrNotClaimed` on
  completion. The client contract is now stateful in a way a plain assignment
  would not be.
- **Duplicate physical work is possible, not merely theoretical.** If a
  station is genuinely still working when its lease lapses, another station can
  claim the same task. The lease bounds the *system's* view, not the physical
  world. Renewal is the mitigation, and the residual risk is accepted.
- **The sweep needs a caller.** `POST /tasks/expire-leases` is exposed as an
  endpoint but nothing schedules it inside this service — an external scheduler
  (cron, a Kubernetes `CronJob`) has to invoke it. Lazy evaluation means
  correctness does not depend on that, but *visibility* in the queue-depth read
  model does.
- **Two evaluation paths must stay consistent.** Lazy checks and the eager
  sweep both implement expiry semantics. They agree today because both funnel
  through `ExpireLeaseIfDue` / `lease.expired`, and that funnel has to be
  preserved.
- **`ExpireLeases` scans all claimed tasks.** `FindAllClaimed` is an
  unindexed-by-expiry full scan of the claimed set. Fine at current scale;
  it would need an expiry index to scale.
