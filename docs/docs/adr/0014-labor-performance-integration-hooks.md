---
id: 0014-labor-performance-integration-hooks
title: 14. Labor-performance integration hooks — check-in/check-out wiring and TaskCompleted enrichment
sidebar_label: 14. Labor-performance integration hooks
sidebar_position: 14
description: Wires the already-modeled Station CheckIn/CheckOut domain methods to real REST endpoints, adds a Task claim-timestamp, and enriches the existing TaskCompleted Kafka event with the completing associate's identity and task duration — additive inputs for a new labor-performance bounded context.
---

# 14. Labor-performance integration hooks — check-in/check-out wiring and `TaskCompleted` enrichment

## Status

**Accepted.**

## Context

A new `labor-performance` bounded context is being built as a separate
service. It needs two facts this service does not currently expose for any
completed task: **who** (which associate) completed it, and **how long**
it took. Both facts already have almost everything they need already
modeled here, just not wired up or surfaced:

- `internal/domain/station/station.go` has a complete `Station` aggregate
  with `CheckIn(occupant OccupantId) error`, `CheckOut() error`,
  `Occupant()`, and `IsOccupied()` — `OccupantId` is documented as
  identifying "the worker or robot occupying a station." None of this is
  called from `internal/application/usecases/` or
  `internal/adapters/inbound/http/`. It is fully dead code: a station can
  be registered and claim tasks, but nothing can ever record who is
  standing at it.
- `internal/domain/task/task.go`'s `Task` has a `lease *Lease` (station +
  expiry) but no record of *when* the current claim started — only when it
  expires. Elapsed time-on-task (a natural "how long did this take"
  measure) cannot be computed from anything the aggregate exposes today.
- `TaskCompleted` (`internal/domain/shared/events.go`) carries only
  `TaskId` and `StationId`. The Kafka publisher
  (`internal/adapters/outbound/kafka/publisher.go`) already resolves one
  enrichment fact this way — `WorkUnitId`, via a `TaskRepo.FindById`
  lookup — because the domain event itself stays deliberately thin (see
  [ADR-0004](./0004-kafka-integration-events-and-envelope.md)). Associate
  identity and duration are the same shape of problem: facts this service
  can resolve at publish time from state it already owns, without
  widening the domain event.

The forces at play:

- **This service remains the sole owner of `Station` and `Task` state.**
  `labor-performance` should not need to query this service synchronously
  to learn who completed a task or how long it took — that would create a
  new inbound coupling in the wrong direction (a downstream context
  querying upstream state on its own schedule) instead of consuming a
  fact this service already publishes.
- **Not every station has a checked-in occupant.** Robot stations, or
  human stations where check-in was skipped, are legitimate states this
  service already tolerates (`Occupant()` returns `nil`). Associate
  identity has to be an optional, best-effort fact, not a hard
  requirement that would newly reject completions from occupant-less
  stations.
- **A task's claim can predate this change.** Any task claimed before a
  `claimedAt` column exists has no way to retroactively know when its
  claim started. Duration must degrade gracefully (to zero, not an error
  or a crash) for such tasks rather than assume the column is always
  populated.
- **`Claim` sets state once, at the moment of claiming; `Complete` reads
  it later.** The natural home for a claim-start timestamp is the same
  place the lease's start is implied (`Claim`), not something threaded in
  as a new parameter to `Complete` — the timestamp needs to exist before
  completion is even possible, exactly like the lease itself.
- **The existing repo-lookup-enrichment pattern already solves this
  shape of problem once** (`WorkUnitId` via `TaskRepo`). Reusing it for
  `AssociateId` (via a new `StationRepo` lookup) and `DurationSeconds`
  (via the already-loaded `Task`'s new `ClaimedAt()`) needed no new
  architectural pattern, just one more repo dependency on the publisher
  it already had a `TaskRepo` for.

## Decision

**Wire the existing `Station.CheckIn`/`CheckOut` domain methods to two new
REST endpoints via two new use cases, add a `claimedAt *time.Time` field
to `Task` (set once, inside the existing `Claim` method, with a
`ClaimedAt()` getter), and enrich `TaskCompletedData` (the Kafka
publisher's wire payload) with `AssociateId` and `DurationSeconds`,
resolved at publish time exactly the way `WorkUnitId` already is.**

### Check-in/check-out: application + HTTP only, no domain change

`Station.CheckIn`/`CheckOut` needed zero domain changes — they were
already correct, just unreachable. Two new use cases,
`CheckInStation`/`CheckOutStation`, mirror the existing simple use-case
shape (`RegisterStation`'s struct-with-ports pattern): load the station,
call the domain method, save. Two new routes,
`POST /stations/{stationId}/check-ins` and
`POST /stations/{stationId}/check-outs`, follow the existing chi
handler/DTO/RFC 7807 conventions exactly.

**Neither use case publishes a domain event.** This is a deliberate
departure from the pattern every other write use case in this service
follows (`ClaimNext` -> `TaskClaimed`, `CompleteTask` -> `TaskCompleted`,
etc.), so it is worth being explicit about the reasoning: check-in/
check-out is operational shift-management state (a worker badging in or
out of a physical position), not itself a fact about a unit of work
moving through its lifecycle. Nothing in this service's own domain model
currently needs to react to a check-in or check-out as an event — the
Station aggregate's occupant is simply read, later, at whatever moment a
`TaskCompleted` happens to fire. If a future consumer needs to react to
check-in/check-out as first-class events (e.g. `labor-performance`
tracking shift start/end directly), that is a new, separate decision, not
retrofitted here speculatively.

### `Task.ClaimedAt` — set once by `Claim`, never cleared by `Complete`

```go
func (t *Task) Claim(stationId shared.StationId, stationCapabilities shared.CapabilitySet, now time.Time, leaseDuration time.Duration) error {
    ...
    t.status = Claimed
    t.lease = &Lease{StationId: stationId, Expiry: now.Add(leaseDuration)}
    claimedAt := now
    t.claimedAt = &claimedAt
    return nil
}
```

`ClaimedAt()` returns `nil` for a task that has never been claimed, or
one rehydrated from a row persisted before this column existed
(`Rehydrate` takes `claimedAt *time.Time` as its final, additive
parameter). Unlike `lease`, which `Complete` clears to `nil`, `claimedAt`
is **not** cleared on completion — `CompleteTask`'s Kafka enrichment
needs it to compute duration *after* the task has already transitioned to
`Completed`.

Persisted via one additive, nullable migration
(`migrations/0007_task_claimed_at.up.sql`:
`ALTER TABLE tasks ADD COLUMN claimed_at TIMESTAMPTZ;`), so every
pre-existing row defaults to `NULL` — old tasks answer `ClaimedAt() ==
nil` exactly like a task rehydrated with the parameter explicitly
omitted.

### `TaskCompleted` stays thin; the Kafka publisher resolves both facts

Per this service's established discipline (ADR-0004, and ADR-0011's
"domain events stay thin" precedent), `TaskCompleted` itself gained no
new fields. `internal/adapters/outbound/kafka/publisher.go` — which
already loads the `Task` via `TaskRepo` to resolve `WorkUnitId` — now
also loads the `Station` via a new `StationRepo` dependency to resolve
`AssociateId`, and computes `DurationSeconds` from the already-loaded
`Task`'s `ClaimedAt()`:

```go
type TaskCompletedData struct {
    TaskId          string `json:"task_id"`
    StationId       string `json:"station_id"`
    WorkUnitId      string `json:"work_unit_id"`
    AssociateId     string `json:"associate_id,omitempty"`
    DurationSeconds int64  `json:"duration_seconds,omitempty"`
}
```

`AssociateId` is `""` (omitted on the wire) when the station has no
occupant — a soft, best-effort fact, never a reason to fail the publish.
`DurationSeconds` is `0` (omitted) when `ClaimedAt()` is `nil` (a
pre-migration task), rather than a negative or nonsensical value.

### Why an event on the existing topic, not a new query endpoint

`labor-performance` could instead poll or call a new
`GET /tasks/{id}/completion-details`-style endpoint. That was rejected:
this service already publishes `TaskCompleted` as the canonical "this
unit of work finished" fact, and Kafka is this workspace's established
cross-service integration mechanism (ADR-0004). Adding a second,
synchronous read path for the same underlying event would duplicate the
completion signal across two mechanisms that could disagree, and would
put a new inbound HTTP dependency onto this service's completion path
that does not otherwise need one. Enriching the existing event keeps the
coupling one-way and event-driven: this service publishes what it knows,
once, at the moment it knows it.

## Consequences

### Easier

- **`labor-performance` gets both facts it needs from a single event it
  already has to consume anyway** (`TaskCompleted`), with no new
  endpoint, topic, or schema version to coordinate.
- **Check-in/check-out required zero domain changes.** `Station.CheckIn`/
  `CheckOut`/`Occupant`/`IsOccupied` were already correct and already
  unit-tested (`station_test.go` predates this ADR); this round is purely
  "plug in what already works."
- **Duration is computed from data this service already captured for an
  unrelated reason** (the lease's implicit claim-start moment), so no new
  timer, clock call, or external correlation was needed — only recording
  what `Claim` already knew.
- **Backward-compatible by construction.** A `NULL claimed_at` and an
  occupant-less station both degrade to well-defined zero values
  (`0` seconds, `""` associate) rather than errors, so no historical data
  needs to be backfilled for this to ship.

### Harder

- **Check-in/check-out is now the one write path in this service that
  publishes no domain event**, a deliberate but real inconsistency with
  every other use case's shape. A future maintainer skimming
  `usecases/` for "does this need a `Publisher`" will find an exception
  here and needs this ADR (or the code comment pointing at it) to
  understand why.
- **The Kafka publisher now depends on `StationRepo` in addition to
  `TaskRepo`**, a second repository dependency and a second point of
  failure (`kafka: lookup station %s for enrichment` joins `kafka: lookup
  task %s for enrichment` as a way `Publish` can now fail) on what was
  previously a single-lookup enrichment path.
- **`AssociateId` can be stale or wrong relative to who "actually" did
  the work**, since it is read from whichever occupant happens to be
  checked in *at publish time*, not necessarily the same person who was
  checked in for the task's entire duration (a worker could check out and
  a replacement check in mid-task, and the replacement would get
  attributed). This is accepted as a known limitation of a best-effort
  fact, not solved by this change — a stronger guarantee would require
  snapshotting the occupant at claim time, which was deliberately not
  done here to keep this round additive and minimal.
- **`ClaimedAt` is never cleared, including across a task that is claimed,
  lease-expires back to Pending, and is claimed again by a different
  station.** `ExpireLeaseIfDue` and a fresh `Claim` both leave the
  previous `claimedAt` in place until the new `Claim` call overwrites it —
  correct for the common case, but worth noting: there is a narrow window
  between an expiry and a re-claim where `ClaimedAt()` reports the *prior*
  claim's start time on a task that is technically back in the pool.
  This is harmless in practice (nothing reads `ClaimedAt()` on a
  non-Completed task), but is a latent surprise for a future domain method
  that might.