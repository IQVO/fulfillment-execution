---
id: 0025-cpt-missed-sweep-and-package-manifested
slug: /adr/0025-cpt-missed-sweep-and-package-manifested
title: 0025. TaskCPTMissed sweep and PackageManifested on the outbox (ADR 0014 companion)
sidebar_label: 0025. TaskCPTMissed sweep and PackageManifested
description: "ADR 0025 — this service's half of order-management ADR 0014 §5's promise feedback loop: a Clock-driven sweep that raises TaskCPTMissed for tasks still open past their CPT, and PackageManifested raised alongside LabelApplied on a successful SLAM pass, both flowing through the existing transactional outbox (ADR 0020) onto warehouse.fulfillment.events, additive to the wire."
---

# 0025. TaskCPTMissed sweep and PackageManifested on the outbox

## Status

Accepted — implemented in the same change that introduced this record.
**Companion to order-management ADR 0014, §5 "The loop closes:
OrderRepromised"** — the same relationship WES's ADR 0018
(`PathCapacityChanged`) has to its own upstream companion decision.
order-management's ADR 0014 designs the consumer side of this loop
(`RepromiseOrder`, `OrderRepromised`) and explicitly defers the two
events this ADR defines to "fulfillment-execution's own ADR." This is
that ADR. order-management's `RepromiseOrder` consumer is out of scope
here entirely — a separate, later task, fed this ADR's real shipped
event shape as ground truth (see order-management ADR 0014's Rollout
step 5).

## Context

order-management ADR 0014 replaces `now + leadTime` promise-date
arithmetic with a CPT window derived from real fulfillment capability,
and closes with a feedback loop: today, "nothing flows back" — this
service publishes only `TaskCompleted`, so a missed CPT (a task still
sitting in the pool past its deadline) or a completed SLAM pass are both
invisible to the order that is waiting on them. order-management's ADR
0014 §5 names the fix and puts the two triggering facts in this
service's court:

> A new inbound Kafka consumer subscribes to
> `warehouse.fulfillment.events` for two events fulfillment-execution
> will add under its own ADR: `TaskCPTMissed` (a task still open past
> its CPT) and `PackageManifested` (SLAM pass).

Both facts already exist as first-class domain concepts here:
`Task.CPT()` / `Task.Status()` are exactly what the lease-expiry sweep
(`ExpireLeases`) already inspects for a different overdue condition, and
a SLAM pass (`Package.Weigh` succeeding, i.e. `LabelApplied`) is a real
state transition this service already publishes half of. Neither event
needs a new aggregate, a new port shape, or a new wire mechanism — the
work is to detect the CPT-miss condition, raise the two events at the
right point in existing use cases, and route them through the same
transactional outbox (ADR 0020) every other integration event already
uses.

## Decision

### 1. `Task.IsCPTMissed(now)` — pure domain predicate

```go
func (t *Task) IsCPTMissed(now time.Time) bool {
	if t.status == Completed {
		return false
	}
	return !now.Before(t.cpt.Time())
}
```

Mirrors `Lease.expired`'s own boundary convention exactly: a task due
**exactly** at `now` counts as missed (not-before, not
strictly-after), so a sweep tick landing precisely on the CPT catches it
immediately rather than waiting a full tick. Pending and Claimed are
both "still open" — an unconfirmed claim past its CPT is just as much a
promise risk as an unclaimed one. Pure domain logic, no I/O; pinned by
explicit boundary tests (`TestIsCPTMissed_ExactlyAtCPT_CountsAsMissed`,
`TestIsCPTMissed_BeforeCPT_NotMissed`) precisely because a boundary
comparison is exactly what a gremlins mutant flips.

### 2. `TaskRepo.FindOpenPastCPT(ctx, now)` — new query, both adapters

Widened `ports.TaskRepo` additively (same shape as the existing
`FindAllClaimed`, which backs `ExpireLeases`):

```go
FindOpenPastCPT(ctx context.Context, now time.Time) ([]*task.Task, error)
```

Postgres: `WHERE status IN ('PENDING', 'CLAIMED') AND cpt <= $1`. In-memory:
filters via `Task.IsCPTMissed` directly, so the two implementations can
never disagree about the boundary. Every existing `ports.TaskRepo`
implementer (postgres, memory, and every test fake in
`internal/application/usecases/fakes_test.go`,
`internal/adapters/outbound/kafka/analytics_publisher_test.go`) was
updated to satisfy the widened interface.

### 3. `SweepCPTMisses` — a new Clock-driven sweep, structurally identical to `ExpireLeases`

```go
type SweepCPTMisses struct {
	Tasks      ports.TaskRepo
	Publisher  ports.EventPublisher
	Clock      ports.Clock
	UnitOfWork ports.UnitOfWork // one scope per reported task, ADR 0020
}

func (uc *SweepCPTMisses) Execute(ctx context.Context) (int, error) {
	now := uc.Clock.Now()
	overdue, err := uc.Tasks.FindOpenPastCPT(ctx, now)
	...
	for _, t := range overdue {
		err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
			return uc.Publisher.Publish(ctx, shared.NewTaskCPTMissed(t.Id(), t.OrderRef(), string(t.Type()), t.CPT().Time(), now))
		})
		...
	}
	return reported, nil
}
```

The one structural difference from `ExpireLeases`: **`SweepCPTMisses`
never mutates the `Task` aggregate.** `ExpireLeases` frees a task back to
Pending (a real lifecycle transition) before saving and publishing; a
CPT miss changes nothing about the task's own state — it is a fact
reported upstream, not a state transition here. There is therefore no
`Tasks.Save` call in the loop, only `Publish`.

### 4. Re-fire-every-sweep-tick, not exactly-once — the key design decision

**A task still open past its CPT raises `TaskCPTMissed` again on every
sweep pass, for as long as it remains open.** No "already reported"
state is tracked (no new table, no per-task flag). This is the simpler
option and it mirrors `ExpireLeases`'s own unconditional-on-match style,
but the decision is deliberate, not merely the path of least resistance
— and the justification is not invented here. order-management's ADR
0014 §5 states the design point explicitly, as the stated reason its own
consumer needs an inbox:

> `RepromiseOrder` is idempotent on `(orderId, sourceEventId)`, using the
> same inbox convention the fleet's other Kafka consumers use, **because
> fulfillment-execution's missed-CPT sweep will re-emit on every pass.**

In other words: order-management's own ADR already designed its
consumer around this exact behavior, before this service's
implementation existed. Building the alternative (dedup-tracking state
here, so `TaskCPTMissed` fires exactly once per task) would be solving a
problem order-management's ADR already solved on its own side, at the
cost of new persistent state this service does not otherwise need —
worse on both counts. The trade this accepts: the topic sees one
`TaskCPTMissed` message per overdue task per sweep interval for as long
as it stays overdue, which is bounded by how often the sweep is
triggered (see §6) and is exactly the same shape of trade `ExpireLeases`
already accepted for `LeaseExpired`-adjacent noise.

### 5. `PackageManifested` — raised alongside `LabelApplied`, additively, in `RunSlam`

```go
if !labelApplied {
	return uc.Publisher.Publish(ctx,
		shared.NewWeightDiscrepancyDetected(...), shared.NewPackageDiverted(...))
}
return uc.Publisher.Publish(ctx,
	shared.NewLabelApplied(packageId, now),
	shared.NewPackageManifested(packageId, p.OrderRef(), now),
)
```

"SLAM pass" (per order-management ADR 0014 §5) means exactly the
label-applied branch: the weigh-check passed. A diverted package (weight
outside tolerance) was **not** manifested and does not raise this event
— the diverted branch is untouched. This is the smallest possible
change to an existing use case, not a new one: one new event, in the
one branch that already means "SLAM passed," inside the same
`UnitOfWork` scope `LabelApplied` already commits in.

### 6. Trigger mechanism — a REST endpoint, matching `ExpireLeases`'s real convention

Verified before deciding: this repo has **no background ticker/cron
loop** for `ExpireLeases` in `cmd/execution/main.go` — it is invoked
externally via `POST /tasks/expire-leases`
(`internal/adapters/inbound/http/router.go` /`handlers.go`), presumably
by an operator/cron/harness hitting the REST endpoint. `SweepCPTMisses`
is wired the identical way: `POST /tasks/sweep-cpt-misses`, same
collection-level-action-on-`tasks` convention, same `{"<count>": n}`
response shape (`{"reported": n}` vs. `ExpireLeases`'s `{"freed": n}`),
registered in `apis/openapi.yaml` right next to
`/tasks/expire-leases`. No ticker was invented for this ADR; matching
the repo's real existing convention was preferred over introducing a
second dispatch mechanism.

### 7. Wire shape — additive, through the existing outbox, zero behavior change for `TaskCompleted`-only consumers

`internal/adapters/outbound/kafka/publisher.go`'s `Encode`/`Publish`
widened from "TaskCompleted only" to a three-case switch
(`TaskCompleted`, `TaskCPTMissed`, `PackageManifested`); every other
event type is still skipped exactly as before. Two new wire payload
structs, flat JSON, matching `TaskCompletedData`'s style:

```go
type TaskCPTMissedData struct {
	TaskId   string    `json:"task_id"`
	OrderRef string    `json:"order_ref"`
	TaskType string    `json:"task_type,omitempty"`
	Cpt      time.Time `json:"cpt"`
}
type PackageManifestedData struct {
	PackageId string `json:"package_id"`
	OrderRef  string `json:"order_ref"`
}
```

Neither needs the repo-lookup enrichment `TaskCompleted` requires —
every field they carry lives directly on the domain event already.
Both route through `postgres.OutboxPublisher`/`OutboxRelay` exactly like
`TaskCompleted` does today: `SweepCPTMisses`/`RunSlam` write the encoded
message into `outbox_events` inside their own `UnitOfWork` scope, and
the existing relay drains it onto `warehouse.fulfillment.events` — no
new outbox, no new relay, no new topic. `TaskCompletedData`'s shape is
byte-for-byte unchanged, and any consumer that already skips unrecognized
event types by convention (the CloudEvents `type` since ADR-0032) (order-management's future
`RepromiseOrder`, labor-performance's own `TaskCompleted`-only consumer)
sees zero behavior change.

### 8. `OrderRef`, not order-management internals

Both events carry `shared.OrderRef` (this service's own existing value
object — WES's `WorkUnit` id, per the `orderRef` cross-service
contract already documented in `api-and-integration.md`), never an
order-management `OrderId` or any type this service has no business
knowing about. `TaskCPTMissed` additionally carries `TaskType` and `CPT`
— useful for a future consumer to reason about "how late" and "which
leg" without a repo lookup back into this service — deliberately not
more than that (no station id, no lease detail): those are not premises
of a promise recompute.

## Consequences

**Easier**

- The promise feedback loop's producing half exists and is real: a task
  sitting open past its CPT, or a package that just passed SLAM, is now
  a fact on the wire, not silently absorbed.
- Nothing about the existing wire shape changed. `TaskCompletedData` is
  untouched; a consumer reading only `TaskCompleted` off this topic
  (order-management's existing consumers, labor-performance's) sees
  zero behavior change — verified by the full existing test suite
  passing unmodified plus new coverage for the widened switch.
- `SweepCPTMisses` reuses every mechanism this service already has:
  `ports.TaskRepo`, `ports.EventPublisher`, `ports.UnitOfWork`, the
  outbox, the REST-triggered-sweep convention. No new infrastructure.

**Harder**

- `TaskCPTMissed` re-fires on every sweep pass while a task stays
  overdue — real Kafka volume proportional to (overdue task count) ×
  (sweep frequency), not to distinct promise-risk events. This is an
  accepted, cited trade (§4), not an oversight, but it does mean this
  topic is noisier per-overdue-task than `TaskCompleted` ever was. A
  consumer that does not itself dedupe (contrary to how order-management
  ADR 0014 explicitly designs `RepromiseOrder`) would over-react.
- `SweepCPTMisses` has no ticker of its own — like `ExpireLeases`, its
  cadence is whatever external caller invokes
  `POST /tasks/sweep-cpt-misses`, on whatever schedule that caller
  chooses. This ADR does not add one; it was out of scope (matching the
  existing convention, not inventing a new one, per §6).
- Two more branches for a future reader of `internal/adapters/outbound/kafka/publisher.go`'s
  `Encode`/`Publish` to keep additive when the next event joins the
  integration contract — mitigated by the existing arch-fitness tests
  and this ADR's explicit call-out that `TaskCompletedData` must never
  change shape.

## Alternatives considered

- **Track per-task "already reported" state so `TaskCPTMissed` fires
  exactly once.** Rejected: solves a problem order-management's own ADR
  0014 §5 already solved on the consumer side (the cited idempotency
  note), at the cost of new persistent state this service does not
  otherwise need. Re-firing is the ADR-0014-intended design, not a gap
  to close here.
- **A background ticker for `SweepCPTMisses` instead of a REST
  endpoint.** Rejected as inventing a second dispatch convention when
  this repo already has exactly one for a Clock-driven sweep
  (`ExpireLeases`'s externally-triggered REST endpoint) — verified by
  reading `cmd/execution/main.go` before deciding, not assumed.
- **Mutate the Task aggregate on a CPT miss** (e.g. a new status or a
  "missed" flag). Rejected: a CPT miss is not a lifecycle transition —
  the task is still exactly as claimable/completable as before. Keeping
  `SweepCPTMisses` a pure read-and-publish (no `Save`) keeps the
  aggregate's invariants exactly as `ExpireLeases`/`RunSlam` already
  established them.
- **Raise `PackageManifested` as a replacement for `LabelApplied`**
  rather than alongside it. Rejected: `LabelApplied` is an existing,
  possibly-consumed event (documented in the AsyncAPI catalogue); this
  ADR is additive everywhere, including here.

## Verification performed

All run locally in the worktree before pushing; every command below
produced the quoted result.

- `go build ./...`, `go vet ./...`: clean.
- `gofmt -l .`: no output (clean).
- `go test ./... -race`: all packages pass, including the full BDD suite
  (`features/cpt_missed_sweep.feature`, 3 new scenarios: a task past its
  CPT is reported, a task not yet due is not reported, a still-overdue
  task is reported again on the next pass) and every existing
  `ExpireLeases`/`RunSlam` test (updated where `RunSlam`'s
  label-applied-branch event count grew from 1 to 2, no other existing
  assertion touched).
- `go build -tags=integration ./... && go vet -tags=integration ./...`:
  clean.
- `go test -tags=integration ./internal/adapters/outbound/postgres/... -race -count=1`
  against a **testcontainers** `postgres:16-alpine` (no external
  `DATABASE_URL`, no skip): includes two new tests —
  `TestOutbox_SweepCPTMisses_RoundTripsThroughRelay` (a real
  create-task → advance-clock → sweep → outbox-row → relay round trip,
  asserting the relayed message is keyed by `TaskId` and carries
  `order_ref`/`task_type`) and
  `TestOutbox_RunSlam_LabelAppliedAndPackageManifestedRoundTripTogether`
  (real seal → SLAM-pass → outbox → relay, asserting `PackageManifested`
  reaches the integration topic keyed by `PackageId` while `LabelApplied`
  correctly stays analytics-only) — both pass alongside the full existing
  outbox integration suite, unmodified.
- `go test -tags=integration ./... -race -count=1` against a real
  Postgres container (`DATABASE_URL` set) plus the existing
  testcontainers-based Kafka integration tests elsewhere in the repo:
  all pass, including a new `TestTaskRepo_FindOpenPastCPT_ReturnsOnlyOpenTasksPastCPT`
  proving the real Postgres query excludes a Completed-but-overdue task
  and a not-yet-due task while including an overdue Pending one.
- `go test ./internal/architecture/... -v` (arch-test): passes unchanged
  — this ADR adds an outbound publisher widening and a new use case, not
  a new inbound consumer or a dependency-rule violation.
- `make mutation-fast` (gremlins on `./internal/domain/task`): the new
  `IsCPTMissed` boundary is covered by explicit exactly-at/one-instant-
  before/Completed-never tests designed specifically to kill a
  `CONDITIONALS_BOUNDARY` mutant on the `!now.Before(...)` comparison.
- Every REST/handler test updated to wire `SweepCPTMisses` alongside the
  other use cases (`handlers_test.go`, `features_test.go`) — no existing
  handler test's assertions changed.
