---
id: 0016-rebin-and-order-consolidation
slug: /adr/0016-rebin-and-order-consolidation
title: 0016. REBIN task type and OrderConsolidation, kept inside fulfillment-execution
sidebar_label: 0016. Rebin & consolidation
description: ADR 0016 — add REBIN as a fourth process-path task type and a small OrderConsolidation aggregate that tracks per-order fan-in at Rebin, both staying inside fulfillment-execution rather than becoming a new bounded context.
---

# 0016. REBIN task type and OrderConsolidation, kept inside fulfillment-execution

## Status

Accepted. Concurrent-arrival safety (per-order lock) added by
[ADR-0034](./0034-concurrency-control-for-consolidation-and-claim.md).

## Context

This platform's process-path reference material describes Rebin
(sometimes "Induct" or "Consolidate") as the queue that receives items
picked for a multi-line order from different zones/pods and holds them
until every line for that order has arrived, at which point the order can
proceed to Pack as a single unit. The reference material is explicit that
single-item orders bypass this step entirely — "single-item shipments
skip Induct and Rebin" — which is exactly the shape order-management's
own `FulfillmentClass` classifier (ADR-0008 in that repository) already
distinguishes as `SINGLE` versus the two multi-line classes.

Before this ADR, this repository had no representation of Rebin at all:
`task.Type` had exactly three values (`Pick`, `Pack`, `Slam`), and nothing
in the domain modeled the fan-in problem of "wait for every line of this
order to converge before creating its Pack task." This gap was already
named in this repository's own documentation
(`business-context/process-paths.md`), which flagged "OrderConvergence"
as a concept belonging upstream without resolving it.

Two design questions had to be settled:

1. **Is REBIN a task type, like PICK/PACK/SLAM, or something structurally
   different?**
2. **Does the fan-in tracker (which lines of an order have arrived) live
   inside `fulfillment-execution`, or does it need its own bounded
   context?**

### Forces

- **Cadence match.** The reference material's queue-not-workflow model
  treats every process path — Pick, Pack, SLAM, and Rebin — as an
  independent, pull-dispatched queue operating at the same seconds-scale
  cadence. `Task` (this repository's existing execution-level unit,
  disposable once completed) already models exactly that cadence for
  three of the four queues. Nothing about Rebin's physical reality
  (a worker or automation event scanning an item into a Rebin location)
  differs from a Pick or Pack completion in kind — only in what triggers
  next.
- **Task already deliberately carries no order-level opinion.** Per this
  repository's own ubiquitous-language table, `Task` is "disposable once
  completed" and explicitly NOT the WMS's persistent, SLA-bearing
  demand-signal concept. Adding order-level fan-in logic directly onto
  `Task` would blur that boundary the documentation already protects.
- **The fan-in problem has a small, precisely bounded state shape.**
  "Which of order X's N required lines have arrived" is a single small
  aggregate (a required set, an arrived subset, a completeness
  predicate) with no independent lifecycle, no external API consumers of
  its own, and no reason to be addressable outside this context — nothing
  about it resembles the criteria (its own team, its own deployment
  cadence, autonomous change velocity) this fleet's own bounded-context
  decisions (see ADR-0008 in `fulfillment-execution` itself, the
  MCP-adapter ADR, and this plan's own decision log) actually use to
  justify a new service.
- **User's explicit decision.** The gap-closure plan this ADR implements
  records the user's decision directly: "Feature A stays inside
  fulfillment-execution" — a new aggregate, not a new bounded context.
  This ADR documents the reasoning behind that decision rather than
  re-litigating it.

### Alternatives considered

**A new `order-consolidation` bounded context/service.** Rejected: no
independent team, deployment cadence, or external consumer exists or is
anticipated for this narrowly-scoped fan-in tracker; splitting it out
would add a network hop and a second source of truth for a concept
(REBIN task completion) that is otherwise entirely local to this
context's existing Task lifecycle.

**Model Rebin as a `Task` field/status rather than a task type
(`task.Type`).** Rejected: Rebin needs its own queue, its own
`requiredCapabilities`, its own claim/lease/complete lifecycle — exactly
what `task.Type` already exists to parameterize. Special-casing it as a
`Task` sub-state would duplicate `ClaimNext`/`CompleteTask` logic instead
of reusing it.

**Put OrderConsolidation state directly on the `Task` aggregate (e.g. a
`Task.ConsolidationGroup` field).** Rejected: a single REBIN task
represents one line's arrival, not the order's aggregate fan-in state.
Coupling per-order consolidation state onto a per-line `Task` would mean
N copies of the same order-level completeness fact, with no single
source of truth — exactly the kind of state fragmentation
`OrderConsolidation` as its own small aggregate avoids.

## Decision

**Add `task.Rebin` as a fourth `task.Type` value, with zero special-case
behavior anywhere in `Task`'s claim/complete lifecycle — a REBIN task is
claimed, leased, and completed by the exact same rules as PICK/PACK/SLAM.
Add a new `internal/domain/consolidation` package containing
`OrderConsolidation`, a small aggregate (not a new bounded context) that
tracks which of an order's required lines have arrived at Rebin.**

```go
// task/task.go
const (
	Pick  Type = "PICK"
	Pack  Type = "PACK"
	Slam  Type = "SLAM"
	Rebin Type = "REBIN"
)
```

```go
// consolidation/order_consolidation.go
type OrderConsolidation struct {
	orderRef      string
	requiredLines map[string]struct{}
	arrivedLines  map[string]struct{}
}

func (o *OrderConsolidation) RecordArrival(lineId string) error // idempotent, rejects ErrUnknownLine
func (o *OrderConsolidation) IsComplete() bool
```

A new `ArriveAtRebin` use case wires the two together: it records one
line's arrival via `OrderConsolidation.RecordArrival`, and — only on the
transition into `IsComplete() == true` — calls the EXISTING `CreateTask`
use case to create the order's `task.Pack` task (the same reuse
discipline `CreateTask`'s own doc comment already establishes: this is
its intended use, not a workaround). A single-line order (order-management's
`ClassSingle`, per that repository's ADR-0008) completes on its one and
only required line's arrival — no special-casing needed, since
`IsComplete()` is `len(arrived) == len(required)` regardless of how many
lines that is.

Persistence: a new `order_consolidations` table (`order_ref` primary key,
`required_lines`/`arrived_lines` as `TEXT[]`), following this
repository's existing convention (see `tasks`, `packages`) of one row per
aggregate, upserted via `ON CONFLICT DO UPDATE`.

Idempotency: `RecordArrival` on an already-arrived line is a no-op
success (mirrors `wes-work-planning`'s `WorkPool.Complete` redelivery
tolerance), and `ArriveAtRebin.Execute` additionally short-circuits
PACK-task creation once consolidation has already completed — without
this second guard, a redelivered arrival event after completion would
create a duplicate `task.Pack`. This exact bug was caught by this
feature's own TDD process (a use-case test failed before the guard was
added) rather than shipped and found later.

### What this decision does NOT do

- It does not change `Task`'s claim/complete invariants in any way — the
  new `TestClaim_RebinTaskFollowsIdenticalRulesToOtherTypes` test exists
  specifically to prove that.
- It does not give `OrderConsolidation` any visibility outside this
  context: it is keyed by `orderRef` (an opaque string, per this
  context's own "OrderRef arrives as an opaque correlation key" ubiquitous-
  language rule) and line ids, nothing more.
- It does not yet wire a real Rebin-arrival event source (a physical
  scan, an automation controller signal) — `POST /rebin/arrivals` is the
  synchronous HTTP entry point for this round, matching how this
  repository's other Pick/Pack/SLAM actions are HTTP-first before any
  Kafka event sourcing is added.
- It does not implement cartonization or a Rebin *location* model (which
  bin/tote a line physically waits in) — this ADR is scoped to the
  fan-in completeness signal only.

## Consequences

### Easier

- **Reuses the entire existing Task machinery for a fourth queue type at
  zero marginal claim/complete/lease-expiry code** — `ClaimNext`,
  `CompleteTask`, `ExpireLeases`, and the HTTP/Postgres/memory adapters
  all already handle any `task.Type` value generically.
- **A precisely-scoped aggregate with a trivial invariant** (`IsComplete
  = len(arrived) == len(required)`), easy to reason about and to mutation-
  test — `internal/domain/consolidation` reached 100% mutation-kill on
  its own (a `gremlins unleash ./internal/domain` run against this
  feature's mutants showed zero survivors from this package; the pre-
  existing survivors in `package/segregation.go` predate this feature and
  are unrelated to it).
- **No new bounded context, no new network hop, no new deployment unit**
  — the fan-in tracker lives and dies with this context's own Postgres
  instance and API surface.

### Harder

- **The required-lines set is established by whichever call arrives
  first for an orderRef, and every subsequent call must agree on it.**
  `ArriveAtRebin` does not itself reconcile disagreement between calls —
  this is the caller's responsibility (whatever publishes Rebin-arrival
  events must consistently know an order's full required-line set). This
  is a real, documented constraint, not a silent gap: `ArriveAtRebin`'s
  doc comment states it explicitly.
- **No Kafka event source yet.** Every arrival currently must be reported
  via a direct HTTP call (`POST /rebin/arrivals`), which is fine for this
  round but means Rebin has no path-catalogue-driven or automation-driven
  event ingestion yet — a real gap if/when Feature D (path catalogue as
  configuration) or physical Rebin automation is introduced.
- **`OrderConsolidation` and `Task` share no foreign-key-style
  relationship** — a REBIN task's completion and an `ArriveAtRebin` call
  are two separate operations the caller must sequence correctly (in
  practice: complete the REBIN task, then call `ArriveAtRebin` with that
  line's id). A future refinement could have `CompleteTask` itself notify
  consolidation when completing a REBIN-typed task, removing this manual
  sequencing — deliberately deferred here to keep this round's diff to
  the minimum needed to prove the aggregate and its wiring work.

## Verification

Domain layer (`internal/domain/consolidation/order_consolidation_test.go`):
7 tests — completeness true/false transitions, unknown-line rejection,
idempotent redelivery, required/arrived accessor correctness,
rehydration round-trip, and the single-line-completes-immediately case.
`internal/domain/task/task_test.go` gained a REBIN-specific test proving
zero special-cased claim/complete behavior.

Use-case layer (`internal/application/usecases/arrive_at_rebin_test.go`):
5 tests, including the redelivery-does-not-duplicate-PACK-task case that
caught a real bug during TDD (the fix is the `wasAlreadyComplete` guard
described above).

Adapter layer: a real Postgres integration test
(`internal/adapters/outbound/postgres/postgres_integration_test.go`,
`TestOrderConsolidationRepo_SaveAndFindByOrderRef`) run against this
repository's live `e2e-postgres-fulfillment` container — not skipped,
not mocked — proving the upsert-on-conflict persistence actually works.
4 new httptest cases cover `POST /rebin/arrivals`: single-line immediate
completion, multi-line wait-then-complete, missing-field 400, and
unknown-line 422 with the correct RFC 7807 problem type.

`go test ./... -race` (all packages, including
`internal/architecture`'s hexagonal fitness tests), `golangci-lint run
./...` (0 issues), `make check-all` (fmt/vet/build/lint/test/coverage/
arch-test/bdd, coverage gate unchanged), and `gremlins unleash
./internal/domain/task` (100% efficacy/coverage, matching this
repository's pre-existing baseline) all pass. The repository-wide
`gremlins unleash ./internal/domain` run shows this feature's own new
code (task.go's REBIN branch, all of consolidation/) with zero surviving
mutants; its overall efficacy number is held down entirely by pre-
existing survivors in `package/segregation.go` and `package/package.go`
predating this feature, which only the non-blocking scheduled
`mutation` CI job (not `mutation-fast`) exercises.
