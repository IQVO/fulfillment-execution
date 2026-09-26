---
id: 0027-cloudevents-envelope-migration
title: ADR-0027 — CloudEvents 1.0 envelope migration for the integration topics
sidebar_label: 0027 · CloudEvents envelope migration
sidebar_position: 27
description: Migrate warehouse.fulfillment.events and warehouse.work-planning.events from the flat platform envelope to a CloudEvents 1.0 structured envelope, via a dual-read/dual-write bake period, with zero consumer-facing breakage.
---

# ADR-0027 — CloudEvents 1.0 envelope migration for the integration topics

## Status

**Proposed. Companion to wes-work-planning ADR-0021. Neither is meaningful
without the other.**

This ADR requires the user's explicit acceptance (flipping this Status line
to `Accepted`) before Phase 2 implementation work — described below — may
start in any of the five consuming packages. Do not treat "PR opened" as
acceptance.

## Context

This repo's own [ADR-0004](./0004-kafka-integration-events-and-envelope.md)
already documents a CloudEvents 1.0 structured envelope as the target wire
shape for `warehouse.fulfillment.events`, and wes-work-planning's own
ADR-0004 (`0004-kafka-integration-events.md`) documents the same target for
`warehouse.work-planning.events`. Both ADRs candidly record that the target
was never implemented — the running publishers still write the flat
envelope (`event_id`/`event_type`/`occurred_at`/`source`/`data`), and both
repos' `apis/asyncapi.yaml` already fully specify the CloudEvents shape,
complete with worked examples. This ADR is the plan for closing that gap
without a breaking change to any consumer, and the ADR of record that
implementers should work from (not the external planning document that
preceded it).

### Current flat envelope (both publishers, byte-identical shape)

```go
// fulfillment-execution: internal/adapters/outbound/kafka/publisher.go
// wes-work-planning: internal/adapters/kafka/envelope/envelope.go
type Envelope struct {
    EventId    string          `json:"event_id"`
    EventType  string          `json:"event_type"`
    OccurredAt time.Time       `json:"occurred_at"`
    Source     string          `json:"source"`
    Data       json.RawMessage `json:"data"`
}
```

This repo publishes to `warehouse.fulfillment.events` (event types on the
wire: `TaskCompleted`, `TaskCPTMissed`, `PackageManifested` — this repo's
own ADR-0025). wes-work-planning publishes to `warehouse.work-planning.events`
(event types on the wire: `WorkReleased`, `PathCapacityChanged` — its
ADR-0018). Both also run a separate, untouched analytics
publisher/topic (`warehouse.fulfillment.analytics`, `warehouse.wes.analytics`)
— out of scope; no consumer decodes those against this envelope shape the
way integration consumers do.

### Every fleet consumer of these two topics (verified by decoding, not by ADR prose)

Re-verified against current `origin/develop` on 2026-09-26 as part of this
ADR's Phase 0 baseline — unchanged from the plan's original table, no 6th
consumer found.

| # | Publishing topic | Consuming repo | File | Struct(s) decoded | Event type(s) handled |
|---|---|---|---|---|---|
| 1 | `warehouse.work-planning.events` | fulfillment-execution (this repo) | `internal/adapters/inbound/kafka/consumer.go` | `Envelope` / `WorkReleasedData` | `WorkReleased` |
| 2 | `warehouse.work-planning.events` | order-management | `internal/adapters/outbound/kafkapathcapacity/consumer.go` | `envelope` (local, unexported) / `capacityData` | `PathCapacityChanged` |
| 3 | `warehouse.fulfillment.events` | wes-work-planning | `internal/adapters/inbound/kafka/consumer.go` (`fulfillmentReader`) | inline `taskCompletedData` | `TaskCompleted` |
| 4 | `warehouse.fulfillment.events` | labor-performance | `internal/adapters/inbound/kafka/consumer.go` | `taskCompletedData` | `TaskCompleted` |
| 5 | `warehouse.fulfillment.events` | order-management | `internal/adapters/inbound/kafka/repromise_consumer.go` | `fulfillmentEnvelope` / `taskCPTMissedData`, `packageManifestedData` | `TaskCPTMissed`, `PackageManifested` |

**5 consuming packages across 3 repos** (order-management appears twice,
via two independent consumer packages for two different topics — do not
conflate them into one PR). Every one of these decodes `event_id` as its
primary-key idempotency token (`processed_events`/`MarkProcessed`) — this
fact is load-bearing for the dual-write design below. This repo (task 1 in
the table) is itself both a publisher AND a consumer here — it decodes
wes-work-planning's `WorkReleased` at the same time it publishes its own
`TaskCompleted`/`TaskCPTMissed`/`PackageManifested`, so this migration
touches this repo's code on both the inbound and outbound side.

Confirmed **zero** other hits fleet-wide (inventory-storage,
workforce-management, facility-layout, process-path-management,
warehouse-ops-agent, network-fulfillment, e2e-tests — grepped
`origin/develop` for both topic string literals in every repo; e2e-tests
only *mentions* the topic in a comment, it asserts through HTTP side
effects, not by decoding Kafka itself, so it needs no code change, only a
black-box re-verification once the cutover phase lands).

## Decision

**We will migrate both publishers to a CloudEvents 1.0 structured envelope,
keeping the existing topic names unchanged, via a dual-read (consumers
first) then dual-write (publishers) then cutover sequence, so that no
consumer-facing breakage occurs at any point in the rollout.**

### 1. CloudEvents schema (field-by-field mapping)

```json
{
  "specversion": "1.0",
  "id": "<same value as today's event_id — a UUID v4>",
  "type": "com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>",
  "source": "/warehouse/<service-name>",
  "subject": "<the raising aggregate's instance id>",
  "time": "<same value as today's occurred_at — the domain clock, not publish time>",
  "datacontenttype": "application/json",
  "data": { }
}
```

| Flat field | CloudEvents field | Change |
|---|---|---|
| `event_id` | `id` | same value, renamed key |
| `event_type` (bare name, e.g. `TaskCompleted`) | `type` | value becomes the reverse-DNS string already sketched in both ADR-0004s and already live in both `apis/asyncapi.yaml` (`com.warehouse.wes.fulfillment-execution.task.TaskCompleted`, `com.warehouse.wes.work-planning.workunit.WorkReleased`) — this convention is not new, only unimplemented; reuse it verbatim, do not re-derive segment names |
| `occurred_at` | `time` | same value, renamed key |
| `source` (bare service name, e.g. `fulfillment-execution`) | `source` | value becomes the URI form already used in both asyncapi.yaml's examples (`/warehouse/fulfillment-execution`, `/warehouse/wes-work-planning`) |
| — (none today) | `subject` | **new field**, populated with the raising aggregate's instance id (task id for `TaskCompleted`, work unit id for `WorkReleased`, etc.) — already present in both asyncapi.yaml's worked examples, never implemented |
| — (none today) | `specversion` | **new field**, fixed `"1.0"` — this is the dual-read discriminator: its presence/absence is exactly how a consumer tells the two shapes apart, so this field must never be added to the flat envelope even experimentally |
| — (none today) | `datacontenttype` | **new field**, fixed `"application/json"` (the *inner* `data` payload's type; the outer message as a whole is `application/cloudevents+json` per both asyncapi.yaml's `defaultContentType`, but that is a transport/Content-Type concern, not a JSON field, and is out of scope — Kafka messages here carry no Content-Type header today) |
| `data` | `data` | **byte-identical, unchanged.** No existing consumer's payload struct (`WorkReleasedData`, `capacityData`, `taskCompletedData`, `taskCPTMissedData`, `packageManifestedData`) changes shape. This migration is envelope-only. |

Kafka message key is unchanged: still `id` (== today's `event_id`), so
partitioning/ordering semantics are untouched.

### 2. Do NOT rename the topic — this overrides this repo's own ADR-0004

This repo's own ADR-0004 states, verbatim, in its Consequences → Harder
section:

> "Migrating requires both sides to move together, because the topic name
> changes too."

**This ADR explicitly overrides that sentence.** Renaming the topic turns
an envelope migration into a topic migration — every consumer would need
dual-subscription logic, offset/consumer-group handling for a second
topic, and a coordinated cutover of the topic name itself, none of which
the actual problem requires. The envelope's own `specversion` field already
gives dual-read a discriminator with zero topic-level complexity. This ADR
records that reversal explicitly, with this rationale, rather than
silently ignoring the original claim. `warehouse.fulfillment.events` and
`warehouse.work-planning.events` keep their real names throughout and after
this migration; only the message *shape* on those topics changes, in a
soaked, dual-read/dual-write-gated sequence.

### 3. Dual-write mode: same topic, two physical messages per event, and why that's safe

`EVENT_ENVELOPE_MODE=flat|cloudevents|dual` on both publishers (default
`flat` — a no-op merge, matching this fleet's every other
opt-in-behind-env-var convention). In `dual` mode, one domain event
produces **two Kafka messages** on the *same* topic, same key
(`id`/`event_id`), one in each shape.

This is safe specifically *because* every consumer already deduplicates on
`event_id` via a primary-key `processed_events` insert (or the in-memory
equivalent) before applying an event's effect (this repo's own ADR-0004,
unchanged by this migration). A dual-read-capable consumer that receives
both physical messages processes whichever arrives first and silently
no-ops the second via the existing idempotency gate — no new dedup logic
needed anywhere.

**The one hazard this depends on**: a consumer must never attempt to
decode a CloudEvents-shaped message with a flat-only struct. `source`/`data`
happen to unmarshal into the old `Envelope` harmlessly, but `id` does NOT
populate `EventId` (wrong JSON key) — a not-yet-upgraded flat-only consumer
would silently read `EventId=""` from every CloudEvents message. Since
`processed_events.event_id` is a primary key, the **first** such message
would insert successfully (silently corrupting nothing, since the decoded
`EventType` is also `""` and gets skipped as unrecognized) — but the
**second and every subsequent** CloudEvents message would collide on the
now-already-inserted empty-string key and be silently treated as "already
processed" and dropped, **including ones whose flat sibling message a
consumer legitimately still needs**. This is exactly why dual-write mode
going live is gated on every consumer being verified dual-read first, with
no exception — it is not a preference, it is the one landmine in this
design and it is fatal if the order is ever reversed.

### 4. Where the new schema documentation lives: companion ADRs, not a new shared ADR

warehouse-docs' own `docs/adr/index.md` is explicit: "Every ADR lives in
its owning context's own repository … copying ADR content here would
create a second source of truth." A new shared warehouse-docs ADR would
violate that standing rule for no benefit — the schema is not owned by a
third context, it is owned jointly by exactly the two publishers already
writing it down. This ADR follows the fleet's proven companion-ADR-pair
pattern: a new ADR in each repo (this one, and wes-work-planning's ADR-0021),
each naming the other as companion in its Status line, each superseding its
own ADR-0004's "documented but not implemented" framing, together carrying
the one schema table above (duplicated by agreement, exactly like the
original envelope itself — same rationale, restated here: a shared Go
library would reintroduce coordinated-redeploy coupling). warehouse-docs'
`context-map.md` gets a footnote update in the docs closeout phase pointing
at both, not a copy of either.

## Phase sequence

### Phase 0 — Baseline (no code)

1. Confirm both repos clean on `develop`, CI green, cluster pods at current
   `develop` HEAD.
2. Confirm the 5-consumer table above is still accurate by re-running the
   same greps immediately before starting — a parallel PR could have added
   a 6th consumer since this ADR was written.
3. Run the full e2e suite once, green, as the pre-migration baseline to
   diff every later phase against.

**Rollback:** nothing changed yet; N/A.

### Phase 1 — Companion ADRs (docs-only, both repos, merged first)

This ADR and its wes-work-planning companion (ADR-0021). PR into `develop`
for both, CI green, **do not merge yet** — hold both open until the user
accepts the design, then flip both Status lines to `Accepted` in a
follow-up commit on each PR, re-verify CI, merge both.

**Verification:** both PRs' Docusaurus build job green; `index.md`/
`sidebars.ts` render the new entries; a fresh `git grep 0027-cloudevents` /
`0021-cloudevents` on `origin/develop` after merge finds both files.

**Rollback:** revert both merge commits; zero runtime impact either way —
this phase touches no code.

### Phase 2 — Upgrade every consumer to dual-read (parallel, 3 repos, 5 packages)

No env var on the consumer side — dual-read capability is unconditional
and purely additive: decode either shape, normalize to one internal struct
(`event_id`/`event_type`/`occurred_at`/`source`/`data`) before handing off
to the existing (unchanged) `handleXEvent` logic. Concretely: peek the raw
JSON for a `specversion` key (or attempt-decode a minimal
`{Specversion string}` probe struct) to pick the decode path, mapping
`id`→event_id, `type`→event_type (stripping the `com.warehouse....` prefix
back to the bare event name the existing switch/case statements already
key on), `time`→occurred_at, `source`→source (cosmetic only), `data`→data
unchanged.

- **Task 2a — fulfillment-execution (this repo)**: `internal/adapters/inbound/kafka/consumer.go`
  decodes wes-work-planning's `WorkReleased`. Add the discriminator +
  CloudEvents decode path; `WorkReleasedData` struct is untouched.
- **Task 2b — order-management**: `internal/adapters/outbound/kafkapathcapacity/consumer.go`
  decodes wes-work-planning's `PathCapacityChanged`. Same treatment;
  `capacityData` untouched.
- **Task 2c — wes-work-planning**: `internal/adapters/inbound/kafka/consumer.go`
  (`fulfillmentReader` path) decodes this repo's `TaskCompleted`. Same
  treatment; the other readers on this same `Consumer` are untouched.
- **Task 2d — labor-performance**: `internal/adapters/inbound/kafka/consumer.go`
  decodes this repo's `TaskCompleted`. Same treatment.
- **Task 2e — order-management**: `internal/adapters/inbound/kafka/repromise_consumer.go`
  decodes this repo's `TaskCPTMissed` and `PackageManifested`. Same
  treatment; this file's `fulfillmentEnvelope` is a different Go type from
  Task 2b's `envelope` despite both living in order-management — keep them
  independently updated.

**Tests per package (all real, run locally before pushing):** decode a
flat-shaped fixture (regression), decode a CloudEvents-shaped fixture (same
normalized result), fail-soft on an unrecognized/malformed `specversion`,
and a testcontainers Kafka integration test publishing one flat and one
CloudEvents message with the same `event_id`/`id`, asserting the use case
fires exactly once.

**PR/git discipline:** one branch per repo
(`feature/dual-read-cloudevents-envelope`), commit message + PR body to a
file and `--body-file`, CI green, merge into `develop`. Three repos can run
in parallel.

**Rollback:** revert the merge commit per repo. Zero runtime behavior
change even before revert — no publisher emits CloudEvents yet.

### Phase 3 — Cluster verification of dual-read BEFORE any publisher change

Deploy Phase 2's merged code to all 3 repos, hand-craft one CloudEvents-shaped
message per consumer/topic pair onto the real topic, confirm each of the 5
consumers produces the correct real side effect from the synthetic message
alone, then send an equivalent flat-shaped synthetic message and confirm
the identical side effect. Confirm no pod restarted throughout.

**Rollback:** this phase makes no persistent infra/config change. If any
pair fails, that pair's Phase 2 PR gets a follow-up fix; do not proceed to
Phase 4 until all 5 pass.

### Phase 4 — Publishers grow `EVENT_ENVELOPE_MODE`, default `flat` (non-breaking merge)

Split each publisher's `Encode` to build either shape based on
`EVENT_ENVELOPE_MODE` (`flat` default, byte-identical golden-file test;
`cloudevents`; `dual`, two messages, same key). `type` construction via a
small pure function using the subdomain/bounded-context constants already
named in each asyncapi.yaml. `subject` from the raising aggregate's id,
already available at every call site — for this repo, `TaskId` for
`TaskCompleted`/`TaskCPTMissed`, `PackageId` for `PackageManifested`. PR
into `develop` per repo, CI green, merge. No infra change yet.

**Verification:** CI green on new envelope-mode unit tests;
`git grep EVENT_ENVELOPE_MODE origin/develop` finds the new code; deploy
and confirm via pod logs that `mode=flat` is what's actually logged in
production right now.

**Rollback:** revert the merge commit per repo; or do nothing — the
running code already defaults to byte-identical `flat` output.

### Phase 5 — Infra: flip to `dual` for both publishers

warehouse-infra PR wiring `var.event_envelope_mode` (default `"flat"`) into
both services' `extraEnv` as `EVENT_ENVELOPE_MODE`, in both branches of
whatever ternary computes each service's `extraEnv`. Set to `"dual"`,
merge, `terraform apply`, mandatory `kubectl rollout restart` + `rollout status`
for both services.

**Verification:** pod logs show `mode=dual` for both. Trigger one real
`TaskCompleted` and one real `WorkReleased`; confirm two messages landed on
each real topic with the same key, one flat, one CloudEvents; confirm each
of the 5 downstream consumers still produced exactly one side effect.

**Rollback:** flip `event_envelope_mode` back to `"flat"`, re-apply,
rollout restart. Pure config revert, no consumer impact.

### Phase 6 — Bake period

Run `dual` mode for a defined soak window (at least one full e2e
regression cycle plus 24h of ambient traffic). Monitor DLQ topics,
`RESTARTS` counts, and `processed_events` growth rate (should roughly
double in row count without doubling distinct processed rows). Run the
full e2e suite again at the end; diff against the Phase 0 baseline.

**Rollback:** same as Phase 5.

### Phase 7 — Flip default to `cloudevents`-only

warehouse-infra sets `event_envelope_mode = "cloudevents"` for both
publishers. This is the actual breaking-shape cutover — flat messages stop
appearing entirely — but every consumer has run dual-read in production,
proven in Phase 3 and soaked in Phase 6, so no consumer change accompanies
this phase. `terraform apply`, mandatory rollout restart, `rollout status`.

**Verification:** publisher logs show `mode=cloudevents`; real-traffic
probe repeated, confirm exactly one message per event now, confirm each of
the 5 consumers still produces its correct side effect. Full e2e suite
green.

**Rollback:** flip back to `"dual"` (not all the way to `"flat"` — no
reason to stop CloudEvents from being written once cutover succeeds).
Re-apply, restart.

### Phase 8 — Second bake period, then fleet-wide flat-path deletion

Soak `cloudevents`-only for a second defined window with the same
monitoring checklist. Cleanup PRs (5 consumer packages + 2 publishers = 7
repos-worth of follow-up PRs, all independent, all can run in parallel):
each consumer deletes the flat-decode branch and `specversion`
discriminator, collapsing to a single CloudEvents-only decode path; each
publisher deletes the `flat` and `dual` Encode branches and the
`EVENT_ENVELOPE_MODE` env var entirely; warehouse-infra removes
`var.event_envelope_mode` and its wiring. Standard PR/CI/merge per repo;
develop→main release PRs where a cluster deploy is warranted; final
rollout restarts; final e2e run.

**Rollback:** this phase is the one point of no return — once the flat
decode/encode code is deleted, reverting requires a revert PR, not a
config flip. Do not start this phase's deletions until Phase 7's soak
window has produced zero DLQ/restart/e2e regressions.

### Phase 9 — Docs closeout (fleet-wide follow-up doc PR)

This repo's ADR-0004 caution block gets replaced with a short note
pointing at this ADR as the one that shipped it. wes-work-planning's
ADR-0004 gets the equivalent pointer to its companion (ADR-0021). Both
repos' `apis/asyncapi.yaml` drop the "not yet wired"/gap language specific
to the envelope shape. warehouse-docs `context-map.md` gets a one-line
annotation noting the envelope is now CloudEvents 1.0. Doc-only PR per
repo, ordinary GitFlow, no cluster impact.

## Rollback summary (one line per phase boundary)

| After phase | Rollback action | Blast radius if skipped |
|---|---|---|
| 1 (ADRs) | revert merge commit | none — docs only |
| 2 (consumer dual-read) | revert merge commit per repo | none — inert until Phase 4 |
| 3 (cluster probe) | no persistent change to undo | N/A |
| 4 (publisher dual-mode code) | revert merge commit per repo | none — default stays `flat` |
| 5 (infra: dual live) | `event_envelope_mode="flat"`, re-apply, restart | none if reverted promptly — consumers already dual-capable |
| 6 (bake) | same as Phase 5 | none |
| 7 (infra: cloudevents-only) | `event_envelope_mode="dual"`, re-apply, restart | none — dual is a safe landing spot |
| 8 (bake #2 + deletion) | **revert PR** to re-add deleted flat code (not a config flip) | first phase where rollback is a code change, not a var flip — gate hard on Phase 7's soak being clean |
| 9 (docs) | revert merge commit | none — docs only |

## Consequences

### Easier

- Both publishers finally match the CloudEvents shape their own
  `apis/asyncapi.yaml` has documented since ADR-0004, closing a
  spec-vs-code gap that both ADR-0004s candidly admit exists today.
- The topic-rename claim in this repo's own ADR-0004 is retired — future
  readers no longer need to reconcile "the topic name changes too" with a
  migration that in fact never touches the topic name.
- Every migration phase is independently reversible via a config flip,
  except the final cleanup phase, which is explicitly gated on two full
  soak windows producing zero regressions.

### Harder

- Two physical messages per event during the `dual` window roughly doubles
  `processed_events` row growth and roughly doubles Kafka throughput on
  both topics for the bake period's duration — an accepted, temporary cost.
- The dual-write hazard described in Design decision #3 means Phase 4/5
  ordering is not a preference: shipping a publisher's `dual` mode before
  every consumer is dual-read-capable would silently and permanently drop
  a subset of legitimately-needed messages behind a poisoned empty-string
  idempotency key. This ADR's phase gating exists specifically to make
  that ordering mistake structurally impossible to reach by following the
  numbered phases in order.
- This migration touches five consumer packages across three other repos'
  release cadences (order-management, labor-performance, plus this repo
  and wes-work-planning themselves) — coordination cost that a single-repo
  change would not have.
- This repo is both a publisher and a consumer in this migration (it
  decodes wes-work-planning's `WorkReleased` while publishing its own
  events), so its own Phase 2 and Phase 4 work happen in the same repo at
  different times — care is needed not to conflate the two PRs.

## Non-goal

This migration does NOT touch the other five publishers (order-management,
inventory-storage, workforce-management, facility-layout,
process-path-management), which still publish the flat envelope and whose
own ADR-0004-equivalents make no CloudEvents claim. Their migration, if
ever wanted, is separate future work.
