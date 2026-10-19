---
id: 0051-interleaving-hints-advise-claimnext
slug: /adr/0051-interleaving-hints-advise-claimnext
title: 51. InterleavingHintIssued is an advisory tie-break inside one claimNext priority band
sidebar_label: "51. Interleaving hints advise claimNext"
description: "ADR 0051 — fulfillment-execution consumes task-interleaving's InterleavingHintIssued into a local one-active-hint-per-station copy; claimNext uses the active, unexpired hint only as a tie-break among same-band peers and never across a priority/CPT boundary; every failure mode fails open to today's behavior; dedupe on the CloudEvents id in the same transaction as the hint upsert."
---

# 51. InterleavingHintIssued is an advisory tie-break inside one claimNext priority band

## Status

Accepted (2026-10-17). Additive: no existing event, endpoint, payload or
claimNext ordering rule changes. The producer contract is task-interleaving's
own `apis/asyncapi.yaml` (its ADR 0002), pinned and consumed independently
here, never imported.

## Context

task-interleaving watches a station complete a task and recommends which open
tasks nearby the operator could do next with the smallest detour. It publishes
the recommendation as `InterleavingHintIssued` on
`warehouse.task-interleaving.events` (CloudEvents 1.0, key and `subject` = the
station id, `type`
`com.warehouse.wes.task-interleaving.stationtrip.InterleavingHintIssued`,
`data` = `{hint_id, station_id, site_id, after_task_id, origin_location,
expires_at, policy_version, candidates: [{rank, task_id, task_type, location,
detour_m}]}`, rank starts at 1). Its own contract states the hint is an
**advisory hint, never an assignment**.

claimNext(stationId, capabilities) is this context's ONLY claim path: pull,
not push; the system selects work, not workers
([ADR-0002](./0002-pull-based-claimnext-dispatch.md)). It scans the open pool
of one task type, earliest CPT first, and claims the first candidate the
station's capabilities satisfy, at-most-once via the compare-and-set of
[ADR-0003](./0003-lease-based-at-most-once-claiming.md). Two tasks of one type with the
same CPT instant are a **priority band** — peers the repository may return in
either order ([ADR-0049](./0049-task-pool-events-task-available-and-task-claimed.md)).

Until now the hint stream had no consumer here: the saga edge closed on the
producer side ([ADR-0049](./0049-task-pool-events-task-available-and-task-claimed.md)
feeds task-interleaving's pool copy) but a hint could never influence
dispatch.

## Decision

### A local hint copy, one ACTIVE hint per station

A new table `interleaving_hints` (migration 0021): `(station_id PRIMARY KEY,
hint_id, site_id, expires_at, candidates JSONB, received_at)`. One row per
station: a newer `InterleavingHintIssued` for the same station **supersedes**
the previous one wholesale (the producer raises no "superseded" event; the
next hint IS the notification). Only what claimNext needs is kept:
`{rank, task_id}` per candidate, in rank order.

The consumer (`TASK_INTERLEAVING_MODE=kafka`, group from
`TASK_INTERLEAVING_CONSUMER_GROUP`, boot error when unset in kafka mode;
`permissive` — the default — wires nothing) follows the
[ADR-0039](./0039-product-classification-local-copy.md) shape: FetchMessage,
then `usecases.ApplyInterleavingHint` claims the CloudEvents `id` and upserts
the hint in ONE UnitOfWork (the same `processed_events` store and mechanism
every consumer here uses), then CommitMessages. A message that is not a valid
CloudEvent, or whose payload breaks the pinned contract (missing
hint_id/station_id/site_id/expires_at, empty candidates, rank < 1, a blank
candidate task_id), is WARN-logged and committed past; any other type is
ignored and committed past. A brand-new group starts at the LATEST offset:
hints expire within seconds, so replaying history is pointless — the next
hint for a station supersedes anything missed.

### claimNext: a tie-break INSIDE one priority band, never across it

The station's active, unexpired hint re-orders only the peers of one priority
band (one CPT instant — claimNext already scans a single task type): within a
band, hinted candidates still in the open pool are tried first, lowest rank
before higher, followed by the unhinted peers in the repository's own order.
Bands themselves are never reordered: a hinted candidate with a later CPT
stays behind every candidate of an earlier band. The capability check and the
compare-and-set are untouched and apply to every candidate, hinted or not, so
the at-most-once claim invariant is unchanged.

### Fail-open everywhere

A hint that cannot be used costs nothing and is invisible in behavior: no
hint stored, an expired hint (`expires_at` filtered in SQL AND re-checked by
claimNext), a hint whose candidates have all left the pool, a hinted
candidate the station cannot work, or a hint lookup that fails (WARN, then
claim exactly as without a hint). With `TASK_INTERLEAVING_MODE` unset,
claimNext runs the pre-0051 code path byte-for-byte (`Hints` is nil).

## Consequences

- Dispatch can prefer the lower-detour task among same-priority peers; work
  selection stays pull-based and the system still selects — the hint only
  breaks ties the repository already left unspecified.
- Exactly-once effect despite at-least-once delivery: the CloudEvents-id
  claim and the hint upsert commit or roll back together, so a redelivery
  (including one caused by a restart before the offset commit) has exactly
  one effect.
- An ignored hint is free: no retry storm, no DLQ traffic, no dispatch
  failure; task-interleaving can be down, silent or wrong without affecting
  claiming.
- The hint copy is per-station only; nothing here interprets `detour_m`,
  `policy_version`, `origin_location` or `after_task_id` — they are not
  decoded. A future use (e.g. publishing `InterleavingHintConsumed`
  analytics) would be a new, additive decision.
