---
id: 0062-pick-tasks-are-announced-to-the-open-task-pool
slug: /adr/0062-pick-tasks-are-announced-to-the-open-task-pool
title: 62. PICK tasks are announced to the open-task pool
sidebar_label: "62. PICK announced to the pool"
description: "ADR 0062 — a PICK whose plan carries both the release site (WorkReleased.data.pick.site_id) and the pick face (from_bin) gets a Placement and is announced with TaskAvailable, with the pick face as both ends of the trip; a pick missing either is not announced (omit-when-unknown) and can only be an anchor, never a companion."
---

# 0062. PICK tasks are announced to the open-task pool

## Status

Proposed (2026-11-06). Pick-batching wave 2b; consumes the pinned additive
producer contract of wes-work-planning's pick-site change (its ADR 0048).

## Context

`TaskAvailable` is published only for tasks with a determinable placement:
`Task.Placement()` consulted only the planned bins of RELOCATE and
REPLENISH moves, so a PICK always answered "no placement" — even a pick
plan with a `FromBin` (possible since ADR 0054) — and
`encodeTaskAvailable` dropped the event. The AsyncAPI even said "a PICK
task has no location in this service". Without an announcement
task-interleaving never sees a PICK: it cannot rank one, its existing
tie-break never applies, and the planned pick batching has nothing to form
batches from.

The wave-1 spike (pick-batching `spike-prerequisites.md`, Q5) also found
why nothing could fix this inside this service: 100% of order-driven PICK
releases carried no site and no `pick` object at all, and neither was
sourceable from the events wes-work-planning consumed. That gap is closed
upstream (wave 1.5) by an additive, omit-when-unknown producer contract —
pinned here as consumed, independently of the producing branch:

- `WorkReleased.data.pick` gains optional `site_id` (the site the pick is
  worked at) next to the existing optional `from_bin` (the pick face);
  both are omitted when the producer does not know them, and a payload
  that predates them decodes exactly as before.
- `WorkReleased` also publishes its top-level `site_id` for every work
  kind that has one; this service keeps reading the top-level field only
  for transfer facts, and the PICK placement from `pick.site_id`.

## Decision

1. `PickPlan` carries `SiteId`, decoded from `WorkReleased.data.pick.site_id`
   (absent = empty, behavior exactly as today) and persisted additively as
   `tasks.pick_site_id` (migration 0025; NULL rehydrates as empty).
2. `Task.Placement()` consults the pick plan: a pick with BOTH the site and
   the pick face yields `{SiteId: the plan's site, FromLocation: FromBin,
   ToLocation: FromBin}` — the station the operator ends at is not known
   at announce time, so the pick face stands for both ends of the trip,
   exactly like the one known bin of a RELOCATE Assign or Vacate. The site
   is the plan's own `site_id`, never derived from the bin (a pick face is
   not required to be a facility-layout LocationCode).
3. A pick missing either is NOT announced — omit-when-unknown, exactly like
   a RELOCATE whose bins carry no site. `encodeTaskAvailable` needed no PICK
   special case: its existing no-placement skip already covers it. An
   unannounced pick can only ever be a batch anchor, never a companion.
4. The AsyncAPI text claiming a PICK has no location is corrected
   (descriptions only; no field of this service's own events changes —
   `TaskAvailable` stays `:v1`, and the announced PICK uses the existing
   topic, schema and field set).

## Consequences

- **Publish volume.** Every released PICK whose plan carries both fields now
  publishes one `TaskAvailable` on `warehouse.fulfillment.events`; before
  this record PICKs published none. PICK is the fleet's largest work kind,
  so the announcement rate equals the sited-pick release rate. Every
  consumer of the topic must tolerate `task_type: "PICK"` on TaskAvailable;
  the only payload consumer is task-interleaving (the pinned consumer-first
  contract), whose PICK parity and load suite have already merged
  (its ADR 0012). Until the upstream propagation is complete the practical
  volume is small: order pick units still release no pick plan (a known
  fleet gap — order lines carry no quantity), so from_bin reaches only
  REST-enqueued picks; the volume grows when the per-line-quantity producer
  ask lands. task-interleaving's hint pool grows from moves to all sited
  picks; its recommender was load-tested for exactly that in wave 2c.
- Existing interleaving tie-breaks start applying to PICK with no other
  change in this repo; ClaimNext, `POST .../task-claims` and the lease
  rules are untouched (pinned by an end-to-end test: the announced pick is
  handed out by ClaimNext exactly as before, earliest CPT first).
- A legacy payload (no `pick.site_id`, no `pick.from_bin`, no `pick` object
  at all) decodes and behaves byte-identically to before: no plan change,
  no placement, no announcement — pinned by unit, golden and
  testcontainers tests.
- Rollback: revert the deploy; `tasks.pick_site_id` is nullable and unused
  by older code. No data migration needed in either direction.

## Addendum (2026-11-13): current state

Built and on `develop` (PR #279): `Task.Placement()` returns a placement for a PICK with both a site and a pick face, and none otherwise (`internal/domain/task/placement.go:32`, `pickPlacement` at `:65`); `tasks.pick_site_id` is migration `0025_task_pick_site_id`; the `TaskAvailable` description in `apis/asyncapi.yaml` states that a sited PICK is announced. The Status line above is unchanged.
