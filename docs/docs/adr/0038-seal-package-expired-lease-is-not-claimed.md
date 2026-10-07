---
id: 0038-seal-package-expired-lease-is-not-claimed
slug: /adr/0038-seal-package-expired-lease-is-not-claimed
title: 38. SealPackage with a missing or expired lease returns task-not-claimed
sidebar_label: "38. Seal: expired lease is not-claimed"
description: "ADR 0038 — Task.VerifyHeldBy returns ErrNotClaimed (409 task-not-claimed) for a missing or expired lease, like Complete and RenewLease; a lease actively held by another station stays ErrNotOwner (409 task-not-owner)."
---

# 38. SealPackage with a missing or expired lease returns task-not-claimed

## Status

Accepted. Decided 2026-10-06 (audit decision 12b). Refines the error table in
[ADR-0005](./0005-rfc-7807-problem-details.md) and the owner-only rule in
[ADR-0003](./0003-lease-based-at-most-once-claiming.md); neither body is
edited. The HTTP status (`409`) and the set of problem types are unchanged.

## Context

`Complete` and `RenewLease` treat a lapsed lease as "the task is no longer
claimed": they check expiry first, free the task and return `ErrNotClaimed`
(`409 task-not-claimed`); only a live lease held by someone else is
`ErrNotOwner` (`409 task-not-owner`).

`SealPackage` guards on `Task.VerifyHeldBy`, a read-only variant. It returned
`ErrNotOwner` for **every** failure — no lease, another station's lease, or an
expired lease. So the same condition (a lease that has lapsed) produced
`task-not-claimed` on `complete` / `renew-lease` and `task-not-owner` on
`seal-package`. ADR-0005 tells clients that `task-not-owner` means "my lease
lapsed and someone else has it"; for an expired-but-unclaimed task that was
misleading, and a client branching on `type` needed endpoint-specific logic.

## Decision

`Task.VerifyHeldBy(stationId, now)` now:

1. no lease, **or** a lease expired at `now` (inclusive boundary, whoever held
   it) → `ErrNotClaimed` (`409 task-not-claimed`);
2. an active lease held by a **different** station → `ErrNotOwner`
   (`409 task-not-owner`);
3. otherwise `nil`.

The order mirrors `RenewLease` (expiry before ownership), so the same
condition yields the same error on every endpoint. `VerifyHeldBy` stays
read-only: it does not free the task (unlike `Complete`/`RenewLease`), because
`SealPackage` persists nothing on the task.

`internal/adapters/inbound/http/errors.go` is unchanged: both errors were
already mapped to `409` and have their own problem-catalog entry, so
`statusFor` / `problemCatalog` stay one-for-one. The OpenAPI `409` text for
`POST /tasks/{id}/seal-package` describes both cases and the generated API
reference was regenerated.

## Consequences

### Easier

- One condition, one error, across `complete`, `renew-lease` and
  `seal-package`; clients branch on `type` without per-endpoint rules.
- `task-not-owner` regains a single meaning: *another station holds an active
  lease*.

### Harder

- **Behaviour change for a narrow case.** A client that treated `409
  task-not-owner` from `seal-package` as "re-claim" must now also handle `409
  task-not-claimed` the same way. Status code, body shape and the set of
  problem types are unchanged, so the change is additive for clients that
  already handle both types from `complete`. Ecosystem check
  (origin/develop of the other fleet repos): the only caller of
  `seal-package` is e2e-tests' `warehouse-day` simulator
  (`cmd/warehouse-day/floor.go`), which treats any non-2xx as a logged
  finding and never branches on the problem type; no other repo references it.
- Existing ADR text (0003 "Only the owner may renew or complete" and 0005's
  description of `task-not-owner`) remains as originally written; this ADR is
  the current statement for the expired/missing-lease case.
