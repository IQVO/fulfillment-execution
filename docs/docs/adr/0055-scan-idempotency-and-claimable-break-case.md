---
id: 0055-scan-idempotency-and-claimable-break-case
slug: /adr/0055-scan-idempotency-and-claimable-break-case
title: 55. Idempotent pick scans; BREAK_CASE is claimable
sidebar_label: "55. Idempotent scans; claimable BREAK_CASE"
description: "ADR 0055 — amends ADR 0054. POST /tasks/{id}/scans requires an Idempotency-Key and replays the stored answer on a retry, so a lost response never double-counts a scan; claim-next accepts taskType BREAK_CASE."
---

# 55. Idempotent pick scans; BREAK_CASE is claimable

## Status

Proposed (2026-10-24). Amends [ADR-0054](./0054-case-and-each-pick-scans-and-break-case-task.md).
Reuses the middleware of [ADR-0028](./0028-idempotency-key-middleware.md).

## Context

A scan adds eaches to the task's pick count. The warehouse-floor-client keeps its commands in an offline
outbox and resends them with the same `Idempotency-Key` when a response is lost. Without a de-duplication
the resend counts the scan twice, and the pick reaches its planned quantity (or `pick-over-quantity`) early.
ADR-0054 also left `BREAK_CASE` out of `AnyTaskType`, so a station could not claim the task it must complete.

## Decision

- `POST /tasks/{id}/scans` runs behind `RequireIdempotencyKey`, inside the scan's own transaction. Same key
  and same body on the same path replays the stored status and body and does not apply the scan again
  (a stored 4xx is replayed too). A missing key is `400 idempotency-key-required`. The same key with a
  different body is `422 idempotency-key-reused`, as for `POST /tasks`.
- Each physical scan uses its own key, so two scans with different keys both count.
- The key is one table key across routes and the scan body carries no task id, so the middleware now also
  compares the stored path: the same key and body on another task is `422 idempotency-key-reused`, not a replay.
  The 400 detail no longer says the endpoint "creates a resource".
- `BREAK_CASE` joins the claimable task types: `claim-next` `taskType`, the `AnyTaskType` enum and the MCP
  process-path argument accept it. Station capabilities are matched as for every type, against the
  capabilities stamped on the task at release. `BREAK_CASE` is still not creatable through `POST /tasks`, and
  `POST /tasks/{id}/break-case` stays the only way to complete it (`POST /tasks/{id}/complete` is refused).

## Consequences

- Breaking for callers of `POST /tasks/{id}/scans` that send no key: they get 400 until they send one.
- Quantities stay in eaches; `TaskCompleted` and the break call to inventory-storage are unchanged.
