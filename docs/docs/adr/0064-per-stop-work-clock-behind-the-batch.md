---
id: 0064-per-stop-work-clock-behind-the-batch
slug: /adr/0064-per-stop-work-clock-behind-the-batch
title: 64. The work clock runs per stop, behind the batch
sidebar_label: "64. Per-stop work clock"
description: "ADR 0064 — tasks carry work_started_at (NULL = the claim time, so every pre-batching row keeps today's duration), a batched pick's clock starts when the previous stop completes, and publisher_encode uses DurationBase() so a non-batched TaskCompleted is byte-identical to before."
---

# 0064. The per-stop work clock

## Status

Proposed (2026-11-08). Pick-batching wave 3; companion to ADR 0063.

## Context

`TaskCompleted.duration_seconds` is completion minus claim time, and
`labor-performance` recovers each claim instant from it. If a batched
picker's first stop is claimed at 10:00 and the last stop completes at
10:20, a claim-based clock books all twenty minutes against every stop:
batched pickers look slow, exactly the failure mode the plan flagged
("labor timing must stay per stop"). Each stop's clock must start when the
previous stop completes — the NextStopClock policy — and nothing outside a
batch may change by a byte, because `labor-performance` parses today's
payloads as-is.

## Decision

- `Task` gains `work_started_at` (migration 0026, nullable). **NULL means
  the claim time**, so every existing row and every task outside a batch
  keeps exactly today's duration.
- `StartWork(at)` stamps it. The batch lifecycle hooks call it: the first
  member's clock starts at its claim (identical to today), each later
  member when the previous stop completes. A NULL-safe `DurationBase()`
  returns `work_started_at` when set, else the claim time.
- `publisher_encode.go` uses `DurationBase()` in place of `ClaimedAt()`
  for `duration_seconds`. The golden test for a non-batched task
  (work_started_at NULL) stays **byte-identical**, which is the proof the
  change is invisible outside a batch.

## Consequences

- Per-stop durations sum to the walk with zero idle gap between stops and
  the true idle gap only after the batch's last stop, which is what
  labor-performance's idleness recovery needs.
- No consumer change: the field is the same `duration_seconds` v1 payload,
  only its meaning sharpens for batched picks, and `batch_id` (ADR 0063)
  is how the consumer tells the two apart.
- Standards (ADR 0004) stay frozen; whether batched picks beating a
  single-pick standard warrants a revision is a human decision after wave 4
  data.
