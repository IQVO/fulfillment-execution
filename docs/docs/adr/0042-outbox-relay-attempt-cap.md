---
id: 0042-outbox-relay-attempt-cap
slug: /adr/0042-outbox-relay-attempt-cap
title: 42. Outbox relay attempt cap (OUTBOX_MAX_ATTEMPTS)
sidebar_label: "42. Outbox relay attempt cap"
description: "ADR 0042 — the outbox relay excludes a row from its claims once it has failed OUTBOX_MAX_ATTEMPTS sends (default 20, 0 = unlimited), logs it at ERROR and leaves it in outbox_events for inspection, so one poison row can no longer stop the relay forever. An excluded row is overtaken by later rows with the same key."
---

# 42. Outbox relay attempt cap (OUTBOX_MAX_ATTEMPTS)

## Status

Accepted. Decided 2026-10-12 (fleet gap-fix wave, decision D12). Amends the
relay retry behaviour of [ADR-0020](./0020-transactional-outbox.md); that
ADR's body is not edited. It follows the fleet convention that
inventory-storage introduced first (same option name, env var, default and
log fields).

## Context

The relay of [ADR-0020](./0020-transactional-outbox.md) drains
`outbox_events` one row at a time in id order and stops a pass at the first
failed send, recording `attempts` and `last_error` on that row. That keeps
per-key ordering, but a row the broker will never accept (a message over the
broker's size limit, a topic that no longer exists, a payload a broker-side
policy rejects) is retried forever: every later row, of every key and both
topics, waits behind it. One poison row stops the whole relay, and with it
every integration and analytics event this service publishes.

## Decision

1. **`postgres.WithMaxAttempts(n)`.** With `n > 0` the relay's claim query
   only selects rows with `attempts < n`. A row that has failed `n` sends is
   excluded from future claims. It is **not** deleted or moved: it stays in
   `outbox_events` with `published_at IS NULL`, its `attempts` and its
   `last_error`, so an operator can inspect it.
2. **Logged at ERROR once.** The failed send that brings a row to the cap is
   logged at ERROR, after the pass commits, with `outbox_id`, `topic`,
   `event_type`, `key`, `attempts`, `max_attempts` and the error. Failures
   below the cap are logged at WARN.
3. **Configuration.** `cmd/execution` (the only process that runs the relay)
   reads `OUTBOX_MAX_ATTEMPTS`: default `20`
   (`postgres.DefaultOutboxMaxAttempts`), `0` means unlimited (the previous
   behaviour), a malformed or negative value falls back to the default with a
   WARN instead of failing the boot. It reaches the relay through
   `composition.PublisherConfig.RelayMaxAttempts`. The chart value is
   `config.outboxMaxAttempts`.
4. **Re-arming.** After fixing the cause, an operator re-arms a row with
   `UPDATE outbox_events SET attempts = 0 WHERE id = ...`; the next pass
   claims it again (runbook).
5. **Sensor.** The architecture fitness test
   `TestOutboxRelaysPassAttemptCap` fails when a non-test file under `cmd/`
   or `internal/composition/` (where this repo builds the relay) calls
   `NewOutboxRelay` without an inline `WithMaxAttempts(...)`.

## Consequences

### Easier

- One poison row costs at most `OUTBOX_MAX_ATTEMPTS` failed passes, then the
  relay moves on. The rest of the outbox keeps flowing.
- The excluded row is visible (ERROR log, the row itself) instead of a
  silently growing lag.

### Harder

- **Per-key ordering is given up for an excluded row.** Rows behind it,
  including rows with the **same key** (the same task or package), are
  published before it. A consumer can see a later state of an aggregate
  without the earlier event, and sees the earlier one only if an operator
  re-arms it (then out of order). That is the deliberate trade: ordering is
  kept while a row is retried, and broken only once a row has failed `N`
  times and is presumed poison.
- A long broker outage also counts as failures: with the default `20` and
  the relay's `1s` interval, a row excluded during a broker outage stays
  excluded after the broker comes back and must be re-armed. Operators who
  prefer to wait out outages can set `OUTBOX_MAX_ATTEMPTS=0`, or a larger
  value.
- No dead-letter topic is introduced for the outbox: the table itself holds
  the excluded rows.
