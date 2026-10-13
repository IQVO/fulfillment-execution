---
id: 0045-drop-legacy-domain-events-table
slug: /adr/0045-drop-legacy-domain-events-table
title: 45. Drop the legacy domain_events table
sidebar_label: "45. Drop legacy domain_events"
description: "ADR 0045 — migration 0018 drops domain_events, a table created by 0001_init that no code ever read or wrote. The owner approved the destructive step on 2026-10-12; the down migration recreates it empty."
---

# 45. Drop the legacy `domain_events` table

## Status

Accepted (2026-10-12).

## Context

`migrations/0001_init.up.sql` created a `domain_events` table (id, event_name,
occurred_at, payload) in the service's first iteration. Events have since left
the service only through the transactional outbox
([ADR-0020](./0020-transactional-outbox.md)): no adapter, use case or test
reads or writes `domain_events`. The 2026-10-12 ecosystem-gaps review listed it
as dead schema.

The fleet keeps migrations additive, so the earlier decision (2026-10-06) was
to keep the table until its owner explicitly approved a destructive change.
The owner gave that approval on 2026-10-12.

## Decision

Migration `0018_drop_domain_events` runs `DROP TABLE IF EXISTS domain_events`.
The down migration recreates the table empty, with the 0001 definition, so a
rollback restores the schema shape (there was never data to restore).

## Consequences

- One fewer table in the OLTP schema, and the data model pages no longer have
  to explain an unused table.
- The drop is safe for every running version: no released binary touches the
  table, so old and new pods can run side by side during a rollout.
- `TestMigrations_DropLegacyDomainEvents` (integration, testcontainers)
  proves the table is gone after migrations and the used tables are intact.
- Any future event store would be a new decision with its own table, not a
  revival of this one.
