---
id: 0004-kafka-integration-events-and-envelope
title: 4. Kafka for integration events, with a CloudEvents-typed catalogue
sidebar_label: 4. Kafka + event envelope
sidebar_position: 4
description: Integrate with sibling contexts over Kafka using a shared envelope, an AsyncAPI catalogue with DDD-coordinate CloudEvents types, and adapter-side enrichment.
---

# 4. Kafka for integration events, with a CloudEvents-typed catalogue

## Status

**Superseded by [ADR-0032](./0032-cloudevents-mandatory-envelope.md)** for
everything about the event envelope (2026-09-30): the flat
`event_id`/`event_type`/`occurred_at` envelope described below is retired,
and the spec-vs-wire gap recorded under Consequences is closed — every
message is now a CloudEvents 1.0 event. The rest of this record (Kafka as
the transport, the idempotency port, adapter-side enrichment, the `type`
convention) still stands.

Originally **Accepted**, with a known unresolved gap between the published
AsyncAPI contract and the then-current publisher.

## Context

This service has two integration needs with `wes-work-planning`:

1. **Inbound.** Work Planning releases work continuously (waveless). Each
   released unit must become a `Task` here.
2. **Outbound.** Work Planning needs to know when a released unit is actually
   done, so its own plan-versus-actual is real rather than assumed. The repo's
   integration notes name this the **drum-buffer-rope feedback edge:
   Execution → Orchestration**.

The forces:

- **Contexts must not share aggregates.** The reference model is emphatic: *"No
  shared aggregates across contexts… All cross-context communication is via
  integration events/published APIs — enforce this with an explicit
  Anti-Corruption Layer at each boundary."*
- **Synchronous coupling is wrong for this edge.** If Work Planning called this
  service over HTTP to create a task, a deploy here would stop work release.
  Neither side should be able to take the other down.
- **The platform already had a convention.** Four services already used the same
  flat envelope, `github.com/segmentio/kafka-go`, `KAFKA_BROKERS`, and a shared
  broker at `~/warehouse-systems/docker-compose.kafka.yml`. A fifth format
  would be a fifth thing to learn.
- **Delivery is at-least-once.** Redelivery is routine, not exceptional.
  Creating a duplicate `Task` from a redelivered `WorkReleased` would put
  phantom work on the floor.
- **Consumers need to route without parsing.** A subscriber on a
  multiplexed topic should be able to filter on one attribute.
- **The domain event and the wire payload have different requirements.** The
  domain's `TaskCompleted` carries `TaskId` and `StationId`. Work Planning
  needs a third field, `work_unit_id`, to correlate the completion to what it
  released.

## Decision

**We will integrate over Kafka, publish a documented event catalogue with
CloudEvents-style DDD-coordinate types, and enrich for the wire in the adapter
rather than in the domain.**

### Transport

- `github.com/segmentio/kafka-go` (pure Go, no cgo), brokers from
  `KAFKA_BROKERS` (default `localhost:9092`), against the platform's shared
  broker. This repo's `docker-compose.yml` deliberately defines **only
  Postgres** — a second broker would fragment integration testing.
- Consume `warehouse.work-planning.events`, filtering on the CloudEvents
  `type` `com.warehouse.wes.work-planning.workunit.WorkReleased` (ADR-0032;
  originally a flat `event_type == "WorkReleased"` filter), consumer group
  `fulfillment-execution`.
- Publish `TaskCompleted` to `warehouse.fulfillment.events`, keyed by task id
  so all events for one task share a partition and preserve order.
- Publisher selection is env-driven — `EVENT_PUBLISHER=log|kafka`, default
  `log` — so tests and local runs need no broker, and no use case knows which
  implementation it received.

### The catalogue and the `type` convention

`apis/asyncapi.yaml` (AsyncAPI 2.6.0) documents **eleven of the thirteen**
domain events as the published catalogue (the two Rebin events,
`ItemArrivedAtRebin` and `OrderConsolidated`, are in-process only and not in
the spec — see the [Events page](../api-reference/events.md)), with each
message stating its current publication
status. Event types follow the platform-wide convention:

```
com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>
com.warehouse.wes.fulfillment-execution.task.TaskCompleted
```

The DDD coordinates are encoded *in the type string* so a consumer can route
or filter purely on `type` without opening `data` — and so the name itself
says which context owns the fact and which aggregate raised it.

### Anti-Corruption Layer on the inbound edge

> **Historical for the `path_id` mapping — superseded by
> [ADR-0017](./0017-process-path-catalogue-as-configuration.md).** The text
> below describes the original prefix-convention guess. Today `path_id` is
> resolved through the process-path catalogue (`ports.PathCatalogue`, file or
> Kafka source) and an unknown id is a hard error; capabilities come from the
> catalogue entry, not from the type.

The consumer decodes the envelope and extracts three scalars, mapping each into
this context's own vocabulary — `path_id` → `task.Type` (by prefix
convention), `work_unit_id` → `shared.OrderRef`, `cpt` → `shared.CPT`, with
capabilities derived from the type. No upstream struct crosses the boundary,
and it calls the **existing** `CreateTask` use case rather than a parallel one.

### Idempotency as a port

`ports.ProcessedEvents.MarkProcessed(ctx, eventId) (bool, error)` returns
`true` only if this call newly recorded the id, so check-and-record is atomic
rather than a read-then-write race. Postgres backs it with
`processed_events (event_id TEXT PRIMARY KEY, processed_at TIMESTAMPTZ)` —
the primary-key constraint *is* the deduplication; the memory adapter uses a
mutex-guarded map.

### Enrichment happens in the adapter

`TaskCompleted` stays thin in the domain. The Kafka publisher looks the task
back up through `ports.TaskRepo` and reads `OrderRef()` to populate
`work_unit_id`. The correlation key exists because a specific downstream needs
it; letting that requirement reshape the domain event would be the tail wagging
the dog. The same repo-lookup-enrichment pattern is used by
`inventory-storage`'s publisher for `ReservationRevoked`, so it is a platform
convention rather than a local improvisation.

## Consequences

### Easier

- **The two contexts deploy independently.** Neither can take the other down;
  Kafka retains messages across a restart.
- **The domain model stays clean.** No integration field was added to any
  aggregate or domain event to serve a consumer's join.
- **Redelivery is safe.** The idempotency test feeds the same `event_id` twice
  and asserts exactly one task exists.
- **The contract is machine-checked.** `apis/asyncapi.yaml` and
  `apis/openapi.yaml` are both linted by Spectral in the `api-lint` CI job.
- **New consumers need no change here.** Anything can subscribe to
  `warehouse.fulfillment.events` and filter on the event type.
- **Local development needs no broker.** The default `log` publisher means
  `go run ./cmd/execution` works with nothing else running.

### Harder

- **The published spec and the wire format have diverged.** This is the real
  outstanding cost and it is worth stating precisely. `apis/asyncapi.yaml`
  specifies a CloudEvents 1.0 structured envelope (`specversion`/`id`/`source`/
  `type`/`subject`/`time`/`datacontenttype`/`data`) on channel
  `warehouse.fulfillment-execution.events`. The publisher today writes the
  older flat platform envelope (`event_id`/`event_type`/`occurred_at`/`source`/
  `data`) to `warehouse.fulfillment.events`, with snake_case payload keys.
  `wes-work-planning` reads *that* shape, so the live integration is correct
  and working — but the document describes where the platform is going, not
  where the code is. Migrating requires both sides to move together, because
  the topic name changes too. Spectral validates the spec's internal
  consistency, not the code's conformance to it, so nothing catches this
  automatically.
- **Three of thirteen domain events are actually published.** The catalogue
  documents eleven of the thirteen (two Rebin events are in-process only and
  not in `asyncapi.yaml` at all); of those eleven, `TaskCompleted`,
  `TaskCPTMissed`, and `PackageManifested` are actually on the wire — the
  other eight are in-process only. That is honest in the spec and on the
  [Events page](../api-reference/events.md), but a consumer reading the
  catalogue alone could over-estimate what is available.
- **Eventual consistency between contexts.** Work Planning's view of
  completion lags reality by the publish-plus-consume latency. Acceptable
  here, but it is a real property of the design.
- **Enrichment adds a read on the publish path.** Every `TaskCompleted`
  publish does a `TaskRepo.FindById`. Cheap, but it means publishing is no
  longer a pure function of the event.
- **The `path_id` prefix convention is fragile.** *(Historical — resolved by
  [ADR-0017](./0017-process-path-catalogue-as-configuration.md): `path_id` is
  now resolved through the process-path catalogue and an unrecognised id is a
  hard error, not a silent Pick.)* `pick-*`/`pack-*`/`slam-*`
  with a `PICK` default means an unrecognised `path_id` silently produces a
  Pick task. Documented as a known simplification in `INTEGRATION.md`, the
  README, and the [Integration contracts](../ecosystem/integration-contracts.md)
  page.
- **Dead-letter queue.** A message that fails to process is now published to
  `<topic>.dlq` (original key/value preserved, plus `x-dlq-error` and other
  failure-context headers) instead of only being logged — see
  `internal/adapters/inbound/kafka/consumer.go`'s `SendToDeadLetter`. This
  makes messages inspectable/replayable instead of silently lost, but does
  not change the ordering caveat below: because `MarkProcessed` runs
  *before* `CreateTask`, an event whose task creation fails and is
  dead-lettered is still recorded as already-processed, so replaying it off
  the DLQ requires clearing its `MarkProcessed` record first (or a
  compensating uniqueness check) — DLQ delivery is not itself automatic
  retry.
- **In-memory idempotency does not survive restart.** Running with
  `EVENT_PUBLISHER=kafka` but no `DATABASE_URL` gives process-lifetime
  deduplication only.

## Addendum (2026-09-30)

Superseded for the envelope by
[ADR-0032](./0032-cloudevents-mandatory-envelope.md): CloudEvents 1.0 is
now mandatory for every Kafka message this service produces or consumes,
with no flat or dual mode. The "published spec and the wire format have
diverged" consequence above no longer holds.

## Addendum (2026-09-26)

The CloudEvents migration referenced above as a future intent now has a
concrete plan: [ADR-0027](./0027-cloudevents-envelope-migration.md)
(Proposed), a companion to wes-work-planning's own envelope-migration ADR
(ADR-0021). It implements the CloudEvents envelope this ADR documented but
never shipped, via a non-breaking dual-read/dual-write bake period. See
ADR-0027 for the schema, phase sequence, and rollback plan.
