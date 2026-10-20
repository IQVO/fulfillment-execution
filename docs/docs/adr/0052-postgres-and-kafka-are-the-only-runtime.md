---
id: 0052-postgres-and-kafka-are-the-only-runtime
slug: /adr/0052-postgres-and-kafka-are-the-only-runtime
title: 52. Postgres and Kafka are the only runtime of every binary
sidebar_label: "52. Postgres and Kafka are the only runtime"
description: "ADR 0052 — every shipped binary of fulfillment-execution runs only on a real Postgres and a real Kafka: a missing DATABASE_URL, KAFKA_BROKERS or consumer group is a boot error naming the variable, there is no in-memory storage, log publisher or permissive/off mode, /readyz reflects both dependencies, and a fitness test keeps the memory adapters out of cmd/."
---

# 52. Postgres and Kafka are the only runtime of every binary

## Status

Accepted (2026-10-19). Fleet decision of the same date: every bounded context
runs only on a real database and a real broker; the in-memory adapters are
test doubles. Amends [ADR-0001](./0001-hexagonal-ports-and-adapters.md) (the
publisher swap is no longer a runtime choice), [ADR-0004](./0004-kafka-integration-events-and-envelope.md)
(`EVENT_PUBLISHER=log|kafka`, default `log`, and the `localhost:9092` default),
[ADR-0020](./0020-transactional-outbox.md) (the "no `DATABASE_URL`" rows of the
wiring table), [ADR-0029](./0029-resilience-circuit-breakers-retry-dlq-shutdown.md)
(what `/readyz` checks), [ADR-0039](./0039-product-classification-local-copy.md)
and [ADR-0051](./0051-interleaving-hints-advise-claimnext.md) (`permissive` and
`off` are gone; `kafka` is the only mode). The bodies of those records are not
edited. No public event shape, topic name or REST route changes.

## Context

Until now `cmd/execution` could start with no `DATABASE_URL` (in-memory
repositories, lost on restart), with `EVENT_PUBLISHER` unset (a log sink that
sends nothing), with `KAFKA_BROKERS` unset (a `localhost:9092` default that is
never right in a cluster), and with the two local-copy consumers in
`permissive`. Every one of those starts, answers `/healthz` and `/readyz` with
200, and silently does less than the deployment believes: a pod without Kafka
accepted work and published nothing. `cmd/mcp` and `cmd/fulfillment-projector`
had the same shape (the projector defaulted its broker to `localhost:9092`).
The Schemathesis `contract` job ran against the in-memory boot, so it never
exercised a Postgres text column. PostgreSQL text cannot hold U+0000: a
statement that carries one aborts the surrounding transaction (SQLSTATE 22021,
then 25P02 for the next statement), which on the real runtime is a 500 after the
Idempotency-Key was recorded, and the schema has no way to say "no U+0000".

## Decision

1. **No degraded mode in any binary under `cmd/`.** `cmd/execution`,
   `cmd/mcp`, `cmd/fulfillment-projector`, `cmd/fulfillment-reports` and
   `cmd/sweep` run on Postgres and Kafka only. The in-memory repositories and
   `events` log/buffered/multi publishers stay in the tree as test doubles; the
   wall clock moved to `internal/adapters/outbound/system` so no binary needs
   the memory package. `TestRuntimeBinaryHasNoInMemoryAdapters`
   (`internal/architecture`) fails the build when a non-test file under `cmd/`
   imports the memory package, the log/buffered/multi publishers or the
   permissive/in-memory copies of the two local-copy adapters, and
   `TestRuntimeBinarySensorBites` proves the detector fires.
2. **Misconfiguration is a boot error that names the variable**, raised before
   the listener binds or anything dials. Required: `DATABASE_URL`,
   `KAFKA_BROKERS` (no default), `WORK_RELEASED_CONSUMER_GROUP`,
   `PRODUCT_CLASSIFICATION_CONSUMER_GROUP`, `TASK_INTERLEAVING_CONSUMER_GROUP`
   for `cmd/execution`; `DATABASE_URL` and `KAFKA_BROKERS` for `cmd/mcp`;
   `ANALYTICS_DATABASE_URL` and `KAFKA_BROKERS` for the projector.
   `EVENT_PUBLISHER`, `PRODUCT_CLASSIFICATION_MODE` and `TASK_INTERLEAVING_MODE`
   remain accepted but only with the value `kafka` (or unset); `log`,
   `permissive`, `off`, and the earlier-removed `http` fail the boot, so a stale
   deployment fails loudly instead of degrading. `MIGRATIONS_DATABASE_URL`
   still falls back to `DATABASE_URL` (ADR-0031).
3. **Kafka consumers and the outbox relay always run in `cmd/execution`.** The
   relay and the `WorkReleased` dead-letter writer no longer depend on a mode.
   `cmd/mcp` writes `outbox_events` in the use case's transaction and does not
   run a relay (unchanged: only `cmd/execution` drains). `run()` returns a
   non-zero error when a consumer or the relay stops on its own.
4. **`/readyz` is a dependency check, `/healthz` stays liveness.** `/readyz`
   is 200 only while the process is not draining, a Postgres ping succeeds and
   a Kafka broker answers a metadata request (and, with
   `PATH_CATALOGUE_SOURCE=kafka`, after the catalogue replay, ADR-0044). The
   verdict is cached for 3 s and each probe is bounded at 2 s. Otherwise it is
   503 `application/problem+json` of type `dependency-unavailable` naming the
   failing dependency. **It never checks a topic**: the metadata request names
   none, so it cannot auto-create one, and the relay and DLQ writers create
   `warehouse.fulfillment.events`, `warehouse.fulfillment.analytics` and the
   `.dlq` topic on first write (`AllowAutoTopicCreation`, as before). A pod
   that required its output topics to be ready would never receive the traffic
   that creates them.
5. **The Idempotency-Key is required in every deployment.** `apis/openapi.yaml`
   declares the header `required: true` on `POST /tasks`; `400
   idempotency-key-required` was already the answer on Postgres and is now the
   only one.
6. **A NUL (U+0000) is a client error.** `rejectNUL` runs before routing: 404
   for a NUL in the path (it names no resource), 400 `invalid-request` in the
   query, 400 `malformed-request-body` in a body (raw byte or the `\u0000`
   escape; an escaped backslash followed by `u0000` is not a NUL). Nothing is
   recorded, so the Idempotency-Key stays free for the clean retry.
7. **The contract job runs on the real runtime.** `contract` in CI uses
   `postgres:16` and `confluentinc/confluent-local:7.6.1` service containers,
   waits on `/readyz`, passes `--generation-allow-x00 false` (the schema cannot
   say "no U+0000") and a `before_call` hook that gives each request a fresh
   Idempotency-Key.
8. **Helm.** The chart refuses to render without `database.url` or
   `database.existingSecret` and without `kafka.brokers`; `config.eventPublisher`
   defaults to `kafka`; the three consumer groups are chart values (the old
   `fulfillment-execution` default for `WORK_RELEASED_CONSUMER_GROUP` is kept so
   committed offsets survive) and `extraEnv` wins when it names a group, so
   warehouse-infra never receives a duplicate env entry. `kafka.enabled` and
   `mcp.eventPublisher` are removed.

## Consequences

- A broken Postgres or Kafka now takes the pod out of the Service endpoints
  instead of letting it accept work it cannot persist or publish. The cost is
  that a broker outage removes every replica from rotation, including read-only
  REST traffic; a flapping broker can flap readiness (the 3 s cache damps it).
- The topic convention is untouched: topics are auto-created on first write and
  `/readyz` does not require them, so a fresh cluster still comes up.
  Consumers still need their topics to exist when they start (a kafka-go group
  consumer that starts before its topic exists does not pick it up later); the
  integration test pre-creates the three consumed topics, as the cluster does.
- Local development needs Postgres and Kafka (`docker compose up`, or the
  commands in `scripts/contract-test.sh`). Unit, httptest and BDD tests keep
  using the in-memory doubles and need nothing.
- Existing deployments must provide `WORK_RELEASED_CONSUMER_GROUP`,
  `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` and `TASK_INTERLEAVING_CONSUMER_GROUP`
  (the chart now does by default) and must not set `EVENT_PUBLISHER=log`.
- Out of scope and unchanged: the synchronous-edge `*_MODE` variables toward
  sibling contexts (`LOCATION_ROLE_MODE`, and `FACILITY_LAYOUT_BASE_URL`), the
  circuit-breaker behaviour, and every event schema and topic name.
