---
id: 0035-kafka-hash-partition-key
slug: /adr/0035-kafka-hash-partition-key
title: 35. Kafka hash partition key — every message keyed by its aggregate id
sidebar_label: 35. Kafka partition key
description: "ADR 0035 — every outbound Kafka writer uses the Hash balancer over a per-aggregate message key (TaskId/PackageId), so all events of one aggregate land on one partition and keep their order on the 8-partition business topics. Also records the shared synchronous-writer settings (RequireAll, 10ms BatchTimeout) and the boot-time retry plus startupProbe that cover the fleet's first-dial reset."
---

# 35. Kafka hash partition key — every message keyed by its aggregate id

## Status

Accepted. Records a decision that was implemented (and verified against a
real broker) without an ADR. Complements
[ADR-0004](./0004-kafka-integration-events-and-envelope.md) (transport),
[ADR-0020](./0020-transactional-outbox.md) (the relay is the writer) and
[ADR-0032](./0032-cloudevents-mandatory-envelope.md) (CloudEvents `subject`).

## Context

Every business topic runs with 8 partitions. Kafka only orders messages
**within a partition**, so per-aggregate ordering (`TaskCreated` →
`TaskClaimed` → `TaskCompleted` for one task; `PackageSealed` →
`LabelApplied` → `PackageManifested` for one package) holds only if every
message of an aggregate reaches the same partition.

This service always set a non-nil, per-aggregate `Message.Key` (the `TaskId` or
`PackageId`, also used as the CloudEvents `subject`). But the writers were
built with kafka-go's `LeastBytes` balancer, which routes purely by cumulative
byte volume and **ignores `Message.Key`**. A correct key combined with the
wrong balancer is a silent bug class: nothing fails, events of one aggregate
simply start interleaving across partitions, and consumers see them out of
order. A fake-writer unit test cannot catch it, because a fake never simulates
partition routing.

Two adjacent settings turned out to matter for the same writers:

- kafka-go's default `RequiredAcks` is `RequireNone`: `WriteMessages` returns
  without waiting for the broker, so the outbox relay marked rows published
  that the broker never stored — silently at-most-once, the opposite of the
  outbox's guarantee.
- kafka-go's default `BatchTimeout` is 1s: a synchronous write waits a full
  second for a batch that never fills, capping a one-row-per-call relay at
  about 1 event/s.

## Decision

1. **Every Kafka writer in this service uses `Balancer: &kafkago.Hash{}`**
   (FNV-1a over `Message.Key`), and every message carries the aggregate id as
   its key:
   - `Publisher` (integration topic) and `AnalyticsPublisher` (analytics
     topic): `internal/adapters/outbound/kafka/publisher.go` /
     `analytics_publisher.go` — key = `TaskId`/`PackageId`;
   - `RelaySink`, the topic-less writer the outbox relay uses
     (`encoded.go`): forwards each `Encoded.Key` verbatim;
   - the WorkReleased consumer's dead-letter writer
     (`NewDeadLetterWriter` in `writer_config.go`): the original message key
     is preserved on `<topic>.dlq`, so per-key order survives dead-lettering.
2. **All synchronous writers share one configuration** (`writer_config.go`):
   `RequiredAcks: RequireAll` and `BatchTimeout: 10ms`. `RequireAll` is the
   durable choice (on the single-broker kind cluster it equals the leader's
   ack); 10ms keeps writes batched under load while flushing a lone event
   almost immediately.
3. **The contract is verified against a real broker**, not a fake:
   `partition_key_integration_test.go` starts Kafka with testcontainers
   (never `KAFKA_BROKERS`/localhost, so CI cannot skip it) and asserts that
   same-key messages from the integration publisher, the analytics publisher
   and the relay sink land on one partition. The dead-letter writer is covered
   by a unit test on its configuration (`cmd/execution/dlq_writer_test.go`),
   since it reuses the same `kafkago.Hash` balancer.
4. **Boot-time dials are retried, and probed with a `startupProbe`.** The
   fleet's Istio native sidecars reset every pod's first outbound TCP dial
   ~10s after start. `internal/adapters/outbound/bootretry` retries the
   boot-time dials (migrations, pool ping, the catalogue consumer's initial
   broker dial) with exponential backoff (~31s budget), and the chart's
   `startupProbe` (`failureThreshold × periodSeconds` = 60s) keeps liveness
   and readiness probing from killing the pod while that retry runs. This is
   about the same Kafka connections, not about partitioning, and is recorded
   here so the writer/dial settings live in one place.

## Consequences

### Easier

- Per-aggregate event order holds on the 8-partition topics, so consumers
  (order-management's `RepromiseOrder`, labor-performance, the analytics
  projector) can rely on `TaskCreated` before `TaskCompleted` for one task.
- The outbox's at-least-once, no-silent-loss guarantee is real end to end:
  `RequireAll` means a row is marked published only after the broker has it.
- One place (`writer_config.go`) to change a writer setting for every path.

### Harder

- **Hot keys.** An aggregate id maps to one partition, so a very busy
  aggregate cannot be spread over partitions. Aggregates here are short-lived
  (one task, one package), so this is theoretical today.
- **Changing the partition count remaps keys.** Adding partitions changes the
  hash→partition mapping, so ordering is only guaranteed within a partition
  count's lifetime; a partition increase needs a drain-and-cutover plan.
- **`RequireAll` costs latency** compared with `RequireNone` — acceptable for
  a transactional-outbox relay that is already asynchronous to the request.
- **The key is not cross-aggregate.** Events of *different* aggregates (a task
  and the package sealed from it) can still arrive in any relative order;
  consumers must correlate by id, not by arrival order.
