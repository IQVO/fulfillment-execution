---
id: 0046-workreleased-consumer-commits-after-settle
slug: /adr/0046-workreleased-consumer-commits-after-settle
title: 46. The WorkReleased consumer commits after settling and never dead-letters a cancelled message
sidebar_label: "46. WorkReleased commit-after-settle"
description: "ADR 0046 — the WorkReleased consumer fetches with FetchMessage and commits only once a message is applied or dead-lettered; a shutdown that interrupts a healthy message is a clean stop that neither dead-letters nor commits it, so it is redelivered and the CloudEvents-id claim absorbs the replay. Amends ADR 0029."
---

# 46. The WorkReleased consumer commits after settling and never dead-letters a cancelled message

## Status

Accepted (2026-10-13). Amends [ADR-0029](./0029-resilience-circuit-breakers-retry-dlq-shutdown.md)
§5 ("the offset is auto-committed by `ReadMessage` before `Handle` runs").
Fleet rollout of the network-fulfillment escape of 2026-10-13
(IQVO/network-fulfillment#89).

## Context

`internal/adapters/inbound/kafka/consumer.go` read `WorkReleased` with
`ReadMessage`, which commits the consumer-group offset before the message is
handled, and dead-lettered any message whose bounded retry
(`handleMessageWithRetry`, 3 attempts) failed.

When the pod is stopped while a healthy `WorkReleased` is mid-handle, the
retry wrapper fails with `context.Canceled`. `Run` treated that like an
exhausted poison message: it logged `kafka message handling failed after
retries` and published the message to `warehouse.work-planning.events.dlq`
on the already-cancelled context. Two outcomes were possible, both wrong:

- kafka-go's `Writer` can still flush a batch it accepted after the caller's
  context is done, so the healthy message landed on the DLQ (with
  `x-dlq-error: context canceled`) and its Task was never created; or
- the DLQ publish failed, and because the offset was already committed the
  `WorkReleased` was lost for good.

The fleet's consumer contract (the shutdown rollout of 2026-10-13) is: never
dead-letter a message that only cancellation interrupted, and leave its
offset uncommitted so it is redelivered. With `ReadMessage` the second half is
impossible, so skipping the DLQ alone would turn every such shutdown into a
silent loss.

## Decision

1. The `WorkReleased` consumer reads with `FetchMessage` (through
   `fetchRecovering`, the same broker-outage recovery as before) and calls
   `CommitMessages` only once the message is **settled**: applied, ignored
   (another event type), or published to the DLQ.
2. When `Handle` fails and the consumer's context is done, the message is
   neither dead-lettered nor committed and `Run` returns `nil` — a clean stop.
   On the next start the message is redelivered; `ApplyWorkReleased` claims
   the CloudEvents id in the same unit of work as the Task, so the replay is
   applied exactly once.
3. Deterministic poison (not a CloudEvent, unknown `path_id`, unknown
   `work_kind`) and exhausted transient failures are still dead-lettered and
   then committed, exactly as before. A failed DLQ publish is still logged and
   the loop goes on (the message is committed, as before).
4. A failed commit is logged at WARN (`kafka commit failed; the message will
   be redelivered and deduplicated`) and the loop goes on; when the context is
   done it is a clean stop.

## Consequences

- No healthy `WorkReleased` reaches the DLQ because of a rollout, and none is
  lost: the integration test
  `TestRun_ShutdownMidHandle_IsCleanDeadLettersNothingAndRedelivers`
  (testcontainers Kafka) parks the use case, cancels `Run`, and asserts `Run`
  returns nil, the DLQ stays empty, and a restarted consumer in the same group
  creates the Task.
- Delivery for this consumer is now at-least-once instead of at-most-once.
  Duplicates are absorbed by the processed-events claim that already existed
  for broker redelivery.
- The `/readyz`-first shutdown sequence of ADR-0029 is unchanged.
