---
id: 0057-a-dead-background-loop-takes-the-process-down
slug: /adr/0057-a-dead-background-loop-takes-the-process-down
title: 57. A dead background loop takes the process down
sidebar_label: "57. A dead background loop takes the process down"
description: "ADR 0057 — a Kafka consumer or the outbox relay that returns while the service is meant to run ends the process (run() returns an error, exit non-zero, the kubelet restarts the pod); a stop the service asked for is clean. Records and pins the rule serveUntilSignal already implements."
---

# 57. A dead background loop takes the process down

## Status

Accepted (2026-10-25). Respects [ADR-0052](./0052-postgres-and-kafka-are-the-only-runtime.md) (real Postgres and
Kafka). No wire change, no migration, no new env var, no chart value.

## Context

A Kafka consumer that returns an error and is only logged leaves the service looking healthy: `/healthz` and
`/readyz` keep answering 200 and the read model behind the consumer freezes for hours. This happened live in
network-inventory-planning: under host load the broker reset connections and answered `Group Load In Progress`,
and its pack-level consumer died and never came back (fixed there by its ADR-0019).

This service has the same shape of risk: six background loops, each logging `... consumer stopped` or
`outbox relay stopped` and returning (the process-path catalogue, WorkReleased, product classification,
interleaving hint and pack level consumers, plus the relay). The question was whether any of them could die
silently. Reading the code shows they cannot: every loop closes a `done` channel on ANY return
(`defer close(done)`), and `serveUntilSignal` selects on `watchDone` over all six. The rule has been in force
since the real-runtime change (ADR-0052) but was neither written down nor tested.

## Decision

A background loop that stops while the service is still meant to run ends the process.

1. Every loop is passed to `serveUntilSignal` as a `namedDone`. The first one whose channel closes before a
   shutdown was requested makes `serveUntilSignal` return `"<loop> stopped unexpectedly: ..."`, after the usual
   graceful stop. `run()` returns it, `main` exits 1, the kubelet restarts the pod, and the consumers rejoin
   their groups from the committed offset.
2. A stop the service asked for is clean and reports nothing. This holds because the loops run on contexts
   that only `serveUntilSignal` cancels (`consumerCtx` via `cancelConsumer`, the relay's own context via
   `stopRelay`), and it does so after the select: the outer context ending (SIGTERM, the integration test) can
   never close a loop's channel first and be misread as a crash (the network-fulfillment race, its PR #119).
3. `watchDone`'s channel is buffered to the number of loops, so several loops dying together never block.
   Only the first death is acted on.

No in-process retry is added: a restart rejoins every group with fresh connections, and a retry loop would need
its own backoff and give-up rule.

## Consequences

- A transient broker problem shows as a pod restart (visible in `kubectl get pods` and restart-count alerts)
  instead of a stale read model.
- If the broker is down for long the pod crash-loops with backoff, which is the accurate state of a service
  that cannot consume.
- `cmd/execution/loop_death_test.go` pins the rule: a table over `watchDone`, several deaths never block, each of
  the six loops dying makes `serveUntilSignal` return an error naming it, and a requested shutdown returns nil.
  Removing the `died` case, or flagging the shutdown branch as a death, fails these tests.
- A loop added later must be listed in the `serveUntilSignal` call in `run()`, or its death is not noticed.
