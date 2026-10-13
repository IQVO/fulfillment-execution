---
id: 0044-listener-before-catalogue-replay
slug: /adr/0044-listener-before-catalogue-replay
title: "44. Kafka catalogue mode: HTTP listener before the replay, /readyz false until it catches up"
sidebar_label: "44. Listener before catalogue replay"
description: "ADR 0044 — with PATH_CATALOGUE_SOURCE=kafka, cmd/execution starts its HTTP listener before the process-path catalogue replay has caught up. /healthz answers from the start, /readyz answers 503 until the replay reaches the startup watermark, and the WorkReleased consumer starts only afterwards. A replay that misses WaitReadyTimeout is still fatal."
---

# 44. Kafka catalogue mode: HTTP listener before the replay, `/readyz` false until it catches up

## Status

Accepted. Decided 2026-10-12 (fleet gap-fix wave, decision D24). Amends the
"startup waits for the initial replay before serving" part of the
[ADR-0017](./0017-process-path-catalogue-as-configuration.md) addendum and
the readiness semantics of
[ADR-0029](./0029-resilience-circuit-breakers-retry-dlq-shutdown.md) (the
gate was only ever flipped by shutdown). Neither body is edited.

## Context

With `PATH_CATALOGUE_SOURCE=kafka`, `cmd/execution` replayed
`warehouse.process-path-management.events` into memory and blocked the boot
until every partition reached the offset captured at startup
(`kafkacatalog.WaitReadyTimeout`, 60 s). Only then did it run the
migrations, open the pool and start the HTTP listener. During all of that
`/healthz` did not answer, so the chart's startup probe (2 s x 30 = 60 s)
had to cover the replay **plus** the migrations and the boot retries. A
replay that took most of its 60 s, followed by a slow migration, got the pod
killed by the startup probe even though it would have become ready, and a
crash loop never let the replay finish. Nothing in the probe results told
an operator that the process was alive and replaying.

## Decision

1. **The replay runs in the background from the start.** `loadCatalogue`
   creates the Kafka catalogue consumer (under boot retry, as before),
   starts its loop and returns a `waitReady` function instead of waiting.
   Migrations, the pool and every adapter are built while the replay runs.
2. **The listener starts before the wait.** `serveWhileWarmingUp` puts the
   readiness gate into warm-up (`Readiness.StartWarmUp`), starts the HTTP
   listener, then waits for the replay (same `WaitReadyTimeout`).
   `/healthz` answers `200` from the start; `/readyz` answers
   `503 {"status":"not_ready"}` (the existing contract) until the replay has
   caught up, then `200`.
3. **Work that needs the catalogue waits for it.** The `WorkReleased`
   consumer (which resolves `path_id` against the catalogue), the
   `ProductClassified` consumer and the outbox relay start only after the
   warm-up has finished, exactly where they started before.
4. **Failure is unchanged.** A replay that fails or misses the timeout
   shuts the listener down and fails the boot with
   `process-path catalogue did not become ready within 1m0s`.
5. **Shutdown wins.** The gate has two independent conditions: warm-up
   (cleared by `FinishWarmUp`) and shutdown (`SetNotReady`, never cleared),
   so finishing the warm-up can never undo a shutdown flip. In the current
   wiring the SIGTERM handler (`waitForShutdown`) is installed only after
   the warm-up, so a SIGTERM during the replay terminates the process with
   Go's default signal behaviour, exactly as it did before this ADR when no
   listener was up yet.
6. **File mode is unchanged**: no warm-up, ready as soon as the listener
   starts.
7. **The timeout clock starts at the wait.** The 60 s `WaitReadyTimeout`
   now counts from the moment the listener is up (after migrations and the
   pool), not from process start; the replay has been running in the
   background since `loadCatalogue`, so its effective budget is slightly
   longer than before.

## Consequences

### Easier

- The startup probe no longer has to cover the replay: it only waits for
  the migrations, the pool and the listener. The replay keeps its own 60 s
  budget, enforced by the process, not by a probe that kills it.
- An operator sees "alive but not ready" (`/healthz` 200, `/readyz` 503) and
  the log line `http listener up; /readyz stays not ready until the
  process-path catalogue has replayed its initial history` instead of a
  silent pod.
- Migrations and the replay overlap, so a Kafka-mode boot is shorter.

### Harder

- During warm-up the REST API answers if called directly (bypassing the
  Service, which routes only to ready pods). No REST handler reads the
  catalogue, so such a call behaves as it would after the warm-up; events it
  produces wait in the outbox until the relay starts.
- The Service sees the pod only after the replay; that is the guarantee
  ADR-0017 asked for, now expressed through readiness rather than through
  the absence of a listener.

Pinned by `TestServeWhileWarmingUp_ListenerUpAndNotReadyUntilReplayCatchesUp`,
`TestServeWhileWarmingUp_ReplayTimeoutFailsTheBootAndStopsTheListener`,
`TestServeWhileWarmingUp_FileModeIsReadyAtOnce` (`cmd/execution`) and the
`Readiness` warm-up tests (`internal/adapters/inbound/http`).
