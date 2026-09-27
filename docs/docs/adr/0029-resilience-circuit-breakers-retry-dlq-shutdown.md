---
id: 0029-resilience-circuit-breakers-retry-dlq-shutdown
slug: /adr/0029-resilience-circuit-breakers-retry-dlq-shutdown
title: "29. Circuit breakers, retry, dead-letter queue, and graceful shutdown for outbound dependencies"
sidebar_label: "29. Resilience: breakers, retry, DLQ, shutdown"
sidebar_position: 29
description: "ADR 0029 — a per-dependency sony/gobreaker circuit breaker plus cenkalti/backoff jittered retry wraps both cross-context HTTP clients (facilitylayout, productclassification), each falling back to its EXISTING fail-open/PermissiveLookup contract on trip; the WorkReleased Kafka consumer gains bounded in-process retry before its existing DLQ publish; and the HTTP server gains a /readyz gate flipped before shutdown. Direct port of order-management ADR-0025, the fleet reference implementation."
---

# 29. Circuit breakers, retry, dead-letter queue, and graceful shutdown for outbound dependencies

## Status

Accepted — implemented in the same change that introduced this record.
Direct port of order-management's ADR-0025 (PR #107) — the fleet
reference implementation this change copies pattern-for-pattern,
adapted to this repo's own two outbound HTTP clients and its one
event-creating Kafka consumer.

## Context

This service has two synchronous cross-context HTTP dependencies —
`facilitylayout.Client` (`GET /locations/{locationCode}`, gated by
`LOCATION_ROLE_MODE=http|permissive`) and
`productclassification.Client` (`GET /products/{sku}/classification`,
gated by `PRODUCT_CLASSIFICATION_MODE=http|permissive`) — and one
inbound Kafka consumer that creates a `Task` per `WorkReleased` event
(`internal/adapters/inbound/kafka/consumer.go`), which already gained a
`<topic>.dlq` dead-letter sink in a prior change but dead-lettered a
message on its FIRST failure, with no in-process retry to absorb a
transient blip first.

Until this change, neither HTTP client had a circuit breaker: a
degraded `facility-layout` or `inventory-storage` (the service backing
`PRODUCT_CLASSIFICATION_MODE=http`) meant every single request from
this service paid the full request timeout before falling back, one at
a time, indefinitely — no bulkheading against a slow dependency
consuming request-handling capacity, and no fast-fail once the
dependency was known to be down. Order-management hit exactly this
class of problem and resolved it with ADR-0025 (PR #107): a per-
dependency `sony/gobreaker` circuit breaker, jittered retry via
`cenkalti/backoff/v4` for read-only calls, a `circuit_breaker_state`
Prometheus gauge, DLQ hardening for its own Kafka consumer, and a
`/readyz` graceful-shutdown gate. That PR is this service's designated
fleet reference for the same problem, not a design to reinvent.

Alternatives considered and rejected, mirroring order-management's own
reasoning:

- **One global breaker for all outbound calls.** Rejected: a tripped
  `facility-layout` breaker would then also refuse
  `product-classification` calls, coupling two independent
  dependencies' failure domains for no reason. Each dependency gets its
  own breaker instance and its own bulkheaded `*http.Client`.
- **Retrying every outbound call, including mutating ones.** Rejected:
  neither `GetRole` nor `GetClassification` is the only read this
  service makes, but nothing here is mutating — a blind repo-wide retry
  policy would eventually be applied to a non-idempotent call by
  someone copying this pattern without checking. This ADR is explicit
  that retry applies ONLY to genuinely read-only, idempotent calls.
- **Inventing a new fallback behaviour for the breaker's open state.**
  Rejected: both clients already have a documented, tested fail-open
  contract (`PermissiveLookup`, `Known=false`) for a real transport
  error. The breaker's open state reuses that SAME fallback rather than
  introducing a second, subtly different "breaker is open" behaviour a
  future reader would have to reconcile with the first.

## Decision

**1. `internal/resilience` — shared breaker/timeout policy (new
top-level package, no domain/application import, mirrors
order-management's own `internal/resilience` exactly):**

- `ReadyToTrip(counts gobreaker.Counts) bool` — the ONE trip condition
  every breaker in this service uses: 5 consecutive failures, OR (once
  at least 10 requests have been seen in the current window) a failure
  rate over 50%.
- `DefaultMaxRequests = 1`, `DefaultInterval = 30s`,
  `DefaultTimeout = 30s` — the shared gobreaker tuning: one half-open
  probe at a time, a 30s closed-state rolling-counts window, a 30s
  open-state cooldown before the next probe.
- `StateRecorder` interface (`SetState(dependency string, state
  int64)`) and `RecordStateChange(dependency string, recorder
  StateRecorder) func(...)` — adapts a breaker's `OnStateChange` hook
  into the Prometheus gauge below. `state` is `gobreaker.State`'s own
  int value verbatim (0=closed, 1=half-open, 2=open) — no translation
  table needed.
- `CallTimeout(ctx, maxPerCall) (context.Context, context.CancelFunc)`
  — derives one outbound call's timeout from the INBOUND request's
  remaining context deadline, capped at `maxPerCall`, so a caller with
  a short fuse left is never handed a longer one, and a caller with no
  deadline (a background job, a test) can never hang a call forever.

**2. One breaker instance PER dependency — never one global breaker:**

- `facilitylayout.NewBreakerClient(inner *Client, recorder
  resilience.StateRecorder) *BreakerClient` — dependency name
  `"facility-layout"`.
- `productclassification.NewBreakerClient(inner *Client, recorder
  resilience.StateRecorder) *BreakerClient` — dependency name
  `"product-classification"`.

Each `BreakerClient.Get{Role,Classification}` derives its timeout via
`resilience.CallTimeout`, runs the underlying call through a jittered
`cenkalti/backoff/v4` retry (see §3) AS ONE `gobreaker.Execute` call —
so a retry storm against an already-degraded dependency still only
counts as ONE success/failure toward the breaker's trip condition, not
N — and, when the breaker rejects the call outright (`ErrOpenState` or
`ErrTooManyRequests`), falls back to that same client's EXISTING
`PermissiveLookup` (`Known=false, nil`) — never a new fallback. A
genuine error the breaker let through (not a rejection) propagates
unchanged, exactly as it did before this breaker existed: neither
client's port contract changes shape.

Each `main.go` wiring call constructs its own bulkheaded
`*http.Client`/breaker pair — the two dependencies never share
transport state, so one saturating `facility-layout` cannot starve
`product-classification`'s own connection pool or trip its breaker.

**3. Retry only on genuinely read-only, idempotent calls, max 3,
jittered:**

`GetRole` (`GET /locations/{locationCode}`) and `GetClassification`
(`GET /products/{sku}/classification`) are both pure reads with no side
effect — safe to retry. Both breaker clients retry via
`backoff.NewExponentialBackOff` (50ms initial, 500ms max interval,
jittered) wrapped in `backoff.WithMaxRetries(policy, 2)` (3 total
attempts), bounded by the call's own `CallTimeout`-derived context. A
404/400 (`Known=false, nil` — a legitimate answer, not a failure) is
NEVER retried; only a transport error or unexpected status is.

**4. `circuit_breaker.state` Prometheus gauge, reusing the existing
OTel pipeline:**

`internal/observability.CircuitBreakerMetrics` (implementing
`resilience.StateRecorder`) registers an `Int64Gauge` named
`circuit_breaker.state` on the SAME global `otel.Meter` this service's
other metrics already use — no second Prometheus registry. The OTel
Collector's prometheus exporter re-publishes it as
`circuit_breaker_state{dependency="facility-layout"|"product-classification"}`.

**5. Kafka DLQ hardening — bounded in-process retry before the
existing dead-letter publish:**

`internal/adapters/inbound/kafka/consumer.go`'s `Consumer.Handle` (the
`WorkReleased` consumer — the ONLY event-creating consumer in this
service; `AnalyticsConsumer` builds a read-only in-memory index and
carries no side effect to retry) already had a `<topic>.dlq` sink
(`Consumer.DeadLetter`/`SendToDeadLetter`, `"<topic>.dlq"` naming) from
a prior change, but dead-lettered on the FIRST failure. This ADR adds:
up to `maxHandlerAttempts = 3` total attempts with the same jittered
`cenkalti/backoff/v4` policy as the HTTP clients, bounded by the
message's own span/context — a transient blip (a momentary downstream
hiccup) now heals itself without ever reaching the DLQ.

The retry loop wraps ONLY the retryable, event-creating work
(`Catalogue.Lookup` then `CreateTask.Execute`) — NOT the
`Processed.MarkProcessed` idempotency claim that precedes it. Wrapping
`MarkProcessed` inside the same retry loop would be a correctness bug:
`MarkProcessed` returns `isNew=true` exactly once per `event_id`, so a
retry of the WHOLE handler after a later step's transient failure would
see `isNew=false` on attempt 2 and silently return success without ever
creating the `Task`. The claim itself gets its own narrow retry
(`markProcessedWithRetry`, safe in isolation — until it returns
`isNew=true`, no event-creating work has happened yet), and only once
the claim is confirmed does the event-creating retry loop begin.

Kafka's consumer-group offset is auto-committed by `ReadMessage` before
`Handle` runs, so a message that exhausts every retry and is
dead-lettered is never redelivered — the DLQ is a durable record for
alerting/replay, not a mechanism to avoid losing the offset (unchanged
from the prior DLQ-only change).

**6. Graceful shutdown hardening — `/readyz` flipped first:**

`internal/adapters/inbound/http.Readiness` is a nil-safe, atomic
ready/not-ready gate (zero value: ready). `Handlers.Readiness` backs a
new `GET /readyz` route, separate from the existing `GET /healthz`
(liveness — never flipped by shutdown, so a `livenessProbe`/
`startupProbe` pointed at it is unaffected). `cmd/execution/main.go`'s
shutdown sequence now: (1) `readiness.SetNotReady()` FIRST, before
anything else stops, so a Kubernetes `readinessProbe` polling `/readyz`
has a window to stop routing new traffic before the listener closes;
(2) `srv.Shutdown` drains in-flight HTTP requests; (3) the outbox relay
and the `WorkReleased` consumer are each cancelled and AWAITED
(bounded by the same shutdown deadline, not merely asked to stop and
forgotten); (4) only then do the deferred `Close()` calls registered
earlier in `main` run, guaranteed by defer's LIFO order to fire after
every consumer/relay goroutine has already stopped touching those
resources.

## Consequences

**Easier:**

- A degraded `facility-layout` or `inventory-storage` now fails fast
  (breaker open, immediate fallback) instead of every request paying
  the full timeout one at a time — the existing fail-open behaviour
  both callers (`RegisterStation`, `SealPackage`'s hazard-class lookup)
  already tolerate is now reached faster, not replaced.
- A transient Kafka-consumer blip (momentary catalogue/DB hiccup) now
  self-heals within `Handle` instead of immediately dead-lettering a
  message that would have succeeded on the very next attempt.
- `circuit_breaker_state` gives on-call visibility into which
  dependency (if either) is currently degraded, without a new
  Prometheus registry.
- `/readyz` gives Kubernetes a real signal to stop routing traffic
  during shutdown, closing a window where a request could previously
  race a closing listener.

**Harder / accepted trade-offs:**

- Two more direct dependencies (`sony/gobreaker/v2`,
  `cenkalti/backoff/v4`) and two more tuning constants
  (`maxRetryAttempts`, breaker timeout/interval) a future reader has to
  understand alongside the client they wrap.
- The Kafka consumer's `HandleMessage` is now split into
  `markProcessedWithRetry` + `handleClaimedEvent`, rather than the
  single linear function it was — a slightly less obvious read in
  exchange for correctness under retry (see §5's bug discussion).
- Retry adds up to ~1s of extra latency (3 attempts, jittered
  50-500ms backoff) to a `GetRole`/`GetClassification` call that is
  genuinely down for the whole retry window, before the breaker
  eventually trips and later calls fast-fail instead.
