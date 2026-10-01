---
id: 0020-transactional-outbox
slug: /adr/0020-transactional-outbox
title: 0020. Transactional outbox feeding both the integration and analytics topics
sidebar_label: 0020. Transactional outbox
description: ADR 0020 — why the OLTP use cases stopped publishing to Kafka from inside their own code path and now commit every event's wire form — for BOTH warehouse.fulfillment.events and warehouse.fulfillment.analytics — to one outbox table in the same transaction as the aggregate, with an in-process relay draining it onto the broker.
---

# 0020. Transactional outbox feeding both the integration and analytics topics

## Status

Accepted — implemented in the same change that introduced this record.
Fleet-wide rollout of the pattern; the single-topic reference
implementation is process-path-management's ADR 0003.

## Context

Every state-changing use case in this service followed the same two-step
shape:

```go
if err := uc.Tasks.Save(ctx, t); err != nil { return err }
if err := uc.Publisher.Publish(ctx, shared.NewTaskCompleted(...)); err != nil { return err }
```

Two independent writes to two independent systems, with no compensation.
A crash, a broker timeout or a pod eviction between them leaves a
Completed task in Postgres whose `TaskCompleted` **never reaches
wes-work-planning** (which then never calls `RecordCompletion` for that
work unit) and never reaches the analytics projector (so the throughput
report under-counts). The reverse — the event goes out, then the HTTP
response is lost and the station retries — is already absorbed by the
domain's own idempotency (`Task.Complete` on an already-completed task is
rejected), but the first failure had no answer at all.

Two things make this service's situation harder than a textbook outbox:

1. **Two topics, fanned out from one `Publish`.** ADR 0012 introduced a
   dedicated analytics stream next to the integration stream, wired
   through `events.MultiPublisher(integration, analytics)`. `MultiPublisher`
   stops at the first failing target, so a broker hiccup between the two
   writes could also leave the integration topic ahead of the analytics
   topic (or vice versa) for the same event — a third divergence, this
   time between two Kafka topics.
2. **Both publishers read repositories at publish time.** The integration
   `Publisher` enriches `TaskCompleted` with the task's `OrderRef`,
   the claiming station's occupant and the claim-to-complete duration
   (ADR 0014) via `TaskRepo.FindById` / `StationRepo.FindById`; the
   `AnalyticsPublisher` looks up `task_type` for every task-scoped event.
   Any design that encodes the event *outside* the use case's transaction
   would race the aggregate write it is trying to describe.

## Decision

Adopt the **transactional outbox** pattern in its fan-out variant: the
use case writes the *already-encoded Kafka message* for every
(event × topic) pair into an `outbox_events` table in the same
transaction as the aggregate, and an in-process relay drains that table
onto the broker.

### The outbox row is a wire-ready message, not a domain event

`migrations/0009_outbox.up.sql`:

```sql
CREATE TABLE outbox_events (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    key          BYTEA,
    value        BYTEA       NOT NULL,
    headers      JSONB       NOT NULL DEFAULT '[]',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts     INTEGER     NOT NULL DEFAULT 0,
    last_error   TEXT
);
CREATE INDEX idx_outbox_events_unpublished ON outbox_events (id) WHERE published_at IS NULL;
```

One row per Kafka message: `TaskCompleted` produces **two** rows (one on
`warehouse.fulfillment.events`, one on `warehouse.fulfillment.analytics`),
`TaskCreated` produces one (analytics only), `ItemArrivedAtRebin`
produces none (outside both contracts). The relay never re-encodes, never
consults a repository and never needs to know what a domain event is —
it moves bytes.

### Encode is split from Send, and Encode runs inside the transaction

`internal/adapters/outbound/kafka` gained:

```go
type Encoded struct { Topic, EventType string; Key, Value []byte; Headers []kafkago.Header }
type Encoder interface { Encode(ctx, events ...shared.DomainEvent) ([]Encoded, error) }
```

Both `Publisher` and `AnalyticsPublisher` implement `Encoder`: everything
their `Publish` used to do except `WriteMessages` — envelope build, the
repo-lookup enrichment, the W3C `traceparent` header injection — now
lives in `Encode`. `Publish` became "open the producer span, `Encode`,
`WriteMessages`", so the direct (no-Postgres) path and its existing unit
tests are unchanged. `NewPublisherWithWriter` / `NewAnalyticsPublisherWithWriter`
let the composition root build them with **no writer at all** when they
only act as encoders.

`postgres.OutboxPublisher` implements `ports.EventPublisher`: for each
configured `Encoder` it calls `Encode(ctx, events...)` and inserts one
row per `Encoded` through the querier bound to `ctx`. Because it runs
under the use case's `UnitOfWork`, the enrichment `FindById` calls hit
the **same transaction** that just upserted the task — they see the
`Completed` status and the `claimed_at` timestamp of the row being
described, not whatever an autonomous connection would have seen.

### Unit of work at the application layer

`ports.UnitOfWork` (`Execute(ctx, fn)`) was added and every publishing
use case — `CreateTask`, `ClaimNext`, `CompleteTask`, `ExpireLeases`
(one scope per freed task), `SealPackage`, `RunSlam`, `ArriveAtRebin` —
gained an exported `UnitOfWork` field and wraps **all** its `Save`s and
its `Publish` in one `atomically(ctx, uc.UnitOfWork, fn)` scope. Reads
that only decide whether to act stay outside. A nil `UnitOfWork` runs
`fn` directly, which is exactly the in-memory / log-publisher
configuration — no test needed a change.

`ArriveAtRebin` previously published `ItemArrivedAtRebin` *before* saving
the consolidation; the order is now save-then-publish inside the scope,
and its nested `CreateTask.Execute` **joins** the outer transaction (the
Postgres `UnitOfWork` detects a ctx that already carries a tx) rather
than opening a second one. The integration test
`TestOutbox_ArriveAtRebin_NestedScopeRollsBackEverything` proves a
failure at the very last publish rolls back the consolidation row, the
nested PACK task and every outbox row together.

`postgres.UnitOfWork` binds a `pgx.Tx` to the context; every repository
in the package now issues SQL through `querierFrom(ctx, pool)`, which
resolves to that tx when present and the pool otherwise. The domain
layer is untouched and the arch-go fitness tests pass unchanged.

### The relay

`postgres.OutboxRelay` claims up to `batchSize` unpublished rows with
`SELECT … FOR UPDATE SKIP LOCKED ORDER BY id`, hands them **one at a
time** to a `Sink` (`kafka.RelaySink`, a single topic-less
`kafkago.Writer` that sets `Message.Topic` per row), and marks each
published as it goes. On the first failed send it records
`attempts+1, last_error` on that row, commits what was already sent and
returns — so a later event for the same task can never overtake an
earlier one that has not yet gone out. `SKIP LOCKED` lets a rolling
deploy's overlapping old and new pods run two relays without double
publishing the same row. `Run(ctx)` loops with `OUTBOX_RELAY_INTERVAL`
(default 1s) between empty passes and no sleep after a full batch.

### Wiring matrix (`cmd/execution/main.go`)

| `DATABASE_URL` | `EVENT_PUBLISHER` | publisher wired                                       | relay |
|----------------|-------------------|-------------------------------------------------------|-------|
| unset          | `log`             | `LogPublisher`                                        | no    |
| unset          | `kafka`           | `MultiPublisher(Publisher, AnalyticsPublisher)` direct | no    |
| set            | `log`             | `LogPublisher`                                        | no    |
| set            | `kafka`           | `OutboxPublisher(pool, Publisher, AnalyticsPublisher)` | yes   |

The startup line reads `event publisher configured publisher=kafka
mode=outbox|direct`. The relay goroutine runs next to the HTTP server;
on `SIGTERM` the server is shut down first, then the relay is cancelled
and the process waits (bounded by the same 10s deadline) for its
in-flight pass, so an event committed by a request that completed just
before shutdown is not stranded until the next pod boots.
`cmd/fulfillment-projector`, `cmd/fulfillment-reports` and `cmd/mcp` are
untouched.

## Consequences

**Delivery semantics.**

- *Atomic*: the aggregate row and every one of its outbox rows commit or
  roll back together. There is no longer any state in which a task is
  Completed in Postgres but neither topic will ever hear of it, nor any
  state in which one topic heard and the other did not.
- *At-least-once*: a crash between the relay's `Send` and its `UPDATE`
  republishes that row on the next pass. The envelope id (the CloudEvents
  `id` since [ADR-0032](./0032-cloudevents-mandatory-envelope.md)) is
  minted at encode time and persisted with the row, so a redelivery
  carries the **same** id — and every consumer on both topics already
  dedupes on it (this repo's own projector and `WorkReleased` consumer
  via `processed_events`; wes-work-planning's `RecordCompletion` is
  idempotent per work unit).
- *Per-key ordering preserved*: rows are drained in `id` (= commit)
  order and the relay stops at the first failure.
- *Latency*: up to one `OUTBOX_RELAY_INTERVAL` (1s) is added between
  commit and the message appearing on the topic, versus the previous
  synchronous write. Nothing in this fleet consumes these topics on a
  sub-second SLA.

**Operational.**

- With `EVENT_PUBLISHER=kafka` and Postgres configured, the OLTP process
  now holds exactly **one** Kafka producer connection (the relay's) instead
  of two.
- A stuck row (a poison message the broker keeps rejecting) blocks the
  relay for every later row — by design, since skipping it would break
  ordering. `attempts` / `last_error` make it visible; clearing it is a
  manual `UPDATE … SET published_at = now()` decision, not an automatic
  one.
- `outbox_events` grows unbounded; published rows are not purged in this
  change. A retention sweep is a follow-up, not a prerequisite.
- The direct Kafka path (`DATABASE_URL` unset) still exists for
  in-memory dev runs and keeps the old non-atomic behaviour — there is
  no transaction to bind to. It is not a production configuration.

**CLAUDE.md** is a protected file and was not edited; its
`outbound/events` line should additionally mention the outbox.

## Alternatives considered

- **Publish first, then save.** Inverts which failure you get; a
  message on the topic for a completion Postgres rolled back is strictly
  worse for a consumer that trusts the topic.
- **Keep the direct fan-out and add retries around `Publish`.** Bounded
  retries still lose the event on pod eviction; unbounded ones hold the
  HTTP request open against a down broker.
- **Store the domain event and let the relay encode.** Simpler table,
  but the relay would then run the enrichment lookups against whatever
  the repositories hold *at relay time* — a task re-claimed or a station
  re-occupied in the meantime would leak into the event describing an
  earlier completion. Encoding inside the transaction pins the payload
  to the state it describes.
- **One outbox table per topic.** Two relays, two commit orders, and no
  single `id` sequence to reason about ordering with. The `topic`
  column is cheaper and keeps one relay.
- **Change-data-capture (Debezium) on the outbox table.** Removes the
  in-process relay but adds Kafka Connect to a local `kind` fleet whose
  Kafka is a single Bitnami broker; not justified at this scale.
- **Kafka transactions.** Only span Kafka writes; they cannot include
  the Postgres write, which is the whole problem.

## Verification performed

All run locally against the worktree before pushing; every command
below produced the quoted result.

- `make check` (gofmt, vet, build, golangci-lint, `go test ./... -race`):
  `check: OK`.
- `make arch-test`: `TestHexagonalDependencyRules` and
  `TestRepositoryPortImplementersFollowRepoNamingConvention` pass — the
  domain layer's dependency rule is unchanged.
- `make coverage` (`./internal/domain/...,./internal/application/...`):
  97.1% before → **97.4%** after (gate 90%).
- `go test ./internal/application/usecases/ -race`: 97 tests pass, 20 of
  them new in `unit_of_work_test.go` — per publishing use case, every
  Save and Publish runs inside exactly one scope, a publish failure rolls
  it back, nil `UnitOfWork` still works, and `ArriveAtRebin`'s nested
  `CreateTask` joins rather than opens a scope.
- `go test ./internal/adapters/outbound/kafka/ -race`: existing `Publish`
  and tracing tests unchanged and passing; new `Encode` tests for both
  publishers and `RelaySink` tests.
- `go build -tags=integration ./... && go vet -tags=integration ./...`
  and `golangci-lint run --build-tags integration ./...`: clean.
- `go test -tags=integration ./internal/adapters/outbound/postgres/ -run Outbox -race -count=1`
  against a **testcontainers** `postgres:16-alpine` (no external
  `DATABASE_URL`, no skip): six tests pass — commit-together across both
  topics with enrichment visible from inside the transaction; rollback of
  the aggregate on a failed encode; relay publishes four rows in commit
  order across both topics and a second pass is a no-op; relay stops at a
  failed row (`attempts=1`, `last_error` set), later rows stay pending,
  and the next pass drains them in order; trace headers round-trip
  through JSONB; nested-scope rollback.

Not verified in this change: a live run against the `warehouse` kind
cluster's Kafka broker (the relay's `RelaySink` is exercised only through
the fake `Writer`), and the behaviour of two overlapping relay pods
under `SKIP LOCKED`.
