---
id: 0001-hexagonal-ports-and-adapters
title: 1. Hexagonal (ports and adapters) architecture
sidebar_label: 1. Hexagonal architecture
sidebar_position: 1
description: Adopt hexagonal architecture with a strict inward-pointing dependency rule, keeping the domain free of framework and SQL types.
---

# 1. Hexagonal (ports and adapters) architecture

## Status

**Accepted.** Recorded in `CLAUDE.md` under the heading
"Architecture (NON-NEGOTIABLE)" and in effect from the first commit that
introduced the domain layer.

## Context

This service owns invariants that are genuinely subtle and genuinely
expensive to get wrong: at-most-once claiming, capability matching, lease
expiry ordering, no double-complete, seal-requires-contents, and the SLAM
weigh-check tolerance. Every one of them is a business rule, and every one has
a failing path that matters more than its happy path.

Several forces bear on where that logic should live:

- **The rules need to be testable without infrastructure.** A test for "an
  unrenewed lease frees the task after five minutes" should not need Postgres,
  and should not need `time.Sleep`. If it does, it will be slow, flaky, and
  therefore run rarely.
- **The storage decision is not settled at the domain's level.** The service
  must run against Postgres in deployment and against in-memory
  implementations for local runs and tests, with identical behaviour.
- **There are two inbound paths, not one.** Tasks are created over HTTP
  (`POST /tasks`) *and* by a Kafka consumer reacting to `WorkReleased`. Both
  must go through the same rules. Logic living in HTTP handlers would have to
  be duplicated or reached into by the consumer.
- **Event publishing is pluggable.** A log publisher by default, a Kafka
  publisher when `EVENT_PUBLISHER=kafka` — chosen at startup, invisible to the
  code that raises the events.
- **Frameworks churn faster than domains.** chi, pgx and kafka-go will be
  upgraded, replaced, or reconfigured. The rule that a station cannot claim a
  task it lacks capabilities for will not change when they are.

The competing option is the conventional layered/MVC arrangement — handlers
call services, services call repositories, and repository types (or an ORM's
entities) travel upward through all of them. It is faster to write initially
and is the default shape of most Go HTTP services.

## Decision

**We will structure the service as hexagonal (ports and adapters), with a
dependency rule that points strictly inward:**

> **Domain depends on nothing. Application depends on domain. Adapters depend
> on application and domain.** No framework type and no SQL type appears in
> the domain layer.

Concretely:

- `internal/domain/` — `task`, `station`, `package`, `shared`. Pure Go. Its
  only imports are the standard library. Aggregates expose behaviour, keep
  their fields unexported, and return typed errors.
- `internal/application/ports/` — the twelve outbound interfaces the
  application needs (six when this ADR was written): `TaskRepo`, `StationRepo`,
  `PackageRepo`, `EventPublisher`, `Clock`, `ProcessedEvents`, and, added
  since, `OrderConsolidationRepo`, `PathCatalogue`, `UnitOfWork`, `Metrics`,
  `ProductClassificationLookup`, `LocationRoleLookup`. Declared here, on the
  *consumer* side, not next to their implementations.
- `internal/application/usecases/` — one struct per use case, dependencies as
  plain fields, depending only on the domain and on `ports`.
- `internal/adapters/inbound/` — chi HTTP handlers, Kafka consumer. They
  translate a protocol into a use-case call and back.
- `internal/adapters/outbound/` — `postgres`, `memory`, `events`, `kafka`.
  They implement `ports`.
- `cmd/execution/main.go` — the only place that knows both a `pgxpool` and a
  use case exist.

Two consequences of the rule are load-bearing and worth naming explicitly:

**Time is a port.** `ports.Clock` exists so no domain method ever calls
`time.Now()`. `Task.Claim`, `RenewLease`, `Complete` and `ExpireLeaseIfDue`
all take `now` as a parameter. This is what makes lease expiry deterministic
under test.

**Ports are declared by the consumer.** `TaskRepo` lives in
`application/ports`, not in `adapters/outbound/postgres`. The application
states what it needs; adapters satisfy it. That is what inverts the
dependency.

## Consequences

### Easier

- **Invariants are unit-testable with zero infrastructure.** The domain test
  suite is pure function calls. Lease expiry is tested by advancing a fixed
  clock, not by sleeping.
- **Both inbound paths share one implementation of the rules.** The Kafka
  consumer calls the *existing* `CreateTask` use case rather than a parallel
  one — explicitly the intended design, and the reason there is no second code
  path to keep in agreement.
- **Storage is genuinely swappable.** `DATABASE_URL` unset selects in-memory
  repositories; the service runs fully with a single `go run` and no
  infrastructure. That is not a testing convenience bolted on afterwards, it
  is the same interface.
- **The publisher swap is invisible.** `EVENT_PUBLISHER=kafka` replaces one
  `ports.EventPublisher` implementation with another. No use case changed when
  Kafka publishing was added.
- **Framework upgrades stay in one directory.** A chi major version affects
  `adapters/inbound/http` and nothing else.

### Harder

- **More files and more indirection.** Adding a field to a response means
  touching the aggregate, possibly the use case, the DTO, and the mapping
  between them. For a CRUD endpoint this is pure overhead, and it is honest to
  say so — the layering pays for itself in the aggregates with real
  invariants, not in `GET /healthz`.
- **Explicit DTO mapping everywhere.** Domain structs are never serialised
  onto the wire, so every response has a hand-written mapping in
  `dto.go`. This is deliberate — it means an internal rename cannot silently
  change the public API — but it is real, repetitive work.
- **`Rehydrate` constructors are a compromise.** Repositories need to rebuild
  an aggregate from persisted state without re-running construction
  validation, so each aggregate exposes a `Rehydrate` alongside `New`. It is a
  small hole in the encapsulation, opened deliberately and used only by
  adapters.
- **The rule must be enforced, not merely written down.** A dependency rule
  that lives only in a document decays. That pressure is what led directly to
  [ADR-0006](./0006-arch-go-architecture-fitness-tests.md), which makes the
  rule executable.
- **Newcomers need orientation.** "Where does this code go?" is not obvious
  from a file listing. The [Architecture](../overview/architecture.md) page
  exists to answer it.
