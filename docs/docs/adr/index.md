---
id: index
title: Architecture Decision Records
sidebar_label: About ADRs
sidebar_position: 0
slug: /adr/
description: Why this service keeps ADRs, the template it uses, and how to propose a new one.
---

# Architecture Decision Records

An **Architecture Decision Record (ADR)** captures a single architecturally
significant decision: the forces at play, the choice made, and what that
choice commits the team to. One decision, one file, numbered in the order they
were taken.

## Why these exist

Code shows *what* was built. Tests show that it works. Neither shows **why**
the obvious alternative was rejected — and that is the expensive knowledge to
lose.

Concretely, in this repository: nothing in `internal/domain/task/task.go`
explains why there is a lease rather than a database row lock, or why
`ExpireLeaseIfDue` is called before the already-claimed check inside `Claim`.
Both are load-bearing decisions with real alternatives, and both look like
arbitrary style choices to anyone reading the code cold. An ADR is the
cheapest possible place to put that reasoning: next to the code, in version
control, dated, and immutable once accepted.

The ADRs here are **reconstructed from decisions actually made in this
repository** — visible in `CLAUDE.md`, `TASKS.md`, `INTEGRATION.md`, the CI
workflow and the git history. They are not generic best-practice essays.

## The template

These records use [Michael Nygard's
template](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions),
the de facto standard, with five sections:

```markdown
# NNNN. Title in the imperative

## Status
Proposed | Accepted | Deprecated | Superseded by ADR-NNNN

## Context
The forces at play: technical, business, operational. Written in
value-neutral language — what is true, not what we want to do about it.

## Decision
The response to those forces, stated in the active voice:
"We will …"

## Consequences
What becomes easier and what becomes harder as a result. Both.
An ADR with only positive consequences is not describing a real decision.
```

Two properties make the format work, and are worth preserving:

- **The Context section is neutral.** If it argues for the decision, it has
  stopped describing the problem and started selling the solution — and a
  future reader can no longer tell whether the reasoning still holds.
- **The Consequences section is honest about costs.** Every decision here has
  a "what becomes harder" paragraph. Pull dispatch gave up global
  optimisation; leases gave up immediate detection of an abandoned claim.
  Recording that is the point.

## Lifecycle

ADRs are **immutable once accepted**. A decision that changes is not edited —
a new ADR supersedes it, and the old one's status becomes
`Superseded by ADR-NNNN`. The record of what was believed, and when, is as
valuable as the current answer.

## Proposing a new one

1. Copy the template above into `docs/docs/adr/NNNN-short-kebab-title.md`,
   taking the next free number.
2. Set the status to `Proposed` and open a PR.
3. Discuss in review. If the decision changes shape, edit the PR — the
   immutability rule starts at acceptance, not at drafting.
4. On merge, set the status to `Accepted`.
5. Add the file to the ADR list in `docs/sidebars.ts`.

If a decision supersedes an existing one, say so in both files: the new one
gets a "Supersedes ADR-NNNN" line in its Context, the old one gets
`Superseded by ADR-NNNN` in its Status.

## The records

| # | Title | Status |
| --- | --- | --- |
| [0001](./0001-hexagonal-ports-and-adapters.md) | Hexagonal (ports and adapters) architecture | Accepted |
| [0002](./0002-pull-based-claimnext-dispatch.md) | Pull-based `claimNext` dispatch over push assignment | Accepted |
| [0003](./0003-lease-based-at-most-once-claiming.md) | Lease-based at-most-once claiming over a hard lock | Accepted |
| [0004](./0004-kafka-integration-events-and-envelope.md) | Kafka for integration events, with a CloudEvents-typed catalogue | Superseded (envelope) by [ADR-0032](./0032-cloudevents-mandatory-envelope.md) |
| [0005](./0005-rfc-7807-problem-details.md) | RFC 7807 `application/problem+json` for every error response | Accepted |
| [0006](./0006-arch-go-architecture-fitness-tests.md) | arch-go fitness tests to enforce the dependency rule | Accepted |
| [0007](./0007-godog-bdd-acceptance-tests.md) | godog (Gherkin) acceptance tests for the invariants | Accepted |
| [0008](./0008-mcp-inbound-adapter.md) | Model Context Protocol as an inbound adapter, not a new service | Accepted |
| [0009](./0009-fragile-and-hazmat-handling-flags.md) | Fragile and hazmat handling flags carried on Task and Package | Accepted |
| [0010](./0010-package-segregation-and-sort-lane.md) | Live per-item DOT hazard classification, same-package segregation, and SortLane | Accepted |
| [0011](./0011-gift-wrap-handling-flag.md) | Gift wrap handling flag carried on Task and Package | Accepted |
| [0012](./0012-analytical-data-product.md) | Per-service analytical data product (report) via a separate analytics topic | Accepted |
| [0013](./0013-fulfillment-mfe-console-adoption.md) | Adopt the fleet's micro-frontend console architecture — `fulfillment-mfe`, `GET /tasks?orderRef=`, and CORS | Accepted |
| [0014](./0014-labor-performance-integration-hooks.md) | Labor-performance integration hooks — check-in/check-out wiring and `TaskCompleted` enrichment | Accepted |
| [0015](./0015-wcs-equipment-anti-corruption-seam.md) | Structural anti-corruption-layer seam for the (unbuilt) WCS tier | Accepted |
| [0016](./0016-rebin-and-order-consolidation.md) | REBIN task type and OrderConsolidation, kept inside fulfillment-execution | Accepted |
| [0017](./0017-process-path-catalogue-as-configuration.md) | Process-path catalogue as configuration, replacing the path_id-prefix guess | Accepted |
| [0018](./0018-installed-capacity-read-endpoint.md) | Installed-capacity read endpoint for workforce-management's capacity ceiling | Accepted |
| [0019](./0019-standard-metrics-convention.md) | Standard metrics convention across the fleet (Tier 1 baseline, Tier 2 naming) | Accepted |
| [0020](./0020-transactional-outbox.md) | Transactional outbox feeding both the integration and analytics topics from one table, with an in-process relay | Accepted |
| [0021](./0021-rest-identity-static-bearer-scopes.md) | Adopt the fleet REST identity — static bearer keys with read/read-write scopes, `AUTH_MODE` rollout gate | Superseded by [ADR-0022](./0022-remove-rest-mcp-auth.md) |
| [0022](./0022-remove-rest-mcp-auth.md) | Remove the REST + MCP static-bearer auth layer (fleet-wide auth removal) | Accepted |
| [0023](./0023-task-type-on-wire.md) | Put the completed task's own type on `TaskCompleted`'s wire payload, closing labor-performance's documented per-task-type gap | Accepted |
| [0024](./0024-station-location-code-and-workcenter-role-check.md) | Optional `Station.locationCode`, validated against facility-layout's WorkCenter role | Accepted |
| [0025](./0025-cpt-missed-sweep-and-package-manifested.md) | `TaskCPTMissed` sweep and `PackageManifested` on the outbox (companion to order-management ADR 0014 §5) | Accepted |
| [0026](./0026-on-time-to-cpt-kpi.md) | On-time-to-CPT KPI on the throughput analytics data product (companion to order-management ADR 0014 §6) | Accepted |
| [0027](./0027-cloudevents-envelope-migration.md) | Migrate `warehouse.fulfillment.events` to a CloudEvents 1.0 structured envelope via a dual-read/dual-write bake period; companion to wes-work-planning's own envelope-migration ADR-0021 | Superseded by [ADR-0032](./0032-cloudevents-mandatory-envelope.md) |
| [0028](./0028-idempotency-key-middleware.md) | Transactional Idempotency-Key middleware for `POST /tasks`, direct port of order-management ADR-0023 | Accepted |
| [0029](./0029-resilience-circuit-breakers-retry-dlq-shutdown.md) | Circuit breakers, jittered retry, dead-letter-queue hardening, and graceful shutdown for outbound dependencies, direct port of order-management ADR-0025 | Accepted |
| [0030](./0030-horizontal-autoscaling-and-pgxpool-tuning.md) | Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning, direct port of order-management ADR-0026 | Accepted |
| [0031](./0031-migrations-direct-postgres-connection.md) | Run golang-migrate against a direct Postgres connection, not PgBouncer — direct port of order-management ADR-0029 | Accepted |
| [0032](./0032-cloudevents-mandatory-envelope.md) | CloudEvents 1.0 as the mandatory event envelope — every Kafka message (integration + analytics), SDK-built, no flat/dual modes | Accepted |
