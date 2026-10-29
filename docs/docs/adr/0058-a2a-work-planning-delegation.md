---
id: 0058-a2a-work-planning-delegation
slug: /adr/0058-a2a-work-planning-delegation
title: 0058. Delegating work requests to Work Planning over A2A
sidebar_label: 0058. A2A Work Planning delegation
description: "ADR 0058 — the PULL side of the execution/planning seam: an outbound Agent2Agent client (official a2a-go/v2 SDK, real card discovery) behind a WorkPlanning application port, driven by the RequestNextWork use case over POST /paths/{pathId}/work-requests; the authoritative fact still returns as the WorkReleased CloudEvent on Kafka."
---

# 0058. Delegating work requests to Work Planning over A2A

## Status

**Accepted**

## Context

This context is PULL-shaped end to end: a station claims the next task
(`claim-next`), the system selects work rather than workers. Upstream, Work
Planning & Release (wes-work-planning) decides which unit to admit into a
path's pool, and the fact arrives here as the `WorkReleased` CloudEvent,
consumed into Task creation. What did not exist is the loop CLOSING action:
when execution's claimable queue for a path runs dry, nothing asks planning
for more work — the pull stops at the context boundary.

The fleet now has the delegation channel for exactly that: wes-work-planning
exposes an A2A agent ([its ADR-0046](https://github.com/IQVO/wes-work-planning/blob/develop/docs/docs/adr/0046-a2a-inbound-adapter.md))
with a curated `release_next_work` skill over the same use cases REST and MCP
drive, discovered via its Agent Card. The forces:

- **Pull, not push, is this context's core design rule.** The natural seam is
  execution asking planning for the next unit — planning keeps the release
  decision (release policy, WIP invariant), execution keeps the judgement of
  WHEN to ask. Neither learns the other's internals.
- **A2A must not become a data-sync channel.** Facts (what was released)
  still travel as CloudEvents on Kafka; only the delegation (please act)
  travels over A2A. The acknowledgement this context returns to its caller
  is exactly that — an acknowledgement, not the authoritative record.
- **The dependency rule is load-bearing** (ADR-0006): the application layer
  may not know A2A. The delegation must be a port, with the protocol in an
  outbound adapter.
- **The peer is a separate deployable that may be down** — including during
  cluster cold start, when every pod boots at once. Boot must not depend on
  the planning agent's availability, and an unreachable peer must surface as
  a typed outcome, never a panic or a raw transport error.

## Decision

**We will add an outbound A2A client — a `WorkPlanning` application port
implemented by an `internal/adapters/outbound/a2a` adapter over the official
a2a-go/v2 SDK — driven by a `RequestNextWork` use case exposed as
`POST /paths/{pathId}/work-requests`. The released work's authoritative fact
still arrives as the `WorkReleased` CloudEvent through the existing
consumer; nothing about the event path changes.**

### The port, the adapter, the use case

- `ports.WorkPlanning` (with `ports.ReleasedWork` and typed outcomes
  `ErrNoPendingWork`, `ErrUnknownWorkPlanningPath`,
  `ErrWorkPlanningUnreachable`): the application layer's entire view of the
  seam. Goal-shaped — "admit the next unit for this path" — never a data
  query.
- `internal/adapters/outbound/a2a.Client` implements it with REAL card
  discovery: fetch `/.well-known/agent-card.json` from the configured base
  URL, build the protocol client from the card (`a2aclient.NewFromCard`),
  `SendMessage` with the skill request as a Data part, parse the peer's
  envelope (its ADR-0046 contract, mirrored as a test-local copy so contract
  drift fails loudly), and map its in-band RFC 7807 problems onto the typed
  outcomes (409 → no pending work, 404 → unknown path, anything else →
  unreachable/off-contract). The card is resolved lazily on first use and
  cached, so boot never depends on the peer.
- `usecases.RequestNextWork`: validates the path id locally (empty is
  refused, never sent), fails closed with `ErrWorkPlanningNotConfigured`
  when the port is nil, and otherwise delegates. No local state changes —
  by design: the resulting fact arrives through the event consumer.

### The REST surface

`POST /paths/{pathId}/work-requests` (operationId `requestNextWork`): 200
with the acknowledgement (workUnitId, CPT, reference), 409 when planning
answers a business rule (empty pool / WIP limit — a legitimate "nothing to
release", not an error condition), 404 unknown path, 502 peer unreachable,
503 when this deployment has no `WORK_PLANNING_A2A_URL`, 400 empty path id.
Deliberately NOT behind the idempotency middleware: each call asks for
exactly one more unit, and repeating the ask is a legitimate second pull.

### Configuration: one env var, fail-closed

`WORK_PLANNING_A2A_URL` (set by warehouse-infra's `extraEnv`, pointing at
the planning agent's in-cluster Service). Unset means the surface is
explicitly OFF: the endpoint answers 503 rather than dialing anything. No
new chart value, no new deployable — the client is a library inside the
existing OLTP process; A2A servers get their own binaries (the peer's
ADR-0046), A2A clients ride along with their caller.

## Consequences

### Easier

- **The pull loop now closes across the context boundary** with zero change
  to the event path: delegate over A2A out, `WorkReleased` on Kafka back,
  Task created by the existing consumer.
- **The application layer stays protocol-clean.** Only the adapter knows
  A2A; a future protocol change (or a test double) swaps the port
  implementation.
- **Real discovery, not assumptions.** The client trusts the peer's
  advertised card, so transport/interface changes on the peer surface as
  typed unreachable errors here instead of silent misbehaviour.
- **Failure is explicit and typed at every layer**: use case → port → HTTP
  status + RFC 7807 type, with the peer's own reason preserved in the
  detail.

### Harder

- **A synchronous cross-context call exists again — bounded.** It is a
  single goal delegation with a 10s timeout, and the no-data-sync rule
  (queries still forbidden; only this one act) must be enforced in review,
  not by the compiler.
- **One more contract to track.** The adapter mirrors the peer's envelope
  shape; a deliberate peer-side contract change must update this mirror in
  the same wave.
- **The 200 body is a lure for misuse.** Callers could treat it as the
  record of released work; the spec text and this record both state the
  authoritative fact is the CloudEvent. Consoles should render the event,
  not poll this endpoint.
