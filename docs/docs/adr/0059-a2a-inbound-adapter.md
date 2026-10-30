---
id: 0059-a2a-inbound-adapter
slug: /adr/0059-a2a-inbound-adapter
title: 0059. A2A inbound adapter (third driving adapter)
sidebar_label: 0059. A2A inbound adapter
description: "ADR 0059 - fulfillment-execution is an Agent2Agent (A2A) agent through a third driving adapter over the same use cases as REST and MCP. Two curated goal-shaped skills, backlog_status (read) and complete_task (write, the one write MCP already offers), in a message-only agent served by cmd/a2a on port 8093. A2A carries delegation only; TaskCompleted still leaves as a CloudEvent through the transactional outbox. Independent of the outbound client of ADR-0058."
---

# 0059. A2A inbound adapter (third driving adapter)

## Status

**Accepted**. Follows [ADR-0008](./0008-mcp-inbound-adapter.md) (the MCP
adapter this one is modelled on), [ADR-0022](./0022-remove-rest-mcp-auth.md)
(every endpoint is unauthenticated), [ADR-0020](./0020-transactional-outbox.md)
(transactional outbox), [ADR-0032](./0032-cloudevents-mandatory-envelope.md)
(CloudEvents on every Kafka message), [ADR-0052](./0052-postgres-and-kafka-are-the-only-runtime.md)
(Postgres and Kafka are the only runtime) and sits beside
[ADR-0058](./0058-a2a-work-planning-delegation.md), the OUTBOUND A2A client.

## Context

The fleet is adopting the Agent2Agent protocol so that a bounded context can
**delegate** a goal to another context's agent. This context is already an A2A
*client* (ADR-0058: it asks wes-work-planning for the next work unit) and
already exposes its use cases over REST and MCP. What it did not have is the
other half: an A2A *server* other agents can delegate to.

Three transports now carry three different things and must not overlap:

| Transport | Carries |
|---|---|
| Kafka + CloudEvents | **facts** between contexts (ADR-0032) |
| MCP | an agent's **tools** (reads and single actions) |
| A2A | **delegation** of a goal to another context's agent |

## Decision

Add `internal/adapters/inbound/a2a`, a third driving adapter over the **same**
use cases (`GetQueueDepth`, `CompleteTask`, and the read-only task-query port
the MCP adapter already uses) as the HTTP and MCP adapters, built on the
official `a2aproject/a2a-go/v2` SDK (pinned v2.7.0) and served by a new
`cmd/a2a` binary: JSON-RPC 2.0 over HTTP, port **8093**, the Agent Card at
`/.well-known/agent-card.json`, the endpoint mounted at `/` and `/a2a`, and
`GET /healthz`. `cmd/a2a` is a clone of the `cmd/mcp` composition root (same
real Postgres, same real outbox publisher, relay NOT started) plus one
mandatory variable, `A2A_AGENT_URL`: an absolute http(s) URL advertised on the
card, refused at boot otherwise.

### Inbound and outbound stay separate

`internal/adapters/inbound/a2a` (server) and `internal/adapters/outbound/a2a`
(the ADR-0058 client) share no code and no state. The server never imports the
SDK's `a2aclient` package; the client is untouched by this change and keeps
living inside the OLTP process. `cmd/a2a` does not wire the client. An
architecture test pins all of it (see Consequences).

### Two curated skills, not one per endpoint

Derived from the MCP tool surface (`get_queue_status`, `find_claimable_work`,
`diagnose_stuck_tasks`, `complete_task`), not from the REST routes:

| Skill | Kind | What it does |
|---|---|---|
| `backlog_status` | read | for one process path (PICK, PACK, SLAM, RELOCATE, REPLENISH, BREAK_CASE): Pending depth, how many tasks are claimable now with the earliest-CPT one (with its RELOCATE/REPLENISH move), and that path's claimed tasks whose lease has expired or expires within `withinSeconds` (default 0). Changes nothing. |
| `complete_task` | write | completes a claimed task for the station holding the claim, with the operator's confirmation for RELOCATE/REPLENISH; publishes `TaskCompleted` through the transactional outbox. |

The three MCP diagnostics that answer one question ("how is this path's
backlog?") are composed into `backlog_status` rather than offered as three
skills.

**Write gate.** An A2A skill may change state only if the context's MCP
surface already offers that write. MCP offers exactly one: `complete_task`
(annotated destructive, [governance charter](../mcp/governance-charter.md)).
A2A offers the same single write and nothing wider: it does not claim,
release, create or scan work, and does not run the BREAK_CASE flow (a
BREAK_CASE task completes only through its own endpoint, REST and MCP agree,
and the skill answers that as a `409`). The reports tools of MCP are not
carried over: they are a client of another deployable and not a
delegation-shaped goal.

### Message-only, answered in-band

Both skills are transactional request/response work, so the agent is
message-only (no Task lifecycle). Domain rejections and malformed requests are
**answered** as RFC 7807-shaped `problem`s inside the message envelope
(`{skill, data | problem}` plus a one-line text part), never raised as JSON-RPC
transport errors, with the **same classification as the REST adapter**
(`statusFor`, [ADR-0005](./0005-rfc-7807-problem-details.md)) and its problem
slugs: `404` task not found; `400` malformed request or invalid argument
(including an unknown process path); `409` already completed, not claimed,
not the claim owner, BREAK_CASE completion; `422` the missing or mismatched
RELOCATE/REPLENISH confirmation, or confirmation inputs on another task type;
`500` anything else, with a generic detail (the cause goes to the server log
and trace, never to the caller). The `503` class of the REST adapter
(inventory-storage unreachable for a case break) is not reachable from these
two skills and is not mapped.

### Strict input

Unknown argument names are **rejected**, not ignored (a misspelled
`withinSecs` must not silently drop the lease window), and trailing data after
the JSON document is rejected. Go's `encoding/json` matches names
case-insensitively, so `withinseconds` still reaches `withinSeconds`. Optional
values are pointers, so `quantity: 0` is refused (`at least 1`) exactly as
REST refuses it. This context has no deprecated alias or key that the A2A
surface withholds, so there is no alias mapping to do; the process path is
case-sensitive upper case as on MCP.

### Facts still travel on Kafka

A2A never becomes a data-sync channel. `complete_task` writes the same outbox
rows `POST /tasks/{id}/complete` writes (integration and analytics topics, in
the use case's transaction); `cmd/a2a` never runs the relay (only
`cmd/execution` drains `outbox_events`). Proven on real Postgres by
`TestA2ACompleteTask_LeavesUnpublishedOutboxRowsForBothTopics` (build tag
`integration`), which also proves a rejected completion stores nothing.

### Unauthenticated, like REST and MCP

The card declares no security schemes: this surface is unauthenticated by the
same fleet-wide decision as REST and MCP (ADR-0022). Schemes are added to the
card, not in a redesign, if that decision is ever reversed.
`TestNoAuthMiddlewareReintroduced` already scans `internal/adapters/inbound/**`,
which now includes this adapter.

## Consequences

- A fifth deployable (`/app/a2a`) in the same image and an `a2a.*` chart block
  (`<release>-a2a`, default **off**), exposed through Kong at
  `/api/a2a/fulfillment-execution` rewritten to `/`, so the well-known card
  stays reachable under the same prefix. The chart takes the pod's
  `A2A_AGENT_URL` from the in-cluster Service DNS unless `a2a.agentUrl`
  overrides it. Port 8093 is the fleet's A2A port (8080 api, 8090 mcp, 8091
  projector admin, 8092 reports).
- The dependency rule is extended (`TestA2AInboundAdapterDependencyRule`): the
  adapter depends only on application and domain, nothing else in `internal/**`
  depends on it, it never imports `a2aclient`, and only the outbound adapter
  (plus tests) does. `internal/adapters/inbound/**` already may not depend on
  `outbound/**`, which keeps the two A2A halves apart.
- `complete_task` is **not idempotent**: a second call on the same task is
  rejected as already completed (`409`), nothing is written. The card says so.
  Using the A2A `messageId` as an idempotency key is a possible follow-up; the
  MCP tool has the same property today.
- Unlike MCP, a delegated completion has no human or model in the loop by
  construction: the at-most-once and claim-ownership invariants bound a
  mistaken call, and the RELOCATE/REPLENISH confirmation values are the
  operator's, never the delegating agent's to invent (the card says so).
- `cmd/a2a` does not register the report tools and so needs no
  `REPORTS_BASE_URL`; adding a skill later needs its own ADR section and, if it
  writes, an MCP counterpart first.
- The A2A server is stateless (message-only), so unlike `cmd/mcp` it has no
  session affinity concern; the chart still ships it single-replica and without
  an HPA until a load case exists.
