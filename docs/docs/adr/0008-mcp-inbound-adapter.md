---
id: 0008-mcp-inbound-adapter
title: 8. Model Context Protocol as an inbound adapter, not a new service
sidebar_label: 8. MCP inbound adapter
sidebar_position: 8
description: Expose this bounded context to the AI ecosystem via an MCP server built as a second driving adapter over the existing use cases — Streamable HTTP, official Go SDK, unauthenticated by fleet decision (ADR-0022), curated intent-level tools.
---

# 8. Model Context Protocol as an inbound adapter, not a new service

## Status

**Accepted — auth section superseded by ADR-0022.** First implementation is
`fulfillment-execution`; this ADR is the reference the other four bounded
contexts copy. The original static-bearer-key design (and the read/read-write
scope gating, `auth.go`, and `mcp-secret.yaml` that implemented it) was removed
fleet-wide by [ADR-0022](./0022-remove-rest-mcp-auth.md); the MCP endpoint is
unauthenticated by decision and relies on in-cluster network placement.

**Addendum (2026-09-07) — deployable.** Until this date `cmd/mcp` existed only
as code: the Dockerfile did not build it, the Helm chart had no MCP workload,
and the binary mounted the (then authenticated) MCP handler as its root handler
with no health endpoint (so a Kubernetes probe would have been answered 401).
`feature/mcp-deployment` closes that gap:

- the image now carries `/app/mcp` next to the other three binaries;
- `cmd/mcp` serves `GET /healthz` and mounts the MCP
  Streamable HTTP handler at **both** `/` and `/mcp` (the latter matches
  warehouse-ops-agent's `*_MCP_ENDPOINT` convention);
- the chart gains `mcp-deployment.yaml` and `mcp-service.yaml` (ClusterIP
  `<release>-mcp`, port 8090), both gated on `mcp.enabled` (default `false`), with
  `app.kubernetes.io/component: mcp` on selector and template so the pod is
  distinguishable from the OLTP/projector/reports pods sharing the base
  selector. (An `mcp-secret.yaml` with `MCP_READ_KEY` / `MCP_READWRITE_KEY` was
  part of this addendum originally and was deleted by ADR-0022.)

Deployment to the `warehouse` kind cluster is wired from `warehouse-infra`
(`terraform/services.tf` sets `mcp.enabled`;
`terraform/ops-agent.tf` points warehouse-ops-agent at
`http://fulfillment-execution-mcp.warehouse-systems.svc.cluster.local:8090/mcp`)
and is recorded there once applied.

**Addendum (2026-10) — complete_task publishes through the outbox.** `cmd/mcp`
originally built `CompleteTask` with the log publisher and no `UnitOfWork`, so
`complete_task` over MCP completed the task but never emitted `TaskCompleted`.
`cmd/mcp` now builds its publisher through the same
`internal/composition.BuildEventPublisher` as `cmd/execution`
(`EVENT_PUBLISHER=kafka` + `DATABASE_URL` → the transactional outbox of
[ADR-0020](./0020-transactional-outbox.md), integration and analytics rows
written in the use case's transaction) and wires the same `UnitOfWork`. Only
`cmd/execution` runs the outbox relay that drains those rows to Kafka; the MCP
process only inserts them. Covered by a testcontainers integration test
(`cmd/mcp/outbox_integration_test.go`).

## Context

The platform is being connected to the AI ecosystem (Claude, Cursor, ChatGPT,
agent frameworks). The interoperability standard those clients speak is the
**Model Context Protocol (MCP)**: a client discovers a server's *tools*
(model-callable functions), *resources* (read-only context), and *prompts*
(reusable templates), then an LLM decides which to call.

The forces:

- **There is already a clean action surface.** Every capability of this service
  is an application-layer **use case** (`internal/application/usecases`), one
  struct per use case, reached through ports. The `chi` HTTP adapter is a thin
  driving adapter over exactly those use cases. An AI client needs the same
  actions the HTTP client already has.
- **The domain must not learn about MCP.** ADR-0001's dependency rule is
  load-bearing: domain depends on nothing, application depends on domain,
  adapters depend inward. A protocol whose shape is set by an external LLM
  ecosystem is precisely the kind of concern that must stay in an adapter.
- **MCP has an idiomatic Go path now.** The official **MCP Go SDK**
  (`github.com/modelcontextprotocol/go-sdk`) is a Tier-1 SDK. Building the
  server in Go keeps it in the same language, module, and quality gate as the
  rest of the service — no Python sidecar, no second toolchain.
- **The spec is versioned aggressively.** Revisions in 2025-06, 2025-11, and
  2026-07 have already deprecated features (`roots`, `sampling`, `logging` —
  SEP-2577). Whatever is built will need to track a moving contract.
- **Tools are model-controlled and can act.** Unlike an HTTP client driven by
  code we wrote, an LLM chooses *when* to call a tool and *with what arguments*.
  The spec's own guidance is emphatic: curate a small set of intent-level
  tools, treat tool invocation as requiring host consent, and guard
  state-changing tools most heavily.
- **This is an internal, non-user-facing deployment.** The servers run inside
  the `warehouse` kind cluster for agent and developer use, not on the public
  internet for end users. The original design used a static bearer token for
  this case; the fleet later chose to run MCP (and REST) unauthenticated
  inside the cluster instead ([ADR-0022](./0022-remove-rest-mcp-auth.md)).

## Decision

**We will expose this bounded context to the AI ecosystem through an MCP server
built as a second driving adapter over the existing use cases — leaving the
domain and application layers untouched.**

### The adapter, mirroring the HTTP one

A new `internal/adapters/inbound/mcp/` sits beside `internal/adapters/inbound/http/`:

```
internal/adapters/inbound/mcp/
  server.go      MCP Server wiring (Go SDK), capability registration
  tools.go       intent-level tool handlers -> call use cases
  resources.go   read-model resources (scoped, not bulk)
  prompts.go     workflow prompts (operational SOPs)
  mapping.go     tool I/O <-> DTOs; RFC 7807 -> structured tool errors
```

It depends inward on `application` exactly as the HTTP adapter does. No MCP type
appears in `internal/domain/**` or `internal/application/**`. The tool handlers
call the **same** use case structs the HTTP handlers call — never a parallel
code path, never the domain directly.

### A separate `cmd/mcp` binary

The MCP server ships as its own composition root, `cmd/mcp/main.go`, reusing the
same repositories, ports, and `EVENT_PUBLISHER` wiring as `cmd/execution` (the
shared `internal/composition` builder). Two
deployables from one module: the HTTP service and the MCP server. This isolates
blast radius and lets the two scale independently.

### Streamable HTTP only

The single supported transport is **Streamable HTTP**, stateless where the SDK
allows. We do not ship stdio builds; local desktop-client use goes through the
same HTTP endpoint. One transport is one thing to trace and test.

### Curated, intent-level tools — not one tool per endpoint

Tools are designed around decisions an agent makes, not around REST endpoints.
Mechanically wrapping every HTTP route would overwhelm the model — the
documented number-one MCP anti-pattern. The pilot surface for this context:

- `get_queue_status` (read) — depth and oldest CPT per process path.
- `find_claimable_work` (read) — what a station could claim right now.
- `diagnose_stuck_tasks` (read) — leases near expiry and why.
- `complete_task` (write, annotated destructive) — wraps the `CompleteTask`
  use case; the existing at-most-once / ownership / lease invariants make a
  model-invoked completion safe by construction.

Resources expose existing read models as **scoped** context contracts
(`queue://fulfillment/{processPath}/status`), never a database dump. Prompts
encode operational SOPs (e.g. `triage_backlog`: how to read the queue, when to
escalate, what "done" means).

### Unauthenticated, in-cluster

> **Superseded by [ADR-0022](./0022-remove-rest-mcp-auth.md).** This section
> originally specified static bearer-key auth (`auth.go`, per-client keys from
> a Kubernetes Secret, read-only and read-write key classes gating write
> tools, `401` on a missing key) behind an OAuth-ready interface. All of it was
> removed fleet-wide. Today there is no auth middleware, no key Secret and no
> scope gating: every MCP client may call every tool, `complete_task`
> included. The safeguard is network placement (the server stays in-cluster)
> plus the domain invariants. Re-adopting auth is an ADR-0022 decision, not
> something this adapter anticipates with a seam.

### Reuse the existing observability

The adapter is instrumented with the same OpenTelemetry setup as the HTTP and
Kafka boundaries: a span per tool call (`mcp.tool <name>`, outcome recorded on
the span), and every request to `cmd/mcp` carries the same HTTP RED
instrumentation as the REST routers (`otelchi` + `otelchimetric`,
[ADR-0019](./0019-standard-metrics-convention.md)). There are **no dedicated
MCP invocation or denial counters**: invocation volume and latency are read
from the spans and `http.server.request.duration`, and there is no denial path
to count now that auth is gone. MCP calls appear in Jaeger and Grafana next to
HTTP requests, continuing the same distributed traces.

## Consequences

### Easier

- **The domain and application layers do not change at all.** MCP is purely
  additive; the dependency rule (ADR-0001) is preserved and checked by the
  existing arch-go fitness tests (ADR-0006).
- **One action surface, two protocols.** HTTP and MCP call the same use cases,
  so behaviour — including every invariant — is identical regardless of caller.
- **Model-invoked writes are safe by construction.** At-most-once claiming
  (ADR-0003) and ownership checks already reject a double `complete_task`; the
  RFC 7807 error (ADR-0005) surfaces as a clean structured tool error.
- **It stays in Go, in one quality gate.** The MCP adapter is unit-tested to the
  same ≥90% bar, linted, and CI-gated like every other package.
- **The auth question is closed, not deferred.** Moving to OAuth is no longer a
  contained adapter change behind a seam: auth was deliberately removed
  (ADR-0022), so adding it back is a new decision.

### Harder

- **A second deployable to run.** `cmd/mcp` is another binary, image,
  Helm release, and ingress. The isolation is deliberate but it is real
  operational surface that did not exist before.
- **No authentication.** The MCP endpoint is open to anything that can reach
  it (ADR-0022). That is appropriate only while the server stays in-cluster;
  it does **not** cover user-facing or multi-tenant use.
- **The MCP spec is a moving target.** Aggressive versioning and deprecations
  mean the SDK must be pinned and revisited; features like `roots`/`sampling`
  are already deprecated and must be avoided in favour of tool parameters.
- **Tool curation is an ongoing discipline, not a one-time choice.** Nothing in
  the compiler stops a future PR from adding a tool per endpoint. The MCP
  governance charter (`docs/mcp/governance-charter.md`) and a CI lint on tool
  count/annotations exist to hold the line; without them the surface degrades.
- **LLM-chosen arguments are untrusted input.** Every tool handler must validate
  its inputs defensively — the caller is a model, not our own code — which is
  stricter than what the HTTP DTO layer assumes.
- **`complete_task` is a state change an autonomous agent can trigger.** It is
  annotated destructive and the spec expects host-side consent, but there is
  no server-side scope gate or rate limit (ADR-0022), and the residual risk of
  an agent completing the wrong task is higher than for a human-driven HTTP
  call. The domain invariants bound the damage; they do not eliminate the
  judgement risk.
