---
id: 0013-fulfillment-mfe-console-adoption
title: 13. Adopt the fleet's micro-frontend console architecture — fulfillment-mfe remote, GET /tasks?orderRef=, and CORS
sidebar_label: 13. fulfillment-mfe console adoption
sidebar_position: 13
description: This service's local adoption of the fleet-wide micro-frontend console architecture (canonical decision recorded in warehouse-ops-agent's ADR-0002) — the fulfillment-mfe Module Federation remote in web/, the GET /tasks?orderRef= endpoint it and the console's Order Lifecycle BFF both depend on, and the CORS middleware that makes either reachable from a browser.
---

# 13. Adopt the fleet's micro-frontend console architecture — `fulfillment-mfe`, `GET /tasks?orderRef=`, and CORS

## Status

**Accepted.**

## Context

`warehouse-ops-agent`'s [ADR-0002](https://github.com/claudioed/warehouse-ops-agent)
(the fleet-wide canonical record for this decision) settled the strategic
question for all six bounded contexts at once: one Module Federation
micro-frontend remote per service, owned in that service's own repo, composed
at runtime by a separate `warehouse-console` shell, with the one genuinely
cross-cutting screen (Order Lifecycle) served by a thin BFF hosted in
`warehouse-ops-agent` rather than as a new bounded context. That record is
**not duplicated here** — this ADR is the local, adoption-side half: what
changed inside *this* repository as a direct consequence, and why those
specific changes were the minimal correct shape rather than something this
repo invented independently.

This service was one of three with a documented, concrete gap the canonical
decision surfaced: `fulfillment-execution` exposed no way to look up "every
task recorded for a given order," yet the console's Order Lifecycle screen
(and, standalone, this service's own `fulfillment-mfe` remote) needed exactly
that to trace a Pick/Pack/SLAM task back to the order it belongs to. The
other two forces, specific to this repo:

- **No service had ever needed a browser client before.** `router.go` had no
  CORS middleware at all — a same-origin assumption was implicit everywhere,
  same as the other five contexts before their own adoption.
- **The join key `fulfillment-execution` expects is not the plain order
  id.** `Task.OrderRef` is stamped from `WorkReleased`'s `work_unit_id`,
  which is `wes-work-planning`'s own composite id
  (`<orderId>-line-<lineNo>`), not `order-management`'s `OrderId`. Any
  caller of `GET /tasks?orderRef=` — this service's own `fulfillment-mfe`
  screen or the console's BFF — must supply that composite id, not the
  order id directly; the BFF resolves it by querying
  `wes-work-planning`'s `GET /work-units?reference=` first (see
  `warehouse-ops-agent`'s ADR-0002 for the full multi-hop join). This
  service does not translate the key itself — `orderRef` here means
  exactly what `Task.OrderRef()` already means, unchanged by this record.

## Decision

**Ship the same three additive pieces every adopting service in the fleet
shipped, and nothing more: the `GET /tasks?orderRef=` endpoint
([e10cd49](https://github.com/claudioed/fulfillment-execution/commit/e10cd49),
merged via `feature/tasks-by-order-ref`), a `web/` directory containing the
`fulfillment-mfe` Module Federation remote, and `go-chi/cors` middleware on
the existing HTTP adapter.**

### `GET /tasks?orderRef=` — the minimal additive read

`ports.TaskRepo.FindByOrderRef` backs `usecases.GetTasksByOrderRef`, wired at
`GET /tasks?orderRef=` in `internal/adapters/inbound/http/handlers.go`. No
existing endpoint, domain type, or use case was modified to build it — the
same "one new file alongside the existing repository-adapter pattern"
discipline the canonical ADR calls for. It returns every task recorded for
`orderRef` as an array, including retried legs (a lease-expiry retry creates
a second task for the same leg), and never 404s on an unrecognized
`orderRef` — 200 with an empty array instead, the same convention
`GET /queues/{taskType}/depth` already established for an unknown
`taskType`. This is the endpoint documented in `apis/openapi.yaml`'s
`operationId: getTasksByOrderRef` and now rendered at
`docs/api-reference/rest/get-tasks-by-order-ref` (the generated page this
record's own PR restored — see the sibling stale-docs fix landed alongside
this ADR).

### `web/` — `fulfillment-mfe`, this repo's own remote

`web/` is a Vite + React Module Federation **remote** (`fulfillment-mfe`,
dev port `5184`), consumed by the separate `warehouse-console` shell repo.
It is a plain browser client of this service's own REST API
(`FULFILLMENT_API_BASE`) — nothing in `web/` talks to any other bounded
context, and nothing in `internal/` knows `web/` exists. `FulfillmentScreen`
ships two panels: the pre-existing queue-depth read (`GET
/queues/{taskType}/depth`) and a new task-by-`orderRef` lookup table backed
directly by the endpoint above. Decisions about what this screen shows are
this repo's own — same as its REST API — not something `warehouse-console`
or any sibling remote's PR touches. `web/` has its own `package.json`, build,
and dev server; it does not participate in this repo's Go quality gate
(`make check`/`check-all`) and is not part of the Go module.

### CORS — additive middleware, not a gateway

`corsMiddleware()` in `internal/adapters/inbound/http/router.go` allows
`http://localhost:5173` (the `warehouse-console` shell) and
`http://localhost:5184` (this repo's own `fulfillment-mfe` dev server) by
default, overridable via `CORS_ALLOWED_ORIGINS` (comma-separated) for
staging/prod. REST is unauthenticated by decision (ADR-0022; the static-bearer
auth this ADR was first written against was removed) and there are no cookies, so
`AllowCredentials: false` — no credentialed-CORS surface was added. This was
added directly to the existing HTTP adapter, not via a shared API gateway or
reverse proxy, matching every other adopting service.

## Consequences

### Easier

- **The console's Order Lifecycle screen can now reach this service's
  data at all.** Before `GET /tasks?orderRef=` existed, no lookup-by-key
  endpoint existed on this service; the BFF's fourth hop was structurally
  impossible.
- **Ownership of the fulfillment screen stays with this repo.** A future
  change to `Task`'s shape (a new field, a new status) and the screen that
  renders it land in the same PR, same repo, same review — no coordination
  with `warehouse-console` or any sibling remote required.
- **The new endpoint is genuinely minimal and additive**, reviewed and
  tested to this repo's own ≥90% coverage bar; no existing domain, use
  case, or endpoint changed shape to support it.

### Harder

- **CORS is now permanent surface on this service.** `CORS_ALLOWED_ORIGINS`
  must be kept current as new remote dev ports or a real deployed console
  origin appear; a forgotten update is a silent browser-side "Failed to
  fetch," not a loud backend error — the same risk the canonical ADR
  already flagged fleet-wide.
- **`web/` is a second build/release surface inside this repo** with its
  own npm dependency tree (including `@warehouse/ui-kit` via
  `file:../../warehouse-ui-kit`, a sibling-checkout coupling, not an npm
  registry publish) that this repo's Go CI does not exercise — a gap that
  needs its own CI job, not assumed coverage from `make check-all`.
- **The `orderRef` join-key asymmetry is invisible from this repo's API
  alone.** `GET /tasks?orderRef=` looks, from this service's own docs, like
  it takes "the order reference" in the ordinary sense; only
  `warehouse-ops-agent`'s ADR-0002 records that a console caller must
  supply the *WorkUnit* id, not the *Order* id. A caller reading only this
  repo's OpenAPI spec and reasoning from the parameter name alone could
  reasonably (and incorrectly) pass the order id straight through.
