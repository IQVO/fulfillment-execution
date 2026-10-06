---
paths:
  - "internal/**"
  - "cmd/**"
  - "migrations/**"
---

# Architecture & Layout Notes

Hexagonal / Ports & Adapters (ADR-0001). Strict dependency rule: **domain
depends on nothing; application depends on domain; adapters depend on
application/domain; only `cmd/` wires every layer.** No framework or SQL types
in the domain layer. Enforced by `make arch-test` (ADR-0006,
`internal/architecture/`).

Non-obvious placement facts (everything else: `ls internal/`):

- Four `cmd/` binaries: `execution` (OLTP composition root),
  `fulfillment-projector` (analytics WRITER: analytics topic -> analytical DB),
  `fulfillment-reports` (analytics READ-ONLY reader, serves `GET /reports/...`),
  `mcp` (MCP server, adds the report tool).
- `internal/adapters/kafka/cloudevents/` is the ONLY CloudEvents 1.0 envelope
  helper (`New`/`Decode`/`ContentTypeHeader`, ADR-0032).
- `internal/domain/consolidation/` (OrderConsolidation, Rebin fan-in tracker)
  is execution-scoped only. `internal/domain/pathcatalog/` is the process-path
  catalogue model (prefix-match lookup, ADR-0017).
- `internal/analytics/report/` is the analytical read model + store ports
  (ADR-0012); the OLTP layers must never import it.
- Outbound ports live in `internal/application/ports/` (incl.
  `EquipmentCommandPort`, the WCS ACL seam, ADR-0015). Postgres repos,
  migrations and the transactional outbox are in
  `internal/adapters/outbound/postgres/` (ADR-0020); SQL files in
  `migrations/` and `migrations/analytics/`.
- `charts/fulfillment-execution/` is the Helm chart, deployed by
  warehouse-infra's `local.services` map.
- Typed domain errors map to HTTP status as RFC 7807
  `application/problem+json` (ADR-0005) in the adapter layer only.
