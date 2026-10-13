---
id: 0043-reports-api-openapi-contract
slug: /adr/0043-reports-api-openapi-contract
title: "43. The reports binary's routes are a contract: apis/openapi-reports.yaml"
sidebar_label: "43. Reports API contract"
description: "ADR 0043 — every route of cmd/fulfillment-reports (GET /healthz, /reports/throughput, /reports/throughput/freshness) is specified in apis/openapi-reports.yaml, linted by Spectral, rendered as a generated Reports API reference and pinned by Go contract tests against the real router. Fleet gap-fix decision D18."
---

# 43. The reports binary's routes are a contract: `apis/openapi-reports.yaml`

## Status

Accepted. Decided 2026-10-12 (fleet gap-fix wave, decision D18: every
`*-reports` binary's routes go into `apis/openapi-reports.yaml`, the
convention labor-performance started). Extends
[ADR-0012](./0012-analytical-data-product.md) (the analytical data product
and its separate reader); that ADR's body is not edited.

## Context

ADR-0012 split the Throughput & Lease-Health data product into a writer
(`cmd/fulfillment-projector`) and a read-only reader
(`cmd/fulfillment-reports`, default `:8092`, `:8080` in the chart). The
reader serves three routes (`NewReportsRouter` in
`internal/adapters/inbound/http/reports_handler.go`): `GET /healthz`,
`GET /reports/throughput` and `GET /reports/throughput/freshness`. The MCP
report tools (`get_fulfillment_throughput_report`, `get_on_time_to_cpt`) and
other contexts (the console, warehouse-ops-agent) call them.

None of these routes was in a spec. `apis/openapi.yaml` describes
`cmd/execution` only. The shape lived in a hand-written page
([Throughput report](../analytics/throughput-report.md)) and in the
handler's DTOs, so nothing linted it, nothing rendered it and nothing failed
when the handler and the documentation disagreed.

## Decision

1. **A second OpenAPI 3.0.3 file, `apis/openapi-reports.yaml`,** specifies
   every route of `cmd/fulfillment-reports`: `getReportsHealthz`,
   `getThroughputReport` (query `from`, `to` required RFC 3339; `taskType`,
   `stationId` optional exact match; `granularity` optional, only `hour`)
   and `getThroughputFreshness`; the `ThroughputRow` with the ten fields the
   handler emits, `Freshness`, `Status` and the RFC 7807 `Problem`
   ([ADR-0005](./0005-rfc-7807-problem-details.md)) with the two reports
   problem types (`invalid-report-query` 400, `report-store-error` 500). No
   security scheme ([ADR-0022](./0022-remove-rest-mcp-auth.md)). It
   describes what already ships: no route, field or status changes.
2. **Linted** by the `api-lint` CI job with the same `.spectral.yaml` as the
   OLTP spec.
3. **Rendered** as a generated Reports API reference: a second
   `docusaurus-plugin-openapi-docs` config (`fulfillmentReports`, output
   `docs/docs/api-reference/rest-reports`) and `gen-api-docs:all` /
   `clean-api-docs:all` scripts. The `docs-api-drift` job regenerates both
   specs and fails on any change or untracked page.
4. **Contract-tested against the real router.** Schemathesis (the
   `contract` job) boots `cmd/execution` in memory; `cmd/fulfillment-reports`
   refuses to start without an analytical database. So two Go tests in the
   `test` job read the spec instead:
   - `TestReportsSpecAndRouterAgree` walks the chi router and fails when a
     route is served but not specified, or specified but not served.
   - `TestReportsResponsesConformToSpec` drives every documented status of
     every operation with a stub store and fails on an undeclared status or
     media type, a missing required property or an undocumented property
     (rows included).

## Consequences

- A new reports route or field fails the build until the spec, and so the
  generated reference, describe it.
- Consumers have a published shape to code against.
- The spec does not prove value semantics (that a counter counts the right
  event); the analytics store's integration tests and the handler unit
  tests keep doing that.

## Alternatives considered

- **Add the routes to `apis/openapi.yaml`.** Rejected: that spec describes
  `cmd/execution`, and Schemathesis would then call `/reports/*` on a binary
  that does not serve them.
- **Schemathesis against `cmd/fulfillment-reports` with a testcontainers
  Postgres.** Rejected for now: it needs the projector's migrations and a
  container in the `contract` job for three read-only GET operations whose
  inputs the Go tests already cover.
