# Project: Fulfillment Execution (Core Bounded Context)

Turns released work into completed physical operations: the **task lifecycle**
for Pick, Pack, SLAM, and Rebin. Downstream of Work Planning (which releases
work); issues commands to WCS/equipment (seam only — no WCS tier exists yet).
Defining design rule: **pull, not push** — a station claims the next task
(`claimNext(stationId, capabilities)`); the system selects work, not workers.
Assignment is at-most-once with a **lease**, so an unconfirmed task returns to
the pool rather than vanishing.

Source of truth for the ubiquitous language: `/Users/claudioed/docs/amazon-fulfillment-ddd.md`
and `/Users/claudioed/warehouse-systems-ddd.md`. Honor those exact names.

**Study project**, not production: real DDD/hexagonal patterns, not affiliated
with any company (see README.md banner).

## Project Overview

- Module: `github.com/claudioed/fulfillment-execution`, Go 1.26.
- Three long-running processes + one MCP server (see Architecture below).
- REST inbound API, Kafka inbound consumer (`WorkReleased`), Postgres storage,
  Kafka outbound integration + analytics events — all CloudEvents 1.0 (see
  "Events" below).
- Also owns a browser micro-frontend remote, `web/` (`fulfillment-mfe`) —
  entirely separate build, not part of the Go module or its quality gate.
- Full docs site (generated OpenAPI reference, hand-written AsyncAPI/Events
  page, DDD artifacts, ADRs): <https://claudioed.github.io/fulfillment-execution/>,
  source in `docs/`, deploy workflow `.github/workflows/docs.yml`.

## Architecture (NON-NEGOTIABLE)

Hexagonal / Ports & Adapters. Strict dependency rule: **domain depends on
nothing; application depends on domain; adapters depend on
application/domain.** No framework or SQL types in the domain layer
(enforced by `make arch-test`, ADR-0006).

```
cmd/execution/               main.go — OLTP composition root
cmd/fulfillment-projector/   analytics WRITER: analytics topic -> analytical DB
cmd/fulfillment-reports/     analytics READ-ONLY READER: serves GET /reports/...
cmd/mcp/                     MCP server (adds the report tool)
internal/
  domain/
    task/                    Task aggregate (Pick|Pack|SLAM|Rebin lifecycle, lease)
    station/                 Station aggregate (occupant, capabilities)
    package/                 Package aggregate (pack -> sealed; SLAM weigh-check; segregation)
    consolidation/           OrderConsolidation — Rebin fan-in tracker (execution-scoped only)
    pathcatalog/             Process-path catalogue model (prefix-match lookup, ADR-0017)
    shared/                  value objects: TaskId, StationId, CPT, Capability, events
  analytics/report/          analytical read model + store ports (ADR-0012)
  application/
    ports/                   OUT: TaskRepo, StationRepo, PackageRepo, EventPublisher,
                              Clock, ProductClassificationLookup, LocationRoleLookup, EquipmentCommandPort (ACL seam)
    usecases/                one struct per use case
  adapters/
    inbound/http/            chi handlers, DTOs, error mapping (OLTP + reports)
    kafka/cloudevents/       the ONLY CloudEvents 1.0 envelope helper (New/Decode/ContentTypeHeader, ADR-0032)
    inbound/kafka/           WorkReleased consumer + analytics projector consumer
    inbound/mcp/             MCP tools (incl. get_fulfillment_throughput_report)
    outbound/postgres/       pgxpool repos + migrations + transactional outbox (ADR-0020)
    outbound/analyticsstore/ analytical DB writer + read-only reader
    outbound/memory/         in-memory repos for tests/local
    outbound/kafka/          integration publisher + analytics publisher
    outbound/kafkacatalog/   process-path catalogue Kafka-config consumer
    outbound/filecatalog/    process-path catalogue file-config loader
    outbound/productclassification/ live per-SKU DOT hazard lookup client + permissive fallback
    outbound/facilitylayout/ station LocationCode role lookup client + permissive fallback (ADR-0024)
    outbound/events/         log/buffered/multi publisher
migrations/                  golang-migrate SQL files
migrations/analytics/        analytical schema migrations
web/                         fulfillment-mfe: Vite + React Module Federation remote (see rules/frontend-mfe.md)
charts/fulfillment-execution/ Helm chart (deployed by warehouse-infra's local.services map)
```

Full domain vocabulary, aggregate invariants, domain events, and use-case list
are in `.claude/rules/ubiquitous-language.md` — load it before touching
`internal/domain/` or `internal/application/usecases/`.

## Key Commands

```bash
make check       # fast pre-commit gate: fmt-check vet build lint test
make check-all   # check + coverage(90%) + arch-test + bdd — run before pushing
make vuln        # govulncheck; run when touching go.mod/go.sum
make mutation-fast  # blocking gremlins subset; thresholds in .gremlins.yaml
make integration    # needs DATABASE_URL + running Postgres; excluded from check
lefthook install    # one-time: activates pre-commit/pre-push hooks
```

```bash
# Docs site (Docusaurus) — see rules/docs-and-api-drift.md for the full
# regeneration procedure and why it is NOT wired into `npm run build`.
cd docs && npm ci
npm run clean-api-docs fulfillment && npm run gen-api-docs fulfillment  # regenerate REST reference
npm run typecheck && npm run build   # what docs.yml runs on main; nothing runs them on a PR
```

```bash
docker-compose up -d          # local Postgres 16 (this repo)
# Shared Kafka: warehouse-infra kind cluster, host listener localhost:9092
export PATH_CATALOGUE_FILE=~/warehouse-systems/warehouse-infra/config/process-paths/sortable-fc.yaml
go run ./cmd/execution         # OLTP API (fatal at boot without a catalogue file)
go run ./cmd/fulfillment-projector  # analytics writer
go run ./cmd/fulfillment-reports    # analytics reader API
go run ./cmd/mcp                    # MCP server
```

## REST API surface

15 operations in `apis/openapi.yaml` across Tasks / Stations / Packages /
System, plus `POST /rebin/arrivals` (on the router, missing from the spec). Full endpoint list,
the `orderRef` cross-service contract, and CORS config are in
`.claude/rules/api-and-integration.md`. Spec: `apis/openapi.yaml`. Generated
docs: `docs/docs/api-reference/rest/*.api.mdx` — **must be regenerated by hand**
after any `apis/openapi.yaml` change (see that rule file).

## Events: CloudEvents 1.0 is MANDATORY

Every Kafka message this service produces or consumes (integration
`warehouse.<ctx>.events` AND analytics `warehouse.<ctx>.analytics`) is a
CloudEvents 1.0 event in structured content mode. This is a hard fleet rule,
not a preference:

- No flat envelope (`event_id`/`event_type`/`occurred_at`), no dual-write,
  no dual-read, no envelope toggle env var (`EVENT_ENVELOPE_MODE` is gone).
- Build/validate/(un)marshal with `github.com/cloudevents/sdk-go/v2/event`
  via `internal/adapters/kafka/cloudevents/`; transport stays kafka-go.
- Kafka header `content-type: application/cloudevents+json; charset=UTF-8`.
- Required attributes: `specversion=1.0`, `id` (UUID, stable across outbox
  redelivery), `source=/warehouse/fulfillment-execution`, `type`, `subject` (aggregate id), `time`
  (occurred-at, UTC), `datacontenttype=application/json`,
  `dataschema=urn:warehouse:fulfillment-execution:<events|analytics>:<EventName>:v<N>`.
- `type` = `com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>`;
  for this service: `com.warehouse.wes.fulfillment-execution.<entity>.<EventName>`. Breaking payload
  change => new `.v2` type + new dataschema version, never mutate.
- Consumers dispatch on the FULL `type`, ignore unknown types, dedupe on
  `id`, and DLQ/skip (never crash, never parse a legacy shape) anything that
  fails CloudEvents validation.

Full standard and the fleet's cross-service type catalogue: ADR-0032
(`docs/docs/adr/`).

## Code Standards / Testing

- Go 1.26, modules; chi (`go-chi/chi/v5`); pgx/v5 + pgxpool; golang-migrate.
- Config via env (`DATABASE_URL`, `HTTP_ADDR`, `ANALYTICS_DATABASE_URL`, mode
  flags like `PRODUCT_CLASSIFICATION_MODE=http|permissive`).
- Typed domain errors mapped to HTTP status (RFC 7807 `application/problem+json`,
  ADR-0005) in the adapter layer only.
- Table-driven tests: domain + application (in-memory adapter); one httptest
  per endpoint; build-tagged Postgres integration test (`-tags=integration`,
  skipped without `DATABASE_URL`; Kafka integration tests use testcontainers,
  never a shared external broker).
- gofmt/go vet clean; every package has a doc comment.
- Definition of done: `go build ./...`, `go vet ./...`, `go test ./...` green;
  README run steps current; failing-path tests exist for at-most-once claim,
  capability-mismatch rejection, lease-expiry, SLAM weight-diversion, and
  package segregation rejection.

## Further reading (`.claude/rules/`)

- `ubiquitous-language.md` — full glossary, aggregates & invariants, domain
  events, use cases (the DDD core — read before any domain-layer change).
- `api-and-integration.md` — full REST endpoint table, the `orderRef`
  cross-service contract, CORS, events consumed/published (exact CloudEvents
  `type` strings).
- `analytics-data-product.md` — ADR-0012 analytics topic/DB/report design.
- `frontend-mfe.md` — `web/` fulfillment-mfe micro-frontend remote.
- `docs-and-api-drift.md` — how the Docusaurus docs site is built, why
  OpenAPI reference regeneration is a manual step, and the drift-check
  procedure for `apis/openapi.yaml` / `apis/asyncapi.yaml`.
- ADRs: `docs/docs/adr/0001` through `0032` (latest: 0032 CloudEvents 1.0
  as the mandatory event envelope, superseding 0027 and the envelope part of
  0004) — read the index at `docs/docs/adr/index.md` before assuming a
  decision is undocumented. ADR-0022 removed REST/MCP auth and supersedes
  ADR-0021.
