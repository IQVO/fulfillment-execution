# Fulfillment Execution

> **⚠️ Study project.** This repository is an educational exercise in
> Domain-Driven Design applied to warehouse management/execution systems. It
> follows real industry-standard patterns and terminology (WMS/WES/WCS,
> pull-based dispatch, CloudEvents, RFC 7807, hexagonal architecture) but is
> **not a production system** and is **not affiliated with, endorsed by, or
> representative of any real-world company**.

The task-lifecycle core bounded context for Pick, Pack, Rebin, and SLAM.
Downstream of Work Planning (which releases work); WCS/equipment is a
strategic downstream behind a deliberately unimplemented port (ADR-0015).
Dispatch is **pull, not push**: a station calls `claimNext(stationId,
capabilities)` and the system selects the best-fit pending task — the system
never names a station in advance. Claims are at-most-once and time-boxed by a
**lease**: an unconfirmed claim expires back to the pool rather than
vanishing.

See `CLAUDE.md` for the full architecture and ubiquitous language, and
`/Users/claudioed/docs/amazon-fulfillment-ddd.md` for the source domain model.

## Documentation

Full documentation site: **<https://iqvo.github.io/fulfillment-execution/>**

It covers the business context and ubiquitous language, the DDD model
(subdomain classification, aggregates and invariants, domain events, and the
ddd-crew artifact pack: core domain chart, bounded context canvas, aggregate
design canvas, domain message flow, EventStorming, UML class / ER / sequence
diagrams), an API
reference generated from `apis/openapi.yaml` plus a hand-written Events page
from `apis/asyncapi.yaml`, the ecosystem context map, and the Architecture
Decision Records. Source lives in `docs/` and deploys via
`.github/workflows/docs.yml`.

## Architecture

Hexagonal / Ports & Adapters, with a strict dependency rule: **domain depends
on nothing; application depends on domain; adapters depend on
application/domain.**

```
cmd/execution/               main.go — OLTP composition root
cmd/fulfillment-projector/   analytics WRITER: analytics topic -> analytical DB
cmd/fulfillment-reports/     analytics READ-ONLY READER: serves GET /reports/...
cmd/mcp/                     MCP server (Streamable HTTP; adds the two report tools)
internal/
  domain/
    task/                    Task aggregate (Pick|Pack|SLAM|Rebin lifecycle, lease, CPT-missed)
    station/                 Station aggregate (occupant, capabilities, locationCode)
    package/                 Package aggregate (pack -> sealed; segregation; SLAM weigh-check)
    consolidation/           OrderConsolidation aggregate (Rebin fan-in, ADR-0016)
    pathcatalog/             process-path catalogue model (prefix-match lookup, ADR-0017)
    shared/                  value objects: TaskId, StationId, PackageId, OrderRef, CPT, Capability, 13 events
  analytics/report/          analytical read model + store ports (ADR-0012)
  application/
    ports/                   OUT: TaskRepo, StationRepo, PackageRepo, OrderConsolidationRepo,
                             EventPublisher, UnitOfWork, Clock, ProcessedEvents, PathCatalogue,
                             ProductClassificationLookup, LocationRoleLookup, Metrics,
                             EquipmentCommandPort (empty WCS seam)
    usecases/                one struct per use case (17)
  adapters/
    inbound/http/            chi handlers, DTOs, error mapping (OLTP + reports)
    inbound/kafka/           WorkReleased consumer + analytics projector consumer
    inbound/mcp/             MCP tools (incl. get_fulfillment_throughput_report, get_on_time_to_cpt)
    outbound/postgres/       pgxpool repos + migrations + transactional outbox + relay
    outbound/analyticsstore/ analytical DB writer + read-only reader
    outbound/memory/         in-memory repos for tests/local
    outbound/kafka/          integration publisher + analytics publisher
    outbound/events/         log/buffered/multi publisher
    outbound/filecatalog/    process-path catalogue YAML loader
    outbound/kafkacatalog/   process-path catalogue Kafka replay (PATH_CATALOGUE_SOURCE=kafka)
    outbound/productclassification/  inventory-storage hazard lookup (opt-in)
    outbound/facilitylayout/ facility-layout location-role lookup (opt-in)
migrations/                  golang-migrate SQL files
migrations/analytics/        analytical schema migrations
```

`internal/domain/package` is a Go package named `pack` (not `package`, which
is a reserved keyword) — imported as `pack "github.com/claudioed/fulfillment-execution/internal/domain/package"`.

## Run

### In-memory (no database required)

Every mode needs a process-path catalogue first: `cmd/execution` loads
`PATH_CATALOGUE_FILE` (default `/etc/fulfillment-execution/process-paths.yaml`)
at boot and exits if it is missing or invalid
([ADR-0017](docs/docs/adr/0017-process-path-catalogue-as-configuration.md)). Point it at the fleet catalogue in `warehouse-infra`:

```sh
export PATH_CATALOGUE_FILE=~/warehouse-systems/warehouse-infra/config/process-paths/sortable-fc.yaml
go run ./cmd/execution
```

The service starts on `:8080` using in-memory adapters whenever
`DATABASE_URL` is unset.

### With Postgres

```sh
docker compose up -d postgres
export DATABASE_URL="postgres://fulfillment:fulfillment@localhost:5432/fulfillment_execution?sslmode=disable"
go run ./cmd/execution
```

On startup, `main.go` applies every migration under `migrations/` via
`golang-migrate` before serving traffic. To manage migrations independently
with the `golang-migrate` CLI instead:

```sh
migrate -path migrations -database "$DATABASE_URL" up
```

### Analytics data product (report)

The analytical **report** is a separate read side built from this service's own
domain events (see [ADR-0012](docs/docs/adr/0012-analytical-data-product.md)).
It runs as **two extra processes** against a **separate analytical database**,
fed by a dedicated Kafka topic `warehouse.fulfillment.analytics`:

```sh
# 0. OLTP service publishing to Kafka (integration + analytics topics)
export KAFKA_BROKERS=localhost:9092
EVENT_PUBLISHER=kafka DATABASE_URL="$DATABASE_URL" go run ./cmd/execution

# 1. projector (WRITER): consumes the analytics topic -> analytical DB.
#    Runs the analytical migrations (migrations/analytics) on start.
export ANALYTICS_DATABASE_URL="postgres://fx_analytics:***@localhost:5432/fulfillment_analytics?sslmode=disable"
go run ./cmd/fulfillment-projector           # /healthz on :8091 (ADMIN_ADDR)

# 2. reports (READ-ONLY READER): serves GET /reports/... from the analytical DB.
#    Point it at a read-only-role DSN in anything shared.
ANALYTICS_DATABASE_URL="$ANALYTICS_DATABASE_URL" HTTP_ADDR=:8092 go run ./cmd/fulfillment-reports

# query the report
curl "localhost:8092/reports/throughput?from=2026-01-01T00:00:00Z&to=2027-01-01T00:00:00Z"
curl "localhost:8092/reports/throughput/freshness"
```

The projector is the **only** writer of the analytical DB; the reports binary
connects read-only. The OLTP `cmd/execution` never opens the analytical DB.

### Running the MCP server in Kubernetes

`cmd/mcp` (the Model Context Protocol inbound adapter, [ADR-0008](docs/docs/adr/0008-mcp-inbound-adapter.md))
is built into the same image as `/app/mcp` and deployed by the Helm chart as a
separate Deployment + ClusterIP Service `<release>-mcp` on port **8090** when
`mcp.enabled=true`. It runs the same use cases over the same OLTP
database as the main deployment (it reuses `database.url` /
`database.existingSecret`); `complete_task` publishes `TaskCompleted` through
the same transactional outbox as REST (`EVENT_PUBLISHER`/`KAFKA_BROKERS` are
set on the pod; the relay runs only in the main deployment). It speaks MCP
Streamable HTTP at both `/` and `/mcp`,
and `GET /healthz` unauthenticated for the liveness/readiness probes. The
fleet's REST identity layer was removed (see the ADR below), so all MCP
tool calls are unauthenticated. When `analytics.enabled=true` the pod also
gets `REPORTS_BASE_URL` pointed at the chart's reports Service so the
`get_fulfillment_throughput_report` and `get_on_time_to_cpt` tools are registered (override with
`mcp.reportsBaseUrl`). Locally:

```sh
go run ./cmd/mcp        # :8090 (MCP_ADDR)
curl localhost:8090/healthz                   # {"status":"ok"} — no key needed
curl -X POST localhost:8090/mcp \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

In the `warehouse-infra` kind cluster the chart is enabled and keyed from
Terraform, and warehouse-ops-agent's `FULFILLMENT_MCP_ENDPOINT` points at
`http://fulfillment-execution-mcp.warehouse-systems.svc.cluster.local:8090/mcp`.

### Configuration

| Env var        | Default | Purpose                          |
|----------------|---------|-----------------------------------|
| `HTTP_ADDR`    | `:8080` | HTTP listen address               |
| `DATABASE_URL` | (unset) | Postgres DSN; unset selects memory adapters |
| `MIGRATIONS_DATABASE_URL` | `DATABASE_URL` | Direct (non-PgBouncer) DSN used only for the golang-migrate startup step, in `cmd/execution` and `cmd/mcp` — see [ADR-0031](docs/docs/adr/0031-migrations-direct-postgres-connection.md) |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka broker list, used by the consumers, the publishers and the outbox relay |
| `WORK_RELEASED_CONSUMER_GROUP` | `fulfillment-execution` | Consumer group of the `WorkReleased` consumer on `warehouse.work-planning.events` |
| `PATH_CATALOGUE_SOURCE` | `file` | `file` loads `PATH_CATALOGUE_FILE` at boot; `kafka` replays `warehouse.process-path-management.events` into memory and blocks readiness until the replay catches up — see [ADR-0017](docs/docs/adr/0017-process-path-catalogue-as-configuration.md) |
| `PATH_CATALOGUE_FILE` | `/etc/fulfillment-execution/process-paths.yaml` | Path to the declared process-path catalogue YAML (see `warehouse-infra`'s `config/process-paths/sortable-fc.yaml`). Loaded once at startup; a missing or invalid file is a fatal boot-time error — see [ADR-0017](docs/docs/adr/0017-process-path-catalogue-as-configuration.md) |
| `EVENT_PUBLISHER` | `log` | `log` publishes domain events to stdout only; `kafka` additionally publishes `TaskCompleted`, `TaskCPTMissed` and `PackageManifested` to `warehouse.fulfillment.events` AND fans every domain event to `warehouse.fulfillment.analytics` (feeds the report). When `DATABASE_URL` is also set, both topics are fed through the **transactional outbox** (`outbox_events`, committed in the use case's own transaction and drained by an in-process relay — see [ADR-0020](docs/docs/adr/0020-transactional-outbox.md)); without Postgres, events go straight to the broker |
| `OUTBOX_RELAY_INTERVAL` | `1s` | How long the outbox relay sleeps between passes when it found nothing to publish (Go duration, e.g. `500ms`). Only used with `EVENT_PUBLISHER=kafka` and `DATABASE_URL` set |
| `ANALYTICS_DATABASE_URL` | (unset) | Analytical DB DSN, read by `cmd/fulfillment-projector` (read-write) and `cmd/fulfillment-reports` (read-only role). MUST be a different database from `DATABASE_URL` |
| `ANALYTICS_MIGRATIONS_PATH` | `migrations/analytics` | Analytical golang-migrate migrations the projector runs on start |
| `ADMIN_ADDR` | `:8091` | `cmd/fulfillment-projector` admin/health listen address |
| `MCP_ADDR` | `:8090` | `cmd/mcp` listen address — MCP Streamable HTTP at `/` and `/mcp`, unauthenticated `GET /healthz` |
| `REPORTS_BASE_URL` | (unset) | `cmd/mcp` only: base URL of `cmd/fulfillment-reports`; when set, registers the `get_fulfillment_throughput_report` and `get_on_time_to_cpt` tools |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5184` | Comma-separated browser origins allowed by the CORS middleware on the OLTP API ([ADR-0013](docs/docs/adr/0013-fulfillment-mfe-console-adoption.md)) |
| `PRODUCT_CLASSIFICATION_MODE` | `permissive` | `permissive` (default, no-op, every scanned SKU treated as unclassified) or `http` — live per-scanned-SKU DOT hazard classification lookup from inventory-storage at seal time (ADR-0010) |
| `INVENTORY_STORAGE_BASE_URL` | (unset) | Base URL for inventory-storage's REST API; required when `PRODUCT_CLASSIFICATION_MODE=http` |
| `LOCATION_ROLE_MODE` | `permissive` | `permissive` (default, no-op, a supplied `locationCode` is recorded unchecked) or `http` — live registration-time lookup of a station's `locationCode` role from facility-layout, rejecting a KNOWN non-WorkCenter role (ADR-0024) |
| `FACILITY_LAYOUT_BASE_URL` | (unset) | Base URL for facility-layout's REST API; required when `LOCATION_ROLE_MODE=http` |
| `LOG_LEVEL`    | `info`  | `debug` \| `info` \| `warn` \| `error`, case-insensitive |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | OTel Collector's OTLP/gRPC address (see [Observability](#observability)) |
| `OTEL_SERVICE_NAME` | `fulfillment-execution` | `service.name` resource attribute |
| `SERVICE_VERSION` | `dev` | `service.version` resource attribute |
| `ENVIRONMENT`  | `local` | `deployment.environment.name` resource attribute |

## Observability

Traces and metrics are exported over **OTLP/gRPC** to an OpenTelemetry
Collector; the service does not expose a Prometheus scrape endpoint of its
own — the Collector handles Prometheus exposition for the whole fleet. A
Collector is expected at `OTEL_EXPORTER_OTLP_ENDPOINT` (default
`localhost:4317`); in the `warehouse-infra` kind cluster the Helm chart
points that at the in-cluster Collector Service
(`otel-collector.observability.svc.cluster.local:4317`).

**An unreachable Collector is never fatal.** The OTLP exporters dial lazily
and no blocking dial option is set, so with nothing listening the service
still starts and serves at full speed — telemetry is simply dropped.

### What gets exported

| Signal | What |
|--------|------|
| Traces | One server span per HTTP request (via `otelchi`), named after the **route pattern** (`POST /tasks/{id}/complete`) rather than the raw path; a child span per Postgres query/batch/copy/acquire (via `otelpgx`), carrying the parameterised SQL — query values are never recorded; `kafka.publish <topic>` / `kafka.consume <topic>` spans around the Kafka boundary |
| Metrics | `http.server.request.duration` (histogram, seconds, by route + method + status); `fulfillment.tasks.claimed` and `fulfillment.tasks.completed` counters attributed by `task.type` (PICK \| PACK \| SLAM \| REBIN \| DISPATCH \| ARRIVAL); pgxpool connection gauges; Go runtime metrics (goroutines, GC, memory) |
| Logs | Structured JSON on stdout. Any log emitted while a span is active also carries `trace_id` and `span_id`, so a log line links straight to its trace |

The two task counters are incremented inside the `ClaimNext` and
`CompleteTask` **use cases**, not in the HTTP handler, so they count the real
domain events — a rejected claim or a rejected completion does not count.

### Distributed tracing across services

Trace context crosses the Kafka boundary in the message headers (W3C
`traceparent`), injected on publish and extracted on consume. A `WorkReleased`
published by `wes-work-planning` and the `Task` created from it here are
therefore parts of a **single trace**, as is the `TaskCompleted` this service
publishes back onto `warehouse.fulfillment.events`.

### Seeing it locally

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317 LOG_LEVEL=info go run ./cmd/execution
```

Every request line then looks like:

```json
{"time":"...","level":"INFO","msg":"http request","method":"POST","path":"/tasks",
 "status":201,"trace_id":"8812c36621d214139a08949823716b93","span_id":"14ddf02dbd8913ba"}
```

## Local development / quality gate

Every sensor CI runs is available locally through the `Makefile`, so problems
get caught and self-corrected before they reach the pipeline:

```sh
make help          # list every target
make check         # FAST pre-commit loop: fmt-check, vet, build, lint, test
make check-all     # pre-push gate: check + coverage (90% gate), arch-test, bdd
make vuln          # govulncheck ./... — known CVEs in deps and the Go stdlib
make mutation-fast # blocking mutation subset (./internal/domain/task)
make integration   # needs Docker: Postgres/Kafka tests boot their own containers (testcontainers)
```

Git hooks are managed by [lefthook](https://github.com/evilmartians/lefthook)
(`lefthook.yml`): `pre-commit` runs `make fmt-check vet lint`, `pre-push` runs
`make check`. Activate them once per clone:

```sh
brew install lefthook          # or: go install github.com/evilmartians/lefthook@latest
lefthook install
```

## Tests

```sh
go build ./...
go vet ./...
go test ./...
go test -race ./...
gofmt -l .            # must print nothing

# Postgres integration tests: each test boots its own Postgres via
# testcontainers, so they need Docker but no DATABASE_URL / compose service
go test -tags integration ./internal/adapters/outbound/postgres/...
```

## BDD / Acceptance tests

Business-readable acceptance tests are written in Gherkin and executed with
[godog](https://github.com/cucumber/godog), the official Cucumber
implementation for Go. The feature files live under `features/`:

| Feature file | Covers |
| --- | --- |
| `features/claim_next.feature` | `claimNext` pull dispatch: earliest-CPT selection, capability mismatch, at-most-once claiming |
| `features/lease.feature` | Lease renewal before expiry, and lease expiry returning a Task to the pool |
| `features/complete_task.feature` | Completing a claimed Task, and rejecting a non-owner |
| `features/pack_slam.feature` | Sealing a Package, and the SLAM weigh-check applying a label vs diverting |
| `features/task_guards.feature` | Lifecycle guards: unregistered station, double-complete, non-owner / unclaimed renew, empty seal, double SLAM |
| `features/station_occupancy.feature` | Station check-in / check-out, one occupant at a time, no domain event published |
| `features/installed_capacity.feature` | `GET /capacity/{capability}` installed-capacity read model |
| `features/order_ref_lookup.feature` | `GET /tasks?orderRef=` lookup |
| `features/package_read_model.feature` | `GET /packages/{id}` and `GET /packages?orderRef=` (ADR-0033) |
| `features/cpt_missed_sweep.feature` | `POST /tasks/sweep-cpt-misses` raising `TaskCPTMissed` (ADR-0025) |

The step definitions live in `features_test.go` at the repo root. They are
black-box: each scenario spins up the real chi router (in-memory repositories,
buffered event publisher, fixed Clock) behind an `httptest` server and drives
it with real HTTP calls to the endpoints documented under [API](#api). Every
scenario gets a fresh server and fresh state.

Run them locally with:

```sh
go test ./... -run TestFeatures -v
```

CI runs the same command in the `bdd` job.

## API

All endpoints are unauthenticated (the fleet's REST identity layer was
removed — see the ADR below). All endpoints accept/return JSON. Every error response uses
[RFC 7807](https://www.rfc-editor.org/rfc/rfc7807) Problem Details
(`Content-Type: application/problem+json`) instead of a bespoke shape:

```json
{
  "type": "https://errors.fulfillment-execution.warehouse-systems.dev/task-not-found",
  "title": "Task not found",
  "status": 404,
  "detail": "usecases: task not found",
  "instance": "/tasks/does-not-exist/renew-lease"
}
```

`type` identifies the error category (does not need to resolve to a real
page), `title` is a fixed human summary for that category, `status`
duplicates the HTTP status code, `detail` is the specific error text for
this occurrence, and `instance` is the request path that produced it
(omitted for a validation error on a bare collection-create endpoint like
`POST /tasks`, which has no path segment identifying a specific resource).
See `apis/openapi.yaml`'s `components/schemas/Problem` for the full schema.

### Create a task (put work in the pool)

```sh
curl -sX POST localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{
        "type": "PICK",
        "cpt": "2026-01-01T18:00:00Z",
        "orderRef": "order-42",
        "requiredCapabilities": ["pick"],
        "fragile": false
      }'
```

`fragile` is optional (defaults to `false`); it is a packing hint normally
stamped by `wes-work-planning` at release time, not something a human caller
typically sets by hand. When the service runs against Postgres, `POST /tasks`
also requires an `Idempotency-Key` header (missing: `400`
`idempotency-key-required`; same key with a different body: `422`
`idempotency-key-reused`; same key and body: the stored response is replayed —
[ADR-0028](docs/docs/adr/0028-idempotency-key-middleware.md)); the in-memory
mode does not wire the middleware. `requiredCapabilities` may include `hazmat` — see
[Integration](#integration) below and `docs/docs/adr/0009-fragile-and-hazmat-handling-flags.md`
for why that needs no special handling beyond the value itself.

### Register a station

```sh
curl -sX POST localhost:8080/stations \
  -H 'Content-Type: application/json' \
  -d '{"stationId": "station-1", "capabilities": ["pick"]}'
```

Creates a station with the given capabilities, or updates its capability set
if `stationId` already exists (idempotent re-registration, e.g. recertifying
a station). Closes the pull-dispatch gap for HTTP-only smoke testing: without
this endpoint there was no way to create a station over HTTP, so `claimNext`
below always failed with "station not found" against a freshly-started server.

### claimNext — a station pulls the best-fit pending task

```sh
curl -sX POST localhost:8080/stations/station-1/claim-next \
  -H 'Content-Type: application/json' \
  -d '{"taskType": "PICK"}'
```

Returns the highest-priority (earliest CPT) pending task the station is
certified/equipped for, and leases it to that station. Returns `409` if the
pool has nothing the station can accept right now.

### Renew a lease

```sh
curl -sX POST localhost:8080/tasks/{taskId}/renew-lease \
  -H 'Content-Type: application/json' \
  -d '{"stationId": "station-1"}'
```

### Complete a task

```sh
curl -sX POST localhost:8080/tasks/{taskId}/complete \
  -H 'Content-Type: application/json' \
  -d '{"stationId": "station-1"}'
```

### Seal a package (Pack path)

```sh
curl -sX POST localhost:8080/tasks/{taskId}/seal-package \
  -H 'Content-Type: application/json' \
  -d '{"stationId": "station-1", "contents": ["sku-1", "sku-2"]}'
```

### Run SLAM (Scan, Label, Apply, Manifest)

```sh
curl -sX POST localhost:8080/packages/{packageId}/slam \
  -H 'Content-Type: application/json' \
  -d '{"actualWeight": 2.02, "expectedWeight": 2.0}'
```

Applies the shipping label if the actual weight is within tolerance of
expected; otherwise the package is diverted. Returns `204` in both cases.
Read the package back (below) to see which outcome occurred.

### Read a package (SLAM outcome)

```sh
curl -s localhost:8080/packages/{packageId}
```

Returns the same Package shape seal-package returns (`id`, `orderRef`,
`status`, `scannedContents`, `fragileHandling`, `giftWrapRequested`,
`sortLane`). `status` is `LABELED` when the carton goes to the truck by
its `sortLane` and `DIVERTED` when it goes to problem-solve. Unknown id:
`404` `package-not-found`. See `docs/docs/adr/0033-package-read-model.md`.

### Packages for an order

```sh
curl -s 'localhost:8080/packages?orderRef=order-42'
```

Returns an array of Packages for that order, ordered by id. The array is
empty when the order has none. A missing or empty `orderRef` returns `400`
`invalid-request`, the same as `GET /tasks?orderRef=`.

### Queue depth (read model)

```sh
curl -s localhost:8080/queues/PICK/depth
```

### Sweep expired leases (Clock-driven)

```sh
curl -sX POST localhost:8080/tasks/expire-leases
```

Nothing in the service calls this (or `POST /tasks/sweep-cpt-misses`) on a
timer. In Kubernetes, set `sweeps.enabled=true` on the Helm chart to run both
as `CronJob`s (default off; schedules via `sweeps.expireLeases.schedule` /
`sweeps.cptMisses.schedule`). `sweep-cpt-misses` re-publishes `TaskCPTMissed`
for every overdue task on every pass, so keep its schedule coarse.

### Health check

```sh
curl -s localhost:8080/healthz
curl -s localhost:8080/readyz     # 503 {"status":"not_ready"} once graceful shutdown starts (ADR-0029)
```

### Every route

The router (`internal/adapters/inbound/http/router.go`) and `apis/openapi.yaml`
are kept in a two-way 1:1 match (19 operations) by
`internal/adapters/inbound/http/openapi_routes_test.go`:

| Method | Path | Use case |
|---|---|---|
| `POST` | `/tasks` | `CreateTask` — requires an `Idempotency-Key` header when Postgres is configured (ADR-0028) |
| `GET` | `/tasks?orderRef=` | `GetTasksByOrderRef` |
| `POST` | `/stations` | `RegisterStation` |
| `POST` | `/stations/{stationId}/claim-next` | `ClaimNext` |
| `POST` | `/stations/{stationId}/check-in` | `CheckInStation` |
| `POST` | `/stations/{stationId}/check-out` | `CheckOutStation` |
| `POST` | `/tasks/{id}/renew-lease` | `RenewLease` |
| `POST` | `/tasks/{id}/complete` | `CompleteTask` |
| `POST` | `/tasks/{id}/seal-package` | `SealPackage` |
| `GET` | `/packages/{id}` | `GetPackage` |
| `GET` | `/packages?orderRef=` | `GetPackagesByOrderRef` |
| `POST` | `/packages/{id}/slam` | `RunSlam` |
| `GET` | `/queues/{taskType}/depth` | `GetQueueDepth` |
| `GET` | `/capacity/{capability}` | `GetInstalledCapacity` |
| `POST` | `/tasks/expire-leases` | `ExpireLeases` |
| `POST` | `/tasks/sweep-cpt-misses` | `SweepCPTMisses` |
| `POST` | `/rebin/arrivals` | `ArriveAtRebin` |
| `GET` | `/healthz` | liveness |
| `GET` | `/readyz` | readiness |

## Integration

This service **consumes** `WorkReleased` events published by `wes-work-planning`
and turns each one into a Task via the existing `CreateTask` use case — this
is the intended use of that use case, so the Kafka consumer
(`internal/adapters/inbound/kafka`) calls it directly rather than going
through a new one. It also **publishes** `TaskCompleted` back to Work
Planning to close the control loop (drum-buffer-rope feedback edge:
Execution -> Orchestration).

- **Consumed topic**: `warehouse.work-planning.events` (consumer group `WORK_RELEASED_CONSUMER_GROUP`, default `fulfillment-execution`)
- **Published topic**: `warehouse.fulfillment.events` (`TaskCompleted`, `TaskCPTMissed`, `PackageManifested`) plus the internal analytics topic `warehouse.fulfillment.analytics`
- **Optionally consumed**: `warehouse.process-path-management.events` when `PATH_CATALOGUE_SOURCE=kafka`
- **Broker**: `KAFKA_BROKERS` env var, default `localhost:9092`. This connects
  to the fleet's shared broker in the `warehouse-infra` kind cluster (host
  listener `localhost:9092`);
  this repo's own `docker-compose.yml` does not run Kafka.
- **Client library**: `github.com/segmentio/kafka-go`.

### Envelope

Every message is a **CloudEvents 1.0** event in structured content mode
([ADR-0032](docs/docs/adr/0032-cloudevents-mandatory-envelope.md)); the Kafka
message also carries the header `content-type: application/cloudevents+json; charset=UTF-8`.
There is no other envelope and no envelope toggle:

```json
{
  "specversion": "1.0",
  "id": "6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b",
  "source": "/warehouse/wes-work-planning",
  "type": "com.warehouse.wes.work-planning.workunit.WorkReleased",
  "subject": "wu-8a1f",
  "time": "2026-08-21T22:00:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:wes-work-planning:events:WorkReleased:v1",
  "data": {"path_id": "...", "work_unit_id": "...", "cpt": "RFC3339", "ref": "...", "fragile": false}
}
```

The consumer dispatches on the **full** `type`; any other type is ignored. A
message that is not a valid CloudEvents 1.0 event (including the retired flat
`event_id`/`event_type` shape) is dead-lettered to
`warehouse.work-planning.events.dlq`, never parsed. `data.fragile` is
optional — see the Mapping section below.

### Mapping

`path_id` is resolved through the process-path catalogue
([ADR-0017](docs/docs/adr/0017-process-path-catalogue-as-configuration.md)):
the longest declared `matchPrefix` that prefixes the id wins (so
`pick-zone-a` → `PICK`), and the matched path supplies both the task type and
its required capabilities. A `path_id` that matches no declared path is a
hard handling error — there is no default-to-Pick. The rest of the mapping:

| WorkReleased field     | Task field                                          |
|-------------------------|------------------------------------------------------|
| `data.path_id`          | task type (catalogue lookup; unknown = error)          |
| `data.work_unit_id`     | `orderRef` (`shared.OrderRef`)                        |
| `data.cpt`               | `cpt`                                                 |
| `data.fragile` (optional, default `false`) | `fragile` — a packing hint, see below |
| `data.gift_wrap` (optional, default `false`) | `giftWrap` — a packing hint ([ADR-0011](docs/docs/adr/0011-gift-wrap-handling-flag.md)) |
| `data.ref`               | decoded, not mapped                                   |
| matched path             | required capabilities (from the catalogue)             |

**`data.fragile` is optional**: it is sourced from
`inventory-storage`'s `ProductClassification` concept and stamped by
`wes-work-planning` at release time, but any already-documented producer that
predates this field simply omits it, and the consumer defaults to `false`
rather than rejecting the message. When present and `true`, it means the
order line was classified Fragile upstream; this service threads it straight
into the created `Task.Fragile` flag (no interpretation), and later derives
`Package.FragileHandling` from it in `SealPackage` when the Pack task's
contents are sealed. It does not affect claiming — a fragile task is claimed
by capability match exactly like any other. It also has nothing to do with
hazmat: hazmat is a `requiredCapabilities` value (`hazmat`), matched by the
existing generic capability-set mechanism in `Task.Claim` / `RegisterStation`
— no code change was needed there, only documentation (see
`docs/docs/adr/0009-fragile-and-hazmat-handling-flags.md`).

### Idempotency

Kafka delivery is at-least-once. Before calling `CreateTask`, the consumer
tries to record the event's CloudEvents `id` in a `processed_events` table
(Postgres) or an in-memory set (no `DATABASE_URL`); if the id is already
present, the message is skipped (and still acked/committed) instead of
creating a duplicate Task. See
`internal/adapters/inbound/kafka/consumer_test.go` —
`TestHandleMessage_DoubleDeliveryCreatesExactlyOneTask`.

### Smoke test

With the shared broker reachable at `localhost:9092` (the `warehouse-infra`
kind cluster) and this service running (`go run ./cmd/execution`, with
`PATH_CATALOGUE_FILE` set — see Configuration):

```sh
kafka-console-producer.sh \
  --broker-list localhost:9092 --topic warehouse.work-planning.events <<'EOF'
{"specversion":"1.0","id":"0b7c3e8e-1d2a-4f5b-9c6d-7e8f9a0b1c2d","source":"/warehouse/wes-work-planning","type":"com.warehouse.wes.work-planning.workunit.WorkReleased","subject":"wu-smoke-1","time":"2026-08-21T22:00:00Z","datacontenttype":"application/json","dataschema":"urn:warehouse:wes-work-planning:events:WorkReleased:v1","data":{"path_id":"pick-smoke","work_unit_id":"wu-smoke-1","cpt":"2026-08-21T23:00:00Z","ref":"release-smoke"}}
EOF

curl -s localhost:8080/queues/PICK/depth
```

`depth` should have increased by 1.

### Publishing TaskCompleted

When `EVENT_PUBLISHER=kafka`, the outbound adapter
(`internal/adapters/outbound/kafka`) publishes a `TaskCompleted` message to
`warehouse.fulfillment.events` every time the `CompleteTask` use case
succeeds. `CompleteTask` itself is unchanged — it still just calls
`Publisher.Publish` with the domain event it always raised; only which
`ports.EventPublisher` implementation is wired in at startup changes.

The domain event `TaskCompleted` carries only `TaskId`/`StationId`. Work
Planning's downstream `RecordCompletion` use case needs the original
`work_unit_id` (the Task's `OrderRef`, set from `WorkReleased.data.work_unit_id`
at creation time — see the consumer mapping above), so the publisher adapter
looks the Task back up via `TaskRepo` before publishing and enriches the
payload with it. This mirrors the same repo-lookup-enrichment pattern
inventory-storage's Kafka publisher uses for `ReservationRevoked`.

#### Envelope

A CloudEvents 1.0 event (ADR-0032), keyed and `subject`-ed by task id:

```json
{
  "specversion": "1.0",
  "id": "6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b",
  "source": "/warehouse/fulfillment-execution",
  "type": "com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
  "subject": "task-8a1f",
  "time": "2026-08-21T22:00:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:fulfillment-execution:events:TaskCompleted:v1",
  "data": {"task_id": "task-8a1f", "station_id": "station-03", "work_unit_id": "wu-8a1f",
           "associate_id": "worker-42", "duration_seconds": 245, "task_type": "PICK"}
}
```

Downstream: `wes-work-planning` calls its `RecordCompletion` use case with
`WorkUnitId = data.work_unit_id`.

#### Smoke test

With the shared broker reachable at `localhost:9092` (the `warehouse-infra`
kind cluster) and this service running with Kafka publishing enabled:

```sh
EVENT_PUBLISHER=kafka go run ./cmd/execution
```

In another terminal, start a consumer on the published topic, then drive a
task through the full lifecycle over HTTP:

```sh
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic warehouse.fulfillment.events --from-beginning &

curl -sX POST localhost:8080/tasks -H 'Content-Type: application/json' \
  -d '{"type":"PICK","cpt":"2026-01-01T18:00:00Z","orderRef":"order-smoke-1","requiredCapabilities":["pick"]}'

curl -sX POST localhost:8080/stations/station-smoke/claim-next -H 'Content-Type: application/json' \
  -d '{"taskType":"PICK"}'
# note the "id" field of the returned task as TASK_ID

curl -sX POST localhost:8080/tasks/TASK_ID/complete -H 'Content-Type: application/json' \
  -d '{"stationId":"station-smoke"}'
```

The consumer should print a `TaskCompleted` message whose `data.work_unit_id`
is `"order-smoke-1"`.

## Invariants covered by failing-path tests

- **At-most-once claim**: `internal/domain/task/task_test.go` —
  `TestClaim_RejectsSecondClaimWhileLeaseActive`; also exercised end-to-end in
  `internal/application/usecases/usecases_test.go` —
  `TestClaimNext_AtMostOnce_SecondStationCannotClaimSameTask`.
- **Capability-mismatch rejection**: `internal/domain/task/task_test.go` —
  `TestClaim_RejectsCapabilityMismatch`; `internal/domain/station/station_test.go` —
  `TestValidateAccept_RejectsCapabilityMismatch`.
- **Lease-expiry frees the task**: `internal/domain/task/task_test.go` —
  `TestClaim_ExpiredLeaseFreesTaskForNewClaim`; also
  `internal/application/usecases/usecases_test.go` —
  `TestClaimNext_ExpiredLeaseReturnsTaskToPool` and
  `TestExpireLeases_SweepsExpiredClaimsBackToPending`, both driven by a
  `FixedClock`.
- **SLAM weight-diversion**: `internal/domain/package/package_test.go` —
  `TestWeigh_DivertsOutsideTolerance`.
- **Package segregation rejection**: `internal/domain/package/package_test.go` —
  `TestScanItemWithClass_IncompatibleClassRejectedAndNotAppended`; also
  exercised through `SealPackage` in
  `internal/application/usecases/usecases_test.go`.

## Operator micro-frontend (`web/`)

`web/` is `fulfillment_mfe`, this context's Module Federation remote. It talks only to
this service's own REST API and is never part of `make check`.

**Standalone development** is unchanged:

```bash
cd web && npm install && npm run dev     # http://localhost:5184
```

**Deployed to the kind cluster**, it is built into a static bundle and served
by its own `nginx-unprivileged` pod:

```bash
cd web
docker build --build-context uikit=../../warehouse-ui-kit \
  -t warehouse/fulfillment-execution-frontend:local .
```

The cluster's localhost topology separates the two kinds of traffic onto two
independent entrypoints, and neither proxies to the other:

| URL | Served by | Carries |
|---|---|---|
| `http://localhost/mfes/fulfillment-execution/` | Nginx web gateway → this remote's nginx pod | HTML, JS, CSS, fonts, `remoteEntry.js` |
| `http://localhost:8000/api/fulfillment-execution/` | Kong | this service's REST API |

Kong never serves frontend assets, and the Nginx gateway never proxies an API.
Enable the workload with `frontend.enabled=true` in the Helm chart; the Service
is deliberately `ClusterIP` with no Ingress/HTTPRoute, because frontend path
routing belongs to the Nginx web gateway in `warehouse-infra`.

Because one image must work in more than one environment, the remote reads its
API origin at runtime from `window.__WAREHOUSE_CONFIG__.apiOrigin` (published
by the console shell) rather than baking a hostname in at build time. A
production build with no runtime config **fails loudly** instead of silently
falling back to a developer port; standalone `npm run dev` still uses
`http://localhost:8084`. See `web/src/config.ts`.

Chart invariants are asserted by:

```bash
python3 charts/fulfillment-execution/tests/test_service_selectors.py
python3 charts/fulfillment-execution/tests/test_sweeps_cronjob.py
```

which proves every Service selects exactly one Deployment — the OLTP Service
must never select the frontend, analytics or MCP pods — and that the opt-in
sweep CronJobs render only when `sweeps.enabled=true`.
