---
id: running-locally
title: Running locally
sidebar_label: Running locally
sidebar_position: 4
description: Run the service in-memory in one command, or with Postgres and the shared Kafka broker, and drive the full pull-dispatch flow with curl.
---

# Running locally

## In-memory — no infrastructure needed

Every mode needs a process-path catalogue first: `cmd/execution` loads
`PATH_CATALOGUE_FILE` (default `/etc/fulfillment-execution/process-paths.yaml`)
at boot and exits if it is missing or invalid
([ADR-0017](../adr/0017-process-path-catalogue-as-configuration.md)). Point it at the fleet catalogue in `warehouse-infra`:

```bash
export PATH_CATALOGUE_FILE=~/warehouse-systems/warehouse-infra/config/process-paths/sortable-fc.yaml
go run ./cmd/execution
```

The service listens on `:8080` and selects in-memory adapters whenever
`DATABASE_URL` is unset. This is the fastest way to exercise the whole
pull-dispatch flow.

## With Postgres

```bash
docker compose up -d postgres
export DATABASE_URL="postgres://fulfillment:fulfillment@localhost:5432/fulfillment_execution?sslmode=disable"
go run ./cmd/execution
```

`main.go` applies every migration under `migrations/` via `golang-migrate`
before it serves traffic, so there is no separate migrate step. To manage
migrations yourself instead:

```bash
migrate -path migrations -database "$DATABASE_URL" up
```

## Configuration

| Env var | Default | Purpose |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | HTTP listen address |
| `DATABASE_URL` | *(unset)* | Postgres DSN; unset selects the in-memory adapters |
| `MIGRATIONS_DATABASE_URL` | `DATABASE_URL` | Direct (non-PgBouncer) DSN used only for the golang-migrate startup step ([ADR-0031](../adr/0031-migrations-direct-postgres-connection.md)) |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list, used by the consumers, the publishers and the outbox relay |
| `EVENT_PUBLISHER` | `log` | `log` writes domain events to stdout; `kafka` publishes `TaskCompleted`, `TaskCPTMissed` and `PackageManifested` to `warehouse.fulfillment.events` and every analytics event to `warehouse.fulfillment.analytics` — through the transactional outbox when `DATABASE_URL` is set ([ADR-0020](../adr/0020-transactional-outbox.md)) |
| `OUTBOX_RELAY_INTERVAL` | `1s` | Outbox relay idle poll interval (only with `EVENT_PUBLISHER=kafka` and Postgres) |
| `WORK_RELEASED_CONSUMER_GROUP` | `fulfillment-execution` | Consumer group of the `WorkReleased` consumer |
| `PATH_CATALOGUE_SOURCE` | `file` | `file` loads `PATH_CATALOGUE_FILE`; `kafka` replays the catalogue from `warehouse.process-path-management.events` ([ADR-0017](../adr/0017-process-path-catalogue-as-configuration.md)) |
| `PATH_CATALOGUE_FILE` | `/etc/fulfillment-execution/process-paths.yaml` | Process-path catalogue YAML. **Required for `go run`**: a missing file is a fatal boot error. Point it at `warehouse-infra`'s `config/process-paths/sortable-fc.yaml` |
| `PRODUCT_CLASSIFICATION_MODE` | `permissive` | `http` enables the inventory-storage hazard lookup (needs `INVENTORY_STORAGE_BASE_URL`) |
| `LOCATION_ROLE_MODE` | `permissive` | `http` enables the facility-layout WorkCenter role check (needs `FACILITY_LAYOUT_BASE_URL`) |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5184` | Browser origins allowed by the CORS middleware |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |

The other binaries add their own: `cmd/mcp` reads `MCP_ADDR` (`:8090`) and
`REPORTS_BASE_URL`; `cmd/fulfillment-projector` reads
`ANALYTICS_DATABASE_URL`, `ANALYTICS_MIGRATIONS_PATH` and `ADMIN_ADDR`
(`:8091`); `cmd/fulfillment-reports` reads `ANALYTICS_DATABASE_URL` and
`HTTP_ADDR` (`:8092`). Every binary honours the OpenTelemetry variables
(`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `SERVICE_VERSION`,
`ENVIRONMENT`).

A shared Kafka broker for the whole platform runs in the `warehouse-infra`
kind cluster (host listener `localhost:9092`) — this repo deliberately does
**not** define its own broker.

## Walking the pull-dispatch flow with curl

A station must exist before it can pull. This ordering is the reason
`POST /stations` exists at all — see
[ADR-0002](../adr/0002-pull-based-claimnext-dispatch.md).

```bash
# 1. Register a station with the "pick" capability
curl -sS -X POST localhost:8080/stations \
  -H 'Content-Type: application/json' \
  -d '{"stationId":"station-03","capabilities":["pick"]}'

# 2. Put a Pick task in the pool
#    (with DATABASE_URL set, also pass -H 'Idempotency-Key: <unique>' — ADR-0028)
curl -sS -X POST localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"type":"PICK","cpt":"2026-08-23T18:00:00Z","orderRef":"wu-8a1f","requiredCapabilities":["pick"]}'

# 3. PULL — the system selects the earliest-CPT task this station can do
curl -sS -X POST localhost:8080/stations/station-03/claim-next \
  -H 'Content-Type: application/json' \
  -d '{"taskType":"PICK"}'

# 4. Extend the lease while the work is still in progress
curl -sS -X POST localhost:8080/tasks/$TASK_ID/renew-lease \
  -H 'Content-Type: application/json' \
  -d '{"stationId":"station-03"}'

# 5. Complete it (publishes TaskCompleted)
curl -sS -X POST localhost:8080/tasks/$TASK_ID/complete \
  -H 'Content-Type: application/json' \
  -d '{"stationId":"station-03"}'

# Read model: how much Pick work is still pending?
curl -sS localhost:8080/queues/PICK/depth

# Sweep any leases that lapsed without renewal or completion
curl -sS -X POST localhost:8080/tasks/expire-leases
```

Every endpoint, with full request/response schemas and error shapes, is in the
[API Reference](../api-reference/index.md).

## Tests

```bash
go build ./...
go vet ./...
go test ./...
go test -race ./...
gofmt -l .            # must print nothing

# Postgres integration tests (build-tagged): each test boots its own Postgres
# via testcontainers, so they need Docker but no DATABASE_URL / compose service
go test -tags integration ./internal/adapters/outbound/postgres/...

# Gherkin acceptance specs, executed by godog
go test ./... -run TestFeatures -v
```

The acceptance specs live in `features/` and are black-box: each scenario
spins up the real chi router behind an `httptest` server with in-memory
repositories, a buffered publisher and a fixed `Clock`, then drives it over
real HTTP. That fixed clock is what lets a scenario assert lease expiry
without sleeping — see
[ADR-0007](../adr/0007-godog-bdd-acceptance-tests.md).
