---
id: running-locally
title: Running locally (quickstart)
sidebar_label: Running locally
sidebar_position: 4
description: Prerequisites, make targets, running the service in memory or against Postgres and the platform's shared Kafka broker, seeding data, and a first curl session captured from a real run.
---

# Running locally (quickstart)

## Prerequisites

| Tool | Version | Needed for |
| --- | --- | --- |
| Go | `1.26.9` (from `go.mod`) | building and running every binary, unit and BDD tests |
| Docker | any recent | `docker compose` Postgres, and the testcontainers integration tests |
| golangci-lint | `v2.13.1` (pinned in CI and the `Makefile`) | `make lint`, `make check` |
| lefthook | `>= 1.5.0` | optional pre-commit / pre-push hooks (`lefthook install`) |
| Schemathesis (`st`) | `4.28.0` | `make contract` only |
| gremlins | `v0.6.0` | `make mutation-fast` / `make mutation` only |
| Node | 20 | this docs site (`docs/`) |
| warehouse-infra checkout | – | the process-path catalogue file, and the shared Kafka broker in its kind cluster |

## Make targets

`make help` prints them all. The ones you will use first:

| Target | What it does |
| --- | --- |
| `make build` | `go build ./...` |
| `make test` | `go test ./... -race` (unit, httptest, BDD; no database) |
| `make check` | `fmt-check vet build lint test` (the pre-push hook) |
| `make check-all` | `check` + `coverage arch-test bdd` |
| `make integration` | `go test -tags=integration ./... -race -count=1` (Docker only) |
| `make bdd`, `make contract`, `make arch-test`, `make mutation-fast`, `make vuln` | the individual sensors, see [Testing](../development/testing.md) |

There is no `make run`: start a binary with `go run ./cmd/<binary>` from the
repository root (the binaries read migrations from the relative
`migrations/` directory).

## Requirements: a real Postgres and a real Kafka

There is no in-memory mode ([ADR-0052](../adr/0052-postgres-and-kafka-are-the-only-runtime.md)).
`cmd/execution` needs `DATABASE_URL`, `KAFKA_BROKERS` and a consumer group per
Kafka consumer, and exits at boot naming the first missing variable.
It also needs a process-path catalogue: with the
default `PATH_CATALOGUE_SOURCE=file` it loads `PATH_CATALOGUE_FILE` (default
`/etc/fulfillment-execution/process-paths.yaml`) at boot and exits if it is
missing or invalid
([ADR-0017](../adr/0017-process-path-catalogue-as-configuration.md)). Point it
at the fleet catalogue in `warehouse-infra`:

```bash
export PATH_CATALOGUE_FILE=~/warehouse-systems/warehouse-infra/config/process-paths/sortable-fc.yaml
```

`GET /readyz` is 200 only while Postgres answers a ping and a Kafka broker
answers a metadata request; `/healthz` is liveness only. The next two sections
start the dependencies.

## With Postgres

The repo's `docker-compose.yml` defines **only Postgres 16** (user
`fulfillment`, database `fulfillment_execution`, port 5432; the password is
set in that file):

```bash
docker compose up -d postgres
export DATABASE_URL="postgres://fulfillment:***@localhost:5432/fulfillment_execution?sslmode=disable"
go run ./cmd/execution
```

`cmd/execution` (and `cmd/mcp`) apply every migration under `migrations/`
with golang-migrate before serving, so there is no separate migrate step.
Locally `MIGRATIONS_DATABASE_URL` can stay unset; it falls back to
`DATABASE_URL` ([ADR-0031](../adr/0031-migrations-direct-postgres-connection.md)).
To manage migrations yourself:

```bash
migrate -path migrations -database "$DATABASE_URL" up
```

With Postgres, `POST /tasks` requires an `Idempotency-Key` header
([ADR-0028](../adr/0028-idempotency-key-middleware.md)); a NUL (U+0000) in a
path, query or body is a client error (404 / 400), never a 500.

## With Kafka

There is **one Kafka broker for the whole platform**: the in-cluster broker of
the warehouse-infra kind cluster, reachable from your machine at
`localhost:9092` through its external access listener. This repo
deliberately has no Kafka in `docker-compose.yml` (the old compose broker is
retired), and `KAFKA_BROKERS` has no default: export it.

```bash
export KAFKA_BROKERS=localhost:9092
export WORK_RELEASED_CONSUMER_GROUP=fulfillment-execution
export PRODUCT_CLASSIFICATION_CONSUMER_GROUP=fulfillment-execution-product-classification
export TASK_INTERLEAVING_CONSUMER_GROUP=fulfillment-execution-interleaving
go run ./cmd/execution
```

Events go through `outbox_events` and the always-on in-process relay
([ADR-0020](../adr/0020-transactional-outbox.md)); `EVENT_PUBLISHER`, if set,
must be `kafka`. To read
the catalogue from process-path-management instead of the file, add
`PATH_CATALOGUE_SOURCE=kafka`. Every variable, for every binary, is
on [Configuration](../operations/configuration.md).

## The other binaries

```bash
# MCP server on :8090 (needs DATABASE_URL and KAFKA_BROKERS, same database as cmd/execution)
go run ./cmd/mcp

# Analytics needs a SEPARATE database, e.g. inside the compose Postgres:
#   psql "$DATABASE_URL" -c 'CREATE DATABASE fulfillment_analytics'
export ANALYTICS_DATABASE_URL="postgres://fulfillment:***@localhost:5432/fulfillment_analytics?sslmode=disable"
go run ./cmd/fulfillment-projector   # :8091, runs migrations/analytics, reads warehouse.fulfillment.analytics
go run ./cmd/fulfillment-reports     # :8092, GET /reports/throughput
```

`cmd/execution` and `cmd/mcp` share the same Postgres, so MCP's `complete_task`
rows are drained by `cmd/execution`'s relay. The analytics pair only sees events
once `cmd/execution` is running.

## Seeding data

There is no seed script and no fixture loader. Data enters the way it does in
production:

- **REST:** register stations (`POST /stations`) and create tasks
  (`POST /tasks`), as in the session below.
- **Kafka:** publish a `WorkReleased` CloudEvent to
  `warehouse.work-planning.events` (example on
  [Integration contracts](../ecosystem/integration-contracts.md#verifying-it-end-to-end)).
  A consumer group that has never committed starts at the **latest** offset,
  so publish after the service is up.

## A first session with curl

A station must exist before it can pull; that ordering is why `POST /stations`
exists ([ADR-0002](../adr/0002-pull-based-claimnext-dispatch.md)). Responses
below were captured from `go run ./cmd/execution` in memory (task ids are
time-based strings in this binary, so yours will differ):

```bash
# 1. Register a station with the "pick" capability
curl -sS -X POST localhost:8080/stations \
  -H 'Content-Type: application/json' \
  -d '{"stationId":"station-03","capabilities":["pick"]}'
# 201 {"id":"station-03","capabilities":["pick"],"occupied":false}

# 2. Put a Pick task in the pool
#    (with DATABASE_URL set, also pass -H 'Idempotency-Key: <unique>')
curl -sS -X POST localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{"type":"PICK","cpt":"2026-12-23T18:00:00Z","orderRef":"wu-8a1f","requiredCapabilities":["pick"]}'
# 201 {"id":"20261012T170412.355825000","type":"PICK","status":"PENDING","cpt":"2026-12-23T18:00:00Z","orderRef":"wu-8a1f","requiredCapabilities":["pick"],"fragile":false,"giftWrap":false}

# 3. Read model: pending Pick work
curl -sS localhost:8080/queues/PICK/depth
# 200 {"taskType":"PICK","depth":1}

# 4. PULL: the earliest-CPT task this station can do (5-minute lease)
curl -sS -X POST localhost:8080/stations/station-03/claim-next \
  -H 'Content-Type: application/json' -d '{"taskType":"PICK"}'
# 200 {"id":"20261012T170412.355825000","type":"PICK","status":"CLAIMED",...,"leaseStationId":"station-03","leaseExpiry":"2026-10-12T14:09:12.356726-03:00"}

# 5. Extend the lease, then complete (publishes TaskCompleted)
TASK_ID=20261012T170412.355825000
curl -sS -X POST localhost:8080/tasks/$TASK_ID/renew-lease \
  -H 'Content-Type: application/json' -d '{"stationId":"station-03"}'
# 204
curl -sS -X POST localhost:8080/tasks/$TASK_ID/complete \
  -H 'Content-Type: application/json' -d '{"stationId":"station-03"}'
# 204

# 6. Look the order up, and run the two sweeps the CronJobs call
curl -sS 'localhost:8080/tasks?orderRef=wu-8a1f'          # 200 [ {... "status":"COMPLETED" ...} ]
curl -sS localhost:8080/capacity/pick                      # 200 {"capability":"pick","installed":1}
curl -sS -X POST localhost:8080/tasks/expire-leases        # 200 {"freed":0}
curl -sS -X POST localhost:8080/tasks/sweep-cpt-misses     # 200 {"reported":0}
```

Errors are RFC 7807 problems. From the same run, a second `claim-next` with
nothing left to claim:

```json
{"type":"https://errors.fulfillment-execution.warehouse-systems.dev/no-claimable-task","title":"No claimable task for station capabilities","status":409,"detail":"usecases: no claimable task for station capabilities","instance":"/stations/station-03/claim-next"}
```

Completing from the wrong station gave `409 task-not-owner`, completing twice
gave `409 task-already-completed`, and `claim-next` on an unregistered
station gave `404 station-not-found`. With `EVENT_PUBLISHER=log` each event
shows up as a `domain event published` log line (`event_name` `TaskCreated`,
`TaskClaimed`, `TaskCompleted`). Every endpoint with full schemas is in the
[API Reference](../api-reference/index.md); every problem type is on
[Troubleshooting](../operations/troubleshooting.md).

## Tests

```bash
make test           # unit + httptest + BDD, no database
make integration    # Postgres and Kafka via testcontainers: needs Docker, no DATABASE_URL
make bdd            # go test ./... -run TestFeatures -v
make check-all      # what to run before pushing
```

The acceptance specs in `features/` are black-box: each scenario drives the
real chi router behind an `httptest` server with in-memory repositories, a
buffered publisher and a fixed `Clock`, which is what lets a scenario assert
lease expiry without sleeping ([ADR-0007](../adr/0007-godog-bdd-acceptance-tests.md)).
The full pyramid and the CI jobs are on [Testing](../development/testing.md).
