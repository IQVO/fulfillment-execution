---
id: configuration
title: Configuration reference
sidebar_label: Configuration
sidebar_position: 1
description: Every environment variable each of the four binaries reads (cmd/execution, cmd/mcp, cmd/fulfillment-projector, cmd/fulfillment-reports), with its default, whether it is required, what it does, and the file that reads it.
---

# Configuration reference

The repository builds four binaries from one Go module (and one container
image, see the `Dockerfile`). Each binary is configured **only** through
environment variables, read in its composition root under `cmd/` plus the
shared telemetry setup in `internal/observability/telemetry.go`. There is no
config file other than the process-path catalogue YAML.

Conventions on this page:

- **Default** is the value the binary uses when the variable is unset or
  empty. Every `getenv(key, fallback)` helper in this repo treats an empty
  string the same as unset.
- **Required** means the binary refuses to boot without it.
- The Helm chart (`charts/fulfillment-execution`) sets most of these for
  you; the last section maps chart values to variables.

## `cmd/execution` (the OLTP service)

The main deployable: REST API, the `WorkReleased` and `ProductClassified`
Kafka consumers, the optional process-path catalogue consumer, and the
transactional-outbox relay.

| Variable | Default | Required? | Meaning | Read in |
| --- | --- | --- | --- | --- |
| `HTTP_ADDR` | `:8080` | no | Listen address of the REST API (`/healthz`, `/readyz` and every route in `apis/openapi.yaml`). | `cmd/execution/main.go` |
| `LOG_LEVEL` | `info` | no | `debug`, `info`, `warn`/`warning`, `error` (case-insensitive). Anything else means `info`. Logs are JSON on stdout. | `cmd/execution/main.go` |
| `DATABASE_URL` | *(unset)* | no | Postgres DSN of the OLTP database. **Unset selects the in-memory adapters** for every repository, the processed-events store and the outbox (there is then no `UnitOfWork`, no outbox and no `Idempotency-Key` middleware). Set, the binary runs the migrations, opens a pgxpool (`MaxConns` 10, `statement_timeout` 5s) and pings it. | `cmd/execution/main.go` |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | Direct (non-PgBouncer, session-mode) DSN used **only** for the golang-migrate step at boot. The runtime pool always uses `DATABASE_URL`. See [ADR-0031](../adr/0031-migrations-direct-postgres-connection.md). | `cmd/execution/main.go` |
| `KAFKA_BROKERS` | `localhost:9092` | no (but always used) | Comma-separated broker list. The `WorkReleased` consumer starts **unconditionally**, so this must point at a reachable broker in any real environment. Also used by the publishers, the outbox relay, the DLQ writer, the classification consumer and the catalogue consumer. | `cmd/execution/main.go` |
| `EVENT_PUBLISHER` | `log` | no | `kafka` publishes domain events (through `outbox_events` when `DATABASE_URL` is set, straight to the broker otherwise) **and** wires the `WorkReleased` dead-letter writer. Any other value selects the log publisher: events are only logged and no DLQ exists. | `cmd/execution/main.go`, `internal/composition/publisher.go` |
| `OUTBOX_RELAY_INTERVAL` | `1s` | no | Go duration the outbox relay sleeps after a pass that found fewer than 100 rows. A malformed or non-positive value silently falls back to `1s`. Only used when `EVENT_PUBLISHER=kafka` and `DATABASE_URL` is set. | `cmd/execution/main.go` (`durationEnv`) |
| `OUTBOX_MAX_ATTEMPTS` | `20` | no | Outbox relay attempt cap ([ADR-0042](../adr/0042-outbox-relay-attempt-cap.md)): a row that has failed this many sends is excluded from the relay's claims, logged once at ERROR (`outbox_id`, `topic`, `event_type`, `key`) and left in `outbox_events` for inspection; later rows, including rows with the same key, overtake it. `0` = unlimited (a poison row then blocks the relay forever). A malformed or negative value falls back to `20` with a WARN. Re-arm a row with `UPDATE outbox_events SET attempts = 0 WHERE id = ...` (see the [runbook](./runbook.md)). Chart value `config.outboxMaxAttempts`. | `cmd/execution/main.go` (`outboxMaxAttempts`) |
| `WORK_RELEASED_CONSUMER_GROUP` | `fulfillment-execution` | no | Consumer group of the `warehouse.work-planning.events` consumer. A **brand-new** group starts at the **latest** offset (`NewConsumerWithGroup`), so events published before its first join are not read. | `cmd/execution/main.go`, `internal/adapters/inbound/kafka/consumer.go` |
| `PATH_CATALOGUE_SOURCE` | `file` | no | `kafka` replays `warehouse.process-path-management.events` into memory under a per-process consumer group and blocks boot until the replay reaches the startup watermark (60s timeout, then exit). Any other value means `file`. See [ADR-0017](../adr/0017-process-path-catalogue-as-configuration.md). | `cmd/execution/main.go` |
| `PATH_CATALOGUE_FILE` | `/etc/fulfillment-execution/process-paths.yaml` | **yes in `file` mode** | Process-path catalogue YAML. A missing or invalid file is a fatal boot error. Ignored in `kafka` mode. | `cmd/execution/main.go`, `internal/adapters/outbound/filecatalog/loader.go` |
| `PRODUCT_CLASSIFICATION_MODE` | `permissive` | no | `permissive`: `SealPackage` treats every SKU as unclassified (no hazmat segregation). `kafka`: keep a local copy of product-master's `ProductClassified` events (Postgres table `product_classification_copy`, or in memory without `DATABASE_URL`) and read it. `http` was removed and **fails the boot**; any other value also fails the boot. Case-insensitive. See [ADR-0039](../adr/0039-product-classification-local-copy.md). | `cmd/execution/classification.go` |
| `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` | *(unset)* | **yes when mode is `kafka`** | Stable consumer group for `warehouse.product-master.events`. A new group starts at the **earliest** offset so the copy sees the whole history. The kind cluster uses `fulfillment-execution-product-classification`. | `cmd/execution/classification.go` |
| `LOCATION_ROLE_MODE` | `permissive` | no | `http` (case-insensitive) makes `POST /stations` check a `locationCode` against facility-layout's location role (must be `WorkCenter`), through a retry + circuit-breaker client. Anything else accepts every `locationCode` unchecked. See [ADR-0024](../adr/0024-station-location-code-and-workcenter-role-check.md). | `cmd/execution/main.go` |
| `FACILITY_LAYOUT_BASE_URL` | *(unset)* | only meaningful with `LOCATION_ROLE_MODE=http` | Base URL of facility-layout's REST API (`GET /locations/{locationCode}` is called on it). Not validated at boot: a wrong or empty value makes every lookup fail, and `RegisterStation` then accepts the station unchecked (fail-open). | `cmd/execution/main.go`, `internal/adapters/outbound/facilitylayout/client.go` |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5184` | no | Comma-separated browser origins the CORS middleware allows (methods GET/POST/PUT/DELETE, headers `Content-Type`, `Authorization`, `Idempotency-Key`). The defaults are the console shell and this repo's micro-frontend dev servers. | `internal/adapters/inbound/http/router.go` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC collector address for traces and metrics. A bare `host:port` is turned into `http://host:port`. An unreachable collector is not fatal: telemetry is dropped and a WARN `opentelemetry error` is logged per failed export. | `internal/observability/telemetry.go` |
| `OTEL_SERVICE_NAME` | `fulfillment-execution` | no | `service.name` resource attribute, and the server name used by the HTTP span and `http.server.request.duration` middleware. | `internal/observability/telemetry.go` |
| `SERVICE_VERSION` | `dev` | no | `service.version` resource attribute (there is no build-time version). | `internal/observability/telemetry.go` |
| `ENVIRONMENT` | `local` | no | `deployment.environment.name` resource attribute. | `internal/observability/telemetry.go` |

Not configurable, but worth knowing: migrations are read from the relative
directory `migrations` (the repo root under `go run`, `/app/migrations` in the
image); the lease is 5 minutes (`usecases.DefaultLeaseDuration`); the outbox
relay claims at most 100 rows per pass; graceful shutdown is bounded by 10s;
boot-time dials (migrations, DB ping, the catalogue broker dial) retry 5
times with waits of 1, 2, 4 and 8 s, i.e. 15 s of backoff plus the attempts'
own duration (`internal/adapters/outbound/bootretry`; the code comment said
"~31s" until gap fix D22).

## `cmd/mcp` (the MCP server)

A second deployable over the **same** OLTP database. It runs migrations too,
but never starts a Kafka consumer, never reads the process-path catalogue and
never runs the outbox relay (only `cmd/execution` drains `outbox_events`).

| Variable | Default | Required? | Meaning | Read in |
| --- | --- | --- | --- | --- |
| `MCP_ADDR` | `:8090` | no | Listen address. MCP Streamable HTTP is served at `/` and `/mcp`, plus `GET /healthz`. | `cmd/mcp/main.go` |
| `LOG_LEVEL` | `info` | no | Same levels as `cmd/execution`: `debug`, `info`, `warn`/`warning`, `error`, case-insensitive (it was case-sensitive here until gap fix D22). | `cmd/mcp/main.go` |
| `DATABASE_URL` | *(unset)* | no | OLTP Postgres DSN. Unset gives this process its **own** in-memory stores, which are not shared with a separately running `cmd/execution`. | `cmd/mcp/main.go` |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | Same split as `cmd/execution` ([ADR-0031](../adr/0031-migrations-direct-postgres-connection.md)). | `cmd/mcp/main.go` |
| `EVENT_PUBLISHER` | `log` | no | With `kafka` and `DATABASE_URL`, `complete_task` writes `outbox_events` rows in its transaction for `cmd/execution`'s relay to publish. With `kafka` and no database it publishes straight to the broker. | `cmd/mcp/main.go` |
| `KAFKA_BROKERS` | `localhost:9092` | no | Only dialled in the `EVENT_PUBLISHER=kafka` + no-database case. | `cmd/mcp/main.go` |
| `REPORTS_BASE_URL` | *(unset)* | no | Base URL of `cmd/fulfillment-reports`. When set, the read-only tools `get_fulfillment_throughput_report` and `get_on_time_to_cpt` are registered; unset, they are absent from `tools/list`. | `cmd/mcp/main.go` |
| `OTEL_SERVICE_NAME` | `fulfillment-execution-mcp` | no | Resource `service.name` and the HTTP middleware's server name. | `cmd/mcp/main.go` |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `SERVICE_VERSION`, `ENVIRONMENT` | as above | no | Shared telemetry settings. | `internal/observability/telemetry.go` |

## `cmd/fulfillment-projector` (analytics writer)

Consumes `warehouse.fulfillment.analytics` (group `fulfillment-analytics`,
earliest offset for a new group) and writes the analytical database. It is the
only writer and owns the analytics migrations
([ADR-0012](../adr/0012-analytical-data-product.md)).

| Variable | Default | Required? | Meaning | Read in |
| --- | --- | --- | --- | --- |
| `ADMIN_ADDR` | `:8091` | no | Admin listener; serves only `GET /healthz` (`{"status":"ok"}`). | `cmd/fulfillment-projector/main.go` |
| `ANALYTICS_DATABASE_URL` | *(unset)* | **yes** | Read-write DSN of the analytical database (must differ from the OLTP one). Boot fails with `ANALYTICS_DATABASE_URL is required`. Pool: `MaxConns` 5, `statement_timeout` 10s. | `cmd/fulfillment-projector/main.go` |
| `ANALYTICS_MIGRATIONS_PATH` | `migrations/analytics` | no | Directory of the analytical golang-migrate files, run at boot against `ANALYTICS_DATABASE_URL`. | `cmd/fulfillment-projector/main.go` |
| `KAFKA_BROKERS` | `localhost:9092` | no | Broker list for the analytics consumer. | `cmd/fulfillment-projector/main.go` |
| `LOG_LEVEL` | `info` | no | Case-insensitive, as in `cmd/execution`. | `cmd/fulfillment-projector/main.go` |
| `OTEL_SERVICE_NAME` | `fulfillment-projector` | no | Resource `service.name`. | `cmd/fulfillment-projector/main.go` |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `SERVICE_VERSION`, `ENVIRONMENT` | as above | no | Shared telemetry settings. | `internal/observability/telemetry.go` |

## `cmd/fulfillment-reports` (analytics reader)

Serves `GET /reports/throughput` and `GET /reports/throughput/freshness` over
a read-only pool (`default_transaction_read_only=on`, `MaxConns` 5,
`statement_timeout` 15s). See [Throughput report](../analytics/throughput-report.md).

| Variable | Default | Required? | Meaning | Read in |
| --- | --- | --- | --- | --- |
| `HTTP_ADDR` | `:8092` | no | Listen address. The chart overrides it to `:8080`. | `cmd/fulfillment-reports/main.go` |
| `ANALYTICS_DATABASE_URL` | *(unset)* | **yes** | DSN of the analytical database; should be a read-only role. The chart feeds it from the secret key `ANALYTICS_READER_DATABASE_URL`. | `cmd/fulfillment-reports/main.go` |
| `LOG_LEVEL` | `info` | no | Case-insensitive. | `cmd/fulfillment-reports/main.go` |
| `OTEL_SERVICE_NAME` | `fulfillment-reports` | no | Resource `service.name`. The HTTP middleware reads the same variable but falls back to `fulfillment-execution` when it is unset, so set it explicitly outside the chart. | `cmd/fulfillment-reports/main.go`, `internal/observability/telemetry.go` |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `SERVICE_VERSION`, `ENVIRONMENT` | as above | no | Shared telemetry settings. | `internal/observability/telemetry.go` |

## Helm values that set these variables

From `charts/fulfillment-execution/values.yaml` and the Deployment templates.
The api Deployment also mounts the chart ConfigMap (`templates/configmap.yaml`:
`HTTP_ADDR`, `EVENT_PUBLISHER`, `OUTBOX_MAX_ATTEMPTS`, `KAFKA_BROKERS`, and the
`OTEL_*`/`ENVIRONMENT` keys when `otel.enabled`) with `envFrom`; an explicit
`env` entry wins over the ConfigMap for the same key. Before gap fix D14 the
ConfigMap was rendered but mounted by nothing. The mcp, projector, reports
and frontend Deployments do not mount it.

| Helm value | Sets | On which Deployment |
| --- | --- | --- |
| `config.httpAddr` (`:8080`) | `HTTP_ADDR` | api (`cmd/execution`) |
| `config.eventPublisher` (`log`) | `EVENT_PUBLISHER` | api; mcp unless `mcp.eventPublisher` is set |
| `config.outboxMaxAttempts` (`20`) | `OUTBOX_MAX_ATTEMPTS` | api |
| `kafka.brokers` (`kafka:9092`) | `KAFKA_BROKERS` | api, mcp, projector |
| `database.url` / `database.existingSecret` + `existingSecretKey` | `DATABASE_URL` | api, mcp |
| `database.migrationsExistingSecretKey` (`MIGRATIONS_DATABASE_URL`, `optional: true`) | `MIGRATIONS_DATABASE_URL` | api, mcp |
| `otel.enabled`, `otel.endpoint`, `otel.serviceName` | `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `SERVICE_VERSION` (image tag), `ENVIRONMENT` (`environment`) | api; the other three get fixed service names `fulfillment-execution-mcp`, `fulfillment-projector`, `fulfillment-reports` |
| `pathCatalogue.enabled`, `pathCatalogue.mountPath` | `PATH_CATALOGUE_FILE` (`<mountPath>/process-paths.yaml`) and the ConfigMap mount | api |
| `extraEnv` | anything else, e.g. `PRODUCT_CLASSIFICATION_MODE`, `PATH_CATALOGUE_SOURCE`, `LOCATION_ROLE_MODE` | api |
| `mcp.httpAddr`, `mcp.logLevel`, `mcp.eventPublisher`, `mcp.reportsBaseUrl`, `mcp.extraEnv` | `MCP_ADDR`, `LOG_LEVEL`, `EVENT_PUBLISHER`, `REPORTS_BASE_URL` (derived from the reports Service when `analytics.enabled` and the value is empty) | mcp |
| `analytics.projector.adminAddr`, `analytics.migrationsPath` | `ADMIN_ADDR`, `ANALYTICS_MIGRATIONS_PATH` | projector |
| `analytics.database.*` | `ANALYTICS_DATABASE_URL` (projector: key `ANALYTICS_DATABASE_URL`; reports: key `ANALYTICS_READER_DATABASE_URL`) | projector, reports |
| `analytics.reports.httpAddr` (`:8080`) | `HTTP_ADDR` | reports |

`CORS_ALLOWED_ORIGINS`, `LOG_LEVEL` (api), `OUTBOX_RELAY_INTERVAL`,
`WORK_RELEASED_CONSUMER_GROUP` and `FACILITY_LAYOUT_BASE_URL` have no
dedicated chart value: set them through `extraEnv`. In the warehouse-infra
kind cluster, `PRODUCT_CLASSIFICATION_MODE=kafka` and
`PRODUCT_CLASSIFICATION_CONSUMER_GROUP=fulfillment-execution-product-classification`
come from `terraform/locals.tf` (`sync_edge_env`); `LOCATION_ROLE_MODE` is not
set there, so the facility-layout check runs permissive in that cluster.

## Related pages

- [Runbook](./runbook.md): deployment, migrations, topics and routine procedures.
- [Observability](./observability.md): what the telemetry variables turn on.
- [Running locally](../overview/running-locally.md): the minimal set for a laptop run.
