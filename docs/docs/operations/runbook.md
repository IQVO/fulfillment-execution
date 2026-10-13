---
id: runbook
title: Runbook
sidebar_label: Runbook
sidebar_position: 2
description: How fulfillment-execution is deployed and operated — Helm chart, Deployments and ports, probes, migrations, Kafka topics and consumer groups, the outbox relay, the DLQ, the sweep CronJobs, scaling, every HTTP route outside the OpenAPI spec, and routine procedures.
---

# Runbook

Everything here is read from `charts/fulfillment-execution/`, the four
composition roots under `cmd/`, and the adapters they wire. Variable-level
detail is on [Configuration](./configuration.md); failure diagnosis is on
[Troubleshooting](./troubleshooting.md).

## Deployment shape

One Helm chart, **`fulfillment-execution`** (`charts/fulfillment-execution`),
one container image (`ghcr.io/iqvo/fulfillment-execution`, built from the
`Dockerfile`; it contains the four binaries under `/app` plus
`/app/migrations`). On every merge to `main` the CI `release` job publishes
the image tag and the chart as `oci://ghcr.io/iqvo/fulfillment-execution`.

| Workload | Kubernetes name | Binary | Enabled by | Container port | Service | Probes |
| --- | --- | --- | --- | --- | --- | --- |
| api | `<release>` (component `api`) | `cmd/execution` (image entrypoint) | always | `8080` (`HTTP_ADDR=:8080`) | ClusterIP `80` → `8080` | startup `/healthz` (2s × 30), liveness `/healthz` (every 10s), readiness **`/readyz`** (every 5s) |
| mcp | `<release>-mcp` | `/app/mcp` | `mcp.enabled` (default `false`) | `8090` (`MCP_ADDR`) | ClusterIP `8090` → `8090` | startup, liveness and readiness all on `/healthz` |
| analytics projector | `<release>-projector` | `/app/fulfillment-projector` | `analytics.enabled` (default `false`) | `8091` (admin, `ADMIN_ADDR`) | none | startup, liveness and readiness on `/healthz` (port `admin`) |
| analytics reports | `<release>-reports` | `/app/fulfillment-reports` | `analytics.enabled` | `8080` (`HTTP_ADDR=:8080` from the chart) | ClusterIP `80` → `http` | startup, liveness and readiness on `/healthz` |
| frontend (MFE remote) | `<release>-frontend` | nginx serving `web/` | `frontend.enabled` (default `false`) | `8080` | ClusterIP `80` | see `frontend-deployment.yaml` |
| expire-leases sweep | CronJob `<release>-expire-leases` | busybox `wget` from the service image | `sweeps.enabled` + `sweeps.expireLeases.enabled` (default on) | – | – | – |
| CPT-miss sweep | CronJob `<release>-sweep-cpt-misses` | same | `sweeps.enabled` + `sweeps.cptMisses.enabled` (default on) | – | – | – |

The api Deployment mounts the chart ConfigMap `<release>` with `envFrom`
(`HTTP_ADDR`, `EVENT_PUBLISHER`, `OUTBOX_MAX_ATTEMPTS`, `KAFKA_BROKERS` and,
with `otel.enabled`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`,
`ENVIRONMENT`); its explicit `env` entries win for the same key. No other
workload mounts it (gap fix D14; `charts/fulfillment-execution/tests/render_test.sh`
asserts both).

In the platform, Kong (`:8000`) routes `/api/fulfillment-execution` to the api
Service (warehouse-infra `terraform/locals.tf`, `path =
"${var.api_path_prefix}/fulfillment-execution"`); the frontend is reached
through the Nginx web gateway (`:80`), never through Kong. REST and MCP are
unauthenticated ([ADR-0022](../adr/0022-remove-rest-mcp-auth.md)).

### What readiness means here

`cmd/execution` starts its HTTP listener **only after** every boot step has
succeeded, in this order (`cmd/execution/main.go`):

1. Validate `PRODUCT_CLASSIFICATION_MODE` (bad value: exit).
2. Telemetry setup (never fatal).
3. Load the process-path catalogue: the file (`file` mode), or start the
   replay of `warehouse.process-path-management.events` in the background
   (`kafka` mode).
4. With `DATABASE_URL`: run migrations, open the pool, ping it — each under
   boot retry (5 attempts, 1s doubling delay, 15s of waiting in total).
5. Build publishers and the outbox relay, then listen.
6. `kafka` mode only: wait until every catalogue partition reaches the offset
   captured at startup (`WaitReadyTimeout` 60s, then exit). Meanwhile
   `/healthz` answers 200 and `/readyz` answers 503
   ([ADR-0044](../adr/0044-listener-before-catalogue-replay.md)).
7. Start the Kafka consumers.

So `/healthz` and `/readyz` start answering once storage is ready; in `kafka`
mode `/readyz` turns ready only after step 6.
After that `/readyz` returns `200 {"status":"ready"}` until SIGTERM, when it
flips to `503 {"status":"not_ready"}` **before** the server stops accepting
connections, so the pod leaves the Service endpoints during the drain
([ADR-0029](../adr/0029-resilience-circuit-breakers-retry-dlq-shutdown.md)).
Neither probe checks Postgres or Kafka after boot. The startup probe allows
60s (2s × 30) for step 4; the catalogue replay (step 6) no longer counts against it.

Shutdown order: readiness off, `http.Server.Shutdown`, stop the outbox relay
and wait for its in-flight pass, cancel the consumers and wait for in-flight
messages, then close readers, writers and the pool. All waits share one 10s
deadline.

## HTTP routes outside `apis/openapi.yaml`

`apis/openapi.yaml` documents every `cmd/execution` route (including
`/healthz` and `/readyz`); `apis/openapi-reports.yaml` documents every
`cmd/fulfillment-reports` route ([ADR-0043](../adr/0043-reports-api-openapi-contract.md));
the generated [API Reference](../api-reference/index.md) covers both. The
other binaries serve these routes; the `cmd/fulfillment-reports` rows are
the ones `apis/openapi-reports.yaml` specifies, the `cmd/mcp` and
`cmd/fulfillment-projector` rows are in no spec:

| Binary | Method and path | Response | Notes |
| --- | --- | --- | --- |
| `cmd/fulfillment-reports` | `GET /reports/throughput` | `200 {"rows":[...]}` | Query: `from`, `to` (required, RFC 3339), `taskType`, `stationId` (optional exact match), `granularity` (optional, only `hour`). Missing or malformed `from`/`to`, or another granularity: `400` problem `invalid-report-query`. Store error: `500` problem `report-store-error`. Row fields: `taskType`, `stationId`, `hourBucket`, `completions`, `avgClaimToCompleteSeconds`, `leaseExpiries`, `weighCheckDiverts`, `packagesManifested`, `packagesOnTimeToCPT`, `packagesLateToCPT`. |
| `cmd/fulfillment-reports` | `GET /reports/throughput/freshness` | `200 {"lagSeconds":<float>}` | Now minus the newest `occurred_at` in `analytics_processed_events`. |
| `cmd/fulfillment-reports` | `GET /healthz` | `200 {"status":"ok"}` | Liveness only. |
| `cmd/mcp` | `POST`/`GET`/`DELETE` `/mcp` and `/` | MCP Streamable HTTP | Both mounts serve the same handler; `/mcp` matches warehouse-ops-agent's `FULFILLMENT_EXECUTION_MCP_ENDPOINT` convention. Tools: [MCP tools](../mcp/tools.md). |
| `cmd/mcp` | `GET /healthz` | `200 {"status":"ok"}` | Does not touch Postgres or the reports service. |
| `cmd/fulfillment-projector` | `GET /healthz` (admin port) | `200 {"status":"ok"}` | The projector's only endpoint. |

The full report contract is on [Throughput report](../analytics/throughput-report.md).

## Databases and migrations

| Database | Owner process | Migrations | Tables |
| --- | --- | --- | --- |
| OLTP (`DATABASE_URL`, `fulfillment_execution` in the kind cluster) | `cmd/execution` and `cmd/mcp` (both run migrations at boot) | `migrations/0001`–`0018` | `tasks`, `stations`, `packages`, `order_consolidations`, `processed_events`, `outbox_events`, `idempotency_keys`, `product_classification_copy` (`domain_events` from 0001 was dropped by 0018, [ADR-0045](../adr/0045-drop-legacy-domain-events-table.md)) |
| Analytical (`ANALYTICS_DATABASE_URL`) | `cmd/fulfillment-projector` (only writer, runs migrations); `cmd/fulfillment-reports` reads | `migrations/analytics/0001`–`0002` | `analytics_processed_events`, `analytics_consumed_events`, `analytics_pending_claims`, `throughput_rollup` |

Migrations are golang-migrate `up` runs at process start (`internal/adapters/outbound/postgres/migrate.go`),
never a separate Job. For the OLTP database they go over
`MIGRATIONS_DATABASE_URL` when it is set: golang-migrate serialises
concurrent runs with a session-scoped `pg_advisory_lock`, which PgBouncer's
transaction pooling breaks, so the chart passes a direct DSN from the same
secret (key `MIGRATIONS_DATABASE_URL`, `optional: true`)
([ADR-0031](../adr/0031-migrations-direct-postgres-connection.md)). Without
the key the binary falls back to `DATABASE_URL`.

A failed migration leaves golang-migrate's version table dirty and every
later boot of `cmd/execution` and `cmd/mcp` fails at step 4. Fix the SQL
problem, then force the last good version with the `migrate` CLI against the
direct DSN (`migrate -path migrations -database "$MIGRATIONS_DATABASE_URL" force <version>`)
and restart.

## Kafka

One broker for the whole platform (in-cluster; host access
`localhost:9092`). Every message is CloudEvents 1.0, structured mode
([ADR-0032](../adr/0032-cloudevents-mandatory-envelope.md)).

| Topic | Direction | Process | Consumer group | Start offset for a new group | Failure behaviour |
| --- | --- | --- | --- | --- | --- |
| `warehouse.work-planning.events` | consume `WorkReleased` | `cmd/execution` | `WORK_RELEASED_CONSUMER_GROUP` (default `fulfillment-execution`) | **latest** | 3 in-process attempts (100ms → 2s jittered), then dead-letter; non-CloudEvents messages are dead-lettered at once. Offsets are committed only after the message is applied or dead-lettered; a shutdown mid-handle commits nothing and dead-letters nothing, so the message is redelivered and deduplicated ([ADR-0046](../adr/0046-workreleased-consumer-commits-after-settle.md)). |
| `warehouse.work-planning.events.dlq` | produce | `cmd/execution` (only with `EVENT_PUBLISHER=kafka`) | – | – | Original key/value/headers plus `x-dlq-error`, `x-dlq-original-topic`, `x-dlq-original-partition`, `x-dlq-original-offset`, `x-dlq-failed-at`. A failed DLQ write is logged and the loop goes on. |
| `warehouse.product-master.events` | consume `ProductClassified` | `cmd/execution` (only with `PRODUCT_CLASSIFICATION_MODE=kafka`) | `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` | **earliest** | Explicit commit after the transaction. Transient failure: retried on the same message forever (200ms → 5s), blocking that partition. Invalid payload or non-CloudEvent: WARN and commit past. No DLQ. |
| `warehouse.process-path-management.events` | consume `ProcessPathCreated`/`Updated`/`Deactivated` | `cmd/execution` (only with `PATH_CATALOGUE_SOURCE=kafka`) | `fulfillment-execution-process-path-catalogue-<host>-<pid>-<nanos>` (new per process) | earliest | Full replay on every start into memory. Async commits every 1s. |
| `warehouse.fulfillment.events` | produce `TaskCompleted`, `TaskCPTMissed`, `PackageManifested`, `TransferPicked`, `TransferDispatched`, `TransferArrived` | `cmd/execution` relay (rows written by `cmd/execution` and `cmd/mcp`) | – | – | Keyed by task id or package id (`kafka.Hash` balancer, `RequiredAcks=all`, 10ms batch). |
| `warehouse.fulfillment.analytics` | produce every analytics event | same relay | – | – | Same writer settings. |
| `warehouse.fulfillment.analytics` | consume | `cmd/fulfillment-projector` | `fulfillment-analytics` | **earliest** | Deduped on the CloudEvents id (`analytics_consumed_events`). Handling errors are logged and skipped; no DLQ. |

All three long-running consumers ride out broker outages with a
never-give-up backoff (250ms → 30s) and log `kafka read failed; retrying`
(`kafka fetch failed; retrying` for the classification consumer).

## Transactional outbox and relay

With `DATABASE_URL` and `EVENT_PUBLISHER=kafka`, every state-changing use
case writes its encoded messages to `outbox_events` in the same transaction
as the aggregate ([ADR-0020](../adr/0020-transactional-outbox.md)). The relay
runs **only** in `cmd/execution` (`cmd/mcp` inserts rows but never drains):

- One pass = `SELECT ... FROM outbox_events WHERE published_at IS NULL ORDER
  BY id LIMIT 100 FOR UPDATE SKIP LOCKED`, then send rows one at a time.
- A full batch is followed immediately by another pass; otherwise it sleeps
  `OUTBOX_RELAY_INTERVAL` (1s).
- The first send failure stops the pass at that row (per-key order is kept),
  increments `attempts`, stores `last_error`, logs a WARN and commits what was
  sent.
- **Attempt cap** ([ADR-0042](../adr/0042-outbox-relay-attempt-cap.md)): once
  a row has failed `OUTBOX_MAX_ATTEMPTS` sends (default `20`, chart value
  `config.outboxMaxAttempts`, `0` = unlimited) the claim query adds
  `AND attempts < $max`, so the row is excluded and later rows, **including
  rows with the same key**, overtake it. The failure that reaches the cap is
  logged once at ERROR (`outbox relay: row reached the attempt cap ...`, with
  `outbox_id`, `topic`, `event_type`, `key`, `attempts`). The row stays in
  `outbox_events` until re-armed (below).
- Delivery is at least once; the CloudEvents `id` is stored in the row, so a
  re-send carries the same id and consumers dedupe on it.
- `SKIP LOCKED` lets two api replicas (or a rolling deploy) relay
  concurrently without claiming the same row.

There is no housekeeping job: published `outbox_events` rows and
`idempotency_keys` rows are kept forever (the `created_at` index on
`idempotency_keys` exists for a cleanup job that has not been built).

## Sweeps (scheduled)

The service has no in-process timers. Two CronJobs POST to the api Service
with busybox `wget` ([ADR-0037](../adr/0037-sweeps-scheduled-by-cronjob-default-on.md)):

| CronJob | Schedule | Calls | Effect |
| --- | --- | --- | --- |
| `<release>-expire-leases` | `* * * * *` | `POST /tasks/expire-leases` | Returns `{"freed":N}`; every freed task goes back to Pending and raises `LeaseExpired`. |
| `<release>-sweep-cpt-misses` | `*/5 * * * *` | `POST /tasks/sweep-cpt-misses` | Returns `{"reported":N}`; publishes one `TaskCPTMissed` per open task at or past its CPT, again on every pass until it completes. Kafka volume = overdue tasks × passes. |

Both: `concurrencyPolicy: Forbid`, `startingDeadlineSeconds: 60`,
`activeDeadlineSeconds: 120`, request timeout 30s, `backoffLimit: 0`. A
non-2xx response fails the Job. Set `sweeps.enabled=false` only when another
scheduler calls the endpoints.

## Scaling

| Workload | HPA (all `enabled: false` by default) | Pool per process | Notes |
| --- | --- | --- | --- |
| api | `autoscaling.api`: 1–4 replicas, 70% CPU | `MaxConns` 10, `statement_timeout` 5s | Stable consumer group, so replicas share partitions. At 4 replicas: 40 client connections; PgBouncer (`pool_size` 12 per database) multiplexes them. |
| projector | `autoscaling.projector`: 1–2, 70% CPU | `MaxConns` 5, 10s | Stable group `fulfillment-analytics`. |
| reports | `autoscaling.reports`: 1–3, 70% CPU | `MaxConns` 5, 15s, read-only | Stateless. |
| mcp | **no HPA**; keep `mcp.replicaCount: 1` | `MaxConns` 10, 5s | MCP sessions (`Mcp-Session-Id`) live in process memory and the Service has no session affinity. |
| frontend | `autoscaling.frontend`: 1–3, 70% CPU | – | Static assets. |

When an HPA is enabled, the chart omits `replicas:` from that Deployment.
See [ADR-0030](../adr/0030-horizontal-autoscaling-and-pgxpool-tuning.md).

## Routine procedures

**Run a sweep by hand.**
`curl -sS -X POST http://<api>/tasks/expire-leases` and
`curl -sS -X POST http://<api>/tasks/sweep-cpt-misses`. Neither needs a body
or an `Idempotency-Key`.

**Replay a dead-lettered `WorkReleased`.** Read the message from
`warehouse.work-planning.events.dlq` and check `x-dlq-error` (typical causes:
a `path_id` the catalogue does not know, a `transfer_ref` with an unknown
`work_kind`, a non-CloudEvents payload). Fix the cause (for example define
the path in process-path-management), then produce the **original value
unchanged** back onto `warehouse.work-planning.events`. A failed apply leaves
no `processed_events` row (the claim rolls back with the task), so the replay
applies; a message that had in fact been applied is deduplicated by its
CloudEvents `id`.

**Find and re-send outbox rows.** Stuck rows:
`SELECT id, topic, event_type, attempts, last_error FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 20;`
The relay retries them every interval by itself, up to `OUTBOX_MAX_ATTEMPTS`.
Rows the cap excluded (`attempts >= 20` with the default) are no longer
retried: find the cause in `last_error` (a message over the broker's size
limit, a missing topic), fix it, then re-arm the row with
`UPDATE outbox_events SET attempts = 0 WHERE id = ...;` and the next pass
publishes it (after the rows that overtook it). To publish an
already-published row again, set its `published_at` back to `NULL`; the same
CloudEvents `id` goes out and consumers dedupe on it.

**Rebuild the analytics read model.** Scale the projector to 0, point it at
an empty analytical database (or truncate `throughput_rollup`,
`analytics_pending_claims`, `analytics_processed_events` and
`analytics_consumed_events`), reset group `fulfillment-analytics` on
`warehouse.fulfillment.analytics` to the earliest offset with
`kafka-consumer-groups.sh --reset-offsets --to-earliest --execute`, and
scale back up. Kafka retention bounds how far back this goes.

**Change the process-path catalogue.** In `kafka` mode, change the path in
process-path-management; running pods apply the event live. In `file` mode,
change `pathCatalogue.content` (or the external ConfigMap); the pod template
carries a `checksum/path-catalogue` annotation, so a Helm upgrade rolls the
pods.

**Rotate database credentials.** `DATABASE_URL`, `MIGRATIONS_DATABASE_URL`
and `ANALYTICS_DATABASE_URL` come from Secrets as environment variables and
are read once at start: update the Secret, then
`kubectl rollout restart deployment/<release> deployment/<release>-mcp` (and
the projector and reports Deployments for the analytics DSN).

**Rebuild the product-classification copy.** A new value of
`PRODUCT_CLASSIFICATION_CONSUMER_GROUP` starts at the earliest offset, and
the upsert is version-guarded, so replaying is safe. Events whose CloudEvents
id is already in `processed_events` are skipped, so this only refills rows
that were lost together with their `processed_events` entries (for example a
fresh database).
