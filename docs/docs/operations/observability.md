---
id: observability
title: Observability
sidebar_label: Observability
sidebar_position: 3
description: Every OpenTelemetry metric instrument the four binaries emit (name, type, unit, attributes), span names, structured log fields and key log messages, the OTLP exporter settings, the Grafana dashboard in warehouse-infra, and suggested alerts.
---

# Observability

All four binaries share one telemetry setup,
`internal/observability/telemetry.go` (`observability.Setup`), following the
fleet metrics convention ([ADR-0019](../adr/0019-standard-metrics-convention.md)).

## How telemetry leaves the process

- **Push, not scrape.** Traces and metrics are exported over **OTLP/gRPC** to
  `OTEL_EXPORTER_OTLP_ENDPOINT` (default `localhost:4317`; a bare
  `host:port` is turned into `http://host:port`, and the connection is
  insecure). There is **no `/metrics` endpoint** on any binary. In the
  cluster the chart points it at
  `otel-collector.observability.svc.cluster.local:4317`, and the Collector
  re-exports to Prometheus (dots become underscores, counters gain `_total`).
- **Never fatal.** The exporters dial lazily. With no Collector the services
  start and serve normally; each failed export logs a WARN
  `opentelemetry error` line.
- **Resource attributes:** `service.name` (`OTEL_SERVICE_NAME`; defaults
  `fulfillment-execution`, `fulfillment-execution-mcp`,
  `fulfillment-projector`, `fulfillment-reports`), `service.version`
  (`SERVICE_VERSION`, default `dev`), `deployment.environment.name`
  (`ENVIRONMENT`, default `local`).
- **Propagation:** W3C `traceparent` plus baggage, over HTTP headers and over
  Kafka message headers (`internal/observability/kafkacarrier.go`).

## Metrics

### Service-defined instruments

| Instrument | Type | Unit | Attributes | Recorded by | Emitted from |
| --- | --- | --- | --- | --- | --- |
| `fulfillment.tasks.claimed` | Int64 counter | `{task}` | `task.type` (`PICK`, `PACK`, `SLAM`, `REBIN`) | `ClaimNext`, once per won claim | `cmd/execution` (`internal/observability/metrics.go`) |
| `fulfillment.tasks.completed` | Int64 counter | `{task}` | `task.type` | `CompleteTask`, once per completion | `cmd/execution` (REST) and `cmd/mcp` (`complete_task`) |
| `circuit_breaker.state` | Int64 gauge | `{state}` | `dependency` (`facility-layout`) | the facility-layout breaker's state changes: `0` closed, `1` half-open, `2` open | `cmd/execution`, only when `LOCATION_ROLE_MODE=http` (`internal/observability/circuit_breaker_metrics.go`) |

In Prometheus these appear as `fulfillment_tasks_claimed_total`,
`fulfillment_tasks_completed_total` and `circuit_breaker_state`. The gauge is
written only on a transition, so it has no series until the breaker first
changes state.

### Library instruments

| Instrument(s) | Type | Unit | Attributes | Source | Emitted from |
| --- | --- | --- | --- | --- | --- |
| `http.server.request.duration` | Float64 histogram (buckets 5ms … 10s) | `s` | `http.method`, `http.scheme`, `http.route` (the chi pattern). **No status-code attribute.** | `github.com/riandyrn/otelchi/metric` v0.12.3 | `cmd/execution`, `cmd/fulfillment-reports`, `cmd/mcp` routers. The projector's admin server is not instrumented. |
| `pgxpool.acquires`, `pgxpool.acquire_duration`, `pgxpool.acquired_connections`, `pgxpool.canceled_acquires`, `pgxpool.constructing_connections`, `pgxpool.empty_acquire`, `pgxpool.empty_acquire_wait_time`, `pgxpool.idle_connections`, `pgxpool.max_connections`, `pgxpool.max_idle_destroys`, `pgxpool.max_lifetime_destroys`, `pgxpool.new_connections`, `pgxpool.total_connections` | observable counters / up-down counters / gauge | – | – | `github.com/exaring/otelpgx` v0.12.0 `RecordStats` | `cmd/execution` (with `DATABASE_URL`), `cmd/fulfillment-projector`, `cmd/fulfillment-reports`. `cmd/mcp` does **not** register pool stats. |
| `go.memory.used`, `go.memory.limit`, `go.memory.allocated`, `go.memory.allocations`, `go.memory.gc.goal`, `go.goroutine.count`, `go.processor.limit`, `go.config.gogc` | runtime observables | various | – | `go.opentelemetry.io/contrib/instrumentation/runtime` v0.71.0 | all four binaries |

There is no metric for consumer lag, outbox backlog or DLQ volume. Use the
broker's consumer-group lag, the SQL in the [Runbook](./runbook.md#routine-procedures)
and the log lines below.

## Traces

| Span name | Kind | Where | Notes |
| --- | --- | --- | --- |
| `<METHOD> <route pattern>`, for example `POST /tasks/{id}/complete` | server | every chi router (otelchi with `WithChiRoutes` and the method in the name) | Low-cardinality: the route pattern, not the raw path. |
| `kafka.consume warehouse.work-planning.events` | consumer | `WorkReleased` consumer | Parent is the producer's span from the message headers, so a release in wes-work-planning and the task created here share one trace. The span covers all in-process retries. Attributes `messaging.system=kafka`, `messaging.destination.name`, `messaging.operation.name=process`. |
| `kafka.consume warehouse.product-master.events` | consumer | `ProductClassified` consumer | Same attributes. |
| `kafka.consume warehouse.fulfillment.analytics` | consumer | projector | Same attributes. |
| `kafka.publish warehouse.fulfillment.events`, `kafka.publish warehouse.fulfillment.analytics` | producer | publishers in **direct** mode only (`EVENT_PUBLISHER=kafka` without `DATABASE_URL`) | With the outbox, the trace context of the request's span is written into the stored headers at encode time, so the downstream consumer span still joins the request's trace, but no publish span is recorded. |
| `mcp.tool <tool name>`, for example `mcp.tool complete_task` | internal | every MCP tool call | Attribute `mcp.tool.name`; status `Error` with the message when the tool fails. |
| SQL spans (`query SELECT ...` and friends) | client | every pgx pool (`otelpgx`) | Statements are recorded in parameterised form; parameter values are not recorded. |

The outbox relay, the process-path catalogue consumer and the sweeps'
CronJob pods create no spans of their own (the sweep requests are traced
server-side as `POST /tasks/expire-leases` and `POST /tasks/sweep-cpt-misses`).

**Trace continuity across the outbox (gap fix D27).** Both encoders inject
the W3C `traceparent`/`tracestate` of the writing use case's context into the
message headers at encode time (`observability.InjectKafkaTrace`); the
`OutboxPublisher` stores them in `outbox_events.headers` (JSONB, migration
`0009`) in the same transaction; the relay sends the stored headers unchanged,
even though its own pass runs without the request's context; and every
consumer that starts a span extracts first (`observability.ExtractKafkaTrace`).
A row written without an active span has no `traceparent` and relays with
exactly its stored headers. Pinned by the integration tests
`TestOutboxTrace_TraceparentCrossesTheOutboxToKafka` and
`TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged`
(`internal/adapters/outbound/postgres/outbox_trace_integration_test.go`).

## Logs

JSON lines on stdout (`log/slog` JSON handler), level from `LOG_LEVEL`.
Every record has `time`, `level`, `msg`; a record logged with a context that
carries an active span also has **`trace_id`** and **`span_id`**
(`internal/observability/slogotel.go`), which is how a log line links to its
trace.

The request logger writes one `http request` line per request on
`cmd/execution` and `cmd/fulfillment-reports`, with `method`, `path`
(CR/LF stripped), `status`, `duration_ms`, `bytes` and `request_id`; status
`5xx` logs at `ERROR`, everything else at `INFO`. Example from a local run:

```json
{"time":"2026-10-12T14:04:12.358153-03:00","level":"INFO","msg":"http request","method":"POST","path":"/tasks/20261012T170412.355825000/complete","status":204,"duration_ms":0,"bytes":0,"request_id":"claudioeds-MacBook-Pro.local/YYHwzrf60N-000010","trace_id":"cb33b6f3f5af6bc4e8d63e56024c25f7","span_id":"81fb421a6c10c93e"}
```

Messages worth searching for or alerting on:

| `msg` | Level | Meaning |
| --- | --- | --- |
| `kafka message handling failed after retries` | ERROR | A `WorkReleased` failed 3 attempts and went to the DLQ (`topic`, `attempts`, `error`). |
| `kafka message is not a valid CloudEvent; dead-lettering` | WARN | Poison message on `warehouse.work-planning.events`. |
| `failed to publish message to dead-letter topic` | ERROR | The DLQ write itself failed; the message is now only in this log. |
| `kafka read failed; retrying` / `kafka fetch failed; retrying` | WARN | Broker unreachable; the consumer backs off (up to 30s) and keeps trying. |
| `applying ProductClassified failed; retrying the same message` | ERROR | Transient failure; that partition is blocked until it succeeds. |
| `skipping ProductClassified with an invalid payload` | WARN | Contract violation from product-master; committed past. |
| `analytics message handling failed` | ERROR | Projector could not apply an event; it moves on. |
| `outbox relay pass failed` | ERROR | A send or SQL failure in the relay; the row keeps `published_at` NULL and is retried. |
| `outbox relay: send failed, stopping this pass at the failed row` | WARN | One failed send below the attempt cap (`outbox_id`, `topic`, `attempts`). |
| `outbox relay: row reached the attempt cap and is excluded from future claims; re-arm it with attempts = 0 after fixing the cause` | ERROR | A row failed `OUTBOX_MAX_ATTEMPTS` sends and is now skipped ([ADR-0042](../adr/0042-outbox-relay-attempt-cap.md)); fields `outbox_id`, `topic`, `event_type`, `key`, `attempts`, `max_attempts`, `error`. Alert on it. |
| `outbox relay did not stop before the shutdown deadline` / `kafka consumer did not stop before the shutdown deadline` | WARN | Graceful shutdown hit its 10s bound. |
| `retrying` (with `op`, `attempt`, `in`) | WARN | A boot-time dial (migrations, ping, catalogue) is being retried. |
| `domain event published` (with `event_name`, `occurred_at`) | INFO | The log publisher (`EVENT_PUBLISHER=log`): the event went nowhere else. |
| `opentelemetry error` | WARN | An export to the Collector failed. |

## Dashboard

warehouse-infra ships a Grafana dashboard for this context:
`terraform/dashboards/contexts/fulfillment-execution.json`, titled
**"Warehouse — Fulfillment Execution"** (uid
`warehouse-fulfillment-execution`). Its rows:

1. **API Gateway (Kong):** request rate, 5xx ratio and p95 latency for the
   `httproute.warehouse-systems.fulfillment-execution.*` Kong service.
2. **Service HTTP RED:** request rate by `http_route`, 5xx rate and p95 of
   `http_server_request_duration_seconds` for the `fulfillment-(execution|projector|reports).*`
   service names (this regex also matches `fulfillment-execution-mcp`).
3. **Business metrics:** `fulfillment_tasks_claimed_total` and
   `fulfillment_tasks_completed_total` rates by `task_type`.
4. **Go runtime:** goroutines and memory used.
5. **Logs (Loki):** live stream, volume by level, and ERROR/WARN lines for
   `{app="fulfillment-execution"}` (Istio containers excluded).

The row-2 error panel filters on `http_response_status_code` /
`http_status_code`, which otelchi v0.12.3 does not emit, so it stays empty.
Use the Kong 5xx panel or the `http request` log lines with `status>=500`
for service errors.

## Suggested alerts

None of these exist yet; they are built only from signals listed above.

| Alert | Signal | Why |
| --- | --- | --- |
| Completions stalled | `sum(rate(fulfillment_tasks_completed_total[15m])) == 0` while `sum(rate(fulfillment_tasks_claimed_total[15m])) > 0` | Stations claim but nothing completes: leases will expire and churn the pool. |
| Queue backing up | `GET /queues/{taskType}/depth` growing across checks (MCP `get_queue_status` gives the same number) | Too few stations pulling. |
| Facility-layout breaker open | `max(circuit_breaker_state{dependency="facility-layout"}) == 2` for 5m | Station registrations are being accepted unchecked. |
| Work landing in the DLQ | Loki: count of `kafka message handling failed after retries` or `kafka message is not a valid CloudEvent` > 0 in 10m | Released work did not become a task. |
| Outbox not draining | Loki: `outbox relay pass failed` repeating; SQL: oldest `outbox_events.created_at` with `published_at IS NULL` older than a few minutes | Downstream contexts stop seeing completions, CPT misses and manifests. |
| Classification partition blocked | Loki: `applying ProductClassified failed; retrying the same message` repeating | The local hazmat copy goes stale. |
| Analytics stale | `GET /reports/throughput/freshness` returns `lagSeconds` above the 30s SLA for 10m | Projector down or lagging. |
| Latency | `histogram_quantile(0.95, sum by (le, http_route) (rate(http_server_request_duration_seconds_bucket{service_name="fulfillment-execution"}[5m]))) > 0.5` | `claim-next` and `complete` are on the station's critical path. |
