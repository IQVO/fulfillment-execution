# Analytics Data Product (ADR-0012)

Additive read side built from this service's OWN domain events. The OLTP
domain/application layers are NOT modified and must NOT import the analytics
store (arch-test enforces this).

- `internal/analytics/report/` depends on nothing.
- Events are fanned to a SEPARATE topic `warehouse.fulfillment.analytics` by
  a dedicated outbound adapter (`outbound/kafka/analytics_publisher.go`). The
  integration topic `warehouse.fulfillment.events` and its publisher are
  untouched by this. Task-scoped events are enriched with `task_type` via a
  TaskRepo lookup (domain events stay thin).
- SEPARATE analytical Postgres (`ANALYTICS_DATABASE_URL`), own migrations
  (`migrations/analytics/`), read-only role for the reader.
- Three processes:
  - `cmd/execution` (OLTP)
  - `cmd/fulfillment-projector` — the ONLY writer of the analytical DB;
    consumes the analytics topic from FirstOffset, idempotent on the
    CloudEvents `id` (every analytics message is a CloudEvents 1.0 event,
    `dataschema` `urn:warehouse:fulfillment-execution:analytics:<Event>:v1`,
    ADR-0032)
  - `cmd/fulfillment-reports` — read-only reader
- Report: Throughput & Lease-Health, keyed task-type × station × hour.
  - `GET /reports/throughput?from&to&taskType&stationId&granularity` (reports binary)
  - `GET /reports/throughput/freshness` (lag vs real time)
  - MCP tool `get_fulfillment_throughput_report` (calls the reports REST;
    never opens the analytical DB directly)
  - Inputs actually projected: `TaskClaimed`, `TaskCompleted`,
    `LeaseExpired`, `WeightDiscrepancyDetected`, `PackageManifested`
    (`inbound/kafka/analytics_consumer.go`); every other analytics-topic
    event is acknowledged without projecting.
  - On-time-to-CPT KPI (ADR-0026, companion to order-management ADR 0014
    §6): `PackagesManifested`/`PackagesOnTimeToCPT`/`PackagesLateToCPT`
    raw counts on each row (SLAM rows only in practice), attributed to
    the manifested package's originating SLAM task via
    `TaskRepo.FindByOrderRef`. `manifestedAt <= cpt` counts as on time —
    the deliberate mirror of `task.Task.IsCPTMissed`'s own boundary.
    Rate is derived by the caller, never stored. MCP tool
    `get_on_time_to_cpt` sums the raw counts across a window and derives
    the rate.

## Standard metrics convention (ADR-0019)

Tier 1 baseline + Tier 2 naming convention applied fleet-wide; see
`internal/observability/metrics.go` and `httpmetrics.go` for this service's
implementation, and ADR-0019 for the naming rules to follow when adding a
new metric.
