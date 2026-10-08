---
paths:
  - "internal/adapters/inbound/http/**"
  - "apis/openapi*.yaml"
  - "apis/openapi/**"
  - "internal/adapters/**/kafka/**"
  - "internal/adapters/outbound/events/**"
  - "apis/asyncapi*"
---

# API Surface & Cross-Service Integration

## REST API (inbound adapter) — 19 operations in `apis/openapi.yaml`, 19 routes on the router

- POST /tasks                                 -> CreateTask
- GET  /tasks?orderRef=                       -> GetTasksByOrderRef
- POST /stations                              -> RegisterStation (optional `locationCode`, ADR-0024)
- POST /stations/{stationId}/claim-next       -> ClaimNext
- POST /stations/{stationId}/check-in         -> CheckInStation
- POST /stations/{stationId}/check-out        -> CheckOutStation
- POST /tasks/{id}/renew-lease                -> RenewLease
- POST /tasks/{id}/complete                   -> CompleteTask
- POST /tasks/{id}/seal-package                -> SealPackage. 409 `task-not-claimed` when the
  lease is missing or EXPIRED (same error as complete/renew-lease), 409
  `task-not-owner` when ANOTHER station holds an active lease (ADR-0038).
- GET  /packages/{id}                         -> GetPackage (ADR-0033). 200 `PackageResponse`
  (same DTO/schema as seal-package's 201); 404 `package-not-found`. Read
  `status` here to learn the SLAM outcome (LABELED vs DIVERTED).
- GET  /packages?orderRef=                    -> GetPackagesByOrderRef (ADR-0033). 200 array of
  `PackageResponse` ordered by id (empty when none); 400 `invalid-request`
  on missing/empty `orderRef` — same contract as `GET /tasks?orderRef=`.
- POST /packages/{id}/slam                    -> RunSlam (204 in BOTH outcomes — label applied
  or diverted; deliberately unchanged, see ADR-0033)
- GET  /queues/{taskType}/depth               -> GetQueueDepth
- GET  /capacity/{capability}                 -> GetInstalledCapacity (ADR-0018)
- POST /tasks/expire-leases                   -> ExpireLeases
- POST /tasks/sweep-cpt-misses                 -> SweepCPTMisses (ADR-0025)
  Both sweeps are driven by chart CronJobs, ON by default (`sweeps.enabled`,
  ADR-0037): expire-leases every minute, CPT sweep every 5 minutes, both
  configurable, `concurrencyPolicy: Forbid`. There is deliberately no in-process
  ticker (ADR-0003/0025). `TaskCPTMissed` volume scales with the CPT cadence.
- POST /rebin/arrivals                        -> ArriveAtRebin (ADR-0016; tag `Rebin`,
  operationId `arriveAtRebin`). The pick->pack handoff: 204 on success
  (idempotent per (orderRef, lineId); the first call fixes the order's
  required line set; the PACK task is created exactly once, by the arrival
  that completes the set), 400 `invalid-request` (no `instance`), 422
  `rebin-unknown-line` (`consolidation.ErrUnknownLine`), 500.
- GET  /healthz
- GET  /readyz                                 (readiness; flips to 503 on shutdown, ADR-0029)

Router and spec must stay in a two-way 1:1 match —
`internal/adapters/inbound/http/openapi_routes_test.go` walks the chi
router and fails on a route missing from the spec or a spec operation with
no route. Adding a route means adding it to `apis/openapi.yaml` (then
regenerating the docs reference) in the same change.

`cmd/fulfillment-reports` serves a separate read-only surface
(`GET /reports/throughput`, `GET /reports/throughput/freshness`,
`GET /healthz`) — see `analytics-data-product.md`; it is not in
`apis/openapi.yaml` either.

JSON DTOs live in the http adapter; never leak domain structs. Errors are
RFC 7807 `application/problem+json` (ADR-0005).

## The `orderRef` cross-service contract

`GET /tasks?orderRef=` is the read side backing the fleet's cross-service
Order Lifecycle console screen — see ADR-0002 in `warehouse-ops-agent`'s docs
and this repo's own adoption-record ADR-0013. The `orderRef` query param is
**not** order-management's plain order id — it is wes-work-planning's own
per-line WorkUnit id (`<orderId>-line-<lineNo>`), because `Task.OrderRef` is
stamped from the `WorkReleased` Kafka payload's `work_unit_id` field, not
from any order id directly. Callers needing "every task for order X" must
first resolve that order's WorkUnit ids via wes-work-planning's
`GET /work-units?reference=`, then call this endpoint once per WorkUnit (the
console-bff does exactly this). Returns every task for that WorkUnit id
including retried legs, array-shaped, side-effect-free.

## CORS (ADR-0013)

`go-chi/cors` middleware is enabled on every route, allowing
`CORS_ALLOWED_ORIGINS` (env, default
`http://localhost:5173,http://localhost:5184` — the `warehouse-console` shell
and this service's own `fulfillment-mfe` remote).

## Auth (fleet REST identity — currently REMOVED)

ADR-0021 introduced static-bearer-key REST identity with read/read-write
scopes across the fleet; ADR-0022 **removed** the REST + MCP static-bearer
auth layer from this service again and supersedes ADR-0021. Every REST
route (OLTP and `/reports/*`) and every MCP tool is unauthenticated today —
there is no auth middleware and no `AUTH_MODE`/`*_KEY` env var in any
`cmd/*/main.go`. Do not resurrect ADR-0021's design without a new ADR.

## Events: CloudEvents 1.0 is mandatory (ADR-0032)

Every Kafka message this service produces or consumes — integration AND
analytics topics — is a CloudEvents 1.0 event in structured content mode,
built/validated/decoded ONLY via `internal/adapters/kafka/cloudevents`
(official `sdk-go/v2/event`; transport stays kafka-go). Every produced
message carries `content-type: application/cloudevents+json; charset=UTF-8`.
Required attributes: `specversion=1.0`, `id` (UUID v4, minted once in
`Encode`, persisted by the outbox), `source=/warehouse/fulfillment-execution`,
`type`, `subject` (aggregate id = Kafka key), `time`,
`datacontenttype=application/json`,
`dataschema=urn:warehouse:fulfillment-execution:<events|analytics>:<Event>:v1`.
There is no flat envelope, no dual mode, no `EVENT_ENVELOPE_MODE`.
No dual-write, no dual-read, no envelope toggle env var (`EVENT_ENVELOPE_MODE` is gone).
`type` = `com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>`;
for this service `com.warehouse.wes.fulfillment-execution.<entity>.<EventName>`.
A breaking payload change => new `.v2` type + new dataschema version, never
mutate an existing one. Fleet-wide type catalogue: ADR-0032 (`docs/docs/adr/`).

## Events published (AsyncAPI: `apis/asyncapi.yaml`)

`type` = `com.warehouse.wes.fulfillment-execution.<entity>.<Event>`
(entity = `task` | `package`).

| Topic | `type` | `data` fields | Consumers |
| --- | --- | --- | --- |
| `warehouse.fulfillment.events` | `...task.TaskCompleted` | `task_id`, `station_id`, `work_unit_id`, `associate_id`, `duration_seconds`, `task_type`, and optional `order_ref` (the ORDER id the work was released for, from `WorkReleased.ref`; omitted for transfer work and tasks created without one; ADR-0040) and optional `line_no` (the order line, stamped on the Task as `source_line_no` from `WorkReleased.line_no` for order work only and never parsed from the id; omitted when unknown; ADR-0041) | wes-work-planning, labor-performance, inventory-storage (confirms the picked line when `line_no` is present, else on the order's last PICK; decided in inventory-storage) |
| `warehouse.fulfillment.events` | `...task.TaskCPTMissed` | `task_id`, `order_ref`, `task_type`, `cpt` | order-management (ADR-0025) |
| `warehouse.fulfillment.events` | `...package.PackageManifested` | `package_id`, `order_ref` | order-management (ADR-0025) |
| `warehouse.fulfillment.analytics` | `...task.{TaskCreated,TaskClaimed,LeaseExpired,TaskCompleted,ItemPicked}`, `...package.{PackageSealed,WeightDiscrepancyDetected,LabelApplied,PackageDiverted,PackageManifested}` | see `apis/asyncapi.yaml` | this service's analytics projector |

The publisher allowlist in `outbound/kafka/publisher.go` (integration) and
`analytics_publisher.go` (analytics) is the source of truth.
`ItemArrivedAtRebin` and `OrderConsolidated` (ADR-0016) are domain events
too, but are not published on either topic (if ever published, their entity
is `orderconsolidation`).

`work_unit_id` on `TaskCompleted` is what makes the feedback loop to Work
Planning work: the publisher looks the task back up through `ports.TaskRepo`
and reads `OrderRef()` (populated from `WorkReleased.data.work_unit_id` at
creation), so Work Planning gets back exactly the id it sent and can call
`RecordCompletion(workUnitId)`. `associate_id`/`duration_seconds`/`task_type`
are for the labor-performance context (ADR-0014, ADR-0023); all three are
soft/optional (omitted when unavailable — e.g. a robot station never
checks anyone in for `associate_id`, or the completed task can no longer be
found for `task_type`). `task_type` is read directly off the same `Task`
`work_unit_id` already loads via `TaskRepo` — no new repo dependency.

## Events consumed

| Source context | Topic | Full CloudEvents `type` | Effect here |
| --- | --- | --- | --- |
| `wes-work-planning` | `warehouse.work-planning.events` | `com.warehouse.wes.work-planning.workunit.WorkReleased` | Creates a `Task` via `CreateTask` |
| `product-master` | `warehouse.product-master.events` | `com.warehouse.wms.product-master.product.ProductClassified` (every other type ignored) | Only when `PRODUCT_CLASSIFICATION_MODE=kafka`: version-guarded upsert of `product_classification_copy`, read by `SealPackage` (ADR-0039). Group `PRODUCT_CLASSIFICATION_CONSUMER_GROUP` (boot error when unset in kafka mode); FetchMessage, claim + upsert in one `UnitOfWork`, then CommitMessages |
| `process-path-management` | `warehouse.process-path-management.events` | `com.warehouse.wes.process-path-management.processpath.ProcessPath{Created,Updated,Deactivated}` | Only when `PATH_CATALOGUE_SOURCE=kafka` (default `file`): replays into the in-memory process-path catalogue (`outbound/kafkacatalog`) |
| this service | `warehouse.fulfillment.analytics` | the five projecting analytics types | analytics projector (`inbound/kafka/analytics_consumer.go`) |

Consumers decode with `cloudevents.Decode`, dispatch on the FULL `type`
(never a short name), ignore unknown types, dedupe on `id`, read
`time`/`subject` from attributes and the payload via `DataAs`. A message that
fails CloudEvents validation (incl. the retired flat envelope) wraps
`cloudevents.ErrNotCloudEvent`: the `WorkReleased` consumer dead-letters it
to `<topic>.dlq`; the catalogue consumer, the projector and the
`ProductClassified` consumer WARN-log and skip it.

The `WorkReleased` Anti-Corruption Layer maps `data.path_id` -> `task.Type`
via the process-path catalogue (ADR-0017: `pick*`->PICK, `pack*`->PACK,
`slam*`->SLAM, `rebin*`->REBIN by prefix match, not exact match — a real
path id looks like `pick-zone-a`, not bare `pick`). An unknown `path_id` is
a hard handling error — there is no default-to-PICK. Idempotent via
`ProcessedEvents.MarkProcessed` on the CloudEvents `id` (Postgres
primary-key-backed dedup; in-memory adapter uses a mutex map). Consumer
group: `WORK_RELEASED_CONSUMER_GROUP`, default `fulfillment-execution`.

## Outbound synchronous calls (permissive by default)

| Target | Endpoint | Enabled by | Used by |
| --- | --- | --- | --- |
| `facility-layout` | `GET /locations/{locationCode}` | `LOCATION_ROLE_MODE=http` + `FACILITY_LAYOUT_BASE_URL` | `RegisterStation` WorkCenter role check (ADR-0024) |

Product classification is NOT a synchronous call any more: `SealPackage`'s
`ProductClassificationLookup` reads the local copy fed by product-master's
events (`PRODUCT_CLASSIFICATION_MODE=kafka|permissive`, ADR-0039). The old
inventory-storage REST lookup and `INVENTORY_STORAGE_BASE_URL` are removed;
`PRODUCT_CLASSIFICATION_MODE=http` fails the boot.

Inbound synchronous callers: `workforce-management` calls
`GET /capacity/{capability}` (ADR-0018); the console BFF calls
`GET /tasks?orderRef=`; `warehouse-ops-agent` calls the MCP server.

## Transactional outbox (ADR-0020)

`internal/adapters/outbound/postgres/outbox_publisher.go` +
`outbox_relay.go` feed both the integration topic and the analytics topic
from one outbox table with an in-process relay — this repo is the fleet's
REFERENCE implementation for the outbox pattern (see
`process-path-management`'s PR #7 for the original, and the fleet skill's
`transactional-outbox-rollout.md` for the multi-service rollout brief).
