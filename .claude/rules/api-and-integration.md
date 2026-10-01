# API Surface & Cross-Service Integration

## REST API (inbound adapter) — 17 operations in `apis/openapi.yaml`, 17 routes on the router

- POST /tasks                                 -> CreateTask
- GET  /tasks?orderRef=                       -> GetTasksByOrderRef
- POST /stations                              -> RegisterStation (optional `locationCode`, ADR-0024)
- POST /stations/{stationId}/claim-next       -> ClaimNext
- POST /stations/{stationId}/check-in         -> CheckInStation
- POST /stations/{stationId}/check-out        -> CheckOutStation
- POST /tasks/{id}/renew-lease                -> RenewLease
- POST /tasks/{id}/complete                   -> CompleteTask
- POST /tasks/{id}/seal-package                -> SealPackage
- POST /packages/{id}/slam                    -> RunSlam
- GET  /queues/{taskType}/depth               -> GetQueueDepth
- GET  /capacity/{capability}                 -> GetInstalledCapacity (ADR-0018)
- POST /tasks/expire-leases                   -> ExpireLeases
- POST /tasks/sweep-cpt-misses                 -> SweepCPTMisses (ADR-0025)
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

## Events published (AsyncAPI: `apis/asyncapi.yaml`)

CloudEvents 1.0 structured envelope; `type` = 
`com.warehouse.<subdomain>.<bounded-context>.<entity>.<Event>`, e.g.
`com.warehouse.wes.fulfillment-execution.task.TaskClaimed`. Channel:
`warehouse.fulfillment-execution.events` (per the spec — see divergence note
below for what's actually on the wire).

| Event | `type` suffix | `data` fields | On the wire today? |
| --- | --- | --- | --- |
| `TaskCreated` | `task.TaskCreated` | `taskId` | No |
| `TaskClaimed` | `task.TaskClaimed` | `taskId`, `stationId` | No |
| `LeaseExpired` | `task.LeaseExpired` | `taskId` | No |
| **`TaskCompleted`** | `task.TaskCompleted` | `taskId`, `stationId`, `workUnitId`, `associateId`, `durationSeconds`, `taskType` | **Yes** |
| `ItemPicked` | `task.ItemPicked` | `taskId` | No — not raised by any use case either |
| `PackageSealed` | `package.PackageSealed` | `packageId` | No |
| `WeightDiscrepancyDetected` | `package.WeightDiscrepancyDetected` | `packageId`, `expectedWeight`, `actualWeight` | No |
| `LabelApplied` | `package.LabelApplied` | `packageId` | No |
| `PackageDiverted` | `package.PackageDiverted` | `packageId` | No |
| `TaskCPTMissed` | `task.TaskCPTMissed` | `taskId`, `orderRef`, `taskType`, `cpt` | **Yes** (ADR-0025) |
| `PackageManifested` | `package.PackageManifested` | `packageId`, `orderRef` | **Yes** (ADR-0025) |

On the wire today (flat envelope, see divergence below) these three are
the only events `outbound/kafka/publisher.go` forwards to
`warehouse.fulfillment.events` — the publisher allowlist, not the domain
event list, is the source of truth. Known consumers of that topic:
`wes-work-planning` and `labor-performance` (`TaskCompleted`), and
`order-management`'s `RepromiseOrder` consumer (`TaskCPTMissed`,
`PackageManifested`). `ItemArrivedAtRebin` and `OrderConsolidated`
(ADR-0016) are domain events too, but are neither in the AsyncAPI
catalogue nor forwarded to either topic.

`workUnitId` on `TaskCompleted` is what makes the feedback loop to Work
Planning work: the publisher looks the task back up through `ports.TaskRepo`
and reads `OrderRef()` (populated from `WorkReleased.data.work_unit_id` at
creation), so Work Planning gets back exactly the id it sent and can call
`RecordCompletion(workUnitId)`. `associateId`/`durationSeconds`/`taskType`
are for the labor-performance context (ADR-0014, ADR-0023); all three are
soft/optional (omitted when unavailable — e.g. a robot station never
checks anyone in for `associateId`, or the completed task can no longer be
found for `taskType`). `taskType` is read directly off the same `Task`
`workUnitId` already loads via `TaskRepo` — no new repo dependency.

## Events consumed

| Source context | Topic | `event_type` | Effect here |
| --- | --- | --- | --- |
| `wes-work-planning` | `warehouse.work-planning.events` | `WorkReleased` | Creates a `Task` via `CreateTask` |
| `process-path-management` | `warehouse.process-path-management.events` | catalogue events | Only when `PATH_CATALOGUE_SOURCE=kafka` (default `file`): replays into the in-memory process-path catalogue (`outbound/kafkacatalog`) |

`WorkReleased` arrives in the flat platform envelope (not CloudEvents); the
Anti-Corruption Layer maps `data.path_id` -> `task.Type` via the process-path
catalogue (ADR-0017: `pick*`->PICK, `pack*`->PACK, `slam*`->SLAM, `rebin*`->
REBIN by prefix match, not exact match — a real path id looks like
`pick-zone-a`, not bare `pick`). An unknown `path_id` is a hard handling
error — there is no default-to-PICK. Idempotent via `ProcessedEvents.MarkProcessed`
(Postgres primary-key-backed dedup; in-memory adapter uses a mutex map).
Consumer group: `WORK_RELEASED_CONSUMER_GROUP`, default `fulfillment-execution`.

## Outbound synchronous calls (both permissive by default)

| Target | Endpoint | Enabled by | Used by |
| --- | --- | --- | --- |
| `inventory-storage` | `GET /products/{sku}/classification` | `PRODUCT_CLASSIFICATION_MODE=http` + `INVENTORY_STORAGE_BASE_URL` | `SealPackage` per-SKU DOT hazard lookup (ADR-0010) |
| `facility-layout` | `GET /locations/{locationCode}` | `LOCATION_ROLE_MODE=http` + `FACILITY_LAYOUT_BASE_URL` | `RegisterStation` WorkCenter role check (ADR-0024) |

Inbound synchronous callers: `workforce-management` calls
`GET /capacity/{capability}` (ADR-0018); the console BFF calls
`GET /tasks?orderRef=`; `warehouse-ops-agent` calls the MCP server.

## KNOWN DIVERGENCE: AsyncAPI spec vs. actual wire format

Documented (not hidden) in `docs/docs/api-reference/events.md`. The
**publisher today still uses the flat platform envelope**, not the
CloudEvents shape the spec describes:

| | `apis/asyncapi.yaml` (target) | `internal/adapters/outbound/kafka/publisher.go` (today) |
| --- | --- | --- |
| Channel/topic | `warehouse.fulfillment-execution.events` | `warehouse.fulfillment.events` |
| Envelope | CloudEvents 1.0 structured | flat platform envelope |
| Type field | `type: com.warehouse...TaskCompleted` | `event_type: "TaskCompleted"` |
| Id field | `id` | `event_id` |
| Timestamp | `time` | `occurred_at` |
| Source | `/warehouse/fulfillment-execution` | `fulfillment-execution` |
| Payload keys | `taskId`, `stationId`, `workUnitId` | `task_id`, `station_id`, `work_unit_id` |

The live integration with `wes-work-planning` works correctly against the
flat envelope on `warehouse.fulfillment.events` — it is the AsyncAPI document
describing the *intended future* contract, not the current wire format.
Migrating the publisher to CloudEvents is outstanding work; both producer and
consumer sides would move together since the topic name also changes.
**This is narrative/documentation staleness only — no code fix needed**, just
keep the divergence note current if either side changes.

## Transactional outbox (ADR-0020)

`internal/adapters/outbound/postgres/outbox_publisher.go` +
`outbox_relay.go` feed both the integration topic and the analytics topic
from one outbox table with an in-process relay — this repo is the fleet's
REFERENCE implementation for the outbox pattern (see
`process-path-management`'s PR #7 for the original, and the fleet skill's
`transactional-outbox-rollout.md` for the multi-service rollout brief).
