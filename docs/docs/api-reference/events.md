---
id: events
title: Events (AsyncAPI)
sidebar_label: Events
sidebar_position: 3
description: The mandatory CloudEvents 1.0 envelope, the type naming convention, and every event this service publishes and consumes — from the real apis/asyncapi.yaml.
---

# Events

The asynchronous contract is `apis/asyncapi.yaml` (**AsyncAPI 2.6.0**), linted
in CI by Spectral alongside the OpenAPI spec. This page is hand-authored from
that file and the adapter code; the two describe the same wire format.

## The CloudEvents envelope (mandatory)

Every Kafka message this service produces **or** consumes — on the
integration topics and on the internal analytics topic — is a
**CloudEvents 1.0** event in the Kafka protocol binding's *structured*
content mode: the whole event (context attributes plus `data`) is the JSON
message value. There is no other envelope and no envelope toggle
([ADR-0032](../adr/0032-cloudevents-mandatory-envelope.md)).

- Kafka header on every produced message:
  `content-type: application/cloudevents+json; charset=UTF-8`, alongside the
  W3C `traceparent`/`tracestate` headers.
- Kafka key: the aggregate id (task id or package id) — unchanged, with the
  `Hash` balancer, so all events for one aggregate share a partition.
- Built and validated with the official `github.com/cloudevents/sdk-go/v2/event`
  package, through the single helper `internal/adapters/kafka/cloudevents`.

### Context attributes (all required)

| Attribute | Value |
| --- | --- |
| `specversion` | `"1.0"` |
| `id` | UUID v4, minted once per occurrence and persisted with the outbox row, so a redelivery carries the same id. The consumer deduplication key. |
| `source` | `/warehouse/fulfillment-execution` |
| `type` | `com.warehouse.wes.fulfillment-execution.<entity>.<EventName>` |
| `subject` | The aggregate instance id — equal to the Kafka key |
| `time` | The domain occurred-at instant, UTC, RFC 3339 |
| `datacontenttype` | `application/json` |
| `dataschema` | `urn:warehouse:fulfillment-execution:<events\|analytics>:<EventName>:v1` |
| `data` | The event payload, snake_case keys |

### The `type` naming convention

```
com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>
```

For this service `<subdomain>` is `wes`, `<bounded-context>` is
`fulfillment-execution`, and `<entity>` is the raising aggregate — `task` or
`package`. The same `type` names an occurrence on both the integration and the
analytics topic; `dataschema` names the payload shape. A breaking payload
change ships as a new `.v2` type with a new dataschema version, never by
mutating the old one.

### Example

```json
{
  "specversion": "1.0",
  "id": "6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b",
  "source": "/warehouse/fulfillment-execution",
  "type": "com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
  "subject": "task-8a1f",
  "time": "2026-09-30T15:00:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:fulfillment-execution:events:TaskCompleted:v1",
  "data": {
    "task_id": "task-8a1f",
    "station_id": "station-03",
    "work_unit_id": "wu-8a1f",
    "associate_id": "worker-42",
    "duration_seconds": 245,
    "task_type": "PICK"
  }
}
```

## Channels

| Channel | Direction | Content |
| --- | --- | --- |
| `warehouse.fulfillment.events` | published | Integration events (below) |
| `warehouse.fulfillment.analytics` | published, consumed by this service's own projector | Analytics events ([ADR-0012](../adr/0012-analytical-data-product.md)) |
| `warehouse.work-planning.events` | consumed | `WorkReleased` from wes-work-planning |
| `warehouse.process-path-management.events` | consumed (when `PATH_CATALOGUE_SOURCE=kafka`) | `ProcessPath*` catalogue events |

Server: `kafka.warehouse-systems.internal:9092`, protocol `kafka`, configured
per service via `KAFKA_BROKERS`.

## Events published — integration (`warehouse.fulfillment.events`)

| `type` | `data` fields | Consumed by |
| --- | --- | --- |
| `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` | `task_id`, `station_id`, `work_unit_id`, `associate_id`, `duration_seconds`, `task_type` | wes-work-planning, labor-performance |
| `com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed` | `task_id`, `order_ref`, `task_type`, `cpt` | order-management |
| `com.warehouse.wes.fulfillment-execution.package.PackageManifested` | `package_id`, `order_ref` | order-management |

### `TaskCompleted` and the `work_unit_id` enrichment

`work_unit_id` is the field that makes the feedback loop work. The *domain*
event carries only `TaskId` and `StationId`; the publisher looks the task back
up through `ports.TaskRepo` and reads `OrderRef()` — which was populated from
`WorkReleased.data.work_unit_id` when the task was created. So the value
Work Planning gets back is exactly the one it sent, and it can call
`RecordCompletion(workUnitId)` directly. `associate_id`, `duration_seconds`
and `task_type` are soft enrichments for labor-performance (ADR-0014,
ADR-0023), omitted when unavailable.

### `TaskCPTMissed` and `PackageManifested` — the promise feedback loop (ADR-0025)

These two events are the fulfillment-execution half of order-management
ADR 0014 §5's promise feedback loop: order-management's `RepromiseOrder`
consumer reacts to them to recompute — and, if it moved, re-promise — the
affected shipment group's delivery date. Neither needs a repo-lookup
enrichment: every field comes straight off the domain event.

`TaskCPTMissed` is raised by the Clock-driven CPT-missed sweep (`POST
/tasks/sweep-cpt-misses`) for a task still open (Pending or Claimed) at
or past its CPT, and **re-fires on every sweep pass** for as long as the
task stays overdue — each pass is a new occurrence with a new `id`, so a
consumer must dedupe on its own business key.

`PackageManifested` is raised alongside `LabelApplied` — additively,
never in its place — when a package passes its SLAM weigh-check. A
diverted package (weight outside tolerance) was **not** manifested and
does not raise this event.

## Events published — analytics (`warehouse.fulfillment.analytics`)

Same `type` strings, `dataschema` `urn:warehouse:fulfillment-execution:analytics:<EventName>:v1`:

| `type` | `data` fields |
| --- | --- |
| `...task.TaskCreated` | `task_id`, `task_type` |
| `...task.TaskClaimed` | `task_id`, `task_type`, `station_id` |
| `...task.LeaseExpired` | `task_id`, `task_type` |
| `...task.TaskCompleted` | `task_id`, `task_type`, `station_id` |
| `...task.ItemPicked` | `task_id`, `task_type` |
| `...package.PackageSealed` | `package_id` |
| `...package.WeightDiscrepancyDetected` | `package_id`, `expected_g`, `actual_g` |
| `...package.LabelApplied` | `package_id` |
| `...package.PackageDiverted` | `package_id` |
| `...package.PackageManifested` | `package_id`, `order_ref`, `task_type`, `station_id`, `on_time`, `resolved` |

`ItemArrivedAtRebin` and `OrderConsolidated` (ADR-0016) are in-process only
and are not published on either topic.

## Events consumed

| Source context | Topic | Full `type` | Effect here |
| --- | --- | --- | --- |
| `wes-work-planning` | `warehouse.work-planning.events` | `com.warehouse.wes.work-planning.workunit.WorkReleased` | Creates a `Task` via the `CreateTask` use case |
| `process-path-management` | `warehouse.process-path-management.events` | `com.warehouse.wes.process-path-management.processpath.ProcessPathCreated` / `ProcessPathUpdated` / `ProcessPathDeactivated` | Maintains the in-memory process-path catalogue |

Consumers dispatch on the **full** `type` string — never a short name, never a
suffix match — and silently ignore unknown types, so new event types upstream
cannot break them. A `WorkReleased` looks like:

```json
{
  "specversion": "1.0",
  "id": "11111111-1111-4111-8111-111111111111",
  "source": "/warehouse/wes-work-planning",
  "type": "com.warehouse.wes.work-planning.workunit.WorkReleased",
  "subject": "wu-8a1f",
  "time": "2026-08-23T09:00:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:wes-work-planning:events:WorkReleased:v1",
  "data": {
    "path_id": "pick-zone-a",
    "work_unit_id": "wu-8a1f",
    "cpt": "2026-08-23T18:00:00Z",
    "ref": "order-4471"
  }
}
```

Mapping into this context's model (the Anti-Corruption Layer):

| From | To | How |
| --- | --- | --- |
| `data.path_id` | `task.Type` | process-path catalogue lookup, longest `matchPrefix` wins (`pick-zone-a`→`PICK`); no match is a hard error, never a default ([ADR-0017](../adr/0017-process-path-catalogue-as-configuration.md)) |
| `data.work_unit_id` | `shared.OrderRef` | direct |
| `data.cpt` | `shared.CPT` | RFC 3339 → `time.Time` |
| *(from the matched path)* | `shared.CapabilitySet` | the path definition's `requiredCapabilities` |
| `data.ref` | *(unused)* | decoded but not mapped — `work_unit_id` is the correlation key |

### Invalid and legacy messages

A message that is not a valid CloudEvents 1.0 event — bad JSON, a missing
required attribute, a wrong `specversion`, or the retired flat
`event_id`/`event_type`/`occurred_at` envelope — is a deterministic poison
message. It is never parsed any other way and never retried:

- the `WorkReleased` consumer dead-letters it to
  `warehouse.work-planning.events.dlq` (ADR-0029);
- the process-path catalogue consumer and the analytics projector log it at
  WARN (topic/partition/offset) and commit past it.

### Idempotency

Kafka delivers at least once, so every consumer is idempotent by
construction. Before creating a task the `WorkReleased` consumer calls
`ProcessedEvents.MarkProcessed(ctx, id)` with the CloudEvents `id`, which
returns `true` only if this call newly recorded it:

- **New id** → create the `Task`.
- **Already present** → skip and acknowledge anyway.

Postgres backs this with `processed_events (event_id TEXT PRIMARY KEY,
processed_at TIMESTAMPTZ)` — the column keeps its name and now stores the
CloudEvents `id`; the primary-key constraint *is* the deduplication. The
in-memory adapter uses a mutex-guarded map. A unit test feeds the same `id`
twice and asserts exactly one task exists.

Consumer group: `WORK_RELEASED_CONSUMER_GROUP`, default `fulfillment-execution`.

## Smoke-testing the real thing

With the shared broker from the `warehouse-infra` kind cluster reachable at
`localhost:9092`:

```bash
# Consume: publish a WorkReleased CloudEvent and watch a Task appear
kafka-console-producer.sh --broker-list localhost:9092 \
  --topic warehouse.work-planning.events <<'EOF'
{"specversion":"1.0","id":"11111111-1111-4111-8111-111111111111","source":"/warehouse/wes-work-planning","type":"com.warehouse.wes.work-planning.workunit.WorkReleased","subject":"wu-8a1f","time":"2026-08-23T09:00:00Z","datacontenttype":"application/json","dataschema":"urn:warehouse:wes-work-planning:events:WorkReleased:v1","data":{"path_id":"pick-zone-a","work_unit_id":"wu-8a1f","cpt":"2026-08-23T18:00:00Z","ref":"order-4471"}}
EOF
curl -sS localhost:8080/queues/PICK/depth

# Publish: complete a task with EVENT_PUBLISHER=kafka and watch it land
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic warehouse.fulfillment.events --from-beginning
```

Sending the same `id` twice must still produce exactly one task — that is the
idempotency guarantee, and it is worth verifying by hand at least once.
