---
id: integration-contracts
title: Integration contracts
sidebar_label: Integration contracts
sidebar_position: 2
description: The exact topics, envelopes, mappings, idempotency guarantees and configuration behind this service's live Kafka edges.
---

# Integration contracts

The operational detail behind the Kafka edges on the
[Context map](./context-map.md). Everything here is taken from the adapter
code and `INTEGRATION.md`, not from intent.

## Topic summary

| Direction | Topic | Event | Adapter |
| --- | --- | --- | --- |
| Consume | `warehouse.work-planning.events` | `WorkReleased` | `internal/adapters/inbound/kafka/consumer.go` |
| Publish | `warehouse.fulfillment.events` | `TaskCompleted`, `TaskCPTMissed`, `PackageManifested`, `TransferPicked`, `TransferDispatched`, `TransferArrived` | `internal/adapters/outbound/kafka/publisher.go` |
| Consume (opt-in) | `warehouse.process-path-management.events` | process-path catalogue events | `internal/adapters/outbound/kafkacatalog` — only when `PATH_CATALOGUE_SOURCE=kafka` |
| Publish + consume | `warehouse.fulfillment.analytics` | analytics events (ADR-0012) | `outbound/kafka/analytics_publisher.go` → `inbound/kafka/analytics_consumer.go` |

Every message on every one of these topics is a **CloudEvents 1.0** event in
structured content mode, built and validated with the official
`sdk-go/v2/event` package via `internal/adapters/kafka/cloudevents`, and every
produced message carries `content-type: application/cloudevents+json;
charset=UTF-8` ([ADR-0032](../adr/0032-cloudevents-mandatory-envelope.md)).
There is no flat envelope and no envelope toggle.

Client library on both sides: `github.com/segmentio/kafka-go` (pure Go, no
cgo). Broker list comes from `KAFKA_BROKERS`, default `localhost:9092`.

A shared broker for the whole platform runs in the `warehouse-infra` kind
cluster, exposed on the host at `localhost:9092`. This repo's own
`docker-compose.yml` deliberately defines **only Postgres** — adding a second
broker would fragment the platform's integration testing.

## Inbound: `WorkReleased`

### Envelope

A CloudEvents 1.0 event; this consumer dispatches on the full `type`
`com.warehouse.wes.work-planning.workunit.WorkReleased` and ignores any other
type:

```json
{
  "specversion": "1.0",
  "id": "11111111-1111-4111-8111-111111111111",
  "source": "/warehouse/wes-work-planning",
  "type": "com.warehouse.wes.work-planning.workunit.WorkReleased",
  "subject": "wu-8a1f",
  "time": "2026-08-21T22:00:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:wes-work-planning:events:WorkReleased:v1",
  "data": {
    "path_id": "pick-zone-a",
    "work_unit_id": "wu-8a1f",
    "cpt": "2026-08-23T18:00:00Z",
    "ref": "order-4471",
    "fragile": false,
    "gift_wrap": false
  }
}
```

`fragile` and `gift_wrap` are optional packing hints (default `false`).

### The translation (Anti-Corruption Layer)

| From | To | How |
| --- | --- | --- |
| `data.path_id` | `task.Type` | process-path catalogue lookup, longest `matchPrefix` wins — see below |
| `data.work_unit_id` | `shared.OrderRef` | direct |
| `data.cpt` | `shared.CPT` | RFC 3339 timestamp |
| *(from the matched path)* | `shared.CapabilitySet` | the path definition's `requiredCapabilities` |
| `data.fragile` / `data.gift_wrap` | `Task.Fragile` / `Task.GiftWrap` | direct, default `false` |
| `data.ref` | — | decoded, not mapped |

The consumer then calls the **existing** `CreateTask` use case. No new use
case was introduced for the Kafka path — creating a task from released work
*is* what `CreateTask` is for, and giving the Kafka path its own parallel use
case would have meant two code paths that must stay in agreement.

:::info[path_id resolves through the process-path catalogue]
Since [ADR-0017](../adr/0017-process-path-catalogue-as-configuration.md) the consumer calls `PathCatalogue.Lookup(path_id)`:
the **longest** declared `matchPrefix` that prefixes the id (case-insensitive)
wins, and the matched definition supplies both the task type (its `id`, e.g.
`PICK`, `PACK`, `SLAM`, `REBIN`) and the required capabilities. A `path_id`
that matches nothing is a **hard error** — the message is not acked as a
silent Pick task, which is what the retired prefix-guessing code used to do.

The catalogue comes from `PATH_CATALOGUE_SOURCE`: `file` (default) loads
`PATH_CATALOGUE_FILE` once at boot; `kafka` replays
`warehouse.process-path-management.events` into memory and follows live
changes. The `warehouse-infra` kind cluster runs the `kafka` source; its
`config/process-paths/sortable-fc.yaml` is kept only as the rollback target
and as a handy file for local `go run`.
:::

### Idempotency

Kafka is at-least-once, so redelivery is expected, not exceptional. Before
creating anything, the consumer calls:

```go
isNew, err := c.Processed.MarkProcessed(ctx, env.EventId) // the CloudEvents id
if !isNew {
    return nil   // already applied by a prior delivery — ack anyway
}
```

`MarkProcessed` returns `true` **only** if this call newly recorded the id, so
the check-and-record is a single atomic operation rather than a
read-then-write race.

| Adapter | Mechanism |
| --- | --- |
| Postgres | `processed_events (event_id TEXT PRIMARY KEY, processed_at TIMESTAMPTZ)` — the column now stores the CloudEvents `id`; the primary-key constraint *is* the deduplication |
| Memory | mutex-guarded `map[string]struct{}` |

A unit test feeds the same `id` twice and asserts exactly one task exists.

### Failure handling

A message that fails handling after the in-process retries (ADR-0029: 3
attempts with jittered backoff) is published to
`warehouse.work-planning.events.dlq` with `x-dlq-*` failure headers, and the
loop continues. A message that is not a valid CloudEvents 1.0 event —
including the retired flat `event_id`/`event_type` envelope — is a
deterministic poison message: it is dead-lettered immediately, never retried
and never parsed any other way. The DLQ writer is wired only when
`EVENT_PUBLISHER=kafka`; otherwise the failure is only logged. Since
ADR-0036 the processed-event claim, the catalogue lookup and the task
creation commit in ONE UnitOfWork (`usecases.ApplyWorkReleased`), so a
failed create leaves no `processed_events` row — a replayed dead-lettered
message simply re-applies, no manual clearing needed. Only a message that
genuinely completed keeps its row.

Consumer group: `WORK_RELEASED_CONSUMER_GROUP`, default `fulfillment-execution`.

WorkReleased may carry an optional inter-warehouse-transfer correlation
block (`transfer_ref`, `work_kind`, `site_id`, `sku`, `quantity`); see
[ADR-0036](../adr/0036-transfer-task-types-and-facts.md). Its presence
turns the created task into transfer work whose completion publishes a
transfer fact; absence is exactly the pre-ADR-0036 behavior.

## Outbound: transfer facts (`TransferPicked` / `TransferDispatched` / `TransferArrived`)

When a task carrying a transfer correlation block completes, exactly ONE
fact is selected by `work_kind` and published on
`warehouse.fulfillment.events` alongside the unchanged `TaskCompleted`,
committing with the task's Completed state in the same outbox transaction:

| `work_kind` | fact |
| --- | --- |
| `TRANSFER_PICK` | `TransferPicked` |
| `TRANSFER_DISPATCH` | `TransferDispatched` |
| `TRANSFER_ARRIVAL` | `TransferArrived` |

Envelope: type `com.warehouse.wes.fulfillment-execution.transfer.<Fact>`,
dataschema `urn:warehouse:fulfillment-execution:events:<Fact>:v1`,
subject and Kafka key = the completing task id, `time` = the completion
time. Payload: `{transfer_ref, demand_id?, work_unit_id, task_id,
work_kind, site_id?, sku?, quantity?}` — `demand_id` is the release's own
`ref`. Non-transfer tasks never publish a transfer fact.

## Outbound: `TaskCompleted`

### Selection

The publisher is chosen at the composition root:

```go
if getenv("EVENT_PUBLISHER", "log") != "kafka" {
    return events.NewLogPublisher(logger), nil, func() {}
}
// with Postgres: encoders feed the transactional outbox (ADR-0020)
integration := outboundkafka.NewPublisherWithWriter(nil, tasks, stations, uuid.NewString)
analytics := outboundkafka.NewAnalyticsPublisherWithWriter(nil, tasks, uuid.NewString)
return postgres.NewOutboxPublisher(pool, integration, analytics), relay, ...
```

Both satisfy `ports.EventPublisher`, so **no use case knows which one it
got** — the default stays `log` so tests and local runs need no broker.

### Envelope on the wire

```json
{
  "specversion": "1.0",
  "id": "6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b",
  "source": "/warehouse/fulfillment-execution",
  "type": "com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
  "subject": "task-8a1f",
  "time": "2026-08-22T14:04:00Z",
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

Message key (and CloudEvents `subject`): the task id, so all events for one
task land on the same partition and preserve order. The `id` is a UUID v4
minted once at encode time and stored in the outbox row, so a relay retry
republishes the same `id`.

### The enrichment

`TaskCompleted` as a domain event carries only `TaskId` and `StationId`. The
publisher backfills the third field:

```go
t, err := p.Tasks.FindById(ctx, tc.TaskId)
workUnitId = string(t.OrderRef())
```

`OrderRef` was populated from `WorkReleased.data.work_unit_id` when the task
was created, so the value returned is exactly the one Work Planning sent —
it can call `RecordCompletion(workUnitId)` without a join.

The enrichment happens **in the adapter**, never on the domain event. The
correlation key exists because a specific consumer needs it; letting that
requirement shape the domain model would be the tail wagging the dog. The same
repo-lookup pattern is used by `inventory-storage`'s publisher for
`ReservationRevoked`, so it is a platform convention.

`TaskCPTMissed` and `PackageManifested` (ADR-0025) are published the same
way, keyed by task id and package id respectively. Any other event passed to
this publisher is **skipped**, not errored — it is not part of the published
integration contract.

## Configuration

| Env var | Default | Effect |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list for the consumers, the publishers and the outbox relay |
| `EVENT_PUBLISHER` | `log` | `kafka` swaps in the Kafka encoders (via the outbox when Postgres is set) for `ports.EventPublisher`, and wires the `WorkReleased` DLQ writer |
| `WORK_RELEASED_CONSUMER_GROUP` | `fulfillment-execution` | Consumer group of the `WorkReleased` consumer |
| `OUTBOX_RELAY_INTERVAL` | `1s` | Idle poll interval of the outbox relay |
| `PATH_CATALOGUE_SOURCE` | `file` | `kafka` makes this service a consumer of `warehouse.process-path-management.events` |
| `DATABASE_URL` | *(unset)* | Unset selects in-memory repositories **including** `ProcessedEvents` — idempotency then holds only for the lifetime of the process |

That last row matters operationally: running with `EVENT_PUBLISHER=kafka` but
without `DATABASE_URL` gives you in-memory deduplication, so a restart forgets
which events were processed and redelivered messages will create duplicate
tasks. The consumer starts regardless of storage choice.

## Verifying it end to end

With the shared broker running:

```bash
# 1. Inbound — publish a WorkReleased, confirm a Task appears
kafka-console-producer.sh --broker-list localhost:9092 \
  --topic warehouse.work-planning.events <<'EOF'
{"specversion":"1.0","id":"11111111-1111-4111-8111-111111111111","source":"/warehouse/wes-work-planning","type":"com.warehouse.wes.work-planning.workunit.WorkReleased","subject":"wu-8a1f","time":"2026-08-23T09:00:00Z","datacontenttype":"application/json","dataschema":"urn:warehouse:wes-work-planning:events:WorkReleased:v1","data":{"path_id":"pick-zone-a","work_unit_id":"wu-8a1f","cpt":"2026-08-23T18:00:00Z","ref":"order-4471"}}
EOF
curl -sS localhost:8080/queues/PICK/depth      # depth increments by exactly 1

# 2. Send the identical CloudEvent (same id) again — depth must NOT change

# 3. Outbound — with EVENT_PUBLISHER=kafka, drive the flow over HTTP
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic warehouse.fulfillment.events --from-beginning
# then register a station, claim-next, complete — and watch the message land
# with work_unit_id == "wu-8a1f"
```

## Contract linting

Both `apis/openapi.yaml` and `apis/asyncapi.yaml` are linted by
[Spectral](https://stoplight.io/open-source/spectral) in the `api-lint` CI
job, using `.spectral.yaml` and `.spectral.asyncapi.yaml`. A contract change
that breaks either ruleset fails the build before it can reach a consumer.

Spectral validates the spec's internal consistency; the code's conformance
to the CloudEvents wire format is pinned by the golden exact-JSON tests in
`internal/adapters/outbound/kafka/golden_test.go` (one per published `type`)
and by the testcontainers Kafka integration tests. See the
[Events](../api-reference/events.md) page for the full catalogue.
