---
id: 0032-cloudevents-mandatory-envelope
title: ADR-0032 — CloudEvents 1.0 as the mandatory event envelope
sidebar_label: 0032 · CloudEvents 1.0 mandatory envelope
sidebar_position: 32
description: Every Kafka message fulfillment-execution produces or consumes — integration and analytics topics alike — is a CloudEvents 1.0 event in structured content mode, built and validated with the official sdk-go event package. The flat envelope, the analytics schema_version envelope, EVENT_ENVELOPE_MODE and all dual-read/dual-write code are removed. Supersedes ADR-0027 and the envelope part of ADR-0004. (Accepted)
---

# ADR-0032 — CloudEvents 1.0 as the mandatory event envelope

## Status

**Accepted** (2026-09-30). Fleet-wide standard, adopted by every
warehouse-systems service in one coordinated cutover.

Supersedes [ADR-0027](./0027-cloudevents-envelope-migration.md) (the
dual-read/dual-write migration plan) and the envelope part of
[ADR-0004](./0004-kafka-integration-events-and-envelope.md) (the flat
`event_id`/`event_type`/`occurred_at` envelope and its documented
spec-vs-wire gap).

## Context

ADR-0004 documented CloudEvents as the target envelope but shipped the flat
platform envelope. ADR-0027 planned a gradual migration behind an
`EVENT_ENVELOPE_MODE=flat|cloudevents|dual` toggle with a hand-rolled
`CloudEvent[T]` struct and a dual-read consumer that discriminated on
`specversion`. That design kept two wire formats alive indefinitely, put
an untyped toggle on every deployment, and duplicated the envelope logic
per service. The analytics topic used yet another envelope (the flat shape
plus `schema_version`).

The fleet decided to stop coexisting: one envelope, everywhere, cut over
together.

## Decision

### 1. Scope

**Every** message this service writes to or reads from Kafka is a
CloudEvents 1.0 event:

| direction | topic | content |
|-----------|-------|---------|
| publish | `warehouse.fulfillment.events` | integration events (`TaskCompleted`, `TaskCPTMissed`, `PackageManifested`) |
| publish | `warehouse.fulfillment.analytics` | analytics events (10 types, see below) |
| consume | `warehouse.work-planning.events` | `WorkReleased` from wes-work-planning |
| consume | `warehouse.process-path-management.events` | `ProcessPath*` catalogue events |
| consume | `warehouse.product-master.events` | `ProductClassified` from product-master, into the local classification copy ([ADR-0039](./0039-product-classification-local-copy.md)) |
| consume | `warehouse.fulfillment.analytics` | the analytics projector (`cmd/fulfillment-projector`) |

There is no flat envelope, no dual-write, no dual-read and no
`EVENT_ENVELOPE_MODE` (or any other envelope toggle). Topic names and
routing are unchanged.

### 2. Encoding

- CloudEvents **Kafka protocol binding, structured content mode**: the
  Kafka message value is the JSON event format.
- Every produced message carries the Kafka header
  `content-type: application/cloudevents+json; charset=UTF-8`, alongside
  the existing W3C `traceparent`/`tracestate` headers (trace context is
  **not** duplicated into CloudEvents extensions).
- The Kafka message key is unchanged (the aggregate id: `TaskId` or
  `PackageId`), and every writer keeps `kafkago.Hash{}` so per-aggregate
  ordering holds.
- Events are built, validated and (un)marshalled with the official SDK
  event package `github.com/cloudevents/sdk-go/v2/event` (v2.16.2). The
  SDK's protocol/client packages are not used; transport stays
  `segmentio/kafka-go`.
- The single helper lives in `internal/adapters/kafka/cloudevents`
  (`New(Spec)`, `Decode([]byte)`, `ContentTypeHeader()`, `Type`,
  `DataSchema`, `ErrNotCloudEvent`). Nothing else builds an envelope.

### 3. Context attributes (all required)

| attribute | value in this service |
|-----------|-----------------------|
| `specversion` | `1.0` |
| `id` | UUID v4 (`uuid.NewString`), minted **once** at `Encode` time. The outbox persists the encoded bytes, so the relay republishes the same `id` on every redelivery. `(source, id)` is the consumer idempotency key. |
| `source` | `/warehouse/fulfillment-execution` |
| `type` | `com.warehouse.wes.fulfillment-execution.<entity>.<EventName>` |
| `subject` | the raising aggregate's id — the same value as the Kafka key |
| `time` | the domain event's occurred-at, UTC, RFC 3339 |
| `datacontenttype` | `application/json` |
| `dataschema` | `urn:warehouse:fulfillment-execution:<events\|analytics>:<EventName>:v1` |

`data` is byte-for-byte the payload object this service published before
the cutover (snake_case keys). The analytics `schema_version` field is
removed; `dataschema` replaces it. No extension attributes.

### 4. Types

Entity segments are the aggregates already catalogued in
`apis/asyncapi.yaml`: `task` and `package`. The same `type` names an
occurrence on both topics; `dataschema` distinguishes the payload shape.

**Published — integration (`warehouse.fulfillment.events`):**

```
com.warehouse.wes.fulfillment-execution.task.TaskCompleted        -> wes-work-planning, labor-performance
com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed        -> order-management
com.warehouse.wes.fulfillment-execution.package.PackageManifested -> order-management
com.warehouse.wes.fulfillment-execution.transfer.TransferPicked      -> network-inventory-planning (transfer saga), destination receipt correlation
com.warehouse.wes.fulfillment-execution.transfer.TransferDispatched  -> network-inventory-planning (transfer saga)
com.warehouse.wes.fulfillment-execution.transfer.TransferArrived     -> network-inventory-planning (transfer saga), destination receipt/stow
```

**Published — analytics (`warehouse.fulfillment.analytics`):**

```
com.warehouse.wes.fulfillment-execution.task.TaskCreated
com.warehouse.wes.fulfillment-execution.task.TaskClaimed
com.warehouse.wes.fulfillment-execution.task.LeaseExpired
com.warehouse.wes.fulfillment-execution.task.TaskCompleted
com.warehouse.wes.fulfillment-execution.task.ItemPicked
com.warehouse.wes.fulfillment-execution.package.PackageSealed
com.warehouse.wes.fulfillment-execution.package.WeightDiscrepancyDetected
com.warehouse.wes.fulfillment-execution.package.LabelApplied
com.warehouse.wes.fulfillment-execution.package.PackageDiverted
com.warehouse.wes.fulfillment-execution.package.PackageManifested
```

`ItemArrivedAtRebin` and `OrderConsolidated` (ADR-0016) remain in-process
only and are not published. Should they ever be, their entity segment is
the aggregate that raises them — `orderconsolidation`
(`com.warehouse.wes.fulfillment-execution.orderconsolidation.ItemArrivedAtRebin`
/ `...orderconsolidation.OrderConsolidated`).

**Consumed (dispatch on the FULL type string, never a short name):**

```
com.warehouse.wes.work-planning.workunit.WorkReleased
com.warehouse.wes.process-path-management.processpath.ProcessPathCreated
com.warehouse.wes.process-path-management.processpath.ProcessPathUpdated
com.warehouse.wes.process-path-management.processpath.ProcessPathDeactivated
com.warehouse.wms.product-master.product.ProductClassified                (local classification copy, ADR-0039)
com.warehouse.wes.fulfillment-execution.task.TaskClaimed                  (analytics projector)
com.warehouse.wes.fulfillment-execution.task.TaskCompleted                (analytics projector)
com.warehouse.wes.fulfillment-execution.task.LeaseExpired                 (analytics projector)
com.warehouse.wes.fulfillment-execution.package.WeightDiscrepancyDetected (analytics projector)
com.warehouse.wes.fulfillment-execution.package.PackageManifested         (analytics projector)
```

Versioning: additive payload changes keep the type and dataschema. A
breaking payload change requires a new `dataschema` version **and** a new
`.v2`-suffixed type, published as a new event — never by mutating the old
one.

### 5. Consumer rules

1. Decode with `cloudevents.Decode` (SDK unmarshal + `specversion == 1.0`
   + `Validate()`).
2. A message that is not a valid CloudEvents 1.0 event (bad JSON, missing
   attributes, the retired flat envelope) is a deterministic poison
   message and returns an error wrapping `cloudevents.ErrNotCloudEvent`:
   - the `WorkReleased` consumer dead-letters it to `<topic>.dlq` (its
     existing ADR-0029 DLQ path);
   - the process-path catalogue consumer and the analytics projector log
     it at WARN with topic/partition/offset and commit past it;
   - the `ProductClassified` consumer (ADR-0039) logs it at WARN and commits
     past it, and does the same for a `ProductClassified` whose payload
     breaks the contract.
   It is never retried, never crashes the loop, and never falls back to a
   flat parser.
3. Unknown types are ignored silently (forward compatibility).
4. Deduplication uses the CloudEvents `id` (`processed_events.event_id`
   keeps its column name, now populated from `id`).
5. `time` and `subject` come from the context attributes; the payload from
   `DataAs`.

## Consequences

- **One wire format.** Spec and code agree; `apis/asyncapi.yaml` describes
  exactly what is on each topic, with the exact `type` and `dataschema`
  per message.
- **Breaking, coordinated cutover.** Old flat messages are rejected. Every
  service's PR merges and deploys as one set; before deploying, each
  service's outbox is drained (its rows were pre-encoded flat) and the
  `warehouse.*.events` / `warehouse.*.analytics` topics are recreated and
  the process-path catalogue re-seeded, so no flat message remains for a
  `FirstOffset` replay. See warehouse-infra `docs/cloudevents-cutover.md`.
- **Removed:** `EVENT_ENVELOPE_MODE`, `EnvelopeMode`/`ParseEnvelopeMode`,
  `NewPublisherWithMode`/`NewPublisherWithWriterAndMode`, the hand-rolled
  `CloudEvent[T]`, the flat `Envelope`/`TaskCPTMissedEnvelope`/
  `PackageManifestedEnvelope`/`AnalyticsEnvelope` types, the dual-read
  `decodeEnvelope`/`bareEventType`, short-name dispatch constants, and the
  flat/dual-only tests.
- **Tests:** golden exact-JSON tests per published type on both streams
  (all attributes, type, key and `content-type` header), a
  legacy-flat-rejected test per consumer, and the testcontainers Kafka
  integration tests assert the CloudEvents wire format end to end.
- **New dependencies:** `github.com/cloudevents/sdk-go/v2` (event package
  only) and `github.com/google/uuid` (promoted from indirect) for the
  UUID v4 `id`.
