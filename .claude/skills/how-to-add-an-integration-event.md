# How to add an integration event (publish and consume)

Use when asked to publish a new cross-context integration event, or
consume one from a sibling bounded context. This fleet's Kafka is ONE
broker platform-wide — every design decision below exists because that
shared-broker reality has already caused a real incident once
(wes-work-planning#67).

## Publishing a new integration event

### 1. Is it actually cross-service?

Not every domain event this service raises belongs on the wire. Check
`internal/adapters/outbound/kafka/publisher.go`'s doc comment — this
service forwards only `TaskCompleted`, `TaskCPTMissed` and
`PackageManifested` on the integration topic; the rest go only to the
internal analytics topic (ADR-0012), not to sibling contexts. Before adding a new event to the
Kafka publisher, confirm a sibling context genuinely needs to react to
it — `TaskCompleted` is forwarded because wes-work-planning calls
`RecordCompletion(WorkUnitId)` off it and labor-performance consumes its
`AssociateId`/`DurationSeconds`/`TaskType` fields (ADR-0014, ADR-0023).

### 2. Envelope: CloudEvents 1.0, structured mode — mandatory (ADR-0032)

Every message is a CloudEvents 1.0 event, built ONLY through
`internal/adapters/kafka/cloudevents` (`cloudevents.New(Spec)` — the official
`sdk-go/v2/event` package under the hood). Never hand-roll an envelope
struct, never add a flat/dual mode or an envelope env var:

```json
{
  "specversion": "1.0",
  "id": "<uuid v4, minted once in Encode>",
  "source": "/warehouse/fulfillment-execution",
  "type": "com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
  "subject": "<aggregate id == Kafka key>",
  "time": "<occurred-at, RFC3339 UTC>",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:fulfillment-execution:events:TaskCompleted:v1",
  "data": { "task_id": "...", "station_id": "...", "work_unit_id": "...",
            "associate_id": "...", "duration_seconds": 123, "task_type": "PICK" }
}
```

`type` = `com.warehouse.wes.fulfillment-execution.<entity>.<EventName>`,
where `<entity>` is the raising aggregate (`task`, `package`; already
catalogued in `apis/asyncapi.yaml`). Analytics events use the same `type`
with `dataschema` `...:analytics:<EventName>:v1`. A breaking payload change
is a new `.v2` type plus a new dataschema version. Every produced message
also carries `cloudevents.ContentTypeHeader()`
(`content-type: application/cloudevents+json; charset=UTF-8`).

### 3. Implementation

The event struct should already exist as a domain event the aggregate
raises (`internal/domain/shared/events.go`'s `TaskCompleted`) —
publishing wires an EXISTING domain event onto Kafka, it doesn't invent a
new payload shape at the adapter layer. In the Kafka publisher adapter
(`internal/adapters/outbound/kafka/publisher.go`):

- Add the event's case inside `Encode` (a type switch on
  `shared.DomainEvent`) that builds the payload struct and calls
  `p.encodeIntegration(entity, "<EventName>", subject, occurredAt, data)` —
  which mints the `id` and wraps the payload with `cloudevents.New`
- Enrich via repo lookups where the domain event itself doesn't carry
  enough (this service resolves `OrderRef`, `AssociateId`, and
  `DurationSeconds` via `Tasks.FindById`/`Stations.FindById` at encode
  time — see the doc comment crediting the same
  repo-lookup-enrichment pattern inventory-storage's Kafka publisher
  uses)
- Give the message a partition key that keeps ordering where it matters
  — this service uses the task id (`Key: []byte(tc.TaskId)`)
- Use `Topic` — this service's own topic constant
  (`warehouse.fulfillment.events`), never a sibling's

### 4. Contract + docs

- Add the message to `apis/asyncapi.yaml` under this service's channel,
  matching the entity-grouping convention already there (grouped by
  aggregate — `task`/`package` — not chronologically). CI's `api-lint`
  job runs Spectral against this file
  (`spectral lint apis/asyncapi.yaml --ruleset .spectral.asyncapi.yaml`),
  so a schema mistake fails the PR immediately, not silently.
- Unlike the REST reference (which `docs-api-drift` regenerates
  mechanically from `apis/openapi.yaml`), this repo's events page,
  `docs/docs/api-reference/events.md`, is **hand-authored** from
  `apis/asyncapi.yaml` — its own front matter says so explicitly
  ("This page is hand-authored from that file — everything below is
  traceable to it or to the adapter code"). There is no
  `gen-async-docs`-style generator here; update that markdown file by
  hand to match the new message, and run `cd docs && npm run build`
  (`onBrokenLinks`/`onBrokenAnchors` both `throw`) to catch a broken
  cross-reference before opening the PR.

### 5. Test

Add a golden exact-JSON test for the new `type` in
`internal/adapters/outbound/kafka/golden_test.go` (every attribute, key and
`content-type` header) and unit-test the payload via `decodeCE` against a
fake `Writer` (see `publisher_test.go` — never a real broker in a unit test). If this event
now needs a `_integration_test.go` asserting real delivery, it MUST use
testcontainers — see `internal/architecture/fitness_test.go`'s
`TestKafkaIntegrationTestsUseTestcontainers`, which statically fails CI
on a skip-gated `KAFKA_BROKERS` test or a hardcoded `localhost:9092`.
This repo's own working recipe already exists at
`internal/adapters/outbound/kafkacatalog/consumer_integration_test.go`
(`tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", ...)`).

## Consuming an integration event from a sibling context

### 1. Never import the sibling's Go packages

This service knows a sibling's topic name, its exact CloudEvents `type`
strings, and its payload shape ONLY — never its Go types. Decode with
`cloudevents.Decode`, dispatch on the FULL `type` constant (e.g.
`inboundkafka.TypeWorkReleased`), read the payload with `e.DataAs`, dedupe on
`e.ID()`, and treat `cloudevents.ErrNotCloudEvent` as a deterministic poison
message (DLQ if the consumer has one, else WARN + skip). Add a
legacy-flat-message-rejected test. `internal/adapters/inbound/kafka/consumer.go`'s own
`WorkReleasedData` struct hand-mirrors wes-work-planning's published
`WorkReleased` payload locally rather than importing that repo's Go
module. Do the same for any new consumer.

### 2. Choose the right consumer-group pattern — this is the part that bites

Two DIFFERENT correct patterns exist in this very repo. Picking the
wrong one is THE most common integration-event mistake in this fleet,
and it was learned from a real incident (wes-work-planning#67, described
in `internal/architecture/fitness_test.go`'s
`TestKafkaConsumerGroupNeverHardcodedInline` doc comment).

**Pattern A — long-lived, configurable single-instance consumer group.**
Use when exactly ONE instance of this consumer ever runs at a time. This
service's own OLTP `WorkReleased` consumer is the reference: the group
id is env-configurable, never a bare literal —
`cmd/execution/main.go`:

```go
consumerGroup := getenv("WORK_RELEASED_CONSUMER_GROUP", "fulfillment-execution")
consumer := inboundkafka.NewConsumerWithGroup(kafkaBrokers, workReleasedTopic, consumerGroup, createTask, processedEvents, catalogue, logger)
```

`getenv(...)`'s default (`"fulfillment-execution"`) is fine for a single
long-lived Deployment, but a locally-run e2e-tests harness process MUST
override `WORK_RELEASED_CONSUMER_GROUP` to a unique per-run value, or it
joins the SAME group as the live in-cluster pod and Kafka's rebalance
protocol starves one of the two members silently — the exact incident
this pattern exists to prevent.

**Pattern B — named single-instance constant.** Use when this consumer's
job is a local analytics/read-model projection where exactly one instance
is ever expected and env-configurability adds no value — this service's
analytics projector consumer uses a plain named constant
(`AnalyticsConsumerGroup`), reused across restarts on purpose, because
Kafka's committed-offset resume semantics are exactly what a single
long-lived projector wants.

**Never do this**: a bare inline string literal
(`GroupID: "some-literal"`). `internal/architecture/fitness_test.go`'s
`TestKafkaConsumerGroupNeverHardcodedInline` statically greps every `.go`
file in this repo for `GroupID:\s*"[^"]+"` and fails CI on a match — a
group id must always trace back to a named symbol (const, var, env
lookup, or function call), never a literal a reviewer can't reason
about.

### 3. Readiness gate, if this consumer backs a local cache

If the consumer replays a topic's full history to build a cache other
code depends on, expose a `Ready()` gate the health check consults, and
block readiness (not process startup — a transient Kafka outage
shouldn't be fatal) until the initial replay finishes. A readiness check
that only re-evaluates on a NEW message arriving deadlocks forever on an
ordinary restart where a shared/already-caught-up group never gets a new
message to trigger it — use `OffsetFetch` against the group's committed
offset, or (simpler and less bug-prone) just use the per-process-unique
group pattern, which sidesteps the whole class of bug. This service's own
`WorkReleased` consumer instead relies on `ports.ProcessedEvents.MarkProcessed`
for exactly-once application (idempotency by the CloudEvents `id`, not
readiness gating) — see `HandleMessage`'s
`isNew, err := c.Processed.MarkProcessed(ctx, env.EventId)` check before
creating a Task.

## Verify before opening the PR

```bash
make check-all    # includes arch-test — will catch a sibling-package import
                   # AND a hardcoded/inline GroupID literal
```

Prove any new consumer/publisher behavior actually matters by running
its testcontainers-based integration test against a real broker, not
just the unit test against a fake `Writer`/`Reader`.
