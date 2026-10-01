# Cross-service integration (additive — Task 7, do NOT touch existing domain code)

> **Historical brief.** This is the original task spec for the first Kafka
> integration and is kept for context. Parts of it are superseded: the
> `path_id`-prefix mapping with a default-to-Pick was replaced by the
> process-path catalogue (ADR-0017 — an unknown `path_id` is now a hard
> error), and the publisher now also emits `TaskCPTMissed` and
> `PackageManifested` (ADR-0025). For the current contract see
> `.claude/rules/api-and-integration.md` or the docs site's
> *Integration contracts* page. The flat `event_id`/`event_type`/
> `occurred_at` envelope this brief originally specified is retired: every
> Kafka message is now a CloudEvents 1.0 event (ADR-0032), shown below.

This service CONSUMES `WorkReleased` from wes-work-planning and turns each one
into a Task via the existing `CreateTask` use case — this IS the intended use
of that use case, so call it directly (no new use case needed). Strictly
additive: new adapter only, no change to existing aggregates/invariants.

## Envelope (CloudEvents 1.0, fleet-wide — ADR-0032)

```json
{
  "specversion": "1.0",
  "id": "6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b",
  "source": "/warehouse/wes-work-planning",
  "type": "com.warehouse.wes.work-planning.workunit.WorkReleased",
  "subject": "wu-8a1f",
  "time": "2026-08-21T22:00:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:wes-work-planning:events:WorkReleased:v1",
  "data": {"path_id": "...", "work_unit_id": "...", "cpt": "RFC3339", "ref": "...", "fragile": false}
}
```

## Kafka

- Client library: `github.com/segmentio/kafka-go`.
- Broker: `KAFKA_BROKERS` env var (default `localhost:9092`). A shared broker
  already runs in the `warehouse-infra` kind cluster (host listener
  `localhost:9092`) — connect to it, do not add your own Kafka service to this repo's docker-compose.yml.
- New inbound package `internal/adapters/inbound/kafka/` — a consumer on topic
  `warehouse.work-planning.events`, dispatching on the full CloudEvents `type`
  `com.warehouse.wes.work-planning.workunit.WorkReleased`.
- Mapping: `WorkReleased.data.path_id` → task type is NOT derivable from
  path_id alone in general, but for this integration assume the path_id string
  itself carries the task type as a prefix convention (e.g. "pick-*" → Pick,
  "pack-*" → Pack, "slam-*" → SLAM); default to Pick if no prefix matches, and
  say so explicitly in the README as a known simplification for this round.
  `WorkReleased.data.work_unit_id` → task's `ref`. `WorkReleased.data.cpt` →
  task's CPT. Required capabilities: derive from task type (e.g. Pick needs
  "pick" capability) matching the capability names Workforce Management uses.
- Consumer calls the existing `CreateTask` use case with these mapped fields.

## Idempotency (required — Kafka is at-least-once)

Add a new Postgres table `processed_events (event_id TEXT PRIMARY KEY,
processed_at TIMESTAMPTZ)` via a new migration. Before calling `CreateTask` for
a consumed message, attempt to insert its CloudEvents `id`; if it already exists,
skip (ack/commit anyway) — do not create a duplicate Task for the same
`WorkReleased` event. In-memory adapter: thread-safe `map[string]struct{}`.
Unit-test: consuming the same `WorkReleased` id twice creates exactly
one Task.

---

## Addendum — `data.fragile` (product-classification handling flags round)

`WorkReleased.data` gained an optional `fragile: bool` field, defaulting to
`false` when absent — the same kind of known simplification as the `path_id`
prefix convention above, and documented the same way in this repo's own
README/INTEGRATION notes. It is sourced from `inventory-storage`'s
`ProductClassification` concept and stamped by `wes-work-planning` at
release time; this service does not call `inventory-storage` directly. It
maps straight onto the created `Task.Fragile` flag with no interpretation,
and `SealPackage` later derives `Package.FragileHandling` from it. See
`docs/docs/adr/0009-fragile-and-hazmat-handling-flags.md`.

Hazmat handling required **no change to this consumer or to `CreateTask`**:
it is a `requiredCapabilities` value (`hazmat`) matched by the pre-existing
generic `CapabilitySet` mechanism.

## Definition of done for Task 7

- New consumer adapter compiles and is unit-tested (feed it a CloudEvent,
  assert exactly one Task created; feed the same id twice, assert still
  exactly one).
- Existing full suite (`go build ./...`, `go vet ./...`, `go test ./...`,
  `go test ./... -race`) still green, unchanged.
- README gains an "Integration" section: topic consumed, exact JSON schema
  above, the path_id-prefix simplification called out explicitly, the
  `KAFKA_BROKERS` env var.
- Do a REAL smoke test: with the shared broker running, publish a
  `WorkReleased`-shaped message via `kafka-console-producer.sh` (or a small Go
  one-off) to `warehouse.work-planning.events` and confirm a new Task appears
  via `GET /queues/{taskType}/depth` before declaring done.

---

## Task 8 — Publish TaskCompleted (additive, do NOT touch existing domain code)

This service now ALSO PUBLISHES `TaskCompleted` to close the control loop back
to Work Planning (the drum-buffer-rope feedback edge: Execution -> Orchestration).
Strictly additive: a new outbound adapter hook, no change to existing
aggregates, invariants, or use cases (including `CompleteTask` itself — do not
alter its logic, only add a publish call using its existing return value).

### Envelope

```json
{
  "specversion": "1.0",
  "id": "6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b",
  "source": "/warehouse/fulfillment-execution",
  "type": "com.warehouse.wes.fulfillment-execution.task.TaskCompleted",
  "subject": "task-8a1f",
  "time": "2026-08-21T22:00:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:fulfillment-execution:events:TaskCompleted:v1",
  "data": {"task_id": "task-8a1f", "station_id": "station-03", "work_unit_id": "wu-8a1f",
           "associate_id": "worker-42", "duration_seconds": 245, "task_type": "PICK"}
}
```

`work_unit_id` is the completed Task's `OrderRef()` — recall from Task 7 that
`WorkReleased.data.work_unit_id` was mapped into the Task's `ref` (OrderRef) at
creation time. So `OrderRef()` on the completed Task IS the original
`work_unit_id` from Work Planning. The domain event `TaskCompleted` itself only
carries `TaskId`/`StationId` (no OrderRef) — so the Kafka publisher adapter
must look the Task back up via the existing `TaskRepo` port to read its
`OrderRef()` before publishing, exactly the same pattern already used in
inventory-storage's Kafka publisher for `ReservationRevoked` (which backfills
sku/quantity via a repo lookup since its domain event is thin too). Do not
change the `TaskCompleted` domain event's fields — enrich only in the adapter.

### Kafka

- Reuse the SAME `internal/adapters/outbound/kafka/` publisher package added
  for other purposes if one exists from a prior round, or create it now if this
  is the first outbound Kafka adapter in this repo — check first, this repo may
  currently only have the Task 7 CONSUMER, not yet a publisher.
- Topic: `warehouse.fulfillment.events`.
- Publish `TaskCompleted` when the existing `CompleteTask` use case succeeds.
  Hook this the same way `EVENT_PUBLISHER` selection already works elsewhere in
  this codebase (log default, kafka opt-in via `EVENT_PUBLISHER=kafka`) — if
  this repo does not yet have that env-driven selection wired into
  `CompleteTask`'s publisher, add it now, consistent with the existing
  `ports.EventPublisher` interface used by other use cases.

Downstream consumer: wes-work-planning calls its existing `RecordCompletion`
use case with `WorkUnitId = data.work_unit_id`.

### Definition of done for Task 8

- New/extended Kafka publisher unit-tested for `TaskCompleted`: asserts the
  envelope shape including the `work_unit_id` enrichment via `TaskRepo` lookup.
- Existing full suite (`go build ./...`, `go vet ./...`, `go test ./...`,
  `go test ./... -race`) still green, unchanged, including Task 7's consumer.
- README's Integration section gains this new topic published, exact schema.
- Do a REAL smoke test: with the shared broker running and `EVENT_PUBLISHER=kafka`,
  create a task, claim it, complete it via the running binary's HTTP API, and
  confirm a `TaskCompleted` message with the correct `work_unit_id` lands on
  `warehouse.fulfillment.events` via `kafka-console-consumer.sh` before
  declaring done.

---

## Task 9 — Register-station HTTP endpoint (gap-fill, NOT integration, additive)

This is unrelated to Kafka/Tasks 7-8. It fills a pre-existing gap found while
smoke-testing the pull-dispatch path end-to-end: the `Station` aggregate and
`ports.StationRepo` already exist and are fully exercised by unit tests, but
there is no way to create a Station over HTTP — so `POST
/stations/{stationId}/claim-next` always fails with "station not found" against
a freshly-started server, and the pull-dispatch flow cannot be smoke-tested via
HTTP alone. Fix: add the missing use case + endpoint, matching every existing
convention in this codebase. Strictly additive — do not touch the `Station`
aggregate, `ClaimNext`, or any other existing use case; this only ADDS a new
one alongside them.

### What already exists (do not recreate)

- `internal/domain/station/station.go`: `station.New(id shared.StationId,
  capabilities shared.CapabilitySet) *Station` — already validates and
  constructs correctly.
- `internal/application/ports/ports.go`: `StationRepo` interface with `Save`
  and `FindById` — already implemented by both the memory and postgres
  adapters (`internal/adapters/outbound/memory/station_repo.go` and the
  postgres equivalent).

### What to add

1. New use case `internal/application/usecases/register_station.go`:
   `RegisterStation` struct (same shape as every other use case in this repo —
   depends on `ports.StationRepo` and `ports.EventPublisher`), with an
   `Execute(ctx, stationId string, capabilities []string) (*station.Station,
   error)` method. It should be idempotent at the use-case level: registering
   the same `stationId` twice should UPDATE the station's capability set (via
   `station.New` + `Save`, which already overwrites in both adapters) rather
   than error — a re-registration is a legitimate operational action (e.g.
   recertifying a station), not a bug. Do NOT add a new domain event unless one
   already fits the existing 9; if none fits, that's fine, this use case simply
   doesn't publish (match whatever pattern `CreateTask` follows for its
   optional publish, but don't force an event that doesn't belong).

2. New HTTP endpoint: `POST /stations` (note: NOT nested under an existing
   path, this creates a new top-level resource) with body
   `{"stationId": "...", "capabilities": ["pick", ...]}`, returning 201 with
   the created/updated station (id + capabilities + occupied:false). Add the
   route to the existing chi router alongside the others, a request/response
   DTO in `dto.go` following the exact naming convention already used there
   (e.g. `registerStationRequestDTO`, `stationResponseDTO`), and wire domain
   errors (e.g. empty capabilities, if that's a validation `station.New`
   already enforces or should enforce — check first, don't invent a new
   invariant if one doesn't already exist) to the existing HTTP error-mapping
   pattern.

3. Wire `RegisterStation` into `cmd/execution/main.go`'s composition root
   alongside the other use cases, using the same `StationRepo` instance
   `ClaimNext` already uses (so a registered station is immediately claimable).

### Tests

- Unit test `RegisterStation` against the in-memory `StationRepo`: registers a
  new station, returns it correctly; re-registering the same id with different
  capabilities updates it (assert via `FindById`); a subsequent `ClaimNext`
  against that station's id succeeds where it previously would have failed
  with `ErrStationNotFound` (proves the two use cases now interoperate).
- httptest coverage for `POST /stations`, following the existing pattern in
  `router_test.go`: happy path (201 + correct body), and whatever validation
  error path already exists on `station.New` (if any) mapped to the correct
  status.

### Definition of done for Task 9

- New use case + endpoint compile and are unit/httptest-covered as above.
- Existing full suite (`go build ./...`, `go vet ./...`, `go test ./...`,
  `go test ./... -race`) still green, unchanged, including Tasks 0-8 — this
  proves the `Station` aggregate and `ClaimNext` were not modified.
- README gains a short note: the new `POST /stations` endpoint added to the
  REST API table, and a one-line mention that this closes the pull-dispatch
  gap for HTTP-only smoke testing.
- REAL smoke test, no shortcuts: with the compiled binary running (in-memory
  adapters are fine, no Postgres/Kafka needed for this task), `POST /stations`
  to register a station with `["pick"]` capabilities, `POST /tasks` to create a
  Pick task, then `POST /stations/{stationId}/claim-next` and confirm it
  actually returns the claimed task (200, not the previous `station not found`
  error) — this is the exact gap that prompted this task, so proving it closes
  is the actual bar for done, not just green tests.
