---
name: how-to-add-a-rest-endpoint
description: Add or change a REST endpoint in this service in the fleet's hexagonal order (domain invariant, use case, port, HTTP adapter, apis/openapi.yaml, generated docs, godog scenario). Use when touching internal/adapters/inbound/http, apis/openapi.yaml, or exposing a use case over HTTP.
---

# How to add a REST endpoint

Use when asked to add a new REST use case/endpoint to this service. Follow
this order — domain first, adapter last — never the reverse; writing the
HTTP handler before the domain invariant it enforces produces handlers
that validate nothing and use cases that get bypassed.

This walks the exact path `POST /stations/{stationId}/claim-next` took
(`internal/application/usecases/claim_next.go` +
`internal/adapters/inbound/http/handlers.go`'s `PostClaimNext`) as the
concrete worked example — read those two files alongside this guide.

## 1. Domain first: does an invariant already exist, or do you need one?

Check `internal/domain/<aggregate>/` for the rule this endpoint enforces.
A REST endpoint should almost never contain business logic itself — it
decodes a request, calls a use case, encodes the result. `claim-next`'s
real invariant lives on `task.Task.Claim` (at-most-once assignment via a
lease, capability-match rejection) — the handler and use case never
reimplement that check, they just call it. If the operation needs a new
domain rule, add it to the aggregate/value-object in `internal/domain/`,
with its own table-driven unit test, BEFORE touching the application or
adapter layers.

## 2. Application: define the use case

Add a new file in `internal/application/usecases/` (one file per use
case, this repo's convention — not one giant `usecases.go`). Shape,
mirroring `ClaimNext`:

```go
package usecases

// ClaimNext implements PULL dispatch: a station calls claimNext and the
// system selects the highest-priority (earliest CPT) pending task the
// station is certified/equipped for. The station is never named in advance.
type ClaimNext struct {
    Tasks         ports.TaskRepo      // driven ports only — never a concrete adapter
    Stations      ports.StationRepo
    Publisher     ports.EventPublisher
    Clock         ports.Clock         // "now" always comes from here, never time.Now()
    LeaseDuration time.Duration
    Metrics       ports.Metrics       // optional: nil means uninstrumented
    UnitOfWork    ports.UnitOfWork    // optional: nil runs Save+Publish back to back (ADR-0020)
}

func (uc *ClaimNext) Execute(ctx context.Context, stationId shared.StationId, taskType task.Type) (*task.Task, error) {
    // 1. load aggregate(s) via the port (Stations.FindById, Tasks.FindClaimableByType)
    // 2. call the aggregate's own method to apply the rule (t.Claim(...) —
    //    never inline the invariant here; that belongs in internal/domain/)
    // 3. persist + publish atomically via atomically(ctx, uc.UnitOfWork, ...)
    // 4. return the result
}
```

Add the port to `internal/application/ports/` if it doesn't exist yet —
ports are interfaces ONLY. This repo's `internal/architecture/` fitness
tests enforce that statically; a struct or function in a ports package
fails `arch-test` in CI.

Write the use case's unit test against the in-memory adapter
(`internal/adapters/outbound/memory/`) — never a real Postgres/HTTP call
in a unit test. Cover the success path AND the domain-rule failure path
(e.g. `ClaimNext` against a station whose capabilities don't match any
pending task, and against a task another station already holds a live
lease on).

## 3. Adapter: wire the HTTP handler

In `internal/adapters/inbound/http/`:

1. `dto.go` — add the request/response DTO structs (JSON tags, this
   repo's naming convention: `<verb><noun>Request`/`<verb><noun>Response`
   — see `claimNextRequest`/`taskResponse`). DTOs live ONLY in the
   adapter layer — domain types never carry JSON tags.
2. `router.go` — add the route
   (`r.Post("/stations/{stationId}/claim-next", h.PostClaimNext)`).
3. `handlers.go` — add the handler function:
   - decode + validate the request (`json.NewDecoder(r.Body).Decode`,
     then `req.validate()`), converting to domain value objects
     immediately (`shared.StationId(stationId)`, `task.Type(req.TaskType)`)
     — a bad value fails here as a 400, never reaches the use case
   - call the use case's `Execute`
   - map use-case errors to HTTP status via `writeError` (check
     `errors.go` for the existing error→status mapping before adding a
     new error type)
   - encode the domain result back to the response DTO and `writeJSON`
     (`toTaskResponse(t)`)
4. Add the new use case field to the `Handlers` struct and wire it in the
   composition root (`cmd/execution/main.go`'s handler assembly, alongside
   `ClaimNext: &usecases.ClaimNext{Tasks: taskRepo, Stations: stationRepo, ...}`).

Write at least one httptest per endpoint: one success path, one error
path (validation failure AND/OR the domain-rule failure, whichever this
endpoint can produce) — see `handlers_test.go`'s claim-next cases (happy
path, capability mismatch → 409, double-claim → 409).

## 4. Contract: update OpenAPI, then regenerate docs

Add the path to `apis/openapi.yaml` (request/response schemas, the RFC
7807 problem-detail response for each error case — see the existing
`/stations/{stationId}/claim-next` entry for the shape).

Regenerate the Docusaurus REST reference — this repo's `docs-api-drift`
CI job fails the PR if you skip this:

```bash
cd docs
npm run clean-api-docs fulfillment
npm run gen-api-docs fulfillment
```

## 5. Behaviour: add a godog scenario

If this endpoint is user-facing behaviour (not purely internal
plumbing), add a `.feature` file under `features/` exercising it
end-to-end against the real HTTP server — see `features/claim_next.feature`
for the exact shape this repo's `bdd` CI job expects (Given/When/Then over
real HTTP, not mocked; e.g. the mismatched-capability and double-claim
scenarios alongside the happy path).

## 6. Verify before opening the PR

```bash
make check       # fmt-check vet build lint test
make check-all   # + coverage (90% gate) + arch-test + bdd
```

`make coverage`/CI's `test` job gates
`./internal/domain/...,./internal/application/...` at 90% — a new use
case with no test on its failure path is the most common way to miss
this gate.
