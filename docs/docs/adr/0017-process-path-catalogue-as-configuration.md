---
id: 0017-process-path-catalogue-as-configuration
slug: /adr/0017-process-path-catalogue-as-configuration
title: 0017. Process-path catalogue as configuration, replacing the path_id-prefix guess
sidebar_label: 0017. Path catalogue as config
description: ADR 0017 — load process-path definitions (id, required capabilities) from a shared warehouse-infra YAML file at boot, retiring the WorkReleased consumer's documented path_id-prefix-guessing simplification and its silent default-to-Pick bug.
---

# 0017. Process-path catalogue as configuration, replacing the path_id-prefix guess

## Status

Accepted.

## Context

Since this service's first Kafka integration, the `WorkReleased` consumer
has derived a task type from `WorkReleased.data.path_id` using a
**documented prefix convention**: `pick-*` → `PICK`, `pack-*` → `PACK`,
`slam-*` → `SLAM`, defaulting to `PICK` when no prefix matched. This was
always called out explicitly, in this repository's own README,
`INTEGRATION.md`, and `process-paths.md`, as "a known simplification for
this round of integration, not a durable contract" — `path_id` does not
in general carry the task type, and the `PICK` default in particular
meant a malformed or unrecognized `path_id` silently produced a Pick
task rather than surfacing an error.

Meanwhile, the reference process-path architecture this fleet is modelled
against is explicit about the correct shape of this problem: **"declare
paths; let each building instantiate its own topology."** Process paths
are not this service's private concern — `wes-work-planning` validates
`PathId` on `WorkPool`, and `workforce-management` plans headcount
against the exact same capability vocabulary this service's
`RequiredCapabilities` uses. Three services independently either hardcode
or (in this service's case) *guess* the same set of facts, with no single
declared source of truth and no way to add Feature A's `REBIN` path
without touching code in all three places by hand.

Forces:

- **No single one of the three consuming services should own this
  catalogue.** Making `fulfillment-execution` the source of truth would
  make it a silent upstream dependency for `wes-work-planning` and
  `workforce-management`, and vice versa — none of them has planning
  authority over what paths exist in a building's topology.
- **This is data about a deployment, not business logic.** It answers
  "what does this building's topology look like," the same category of
  question `warehouse-infra`'s own `terraform/locals.tf` `services` map
  already answers for "what services exist in this deployment."
- **A malformed or missing catalogue must be a boot-time failure, never
  a runtime fallback.** The exact opposite of the old prefix-default
  behavior: this service must refuse to start with a broken catalogue
  rather than start and silently mis-route work.
- **The catalogue's first real multi-path test needs Feature A's `REBIN`
  to exist first** — this was the explicit sequencing rationale for
  building this feature after Feature A rather than before it, avoiding
  re-touching the catalogue's schema the moment a fourth path appeared.
- **The type system should still do what it can do well.** `task.Type`
  stays a Go string type for compile-time safety inside a single request
  (nothing about this decision makes `task.Type` a bare `string`) — only
  its *valid values* and *per-path required capabilities* move from
  compiled-in constants/switch-statements to catalogue-validated data.

### Alternatives considered

**Keep the prefix convention, just extend it for REBIN.** Rejected: this
would perpetuate the exact documented bug (unrecognized `path_id` silently
becomes Pick) the reference material's own "declare paths" prescription
exists to fix, and would still leave three services independently
guessing the same facts.

**Make `fulfillment-execution` (or any one service) the catalogue's
owner, with the others calling out to it.** Rejected: adds a runtime
dependency and a network hop for what is fundamentally static
configuration, and gives one bounded context unwarranted authority over
a published-language concept none of the three should individually own.

**A new `path-catalogue` service/bounded context.** Rejected outright,
same reasoning as Feature A's OrderConsolidation and Feature E's WCS
seam: no independent team, deployment cadence, or external API consumer
justifies a new service for what is, in the end, a validated lookup
table.

## Decision

**Load the process-path catalogue from a YAML file published by
`warehouse-infra` (`config/process-paths/sortable-fc.yaml`), validated
into an in-memory `pathcatalog.Catalogue` at process startup. The
`WorkReleased` consumer's `deriveTaskType`/`requiredCapabilities`
prefix-guessing functions are deleted outright and replaced by a real
`Catalogue.Lookup(path_id)` call; an unrecognized `path_id` now returns a
hard error instead of defaulting to `task.Pick`.**

```go
// internal/domain/pathcatalog/path_definition.go
// (sketch as first written; the shipped struct also has MatchPrefix and
// DestinationLocationRole — see "Addendum" below and the current file)
type PathDefinition struct {
	Id                   string
	MatchPrefix          string // added by the prefix-family addendum
	Direct               bool
	RequiredCapabilities []string
	// DestinationLocationRole string — optional, added later
}

type Catalogue struct{ /* ... */ }
func (c *Catalogue) Lookup(id string) (PathDefinition, error) // ErrUnknownPath
```

```go
// internal/adapters/outbound/filecatalog/loader.go
func Load(path string) (*pathcatalog.Catalogue, error)
// fails loud on: unreadable file, invalid YAML, zero declared paths,
// any path with an empty id or zero requiredCapabilities.
```

```go
// internal/adapters/inbound/kafka/consumer.go, HandleMessage
pathDef, err := c.Catalogue.Lookup(env.Data.PathId)
if err != nil {
	return fmt.Errorf("kafka: path_id %q not found in the process-path catalogue: %w", env.Data.PathId, err)
}
taskType := task.Type(pathDef.Id)
required := shared.NewCapabilitySet(capabilitiesOf(pathDef)...)
```

`cmd/execution/main.go` loads the catalogue once, before any adapter
stands up, from `PATH_CATALOGUE_FILE` (default
`/etc/fulfillment-execution/process-paths.yaml`, mounted from
`warehouse-infra`'s published file in-cluster); a load failure is fatal —
the process exits before serving any traffic or consuming any message.

> **Addendum — second catalogue source.** `PATH_CATALOGUE_SOURCE` (default
> `file`, the behaviour described above) can be set to `kafka`: the
> `internal/adapters/outbound/kafkacatalog` adapter then keeps an in-memory
> catalogue up to date from process-path-management's
> `warehouse.process-path-management.events` topic (full replay at boot) and
> satisfies the same `ports.PathCatalogue`. Startup waits (bounded by
> `kafkacatalog.WaitReadyTimeout`) for the initial replay to complete before
> serving, so the "never serve against an incomplete catalogue" guarantee is
> preserved; failing to become ready in time is fatal at boot, and a transient
> Kafka outage after that does not stop the process. The file source remains
> the default.

`wes-work-planning` and `workforce-management` mirror this same port +
loader shape against the identical YAML file, in their own follow-up PRs
— this ADR documents the pattern; each repository's own ADR
cross-references this one rather than duplicating the reasoning.

### What this decision does NOT do

- It does not change `task.Type`'s Go-level shape — it is still a string
  type with named constants (`Pick`, `Pack`, `Rebin`, `Slam`) for
  compile-time convenience inside this codebase. Only the catalogue's
  *content* (what capabilities each path needs, whether a `path_id` is
  even recognized) moves to configuration.
- It does not add a network call or a new bounded context — the
  catalogue is a local file read once at boot, not a live dependency on
  another service.
- It does not change any existing `Task` invariant, `ClaimNext`, or
  `CompleteTask` behavior — only how the `WorkReleased` consumer resolves
  a task type from an incoming `path_id`.

## Consequences

### Easier

- **Adding a new process path (or a new building's topology) is now a
  configuration change, not a code change across three repositories** —
  exactly the reference material's "declare paths; let each building
  instantiate its own topology" prescription.
- **A malformed `path_id` is now loud and traceable** (an error with the
  exact unrecognized value in it), replacing a silent, hard-to-debug
  default that could misroute real warehouse work to the wrong queue.
- **One schema, three consumers, zero forking** — `wes-work-planning` and
  `workforce-management` read the identical file this service does; there
  is no risk of the three developing divergent private copies of "what
  paths exist."

### Harder

- **A fourth external dependency at boot**: this service now refuses to
  start without a readable, valid catalogue file. This is a deliberate
  trade (fail loud beats silently mis-routing), but it is a real new
  failure mode operators must understand — a missing `ConfigMap` mount
  now means a `CrashLoopBackOff`, not a degraded-but-running service.
- **A breaking behavior change for any producer relying on the old
  Pick-default.** Any `WorkReleased` event with a `path_id` outside the
  declared catalogue that previously silently became a Pick task now
  fails the whole message handling. This is the intended fix for a
  documented bug, but it is still a breaking change and is called out
  explicitly here and in the PR description, not buried in a diff.
- **Three repositories must now agree on file layout and deployment
  path** for the same YAML — a coordination cost this fleet accepted
  once already for the shared Kafka envelope shape (ADR-0004) and is
  accepting again here for the same reason: a published language needs
  exactly one schema, wherever it is consumed.

## Verification

Domain layer (`internal/domain/pathcatalog/path_definition_test.go`): 5
tests, including a dedicated "all four declared paths" test (`PICK`,
`PACK`, `REBIN`, `SLAM`) — the "real multi-path test" this feature was
deliberately sequenced after Feature A to make meaningful.

Adapter layer (`internal/adapters/outbound/filecatalog/loader_test.go`):
6 synthetic-fixture tests covering every documented failure mode (missing
file, invalid YAML, zero paths, empty id, zero capabilities) plus a
real-integration test, gated on `WAREHOUSE_INFRA_CATALOGUE_PATH`, that
loads the ACTUAL file `warehouse-infra` publishes — not a mock — proving
the schema the two repositories agree on actually parses.

Consumer layer (`internal/adapters/inbound/kafka/consumer_test.go`): the
old prefix-derivation test is replaced by
`TestHandleMessage_DerivesTaskTypeFromCatalogue` (all four declared
paths resolve correctly via the catalogue) and the new
`TestHandleMessage_UnknownPathId_ReturnsError` — the failing-path test
proving the deliberate breaking fix actually works: an unrecognized
`path_id` now errors and creates zero tasks, rather than silently
defaulting to Pick.

`go test ./... -race` (all packages, including `internal/architecture`'s
hexagonal fitness tests), `golangci-lint run ./...` (0 issues, including
the `gosec` G304 finding on the boot-time file read, suppressed with the
same `//nolint:gosec` justification this fleet already uses for
`wes-work-planning`'s own operator-controlled-path migration reader),
`make check-all`, and `gremlins unleash ./internal/domain/task` (100%
efficacy/coverage, matching the pre-existing baseline this feature does
not touch) all pass.

## Addendum (2026-09-04): exact-match lookup was wrong, fixed to prefix-family matching

The version of this decision that first shipped implemented
`Catalogue.Lookup` as an **exact string match** against a bare canonical
id (`"PICK"`, `"PACK"`, etc.), and the catalogue's own schema comment
claimed this exact string was "the join key across all three services."
That was wrong, and it was a real, caught-before-wider-damage regression:
**no real `path_id` value anywhere in this fleet is the bare canonical
id.** `order-management`'s `shared.DefaultPathId` is the plain lower-case
`"pick"`; the e2e test fixtures and `warehouse-ops-agent`'s seeded
targets use station/zone/scenario-qualified forms like `"pick-zone-a"`,
`"pick-soak"`, and `"pick-t5-imbalance"`. The exact-match version of this
catalogue would have rejected every one of those as `ErrUnknownPath` —
the very failure mode ("silently misroute or reject real, valid work")
this ADR's own Context section identified as the reason to replace the
old prefix-guessing convention, just relocated to a different bug.

The deleted prefix-guessing convention actually had this part right: it
matched a whole *family* of `path_id` values sharing a prefix, not one
literal string per task type. The fix restores that family-matching
behavior, but as **declared configuration** instead of hardcoded Go
`strings.HasPrefix` calls: `PathDefinition` gained a `MatchPrefix` field,
and `Catalogue.Lookup(id)` now does a case-insensitive check of "does
`id` equal `MatchPrefix`, or does it start with `MatchPrefix + "-"`" —
never a bare substring match (a hypothetical `"picking-station"` must
NOT match the `pick` family). The warehouse-infra catalogue file gained
a `matchPrefix: pick` (etc.) field per path alongside the existing `id`.

This was caught by adding a dedicated regression test
(`TestCatalogue_Lookup_RealFleetPathIdVariants`,
`TestHandleMessage_ResolvesRealFleetPathIdVariants`) that exercises the
fleet's actual observed `path_id` forms rather than only the synthetic
literal-match fixtures the original tests used — the gap in the original
test suite that let this ship. Every consuming repo's own mirror of this
port must use the same `MatchPrefix` semantics, not a bare exact match.
