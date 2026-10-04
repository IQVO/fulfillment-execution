---
id: 0007-godog-bdd-acceptance-tests
title: 7. godog (Gherkin) acceptance tests for the invariants
sidebar_label: 7. godog BDD acceptance tests
sidebar_position: 7
description: Express the load-bearing business rules as black-box Gherkin scenarios driven through the real HTTP surface, alongside — not instead of — unit tests.
---

# 7. godog (Gherkin) acceptance tests for the invariants

## Status

**Accepted.** Added in the commit "Add godog (Cucumber/Gherkin) BDD acceptance
tests, wire bdd CI job," which introduced `features/`, `features_test.go`, and
the blocking `bdd` job in CI.

## Context

By this point the repository already had a strong test suite: table-driven unit
tests over every domain invariant including failing paths, application-layer
tests against in-memory adapters, one `httptest` per endpoint, build-tagged
Postgres integration tests, ~98% coverage on domain plus application, and
mutation testing with `gremlins`.

So the gap was not coverage. It was two other things.

**First, the rules were only expressed in Go.** The load-bearing rules of this
context are business statements: *"the task handed back is the earliest-CPT
pending task whose required capabilities the station holds"*; *"if the lease
expires unconfirmed, the task returns to the pool so work is never silently
lost."* A warehouse operations person can confirm or refute either sentence.
Neither is legible in `TestClaimNext_SelectsEarliestCPT`, where the rule is
implied by fixture values and assertion order rather than stated.

**Second, unit tests verify pieces in isolation.** Each layer was well tested
individually, but "register a station, create two tasks, pull, renew, complete,
and observe the queue depth change correctly at each step" crosses handler,
DTO, use case, aggregate, repository and publisher. A defect in the *seams* —
a DTO field that silently doesn't map, a status code that regressed, a use case
wired to the wrong repository instance — can hide behind green unit tests. The
`RegisterStation` gap was exactly this kind of defect: every component worked,
and the flow was impossible.

Forces:

- The invariants named in `CLAUDE.md`'s definition of done are business rules
  and deserve a business-readable expression.
- Full-flow coverage across the real HTTP surface was missing.
- Lease expiry must be testable without sleeping — a wall-clock acceptance
  test would be slow and flaky, and would be turned off.
- The suite must run in CI on every push, with no infrastructure.
- Adding a second test framework has a real cost and needs to earn it.

## Decision

**We will express the load-bearing business rules as Gherkin scenarios,
executed by [godog](https://github.com/cucumber/godog) — the official Cucumber
implementation for Go — as black-box tests through the real HTTP surface.**

Ten feature files under `features/` (the first four were the original set; six
were added as the API grew):

| File | Covers |
| --- | --- |
| `claim_next.feature` | Pull dispatch: earliest-CPT selection, capability mismatch, at-most-once claiming |
| `lease.feature` | Renewal before expiry; expiry returning a task to the pool |
| `complete_task.feature` | Completing a claimed task; rejecting a non-owner |
| `pack_slam.feature` | Sealing a package; the SLAM weigh-check labelling versus diverting |
| `station_occupancy.feature` | Station check-in/check-out occupancy rules |
| `task_guards.feature` | Task and package lifecycle guards |
| `order_ref_lookup.feature` | Task lookup by order reference |
| `cpt_missed_sweep.feature` | The CPT-missed sweep (ADR-0025) |
| `installed_capacity.feature` | The installed-capacity read model (ADR-0018) |
| `package_read_model.feature` | The package read model (ADR-0033) |

Step definitions live in `features_test.go` at the repository root. The design
of the harness is the substance of this decision:

- **Black box over real HTTP.** Each scenario spins up the actual chi router
  behind an `httptest` server and drives it with real HTTP calls to the
  documented endpoints. Steps never call a use case directly.
- **Real wiring, fake edges.** In-memory repositories, a buffered event
  publisher, and a **fixed `Clock`**. The units under test are the real ones;
  only the outermost adapters are substituted.
- **Fresh state per scenario.** Every scenario gets a new server and new
  repositories — no ordering dependencies, no shared fixtures.
- **The clock is a first-class step.** `When the clock advances by 6 minutes`
  is what makes lease expiry expressible and instantaneous. This is the payoff
  from `ports.Clock` in [ADR-0001](./0001-hexagonal-ports-and-adapters.md);
  without it this decision would not have been viable.
- **Assertions include events and problem types.** Steps like
  `And a "LeaseExpired" domain event is recorded` and
  `And the response is a Problem Details document of type "no-claimable-task"`
  mean the scenarios also pin the event catalogue and the RFC 7807 contract
  from [ADR-0005](./0005-rfc-7807-problem-details.md).

Scenarios are tagged `@bdd` and run in CI as a dedicated blocking **`bdd`
job** — `go test ./... -run TestFeatures -v`.

This is **additive**. No unit test was removed. Gherkin covers the rules a
domain expert would recognise; unit tests continue to cover exhaustive edge
cases, error branches and boundary values, which are not worth writing in
Gherkin.

## Consequences

### Easier

- **The rules are readable by non-developers.** `lease.feature` opens with a
  four-line prose statement of why the lease exists, then proves it. It is
  documentation that fails the build when it becomes untrue.
- **The seams are covered.** Handler → DTO → use case → aggregate → repository
  → publisher is exercised end to end, over real HTTP, in the same wiring the
  binary uses.
- **Lease expiry is tested honestly and instantly.** `advances by 6 minutes`
  runs in microseconds.
- **Regressions in the HTTP contract fail loudly.** A changed status code or a
  renamed problem type breaks a scenario named after the business rule it
  protects.
- **Scenarios double as executable examples.** They show the real call
  sequence a station client must follow.

### Harder

- **Two test vocabularies to maintain.** Contributors need to know which
  belongs where. The working rule: rules a domain expert would recognise go in
  Gherkin; exhaustive edge cases stay in Go.
- **Step definitions accrete.** `features_test.go` is already substantial, and
  step-definition files tend to grow into a private DSL that is itself
  untested. It needs the same care as production code.
- **Gherkin can be over-applied.** It is a poor fit for parameterised boundary
  testing — "tolerance of exactly 0.05" belongs in a table-driven Go test, not
  a scenario.
- **Indirect failures.** A broken step definition fails a scenario with a
  message about a step, not about the assertion that actually matters. The
  debugging loop is longer than a unit test's.
- **Another CI job and another dependency.** `bdd` is a fifth-plus blocking job
  and godog is a real dependency in `go.mod`.
- **Scenarios must track the API.** Because they are black-box over HTTP, an
  intentional endpoint change requires editing feature files — which is
  correct (the business-visible contract changed) but is friction on every
  such change.
