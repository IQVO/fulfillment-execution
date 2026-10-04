---
name: how-to-test
description: Write or review tests and diagnose a failing coverage, mutation, bdd or integration CI job: the four test layers, the 90% gate, gremlins threshold semantics, the testcontainers rule. Use when adding tests, killing a surviving mutant, or fixing a red check.
---

# How to test

Use when writing or reviewing tests in this repo, or diagnosing a failing
`test`/`mutation-fast`/`bdd`/`integration` CI job. This fleet's quality
bar is layered — passing `go test` is necessary but is the WEAKEST signal
of the four; mutation testing exists specifically because green tests can
assert nothing.

## The four layers, in order of what they actually prove

1. **Unit tests** (`go test ./...`) — prove the code runs without
   panicking and returns SOMETHING. Table-driven, in-memory adapters only
   (`internal/adapters/outbound/memory/`), never a real network/DB call.
2. **Coverage** (CI's `test` job, `go test -coverpkg=./internal/domain/...,./internal/application/...`,
   90% gate) — proves lines executed. Proves nothing about whether the
   test asserted the right thing.
3. **Mutation testing** (`make mutation-fast`, gremlins) — proves the
   tests actually ASSERT, not merely execute. A mutant is a deliberately
   broken version of the code (`<` -> `<=`, `+` -> `-`, etc.); if the test
   suite still passes against the mutant, it "survived" (LIVED) — meaning
   no test would catch that exact bug in production. This is the sensor
   most worth understanding deeply; the pitfalls below are all about it.
4. **BDD / behaviour** (`go test ./... -run TestFeatures -v`, godog) —
   proves the use case works end-to-end through the real HTTP surface,
   not through a mocked port. See `features/claim_next.feature`,
   `features/pack_slam.feature`, `features/complete_task.feature`,
   `features/lease.feature` for this repo's real scenario shape.

## Mutation testing: `<=` fails, not `>=`

`.gremlins.yaml` sets `efficacy`/`mutant-coverage` thresholds under
`unleash.threshold` (currently both `90.0` — read the comment block at
the top of this repo's `.gremlins.yaml` for exactly when and against what
baseline that number was set: `./internal/domain/task` measured 15/15
killed, 100% efficacy; `./internal/domain` overall measured 29/31 killed,
93.55% efficacy, with the floor set to 90 to sit at-or-below both
measurements without starting red). When you deliberately lower coverage
of a package (rare, but happens when removing dead code), you may need to
lower the threshold in the SAME PR with a dated comment explaining why —
never silently; a future reader needs to know the drop was intentional,
not a regression that slipped through.

## Three real pitfalls that have each cost a real CI failure in this fleet

### 1. Zero/origin-value fixtures hide arithmetic mutants

A test built around zero-valued operands makes `a - b` and `a + b`
produce the same result, so a mutant flipping `-` to `+` survives even
though coverage looks complete. This repo's own `Package.Weigh`
(`internal/domain/package/package.go`) is a live example of the kind of
code this bites: `deviation := expectedWeight - actualWeight` then
`if deviation < 0 { deviation = -deviation }`. A test fixture using
`expectedWeight == actualWeight == 0` would pass every assertion while
hiding a `-` → `+` mutant entirely — any new SLAM/weigh-check test must
use distinct, non-zero expected/actual weights and assert the exact
computed deviation, not just "no error".

### 2. Boundary guards need the boundary value itself

`Package.Weigh`'s own `if deviation > WeightTolerance` guard
(`WeightTolerance = 0.05`) is exactly the shape this pitfall describes: a
test trying only a wildly-over-tolerance deviation and a wildly-under one
never exercises `deviation == WeightTolerance` exactly — so a
`CONDITIONALS_BOUNDARY` mutant rewriting `>` to `>=` survives silently.
Every `< 0`/`> 0`/`<= 0`/`> WeightTolerance` guard needs an explicit test
for the boundary value itself (here: a deviation of exactly `0.05` must
NOT divert the package, since the guard is strictly `>`).

### 3. Tie-break / near-equivalent mutants: know when NOT to chase them

A shortest-path relaxation or a priority queue's `Less` has a `<` -> `<=`
mutant that is undetectable by ANY test whose values are all distinct —
the mutation only diverges on an exact tie. Do NOT force an artificial
tied fixture just to kill this; that pins an arbitrary, currently-
unspecified tie-break order as if it were a real invariant, which is
worse than an accepted near-equivalent survivor. `MUTATION.md` in this
repo documents that, as of the last measured run, this repo has ZERO
survived mutants across `internal/domain` (31/31 killed, 100% efficacy)
— if a future domain-layer change introduces one, triage it there per
`MUTATION.md`'s own instructions: add a test if it reveals an untested
behavior, or document why it's equivalent/not worth chasing if not.

## Diagnosing a `mutation-fast` CI failure: diff against develop, don't chase every LIVED line

```bash
gremlins unleash ./internal/domain/task          # on your branch
git stash && git checkout origin/develop -- . && gremlins unleash ./internal/domain/task   # baseline
```

Only entries NEW on your branch are your regression. Confirming the
survivor SET is unchanged from `origin/develop` (currently empty for this
repo, per `MUTATION.md`), not just that the percentage cleared the
`.gremlins.yaml` gate, is the real proof a fix didn't just get lucky on
the threshold.

## Kafka/Postgres integration tests: testcontainers, never a skip-gate

A `-tags=integration` test touching Kafka or Postgres MUST start its own
container via `testcontainers-go`. Never gate on `os.Getenv("KAFKA_BROKERS")`
+ `t.Skip(...)`, and never hardcode `localhost:9092`. This repo's CI
`integration` job (`.github/workflows/ci.yml`) provisions Postgres ONLY
(no Kafka service) — a skip-gated Kafka test silently skips in CI and
proves nothing there, while testcontainers actually exercises the
assertions on the runner. This repo already carries two working
recipes to copy from rather than re-deriving:
`internal/adapters/outbound/kafkacatalog/consumer_integration_test.go`
(Kafka, via `testcontainers-go/modules/kafka`, `confluentinc/confluent-local:7.6.1`)
and `internal/adapters/outbound/postgres/outbox_integration_test.go`
(Postgres, via `testcontainers-go/modules/postgres`). This repo's own
`internal/architecture/fitness_test.go` (`TestKafkaIntegrationTestsUseTestcontainers`)
statically fails CI on a Kafka-touching integration test that gates on
`KAFKA_BROKERS` or hardcodes `localhost:9092` instead.

## Verify before opening the PR

```bash
make check-all   # check + coverage + arch-test + bdd (the full local gate)
```

CI additionally runs `mutation-fast` and `vuln` (govulncheck) on every
push/PR even though they're outside `make check-all` locally — run them
explicitly too (`make mutation-fast`, `make vuln`) before pushing, or a
PR that passes your local gate can still go red in CI.
