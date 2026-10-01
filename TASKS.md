# Build Tasks — Fulfillment Execution

Build the full bounded context described in CLAUDE.md, in order. Keep
`go build ./...` and `go test ./...` green throughout. Read
/Users/claudioed/docs/amazon-fulfillment-ddd.md for the domain model first.

## Task 0 — Skeleton
- `go mod init github.com/claudioed/fulfillment-execution`; create the layout;
  .gitignore (bin/, .env); add chi + pgx deps.

## Task 1 — Domain (pure Go)
- shared: TaskId, StationId, CPT, Capability value objects; DomainEvent + 9 events.
- task: Task aggregate (Pick|Pack|SLAM; Pending->Claimed(lease)->Completed;
  at-most-once claim; capability match; lease expiry frees task).
- station: Station aggregate (one occupant; capability set).
- package: Package aggregate (seal needs scanned contents; SLAM weight tolerance).
- Unit tests for EVERY invariant incl. failing paths.

## Task 2 — Application
- ports: TaskRepo, StationRepo, PackageRepo, EventPublisher, Clock.
- usecases: the 8 use cases from CLAUDE.md. PULL dispatch: ClaimNext selects the
  earliest-CPT pending task matching capabilities and leases it. Depend only on
  domain + ports.
- Unit-test against in-memory adapter, including lease-expiry via a FixedClock.

## Task 3 — Outbound adapters
- memory: thread-safe impls of every port.
- postgres: pgxpool repos + migrations (task, station, package, events);
  build-tagged integration test (skip w/o DATABASE_URL).
- events: log/buffered publisher, kafka-ready interface.

## Task 4 — Inbound HTTP
- chi router + handlers for every endpoint, DTOs, domain-error->HTTP mapping, /healthz.
- httptest per endpoint against in-memory repos.

## Task 5 — Composition & ops
- cmd/execution/main.go wires env -> adapters -> use cases -> router.
- docker-compose.yml (pg16); README.md with run steps + curl examples.

## Task 6 — Verify
- build, vet, test (and `-race`) all green; gofmt clean. Confirm the four named
  invariants each have a red-path test. Do not stop until DoD in CLAUDE.md is met.

## Task 7 — Cross-service integration (additive, see CLAUDE.md's new section)
- Add `github.com/segmentio/kafka-go` dependency.
- New Kafka inbound consumer on warehouse.work-planning.events, filtering
  WorkReleased (today: the full CloudEvents type
  com.warehouse.wes.work-planning.workunit.WorkReleased, ADR-0032), mapping into a CreateTask call (see CLAUDE.md for
  the path_id-prefix task-type derivation simplification — document it).
- Idempotency via a new processed_events table (Postgres migration) / map
  (memory); unit test double-delivery creates exactly one Task.
- README gains an Integration section. REAL smoke test: with the shared broker
  running (docker-compose.kafka.yml in ~/warehouse-systems), publish a
  WorkReleased message and confirm a Task appears via the queue-depth endpoint.
- Full existing suite (build/vet/test/-race) must still be green afterward.

## Task 8 — Publish TaskCompleted (additive, see INTEGRATION.md's Task 8 section)
- Add/reuse a Kafka outbound publisher adapter (check if Task 7 added a
  consumer-only package; this may be the first outbound publisher in this repo).
- Publish TaskCompleted to warehouse.fulfillment.events when CompleteTask
  succeeds, enriching with OrderRef via a TaskRepo lookup (the domain event
  itself only has TaskId/StationId).
- Unit test the message shape including the OrderRef enrichment (today a
  CloudEvents 1.0 event, ADR-0032).
- README's Integration section gains this new topic. REAL smoke test: create,
  claim, complete a task over the running binary's HTTP API with
  EVENT_PUBLISHER=kafka and confirm the message lands on the topic.
- Full existing suite (build/vet/test/-race), including Task 7, must stay green.

## Task 9 — Register-station HTTP endpoint (gap-fill, see INTEGRATION.md's Task 9)
- Not Kafka-related. Fills a gap found during Task 8 smoke-testing: no way to
  create a Station over HTTP, so claim-next always 404s on a fresh server.
- New RegisterStation use case (idempotent: re-registering updates capabilities)
  using the EXISTING Station aggregate and StationRepo port unchanged.
- New POST /stations endpoint, DTOs, wired into main.go alongside ClaimNext's
  existing StationRepo instance.
- Unit test + httptest coverage, including proving a subsequent ClaimNext call
  against a freshly registered station now succeeds.
- README gains the new endpoint in its REST API table.
- Full existing suite (build/vet/test/-race), including Tasks 0-8, must stay
  green — Station aggregate and ClaimNext must be provably unmodified.
- REAL smoke test is the actual bar: POST /stations, POST /tasks, POST
  /stations/{id}/claim-next must return 200 with the claimed task, not the
  previous "station not found" error.

## Task 10 — Quality engineering: linting, coverage, integration tests, mutation tests, CI
Full spec in QUALITY.md at the repo root. Five ordered stages, each gates the
next: (1) golangci-lint clean via the committed .golangci.yml, (2) unit test
coverage >= 90% on internal/domain/... + internal/application/... combined,
(3) real integration tests against live Postgres for every outbound Postgres
adapter, (4) gremlins mutation testing on internal/domain/... only
(exploratory, triaged not gated), (5) .github/workflows/ci.yml — lint+unit+
integration blocking on every push/PR, mutation testing on a weekly schedule/
manual dispatch only, never blocking PRs. Do not stop until every stage's
Definition of Done in QUALITY.md is met, then report the final numbers.

## Task 11 — REST API hardening + OpenAPI 3.0.3 docs + Spectral CI gate
Full spec in REST_API_TASK.md at the repo root. Four ordered stages: (1) audit
this service's HTTP adapter against REST/HTTP Level 2 maturity and fix real
violations (resource nouns, correct verbs/status codes, Location headers,
input validation), (2) migrate all error responses from the bespoke
{"error":...} shape to RFC 7807 application/problem+json, (3) write a very
detailed openapi.yaml (3.0.3) covering every route with full request/response
schemas and real domain-grounded examples, (4) add a new openapi-lint job to
the existing .github/workflows/ci.yml using Spectral, blocking on every
push/PR. Do not stop until every stage's Definition of Done in
REST_API_TASK.md is met, then report the final numbers.

## Task 12 — CI workflow restructure (user-provided template)
Full spec in CI_RESTRUCTURE_TASK.md at the repo root. Rewrite
.github/workflows/ci.yml to the given 4-job structure (lint, test,
integration, mutation) plus top-level permissions/concurrency/defaults,
while preserving Task 11's openapi-lint job as a 5th job. Requires adapting
placeholders to this repo's real values (postgres creds, DATABASE_URL,
whether integration tests self-migrate, current gremlins version), not
copy-pasting blindly. Every job's commands must be verified locally against
this repo before pushing, and the real GitHub Actions run must be confirmed
green via gh run watch. Do not stop until every requirement in
CI_RESTRUCTURE_TASK.md is met.

## Task 13 — Docker Hub publish job in CI
Added a `docker-publish` job to .github/workflows/ci.yml, gated on
`needs: [lint, test, integration, openapi-lint]` and
`if: github.event_name == 'push' && github.ref == 'refs/heads/main'` (never
runs on PRs, only after all quality gates pass on a real push to main).
Builds and pushes the existing repo-root Dockerfile to Docker Hub under the
`claudioed` namespace, tagged `latest` + short git SHA, linux/amd64 only,
using GitHub Actions cache (type=gha) to speed up rebuilds. Requires the
repo to have `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` secrets configured
(user-provided, not committed anywhere).

## Task 14 — Architecture fitness tests (ArchUnit equivalent: arch-go)
Full spec in ARCH_TEST_TASK.md at the repo root. Add
internal/architecture/architecture_test.go using github.com/arch-go/arch-go
to encode the hexagonal dependency rule as executable Go tests (domain
depends on nothing internal, application depends only on domain, inbound and
outbound adapters never depend on each other, only cmd/ wires everything).
New arch-test CI job, added to docker-publish's needs list. Strictly
additive -- do not touch existing production code; if a real architecture
violation is found, report it explicitly rather than silently working around
it. Do not stop until every requirement in ARCH_TEST_TASK.md is met.

## Task 15 — Helm chart lint CI job
Added a `helm-lint` job to .github/workflows/ci.yml using
helm/chart-testing-action@v2.8.0 (`ct lint`) against
charts/fulfillment-execution. Runs on every push/PR (no gating condition, always
blocking), and wired into `docker-publish`'s `needs` list alongside the
existing gates -- a chart that fails helm/Chart.yaml validation or YAML
style rules never reaches Docker Hub either. Verified locally with
`ct lint --charts charts/fulfillment-execution --validate-maintainers=false
--check-version-increment=false` before pushing (chart-testing v3.14.0,
yamllint installed via Homebrew).
