---
paths:
  - "**/*_test.go"
  - "features/**"
  - "Makefile"
  - "docker-compose*"
  - ".golangci.yml"
  - ".gremlins.yaml"
---

# Testing, Code Standards & Local Run

## Code standards

- Go 1.26, modules; chi (`go-chi/chi/v5`); pgx/v5 + pgxpool; golang-migrate.
- Config via env (`DATABASE_URL`, `HTTP_ADDR`, `ANALYTICS_DATABASE_URL`, mode
  flags like `PRODUCT_CLASSIFICATION_MODE=http|permissive`).
- gofmt/go vet clean; every package has a doc comment.

## Testing

- Table-driven tests: domain + application (in-memory adapter); one httptest
  per endpoint; build-tagged Postgres integration test (`-tags=integration`,
  skipped without `DATABASE_URL`). Kafka integration tests use testcontainers,
  NEVER a shared external broker.
- Definition of done: `go build ./...`, `go vet ./...`, `go test ./...` green;
  README run steps current; failing-path tests exist for at-most-once claim,
  capability-mismatch rejection, lease-expiry, SLAM weight-diversion, and
  package segregation rejection.

## Extra make targets

```bash
make vuln           # govulncheck; run when touching go.mod/go.sum
make mutation-fast  # blocking gremlins subset; thresholds in .gremlins.yaml
make integration    # needs DATABASE_URL + running Postgres; excluded from check
lefthook install    # one-time: activates pre-commit/pre-push hooks
```

## Run locally

```bash
docker-compose up -d          # local Postgres 16 (this repo)
# Shared Kafka: warehouse-infra kind cluster, host listener localhost:9092
export PATH_CATALOGUE_FILE=~/warehouse-systems/warehouse-infra/config/process-paths/sortable-fc.yaml
go run ./cmd/execution         # OLTP API (fatal at boot without a catalogue file)
go run ./cmd/fulfillment-projector  # analytics writer
go run ./cmd/fulfillment-reports    # analytics reader API
go run ./cmd/mcp                    # MCP server
```
