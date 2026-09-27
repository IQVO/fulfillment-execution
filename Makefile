# Fulfillment Execution — local quality gate.
#
# These targets mirror the sensors in .github/workflows/ci.yml so the same
# feedback is available locally, before a commit, instead of only post-push.
# `make check` is the fast self-correction loop; `make check-all` is the
# fuller gate to run before pushing.

GOLANGCI_LINT_VERSION := v2.13.1
COVERAGE_THRESHOLD    := 90
COVERPKG              := ./internal/domain/...,./internal/application/...
MUTATION_FAST_PKG     := ./internal/domain/task

.PHONY: help build vet fmt fmt-check lint test coverage integration bdd contract arch-test mutation mutation-fast vuln check check-all

help: ## Show the available targets
	@echo "Fulfillment Execution — make targets"
	@echo ""
	@echo "  help          Print this message (default target)"
	@echo "  build         go build ./..."
	@echo "  vet           go vet ./..."
	@echo "  fmt           gofmt -w . (formats in place)"
	@echo "  fmt-check     Fail if any file is not gofmt-clean"
	@echo "  lint          golangci-lint run ./... (pinned $(GOLANGCI_LINT_VERSION) in CI)"
	@echo "  test          go test ./... -race (unit + httptest + bdd; no DB needed)"
	@echo "  coverage      Coverage run + $(COVERAGE_THRESHOLD)% gate (same command as CI)"
	@echo "  integration   Postgres integration tests — needs DATABASE_URL / a running Postgres"
	@echo "  bdd           godog/Gherkin acceptance tests"
	@echo "  contract      scripts/contract-test.sh — Schemathesis vs apis/openapi.yaml"
	@echo "                (boots the service in-memory; needs st: pip install"
	@echo "                 'schemathesis==4.28.0')"
	@echo "  arch-test     Hexagonal architecture fitness tests (arch-go)"
	@echo "  mutation-fast Fast blocking mutation subset ($(MUTATION_FAST_PKG)), honors .gremlins.yaml"
	@echo "  mutation      Exhaustive mutation run over ./internal/domain (slow, ~scheduled CI job)"
	@echo "  vuln          govulncheck ./... (known CVEs in deps + stdlib)"
	@echo "  check         FAST pre-commit bundle: fmt-check vet build lint test"
	@echo "  check-all     check + coverage arch-test bdd (pre-push gate)"
	@echo ""
	@echo "  Hooks: run 'lefthook install' once to activate the pre-commit/pre-push hooks."

build: ## go build ./...
	go build ./...

vet: ## go vet ./...
	go vet ./...

fmt: ## Format the tree in place
	gofmt -w .

fmt-check: ## Fail if the tree is not gofmt-clean
	@out=$$(gofmt -l .); \
	if [ -n "$$out" ]; then \
		echo "gofmt: the following files are not formatted:"; \
		echo "$$out"; \
		echo "run 'make fmt' to fix"; \
		exit 1; \
	fi; \
	echo "gofmt: clean"

lint: ## golangci-lint run ./...
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "golangci-lint is not installed."; \
		echo "install the version CI pins with:"; \
		echo "  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)"; \
		exit 1; \
	fi
	golangci-lint run ./...

test: ## Unit + httptest + bdd layers (no DB)
	go test ./... -race

coverage: ## Coverage run plus the CI coverage gate
	go test ./... -race -coverprofile=coverage.out -coverpkg=$(COVERPKG)
	@COVERAGE=$$(go tool cover -func=coverage.out | awk '/^total:/ {print $$3}' | tr -d '%'); \
	echo "Coverage: $${COVERAGE}% (gate: $(COVERAGE_THRESHOLD)%)"; \
	if awk -v c="$$COVERAGE" -v t="$(COVERAGE_THRESHOLD)" 'BEGIN { exit !(c < t) }'; then \
		echo "coverage $${COVERAGE}% is below the $(COVERAGE_THRESHOLD)% gate"; \
		exit 1; \
	fi

integration: ## Postgres integration tests — requires DATABASE_URL and a reachable Postgres
	@if [ -z "$$DATABASE_URL" ]; then \
		echo "DATABASE_URL is not set; the integration tests will skip."; \
		echo "start Postgres with 'docker compose up -d' and export e.g."; \
		echo "  DATABASE_URL=postgres://fulfillment:fulfillment@localhost:5432/fulfillment_execution?sslmode=disable"; \
	fi
	go test -tags=integration ./... -race -count=1

bdd: ## godog/Gherkin acceptance tests
	go test ./... -run TestFeatures -v

# Property-based contract tests against apis/openapi.yaml — mirrors the
# `contract` CI job. Not part of check/check-all (needs Python tooling
# installed); CI runs it as its own job on every push and PR.
contract: ## Schemathesis contract tests (needs `st` on PATH)
	./scripts/contract-test.sh

arch-test: ## Architecture fitness tests
	go test ./internal/architecture/... -v

mutation-fast: ## Fast blocking mutation subset (thresholds come from .gremlins.yaml)
	@if ! command -v gremlins >/dev/null 2>&1; then \
		echo "gremlins is not installed."; \
		echo "install the version CI pins with:"; \
		echo "  go install github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0"; \
		exit 1; \
	fi
	gremlins unleash $(MUTATION_FAST_PKG)

mutation: ## Exhaustive mutation run over the whole domain layer (slow)
	@if ! command -v gremlins >/dev/null 2>&1; then \
		echo "gremlins is not installed."; \
		echo "install the version CI pins with:"; \
		echo "  go install github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0"; \
		exit 1; \
	fi
	gremlins unleash ./internal/domain --workers 1 --timeout-coefficient 30

vuln: ## Known CVEs in the dependency graph and the Go stdlib
	@if ! command -v govulncheck >/dev/null 2>&1; then \
		echo "govulncheck is not installed."; \
		echo "install it with:"; \
		echo "  go install golang.org/x/vuln/cmd/govulncheck@latest"; \
		exit 1; \
	fi
	govulncheck ./...

check: fmt-check vet build lint test ## Fast pre-commit bundle
	@echo "check: OK"

check-all: check coverage arch-test bdd ## Fuller pre-push gate (no DB, no mutation)
	@echo "check-all: OK"
