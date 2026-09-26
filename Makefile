# hopper developer tasks. Run `make help` for a list.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Development tools are pinned in tools/go.mod and run with `go tool`.
GOTOOL := go tool -modfile=tools/go.mod

# Integration tests need a Postgres 14+ database. `make pg` starts one in Docker.
PG_CONTAINER ?= hopper-postgres
PG_PORT      ?= 54329
PG_IMAGE     ?= postgres:17-alpine
HOPPER_TEST_DATABASE_URL ?= postgres://hopper:hopper@localhost:$(PG_PORT)/hopper_test?sslmode=disable
export HOPPER_TEST_DATABASE_URL

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## Local development

.PHONY: pg
pg: ## Start a throwaway Postgres for tests (data is not persisted)
	docker run -d --rm --name $(PG_CONTAINER) -p $(PG_PORT):5432 \
		-e POSTGRES_USER=hopper -e POSTGRES_PASSWORD=hopper -e POSTGRES_DB=hopper_test \
		$(PG_IMAGE) -c fsync=off -c synchronous_commit=off -c max_connections=300
	@until docker exec $(PG_CONTAINER) pg_isready -U hopper -d hopper_test >/dev/null 2>&1; do sleep 0.5; done

.PHONY: pg-down
pg-down: ## Stop the test Postgres
	-docker stop $(PG_CONTAINER)

## Quality

.PHONY: check
check: lint test ## Run all linters and tests

.PHONY: lint
lint: ## Lint Go code
	$(GOTOOL) golangci-lint run ./...

.PHONY: fmt
fmt: ## Format Go code
	$(GOTOOL) golangci-lint fmt

.PHONY: test
test: ## Run tests (integration tests need `make pg` or HOPPER_TEST_DATABASE_URL)
	go test -race -cover -timeout 10m ./...

.PHONY: bench
bench: ## Run the benchmark harness against the test database
	go run ./cmd/hopperbench -database-url "$(HOPPER_TEST_DATABASE_URL)"
