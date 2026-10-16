# hopper developer tasks. Run `make help` for a list.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Development tools are pinned in tools/go.mod and run with `go tool`.
GOTOOL := go tool -modfile=tools/go.mod
# dev runs what dev.json describes; see github.com/parallelworks/foundation/dev.
DEV := $(GOTOOL) dev

# Integration tests need a Postgres 14+ database. `make devstack` runs one
# natively on the port in dev.json.
HOPPER_TEST_DATABASE_URL ?= postgres://hopper:hopper@localhost:54329/hopper_test?sslmode=disable
export HOPPER_TEST_DATABASE_URL

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## Local development

.PHONY: devstack
devstack: ## Run Postgres natively for tests until interrupted (data in .devstack/)
	$(DEV) stack

.PHONY: reset
reset: ## Delete the local Postgres data
	$(DEV) reset

## Quality

.PHONY: check
check: lint test ## Run all linters and tests

.PHONY: lint
lint: ## Lint Go code
	$(GOTOOL) golangci-lint run ./...
	cd hopperotel && go tool -modfile=../tools/go.mod golangci-lint run ./...
	cd hopperui && go tool -modfile=../tools/go.mod golangci-lint run ./...

.PHONY: fmt
fmt: ## Format Go code
	$(GOTOOL) golangci-lint fmt

.PHONY: test
test: ## Run tests (integration tests need `make devstack` or HOPPER_TEST_DATABASE_URL)
	go test -race -cover -timeout 10m ./...
	cd hopperotel && go test -race -cover -timeout 10m ./...
	cd hopperui && go test -race -cover -timeout 10m ./...

.PHONY: bench
bench: ## Run the benchmark harness against the test database
	go run ./cmd/hopperbench -database-url "$(HOPPER_TEST_DATABASE_URL)"
