# hopper developer tasks. Run `make help` for a list.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Development tools are pinned in tools/go.mod and run with `go tool`.
GOTOOL := go tool -modfile=tools/go.mod
# dev runs what dev.json describes; see github.com/parallelworks/foundation/dev.
# It has a module of its own, since it needs a newer Go than the rest.
DEV := go tool -modfile=tools/dev/go.mod dev

# Integration tests need a Postgres 14+ database. `make devstack` runs one
# natively on the port in dev.json; set HOPPER_TEST_DATABASE_URL for another.
HOPPER_TEST_DATABASE_URL ?= postgres://hopper:hopper@localhost:54329/hopper_test?sslmode=disable
export HOPPER_TEST_DATABASE_URL

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## Local development

.PHONY: devstack
devstack: ## Run Postgres natively for tests until interrupted (data in .devstack/)
	$(DEV) stack

.PHONY: devstack-wait
devstack-wait: ## Wait until a `make devstack` started elsewhere accepts connections
	$(DEV) wait

.PHONY: reset
reset: ## Delete the local Postgres data (stop `make devstack` first)
	$(DEV) reset

## Quality

.PHONY: check
check: lint test ## Run all linters and tests

.PHONY: lint
lint: ## Lint Go code
	$(GOTOOL) golangci-lint run ./...
	cd hopperotel && go tool -modfile=../tools/go.mod golangci-lint run ./...
	cd hopperui && go tool -modfile=../tools/go.mod golangci-lint run ./...
	cd driver/hoppersqlite && go tool -modfile=../../tools/go.mod golangci-lint run ./...

.PHONY: fmt
fmt: ## Format Go code
	$(GOTOOL) golangci-lint fmt

.PHONY: test
test: ## Run tests (integration tests need `make devstack` or HOPPER_TEST_DATABASE_URL)
	@# Under a running `dev`, whose ports move when taken, its environment
	@# gives the database; otherwise the default above does.
	@if $(DEV) status >/dev/null 2>&1; then $(DEV) exec -- $(MAKE) test-modules; else $(MAKE) test-modules; fi

.PHONY: test-modules
test-modules:
	go test -race -cover -timeout 10m ./...
	cd hopperotel && go test -race -cover -timeout 10m ./...
	cd hopperui && go test -race -cover -timeout 10m ./...
	cd driver/hoppersqlite && go test -race -cover -timeout 10m ./...

.PHONY: bench
bench: ## Run the benchmark harness against the test database
	go run ./cmd/hopperbench -database-url "$(HOPPER_TEST_DATABASE_URL)"

.PHONY: bench-sqlite
bench-sqlite: ## Run the benchmark harness on a SQLite database in a temporary directory
	cd driver/hoppersqlite && go run ./cmd/hopperbench-sqlite

.PHONY: cgo-check
cgo-check: ## Check that hoppersqlite, its tests and every module they import use no cgo
	@cd driver/hoppersqlite && for os in linux darwin windows freebsd openbsd netbsd; do \
		GOOS=$$os CGO_ENABLED=0 go vet ./... || exit 1; \
		pkgs=$$(GOOS=$$os CGO_ENABLED=1 go list -deps -test -f '{{if and .CgoFiles (not .Standard)}}{{.ImportPath}}{{end}}' ./...) || exit 1; \
		if [ -n "$$pkgs" ]; then echo "cgo is not allowed, but these packages use it on $$os:"; echo "$$pkgs"; exit 1; fi; \
	done
