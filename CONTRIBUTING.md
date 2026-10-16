# Contributing

Thanks for helping build hopper. The rules that apply to every change are in
[AGENTS.md](AGENTS.md); this file covers the mechanics.

## Local setup

hopper needs Go (the version in `go.mod`) and a PostgreSQL 14+ database for the
integration tests. `make devstack` runs one natively, with nothing to install:
the first run downloads the Postgres binaries, and the data persists in
`.devstack/` until `make reset`.

```sh
make devstack  # Postgres on localhost:54329, until interrupted
make check     # lint + tests, in another terminal
```

To use a database you already run, set `HOPPER_TEST_DATABASE_URL`. Every test
creates its own schema and drops it afterwards, so tests run in parallel and
leave nothing behind. Without the variable, integration tests are skipped.

Development tools (golangci-lint) are pinned in `tools/go.mod` and run with
`go tool`, so there is nothing to install globally.

## Pull requests

- Branch from `canary` and open the PR against `canary`.
- Write commit messages and the PR title as [Conventional Commits](https://www.conventionalcommits.org)
  (`feat(client): ...`, `fix(rescuer): ...`, `docs: ...`). PRs are
  squash-merged, so the title becomes the commit message.
- Keep [docs/PLAN.md](docs/PLAN.md) current: it is the design of record, and a
  behavior change lands in the same PR as its plan update.
- Changes to the insert, claim or finalize paths include `hopperbench` results
  (`make bench`) for the PR branch and for `canary`.

## Testing

```sh
make test                                   # everything
go test -run TestClient ./...               # one test
go test -race -count=20 -run Concurrency .  # shake out a flaky test
```

Tests run under `GODEBUG=fips140=only`, which is set in `go.mod`. If a test
fails with a FIPS error, the fix is to remove the cryptography, not the flag.
