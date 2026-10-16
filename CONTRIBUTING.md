# Contributing

Thanks for helping build hopper. The rules that apply to every change are in
[AGENTS.md](AGENTS.md); this file covers the mechanics.

## Local setup

hopper needs Go (the version in `go.mod`) and a PostgreSQL 14+ database for the
integration tests. `make devstack` runs one natively, with nothing to install:
the first run downloads the Postgres binaries (Postgres 18), and the data
persists in `.devstack/` until `make reset`, which wants `make devstack`
stopped first.

```sh
make devstack  # Postgres on localhost:54329, until interrupted
make check     # lint + tests, in another terminal
```

A script can start `make devstack` in the background and wait for it with
`make devstack-wait`. A container left from the old `make pg` holds the same
port: stop it with `docker stop hopper-postgres`.

To test against another Postgres, such as one of the other versions CI runs,
set `HOPPER_TEST_DATABASE_URL`. Every test creates its own schema and drops it
afterwards, so tests run in parallel; a test binary that is killed leaves its
schemas behind, which `make reset` clears. Without a database, integration
tests are skipped.

Development tools are pinned and run with `go tool`, so there is nothing to
install globally: golangci-lint in `tools/go.mod`, and dev, which runs
`make devstack`, in `tools/dev/go.mod`, with the newer Go it needs (fetched
by the `go` command unless `GOTOOLCHAIN=local`).

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
