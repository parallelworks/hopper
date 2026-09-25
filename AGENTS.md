# Contributing to hopper

- Every change goes through a pull request against `canary`.
- PR titles follow [Conventional Commits](https://www.conventionalcommits.org)
  (`feat(client): ...`, `fix(rescuer): ...`, `docs: ...`). PRs are squash-merged, so the
  title becomes the commit.
- The core module depends only on the Go standard library and `github.com/jackc/pgx/v5`.
  Integrations (OpenTelemetry, UI) go in separate modules.
- No cryptography. Tests run with `GODEBUG=fips140=only`.
- Every state transition is a conditional statement that checks the prior state and the
  owning client. Changes to the SQL in the claim, finalize, rescue or leader paths need
  a concurrency or chaos test.
- Always use database time (`now()`), never application time, when comparing job times.
- Changes to the insert, claim or finalize paths include `hopperbench` results in the
  PR. A regression of more than 10% against `canary` blocks the merge.
- [docs/PLAN.md](docs/PLAN.md) is the design of record. Change it in the same PR as a
  behavior change.
