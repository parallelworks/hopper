# Releasing hopper

hopper is three Go modules in one repository: the core module at the root,
`hopperotel` and `hopperui`. A release is one version across all three, cut
as three tags on the same commit of `canary`. Until v1.0.0, minor versions may
change the API; patch versions do not.

## Before tagging

1. Every change is on `canary` through a merged PR, and CI is green there.
2. The `[Unreleased]` section of [CHANGELOG.md](../CHANGELOG.md) has become
   `[X.Y.Z] - YYYY-MM-DD`, with the link references at the bottom updated.
3. For a minor release, the §8.2 targets in [PLAN.md](PLAN.md) have been run
   on the reference hardware and the results table there is current
   (`hopperbench` on a separate host, Postgres 17 on 8 vCPU / 32 GB / NVMe,
   `synchronous_commit = on`).
4. `hopperotel/go.mod` and `hopperui/go.mod` require
   `github.com/parallelworks/hopper` at the version about to be tagged. They
   keep their `replace ... => ../` directives, which apply only inside this
   repository: consumers resolve the required version from the tag.

## Tagging

On the release commit of `canary`:

```sh
git tag -a v0.1.0 -m "hopper v0.1.0"
git tag -a hopperotel/v0.1.0 -m "hopperotel v0.1.0"
git tag -a hopperui/v0.1.0 -m "hopperui v0.1.0"
git push origin v0.1.0 hopperotel/v0.1.0 hopperui/v0.1.0
```

The core tag goes first so that the sub-module tags, which require it, can be
resolved the moment they are pushed. Nested-module tags carry the module's
directory as a prefix; that is how the Go tool finds a version of a module
that is not at the repository root.

Then create the GitHub release from the `v0.1.0` tag with the changelog
section as its notes, and verify from an empty module:

```sh
cd "$(mktemp -d)" && go mod init check && go get github.com/parallelworks/hopper@v0.1.0 \
  github.com/parallelworks/hopper/hopperotel@v0.1.0 github.com/parallelworks/hopper/hopperui@v0.1.0
```

## After tagging

- Open a `[Unreleased]` section in the changelog.
- The plan of record's status line names the released version.
- The `canary` ruleset requires two approving reviews; a maintainer merging a
  release-prep PR alone uses `gh pr merge --squash --admin`.
