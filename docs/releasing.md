# Releasing hopper

hopper is three Go modules in one repository: the core module at the root,
`hopperotel` and `hopperui`. A release is one version across all three, cut
as three tags on the same commit of `canary`: `vX.Y.Z`, `hopperotel/vX.Y.Z`
and `hopperui/vX.Y.Z`. Until v1.0.0, minor versions may change the API;
patch versions do not.

Releases are automated with [release-please](https://github.com/googleapis/release-please)
and [goreleaser](https://goreleaser.com) (`.github/workflows/release.yml`).

## How a release happens

1. Every merge to `canary` is a squash commit whose title is a Conventional
   Commit (enforced by the PR-title check). release-please reads those titles
   and keeps **one release PR** open, titled `chore(canary): release X.Y.Z`,
   that holds the next version's changelog entry. `fix` and `perf` bump the
   patch version, `feat` the minor version, and a `!` (breaking change) also
   bumps the minor version before v1.0.0 (`bump-minor-pre-major`).
   release-please manages one package, the core module; the sub-modules are
   versioned with it rather than on their own, so a change anywhere in the
   repository releases all three.
2. The release PR updates `CHANGELOG.md`, the manifest
   (`.release-please-manifest.json`), and the sub-modules' `go.mod`
   requirement on the core module, through the `// x-release-please-version`
   annotation on that line (`extra-files`). The `replace ... => ../`
   directives stay: they apply only inside this repository, and consumers
   resolve the tag.
3. Merging the release PR (with `gh pr merge --squash --admin`, since the
   `canary` ruleset asks for two reviews) creates the `vX.Y.Z` tag and the
   GitHub release with the changelog entry. The workflow then tags
   `hopperotel/vX.Y.Z` and `hopperui/vX.Y.Z` on the same commit, and
   goreleaser builds `hopper`, `hopperbench` and `hopperui` for Linux and
   macOS (amd64, arm64) and attaches the archives and checksums to the
   release.

Nothing else is needed for a normal release. A minor release should still
have the §8.2 targets re-run on the reference hardware and the results
table in [PLAN.md](PLAN.md) brought up to date before the release PR is
merged.

## Setup

No secrets. The workflow runs with the default `GITHUB_TOKEN` and grants
permissions per job: `contents`, `pull-requests` and `actions: write` to the
release-please job, `contents: write` alone to the job that tags the
sub-modules and runs goreleaser. Every
action in `.github/workflows` is pinned to a commit SHA, with the version as
a trailing comment; Dependabot bumps both, once a release is a week old.
CI's `workflows` job audits the workflow files with
[zizmor](https://docs.zizmor.sh) and fails on any finding.
The release binaries are built without the Go build cache.
GitHub does not start workflows for a pull request that token opened, so
after release-please has created or updated its PR the workflow dispatches
`ci.yml` on the PR's branch (`gh workflow run`); that run appears among the
PR's checks like any other. The PR-title check does not run on the release
PR; its title, `chore(canary): release X.Y.Z`, is release-please's own and passes
the ruleset's commit-message pattern.

## Steering a release

- **Force a version:** add a footer `Release-As: X.Y.Z` to a squash commit's
  body (the PR description), and the release PR proposes that version.
- **Hold a change out of the changelog:** only `feat`, `fix`, `perf` and
  `revert` appear; `docs`, `test`, `ci`, `build`, `chore` and `refactor`
  are hidden.
- **Hotfix:** merge the fix to `canary` as `fix(...)`; the release PR updates
  itself. hopper has no long-lived release branches before v1.0.0.

## Verify

After the tags exist, from an empty module:

```sh
cd "$(mktemp -d)" && go mod init check && go get github.com/parallelworks/hopper@vX.Y.Z \
  github.com/parallelworks/hopper/hopperotel@vX.Y.Z github.com/parallelworks/hopper/hopperui@vX.Y.Z
```

## History

v0.1.0 and v0.1.1 were tagged by hand, as this document described then:
annotated tags for the three modules on the release commit, core first, and
the GitHub release created from the changelog section. release-please
starts from the manifest at 0.1.1.
