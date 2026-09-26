## Summary

<!-- What does this change and why? PR titles must follow Conventional Commits, e.g. `feat(client): add snooze`. -->

## Testing

<!-- How was this verified? Changes to the insert, claim or finalize paths include `hopperbench` results against canary. -->

## Checklist

- [ ] `make check` passes
- [ ] No cryptography was added (tests run with `GODEBUG=fips140=only`)
- [ ] New or changed SQL in the claim, finalize, rescue or leader paths has a concurrency or chaos test
- [ ] Behavior changes are reflected in `docs/PLAN.md`
