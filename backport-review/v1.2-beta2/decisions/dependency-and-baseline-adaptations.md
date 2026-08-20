# Dependency and baseline adaptations (#8860, #8858, #8898)

**Status:** lower-risk context adaptations. Adaptation does not imply that every item has semantic risk.

## Original PR patch — intended upstream behavior

Immutable upstream commits: [#8860](https://github.com/GreptimeTeam/greptimedb/commit/f2ffaef9dfee134fa46d54ba65e6f40b37982cef) updates pgwire; [#8858](https://github.com/GreptimeTeam/greptimedb/commit/6249623cfb181b0222dfc03224945c625f326f1a) removes iceberg read; [#8898](https://github.com/GreptimeTeam/greptimedb/commit/b882e393dfadcf9406c76d1017fe64e0652f0202) updates the dashboard baseline.

## Resulting backport patch — what ships

Carriers are [#8860](https://github.com/GreptimeTeam/greptimedb/commit/0ffca14740493d0c1afa556908345d9e135d7656), [#8858](https://github.com/GreptimeTeam/greptimedb/commit/065d0dfaadc2d6252adc5a671ec0658cfd51d4d6), and [#8898](https://github.com/GreptimeTeam/greptimedb/commit/e2a85ff19b00edba80640123b17166704a0c1117), with [final lock alignment](https://github.com/GreptimeTeam/greptimedb/commit/b5c38fbfc15053f42d4c1d843204bbdd52cb4a00) before the #8825 carrier.

## Relative to original PR — exact differences

| Aspect | Original PR | Backport | Why |
| --- | --- | --- | --- |
| pgwire | Update on upstream dependency graph | pgwire 0.40.7 while retaining release `windows-sys 0.60.2` graph | Preserve beta2 dependency baseline |
| Auth public export (#8858) | Upstream export hunk | Adapted because release lacks `SEMANTIC_GRAPH_QUERY` | Release symbol/API context; no behavioral divergence identified |
| Dashboard (#8898) | v0.13.12 → v0.13.13 | v0.13.10 → v0.13.13 | Beta2 starts from an older dashboard baseline |
| Lock resolution | Mainline lock state | Offline release resolver and final package alignment | Keep the aggregate reproducible on the release graph |

## Intentionally excluded/not provided

No semantic divergence is identified for the #8858 export adaptation. No unrelated dependency upgrades or dashboard changes are included. This page does not claim that dependency changes are universally risk-free.

## Compatibility and rollback impact

Lock and dependency changes affect build/reproducibility context; dashboard baseline affects bundled UI assets. Roll back the matching lock/package and asset changes together. The #8858 symbol absence is a release API fact, not a runtime compatibility layer.

## Files reviewers should inspect

- `Cargo.toml` and `Cargo.lock`: inspect pgwire, `windows-sys`, and final lock identities.
- Auth public-export module in `src/`: inspect the missing `SEMANTIC_GRAPH_QUERY` context.
- Dashboard package/baseline files: inspect v0.13.10 → v0.13.13.

## Verification evidence already recorded

Recorded CI status is documented in the [canonical CI status](README.md#canonical-ci-status): run 32341712112 at aggregate HEAD completed with **failure**. Aggregate history and range-diffs are also recorded evidence; no new test result is claimed.

## Raw audit evidence — unabridged differences only

- [`range-diffs/pr-8860.txt`](../range-diffs/pr-8860.txt)
- [`range-diffs/pr-8858.txt`](../range-diffs/pr-8858.txt)
- [`range-diffs/pr-8898.txt`](../range-diffs/pr-8898.txt)

These are unabridged differences between patches, not full shipping patches.
