# CI cache policy

Checks, Rust CI, Integration CI, and Nightly CI use one explicit Rust cache per
build job. Disable the implicit cache in `setup-rust-toolchain` when adding an
explicit cache: otherwise its independent save policy can write PR-scoped
caches even when the explicit cache only saves on main.

The existing main schedules (or manual runs on main) populate shared caches.
PR, merge-group, and non-main manual runs restore caches without saving them.
Fuzz binaries still flow to downstream jobs through workflow artifacts on every
run; only the persistent binary cache is main-written. Re-running a PR cannot
reuse a newly saved PR-specific binary cache under this policy.

Keep helper version variables outside the `CARGO*` and `RUST*` prefixes unless
they affect the compilation being cached. rust-cache includes those prefixes in
its environment hash. Fuzz tool versions remain explicitly included in the fuzz
cache keys. Compiler versions, Cargo manifests, flags, OS, and architecture
remain part of the existing cache isolation.

This policy reduces duplicate and PR-only writes; it does not guarantee that all
build caches fit in the repository quota. Renamed environment variables cause
an initial cold restore until main produces the new keys. Existing caches are
not deleted by this change. Coverage warming and additional runner capacity are
not introduced.

Validate the policy with:

```sh
node --test .github/scripts/tests/ci-cache-policy.test.cjs
```

After deployment, compare the same commit and runner image across repeated runs.
Record exact versus fallback restores, archive size, restore/save duration, and
compilation duration. Verify main writes the keys consumed by PRs and that no
implicit post-action uploads remain. Local policy tests do not establish a
runtime speedup or long-term cache hit rate.
