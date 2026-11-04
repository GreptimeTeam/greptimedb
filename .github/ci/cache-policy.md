# CI cache policy

Checks, Rust CI, Integration CI, and Nightly CI use one explicit Rust cache per
build job. Disable the implicit cache in `setup-rust-toolchain` when adding an
explicit cache: otherwise its independent save policy can write PR-scoped
caches even when the explicit cache only saves on main.

Integration CI's existing weekday schedule runs the fuzz builds and suites,
including the profile-CI binary and Kafka helper, to populate main caches.
To warm these caches manually after deployment, run Integration CI on main with
`fuzz_profile: all`; no internal `ci_command` marker is required.

Rust CI's existing weekday schedule warms the Windows and RISC-V compilation
caches. Nightly CI's existing schedule warms its Windows runtime test caches.
A manual Rust CI run on main can warm the compilation caches sooner. These jobs
keep their separate keys and existing runner types; no extra warmup job is added.
Coverage caches are not warmed by these schedules.
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
