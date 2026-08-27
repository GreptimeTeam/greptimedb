# Architecture Invariants

Repo-wide rules that are easy to violate and expensive to get wrong. They are
**not** general best practices — each one is specific to GreptimeDB, has high
blast radius, and is not caught by `cargo clippy`.

This complements the other docs rather than repeating them:

- `docs/style-guide.md` — micro-level code style (formatting, modules, comments).
- `docs/rfcs/` — per-feature architecture decisions.
- `CONTRIBUTING.md` — how to build, test, and submit.

Paths below are relative to the repo root.

## 1. Persisted and wire formats must stay backward/forward compatible

Anything written to disk or sent over the wire outlives the process that wrote
it: region manifests, WAL entries, SST/Parquet files and their metadata,
metadata KV values (`common-meta` keys, metric-engine metadata), and gRPC
messages. A node running an old version may read data written by a new one and
vice versa.

- Preserve the contract of the actual encoding: Protobuf field numbers/types,
  serde field and variant names, positional field order, and explicitly encoded
  enum discriminants. Do not treat Rust declaration order as a wire contract
  for name-based formats such as JSON.
- Use `#[serde(default)]` for newly optional data and `#[serde(alias)]` for a
  compatible rename. Do not reset or reuse versions within one persisted
  history.
- When a change touches a persisted or wire format, add a case to the
  compatibility test suite. Follow [`tests/compatibility/README.md`](../tests/compatibility/README.md)
  and [`tests/compatibility/AGENTS.md`](../tests/compatibility/AGENTS.md). The
  original compatibility RFC describes a superseded case format and is not the
  current runbook.
- Wire types are generated from the external `greptime-proto` crate; change the
  format there first, then bump the dependency (see invariant 6's pattern).

## 2. Respect crate layering and dependency direction

The workspace is layered; dependencies point downward only.

- `common-*` crates are the base. They must not depend on storage engines,
  `frontend`, `datanode`, or `meta-srv`.
- `store-api` defines the engine contract (e.g. `RegionEngine` in
  `src/store-api/src/region_engine.rs`). Engines (`mito2`, `metric-engine`,
  `file-engine`) implement it; `datanode` drives engines **through the trait**,
  not through engine internals.
- `frontend` reaches storage through `operator` / `query` / `catalog`, not by
  depending on `datanode` internals. Standalone mode is the one bridge, via the
  `RegionServer` adapter in `src/standalone/src/datanode_manager.rs`.
- Do not introduce circular dependencies. New deps go through
  `[workspace.dependencies]` in the root `Cargo.toml`, not per-crate version
  literals.

## 3. Use the shared async runtimes; never block them

Runtimes are partitioned by workload so one workload can't starve another. They
live in `common-runtime` (`src/common/runtime/`).

- Runtime construction belongs in process bootstrap or test/benchmark harnesses.
  Product components use `spawn_global`, `spawn_query`, `spawn_ingest`,
  `spawn_compact`, or `spawn_hb` instead of a component-local Tokio runtime.
- Run CPU-bound or synchronous-blocking work via `spawn_blocking_*`; never do
  heavy CPU or blocking syscalls directly inside an async task.
- Never call `block_on*` from inside an async context or an engine worker — it
  deadlocks the runtime.

## 4. Errors: snafu + `ErrorExt`, no panics in non-test code

Each crate defines its own snafu `Error` enum and implements `ErrorExt`
(`src/common/error/src/ext.rs`).

- Set a meaningful `status_code()`. It drives the client-visible result and
  whether the message is masked (`Internal`/`Unknown` are masked from end users).
- Mark errors the caller may retry with the appropriate `retry_hint()`
  (`Retryable`). Default is non-retryable.
- In non-test code, return errors instead of `unwrap()` / `expect()` / `panic!()`.
  Use `unimplemented!()` (not `todo!()`) for paths that won't be implemented, per
  `docs/style-guide.md`, which also covers `with_context` vs `context`.

## 5. Gate unstable features behind `experimental_` config

Features whose behavior or surface may still change ship behind config keys
prefixed `experimental_` (see existing examples in `config/datanode.example.toml`,
`config/flownode.example.toml`, `config/standalone.example.toml`). Some can be
overridden per-object (e.g. a flow's `WITH (experimental_... = '...')`). This lets
unfinished work merge without freezing it into the stable config surface.

When you stabilize such a feature, drop the prefix and document the migration.

## 6. DataFusion is a pinned fork — workspace dependencies plus patches

GreptimeDB uses a fork at `GreptimeTeam/datafusion`, wired up in the root
`Cargo.toml` through two sections:

- `[workspace.dependencies]` pins directly referenced sub-crates to an exact
  crates.io version.
- `[patch.crates-io]` redirects resolution to the fork. It may also contain
  transitive-only sub-crates with no workspace dependency.

Add a direct dependency to `[workspace.dependencies]` and patch it if needed; a
transitive-only override needs only the patch. Upgrade all exact pins and fork
revisions together.

## 7. Enterprise-gated code is licensed differently from the rest

A few sources in this repo are governed by the GreptimeDB Enterprise License
(`LICENSE-ENTERPRISE`) rather than Apache-2.0. They are compiled only with the
`enterprise` feature, and the license boundary is drawn at **file** granularity —
so how you gate a change decides which license applies to it.

- A whole feature that only exists in the enterprise build goes into its own
  module file (e.g. `src/sql/src/statements/drop/trigger.rs`), pulled in by
  `#[cfg(feature = "enterprise")] pub mod trigger;`. That file carries the
  enterprise header, and must be listed in `licenserc-enterprise.toml`
  (`includes`) **and** in `licenserc.toml` (`excludes`) so the Apache-2.0 check
  skips it. Submodules of such a module inherit all of this.
- A branch on a shared path — an extra enum variant, a match arm, an `if` — stays
  inline behind `#[cfg(feature = "enterprise")]` in the shared file (e.g. the
  trigger variants in `src/sql/src/statements/statement.rs`). The file keeps its
  Apache-2.0 header. Do not split a file just to gate three lines.
- The `enterprise` feature must be forwarded down every dependency edge that
  needs it (`enterprise = ["sql/enterprise", ...]`). A crate that gates code on a
  feature its dependents never enable silently compiles the OSS path.

`make check-enterprise-license` verifies the bookkeeping half of this: every file
behind an enterprise-gated `mod` appears in both configs, and neither config has
stale entries. It runs in CI. Deciding what belongs in a separate file is still
yours — hawkeye alone cannot catch a miss, because a wrongly Apache-headered
enterprise file passes the default check exactly by not being excluded from it.

## Maintenance contract

Update this file when a new repo-wide invariant emerges (a new persisted format,
a new runtime category, a layering rule), or when an existing one changes. Keep
each entry high-signal: if `clippy` or `docs/style-guide.md` already enforces it,
it does not belong here.
