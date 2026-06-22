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

- Add fields, don't repurpose or reorder them. For serde types use
  `#[serde(default)]` / `#[serde(alias)]`; never change the meaning of an existing
  field or the discriminant of an existing enum variant.
- Monotonic version counters (e.g. the mito2 manifest version) only ever move
  forward — never reset or skip.
- When a change touches a persisted or wire format, add a case to the
  compatibility test suite. See `docs/rfcs/2025-07-04-compatibility-test-framework.md`
  — compatibility has been broken on releases before (v0.14.1, v0.15.1) precisely
  because this step was skipped.
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
  depending on `datanode` internals. Standalone mode is the one bridge, via a
  `RegionServer` wrapper (`src/frontend/src/instance/standalone.rs`).
- Do not introduce circular dependencies. New deps go through
  `[workspace.dependencies]` in the root `Cargo.toml`, not per-crate version
  literals.

## 3. Use the shared async runtimes; never block them

Runtimes are partitioned by workload so one workload can't starve another. They
live in `common-runtime` (`src/common/runtime/`).

- Use the categorized spawns — `spawn_global`, `spawn_query`, `spawn_ingest`,
  `spawn_compact`, `spawn_hb` — instead of constructing your own tokio runtime,
  and pick the category that matches the work.
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

## 6. DataFusion is a pinned fork — patch in two places

GreptimeDB depends on a fork at `GreptimeTeam/datafusion`, pinned to a single git
rev. In the root `Cargo.toml`:

- Every DataFusion sub-crate must appear in **both** `[workspace.dependencies]`
  **and** `[patch.crates-io]`, at the **same** rev.
- Adding a new DataFusion sub-crate dependency means adding it to both sections.
- Upgrading DataFusion means bumping the rev for **all** of them together.

## Maintenance contract

Update this file when a new repo-wide invariant emerges (a new persisted format,
a new runtime category, a layering rule), or when an existing one changes. Keep
each entry high-signal: if `clippy` or `docs/style-guide.md` already enforces it,
it does not belong here.
