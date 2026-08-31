# AGENTS.md

Guidance for coding agents (Claude Code, Codex, ...) and contributors working in
this repository. `CLAUDE.md` is a symlink to this file. If `.local/AGENTS.md` or
`.local/CLAUDE.md` is present, you **MUST** read it as well — it holds personal
or machine-local overrides (gitignored, not shared). To record a personal or
machine-local override, write it to `.local/AGENTS.md` (create `.local/` and the
file if absent), not to this shared file.

GreptimeDB is an open-source, cloud-native observability database for unified
collection and analysis of metrics, logs, and traces. It is written in Rust and
provides sub-second querying at PB scale with high cost efficiency.

## Core commands

| Task | Command |
| --- | --- |
| Build (debug) | `make build` |
| Build (release) | `make build RELEASE=true` |
| Run standalone | `cargo run -- standalone start` |
| Targeted Rust tests | `cargo nextest run -p <package>` (preferred over `cargo test`) |
| Full local Rust suite | `make test` |
| SQL tests | `cargo sqlness bare` (single case: `cargo sqlness bare -t <name>`) |
| Format | `make fmt` (and `make fmt-toml` for TOML) |
| Lint | `make clippy` (= `cargo clippy --workspace --all-targets --all-features -- -D warnings`) |
| Type check | `make check` |

Toolchain: Rust nightly, Protobuf compiler (>= 3.15), C/C++ build essentials.
Install the test runner with `cargo install cargo-nextest --locked`.

## Repo map

GreptimeDB is a Cargo workspace rooted at the repository; most crates live under
`src/` (plus `tests-fuzz`, `tests-integration`, `tests/runner`). Key areas:

- **Frontend / protocol**: `src/frontend/` (request orchestration), `src/servers/`
  (wire protocols), `src/sql/` (SQL parsing)
- **Storage engines**: `src/mito2/` (main time-series engine), `src/metric-engine/`
  (metrics), `src/file-engine/`
- **Coordination**: `src/meta-srv/` (metadata & cluster control), `src/meta-client/`
- **Execution / statements**: `src/operator/` (DDL/DML, request conversion, procedures)
- **Stream / transform**: `src/flow/` (continuous aggregation), `src/pipeline/`
- **Query / index**: `src/query/`, `src/promql/`, `src/index/`
- **Shared**: `src/common/`, `src/datatypes/`, `src/store-api/` (engine contract),
  `src/catalog/`, `src/table/`

Before editing a path, read every `AGENTS.md` from the repository root down to
that path. The root guide always applies; a nested guide adds or overrides rules
for its subtree. Changes spanning multiple subtrees must follow every applicable
guide.

`AGENTS.md` files are maps, not manuals. Keep them short and stable: record
where code lives, important boundaries or change-coupling, high-cost gotchas,
and the validation entry point. Implementation details belong in code. Do not
copy exhaustive test lists or workflow logic; link to the source of truth.

High-change areas and specialized test suites carry their own `AGENTS.md` with
a module map, read/write paths, change-coupling points, and gotchas:

- [`src/common/meta/AGENTS.md`](src/common/meta/AGENTS.md)
- [`src/query/AGENTS.md`](src/query/AGENTS.md)
- [`src/servers/AGENTS.md`](src/servers/AGENTS.md)
- [`src/operator/AGENTS.md`](src/operator/AGENTS.md)
- [`src/mito2/AGENTS.md`](src/mito2/AGENTS.md)
- [`src/metric-engine/AGENTS.md`](src/metric-engine/AGENTS.md)
- [`src/flow/AGENTS.md`](src/flow/AGENTS.md)
- [`src/frontend/AGENTS.md`](src/frontend/AGENTS.md)
- [`src/meta-srv/AGENTS.md`](src/meta-srv/AGENTS.md)
- [`tests/compatibility/AGENTS.md`](tests/compatibility/AGENTS.md)
- [`tests/perf/AGENTS.md`](tests/perf/AGENTS.md)

## Read before changing code

- [`.agents/architecture-invariants.md`](.agents/architecture-invariants.md) —
  repo-wide rules that are easy to violate and expensive to get wrong (persisted/
  wire format compatibility, crate layering, async runtimes, error handling,
  feature gating, the DataFusion fork).
- [`.agents/generated-files.md`](.agents/generated-files.md) — tool-generated
  artifacts that must not be hand-edited (sqlness `.result`, `config/config.md`,
  Grafana dashboards, proto).
- [`docs/style-guide.md`](docs/style-guide.md) — code style.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — contribution flow and CLA.

## High-signal entry points

- Main binary: `src/cmd/src/bin/greptime.rs`
- Configuration: `src/common/config/`, example TOMLs in `config/`
- Error handling: `src/common/error/` (`ErrorExt`, `StatusCode`)
- Protocol implementations: `src/servers/src/`

## Worktree safety

- Check `git status --short` before editing. Preserve unrelated tracked changes
  and untracked files; re-read files changed by another process.
- Do not rewrite history, force-push, or remove files in bulk unless the user
  explicitly requests it.
- Update generated artifacts only through the generators documented in
  [`.agents/generated-files.md`](.agents/generated-files.md).

## Validation by change type

Use the narrowest command that covers the change, then expand only when its
blast radius requires it.

| Change | Minimum validation |
| --- | --- |
| One Rust crate | `cargo nextest run -p <package>` |
| Cross-workspace Rust behavior | `make test`; inspect `.github/workflows/rust.yml` when CI parity matters |
| SQL parsing, planning, execution, or output | `cargo sqlness bare -t <case>`; inspect regenerated `.result` files |
| Persisted metadata or wire format | Add/run a case under `tests/compatibility/`; follow its `README.md` and `AGENTS.md` |
| Public configuration | Update example TOMLs, loading/serialization snapshots, and docs; run `make config-docs` |
| Query regression harness or DSL | Follow `tests/perf/AGENTS.md` |
| Enterprise-gated code | Build/test with `--features enterprise` where applicable and run `make check-enterprise-license` |

## Before opening a PR

1. If you added a `.rs`, `.py`, or `.ts` file, apply and verify its license
   header with `hawkeye format` followed by `hawkeye check`. Use the inception
   year from `licenserc.toml`, not the current year.
2. `make fmt`
3. `make clippy`
4. `make test`
5. `make check-udeps` (run `make fix-udeps` if it reports unused dependencies).
6. If you added or changed a public configuration option, update the applicable
   example TOMLs, configuration-loading and serialized-config snapshot tests,
   and related user-facing documentation. Run `make config-docs` (needs Docker)
   and commit the regenerated `config/config.md`.
7. If you changed a persisted or wire format, add a compatibility test case (see
   `.agents/architecture-invariants.md`).
8. If you added or gated an enterprise-only file, give it the enterprise license
   header, list it in `licenserc-enterprise.toml` (`includes`) and
   `licenserc.toml` (`excludes`), and run `make check-enterprise-license`.
9. Use a conventional-commit title, sign off commits (`git commit -s`), and sign
   the CLA.
10. When creating or updating a pull request, follow
   [`.github/pull_request_template.md`](.github/pull_request_template.md): include
   the CLA statement, fill the change-intention section with enough detail, and
   update checklist items accurately.

## More

- Agent skills and resources: [`.agents/`](.agents/) (see [`.agents/README.md`](.agents/README.md))
- Architecture decisions: [`docs/rfcs/`](docs/rfcs/)
- How-to guides: [`docs/how-to/`](docs/how-to/)
