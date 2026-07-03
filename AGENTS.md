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
| Test | `cargo nextest run` (preferred over `cargo test`) |
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

Hot crates carry their own `AGENTS.md` with a module map, read/write paths,
change-coupling points, and gotchas:

- [`src/mito2/AGENTS.md`](src/mito2/AGENTS.md)
- [`src/metric-engine/AGENTS.md`](src/metric-engine/AGENTS.md)
- [`src/flow/AGENTS.md`](src/flow/AGENTS.md)
- [`src/frontend/AGENTS.md`](src/frontend/AGENTS.md)
- [`src/meta-srv/AGENTS.md`](src/meta-srv/AGENTS.md)

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

## Before opening a PR

1. `make fmt`
2. `make clippy`
3. `make test` (or `cargo nextest run`)
4. `make check-udeps` (run `make fix-udeps` if it reports unused dependencies).
5. If you changed sample config under `config/`, run `make config-docs` (needs
   Docker) and commit the regenerated `config/config.md`.
6. If you changed a persisted or wire format, add a compatibility test case (see
   `.agents/architecture-invariants.md`).
7. Use a conventional-commit title, sign off commits (`git commit -s`), and sign
   the CLA.

## More

- Agent skills and resources: [`.agents/`](.agents/) (see [`.agents/README.md`](.agents/README.md))
- Architecture decisions: [`docs/rfcs/`](docs/rfcs/)
- How-to guides: [`docs/how-to/`](docs/how-to/)
