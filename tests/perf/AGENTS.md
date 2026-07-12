# Agent Guidelines for Query Performance Tests

- Keep GitHub Actions YAML thin. Put non-trivial control flow, case expansion,
  report generation, and metadata writing in scripts under `.github/scripts/`;
  workflow steps should mostly invoke those scripts.
- Query regression PR runs should build base/candidate binaries once, then run
  the default case set. Do not hard-code a single case such as
  `promql_pushdown_7913` into the workflow path.
- The case DSL is not required to keep compatibility inside this PR. When the
  DSL changes, update TOML cases, the Python runner, Rust fixture generator, and
  docs together.
- `[case]` is report metadata only. `[scenario]` is the executable regression
  configuration and must include `kind`, data layout, tables, queries, and
  thresholds. Rust owns case schema, defaults, validation, and normalized plan
  output through `query_perf_fixture plan`; `tests/perf/query_regression_runner.py`
  should only orchestrate from that normalized JSON.
- Keep the direct-SST generator generic. Issue-specific behavior belongs in case
  files and thresholds, not in Rust generator logic.
- Before pushing perf harness changes, run at least:
  - `uv run --no-project python -m py_compile .github/scripts/query-regression-run.py .github/scripts/query-regression-summary.py .github/scripts/query-regression-pr-metadata.py tests/perf/query_regression_runner.py`
  - `cargo fmt --all -- --check`
  - `cargo build -p cmd --bin query_perf_fixture --features dev-tools`
  - dry-run the Python runner and Rust fixture generator against all built-in
    cases when the DSL or workflow case selection changes.
