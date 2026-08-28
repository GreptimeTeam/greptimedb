# Agent Guidelines for Query Performance Tests

- Keep GitHub Actions YAML thin. Put non-trivial control flow, case expansion,
  report generation, and metadata writing in scripts under `.github/scripts/`;
  workflow steps should mostly invoke those scripts.
- Runner lifecycle: the default path provisions one ephemeral Aliyun ECS
  instance per run via `.github/scripts/aliyun-ecs-runner-provision.py` and
  always releases it via `aliyun-ecs-runner-teardown.py`; a scheduled janitor
  workflow sweeps leftovers. Build caches live on that instance's system disk
  and are discarded with the VM. Runs do not share a workflow concurrency
  group. The ECS custom image is built from the runner Dockerfile by
  `.github/runner-scale-sets/query-regression/ecs-image/build-ecs-image.py`;
  keep the Dockerfile the single source of the tool contract. Dispatching with
  any other `runner` value treats it as a literal self-hosted runner label
  (see `ecs-image/bootstrap-runner-host.sh` for preparing such a host).
- Query regression PR runs should build base/candidate binaries once, then run
  the default case set. Do not hard-code a single case such as
  `promql_pushdown_7913` into the workflow path.
- Scheduled nightly comparison lives in `query-regression-nightly.yml`: it
  waits for a successful Nightly Build, then calls `query-regression.yml`
  with the previous vs current nightly SHAs. Keep SHA selection in
  `.github/scripts/query-regression-nightly-refs.py`.
- PR comment admission is two workflows: `slash-command-dispatch.yml`
  (peter-evans/slash-command-dispatch) decides whether a `/command` should
  run and `repository_dispatch`es payload context; `query-regression-slash.yml`
  handles `/query-regression` (allowlist, merge SHA, reusable call). Keep
  case-arg validation in `.github/scripts/query-regression-slash.py`. There
  is no PR-label trigger. To add another command, list it in the dispatcher
  and add a `repository_dispatch` handler.
- The case DSL is not required to keep compatibility inside this PR. When the
  DSL changes, update TOML cases, the outer lifecycle script, Rust helpers, and
  docs together.
- `[case]` is report metadata only. `[scenario]` is the executable regression
  configuration and must include `kind`, data layout, tables, queries, and
  thresholds. Rust owns case schema, defaults, validation, and normalized plan
  output through `query_perf_fixture plan`. `.github/scripts/query-regression-run.py`
  owns process lifecycle; `query_regression_runner` consumes normalized plans,
  frontend endpoints, direct-SST materialization requests, and OTLP target/finalize
  requests.
- Keep the direct-SST generator generic. Issue-specific behavior belongs in case
  files and thresholds, not in Rust generator logic.
- Before pushing perf harness changes, run at least:
  - the Python tests in the `test-tooling` job of
    `.github/workflows/query-regression.yml` (ubuntu-latest, not the ECS runner).
    The Checks workflow runs the same tests on ordinary PRs.
  - `cargo fmt --all -- --check`
  - `cargo build -p cmd --bin query_perf_fixture --features dev-tools`
  - `cargo build -p cmd --bin query_regression_runner --features dev-tools`
  - exercise the outer lifecycle script and Rust fixture generator against all
    built-in cases when the DSL or workflow case selection changes.
