# Fuzz Test for GreptimeDB

## Setup
1. Install the [fuzz](https://rust-fuzz.github.io/book/cargo-fuzz/setup.html) cli first.
```bash
cargo install cargo-fuzz
```

2. Start GreptimeDB
3. Copy the `.env.example`, which is at project root, to `.env` and change the values on need.

### For stable fuzz tests
Set the GreptimeDB MySQL address.
```
GT_MYSQL_ADDR = localhost:4002
```

### For unstable fuzz tests
Set the binary path of the GreptimeDB:
```
GT_FUZZ_BINARY_PATH = /path/to/
```

Change the instance root directory(the default value: `/tmp/unstable_greptime/`)
```
GT_FUZZ_INSTANCE_ROOT_DIR = /path/to/
```
## Run
1. List all fuzz targets
```bash
cargo fuzz list --fuzz-dir tests-fuzz
```

2. Run a fuzz target.
```bash
cargo fuzz run fuzz_create_table --fuzz-dir tests-fuzz -D -s none
```

## Crash Reproduction
If you want to reproduce a crash, you first need to obtain the Base64 encoded code, which usually appears at the end of a crash report, and store it in a file.

Alternatively, if you already have the crash file, you can skip this step.

```bash
echo "Base64" > .crash
```
Print the `std::fmt::Debug` output for an input.

```bash
cargo fuzz fmt fuzz_target .crash --fuzz-dir tests-fuzz -D -s none
```
Rerun the fuzz test with the input.
You can override fuzz input with environment variables. For example, to override fuzz input like:

```
FuzzInput {
    seed: 6666,
    actions: 175
}
```

you can run with `GT_FUZZ_OVERRIDE_SEED=6666` and `GT_FUZZ_OVERRIDE_ACTIONS=175`:

```bash
GT_FUZZ_OVERRIDE_SEED=6666 GT_FUZZ_OVERRIDE_ACTIONS=175 cargo fuzz run fuzz_target .crash --fuzz-dir tests-fuzz -D -s none
```

For more details, visit [cargo fuzz](https://rust-fuzz.github.io/book/cargo-fuzz/tutorial.html) or run the command `cargo fuzz --help`.

## Phase 2 Scheduler/Mock Regression Seeds

Phase 2（调度/Mock 层）目前维护一组稳定 replay seeds，放在：

- `tests-fuzz/corpus/phase2_mock/`

每个 seed 文件使用简单的 `key=value` 格式，记录：

- `seed`
- `action_count`
- `dropped_region_bias`
- `route_override_bias`
- `retry_bias`
- `full_listing_bias`

默认回放命令：

```bash
cargo test -p meta-srv test_phase2_mock_fuzz_replay_corpus -- --nocapture
```

如果要回放自定义 corpus，可覆盖目录：

```bash
GT_PHASE2_SEED_DIR=/path/to/phase2_mock cargo test -p meta-srv test_phase2_mock_fuzz_replay_corpus -- --nocapture
```

这组回放测试会复用 Phase 2 的 scheduler/mock harness、fixture builder 与 oracle，确保：

- dropped-region / route override / retry 组合可重放
- 失败输出带 seed 与 evidence summary，便于 triage
- 已提升为稳定 seed 的输入持续受回归保护

## Repartition Metric Dump Artifacts

For `fuzz_repartition_metric_table`, dump artifacts are written under one run directory.

- Table data snapshots: `<logical_table>.table-data.csv`
- SQL traces per logical table: `<logical_table>.trace.sql`
- Seed metadata: `seed.meta`

SQL trace behavior:

- Insert SQL is appended after successful execution with comment fields including
  `started_at_ms` and `elapsed_ms`.
- Repartition events are broadcast to all logical table trace files with comment fields including
  `action_idx`, `started_at_ms`, `elapsed_ms`, and SQL text.

Run directory lifecycle:

- On success, the run directory is cleaned up.
- On failure, the run directory is retained for CI/local diffing.
