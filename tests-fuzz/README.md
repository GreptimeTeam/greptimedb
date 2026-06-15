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

## Phase 3 E2E Checkpoint Evidence Contract

Phase 3（端到端层）当前最小切片由 `fuzz_gc_e2e_cross_region` 和
`tests-fuzz/src/gc_e2e/phase3_harness.rs` 驱动，先覆盖：

- 真实 cluster 启动
- append-mode 写入 + flush 生成 SST
- compaction 生成可回收文件
- `BatchGcProcedure(full_file_listing=true|false)`（corpus 中同时保留 full-listing 与 fast-mode seed）
- repartition-like 与 follower-like 场景证据
  - repartition-like 当前覆盖 **post-GC repartition metadata reconciliation**：通过
    `BatchGcProcedure` 的 `UpdateRepartition` 测试入口消费
    `FileRefsManifest.cross_region_refs`，校验 repartition 元数据从旧目标 region
    调整到相关目标 region；它**不**声明已覆盖 pre-GC deletion-decision 保护。
  - follower-like 当前以 lagging-reader protected set 近似表达。
- GC 后 object-store / manifest / row-count checkpoint 校验

当前 failure evidence 至少包含：

- target name
- seed
- flush rounds / full_file_listing / compaction wait 等覆盖参数
- replay trace
- deleted_files / need_retry_regions / processed_regions
- post-GC reachable snapshot（当前切片以 manifest-after-gc 为准）
- repartition-like post-GC metadata reconciliation stage 与
  `protects_gc_deletion_decision=false` 标记
- follower-like / lagging-reader protected files（`follower_required_files`）
- concise evidence summary

当前 invariant 重点是：

- `deleted_files ∩ reachable_after_gc == ∅`
- `gc_report.deleted_files` 与 object-store 实际删除差集一致
- `manifest_after_gc == sst_after_gc`
- `deleted_files ∩ follower_required_files == ∅`
- row-count checkpoint 仍正确

注意：`scenario_kind=repartition_like` 当前仅证明 post-GC repartition 元数据可根据
crafted `FileRefsManifest.cross_region_refs` 完成 reconciliation；真正的 pre-GC
cross-region reference deletion-protection 还需要让真实 `BatchGcProcedure` 在
`Acquiring` 阶段从相关 manifest 获取 cross-region refs，并在进入 `Gcing` 前形成保护集。

### Phase 3 E2E Smoke Replay Corpus

Phase 3（真实实现 / E2E 层）维护独立的稳定 smoke replay corpus，放在：

- `tests-fuzz/corpus/phase3_e2e/`

它和 Phase 2 的 `phase2_mock` corpus 分开维护。每个 seed 文件使用 `key=value`
格式，当前字段为：

- `seed`
- `flush_rounds`
- `full_file_listing`
- `compaction_wait_secs`
- `table_shape`
- `scenario_kind`

默认回放命令：

```bash
cargo test -p tests-fuzz test_phase3_e2e_replay_corpus -- --nocapture
```

如果要回放自定义 corpus，可覆盖目录：

```bash
GT_PHASE3_SEED_DIR=/path/to/phase3_e2e cargo test -p tests-fuzz test_phase3_e2e_replay_corpus -- --nocapture
```

单 seed / cargo-fuzz 复现可以使用目标内的 override：

```bash
GT_FUZZ_OVERRIDE_SEED=7 \
GT_FUZZ_OVERRIDE_FLUSH_ROUNDS=4 \
GT_FUZZ_OVERRIDE_FULL_FILE_LISTING=true \
GT_FUZZ_OVERRIDE_COMPACTION_WAIT_SECS=2 \
cargo fuzz run fuzz_gc_e2e_cross_region --fuzz-dir tests-fuzz -D -s none
```

建议的 CI 形态：

- PR smoke：运行 `test_phase3_e2e_replay_corpus`，保持 seed 数量少、超时短。
- Nightly：对 `fuzz_gc_e2e_cross_region` 做更长时间运行，并把新的高价值失败 seed
  最小化后提升到 `tests-fuzz/corpus/phase3_e2e/`。
- Fast/full mode guard：至少保留一组相同 `flush_rounds`、`table_shape`、`scenario_kind`
  的 `full_file_listing=true` 与 `false` seed；`test_phase3_e2e_compare_fast_and_full_listing_modes`
  会检查 fast mode 的删除数量不超过 full-listing mode，并在失败时输出 object-store diff
  与 `GcReport.deleted_files` 证据。
- Follower-like guard：`scenario_kind=follower_like` seed 会记录非空
  `follower_required_files`，并校验这些文件没有出现在 `GcReport.deleted_files` 或
  object-store 删除差集中。当前这是 lagging-reader/follower-like 近似：protected set 来自
  GC 后仍在 manifest/object-store 中的文件，用于证明 validator 与证据链能捕获 follower
  overlap；它还不是完整的分布式 follower lag / role-switch 压力测试。
- 失败 artifact 至少保留 seed 文件、override 环境变量、`Phase3E2eEvidence::concise_summary()`、
  replay trace、`deleted_files` / `need_retry_regions` / `processed_regions`、post-GC manifest
  与 object-store checkpoint。这样 triage 时可以确认真实 cluster、`BatchGcProcedure` 与
  datanode GC 路径的证据链。

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
