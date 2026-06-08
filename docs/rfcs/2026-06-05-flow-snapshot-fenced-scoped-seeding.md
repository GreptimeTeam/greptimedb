# Flow 增量读 Snapshot-Fenced Scoped Seeding 修复方案

## 背景

当前 Flow batching 增量读为了安全从 `FullSnapshot` 切到 `Incremental`，会倾向于执行 **unfiltered full query**：

```text
FullSnapshot + SQL + incremental enabled
=> drain dirty signal
=> run unfiltered full query
=> use returned per-region watermark as checkpoint
=> switch to Incremental
```

这个方案的 correctness 比较直接：full query 读完整 source snapshot，返回的 watermark 可以作为后续 incremental 的 lower bound。

但对大表来说，unfiltered full query 可能完全不现实。我们需要一个 bounded 方案：避免全表扫，同时不让 checkpoint 跳过未处理数据。

## 问题定义

不能简单改成：

```text
dirty-window filtered full query
=> returned watermark = H
=> checkpoint = H
=> switch to Incremental
```

因为 scoped query 只覆盖部分时间窗口。

假设 scoped query 只重算窗口 `W`，但返回 watermark `H`：

```text
seq <= H && time in W      已处理
seq <= H && time not in W  未处理
```

如果此时 checkpoint 推到 `H`，后续 incremental 只读：

```text
seq > H
```

那么 `seq <= H && time not in W` 的数据会被永久跳过。

所以核心约束是：

> `checkpoint = H` 之前，必须证明 `seq <= H` 中所有需要覆盖的数据都已经被 sink 覆盖。

dirty window 不能单独作为这个证明。

## 术语

### Unfiltered full query

不加 dirty-window filter，直接按原 query 扫整个 source：

```sql
SELECT ..., date_bin(...) AS time_window, sum(v)
FROM src
GROUP BY ...
```

### Scoped full query

保留 full recompute 语义，但只重算某些时间窗口：

```sql
SELECT ..., date_bin(...) AS time_window, sum(v)
FROM src
WHERE ts >= $window_start AND ts < $window_end
GROUP BY ...
```

它不是 incremental delta，而是对这些窗口做完整重算。

### Snapshot fence / upper sequence

每个 source region 固定一个读取上界：

```text
region_id -> H
```

所有 seeding query 都必须读：

```text
source sequence <= H
```

这样多个 scoped query 才是在同一个逻辑快照上分批处理。

## 目标

1. 避免大表上的 unfiltered full query。
2. 允许用 scoped full query 分批 seed / repair。
3. 保证 `FullSnapshot -> Incremental` 不会 checkpoint skip。
4. 支持 fallback 后 bounded repair。
5. dirty windows 继续作为调度/coverage hint，但不作为 checkpoint correctness 的唯一证明。

## 非目标

1. 不在本 RFC 中重构整个 Flow execution loop。
2. 不要求所有 query shape 都支持 bounded seeding。
3. 没有 time-window 的 query 不强行支持 scoped seed。
4. 不把 dirty window 清空作为 checkpoint advance 的充分条件。

## 核心设计：Snapshot-Fenced Scoped Seeding

### 状态机

新增 seeding 状态：

```rust
enum CheckpointMode {
    FullSnapshot,
    Seeding,
    Incremental,
}
```

或者更明确地引入独立 checkpoint state：

```rust
enum CheckpointState {
    FullSnapshot,
    Seeding {
        low: BTreeMap<RegionId, Sequence>,
        high: BTreeMap<RegionId, Sequence>,
        generation_id: u64,
        pending_windows: DirtyTimeWindows,
        covered_windows: DirtyTimeWindows,
    },
    Incremental {
        checkpoints: BTreeMap<RegionId, Sequence>,
    },
}
```

含义：

- `low`：当前已安全消费的 checkpoint。
- `high`：本轮 seeding 固定的 snapshot upper sequence。
- `pending_windows`：本轮需要 scoped full recompute 的窗口。
- `covered_windows`：本轮已经处理成功的窗口。
- `generation_id`：区分 seeding 期间新来的 dirty。

## Seeding 流程

### 1. 捕获 seeding fence

开始 seeding 时捕获每个 source region 的 upper sequence：

```text
high = { region_1: H1, region_2: H2, ... }
```

这个 `high` 必须是后续所有 scoped full query 的固定 snapshot 上界。

如果当前系统已经支持 `<= sequence` snapshot read，则后续 query 都带：

```text
FLOW_SNAPSHOT_UPPER_SEQS = high
```

或等价 extension。

### 2. Freeze 本轮 coverage

进入 seeding 时 freeze 本轮要处理的窗口集合：

```text
pending_windows = current required windows
```

之后新来的 dirty 不混进本轮，而是进入下一代：

```text
live_dirty_windows / next_generation_dirty
```

避免一边 seed 一边扩大 checkpoint 证明范围。

### 3. 分批执行 scoped full query

每次取一批 windows：

```text
batch = pending_windows.take(max_window_cnt)
```

生成 scoped full query：

```sql
... WHERE time_window in batch ...
```

并且附带 snapshot fence：

```text
read source seq <= high[region]
```

执行成功后：

```text
covered_windows += batch
pending_windows -= batch
```

执行失败则恢复 batch 到 `pending_windows`。

### 4. 完成后统一 checkpoint advance

只有满足：

```text
pending_windows is empty
&& all participating regions proved watermark <= high or == high
&& no stale cursor / no missing region proof
```

才允许：

```text
checkpoint = high
mode = Incremental
```

注意：**每个 scoped query 成功都不能单独 advance checkpoint**。checkpoint 只在整个 generation coverage 完成后推进。

## 初始 Seed vs Fallback Repair

### 初始 Seed

如果没有历史 checkpoint，dirty windows 不够，因为历史数据不一定有 dirty 记录。

初始 seed 的 `pending_windows` 应该来自：

1. retention 范围内所有 time buckets；
2. 或 source 表实际存在的时间范围；
3. 或用户指定的 seed range。

流程：

```text
capture high
enumerate required historical windows
scoped full query each window with seq <= high
all windows covered
checkpoint = high
mode = Incremental
```

如果 query 没有 time-window，或者无法枚举历史窗口：

```text
不能 bounded seed
=> 保持 FullSnapshot/scoped mode
=> 或要求显式允许 unfiltered full seed
```

### Fallback Repair

如果已经有 checkpoint：

```text
low = previous checkpoint
```

fallback repair 可以更小：

```text
capture high
repair windows affected between low and high
checkpoint = high
mode = Incremental
```

这里关键是如何得到 affected windows。

可选方式：

1. dirty notification 可靠且有 barrier：
   - 使用 dirty windows。
   - 需要证明所有 `seq <= high` 的 dirty notification 都已被 Flow 处理。

2. dirty notification 不足以证明：
   - 需要 sequence-level bounded scan `(low, high]` 来发现 affected windows。
   - 或者直接 bounded incremental drain `(low, high]` 后再 checkpoint。

## Dirty Window 的角色

dirty window 应该降级为：

1. 调度信号；
2. scoped full query 的候选 coverage 范围；
3. 失败恢复信号；
4. fallback repair 的 hint。

但 dirty window 不应该单独证明：

```text
all seq <= H are covered
```

checkpoint correctness 必须由 snapshot fence + coverage completion 证明。

## Snapshot Read 要求

需要 query/datanode 支持类似 extension：

```text
FLOW_SNAPSHOT_UPPER_SEQS
```

语义：

```text
source scan must not read rows with sequence > upper_seq[region]
```

如果已有 `<= sequence` snapshot read，这部分可以复用。

可选增强：

```text
FLOW_INCREMENTAL_AFTER_SEQS = low
FLOW_INCREMENTAL_UNTIL_SEQS = high
```

用于 bounded delta drain：

```text
low < seq <= high
```

## Checkpoint Advance 规则

当前逻辑大概是：

```text
if can_advance_checkpoints && watermark complete:
    advance checkpoint
```

需要改成 coverage-aware：

```rust
enum QueryCoverage {
    UnfilteredFull,
    ScopedFull {
        windows: Vec<TimeRange>,
        snapshot_upper: Option<WatermarkMap>,
    },
    BoundedSeedingChunk {
        generation_id: u64,
        windows: Vec<TimeRange>,
        upper: WatermarkMap,
    },
    IncrementalDelta,
}
```

只有这些情况允许 checkpoint advance：

1. `UnfilteredFull` 成功且 watermark complete；
2. `BoundedSeedingChunk` 全部 generation coverage 完成；
3. `IncrementalDelta` 成功且 watermark complete。

普通 `ScopedFull` 成功不能直接 advance checkpoint。

## 最小落地方案

### Phase 1：禁止 unsafe scoped seed

短期先保证不出错：

- scoped dirty-window full query 不允许直接 `FullSnapshot -> Incremental`。
- 如果不走 unfiltered full query，就保持 FullSnapshot/scoped mode。
- 给大表加 guard，避免自动 unfiltered seed。

效果：

```text
不会 skip，但可能进不了 incremental
```

### Phase 2：支持 snapshot-fenced scoped seed

实现：

1. 捕获 `high`。
2. 保存 `Seeding { high, pending_windows }`。
3. scoped full query 附带 `seq <= high`。
4. pending 全部完成后 checkpoint 到 `high`。

效果：

```text
避免全表一次性扫描
支持 bounded seeding
correctness 可证明
```

### Phase 3：优化 fallback repair

实现：

1. dirty notification 携带 region sequence，或提供 dirty barrier。
2. fallback 时确定 `(low, high]` 影响的 windows。
3. 只 repair affected windows。
4. repair 完成后 checkpoint 到 `high`。

效果：

```text
fallback 不需要全量 historical windows
```

## 代码改动点

### `src/flow/src/batching_mode/state.rs`

新增 seeding state：

```rust
Seeding {
    low: BTreeMap<u64, u64>,
    high: BTreeMap<u64, u64>,
    pending_windows: DirtyTimeWindows,
    covered_windows: DirtyTimeWindows,
}
```

### `src/flow/src/batching_mode/task.rs`

- `PlanInfo` 增加 coverage 类型。
- `gen_query_with_time_window()` 区分：
  - unsafe scoped full；
  - bounded seeding chunk；
  - incremental delta。
- scoped seeding query 附带 snapshot upper seq。

### `src/flow/src/batching_mode/task/ckpt.rs`

- checkpoint advance 不再只看 `can_advance_checkpoints`。
- seeding chunk 成功只更新 coverage。
- coverage 完成后统一 advance checkpoint。

### `src/query/src/options.rs`

新增 query extension：

```text
FLOW_SNAPSHOT_UPPER_SEQS
```

或：

```text
FLOW_INCREMENTAL_UNTIL_SEQS
```

### datanode / scan path

确保 TableScan 能按 region upper sequence 打开 snapshot：

```text
scan snapshot seq <= upper_seq
```

## 正确性不变量

1. checkpoint `C` 表示：

   ```text
   all rows with seq <= C are reflected in sink
   ```

2. scoped full query 成功只证明：

   ```text
   rows in covered windows and seq <= H are reflected in sink
   ```

3. 只有当 required coverage complete 时：

   ```text
   covered_windows == required_windows
   ```

   才能：

   ```text
   checkpoint = H
   ```

4. seeding 期间新 dirty 不影响当前 generation：

   ```text
   new dirty => next generation or incremental after H
   ```

## 测试计划

1. scoped full query 成功但 pending windows 未清空：
   - 不 advance checkpoint。
   - 不进入 incremental。

2. 多 batch seeding：
   - batch1 成功不 advance。
   - batch2 成功后 pending empty。
   - advance checkpoint 到 fixed high。

3. seeding 期间新 dirty：
   - 不混入 current generation。
   - checkpoint 仍只到 high。
   - 新 dirty 在 incremental 后触发下一轮。

4. snapshot upper seq 生效：
   - query 不读 `seq > high` 的数据。

5. fallback repair：
   - old checkpoint = low。
   - high captured。
   - repair affected windows。
   - checkpoint = high。

6. no time-window query：
   - 不允许 scoped seeding。
   - 只能 unfiltered seed 或保持 full mode。

## 当前 #8248 的关系

#8248 只修：

```text
incremental query 成功但 watermark fallback 后 dirty signal 丢失
```

它不解决 bounded seeding。

本 RFC 应该作为后续独立设计/PR，不建议混进 #8248。
