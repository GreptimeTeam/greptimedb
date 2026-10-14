# Grafana Mimir Streaming PromQL Engine 源码调研报告

> **English abstract:** Source-level study of Mimir's metadata-first series streaming, grouped aggregation, vector matching, subqueries, histogram semantics, memory accounting, compatibility fallback, and lessons for the native GreptimeDB PromQL engine. Based on pinned source commits; no implementation or benchmark was run.

> 日期：2026-10-14  
> 目标：为 GreptimeDB 下一代 PromQL 引擎的设计提供经源码验证的参考，而非直接移植 Go 实现。  
> 源码快照：`grafana/mimir@51704a4f8a04dcb46a96e167637856f561678764`（上游 main，提交时间 2026-10-13 UTC）  
> 对照原型：`GreptimeTeam/greptimedb#9556`，`killme2008/greptimedb@e0df7f17e4b904bde017e935125ed779f4ebd7a6`。

## 1. 结论摘要

1. **MQE 的核心不是按时间点输出，而是把整个执行计划拆成串联的 series-at-a-time operator。** 最重要的协议是先取 `SeriesMetadata()`，再依同一顺序反复 `NextSeries()`。这使聚合和匹配算子能够预先安排数据依赖并尽早释放样本。
2. **它并非完全流式。** `SeriesMetadata()` 一次加载完整 series 标签集合；instant `topk`/`bottomk` 需要预先消费输入来决定排序与输出身份，range `topk`/`bottomk` 则主要在 metadata 阶段分组、在 `NextSeries()` 阶段逐组计算；`count_values` 在 metadata 阶段提前读值以确定动态输出标签。Vector matching 和 CSE 会缓存暂时无法释放的 series。
3. **两个输出阶段明确分开。** Metadata 阶段确定可能产生的 series；样本阶段逐条拉取。Metadata 中出现的 series 最后可以没有有效样本；这不是错误。
4. **分组聚合的关键是 last contributing series。** Metadata 阶段给每个 group 记录尚未处理的输入数及其最后一个输入序号。只有该 group 完成时才输出和释放，而不是全量 materialize 后统一 `GROUP BY`。
5. **二元向量匹配是内存设计的真正难点。** Metadata 确定 match group 和输出 label set；运行时仍必须逐时间点检测 cardinality violation。MQE 按最后使用位置调整输出顺序，配合 `InstantVectorOperatorBuffer` 处理两侧输入先后顺序。
6. **Range selector 不是简单的 `Vec<samples>`。** 独立的 `RangeVectorOperator` 有 `NextSeries` 和 `NextStepSamples`，通过 float/histogram ring buffer 复用重叠窗口，保留 `(start, end]` 边界、`offset`、`@` 和嵌套子查询时间网格。
7. **Float 和 native histogram 是执行系统的基础类型。** 原始样本流与输出向量分别保存 float/histogram 样本，支持混合样本窗口警告、histogram 内存与生命周期管理。
8. **算子生命周期比 `next()` 复杂，但各阶段有原因。** `Prepare`、`AfterPrepare`、`FinishedReading`、`Finalize`、`Close` 分开，支持共享子图、提前终止、stats/annotations 归并和资源释放。Rust 不必原样照抄这些阶段，但必须覆盖相同语义。
9. **计划 IR 与执行算子分离。** PromQL AST → 可版本化 Query Plan → 优化 → Materializer → Operators。CSE、multi-aggregation、range-vector splitting 和远端执行都在此基础上扩展。
10. **真实兼容性不是一句“完全支持”。** MQE 自己保留了 Prometheus 上游用例的部分禁用/注释、差分测试和 fuzz 测试；实际环境还使用 planning-time fallback。GreptimeDB 的完全兼容应依赖固定 Prometheus 版本的测试矩阵，不能仅验证旧 GreptimeDB 的输出。
11. **适合 GreptimeDB 的是协议与算法，而非全部工程结构。** #9556 已有 `VisibleSeriesBlock`、`RangeProcessor`、`SeriesGrid`、`QueryProgram`；下一步应替换全量 materialization 的上层执行模型，保留成功的底层 kernel，并以 Parquet Footer `SeriesRun` 为优先快读取后端。
12. **版权边界。** Mimir 顶层许可证是 AGPL-3.0，相关文件多有 `SPDX-License-Identifier: AGPL-3.0-only`。建议研究设计、自己实现 Rust 接口与算法；不要未经法律评估直接拷贝代码或机械翻译。

## 2. 研究范围与证据质量

本次直接读取了 MQE 的接口、planner、materializer、evaluator、selector、range window、聚合、二元匹配、subquery、CSE、memory pool、fallback、测试与配置文件，并核对 #9556 的实际对应模块。不是实际编译运行 Mimir，也不是吞吐 benchmark 复测。凡涉及性能改进量均不据此推断具体倍数。

重点源码：

- [README.md](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/README.md)
- [types/operator.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/types/operator.go)
- [types/data.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/types/data.go)
- [planning.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/planning.go)、[planning/materialize.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/planning/materialize.go)
- [evaluator.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/evaluator.go)、[query.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/query.go)
- [aggregations/aggregation.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/aggregations/aggregation.go)
- [binops/one_to_one_vector_vector_binary_operation.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/binops/one_to_one_vector_vector_binary_operation.go)
- [binops/grouped_vector_vector_binary_operation.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/binops/grouped_vector_vector_binary_operation.go)
- [selectors/range_vector_selector.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/selectors/range_vector_selector.go)
- [operators/subquery.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/subquery.go)
- [operators/operator_buffer.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/operator_buffer.go)
- [comparisons/comparisons_test.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/comparisons/comparisons_test.go)、[fuzz/fuzz_test.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/fuzz/fuzz_test.go)

## 3. 真实执行链

```text
HTTP PromQL API
  └─ Engine.NewInstantQuery / NewRangeQuery
       └─ parser + AST preprocessing
            └─ QueryPlanner
                 ├─ AST passes
                 ├─ typed Query Plan IR (supports versioning)
                 └─ plan passes (CSE, matching/filter/metadata hints,
                                  multi-aggregation, range-split/cache, etc.)
                      └─ Materializer
                           └─ Root Operator DAG
                                ├─ Prepare + AfterPrepare
                                ├─ SeriesMetadata()  [标签与依赖阶段]
                                ├─ NextSeries()     [按 series 拉取]
                                │    └─ RangeVector 则在每条 series 内用 NextStepSamples()
                                ├─ FinishedReading + Finalize
                                └─ Close
                                     └─ PromQL Result + Stats + Annotations
```

关键点：**`SeriesMetadata()` 不是读取数据后的后处理，而是物理执行算法的一部分。** 聚合、匹配、重排、过滤可以仅用 labels 构建 series-level dependency graph。随后 `NextSeries` 按顺序消费数据。

`planning/plan.go` 明确定义独立 QueryPlan 版本（快照包含 `QueryPlanV21`），说明分布式计划演进需要版本兼容；不意味着 GreptimeDB 第一阶段就应该实现同样复杂的远端 plan wire 协议。

## 4. Operator 接口与生命周期

[types/operator.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/types/operator.go) 中核心接口：

| 接口 | 功能 | 为什么需要 |
| --- | --- | --- |
| `Operator.Prepare()` | 运行前递归准备 | 获取资源与注册请求，不提前消费流 |
| `AfterPrepare()` | 整个图 Prepare 后第二阶段准备 | 共享子图、合并 selector 请求等场景依赖其他算子已准备完成 |
| `SeriesMetadata(ctx, matchers)` | 返回有序的候选输出 series 标签 | 分组/匹配/输出依赖预计算；还可向下传 matcher hint |
| `InstantVectorOperator.NextSeries()` | 返回下一条 series 的稀疏 evaluation point 序列 | 无须持有全部输入 samples |
| `RangeVectorOperator.NextSeries()` | 切换到下一条 raw range series | 划定范围向量生命周期 |
| `RangeVectorOperator.NextStepSamples()` | 为当前 series 的下一个 evaluation step 提供窗口视图 | 复用重叠窗口及 histogram ring buffer |
| `FinishedReading()` | 上层声明不再消费，递归释放暂存数据 | short-circuit 和未完全消费的查询不能泄漏缓存 |
| `Finalize()` | 返回统计与 annotations | 保证可组合的 warning/stats 传播 |
| `Close()` | 最终释放，任何阶段出错后均安全、可重复 | cancellation/error 下的资源安全 |

限制与隐患：单个 operator **不要求线程安全**；README 明确引擎当前以单 goroutine 执行 operators。并行在更外层或显式的 operator 内进行。接口还要求 `SeriesMetadata` 与 `NextSeries` 顺序一致，但 metadata 一次性返回完整列表，导致内存下限仍约为 `O(S × 标签大小)`。

**可移植的原则**： metadata-first + ownership/lifecycle + pull-based backpressure。**不必复制** Go 的所有方法、`interface{}` 树和池的具体写法。

## 5. 主要算法审阅

### 5.1 Selector：存储与计算的边界

[selectors/selector.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/selectors/selector.go) 在 `SeriesMetadata` 阶段使用 `Querier.Select()` 取得 series 集合和标签，并以 FIFO 列表保留 storage series 引用。`Next()` 才返回下一条的迭代器。

[instant_vector_selector.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/selectors/instant_vector_selector.go) 在每个 evaluation step 通过 memoized iterator 做 lookback、offset、`@`、staleness 与 float/histogram 处理；[range_vector_selector.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/selectors/range_vector_selector.go) 用两个 ring buffer 维护重叠窗口并复用内存。

`ComputeQueriedTimeRange` 的关键边界是窗口下界开、上界闭 `(t-window, t]`，并区分样本时间和 evaluation 时间。这个约束应属于统一 selector/range 语义，而不是每个 kernel 各自推导。

### 5.2 聚合：组完成即归约/释放，而不是全量 group-by

[aggregations/aggregation.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/aggregations/aggregation.go) 的 `ComputeGroups()` 会在 metadata 阶段为每条输入标出所属 group，并记录：

- `remainingSeriesCount`：此 group 尚未消费的输入数；
- `lastSeriesIndex`：最后一条会贡献此 group 的输入序号；
- `remainingInnerSeriesToGroup`：输入 ordinal → group 的紧凑映射。

输出 groups 按 `lastSeriesIndex` 排序。`NextSeries()` 不断从 child 拉取数据，直到**下一待输出 group**完成，随后归约、输出、释放。这样可以让上层聚合（如 `max(sum by(region)(m))`）逐组继续归约，避免全部 input-series-grid 常驻。

结论：GreptimeDB 可以直接借鉴“最后贡献者完成”的算法，但为并行执行应将“最后一个完成”推广为**计数归零 + 有确定性合并顺序**，不能依赖任务调度先后顺序决定浮点累加结果。

### 5.3 Binary matching：metadata 不足以证明时间点上的 cardinality

[one_to_one](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/binops/one_to_one_vector_vector_binary_operation.go)、[grouped](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/binops/grouped_vector_vector_binary_operation.go)。

元数据先按 `on`/`ignoring` 求 match key，构建可能的输出标签、记录两边依赖关系。没有对应时间点样本的 series 可以在 metadata 中存在而在执行时不输出点。grouped matching 还用每个时间点的 presence 记录检查一侧唯一性，违反时报告明确错误。

两边 series 的读取顺序通常不同，因此 [operator_buffer.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/operator_buffer.go) 保留尚未被使用的 series，并尽快丢弃永远不会被用到的数据。`grouped::sortSeries()` 倾向按 many-side 最后使用位置排序以减小缓冲。

**重要结论**：`NextSeries()` 不是“完全 O(1) 内存”的魔法。真正的上界取决于 match graph、输出顺序、缓存策略与查询内存预算。复杂 `group_left`/`group_right` 在极端多对多形态下仍可能需要相当大的缓冲。

### 5.4 TopK、count_values：动态输出 labels 的例外

[aggregations/README.md](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/aggregations/README.md) 明确指出 `topk`/`bottomk` 和 `count_values` 可能在 metadata 阶段提前读取全部输入。原因不是“开发偷懒”，而是输出 series 的身份依赖实际样本和值。

不过 [topkbottomk/topk_bottomk.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/aggregations/topkbottomk/topk_bottomk.go) 又区分 instant/range query：instant 查询要按值确定输出排序；range 查询采用逐组 heap/缓冲策略，[range_query.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/aggregations/topkbottomk/range_query.go) 能按组释放。

[count_values.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/aggregations/count_values.go) 更清楚：它将 sample 值格式化为输出 label value，在扫描时动态创建结果标签集合，并按每个时间点计数。**该 label 是查询计算结果，不是对存储侧 metric-name/TSID 角色列的写入。**

对新引擎的含义：必须容许部分算子先消费数据再确定完整 metadata；不应为了统一接口而强行要求所有 metadata 都可只看标签推导。

### 5.5 Subquery：独立的子 evaluation grid

[planning/core/subquery.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/planning/core/subquery.go) 计算子查询的时间区间和 step，指出 subquery step 按 Unix epoch 0 对齐，而不是按外层查询每一个 evaluation instant 重置。

[operators/subquery.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/subquery.go) 把内部 instant-vector 的多个结果映射成外层 range-vector 的逐窗口视图。结论：正确实现应当**一次评价子网格并复用**，不能将子查询简单展开成重复执行外部查询的 N 次调用。

### 5.6 Native histograms 与 annotations

[types/data.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/types/data.go) 用分开的 `Floats []FPoint` 与 `Histograms []HPoint` 表达同一条 series 的混合时间序列，专用迭代器按时间合并。`RangeVectorStepData` 含边界信息、float/histogram 环形缓冲的视图、anchored/smoothed 等状态。

[functions/rate_increase.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/functions/rate_increase.go) 对 float 与 histogram 区分路径；混合样本、counter reset、bucket schema 的判断还会通过 annotation 通道反馈。

[operators/drop_name.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/drop_name.go) 与 `SeriesMetadata.DropName` 显示 metric-name 可以是延迟移除的输出属性，不能仅以物理列存在与否表达。`MetricNames` 甚至会暂存已被结果中移除的原始 metric name 供警告使用。

### 5.7 优化器、CSE、跨 region/远端

- AST 和 Query Plan 明确有不同的优化阶段。`planning.go` 注册 matcher propagation/reduction、静态空表达式、CSE、multi-aggregation、range-vector splitting/cache 等 passes。
- [CSE instant_vector_operator.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination/instant_vector_operator.go) 允许一个 source 被多个消费者共享，使用按 ordinal 的 ring buffer，仅在仍有人需要时保留样本。
- [multiaggregation/operator.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/optimize/plan/multiaggregation/operator.go) 对同一输入共享一次 `NextSeries`，更新多个聚合状态。
- `planning/plan.go` 定义逐版本演化的 wire plan；`optimize/plan/remoteexec` 体现远端子图执行。不过 GreptimeDB 已有自己的 region/physical-plan 协议，应优先复用现有 query/remote infrastructure，而不是另建一套 MQE 风格 RPC。

## 6. 资源控制与并发约束

Mimir 的 [types/limiting_pool.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/types/limiting_pool.go) 将 pool 申请与 query 内存计费绑定，按 slice **capacity** 而非 len 估算。Native histogram 还需要为每个 histogram 的内部 bucket buffer 预留估算容量。[types/stats.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/types/stats.go) 可跟踪每步 samples、annotation、聚合 stats。

`types/operator.go` 的 `Close()` 在错误后可调用且可重复；`FinishedReading()` 支持早停；`Finalize()` 在读取结束后汇总 annotations/stats。`AGENTS.md` 特别强调 pooled native histogram 指针别名可能导致错误结果甚至崩溃，这不只是内存泄露风险。

对 GreptimeDB：Rust RAII/`MemoryReservation` 能简化部分资源释放，但仍应保留**每查询预算、按节点计费、并发任务上界、取消传播、结果内存归属**这些能力。特别是 CSE/matching buffers 必须计入内存，不可遗漏或重复计费。

## 7. 兼容性现状：不要误读“完整兼容”

Mimir 有 [compat/fallback_engine.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/compat/fallback_engine.go)：只有 planning 返回 `NotSupportedError` 才退回 Prometheus 引擎，普通语义/运行时错误不能被吞掉。也可通过显式请求强制 fallback。

[comparisons/comparisons_test.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/comparisons/comparisons_test.go) 将两种引擎结果对比，并保留复制的上游测试数据。[testdata/upstream/README.md](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/testdata/upstream/README.md) 说明未支持用例被注释或将文件标记为 `.disabled`。本次快照可直接看到 `start_timestamps.test.disabled`、`type_and_unit.test.disabled`，而 `functions.test`、`histograms.test`、`fill-modifier.test` 仍有未支持标记。它们不能直接证明某功能永久缺失，但足以说明测试层尚非无条件 100% 覆盖。

[fuzz/fuzz_test.go](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/fuzz/fuzz_test.go) 对输入表达式、时间、step 做差分，并比较值、错误与 annotations。文档还列出因顺序不确定或 annotation 发出时机导致的预期差异。重要结论：**先固定 oracle 版本，再定义容许差异，不允许以大量“预期差异”掩盖语义回归。**

## 8. 与 GreptimeDB #9556 的架构对照

| 维度 | MQE | #9556 当前原型 | 建议 |
| --- | --- | --- | --- |
| 物理源 | Prometheus `Querier/Select` | Sidecar / Coalesced + 普通 Mito fallback | **Footer SeriesRun 优先、Mito 常规读取兜底**，同一来源契约 |
| 解析与 IR | PromQL parser → 自有 Query Plan IR | PromQL AST + DataFusion logical fragment + `QueryProgram` | 增加**小型 typed PromQL IR**，不引入全套分布式 wire plan |
| 执行模式 | metadata → `NextSeries` | `read_series` → `SeriesGrid` → `outputs` → `BTreeMap` → evaluator | 替换全量集中收集，支持逐 group 归约；不要求所有算子早输出 |
| 输入窗口 | RangeVector 的 ring buffers | `VisibleSeriesBlock` + 专用 range kernel | 保留 block/kernels，加入重叠窗口 ring cursor |
| 聚合 | 可提前完成分组 | 已有 RangeAggregate 局部聚合；上层仍汇集 vector | 借鉴 `remainingSeries`/`lastSeriesIndex`，减少全量 grid |
| Vector matching | metadata graph + per-step cardinality + selective buffer | AST evaluator + 若干 JoinSum shortcut | 独立 matching 算子，不靠无穷堆专用 shortcut |
| Subquery | 内部独立 grid 和 range adapter | 通用新 evaluator 尚不完整 | 第一版算子 IR 就要允许嵌套 grid |
| Histogram | 全链路 float/hist | 多数 native path 仍使用 `Option<f64>` | 核心 `SampleValue` 从 Day 1 设计两类值 |
| 内存 | per-query limiting pools、估算、释放 | MemoryReservation、单请求 dense 预算 | 继续沿用 Rust memory pool，但统一 ownership/buffer 计费 |
| 兼容性 | 上游用例、差分 fuzz、fallback | #9556 原型专项测试 | 以 Prometheus oracle 为主，旧 GreptimeDB 仅次级回归依据 |
| 迁移 | 对 planning unsupported fallback | 环境变量实验性路径 | 以计划阶段明确 capability routing，逐步淘汰旧执行器 |

关联源：[#9556 QueryProgram](https://github.com/killme2008/greptimedb/blob/e0df7f17e4b904bde017e935125ed779f4ebd7a6/src/query/src/research_series_grid/query_program.rs)、[ProgramExec](https://github.com/killme2008/greptimedb/blob/e0df7f17e4b904bde017e935125ed779f4ebd7a6/src/query/src/research_series_grid/query_program/execution.rs)、[VisibleSeriesBlock](https://github.com/killme2008/greptimedb/blob/e0df7f17e4b904bde017e935125ed779f4ebd7a6/src/store-api/src/region_engine.rs)、[RangeProcessor](https://github.com/killme2008/greptimedb/blob/e0df7f17e4b904bde017e935125ed779f4ebd7a6/src/query/src/research_series_grid/scan_range.rs)。

## 9. 哪些要学、哪些不要学

### 借鉴

1. Metadata-first 的 group/matching 编排；查询物理源从纯 row stream 改为 series-native source。
2. Pull-based `NextSeries` 的消费边界；range windows 的 ring buffer；明确有限生命周期的 borrow/view。
3. 聚合的 group completion、vector matching 的 per-step 唯一性校验和缓冲释放策略。
4. Native histogram 作为真正的样本种类、annotation 作为执行结果的一部分。
5. Typed plan IR 和可审阅的按形态优化；CSE 只在语义不变且内存预算可控时开启。
6. 上游一致性测试 + 差分 fuzz + 强制旧路径对照 + unsupported 的计划阶段拒绝。

### 不建议直接照搬

1. 整个 Go 生命周期和所有 pooling 实现。Rust 可用 ownership、RAII 与异步取消简化一部分。
2. 把 `SeriesMetadata()` 一次全量物化当作永恒接口约束。初版可以全量 metadata 并预算限制，但必须预留分片/批量 metadata 的升级空间。
3. 把所有操作都硬塞进“只读标签即可得到结果 series”的 API。动态 output labels 是现实存在的语义。
4. 在没有明确资源证明前做 CSE、跨 shard 重算、query splitting 等多层优化。这些应在功能正确并具备 profiling 以后才加入。
5. 直接使用 MQE 的分布式计划、Go 类型或 AGPL 实现。GreptimeDB 应复用当前 Rust RPC/region routing。
6. 将 footer/sidecar 的仅 Put、时间不重叠约束提升为所有 PromQL 查询的限制。**那只是加速路径的可见性证明条件。**

## 10. 需验证的 GreptimeDB 特有问题

1. **同一逻辑 series 可跨多个 SST、memtable，乃至 region。** 在分布式 `rate` 前必须已完整重建该 series，或证明 region confinement，不能先在不同 region 上分别 `rate` 再求和。
2. **当前 `__name__` 既承担 Metric Engine 物理区分，又被 PromQL 当作逻辑 metric label。** 新执行器应统一转为逻辑 `LabelSet`，上层算子不能把任意生成 label 当成持久 `__name__` 列的修改。`count_values` 属于典型反例。
3. **`TSID` 是寻址辅助而非 series 等价关系的充分证明。** 必须保留完整 key（表身份与完整 label set）以处理 hash 冲突、逻辑表隔离和输出 series 去重。
4. **Footer SeriesRun 的证明边界。** `all_put` + 对同一完整 key 的相关运行块时间不重叠，可以让选择器跳过版本归并；但应额外验证所见的数据源覆盖当前 snapshot，且时间仿射描述、PK、值有效性及数据块行范围均可信。未证明则转普通 Mito 可见样本流，而不是转旧 PromQL executor。
5. **结构兼容性优先于过早极致优化。** 新执行器必须从一开始就允许 native histogram、subquery 的独立 grid、annotations 和完整 PromQL vector matching。

## 11. 推荐后续实验与验收材料

建议给实施 Agent 一份同构 workload：

| Query 类别 | 验证内容 | 要看的度量 |
| --- | --- | --- |
| `sum by(region)(rate(m[5m]))` | 多 series 对少量 group | p50/p95、峰值内存、group state、footer hit |
| `topk(5, m)` | 动态输出身份 | peak buffers、实际 K、tie/NaN 语义 |
| `a * on(host) group_left(zone) b` | matched series 多对一 | cardinality error、缓冲峰值、输出标签 |
| `count_values("v", m)` | 值决定输出 label | 高唯一值数、空标签、非有限值、名称角色 |
| `rate(sum by(host)(m)[1h:1m])` | subquery | 内外 grid 对齐、窗口范围、memory |
| histogram 与混合样本 | Float / histogram 算子语义 | warnings、bucket schema、数值一致性 |
| 同 key 跨 SST/region 的覆盖 | 存储读取正确性 | footer fallback、行数、样本 bitwise 一致 |

还应记录：查询的 AST/IR/物理算子概要、source backend（footer/general）、输入输出 series 数、samples 解码数、峰值预留 bytes、缓存命中、fallback 理由和成本。**没有这些数据，无法判断 stream operator 是否真比现有 #9556 更好。**

## 12. 结论

MQE 最值得学习的不是某个单独算子的代码，而是三个相互配合的约束：

- **先确定标签与依赖，再读取每条 series 的数据。**
- **任何可提前完成的 group 都尽早输出并释放；必须缓存时显式计费。**
- **PromQL 的完整语义位于算子系统本身，存储负责交付正确的可见样本。**

GreptimeDB 下一步不应再扩张 #9556 中的 `outputs -> BTreeMap -> AST evaluator` 全量中间结果，而应该让现有 `VisibleSeriesBlock` / range kernel 接入一个可组合、可限额、可回退到普通 **存储读取** 的原生 PromQL operator graph。详细落地契约见 [同目录 RFC](rfc.md)，其中 Section 4.5–4.10 规定了原生 Footer Reader 的后续性能与功能目标。

## 13. 与 Parquet Footer POC 的新进展和研究结论

此报告是 MQE 源码调研，不代替 Footer 设计审阅；但已对照 [`discord9/greptimedb@3c264d84`](https://github.com/discord9/greptimedb/commit/3c264d84ab5cc1804c43d6357095f3e4697cb387) 的实际进度：

- 当前 Footer Reader 已使用 `PreparedSeriesSource::read_series`、`VisibleSeriesBlock` 和 `SeriesBlockProcessor`，**因此与 #9556 执行接口基本对齐**，不必为执行器另造一个 Parquet 读源。
- `all_put` + 严格时间范围不重叠的证明，适合消除已选完整 series 的物理去重，但前提是当前 pinned snapshot 的相关 SST/memtable 来源、完整 PK 与谓词都已考虑。证明失败应走 Mito 普通可见性扫描，而非旧 PromQL engine。
- 当前 `try_prepare_from_footer()` 先将所选 runs 的仿射值展开成 `Vec<f64>`，收集于 `Vec<SeriesData>`。这种 eager payload 是与 MQE metadata-first 模型的**关键差距**。Mimir 的 Selector 在 metadata 时枚举标签和 series references，而不是预先取回所有 sample values。
- 下一阶段应把 Footer 变为轻量 series locator，加按需的样本块读取和 bounded memory accounting；否则上层改写 `ProgramExec` 只能解决一半的 query-wide materialization。
- 特定仿射 Float64 和毫秒 timestamps 是 Footer POC 的编码能力，不是新 PromQL 语义层的限制。长远需要非仿射、null/stale/NaN、native histogram、精确 predicate、混合旧 SST 和在线 memtable 的正确读取。
- `MAX_SERIES_RUNS = 4096`、Postcard+Base64、metadata/cache 成本及 compaction CPU 需要实测。要在证据表明需要后再演进高基数 Footer 布局，优先保持和 Parquet SST 共生命周期，而不是重新添加 Sidecar。
- Footer F0 的剩余 bug 与 F1 lazy source、F2 通用值、F3 混合来源、F4 空间/吞吐成本应各自拆分 PR，不阻塞新 PromQL 算子开发。

以上是 GreptimeDB 的设计建议，不是 MQE 已经采用 Parquet Footer 的证据。最终接口与里程碑详见 [RFC Section 4](rfc.md#4-footer-seriesrun当前接口与长期演进路线)。
