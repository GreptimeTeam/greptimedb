---
Feature Name: Native Series-Streaming PromQL Query Engine
Tracking Issue: TBD
Date: 2026-10-14
Author: discord9
Status: Proposed
---

# RFC: GreptimeDB 下一代原生 PromQL Query Engine

> **English abstract:** Replace the PromQL-to-DataFusion relational execution chain with a composable native series-streaming engine. Preserve the useful #9556 kernels; keep Mito's visibility semantics authoritative; evolve Parquet footer SeriesRun into a lazy, high-cardinality SeriesSource without external sidecar files.

**状态：Proposed / 待实现 Agent 评估**  
**日期：2026-10-14**  
**目标：实现完整的 PromQL 兼容性，逐步替换并最终移除现有 DataFusion-centric PromQL 执行路径。**  
**基础原型：[#9556](https://github.com/GreptimeTeam/greptimedb/pull/9556)，审阅基准 `e0df7f17`**  
**参考研究：**同目录 [`mqe-source-study.md`](mqe-source-study.md)，Mimir `streamingpromql@51704a4f`。  
**与并行开发对齐：**[Footer POC 分支 `discord9/greptimedb@3c264d84`](https://github.com/discord9/greptimedb/tree/3c264d84ab5cc1804c43d6357095f3e4697cb387) 已包含写入和读取侧、Postcard、`all_put`、范围校验和 fallback，但提交说明仍有测试失败与谓词限制。本文明确其当前接口，**并将 Footer 的懒读取、通用类型、持续写入、元数据可扩展性纳入正式演进目标**。

## 0. 决策摘要（先读此节）

1. **方向确定：建设一套完整的原生 PromQL 引擎。** #9556 不再被定位为几个查询的加速优化，而是下一代执行器的研究基础。传统 PromQL → DataFusion 逻辑/物理算子组合最终退役；DataFusion 继续服务 SQL、执行上下文与必要的跨层设施。
2. **采用 series-native 的可组合 operator graph。** 一个 operator 能逐条消费上游 series，并逐条交付已完成的输出 series；不是 `ProgramExec` 先收集全部 `SeriesGrid` 再对 PromQL AST 作整体解释。可组合性和有界中间状态优先于“所有操作必须立刻输出”。
3. **语义层与物理布局分离。** PromQL 原生 operator 不知道 Footer、Parquet、TSID、sequence、Sidecar 等物理细节；接收正确的可见样本和逻辑 label set。
4. **优先发展 Parquet Footer SeriesRun 为原生 SeriesSource。** #9556 Sidecar 研究中曾报告约 2× 的特定实验收益，有待可复现的同条件验证；生产架构不依赖外部 Sidecar 文件。当前 Footer 已提供完整 PK、行范围、仿射参数、`all_put` 和 Postcard，未来目标是 metadata-first、lazy payload、支持通用存储可见性与高基数查询，详见 Section 4.5–4.10。
5. **Footer 不适用时退回常规 Mito 读取，不退回旧 PromQL 执行。** 普通 Metric logical table、活跃 memtable、覆盖更新、deletes、不同快照与跨 SST 重叠，不能阻止新引擎功能正确运行。
6. **先解决 correctness contract，再处理高并发/全流式。** 选择器、range、aggregation、matching、subquery 与 histogram 必须拥有统一语义；不应将 #9556 中越来越多的特例延续为下一代架构。
7. **元数据阶段是一种优化，不是所有算子的强制纯函数。** 对 `count_values`、`topk` 等样本决定输出 labels 的算子，允许有计费的有界 buffering/metadata 延后确定；不要设计成必然可仅靠标签确定所有输出。
8. **统一的正确性参照是固定版本 Prometheus。** GreptimeDB 旧引擎用于回归与审计，但不作为最终语义 oracle。Unsupported query 的 fallback 只能发生在**计划/绑定前**；运行时出现错误不能隐式重新执行而吞掉错误。
9. **逐步替换，不并排永久维护两套语义实现。** 新引擎走默认之前保留明确的 `auto/native/legacy` 路由及差分检查，完成兼容门槛后删除旧实现。

## 1. Why：为什么 #9556 之后还要改

### 1.1 现状

原有 GreptimeDB PromQL 的很大一部分逻辑通过 DataFusion 的 `SeriesDivide`、`Normalize`、`RangeManipulate`、普通 Projection/UDF、Aggregate、Repartition、Join 等组合实现。问题不只是慢，还包括：

- 在通用 Arrow `RecordBatch` 上重复展开/重组本来已经具有 series 边界的数据；
- range-vector、标签传播、vector matching 与 PromQL 特殊语义被拆到不适合它们的关系算子之间，难以审阅和维护；
- 对新特性不断叠加特殊 planning/optimizer/column-role 补丁，容易造成结果错误；
- 部分局部优化（例如现有融合 `rate` + aggregate）不能消除整个 execution graph 的重复 materialization。

#9556 已证明可通过 `PreparedSeriesSource`、`VisibleSeriesBlock`、`RangeProcessor`、原生 `Evaluator` 避免大量中间 Arrow 处理。不过当前普通 `ProgramExec` 在各 worker 后仍构造 `Vec<SeriesVector>` → `outputs` → `BTreeMap<Labels, Vec<Option<f64>>>` → `evaluate_collected()`，内存峰值和上层 evaluator 的组合性不理想。

### 1.2 目标

- **功能**：可逐步达到 Prometheus PromQL 完整的语法、类型、函数、运算、histogram、subquery、标签、staleness、annotations 语义；支持 standalone 与 distributed，以及正常持续写入的数据。
- **性能**：保留 Footer/series-major 加速和 #9556 range kernel 的优点；在高 series cardinality 和长 evaluation range 下避免不必要的全部输入网格物化。
- **维护**：一条清楚的存储正确性边界，一套原生 operator semantics，减少 DataFusion 特殊扩展节点和隐藏条件。
- **迁移**：功能覆盖与生产验证充分后淘汰旧 PromQL 执行器，不能让 fallback 成为最终架构。

### 1.3 非目标

- 本 RFC **不要求立即重做现有 Footer/Postcard 格式或 writer**；其 POC 修复仍由并行任务负责。但本文规定 Footer 的未来接口及性能/兼容性目标，不把现有 float-only、全量物化、无谓词等 POC 限制固化为长期设计。
- 初始阶段**不重新设计 Mito 的 MVCC、LSM、WAL、memtable、series index**；只是消费其已有可见性能力。
- 不要求所有运算都 O(1) 额外内存，或所有查询都能在读取全部输入前输出首批最终结果。
- 不立即实现分布式 PromQL plan wire format、全套 DAG/CSE 缓存、spill framework、复杂查询成本优化器。
- 不为减少代码量而牺牲 PromQL 边界语义，特别是不把仅 float、仅 append、仅不重叠窗口当作长期支持范围。

## 2. 架构边界

```text
HTTP / PromQL API / QueryContext
                │
                ▼
   PromQL parser + semantic analyzer
                │
                ▼
     Typed PromQL logical IR
                │
                ▼
   Rule/Capability-based planner
                │
                ▼
    Composable Operator Graph
     ├─ Selector / Scalar / String
     ├─ Instant / Range Function
     ├─ Aggregation / Ranking
     ├─ Binary / Vector Matching / Set Ops
     ├─ Subquery / Special functions
     └─ Result adapter + annotations
                │
      SeriesSource / Snapshot API
                │
          Mito Region Engine
           ├─ Footer SeriesRun (fast)
           └─ General visibility scan (correct fallback)
                │
       SST / memtable / indexes
```

两个**不同层次**的 fallback 必须区分：

1. **Storage fallback**：Footer 快读取证明失败 → 普通 Mito 可见样本扫描，仍使用**新 PromQL 引擎**；这是正常查询路径，不算 PromQL 特性不支持。
2. **Engine fallback**：原生 PromQL planner 尚不支持某语义 → 在执行前选择旧 PromQL 引擎；仅迁移期间存在，可度量且最终归零。

**执行过程中任何结果已经对外可见、算子已经消费输入后，不允许自动 engine fallback。** 发生损坏、语义错误、资源不足或取消应按错误语义处理；不能静默重跑产生重复或不一致结果。

## 3. 统一数据与语义模型

### 3.1 保留三种不同“时间序列数据”

A. **Raw visible samples**：来自 Mito 的单条逻辑 series 的样本，已满足 snapshot/merge/dedup/delete 可见性；带原始时间戳、float 或 histogram 值。仅存储层拥有 MVCC 责任。

B. **Range window views**：某一 evaluation instant 的 `(t-window, t]` 活动样本视图，可借助 ring buffer/seek 而非复制。用于 `rate`、`increase`、`*_over_time`，不同窗口可以重叠。

C. **Evaluated instant-vector series**：对齐到指定 evaluation grid 的、稀疏的结果点。不存在的点与 `NaN`、stale marker、`null` 不等价；此结构可选择 sparse 或 dense 表示，不在 API 中固定 `Vec<Option<f64>>`。

这三层不能混用。#9556 的 `VisibleSeriesBlock` 对应 A；`SeriesGrid` 对应 C；MQE `RangeVectorStepData` 对应 B。

### 3.2 类型草案（语义示意，不是立即要求改所有 Rust API）

```rust
struct EvalGrid {
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    // Instant query 可视作只有一个 evaluation instant
}

struct StorageSeriesIdentity {
    region: RegionId,
    full_primary_key: Bytes, // 包含物理表隔离信息，用于定位与 source 合并
}

struct SeriesMeta {
    storage_identity: StorageSeriesIdentity, // 仅供读源、快照覆盖与去重定位
    labels: LabelSet, // PromQL 层的 series 相等性仅由规范化后 label set 决定
    // 原始 metric name 与 name/drop policy 可以作为单独的语义属性
}

enum SampleValue {
    FloatBits(u64),        // 保留 IEEE bits，包括 stale marker、NaN、-0.0
    Histogram(Histogram),  // native histogram（schema、counter/reset hints 等）
}

struct RawPoint { timestamp_ms: i64, value: SampleValue }

enum TimeLayout<'a> {
    Regular { first: i64, step: i128, count: usize },
    Explicit(&'a [i64]),
}

struct VisibleSampleBlock<'a> {
    time: TimeLayout<'a>,
    values: SampleValues<'a>, // dense / lossless affine / histogram / typed slice
}

struct EvaluatedSeries {
    labels: LabelSet,
    points: SeriesPoints,  // sparse timestamp/value 或 grid-index/value；按需 dense
}
```

**注意**：`FloatBits` 是 raw sample 语义；上层 scalar/instant 计算按 PromQL 规则先处理 stale marker。不能简单用 `Option<f64>` 混同“缺失”和“存着一个 NaN/stale 数据”。某些标签函数、名称移除和 annotations 需要保留原始 metric name，但不允许它破坏输出 label equality。

### 3.3 标签身份：TSID 与 `__name__` 不应泄露为语义特例

- Mito 的 `(table_id, tsid)` 仅用于候选定位，**不是 PromQL 的逻辑 series 相等判断依据**；必须处理 hash collision 和完整主键等价。物理完整 PK 用于判断“哪些原始样本应先按存储语义合并”，PromQL 结果的 series label equality 则仅按**规范化后的完整输出 label set**判断。两者不是同一种 key。
- 逻辑 `__name__` 是 label 身份的一部分；存储用于区分 metrics 的特殊列，在 `SeriesSource` 边界统一转为标签或独立内部元数据。上层不应进行“列角色提升/降级”。
- `label_replace`、`label_join`、`count_values`、vector matching 等对标签集合做操作时，只修改计算中的 label set，不修改物理表。
- 输出标签名碰撞检查必须按照 Prometheus 的**同一 evaluation instant**语义，而不是简单全局拒绝具有相同 labels 的 metadata。
- 一些 PromQL 运算会删除 `__name__`，应由统一 `MetricNamePolicy` / `DropMetricName` 规则实现；要保留原始名字供 annotations 使用。不要把是否删除名字散布在各个算子分支。

### 3.4 Native histogram 从架构第一版就要容纳

- 即使早期只完整计算 float，`SampleValue`、`SeriesPoints`、range windows、annotations、序列化协议从开始就允许 histogram。
- Float/hist 混合、bucket schema / counter reset、histogram 类型错误与警告，需明确语义，不能将 histogram 当作 `f64` 或自动静默丢弃。
- 具体存储编码依赖已有/新增 Metric Engine 能力；没有 histogram 支持时，不能因此声称整个 PromQL 已完全兼容。

## 4. Footer `SeriesRun`：当前接口与长期演进路线

> 本节不要求另开 Footer RFC，也不要求 Footer Agent 立刻完成所有目标。4.1–4.4 是已开始实施的接口约束，4.5–4.10 是**新引擎生产化期间的 Footer 演进规范**。

### 4.1 生产者与消费者

- **生产者：Mito Footer `SeriesRun` Reader**。它已经知道 pk、行范围、仿射参数、`all_put` 及 SST 所属 snapshot/file handle。它在读侧验证并选出有效 runs。
- **消费者：PromQL `SeriesSource` Adapter**。它只接受带完整标签身份、严格时间顺序、正确 snapshot 可见性、必要数值的 series blocks。不读取 Footer/Postcard，也不做版本归并。

已有 #9556 接口 `PreparedSeriesSource::read_series(range, &mut dyn SeriesBlockProcessor)` 和 `VisibleSeriesBlock` 可以继续作为**第一阶段接缝**。Footer Reader 在准备阶段选择并验证快路径，使用已有 `process_series(tags, blocks)` 回调交付每条完整 series。后续如果上层 operator 需要真正 pull cursor，可以加一个适配层；**不要强迫正在进行的 Footer 实现推倒重写**。

需要增加的最小能力：**只读 metadata 枚举**（获得候选完整 series key/labels、顺序及来源范围，不解码 values）。已存在的 `num_series()` 不足以让聚合/匹配构造 group graph。具体 API 可选 `series_metadata()` 或分批 `metadata_cursor()`，不限定返回形式，但要求和 `read_series` 遍历次序、身份严格一致。

### 4.2 快路径证明与错误分类

只有当 Reader 对选中 snapshot 和查询覆盖范围证明以下条件，才可以免序列号进行直接读取：

1. **完整数据源覆盖**：所有能影响这些逻辑 series、这些时间点的 SST/row groups/可见 memtable 的记录已经被纳入证明，或它们被已验证的时间/分区裁剪排除。不能只检查命中 Footer 的 SST，而忘记没有 Footer 的旧 SST。
2. **完整 key**：run 归属由完整 PK/逻辑表身份判定；TSID 碰撞不能把不同 series 合并成一条。
3. **Put-only**：每个相关 run 的 `all_put` 有可信的写入语义，并且覆盖必须验证的样本范围；其他 Delete/版本操作不能在可见集合中改变它的结果。
4. **时间与行范围**：`SeriesRun` 中每个 run 的时间戳严格递增，仿射计算不溢出，row range 和来源 Parquet row-group 边界合法且连续对应；同一完整 key 下不同相关 runs 的 `[first,last]` 严格不重叠。
5. **样本有效性**：`all_put` 不自动意味着 field 非空；如果 fast float kernel 要求 non-null，必须额外证明非空，或走可正确表达 null/histogram 的普通读方式。时间、标签、field filters 的语义不能在证明之外被跳过。
6. **版本与快照**：所引用 SST 和 footer 与 pin 住的 region Version/Snapshot 匹配；如果请求需要历史 snapshot / exact sequence range，而此证明不能覆盖它，必须走普通 MVCC 读取。

成立时 `sequence` 不需要进入 **快路径的每样本处理**，这与 Footer Agent 的现有决策一致。不是说普通存储层不再需要 sequence，或仅凭 `all_put` 就可以省去范围完整性检查。

**错误区分**：

```text
NoFooter / UnsupportedRun / CannotProveVisibility
    → storage fallback (general Mito scanner; 仍然由新 PromQL executor 消费)

MalformedFooter / InvalidChecksum / CorruptedPinnedSource
    → 使用既有存储层错误策略；不得把数据损坏默默解释成“没有数据”

ProvenFastPath
    → 直接提供 visible series；不需要上层 dedup/sequence
```

`Postcard` 的体积相对 JSON 优势是 Footer Agent 报告中的工程指标，不是 PromQL 算法的 correctness 依据；PromQL 层不应知道该序列化选择。

### 4.3 Metadata 与 values 的责任边界

Footer 含 pk、row ranges、仿射参数，但是否能**完全重建数值**取决于仿射参数是否覆盖被查询 value 列及其严格无损证明。如果只描述 timestamp 或部分列，就必须读取必要 value pages，不能宣传“无需解码所有样本值”。

可供执行器利用的能力建议显式表达：

```rust
struct ReadCapabilities {
    full_visible_series: bool,
    regular_time_runs: bool,
    lossless_affine_float_values: bool,
    native_histogram_values: bool,
}
```

这里的 capability 是**某次 prepared reader 的声明**，不是全库或全表永久属性。不能因为一个 SST 支持 affine values 就假设所有读到的 series 都支持。

### 4.4 最小集成测试（Footer Agent 完成读取侧后联合跑）

- 单 SST 单 key、跨 row group 相同 key、跨 SST 不重叠 runs：Footer 与普通扫描逐 sample 对等。
- 相邻 runs 恰好端点相同、相交、内部重复时间：拒绝快路径或按已证明的错误策略处理。
- 旧 SST 无 Footer + 新 SST 有 Footer；memtable 有相同 key；相同 TSID 不同完整 PK；不同 logical table 共享 physical Metric Engine region。
- Delete/Update、`LastNonNull`/普通 merge、field null、stale NaN、时间和 tag filters、lookback/`@`/offset 导致的时间裁剪边界。
- Flush/compaction 与查询并行，验证 snapshot pin 和 source metadata 关联；Footer 不可用时结果一致。
- Footer metadata checksum/length/version 错误与不适用条件能区分；资源/取消释放正常。

### 4.5 当前 POC 基线：哪些已经做了，哪些尚未完成

审阅基准为 [`3c264d84`](https://github.com/discord9/greptimedb/commit/3c264d84ab5cc1804c43d6357095f3e4697cb387)，不是 #9556 的外部 Sidecar：

- `writer.rs` 的 `SeriesRun` 记录完整 PK、run 的 SST 行起点/长度、首毫秒 timestamp、可选仿射 timestamp/value 参数和 `all_put`。仅无损的 Float64 仿射值可直接重建。`MAX_SERIES_RUNS = 4096` 是当前的 metadata guard，不是下一代引擎的 series 数上限。
- `metadata.rs` 以 Postcard + Base64 承载原生 Parquet footer KV 数据。没有另外维护 Sidecar 文件。
- `research_parquet_series.rs::try_prepare_from_footer` 已有文件行范围覆盖、候选完整 PK、Put-only、时间仿射、跨文件 run 不重叠、精确时间裁剪等检查，并通过 `PreparedSeriesSource::read_series` 将 `VisibleSeriesBlock` 交付执行端。不满足证明时返回普通 Mito source。
- **尚非真正流式**：`prepare` 目前建立完整的 `Vec<SeriesData>` 和每个 run 的 `Vec<f64>`，普通扫描 fallback 也先组装各 series。它加快某些数据解码，但准备阶段的 payload 内存仍随匹配 sample 规模增长。
- 当前凡存在 query predicate/region partition predicate 即退回一般读取；仅支持一条 Float64 field 的快重建；非仿射值不走 Footer 重建。代码中可见在 `Vec<f64>` 分配之后再 `try_grow` 计费，需要改成申请预算后再分配。
- 最新提交信息仍列出 `test_list_ssts` 因文件大小变化而失败、谓词不支持等问题；不能将“已 functional”理解为验证完成。

这说明 Footer 和 PromQL 执行侧**已经接好最初的接口**，后续不必再建一个物理读取机制；真正要做的是使这个 reader 满足高基数、在线、通用值类型需求。

### 4.6 F1：Metadata-first + lazy payload（优先级最高）

```text
Pinned SST snapshot
  -> validated footer SeriesRun descriptors
  -> logical series metadata (full PK + labels + run references)
  -> requested series ordinal / cursor
  -> lazy read of value blocks
     -> exact affine reconstruction OR selected Parquet pages
     -> general Mito visible scanner when proof does not apply
  -> SeriesBlockProcessor / PromQL operators
```

**接口方向**：

1. `prepare` 先保留轻量 run locator（file identity、完整 PK、row range、时间信息、能力标志），**不为所有 candidate series 预先展开浮点数组**。
2. 提供 series metadata 的访问能力，最初允许一个受预算约束的 `metadata()` 列表，随后可增加 cursor/batch。Metadata ordinal 和 `read_series` 的遍历顺序要一致；完整 PK 与查询逻辑标签不能以 TSID hash 等价替代。
3. `read_series` 按需解码该条 series 的值；优先复用现有 push-style `PreparedSeriesSource`，没有必要每条 series 单开 tokio task/channel。借用值只在回调期间有效；保留值必须由消费者计费。
4. 存储和 cache 按 pinned SST Version、file identity 验证 metadata 归属；不能跨快照复用不兼容的 footer locator。
5. 为 metadata、locator、临时 map/排序、仿射值展开、Parquet decoded buffer 和结果统一执行 **reserve-before-allocate**。如果不能获得内存预算，返回明确资源错误；不能在消耗部分输出以后静默切换执行器。
6. 完成后，`O(全部 selected samples)` 的 prepare-time payload 内存应消失；`O(候选 series 标签和 run descriptors)` 仍需预算，不宣称零内存。

**验收**：固定时间范围、增大 series 数，读取到前一条 series 时，不应预先解码全部其余 series；记录 `prepare_bytes`、`decoded_values_bytes`、`metadata_bytes` 和 `max_live_series_payload`。与现有 Footer POC 测量冷/热查询延迟、p95 和峰值内存。

### 4.7 F2：通用时间布局、sample 类型和查询谓词

当前快路径是“完整 PK 的仿射 timestamp + 无损仿射 Float64 + all_put”的交集，不应成为下一代引擎的最终能力边界。

- **非仿射/不规则 timestamps**：Footer 的完整 PK + row spans 仍可辅助定位；当 `timestamp_step = None` 时从 Parquet 读取真实 timestamp（或让一般 Reader 处理），不生成虚假等间隔数据。
- **非仿射值、null、NaN、stale marker、负零、多 field**：能证明无损的才在 footer 中重建；其余仅读选中的 columns/pages，保持 bitwise 语义和 PromQL 的缺失值处理。不允许把 `all_put` 当作 non-null 的证明。
- **Native histogram**：`SeriesSource` 语义层最终能交付 float/histogram/mixed samples；Footer 首先只需帮助定位 histogram 数据，后续基于实测增加专用描述信息。不能要求所有 histogram 值变成 `f64`。
- **谓词**：首先支持精确 tag matching 和与 source 行范围一致的时间剪裁。复杂 OR/disjoint、field predicate、dynamic filter 和 region-partition predicate 必须精确处理，否则存储级回退；绝不把扩大后的时间包络误当成全部过滤条件。
- **Candidate discovery**：可与现有 Series Index 负责的 `(table_id, tsid)` 候选发现协作，但 Footer 负责 full-key row locator，二者都不能用 TSID hash 取代完整 series 身份。不要再维护第二套持久 Series Index catalog。
- **能力分离**：`LocateSeries`、`RegularTimestamps`、`LosslessValueEncoding`、`VisibilityProven` 是独立条件；Footer 支持前者不代表自动具备后者。

**验收**：同一查询分别走 footer affine、footer locator + Parquet projected reads 和普通 Mito visible scanner，逐样本一致；所有谓词和负时间/半开区间端点有明确反例。

### 4.8 F3：在线数据、混合 SST 和更新删除场景

生产下一代引擎必须支持仍在写入的 Metrics logical table。Footer 能否证明快路径，是存储**优化选择**，而不是查询是否受支持的条件。

- **F3a：保守正确性**。旧 SST 没有 Footer、新旧 SST 混合、memtable 非空、SST overlap、delete/update、historical snapshot、compaction 并发时，均可整体走一般 Mito 可见性读取。入口证明在第一次向 operator 交付样本前完成。
- **F3b：局部快路径**。只有当存储层能证明某个完整物理 series 的所有相关来源已经考虑（包括 memtable 和旧 SST），且 cross-source visibility 没有歧义，才允许逐 series/时间范围混合 Footer shortcut 与普通读取。否则必须先经过 Mito 的 merge/dedup/delete 处理；**不能在上层 PromQL 算子内部补做 MVCC**。
- **F3c：快照与资源生命周期**。Region Version、SST FileHandle 和 footer metadata 在实际 read cursor 结束前 pin 住。flush/compaction/TTL/manifest 切换的 cache invalidation 跟随 SST 生命周期；不引入独立 Sidecar catalog。
- **Sequence 的地位**：`all_put + 跨整个相关读取集合的严格不重叠` 可让特定快路径免执行 sequence comparison；它**没有废除 Mito 的 sequence**。需要 exact sequence range 或不能证明来源完整性时必须走一般可见性处理。

### 4.9 F4：Footer 大小、Postcard 版本化与读写成本

Postcard 相比 JSON 节省的字节数只说明编码更紧凑，不保证大 cardinality 的 Parquet footer 不会成为瓶颈。当前 Footer 受 run cap 和 KV payload 预算限制；提高上限之前必须评估：

- **Writer/compaction 成本**：flush CPU、wall time、额外写字节、压缩比、文件个数和 list SST 行为；避免 Footer 规模显著加重写入负担。
- **冷/热查询成本**：对象存储 metadata 请求次数与下载字节，Parquet footer prefetch，page index，PK/tag decode，metadata-cache 占用与 hit ratio。与无 Footer、原 Sidecar POC、Footer eager POC、Footer lazy 进行相同 workload 对照。
- **布局演进**：必要时研究 row-group scoped run summaries、PK dictionary、紧凑 offset table、分页/分段 metadata 加载。但先拿证据证明当前 `MAX_SERIES_RUNS=4096` 限制了实际目标场景；**不能为了回避大 Footer 又不经论证引入外部文件**。
- **持久格式兼容**：未来版本必须明确 schema/version/编码兼容；未知版本、旧 JSON SST 或无 `all_put` 时禁用 shortcut 但保证普通读取成功。覆盖滚动升级、缓存失效、SST 复制迁移和错误 footer 内容。
- **成本门槛**：只接受 query p50/p95、峰值内存、metadata bytes 和 write amplification 的联合改进。不把 2× 研究数值当成普遍收益或无条件合并标准。

### 4.10 Footer 分期目标（不阻塞并行开发）

| 里程碑 | 必要交付 | 独立验收 |
| --- | --- | --- |
| **F0 POC correctness** | 完成当前 all_put/coverage/strict ordering/fallback；修复 `test_list_ssts`；补齐跨 SST/逻辑表/TSID 冲突、时间裁剪和 resource tests | Footer 命中与不命中下可见样本逐 bit/类型一致；失败原因可观测 |
| **F1 Lazy source** | metadata-first + run locator + demand-driven sample blocks，解决 `Vec<SeriesData>` eager materialization 和预算申请时机 | `prepare` 不随全部 sample 数量线性增长，冷/热性能回归报告 |
| **F2 Generic data** | 非仿射值/时间定位，精确 tag/time filters，扩展 mixed histogram samples | 与 Mito 普通读取、Prometheus oracle 对照；无静默漏样本 |
| **F3 Live/mixed sources** | 持续写入/旧 SST/删除更新的完整读取，以及能证明安全的局部加速 | memtable/flush/compaction/region 变化期间没有可见性回归 |
| **F4 Scalable metadata** | 用实测决定如何扩展 run cap、metadata cache、Footer 版本/编码 | 大文件高 churn 下 query/flush/IO 三方成本可接受，滚动升级正确 |

**当前 Footer Agent 只需按原计划完成 F0，保持 `PreparedSeriesSource` 接缝。** F1 可以与新 PromQL operator 引擎并行；F2–F4 不阻塞基础语义和重叠窗口实现。

## 5. 新的 PromQL IR 与 planner

### 5.1 为什么不能继续把 AST `match` 一路扩张

#9556 中 `Evaluator::evaluate_inner()` 在匹配 AST 时直接执行 `sum/max/count/topk`、二元匹配和 window functions。这用于研究原型很快，但扩展到完整 PromQL 会把类型检查、执行策略、物理 source routing、功能特例纠缠在一起。

新方案将 **Parse / Analyze / Plan / Execute** 分开：

1. **Parse**：复用现有 PromQL parser，保留位置和原表达式（用于 annotations 与错误定位）。
2. **Semantic analyze**：标量、字符串、instant vector、range vector 类型判断；函数和聚合的参数/label cardinality 规则；lookback/offset/`@`/subquery 时间网格归一化。
3. **Typed logical IR**：保存语义节点和需要的 type/label policy，保持结构简单。
4. **Lower**：将 IR materialize 成可组合的 operator graph，选择 source 及 streaming/buffering 策略。
5. **Optimize**：只增加可证明保持语义的物理优化，例如 direct range kernel、可提前完成的 group aggregation。复杂 CSE/远端 plan 重写先不做。

### 5.2 建议的最小 IR

```rust
// 示意；沿用现有 Parser/Expr/TimeRange 类型，避免重新写解析器

enum PromValueType { Scalar, String, InstantVector, RangeVector }

enum PromPlanNode {
    Selector(SelectorPlan),
    Number(f64),
    String(String),
    Unary { op: UnaryOp, expr: NodeId },
    Binary { lhs: NodeId, rhs: NodeId, op: BinaryOp, matching: VectorMatching },
    Aggregate { op: AggregateOp, groups: Grouping, param: Option<NodeId>, input: NodeId },
    Function { name: FunctionId, args: Vec<NodeId> },
    Subquery { input: NodeId, range: Duration, step: Duration, at: Option<i64>, offset: i64 },
}

struct TypedNode { kind: PromPlanNode, value_type: PromValueType, span: SourceSpan }
```

- **不要**第一阶段新增完整 SQL/DataFusion-style cost optimizer。
- 每个节点负责一个清晰的语义与物理运算；`rate()` 和 `sum()` 的融合优化可作为经过测试的替换规则，不把融合产物作为唯一实现。
- `EXPLAIN PROMQL`（或既有 EXPLAIN 入口）应能显示**实际**执行的 operator graph、source backend、fallback 原因；不要延续 #9556 中向 EXPLAIN 隐藏 `ProgramNode`、只输出 fallback plan 的做法。

## 6. Operator 执行协议与内存所有权

### 6.1 最小 operator contract

借鉴 MQE，但不复制全部 Go 生命周期。建议在 Rust 用概念上类似接口：

```rust
trait SeriesOperator {
    fn prepare(&mut self, ctx: &mut EvalContext) -> Result<()>;
    fn metadata(&mut self, ctx: &mut EvalContext) -> Result<SeriesMetadataView>;
    fn next_series(&mut self, ctx: &mut EvalContext) -> Result<Option<EvaluatedSeries>>;
    fn finish(&mut self, ctx: &mut EvalContext) -> Result<()>;
}

trait RangeOperator {
    fn next_series(&mut self, ctx: &mut EvalContext) -> Result<bool>;
    fn next_step(&mut self, ctx: &mut EvalContext) -> Result<Option<WindowView<'_>>>;
}
```

这是**语义接口草案**，不是强制现在为每个算子提供一组 `async_trait` 虚函数。具体可以用 enum dispatcher/trait object；已有同步 `PreparedSeriesSource::read_series` 作为 leaf 直接 push 给 kernel 或通过 bounded adaptor 接入 pull pipeline。不要引入“每条 series 一个 tokio task/通道”的设计。

初始 `metadata()` 可以先返回完整 series labels，但每个 label 分配必须计费。未来可以升级为 metadata cursor/batches，不应在上层 public API 中假设 metadata 永远是任意大的单个 `Vec`。

### 6.2 数据与状态 ownership

- `VisibleSampleBlock` 的借用生命周期必须短于 reader 的下一次推进；operator 如需缓存，显式 clone 并计入 QueryMemoryBudget。
- Reader pin 住 Version 与所有相关 SST FileHandle，直到相应 cursor/processor 释放；Footer derived metadata 不能在 compaction 后指向已删除来源。
- Operator graph 的临时 scratch、results、metadata、matching buffer、histogram buckets、CSE duplicates 都计入预算。
- 取消、超时、出错时 RAII 清理；对后续再消费应返回错误或 EOF，而不是使用悬垂借用。
- 先保持**单个 operator 实例内串行消费**。并行在一组可独立 series 范围、分区或明确阶段完成后安排，不隐含共享可变 operator 状态。

### 6.3 不强求所有算子完全流式

按照内存/输出性质分为：

| Operator 类型 | 正确处理方式 | 允许暂存状态 |
| --- | --- | --- |
| Selector / Unary / pointwise scalar | 逐 series push/pull | 单条 series / 一个窗口 |
| `rate`、range functions | 每条 series 复用重叠窗口 ring cursor | 窗口宽度内的 samples，必要时 histogram |
| `sum/avg/count/max/min` 等固定 group | metadata 提前分组，逐 series 更新 accumulator，group 完成即输出 | 各未完成 group 的 grid/accumulator |
| Vector matching / set operators | metadata 构建 matching graph；逐时间点检查重复；按依赖缓冲较小侧 | 匹配组与必要的两侧 series |
| `topk/bottomk` | instant 与 range 不同策略，使用 per-step group heap；动态结果 metadata 可能延迟 | 活跃候选及输出样本 |
| `count_values` | 样本值产生 labels，不可只看 input metadata 得到输出 | 动态 label/group 状态；内存超限明确失败 |
| Subquery | 建立内部 grid，再以 range operator 提供外层窗口 | 内部 series 的必要窗口；限制双层 grid |
| CSE | 后期明确共享 source、引用计数和有界缓冲 | 尚未被所有下游消费的数据 |

## 7. 各类算子必须遵守的语义与算法

### 7.1 Selector 和 range evaluation

- 从可见原始样本按 timestamp 顺序构造样本光标，进行 lookback、staleness、offset、`@`；支持 instant/range 查询。
- 一般窗口是 `(t-window, t]`，overlap 合法且常见。当前 #9556 `RangeProcessor::new_inner()` 的**非重叠窗口限制**只属于它的优化 kernel，绝不能成为新引擎的函数语义约束。
- 复用 #9556 `VisibleSeriesBlock` 的 regular timeline 和 exact integer/affine value 优势。非 affine 与有更新/冲突的数据按普通可见样本 cursor 读取。
- 窗口计算保持统一的 Prometheus rate/increase/irate extrapolation、counter reset、缺点数策略和 staleness 语义；不同 kernel 是同一 oracle 下的两种实现。
- 支持 `float64` 与 native histogram，同一 range 混合样本遵守 Prometheus 选择/告警规则。

### 7.2 Aggregation

- 按完整逻辑 labels + `by/without` 规则计算 group key；`__name__` 的处理由统一 label policy 决定。
- metadata 阶段为每个 group 建立 contributor count/ordinal；运行时有序更新该 group accumulator。
- 对普通 `sum by(...) (rate(...))`，一条输入 series 的 rate 计算完成便可并入 group，释放该 series grid。理想空间上界更接近 `O(G × T + worker_buffer)`，而非 `O(S × T)`。
- `avg` 需要 sum+count，`stddev/stdvar` 需符合 Prometheus 数值路径/顺序，`quantile` 可能需要额外缓冲；不能统一声称所有聚合只需 O(G × T)。
- 为避免任务返回顺序改变浮点结果，明确每组按照稳定 contributor ordinal 归并；并行 partials 采用稳定树或在 oracle 可接受的精度下比较，不能使用 `buffer_unordered` 完成顺序直接决定最终 sum 顺序。
- #9556 的 `RangeAggregate` 和已有优化可以复用作 **fast kernel**，不要再为同一语义实现多个主路径。

### 7.3 Vector matching、binary/set operations

- Metadata 建立 match key 和可用 labels，支持默认 matching、`on`、`ignoring`、`group_left/group_right`、`bool`、set operations（`and/or/unless`）。
- 如果两个 series label set 可匹配，但在某一时间点缺样本，则该时点无 binary 结果；无样本不等于 0，也不等于 NaN。
- 多对多不允许用于需要一对一/多对一的算子；cardinality 错误必须逐 evaluation instant 判断，不能只用 metadata 数量判断。
- 独立于读任务调度顺序建立稳定的 join key 与 output label；关注 name dropping、include labels、空/缺失 label 的区分。
- 使用“先分组，按最后使用序号规划输出，必要时选择缓冲一侧”的算法，明确内存限额和回压。极端 join 如果 buffer 超限则返回 query resource error，不能静默错算。
- 不直接把 `#9556 JoinSum` 扩展成对每一种 matching 的手写 shortcut。先建设标准匹配算子，优化放在此之上。

### 7.4 Dynamic-label aggregation 与 ranking

- `count_values` 的 label value 来自样本，可能动态创建大量 output label sets。输出写入**查询结果的 label map**，不允许触碰物理 `__name__` / 指标来源 marker。
- `topk/bottomk` 按每个 evaluation instant 的实际样本参与排名，NaN、tie、空结果、stable ordering 的选择应通过 Prometheus oracle 验证。
- 这类算子允许 metadata 在读数据后确定；如需要在上游 `metadata()` 时完成预读，其内存、stats 与 annotation 仍归查询统一计费。
- 能按 group/时间片释放就释放；不能提前完成的部分采用 bounded materialization，不强制在 metadata 阶段内存无限增长。

### 7.5 Subquery

- 每层 subquery 的 interval/lookback/offset/`@` 等由 planner 计算独立 grid。按照 Prometheus 的 epoch alignment（而非每个外层 step 重新起算）处理。
- inner instant-vector 计算一次，外层按 range view 复用样本；不同层的时间边界和值缺失语义不能混淆。
- 支持 range function over subquery、nested subquery、scalar 与聚合组合；限制 nesting 和内存由现有查询资源策略控制。

### 7.6 Functions 与 annotations

- 维护“PromQL 函数注册表 → 实现状态 → Oracle case”清单，函数按 family 实现，不把新增函数塞进 `Evaluator::evaluate_inner()` 一个无限扩张的 match。
- Float、histogram、类型错误、某些混合样本窗口都可能带 warning/info annotations，必须传播至 API 层；不允许仅比较数值一致就宣称兼容。
- Metric name drop / retention、NaN、`-0.0`、stale marker 以及 histogram counter reset 要配套测试。

## 8. 从 #9556 最小演化路径（不要推翻已有性能成果）

### 8.1 推荐复用项

- `PreparedSeriesSource` / `SeriesBlockProcessor` / `VisibleSeriesBlock` 及其跨 SST 可见性前置条件（迁移接缝）。
- `RangeProcessor::process_series()` 与 `range::evaluate_*_blocks()`，先保留作为具备特定窗口/值类型条件的快 kernel。
- `InstantMaxProcessor`、`RangeAggregate` 中已经证明正确的部分状态算法；与新聚合算子协调，避免重复维护。
- `TimeGrid` 和 PromQL parser/部分语义辅助函数。
- #9556 的 `QueryProgram` 识别与 source binding 可作为过渡方案，但最终由 typed IR 和新 physical lowering 接管；不保留“编译完后用 AST 全量解释”的中间架构。

### 8.2 首先替换什么

**第一刀不是重写整个存储 Reader，而是替换这段：**

Footer F1 的 lazy source 也要同步推进，但作为独立里程碑：只消掉上层 `outputs` 不会自动消除 Reader 当前 `Vec<SeriesData>` 的全量预读。

```text
read_series
 → RangeProcessor::finish_series()
 → Vec<SeriesGrid>
 → outputs Vec
 → BTreeMap<Labels, Vec<Option<f64>>>
 → Evaluator::evaluate_inner()  // 所有样本先集中
 → Vec<RecordBatch>
```

改为：

```text
read_series (per complete series)
 → RangeKernel (per series)
 → Aggregate/Unary/Matching Operator (consume as produced)
 → downstream Operator (consume completed groups/series)
 → Result Sink (only final output materialization)
```

先实现 **Selector → Rate/Instant/Range → Grouped Sum → Result** 这条通路，测量新路径相较 #9556 `ProgramExec` 的性能与峰值内存。复用当前支持非重叠窗口的 fast kernel，**同时增加通用重叠窗口 kernel**，保证常见 `rate(m[5m])` + `step=1m` 能正确执行。

### 8.3 `RangeProcessor` 必须有逐条输出接口，而不只是增量读取

当前 `RangeProcessor::process_series()` 会逐 series 消费 blocks，但 `emit_series()` 将计算结果加入 `series_output: Vec<SeriesGrid>`，`finish_series()` 再整批返回。**如果不改这个中间接口，就算 `ProgramExec` 不再有 `outputs Vec`，worker 内仍可能缓存整个任务的全部 series grid。**

建议第一步引入可借用的下游回调/consumer（API 名称由实现选择），先只支持一种已验证的查询形态：

```rust
// 概念代码：尽量保持现有 PreparedSeriesSource + SeriesBlockProcessor
trait EvaluatedSeriesSink {
    fn on_series(&mut self, series: EvaluatedSeries) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}

// RangeProcessor 的每一次 process_series 完成时：
//   VisibleSeriesBlock iterator -> range kernel -> EvaluatedSeries
//                                     -> sink.on_series(...)
//                                     -> group aggregate state
// 而不是 append 到 series_output Vec 等待 finish_series()
```

这样可以在不修改 Footer Reader 原生 `read_series(range, processor)` 调用形式的前提下，消除第一处不必要的 `O(本任务 series 数 × T)` 缓冲。对于需要复用 DataFusion `PhysicalExpr` 的标量表达式，可以先保留单 series 的临时 Arrow 数组；不必一开始就把所有 kernel 重写成专用字节码。

另一个关键点是**浮点归约顺序**：任务可以 `buffer_unordered` 并行读、并行计算，但不能用先完成者先累加造成不可控结果顺序。最小实现可按稳定的 series ordinal/group ordinal 合并；对完成顺序乱序的任务使用有界 reorder buffer，并在内存超限时明确失败或降低并行度。

### 8.4 过渡时不得反向依赖 Sidecar

- Footer Reader 完成之前，新 operator 可以先通过 #9556 `PreparedSeriesSource` mock 和普通 Mito 读侧开展开发。
- Footer 读取完成后替换底层 adapter，无需改动上层语义算子。
- Sidecar 保留为离线对比 benchmark，不要求新 engine 配置 Sidecar 文件或环境变量。

## 9. 分布式正确性与执行布局

### 9.1 基本原则

**先保证全局 PromQL series 的语义正确，再做 region-local 下推。** `rate()` 与 `sum()` 不能一般交换；如果一条逻辑 series 的原始样本分布于多个 region，分别在各个 region 上运行 `rate` 再 merge 是错误的。

允许 region-local 计算的条件必须能证明：

- 同一完整逻辑 series 的样本只由单个输出 fragment 负责，或已按照时间/版本合并成完整 series；
- 所需窗口/lookback/offset 的边界样本正确提供；
- 聚合 pushdown 保留 partial state 的语义，特别是无输入组、NaN、histogram 与 annotation；
- 不因不同 region 的物理排序/调度改变不允许改变的浮点归约结果。

### 9.2 第一阶段的建议策略

- 继续使用已有 `MergeScan`、region routing、QueryContext、认证、分区裁剪和物理查询通道。
- Datanode 可执行完整的 per-region selector/range kernel，但跨 region 需要一个**全局完整 series 的合并边界**；如果不能证明每条 series confinement，就先在 frontend/共同执行节点合并 raw visible series，再执行语义算子。
- 如果同一 query 的 region 分布于多个节点，**不应把 #9556 的 single-target binding 限制延续为最终产品能力**；第一版允许慢的 general source，逐步增加多目标数据交换。
- 初始阶段不增加另一套远端 Query Plan Wire 协议。成熟后，如要将 Operator DAG 的子图远程执行，再设计受版本约束的 plan serialization。

## 10. 内存、背压、并发与错误语义

### 10.1 性能目标与资源上界

对于 `sum by(region)(rate(m[5m]))` 等输出低基数的表达式，争取把中间内存从所有原始 `S` 条 series 的 `S × T` 网格，下降到 `G × T` 聚合状态加并发 worker 的有限缓冲，其中 `G` 是输出 group 数，`T` 是 evaluation grid 大小。**这是目标复杂度，不是所有 PromQL 运算的保证。**

- 单查询总内存上限由现有 DataFusion/query memory pool 统一治理；新引擎必须可被计费并能失败退出。
- Metadata、matched group、pending series、native histogram internal buffers、output batches、Source Read caches 等全部计费。
- `next_series` 是 pull-style 背压；若用 `read_series` push/visitor，需上限为 N 的 bounded handoff，禁止无界 sender queue。
- 任务并行先沿用 #9556 的候选 series 分片，但 worker 内使用局部可组合 operator state；跨 worker 合并采用稳定顺序或数值规范。
- Result Sink 对最终 RecordBatch 施加背压；如最终结果要求排序，排序边界应可解释，不能隐式攒起所有中间 series。

### 10.2 Error / fallback

| 场景 | 行为 |
| --- | --- |
| Footer 未覆盖/证明不足 | 普通 Mito source，保持新 PromQL engine |
| Native planner 不支持该语义 | `auto` 在执行前明确 fallback 旧 engine；`native` 返回 unsupported |
| Native 运行期间 cardinality error | 返回 PromQL 错误，绝不重跑旧路径 |
| Resource exceeded | 返回资源错误，释放 reservation，记录原因 |
| Footer corruption / mismatched snapshot | 按存储层 corruption policy 报错，不得当空数据 |
| Cancel/timeout | 向所有 operator 和 read task 传播取消，释放 pins/buffers |
| Native histogram unsupported storage type | 明确能力不足；迁移期间走可正确处理它的旧方案，否则拒绝，不静默忽略 |

## 11. 兼容性矩阵与测试方法

**以固定版本的 Prometheus 引擎作为 oracle；不得只做对比旧 GreptimeDB。** 记录每个用例：Prometheus version、query、query_range 参数、数据集、错误、result、annotations、是否新路径、source backend、执行计划摘要。

### 11.1 类型与操作矩阵

| 类别 | 代表性用例 | 重点 |
| --- | --- | --- |
| Selector | `m{a="x"}`, regex、缺失 label、多个 metric | 标签规范化、name、TSID 冲突 |
| Instant | `m`, `timestamp(m)`, `m offset 5m`, `m @ ...` | lookback、staleness、时间边界 |
| Range | `rate(m[5m])`, `increase`, `irate`, `changes`, `*_over_time` | 重叠窗口、counter reset、样本数量 |
| Aggregation | `sum/avg/count/max/min/quantile/stddev/stdvar`、`by/without` | group metadata、空组、浮点顺序 |
| Ranking | `topk`, `bottomk`, `limitk`, `limit_ratio` | per-step ranking、tie、动态输出集合 |
| Dynamic labels | `count_values`, `label_replace`, `label_join` | 语义标签 vs 物理特殊列 |
| Binary | vector/scalar、vector/vector，`bool` | 对齐、运算、错误、标签传播 |
| Matching | 默认、`on/ignoring`、`group_left/right` | per-step cardinality、冲突、空标签 |
| Set | `and`, `or`, `unless` | 存在性、匹配结果、输出标签 |
| Subquery | `rate(sum by(job)(m)[1h:1m])`、嵌套 | epoch aligned、内外 grid |
| Native histogram | bucket/schema 改变、float/hist 混合、histogram_* | mixed warnings、reset、annotations |
| Extended | `@`、negative offset、anchored/smoothed（按目标 Prometheus 版本） | parser/type/窗口语义 |
| Output/API | instant/range HTTP responses、error、warnings | 空 vector vs scalar、标签顺序、错误编码 |

### 11.2 数据来源矩阵

- 普通 Mito 表与 Metrics Engine logical table；多逻辑表共享物理表。
- SST-only（Footer hit）与 SST+memtable（general Mito fallback）；无 Footer 的旧 SST、混合 Footer 覆盖。
- 更新相同 PK/timestamp、删除、MergeMode、跨多 SST 的重叠与不重叠数据；TSID hash collision 的合成反例。
- 单节点、同节点多 region、多节点多 region、跨 region 共同逻辑 series；snapshot pinned 与 compaction 并行。
- 高基数 tag、稀疏点、churn series、NaN/stale/Inf/-0.0、native histogram 混合数据。

### 11.3 Differential + fuzz

- 采用可直接移植的 Prometheus upstream `promql/testdata` 场景；GreptimeDB 额外添加 Metrics Engine 逻辑/物理表和分布式数据生成器。
- 与固定 Prometheus oracle 比较完整 series label sets、每点 timestamp/value、histogram schema/values、warnings/info、errors。对于浮点差异，明确记录 bitwise/tolerance policy，不允许泛化成“都差不多”。
- AST generator 覆盖 operator 组合、任意合法 `by/on/ignoring`、嵌套 subquery、offset/`@`、多字段函数与 label mutation。对 query_limit / memory_limit / cancellation 加压力测试。
- 保留固定反例库并自动最小化；每个已修 bug 要有 native、旧、Prometheus 三方的归档结果。
- 原始样本来源的 correctness 测试与 PromQL 数值语义测试分开；Footer 快路径与 general Mito 数据必须在运行相同 operator graph 时逐样本一致。

## 12. 迁移与实施阶段（可以拆成多 PR）

### Phase 0：锁定执行/存储契约和基线

**交付**：

- 创建 `native_promql`（名称待实现 Agent 按仓库风格确定）模块及最小 `SeriesSource`/`VisibleSampleBlock` 适配；不新增一套 storage catalog。
- 与 Footer Agent 对齐 Section 4：F0 的完整 key、时间与 all_put 证明、fallback、metadata 枚举、memory reservation；记录 F1 的 eager materialization 改造目标，但不要让新引擎开发等待 F1–F4。
- 建立可复现的 `#9556 ProgramExec`、现有旧引擎、Footer（完成后）的 latency/memory profile 与 Prometheus differential test harness。

**验收**：Footer 不存在、memtable 非空时，仍然可由新 source adaptor 返回正确的 raw visible series；同样输入标签不依赖侧车文件。

### Phase 1：基础 operator 与聚合管道

**交付**：

- Typed IR 的 Selector / Instant/Range / Scalar / Aggregate 基础 lowering。
- 核心 metadata/series-lifecycle、per-group completion、`range` 的 overlap 通用 kernel。
- 保留并调用 #9556 的已有 fast window kernel；改造 `ProgramExec` 为每个完整 series 被消费后尽早向下游传递/归约。

**验收**： `sum by(region)(rate(m[5m]))` 在 `step=1m`、`5m` 两种窗口均与 Prometheus 对齐；高基数到少量 group 时不出现 `O(S × T)` 的中间 series-grid 常驻；没有明显超出 #9556 既有快路径的延迟回退。

### Phase 2：Binary / vector matching / 排序输出

**交付**：默认 matching、`on/ignoring`、`group_left/right`、比较/算术/集合、metric name policy；按 metadata 安排 join 缓冲并逐时点校验 cardinality。

**验收**：实现后 `vector matching` fuzz 通过；特定两侧匹配冲突能精确报错；命中组合算子不必回退旧引擎。

### Phase 3：完整函数/聚合/Subquery/Histogram

**交付**：按 Prometheus pinned version 枚举缺失函数和 aggregations，填齐类型、AST lowering、native histogram、subquery、annotations、dynamic labels（含 `count_values`）、extended modifiers。不是一次性大 PR，每一族功能有专用测试与可回滚 PR。

**验收**：覆盖的语义用例无未解释差异；upstream test corpus 中的 disabled/unsupported 项全部经过逐项处理，不以“skip”绕过；API 端口匹配。

### Phase 4：多 region 与可控优化

**交付**：跨 region 完整 series 重组；按安全证明进行 region-local partial pushdown；完善 multiworker 的结果归并、backpressure、查询取消。

**验收**：同一 full series 分片跨 region 的 `rate` 与单节点 oracle 一致，甚至在一侧旧 SST 无 Footer、另一侧有 Footer 时也一致。对 query 的实际存储 backend、fallback 理由可观测。

### Phase 5：默认切换与淘汰旧路径

**交付**：单一开关/会话选项（建议 `promql_engine = auto | native | legacy`，通过现有 query settings 和 HTTP query context 透传），shadow/differential 运行工具、按场景/租户分批切换、移除旧 PromQL optimizer/extension path。

**验收/退出门槛**：

1. 当前承诺的 PromQL stable 功能都在 native 路径支持，且固定 upstream oracle 的关键全集覆盖；差异要有规范解释和明确修复计划。
2. 典型/高压力 workloads 的 p50/p95、峰值 query memory、CPU、IO 没有系统性不可接受回归；特别检查 Footer-hit 与 Footer-miss 两类查询。
3. 生产 shadow 比对关注值、标签、错误与 annotations，测量 **engine fallback rate** 接近零且无数据语义损失。
4. 新路径在持续写入、TTL/compaction、故障重试、跨 region 视角完整验证；SQL 查询未被影响。
5. 完成后删除旧 PromQL 主执行流程，不留下长期双实现分岔。

## 13. 实验和观测规格

建议最少四组 workload，分别在 `legacy`、`#9556 native research`、`new native + general Mito`、`new native + Footer` 路径测量。在没有 Footer 的环境可以先跑前三组，不能阻塞语义工作。

| Workload | 参数维度 | 主要假设 |
| --- | --- | --- |
| 少量输出组：`sum by(region)(rate(m[5m]))` | S=1k/10k/100k，G=2/16/256，steps=60/1000/10000 | streaming aggregation 大幅降低中间内存 |
| 高输出 series：`rate(m[5m])` | series churn、稀疏程度、overlap window | source regular runs 与 ring kernel 的收益 |
| Join：`a / on(host) group_left(zone) b` | matching fanout、series order、RHS 大小 | metadata buffering 与 cardinality 检查的成本 |
| TopK + `count_values` | 高动态 label cardinality、NaN/tie | 有界 buffering 的上界与失败行为 |
| Multi-SST/region 对照 | Footer 覆盖、同 key overlapping、memtable/live writes | storage fallback 正确性与 fast path 实际命中 |

记录字段：

```text
query_id, engine_path, source_path, source_fallback_reason,
selected_series, output_series, points, output_groups,
footer_runs_considered, footer_runs_accepted, scanned_sample_count,
decoded_sample_count, peak_reserved_bytes, operator_peak_bytes,
reader_wall_ns, kernel_wall_ns, match_wall_ns, aggregate_wall_ns,
result_encoding_wall_ns, cancellations, annotations, correctness_oracle
```

`footer_runs_*` 指标由 Footer Agent 自行实现/暴露；新引擎只需要接收聚合统计信息，不读取 Postcard 元数据。

## 14. 必须保留的设计约束（防止过度设计和局部补丁失控）

1. **先用几个真实查询打通完整链路，再扩展函数集。** 不要为了尚未证明的未来需求造一套巨大的 operator service registry、复杂异步调度框架或重复的物理存储抽象。
2. **语义与优化严格分离。** 每个快 kernel 必须有完整/正确的非快路径作 oracle；不是“暂不支持就跳过该样本”。
3. **Footer 的各阶段独立提交，避免将 F1–F4 塞进当前 POC PR。** 眼下只对齐完整 series、source correctness、metadata、timelines/capabilities 和 storage fallback；Footer 进一步改造由 Section 4.10 的独立里程碑驱动。
4. **不追求形式上的全流式。** 一些 operator 的输出身份取决于数据，必须缓冲；重点是显式、计费、尽早释放，而不是假装能 O(1) 内存处理任意查询。
5. **不把 MQE 的 AGPL 源码直接引入 GreptimeDB。** 本 RFC 和调研只使用架构级启发；Rust 实现保持原创并遵循 GreptimeDB 许可证。
6. **浮点正确性优先。** 任何跨 worker/region reduction 优化，都要有与 oracle 比较的测试和稳定合并顺序，避免“快但每次结果不同”。
7. **独立可审阅的 PR。** 建议沿 Phase 划分，首批不要一次提交包含大量特例与测试的综合执行器重写。

## 15. 给实现 Agent 的首个工作包（可直接执行）

**任务：仅建设最小可组合原生 PromQL pipeline，并与 Footer Agent 对齐，不触碰 Footer 序列化格式。**

1. 阅读本文和 `mqe_source_research_20261014.md`；检查 #9556 `query_program/execution.rs`、`scan_range.rs`、`scan_range_aggregate.rs`、`store-api::region_engine` 的现有实现。
2. 列出最小源码改动：能直接复用哪些 `PreparedSeriesSource`、`VisibleSeriesBlock`、`RangeProcessor` 与 `RangeAggregate`；哪些需要新 `Operator`/`SeriesMeta`/`EvalContext`。优先在原有 Rust crate 内组织，不为每种算子新增 crate。
3. 实现 end-to-end 形态：selector → `rate`（重叠与非重叠）→ `sum by(region)` → result；并让没有 Footer 的普通 Mito 数据同样可通过新引擎执行。
4. 避免 `outputs` 全量收集，**同时修改 `RangeProcessor` 的 `series_output: Vec` 汇集点**，让每条已完成 series 即可向 `EvaluatedSeriesSink` 交付并更新 aggregation state。维护借用与 reservation 上界；并行计算不得改变不可接受的浮点语义。
5. 提供 Prometheus oracle 差分和 baseline 性能测试；在固定 SST 数据集和持续写入数据集上都要覆盖，报告 p50/p95/peak memory/source hit rate。
6. 对齐 Footer Reader 所需的 metadata/可见性接口，**不实现第二套 Footer reader**。参考 `discord9/greptimedb@3c264d84` 的读取侧；Footer F0 修复和 F1 lazy payload 可以与新 operator 并行推进，新算子用可控 mock 和普通 Mito adapter 先验证语义。
7. 输出一份短的 implementation notes：哪些语义已由新 operator 覆盖、哪些仍必须 engine fallback、现有 #9556 哪些代码将成为过渡或删除对象。

**完成标准**：上面这条查询可以在新引擎上单独正确运行、支持真实重叠窗口、无需 sidecar、Mito general+Footer 可替换，且中间 **sample-grid** 内存不再达到 `O(S × T)`（metadata 仍可能占 `O(S × 标签大小)`）。**在此之前不要承诺删除旧引擎或扩大到完整 PromQL。**

---

## 参考资料

- [Mimir Query Engine 架构说明](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/README.md)
- [Mimir series operator 接口](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/types/operator.go)
- [Mimir Aggregation 算法](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/aggregations/aggregation.go)
- [Mimir Vector Matching](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/operators/binops/grouped_vector_vector_binary_operation.go)
- [Mimir Query Planner](https://github.com/grafana/mimir/blob/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/planning.go)
- [Mimir 上游一致性用例](https://github.com/grafana/mimir/tree/51704a4f8a04dcb46a96e167637856f561678764/pkg/streamingpromql/testdata/upstream)
- [GreptimeDB #9556](https://github.com/GreptimeTeam/greptimedb/pull/9556) 及 [QueryProgramExec](https://github.com/killme2008/greptimedb/blob/e0df7f17e4b904bde017e935125ed779f4ebd7a6/src/query/src/research_series_grid/query_program/execution.rs)

**版权说明**：Mimir 代码为 AGPL-3.0-only；这里只总结公开架构和行为，没有直接复制受许可保护的实现代码。
