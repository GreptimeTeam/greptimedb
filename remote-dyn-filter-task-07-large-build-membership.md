# Task 07 - Large Build-Side Equal Join Membership 远端表达

## 任务目标

为“大 build side 的等值 join dyn filter”设计一个可跨 frontend / datanode 传输的 membership 表达方案，覆盖当前 `HashTableLookupExpr` 无法远端序列化的问题，并明确它与 Phase 1 `IN` ABI 的关系。

## 为什么值得独立成任务

Phase 1 只支持把小 build side 的 `InListExpr` 投影为远端 `IN` payload，但 DataFusion 对 equal join 的 membership 形态会在 build side 稍大时自动切到 `HashTableLookupExpr`。如果不单独处理这一类场景，分布式 dyn filter 在很多“build side 不算很大、但已经超出 InList 阈值”的 join 上会直接失效。

当前代码里的触发条件是按 partition 计算的双阈值：

- `hash_join_inlist_pushdown_max_size`，默认 128KB / partition。
- `hash_join_inlist_pushdown_max_distinct_values`，默认 150 distinct values / partition。

因此 Task 07 要解决的不是极端超大 build side 的罕见场景，而是“中等规模等值 join 一旦越过 InList 边界就缺少远端表达”的普遍问题。

## 已澄清的上游事实

### 1. build side 不是边读边多次下发 membership 更新

对当前 DataFusion hash join 实现来说，dynamic filter 的关键 membership 更新并不是随着 build side 每个 batch 逐步下发的高频流，而是：

- 先完成 build side 的 hash table / bounds 收集。
- 每个 build partition 上报一次 `PartitionBuildData`。
- `SharedBuildAccumulator` 等待所有相关 partition 都上报后，统一构造最终 filter expr。
- 之后执行一次 `dynamic_filter.update(...)`，并立即 `mark_complete()`。

也就是说：

- 从“单个 query 的最终 consumer”视角看，hash join dyn filter 更接近“一次最终更新”而不是持续增量流。
- 在 partitioned join 下，内部会有多个 partition 各自上报一次，但对 probe-side scan 可见的是 barrier 汇总后的最终 filter。

这使得“大 build side membership 走一次性远端 payload”成为现实可行的设计方向。

### 2. `HashTableLookupExpr` 不能直接远端序列化

`HashTableLookupExpr` 持有 build-side runtime hash table 引用。DataFusion 自己在 physical expr proto 序列化时都无法直接传它，当前做法是把它降级成 `lit(true)` 跳过这个优化。

因此，后续任务的目标不应该是“直接序列化 `HashTableLookupExpr`”，而应该是“为它代表的大 build side membership 语义设计一个稳定、可传输的替代表达”。

## 主要问题陈述

当前 equal join dyn filter 的 membership 形态分裂为：

- 小 build side: `InListExpr`
- 大 build side: `HashTableLookupExpr`

这两种形态在本地单机执行都成立，但远端传播只适合前者。Task 07 需要为后者定义远端替代表示，并让 datanode scan 能消费它。

## 初步方向：Bloom-Based Membership Payload

一个合理的后续方向是：

- 不运输 `HashTableLookupExpr` 本身。
- 不继续强行塞进 `Payload::Datafusion(Vec<u8>)` 分支。
- 而是把它落到 Task 01 预留的 `Payload::Custom(...)` 分支里。
- 在 frontend / build side 完成 hash join build 阶段后，把最终 membership 状态投影成 `BLOOM` 类型的稳定 payload。
- 将该 payload 一次性发送给 datanode scan，作为远端 dyn filter 的 membership 部分。

这个方向成立的前提是：

1. Bloom 只承担“近似 membership 过滤”的职责，允许 false positive，但绝不能有 false negative。
2. 查询正确性仍由 join 本身保证，Bloom 只是优化项。
3. scan 侧需要新的 apply 路径来消费 `BLOOM` payload，而不是继续假设上游一定给它 DataFusion 的 `DynamicFilterPhysicalExpr`。

## 外部实现先例（用于校准方向，不直接照搬）

- Spark、Velox、Doris 等系统都存在“用 Bloom filter 表达大 build-side 等值 join membership”或相近 runtime filter 思路，说明 Bloom 作为远端 membership payload 是有现实先例的。
- Trino、TiDB 一类实现更偏向“小集合走离散值，大集合退化到范围 / 其他保守表示”，说明 Bloom 不是唯一答案，但它是大集合 membership 过滤里最常见的折中选择之一。
- 这些系统的共同点是：Bloom 都被当成独立的过滤表示，而不是去序列化执行器内部的 hash table 引用对象。

## 但不能直接简化成“转成 Bloom 就完了”的原因

### 1. 语义并不完全等价

`HashTableLookupExpr` 是精确 membership；Bloom 是近似 membership。它可以作为远端替代方案，但不是等价序列化。

### 2. 多列等值 join 需要稳定的复合 key 编码

DataFusion 多列等值 join 会先构造 `struct(...)` 再做 lookup。若要生成 Bloom payload，必须先定义：

- 多列 key 的序列化顺序
- 每列类型编码规则
- null 值处理策略
- schema / encoding version

否则 frontend 构造的 key 与 datanode probe side 计算出的 key 无法一致命中。

### 3. partitioned join 需要处理分区路由语义

DataFusion 在 partitioned 模式下可能构造带 `CaseExpr` 的分区路由 filter。Task 07 需要明确：

- 是生成一个全局 Bloom，牺牲部分选择率换简单协议；
- 还是生成按 partition 分片的 Bloom，并要求 datanode 复现同样的 routing 逻辑；
- 还是直接先从 CollectLeft / 非分区方案切入。

### 4. 仅有 Bloom 不一定覆盖全部收益

当前 hash join dyn filter 还可能叠加 `min/max` bounds。若只传 Bloom，不传 bounds，就会失去一部分 statistics pruning 收益。因此更合理的长期方案可能是：

- membership: `BLOOM`
- bounds: `MIN_MAX`

两者组合使用，而不是二选一。

### 5. datanode 当前没有“远端 Bloom dyn filter payload -> scan pruning”现成路径

GreptimeDB 当前 scan 侧虽然有 bloom filter index applier，但它是从逻辑 `Expr`（如 `IN` / `=` / `OR-of-EQ`）构建的，不是直接接收一个远端传来的 Bloom payload。

所以 Task 07 必须包含 datanode apply 层设计，而不是只定义 wire message。

## 主要落点

- `src/common/query/src/request.rs`
- 可能新增 `src/common/query/src/dyn_filter.rs`
- `src/query/src/dist_plan/merge_scan.rs`
- `src/client/src/region.rs`
- `src/datanode/src/region_server.rs`
- `src/table/src/predicate.rs`
- `src/mito2/src/read/scan_region.rs`
- `src/mito2/src/sst/index/bloom_filter/applier/builder.rs`

## 可复用的现有模块

Task 07 不需要从零手写 Bloom 算法或位图 probing 逻辑，以下模块值得优先复用：

1. `src/index/src/bloom_filter/applier.rs`
   - 已有基于 `fastbloom::BloomFilter` 的 probe 逻辑。
   - 其中 `InListPredicate` 和 `contains(...)` 风格的匹配方式可作为 runtime Bloom dyn filter 的低层参考。
2. `src/index/src/bloom_filter/creator.rs`
   - 已有 Bloom 构建参数、误判率、编码格式和内存控制经验。
   - 即便最终不直接复用文件格式，也应复用其参数选型与 codec 思路。
3. `src/index/src/bloom_filter/reader.rs`
   - 若后续决定让远端 payload 与本地 Bloom bytes 表示保持兼容，可借用 reader/metadata 约定。
4. `fastbloom::BloomFilter`
   - 当前代码栈已经在使用，第一版无需再引入新的 Bloom 实现。

复用原则：优先复用“Bloom 数据结构、编码、probe 逻辑”，不要直接复用“面向 SST 持久化索引的整条业务流程”。

## 不建议直接复用的现有模块

以下模块和当前 SST bloom index 工作流绑定很深，不适合被直接拿来当 remote dyn filter 路径：

1. `src/mito2/src/sst/index/bloom_filter/applier/builder.rs`
   - 它的职责是从逻辑 `Expr` 中提取 `Eq` / `InList` / `OR-of-EQ`，再转成 `InListPredicate`。
   - remote dyn filter 场景里，上游输入将是 query-scoped 的远端 `BLOOM` payload，而不是逻辑 SQL 表达式。
2. `src/mito2/src/sst/index/bloom_filter/applier.rs`
   - 它的职责是读取 SST / puffin 中的 bloom index blob，并对 row-group 搜索范围做裁剪。
   - remote dyn filter 需要的是“内存中的 query-scoped filter state + scan apply 路径”，不是读取持久化索引文件。
3. `src/mito2/src/sst/index/bloom_filter/creator.rs`
   - 这是文件级索引构建流程的一部分，关注点是落盘和 segment 化，不是 runtime membership payload 的生成。

边界原则：

- 可复用的是 low-level Bloom primitive。
- 不应直接复用的是 SST index builder / applier 的文件级工作流。

## 第一版最小实现路径

建议第一版把 remote Bloom dyn filter 做成一条独立、简单、query-scoped 的 runtime 路径，而不是硬接入现有 SST bloom index 框架。

### Step 1 - 定义独立的 `BLOOM` wire payload

- 在 `src/common/query/src/request.rs` 或新建 `src/common/query/src/dyn_filter.rs` 中定义：
  - bloom bytes
  - hash / seed 参数
  - false positive rate 或构建参数
  - key encoding version
  - null policy
  - scope（single-column / composite / optional partition scope）

### Step 2 - frontend 只做“一次最终 Bloom 更新”

- 在 hash join build 完成后，把最终 membership 投影为 Bloom payload。
- Phase 1.5/2 的最小实现先只覆盖单列等值 join。
- 暂不尝试复用 DataFusion 的 `HashTableLookupExpr` 本体，也不做增量 Bloom update。

### Step 3 - datanode 维护独立的 runtime Bloom state

- 在 `src/datanode/src/region_server.rs` / query-scoped state 模块中保存远端 Bloom dyn filter。
- 它和现有 `DynamicFilterPhysicalExpr` / SST bloom index 都保持解耦，只共享底层 Bloom bytes / probing primitive。

### Step 4 - scan 增加专门的 apply / evaluate 路径

- 在 `src/table/src/predicate.rs`、`src/mito2/src/read/scan_region.rs` 增加 remote Bloom dyn filter 的应用入口。
- 第一版可以只要求它参与 runtime filtering，不强求直接接入所有现有 pruning optimizer。
- 若需要与统计裁剪协同，则继续保留并组合 `MIN_MAX` payload。

### Step 5 - 再考虑与现有 bloom index 的协同

- 等 runtime Bloom dyn filter 路径稳定后，再评估是否能与 `src/index/src/bloom_filter/*` 的表示格式进一步统一。
- 不要在第一版就把 query-scoped runtime filter 和 file-scoped persistent bloom index 强行合流。

## 前置依赖

- Task 01 完成 `IN` / `MIN_MAX` / `BLOOM` ABI 扩展位定义。
- Task 02-04 完成 query-scoped dyn filter control plane 和 datanode apply 基础能力。

## 实现范围

1. 明确“大 build side equal join membership”的远端目标语义。
2. 设计 `BLOOM` payload 结构，至少包括：
   - bitset bytes
   - hash function / seeds
   - key encoding version
   - false positive rate 或构建参数
   - null policy
   - 是否为单列 / 多列 key
   - 可选 partition scope
3. 设计从 DataFusion build-side membership 状态投影到远端 `BLOOM` payload 的 frontend 逻辑。
4. 设计 datanode 侧的 apply 逻辑，让 table scan 能把远端 `BLOOM` membership 用于 pruning / filtering。
5. 决定 Bloom 与 `MIN_MAX` 是否组合下发，以及组合后的应用顺序。
6. 明确 Phase 2/3 的支持边界：单列优先，还是直接覆盖多列等值 join。

## 推荐子步骤

1. 先锁定“单列等值 join + 一次最终 Bloom 更新”的最小可行路径。
2. 明确 Bloom key 编码规范，并做 frontend / datanode 一致性测试。
3. 增加 per-partition 与 global Bloom 的方案比较，选择 Phase 1.5/2 的默认方向。
4. 评估是否需要保留 fallback：若 Bloom 构建开销或 bitset 太大，则继续降级为不下发。
5. 评估如何与现有 bloom filter index / parquet bloom filter 能力协同，而不是重复实现两套近似过滤机制。

## 验收标准

- 已有证据证明 hash join 大 build side membership 只需要一次最终远端更新即可表达主要收益。
- `HashTableLookupExpr` 的远端替代表达已定义，且不依赖传输 DataFusion runtime hash table 本体。
- 单列等值 join 场景下，远端 `BLOOM` dyn filter 能在 datanode scan 侧带来可观过滤收益。
- false positive 不影响查询正确性，false negative 被设计和测试明确禁止。
- 对多列 key、partitioned join、null 语义、bounds 组合的限制被文档化。

## 风险与注意点

- 不要把 Bloom 方案描述成“序列化 HashTableLookupExpr”，它只是一个可运输的近似替代表达。
- per-partition 阈值不等于全局 build side 大小，分区 join 下可能需要不同策略。
- 多列 key 的编码协议如果不先锁定，后续很容易出现 frontend / datanode 不一致。
- 只做 Bloom 而不考虑 `MIN_MAX`，可能拿不到 hash join dyn filter 的完整收益。
- 现有 scan 侧 bloom 能力主要围绕逻辑表达式和索引 applier，Task 07 需要补新的远端 payload 消费路径。
