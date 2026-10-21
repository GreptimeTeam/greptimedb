---
Feature Name: 分段 Native PromQL 执行与 SeriesFrame Exchange
Tracking Issue: N/A
Date: 2026-10-21
Author: "@discord9"
---

# RFC：分段 Native PromQL 执行与 SeriesFrame Exchange

## 1. 问题

`FusedSeriesExec` 现在把**一整棵树**交给每个 partition 独立执行。这个假设在两处过强。

### 1.1 跨分区的逻辑 series 无法合并

`execute(partition)` 为每个 partition 建独立的 `NativeExecutionState`。这只能保证**一条原始 series** 完整进入某个 state。

但标签改写之后，来自不同原始 series 的数据可能属于同一条**逻辑 series**，后续 `Window` 需要一起计算——它们却在不同 state 里。

**语义推演的反例（未运行验证）**：

| 分区 | 原始 series | 样本 |
|---|---|---|
| P0 | `M{host="web-1", __type__="counter"}` | (0 ms, 1) |
| P1 | `M{host="web-1", __type__="gauge"}` | (60000 ms, 3) |

内层 `sum_over_time(x[1ms])` 去掉 `__type__` 后，两片都属于 `{host="web-1"}`。外层 `idelta(...[120s])` 在 60000 ms 求值，正确答案是 3−1=2。

实际：每个 partition 的外层只看到一个点，`idelta` 少于两个样本返回 `None`，**最终只有空 chunk，静默返回空**。末端 `LabelSets` 没有占用位置可查，也不会报错。

**失败模式是「返回空」而不是「返回错」**，这是最危险的一类。

### 1.2 chunk 连续性没有保证

`KeyPartitioned(read_key)` 保证相同 key 进同一 partition，**但不保证同一 series 的 chunks 连续到达**。`with_new_children()` 还在用旧的 `self.ordering`。

两个合法源分区各输出 `A1(false), A2(true)` 和 `B1(false), B2(true)`，重分区后可能变成 `A1, B1, A2, B2` —— `SequenceValidator` 报 `raw series key changed before final chunk`。

---

## 2. 设计

**把 `FusedSeriesExec` 定义成一段能按 partition 独立执行的计算，用支持 SeriesFrame 的 exchange 连接这些段。**

跨分区的数据汇合发生在**明确的边界**上，每个 Fused 内的窗口状态仍由自己的 partition 独占。

```
Fused 阶段 A（内层 Window / Map）partition 0, 1
    ↓
SeriesFrame Exchange：按逻辑 series key 分区
    ↓
Fused 阶段 B（合并后运行外层 Window）partition 0, 1
```

阶段 A 的两个 partition 各产生一段 `{host="web-1"}`，exchange 把两段都送到 B 的同一个 partition；那个 partition 收齐、合并后，再执行外层 Window。

**核心放宽**：原来要求「按原始 `read_key` 分区执行整棵树」，现在放宽成「**每一段树只需要满足这一段的分区要求**」。

### 2.1 切段规则

**只要当前分区能提供下游所需的完整输入，就继续融合；需要改变数据归属、或完整性要求不满足时，才切一段。**

| 下游计算 | 需要放在一起的数据 | 边界 |
|---|---|---|
| 普通逐 series 的 `Window` / `Map` | 同一输入 series | 已满足，继续融合 |
| 标签改写后的外层 `Window` | 同一 canonical label set 的所有片段 | 按逻辑 series key exchange，再 merge |
| `sum by(job)` 聚合 | 同一聚合 group 的贡献 | 本地 partial，按 group key exchange，final |
| `on(job)` binary matching | 两侧同一 matching key | 两侧一致分区规则，owner 内匹配 |

**标签发生变化，不一定立即切段。** 可以继续做后面的逐点计算，等 consumer 确实需要合并后的完整输入，再建边界。

反过来，如果入口就用了一个足够安全的分区 key，保证后面可能合成同一逻辑 series 的数据一直在同一个 partition，**exchange 可以省掉**。

**但「可省 exchange」不等于「可省 merge」。** 即使 owner 正确，本地仍可能需要完整性边界——同一 owner 内收到同 label set 的多块数据，仍需归组、按位置合并、重建逻辑结束标记。

### 2.2 编译期可判定性

最小静态信息：**当前 owner key + 是否允许同 canonical key 多个 piece + 是否已完整归组**。

各变体的身份改写语义：

| `NativeExpr` | 改写身份？ | 需要完整 series？ |
|---|---|---|
| `Map` | 是（数学映射删除 name/type/unit） | 否 |
| `Window` | **按具体函数区分**：多数函数删除 metadata；`last_over_time`、`first_over_time` 保留（`function.rs:66-68` 的 `drops_metric_name`） | **是** |
| `Aggregate` | 按 grouping 改写 | final 需要完整 group，partial 只需本地 |
| `Binary` | matching 输出可能删标签/保留 `on`/复制 include | vector matching 需要同 key 两侧（scalar broadcast 不适用） |

### 2.3 owner key 与 canonical key 是两个 key

| Key | 作用 | 可以怎样选择 |
|---|---|---|
| **Owner / partition key** | 决定数据送到哪个 partition | 可以更粗，让多条不同 series 放在一起 |
| **Canonical / merge key** | 决定哪些片段属于同一条逻辑 series | **必须对应当前节点实际输出的标签身份** |

把 counter 和 gauge 送到同一个 owner 本身没有问题，**把它们当成同一条 series 合并才有问题**。

本版选择完整的 canonical label key 来分区，就继续保持这个简单规则；更粗 owner key 的优化以后再做。

**漏判不保证报错**：分区内各自计算的非重叠窗口结果可能合法落地却数学上错误。**更糟的是结果可能完全为空**——§1.1 的反例正是如此，末端冲突检测无法兜底（没有占用位置可查）。

---

## 3. Repartition 与 Merge 是两个职责

物理上可以合成一个节点，**语义上必须分开**。

| | 管什么 |
|---|---|
| **Repartition** | 数据归谁——按下游需要的 key，把相关帧发给同一个 owner |
| **Merge** | 这些碎片如何成为完整输入——按逻辑 series 归组、按时间/grid 位置合并、重建完成标记 |

两种落地形式：

| 形式 | 职责安排 | 何时用 |
|---|---|---|
| `Fused → SeriesRepartition → Fused` | 下游 Fused 的输入适配层做归组合并 | 合并逻辑薄时 |
| `Fused → SeriesRepartition → SeriesMerge → Fused` | 合并显式成节点，易观察复用 | 需要单独观察/复用时 |

**先定义清楚两个职责，再决定是否合成物理节点。** 合并逻辑薄就放下游 Fused 入口，不为此造框架。

---

## 4. 边界契约

### 4.1 fragments 还是完整 series

**答案分三层**：

| 边界 | 传什么 | 约束 |
|---|---|---|
| `Fused A → Repartition` | **fragments** | 允许同 key 多来源、交错、乱序。每条记录保留 canonical key、labels、grid/绝对位置、fields，以及 **piece 结束信息** |
| `Repartition → Merge adapter` | **仍然 fragments** | 不能声称已满足窗口输入排序。窗口明确拒绝直接绑定的 `Interleaved` 输入 |
| `Merge adapter → Fused B runtime` | **完整已归组的 logical series** | 可按 key 拆成多个有序 chunk，但**只在逻辑 series 最后一块**设 `is_last=true` |

### 4.2 canonical key 的编码

**不能用 `read_key`**——那是原始 series 的身份，而本 RFC 要解决的恰恰是「改写后不同原始 series 变成同一逻辑 series」。

也不能直接 hash 多个 Utf8 标签列：**多列 hash 不是天然等价**，必须统一缺失/null/空字符串、标签全集和列顺序。

**选定**：把输出标签规范化为**稳定有序的 `(name, value)` 对**，过滤空字符串，用无歧义长度编码生成 **Binary canonical key**，以该列 hash。这与 `grid_series::label_set` 的语义一致，只是编码成二进制以便作为分区键。

### 4.3 piece 生命周期与逻辑 series 生命周期

这是本设计里最容易搞错的一点。

| | 什么时候结束 |
|---|---|
| **piece 生命周期** | 该**原始 series** 的 `is_last` 到达时 |
| **逻辑 series 生命周期** | 所有可能归到这个 label set 的 **piece 都结束之后** |

在**旧的**执行栈上，`Emitted::occupy` 只有前者：看到 piece 的 `is_last`，就把整个 label set 标记为 closed，**它以为逻辑 series 也结束了**。于是同一分区里第二块 `{host="web-1"}` 一来就被判为「已关闭标签集又出现」而报错——但那两块本来是互补的。

**依赖基线（实施前必须确认）**：

| 版本 | `Emitted` 行为 |
|---|---|
| `#9820` 当前 HEAD | 仍有 `history.closed`，拒绝已关闭标签集再次出现有效值 |
| `#9777` `e1a631018c29` | **已采用 piece 语义**：不查询 `is_last`，只拒绝同 field、同 position 的有效值重叠 |

`#9777` 新版本的注释写得很明确：

> `is_last` is not consulted: it closes the source series the chunk carries, not the label set, and a later series may still fill the positions that one left empty.

**所以实施前先把执行栈 restack 到包含 `#9777` 最新 piece/merge 语义的版本**，复用它的 source-piece 生命周期和有效位置冲突检查，再在其上实现跨分区 exchange 边界。不要重新实现一遍。

**修法（规则，不是口号）**：

1. **A 的可输出 fragment 节点不再把「一个 source piece 关闭」解释为「canonical label set 永久关闭」**——保留 piece 内的合法性检查，跨 piece 的冲突由合并阶段统一验证。
2. **不能全局删除 `Emitted` 检查。** 默认的完整-series 模式保留现有检查；只有新的 fragment 模式才放宽。
3. **piece 的身份要单独定义**：不能用改写后的标签代替来源身份。
4. **A 中任何后续有状态消费者都不能直接接未合并的 pieces**——这是构建期必须拒绝的形状。

### 4.4 冲突检查不能被「清空有效位置」的操作吞掉

上面第 1 条说「跨 piece 冲突由合并阶段验证」。**这需要一个限制：在完成冲突检查之前，不能越过会删除有效样本的操作。**

**一个仍落在第一版白名单里的反例**：

两条原始 series 在同一时间点都有值，只有 metadata 不同：

| 分区 | 原始 series | 样本 |
|---|---|---|
| P0 | `M{host="web-1", __type__="counter"}` | `(0 ms, 1)` |
| P1 | `M{host="web-1", __type__="gauge"}` | `(0 ms, 3)` |

树：`Input → sum_over_time[1ms] → clamp(min=2, max=1)` ‖ exchange ‖ `Input → 外层 Window`

1. `sum_over_time` 删 metadata，两条输出成为相同的 `{host="web-1"}`，**同一 field、同一 position 各有一个有效值**——这里就应该报冲突。
2. A 内继续执行 `clamp(min=2, max=1)`。因为 `max < min`，`map_math` 返回 `None`（`map.rs:126-131`），所有有效值被清掉，只剩空 closing chunk。
3. Merge 只看到空片段，**没有占用位置可查**——它无法知道前面曾经产生重复值。查询成功返回空。

**这不是靠猜 Map 行为**：当前 Window 产出后立即 `self.track(...)`，所以单分区下第二条冲突输出会在父 Map 执行前报错。延后检查就丢了这个证据。

**规则**：

> 尚未完成跨 piece 冲突检查的输出，只能继续经过**保留各 field 有效位置**的逐点操作。遇到可能删除有效位置的操作，应先完成合并及冲突检查，或拒绝该切段方案。

**最小修法**：把切点提前。

- **A**：`Input → sum_over_time`
- **Exchange / Merge**：检查重复值
- **B**：`Input → clamp → 外层 Window`

仍然一个 exchange、两段 Fused，不需要共享状态或新协调机制。

第一版可以直接处理 `Clamp { max < min }` 这一种情况；不做通用算子效果分析框架。暂不支持就明确拒绝这类树。

必须覆盖两种测试：「两来源在同一 A partition」和「不同 A partition」。

---

## 5. Exchange 的输入输出契约

### 5.1 A 的输出要改

`fused_series.rs:899-950` 当前把结果展开成时间/字段/标签列，**没有编码 `is_last`**，而 `:1314-1330` 直接走这个转换。所以 B 拿到的 A 输出里没有「这个 chunk 是不是该 series 的最后一块」这个信息。

A 需要一个新的**内部 evaluated-fragment 输出模式**，与面向用户的最终行输出区分：

| 模式 | 用途 | 带 `is_last` |
|---|---|---|
| 内部 fragment | 给 exchange / 下游 Fused 消费 | **是**（piece 结束） |
| 最终用户行 | 查询结果 | 否（用户看不到控制信息） |

**这不是矛盾，是缺少承载格式。** frame 一行表示一个 chunk，显式携带 piece-end；merge 输出的 `is_last` 表示 logical-series-end；最终用户行输出才丢弃控制信息。

### 5.2 B 的输入要改

`tree_input` 排除带 grid 的输入，执行流只调 `push_raw_at`。

**但不需要绕回 raw frame**：
- `NativeExpr::Input` 已明确支持 scan/exchange 边界
- `NativeExecutor::push_evaluated_at(path, chunk)` 已存在（`native/execute.rs:2969`）
- `NativeType` **没有** `Evaluated` 变体，只有 `Samples`/`Scalar`/`AggregateState`；是否 evaluated 由 `schema.eval_grid` 判断

所以最小改造是**扩展 Fused 的输入分支**，不是重写 runtime。

### 5.3 空 closing chunk 必须保留

一个 series 可能整段没有值，但它的关闭信息仍然必要。frame 的**一行可以携带零长度的时间/字段数组**——这个行不能因为「没有数据」就被丢掉，否则当前展开路径会让结束信息消失。

**第一版统一保留空 closing**，简单且容易实现。

（注：「全空逻辑 series 的结束事件在任何情况下都必不可少」说得偏强：如果 merge 在 EOF 后确认整组为空、且从未向 B 开始交付这个组，当前 unary 计算也可以整组省略。但本版不做这个优化，不为它增加分支。）

---

## 6. 完成协议

### 6.1 现成设施

DataFusion 已经提供了：

- 每个输入 partition 启动读取任务（`repartition/mod.rs:561-604`）
- 任务成功结束后向每个输出发内部 `None` 哨兵（`:2202-2243`）
- 输出流维护 `remaining_partitions`，收到每个哨兵递减，到零返回 EOF（`:2393-2398`）

### 6.2 死锁陷阱

> **必须「持续消费直到 EOF」，不能「停止消费等生产者结束」。**

如果不读 channel 就等生产者完成，生产者会阻塞在 send（`send` 是 `.await`），形成死锁。

**异常关闭也要交代**：`:2403-2405` 在 channel 意外关闭时也返回 EOF，所以不能把「EOF」直接等同于「所有上游成功结束」。需要检测或排除异常路径。

### 6.3 初版保守方案

一个输出 partition **等所有向它供数的上游结束**，再合并并运行后续窗口。

- 简单、行为容易说清楚
- 代价：会阻塞、会持有中间数据
- 缓冲耗尽应报资源错误并取消输入，**不能等「稍后能释放」**——EOF 前本来就不能释放完整组

以后有了按逻辑 key 排序或明确的分组完成保证，再逐组释放和提前输出。

### 6.4 错误与取消

写成硬契约：

- 上游错误**不视为完成**，不产出部分 merge 结果
- 立即释放缓存和 reservation、停止执行、释放上游
- 下游取消同样向上传播
- **共享 exchange 里其他任务的释放也要验证**——单 stream 的 fail/drop 思路不能直接套到多任务共享状态

---

## 7. 第一版最小形态

**限制：一个 exchange、至多两段。**

**白名单**：`Input` → (`Window` | `Map`)* → 至多一次 exchange → (`Window` | `Map`)* → 输出。不在此名单内的一律拒绝。

**明确拒绝**（不要默认安全）：

- 需要第二次完整性边界的树
- **不能证明 A 内所有有状态消费者已获完整输入的树**（即使只需一次 exchange 也不能放行）
- 多 `Input` / **任何** `Binary`（含 vector matching 与 scalar broadcast）
- `Aggregate` / aggregate-state
- `Instant` / scalar-only 边界
- 未支持的字段类型
- 无法证明 scan 的 source series **同 owner、chunk 连续、有序且有终止标记**的输入
- 无法验证一致 grid/fields、未覆盖身份变换语义的输入

**范围说明**：第一版是**手工绑定树**，不扩展查询编译入口。`native/plan.rs:368-373` 明确说只检查声明 grid 一致、不检查是否覆盖窗口读取范围，且编译器尚不接受 subquery。这两件事是两个交付范围，不能混。

**grid 覆盖责任归属**：

> 调用方负责保证 evaluated input grid 覆盖下游 Window 所需的求值范围，包括 range、offset 和 `@` 的影响；本版验证 schema/grid 一致性，未满足覆盖前提的绑定不在支持范围内。

要求的**不是**「sparse holes 必须有值」，而是计算过的 grid 范围要覆盖到。

**行数估计**（推断，非承诺）：

| 部分 | 实现 |
|---|---|
| 输入分派及绑定 | 250–500 |
| frame 编解码 / 输出 | 300–600 |
| EOF merge / 计费 | 250–500 |
| 切段及 piece 生命周期 | 300–600 ⚠ 最乐观：横跨编译规则、逐节点 `Emitted`、A 内多级 Map、完整性证明 |
| 测试 | 400–800 |
| **合计** | **1,500–3,000** |

测试需覆盖：同/跨分区、空关闭、稀疏位置、冲突、资源耗尽、错误、取消。

**四个必须写成具体案例的验收项**：

| 案例 | 必须满足的结果 |
|---|---|
| §1.1 的两个不重叠片段，经内层 Window 合成同标签集，再算外层 `idelta` | 同 A partition 和跨 A partition 都得到 **2** |
| §4.4 的两个同位置冲突片段，后面接会清空的 `clamp` | **仍报告中间节点的冲突**，不能成功返回空 |
| `last_over_time` / `first_over_time` 的两条输出只在 metadata 上不同 | 保持两条 logical series，不误合并 |
| 优化器在 scan 与 A 之间引入可能交错 chunks 的物理节点 | 保持或恢复输入契约；做不到就拒绝该 native 计划 |

最后一条要针对**优化后的物理路径**检查，不能只看 scan 自身的声明。

---

## 8. 复用，不造框架

- **运输**：选定 frame schema、hash 表达式和完成契约之后，可复用标准 `RepartitionExec` 的运输实现。frame 就是普通 `RecordBatch`（`read_key` Binary / `is_last` Boolean / `time` Struct / `fields` Struct），`DataType::Binary` 有 hash 实现
- **完成协议**：fork 的 `remaining_partitions` 递减到零
- **合并语义**：复用 `grid_series::merge_same_labelset`（同 field 同 position 两个有效值报错，其余按位置合并）
- **标签规范化**：复用 `grid_series::label_set`（过滤空值标签）

**不需要**：自建调度器、注册表、通用 exchange 框架、spill 框架。

**`with_preserve_order()` 的注意点**：只有输入已有 `output_ordering` 且分区数 > 1 才生效；启用后是按声明排序表达式**跨分区归并**，不是 FIFO。它解决的是「排序」，不是「同 key 时间连续」——后者仍需真实排序键或 merge 阶段缓冲。

---

## 9. 风险

| 风险 | 后果 | 依据 |
|---|---|---|
| 把 piece 结束当逻辑 series 结束 | 窗口只算半条，**静默错误** | `native/plan.rs:357-367`（窗口一次读一条 series 到最后一块）、`range_eval.rs:775-778` |
| canonical key 未遵循空标签等价 | 漏查冲突，**静默错误** | `grid_series.rs:295-309` |
| A 在同分区遇到第二个有值的同标签 piece | 合法输入进不了 exchange，**显式拒绝** | `native/execute.rs:710-736`（无有效值的 chunk 走 `Ok(())`） |
| merge 覆盖重复值 / 把 Missing 当 NaN / 丢 sparse grid 位置 | **静默错误** | `grid_series.rs:311-405` |
| 不消费等待 EOF / 缓冲无计费 / 错误当完成 | 挂起或资源失败 | `repartition/mod.rs:2202-2243, 2403-2405` |

---

## 10. 与现有工作的关系

- 当前 `FusedSeriesExec`（`#9820`）交付单段 fused：树驱动、多分区、标签集检测、frame 输入契约。**不含本 RFC 的分段能力。**
- `#9820` 的 PR 描述声明：planner 不得构造「标签改写后跨分区仍有有状态消费者」的树，直到本 RFC 落地。**那是临时约束，不是修复**——绕开形状不等于修好，且漏判代价是静默空结果。
- 本 RFC 是独立 PR，不做在 `#9820` 里。

---

## 11. 参考

- DataFusion 共享状态与输入任务：`repartition/mod.rs:1409-1418`
- 输入分发：`repartition/mod.rs:467-605`
- 完成协议：`repartition/mod.rs:2202-2243, 2393-2398`
- 现有合并语义：`grid_series.rs:311-405`
- 标签规范化：`grid_series.rs:295-309`
