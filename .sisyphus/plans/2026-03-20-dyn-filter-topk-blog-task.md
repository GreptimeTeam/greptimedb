# 任务：GreptimeDB dyn filter / TopK 技术博客

## 目标

撰写一篇 GreptimeDB 技术博客，解释 dynamic filtering 的设计与实现细节，重点说明 `TopK` 如何参与运行时剪枝。文章应贴近官方工程博客风格；正文草稿可以先按“已完成一轮最新最小复测”的目标口吻来组织 benchmark 段落，但任务文档本身必须保持“复测尚未完成、待执行回填”的真实状态。

## 交付物

1. 一篇符合风格的中文技术博文草稿。
2. 一份来源核对清单（源码路径、Explain 证据、benchmark 证据）。
3. 一份最小复测 benchmark follow-up / 结果回填说明，确保复测完成后博客与 benchmark 任务口径一致。

## 风格要求（参考官方技术博客）

- 问题先行，不要先写“本文将介绍”。
- 前两段回答：为什么重要、GreptimeDB 做法有什么不同。
- 语气技术化、直接、证据驱动，避免营销措辞。
- 结构清晰（`##`/`###`），主章节控制在 3-5 个。
- 性能结论必须有 Explain、源码或 benchmark 证据支撑。
- 结尾明确限制与边界，并给出后续验证方向。
- benchmark 段落在博客正文里可以按“最新最小复测已完成”的目标口吻来写，但任务文档必须明确这只是成稿目标，不代表复测当前已完成。

## 已确认技术事实（作为正文锚点）

1. dynamic filters 属于 table predicate 的一部分，来源可包括 `TopK` 或 `Join`，目标是减少扫描。
   - 证据：`/home/discord9/greptimedb/src/table/src/predicate.rs:55`

2. 本文最关键、也是 headline benchmark 的 active path 不是 `PartSortExec`，而是 DataFusion 原生 `SortExec: TopK` 在非 time-index 排序列上持续更新 dynamic filter；GreptimeDB 的 table scan 再读取该 filter 来做文件/row-group 剪枝。
   - 证据：`/home/discord9/langchain-traces-benchmark/topk_dyn_filter_test.sql:33`
   - 证据：`/home/discord9/langchain-traces-benchmark/topk_dyn_filter_test_dyn_filter_first_time.result:127`
   - 证据：`/home/discord9/langchain-traces-benchmark/topk_dyn_filter_test_dyn_filter_first_time.result:130`

3. `PartSortExec` 与 `PartSortExec::can_stop_early` 确实存在，但它们属于 time-index 的 windowed-sort 窄路径；对本文关注的 non-time-index `ORDER BY end_time DESC LIMIT 10` 案例，不应把它写成主要收益来源。
   - 证据：`/home/discord9/greptimedb/src/query/src/part_sort.rs:546`
   - 证据：`/home/discord9/greptimedb/src/query/src/optimizer/windowed_sort.rs:102`

4. 更通用、也是本文 non-time-index 场景主路径的“跳过工作”发生在 table scan / parquet file-range 评估：读取前使用最新 dynamic filters 与 row-group statistics 做剪枝。
   - 证据：`/home/discord9/greptimedb/src/mito2/src/sst/parquet/file_range.rs:128`

5. Explain 输出可直接展示 `TopK` filter，是对外可见证据。
   - 证据：`/home/discord9/greptimedb/tests/cases/standalone/common/order/order_by.result:291`

6. RC2 公开发布内容已将 dynamic filter pushdown 与 `ORDER BY non_indexed_time_column LIMIT k` 的性能提升关联，并给出 `langchain_traces` 案例。
   - 证据：`https://greptime.com/blogs/2026-03-12-greptimedb-v1.0.0-rc2-release`

## 文章必须回答的问题

1. dynamic filtering 在查询执行中解决了什么问题？
2. 为什么 DataFusion 的 `SortExec: TopK` 能成为 non-time-index 场景里的 dynamic filter 生产者？
3. `TopK` 堆阈值如何随执行推进而演化？
4. 阈值如何进入 table scan/parquet file-range 的剪枝决策？
5. 哪些环节参与：DataFusion `SortExec: TopK`、GreptimeDB table predicate、scan-side pruning、explain 输出，以及（仅 time-index windowed-sort 场景）`PartSortExec`？
6. 为什么 non-time-index 排序键收益更大，而 time index 不一定有同量级变化？
7. 何时不该下强结论：过滤器仍接近 `true`、统计不具选择性、`k` 太大、或证据不足？

## 建议文章结构

### 1) 问题开场

用 `ORDER BY end_time DESC LIMIT 10` 的大表场景开场，解释“输出很小但扫描很大”的矛盾。

### 2) 一段话解释 dynamic filtering

强调它是“执行期反馈回路”，不是单纯 planner rewrite。

### 3) `TopK` 如何产出动态阈值

- 先讲 headline case：non-time-index 排序走的是 DataFusion 原生 `SortExec: TopK`，Explain 中可直接看到 `SortExec: TopK(..., filter=[...])`。
- 再讲 GreptimeDB table scan 如何读取更新后的 `dyn_filters`，并在 file-range / row-group 层做 pruning。
- 最后单独说明 `PartSortExec` 的适用边界（只在 time-index windowed-sort 路径），不要把它写成本文主角。

### 4) 阈值如何落到 scan-side pruning（主路径）

- 解释 `Predicate` 的 static + dynamic 双表达。
- 解释 live/snapshot 语义。
- 解释 `file_range.rs` 如何用 row-group stats + dynamic filter 在读取前剪枝。
- 明确说明：对 `ORDER BY end_time DESC LIMIT 10` 这样的 non-time-index 案例，主要收益来自“`SortExec: TopK` 更新 filter -> table scan 消费 filter -> prune file ranges”。

### 5) Explain 证据

给出 `SortExec: TopK(..., filter=[...])` 与 scan 节点 `dyn_filters` 示例，解释每段含义。

### 6) Benchmark 快照

以“最新最小复测”作为**正文目标口径**，重点覆盖 headline case：

- `ORDER BY end_time DESC LIMIT 10` 作为主案例
- `ORDER BY start_time DESC LIMIT 10` 作为对照组

正文里可以自然写成“我们补做了一轮最小复测”，但要明确这不是完整 benchmark campaign。与此同时，任务文档要保留“待执行 / 待回填”状态，不能把目标口吻误写成当前事实。

### 7) 限制与后续

强调收益依赖数据分布、row-group 统计选择性和 `LIMIT k` 大小；并说明博客成稿将以“最新最小复测”作为口径，而当前任务阶段仍需先完成该复测，后续若要做更强对外性能背书，再补完整 benchmark campaign。

## benchmark 采用规则

仅当满足以下条件时才把数字写成“当前版本测量”：

1. baseline/candidate 除 dyn-filter 相关差异外可控。
2. 数据集、查询集、集群形态一致。
3. 至少有一项用户侧指标 + 一项引擎侧证据。
4. 重复运行后结果方向稳定。

否则降级为“机制 + 最小复测趋势”叙述，不写超出口径的绝对化结论。

## 基准执行环境（可复用）

- 构建/推镜像：`/home/discord9/langchain-traces-benchmark/build_push.sh`
- Helm values：`/home/discord9/langchain-traces-benchmark/deployments/greptimedb-cluster/values.yaml`
- 本地 registry：`192.168.50.81:5001`

严禁把 values 里的对象存储凭证等敏感信息写入文章、任务笔记或提交信息。

## 任务拆解

### 任务 1：证据核对

- 对每个技术结论补齐“本地源码 / 公共来源 / 删除”标签。
- 重点核对 headline case 是否明确写成“DataFusion `SortExec: TopK` 产出 filter，GreptimeDB table scan 消费 filter”，以及 `PartSortExec` 仅为旁路窄路径。

完成标准：关键结论均有可追溯证据，且无“把窄路径写成通用路径”的表述。

### 任务 2：正文草稿

- 按上述结构写出完整文章。
- 至少包含：1 个 SQL 示例、1 个 Explain 示例、1 个代码路径图示（可先文字图）。

完成标准：不依赖 fresh benchmark 也可独立评审技术正确性。

### 任务 3：benchmark 口径决策

- 明确博客正文采用“已完成最新最小复测”的目标口径。
- 任何数字结论都要标注来源与口径。
- 明确 headline case 与 control case 的角色，不把对照组写成主结论。

完成标准：文章内不存在两类歧义——既不“像完整 benchmark 但其实只是最小复测”，也不“像复测已完成但任务其实尚未执行”。

### 任务 4：技术评审

- 逐条检查事实/解释/未来工作分类。
- 高风险点复核：100x 说法、non-time-index 收益边界、`SortExec: TopK` 与 table scan 的生产/消费关系、`PartSortExec` 适用边界。

完成标准：无无证据推断，无相互矛盾描述。

### 任务 5：编辑收尾

- 统一标题、导语、图注、术语。
- 检查链接可达、代码块带语言标记。

完成标准：成稿可直接进入审稿流。

## 验收标准

- 技术叙述准确区分：headline case 是 `SortExec: TopK` 产出 dynamic filter、table scan/parquet 消费并剪枝；`PartSortExec` 只是 time-index 窄路径。
- benchmark 口径清晰：博客目标口吻、任务当前状态、历史 PR/旧 artifact 三者不混淆，也不把最小复测伪装成完整 benchmark。
- 所有关键结论均可回溯到源码、Explain、PR 或 benchmark artifact。
- 无敏感信息泄露。
