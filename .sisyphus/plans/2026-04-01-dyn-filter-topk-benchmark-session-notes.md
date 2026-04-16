# Dyn Filter / TopK Benchmark Session Notes

> 临时任务文档，记录这轮 benchmark 的目标、当前进度、疑点和下一步。不是正式对外文档。

## 当前目标

这轮 benchmark 的目标有三个：

1. 补一轮可引用的最小复测数据，重点看 `langchain_traces` 上的 TopK / dyn filter 行为。
2. 确认 blog 主案例 `ORDER BY end_time DESC LIMIT 10` 在当前 main 上是否仍然能稳定看到：
   - `SortExec: TopK`
   - 非空 dynamic filter
   - scan 侧 `dyn_filters`
3. 做出一个可信的对照组，而不是只拿历史 PR 里的简单测试结果。

## 本轮中间结论

目前已经可以确认：

- 当前 main 上，`ORDER BY end_time DESC LIMIT 10` 的 explain 路径是对的。
- 当前 main 上，`ORDER BY start_time DESC LIMIT 10` 的 time-index 路径并没有表现出更强的收益，至少这轮数据里是这样。
- 之前临时做的“disable dyn filter”镜像并不是可靠对照组，不能直接拿来写 benchmark 结论。

## 一个重要前提：当前镜像是 debug 验证镜像，不是最终 benchmark 镜像

这轮已经构建和部署过的镜像，当前都是基于本地 `cargo build` 产物，也就是 **debug binary**。

这意味着：

- 当前结果可以用来验证 dyn filter 是否真的生效 / 失效
- 当前结果可以用来分析 explain 路径和 scan 行为
- 当前结果**不能**作为最终 benchmark 数字直接写进对外结论

正式 benchmark 必须额外补一轮 **release build**：

- `cargo build --release`
- 重新打 candidate / disabled（以及必要时的第三镜像）
- 再重新部署和采集性能数据

所以这份临时文档里的现有数字，全部只应理解为：

- **机制验证数据**
- **排查数据**
- **不是正式性能结论**

## 已完成进度

### 1. 代码与镜像

- worktree 已 fast-forward 到 `upstream/main`
- 当前测试基线 commit：`2b4e12c358818ef0829cc524aa56bbe38cc37980`

已构建镜像：

1. **candidate / 当前 main（debug，用于机制验证）**
   - local push tag: `localhost:5001/nightly-greptimedb:20260401-topk-main-2b4e12c358`
2. **临时 no-dynfilter 镜像（debug，当前怀疑无效）**
   - local push tag: `localhost:5001/nightly-greptimedb:20260401-topk-main-2b4e12c358-no-dynfilter`

### 2. k8s 部署路径

这轮实际使用的部署方式：

1. 用 `/home/discord9/langchain-traces-benchmark/build_push.sh` 的思路构建并推镜像
2. 手动更新 `/home/discord9/langchain-traces-benchmark/deployments/greptimedb-cluster/values.yaml` 中的 tag
3. 在 `/home/discord9/langchain-traces-benchmark/` 下执行 `make deploy-cluster`
4. 用 `tmux` 挂 frontend 的 `kubectl port-forward svc/langchain-traces-frontend 4002:4002`
5. 用本地 `mysql --protocol tcp -h 127.0.0.1 -P 4002` 跑查询

### 3. candidate 已采集的数据

目录：

- `/home/discord9/langchain-traces-benchmark/results/20260401-topk-min/`

已经保存：

- `candidate_end_time_query_run1..3.*`
- `candidate_end_time_explain_run1..3.*`
- `candidate_start_time_query_run1..3.*`
- `candidate_start_time_explain_run1..3.*`

#### candidate 查询耗时（3 次）

`ORDER BY end_time DESC LIMIT 10`

- 3.04s
- 2.88s
- 2.77s

`ORDER BY start_time DESC LIMIT 10`

- 4.87s
- 5.03s
- 5.03s

#### candidate explain 里观察到的现象

`ORDER BY end_time DESC LIMIT 10`

- 稳定出现 `SortExec: TopK(fetch=10)`
- 稳定出现非空 `filter=[end_time ...]`
- scan 节点稳定出现非空 `dyn_filters`
- `MergeScanExec total_do_get_cost` 明显低于 `start_time` 这条路径

`ORDER BY start_time DESC LIMIT 10`

- 稳定出现 `PartSortExec`
- scan 节点出现的是 `DynamicFilter [ empty ]`
- `MergeScanExec total_do_get_cost` 明显更高

### 4. 临时 no-dynfilter 镜像的问题

虽然做了一个最小代码改动，想在 `src/table/src/table/scan.rs` 里让 pushdown 返回 `PushedDown::No`，但当前结果说明这个版本**不能作为真正 disabled baseline**。

原因：

- `disabled_end_time_explain_run1.txt` 里依然能看到：
  - `SortExec: TopK(... filter=[...])`
  - scan 侧的 `dyn_filters`

这说明当前改法没有真正关掉这条动态 filter 路径，或者至少没有关干净。

另外，disabled 版本跑测时还出现过：

- `tcp connect error`
- `h2 protocol error`

这进一步说明它现在不是一个适合直接引用的 benchmark 对照组。

### 5. rerun1 的一个重要现象：超长查询

后来又基于同一份 debug disabled 二进制重新打了一个新 tag：

- `localhost:5001/nightly-greptimedb:20260401-topk-main-2b4e12c358-no-dynfilter-rerun1`

并重新部署验证。

这次最关键的观察不是 explain 文件本身，而是 frontend 日志里记录到的一次完整长查询：

- 查询开始：`2026-04-02T03:08:45Z`
- 查询结束：`2026-04-02T03:25:47Z`
- frontend 日志中的整体耗时：`1022.392746601s`
- 同一条日志里 `MergeScan` 记录：
  - `total_poll_duration: 1022.271527433s`
  - `total_do_get_cost: 114.870425ms`

这个现象很重要，因为 candidate 上同类查询大约在 `2.7s ~ 3.0s`，而 disabled rerun1 直接拉长到了 **17 分钟级别**。

当前更合理的解释是：

- 这版 disabled 镜像虽然还需要进一步确认 explain 细节
- 但它已经显著改变了执行行为
- 至少对 `ORDER BY end_time DESC LIMIT 10` 这条主案例来说，dyn filter 相关优化很可能确实被削弱或禁用了

需要注意：

- 这次超长查询的结果文件没有成功落盘
- 不是查询没跑完，而是保存方式/会话处理上出了问题
- 但 frontend 日志已经提供了足够强的旁证

## 当前疑点

### 疑点 1：time-index 排序现在为什么反而更慢

当前 candidate 数据里：

- `ORDER BY end_time DESC LIMIT 10` 更快
- `ORDER BY start_time DESC LIMIT 10` 更慢

一个可能解释是：

- 数据分布或数据规模和之前简单测试时不一样了
- 即使是 time-index 排序，这条路径现在也未必天然更有利
- 当前 explain 里 `start_time` 路径的 `dyn_filters` 还是 empty，没有形成真正能帮助 scan 的有效条件

### 疑点 2：是否需要单独验证“主键/time-index 排序 + dyn filter”的收益

当前怀疑是合理的：

- 随着数据量变大，time-index 排序这条路径也可能出现正收益
- 但现有数据还不足以说明是“dyn filter 起作用了”，还是“别的路径变化了”

所以可能需要再准备一个专门围绕 time-index 路径的验证镜像或验证方案。

### 疑点 3：disabled 镜像是不是打错了

需要排查两个方向：

1. 镜像本身是否真的来自改过的二进制
2. 改动位置是否并不是这条路径真正的禁用点

这轮额外核对后，镜像打错的可能性已经显著下降：

- 本地 disabled 镜像二进制 hash 与 benchmark 目录里的 `greptime` 一致
- k8s datanode 实际运行的 imageID 也对上了 disabled 镜像的 digest

因此当前更像是：

- 改动确实进入了镜像
- 但禁用点的语义还需要继续理解和验证

## 下一步建议：准备 3 个镜像

后续建议按 3 个镜像推进，而不是只做 2 个。

### 镜像 A：candidate / 当前 main

用途：

- 作为当前真实行为的基线
- 继续补更多 explain / 结果整理

状态：

- 已构建
- 已部署
- 已拿到一轮 3 次数据
- 注意：当前是 **debug 镜像**，只能用于机制验证

### 镜像 B：true disabled dyn filter

目标：

- 真正关掉这条动态 filter 路径
- 用来和 candidate 做可信对照

当前状态：

- 现有 `no-dynfilter` 镜像不可靠，需要重做
- 而且即使重做成功，也要在 **release** 下再补一次正式 benchmark 数据

需要做的事：

1. 重新调查真正的禁用点
2. 确认 explain 里 dyn filter 确实消失或失效
3. 再部署并补齐 3 次 query + 3 次 explain

### 镜像 C：time-index / 主键排序验证版

目标：

- 专门验证 `ORDER BY start_time DESC LIMIT 10` 这类路径是否在当前数据分布下也能从 dyn filter 获得正收益
- 帮助判断 blog 里到底该怎么写 time-index 路径

这个镜像不一定是“改代码得到新行为”，也可能是：

- 打开更细的日志/指标
- 或做一个更聚焦这条路径的实验版本

这部分还需要进一步定方案。

## 建议的后续执行顺序

1. 先确认临时 no-dynfilter 镜像为什么没有真的 disable 成功
2. 用当前 debug 构建继续把“真正的 disabled 镜像”验证清楚
3. 确认机制后，切到 **release build**
4. 用 release 重新做 candidate vs disabled 的正式对照数据
5. 再单独考虑 time-index 路径是否需要第三个验证镜像
6. 最后再回填 benchmark 任务文档和 blog 用到的摘要

## 当前不应下的结论

在拿到真正 disabled baseline 之前，不应该对外下这些结论：

- “当前 main 比关闭 dyn filter 快多少倍”
- “time-index 路径没有收益”
- “所有收益都来自 scan-side pruning”

目前能比较稳地说的是：

- `end_time` 主案例在当前 main 上仍然能看到正确的 explain 证据链
- `start_time` 路径当前表现和之前预期不一致，值得单独调查
- disabled baseline 还没有完全准备好，但已经出现了一个很强的信号：disabled rerun1 上同一条主案例查询被拉长到了约 `1022s`

## 本轮排查补充记录

### datanode 重启原因

`langchain-traces-datanode-0` 当前 `Restart Count = 2`，但从 `describe` 和 `--previous` 日志看，之前两次重启的直接原因不是 OOM，而是启动期连 metasrv 失败：

- `Failed to start datanode`
- `Failed to initialize meta client`
- `tcp connect error`

所以这两次 restart 不能直接拿来当“长查询导致 OOM”的证据。

### disabled rerun1 的日志旁证

frontend 日志里已经出现一次完整的长查询记录：

- `Executing query plan` at `2026-04-02T03:08:45Z`
- `Merge scan finish one region` at `2026-04-02T03:25:47Z`
- `DatafusionQueryEngine execute 1 stream, cost: 1022.392746601s`

更完整一点的 frontend 相关日志如下：

- `2026-04-02T03:08:45Z`：开始执行 query plan
- `2026-04-02T03:08:45Z`：`Merge scan one region, partition: 0, region_id: 4398046511104(1024, 0)`
- `2026-04-02T03:25:47Z`：`Merge scan finish one region ... poll_duration: 1022.271527433s, first_consume: 1022.39s, do_get_cost: 114.870425ms`
- `2026-04-02T03:25:47Z`：`MergeScan partition 0 finished: 1 regions, total_poll_duration: 1022.271527433s, total_do_get_cost: 114.870425ms`
- `2026-04-02T03:25:47Z`：`DatafusionQueryEngine execute 1 stream, cost: 1022.392746601s`

同时 datanode 侧也有同一轮查询的关键日志：

- `2026-04-02T03:08:45Z`：开始 `Handle remote read for region: 4398046511104(1024, 0)`
- 同时启动了 `UnorderedScan partition 0..7`
- `2026-04-02T03:25:47Z`：8 个 partition 依次结束，并打印详细 `scan_metrics`

其中最关键的一条 datanode 日志（partition 0）包含：

- `total_cost: 1021.769668715s`
- `scan_cost: 942.231359688s`
- `yield_cost: 79.347085532s`
- `num_rows: 702571002`
- `rows_before_filter: 1052492468`
- `num_batches: 686115`
- `num_file_ranges: 6876`
- `rg_total: 10294`
- `sst_scan_cost: 938.287959103s`

另外几个 partition 也都在同一量级，整体表现为：

- 每个 partition 都在扫描数亿行
- 总耗时集中在 `scan_cost` / `sst_scan_cost`
- 这和 candidate 上约 `2.8s` 的行为已经完全不是一个量级

这条日志应该保留，后续可以作为：

- debug disabled 版本已经显著改变执行表现
- dyn filter 主路径很可能确实受到影响

后来这次长查询的 explain 结果也成功落盘了：

- 文件：`results/20260401-topk-min/disabled_end_time_explain_rerun1.txt`
- 耗时：`17:05.52`

这份 explain 里最关键的信息包括：

- `MergeScanExec finish_time: 1025.5s`
- `SortExec: TopK(fetch=10) ... filter=[end_time@1 IS NULL OR end_time@1 > ...]`
- `UnorderedScan ... partition_count.count = 54998`
- `files = 143`
- `file_ranges = 54998`

这说明：

- TopK 自己仍然会形成 runtime filter
- 但 disabled 版本下 scan 侧没有像 candidate 那样明显被提前收敛住
- 扫描范围和执行代价都被放大到了一个完全不同的量级

这份 explain 是当前最强的一条 debug 机制验证证据。

### 关于 PartSortExec / post-optimization filter pushdown 的新线索

初步查代码后发现，DataFusion 默认 physical optimizer 本身已经带有一个末尾的：

- `FilterPushdown::new_post_optimization()`

但 Greptime 自己的 `WindowedSortPhysicalRule` 是后面单独 push 进去的，所以现在更合理的猜测是：

- 不是“完全没有 post-optimization filter pushdown”
- 而是 `WindowedSortPhysicalRule` 出现在默认 post pushdown 之后
- 导致 `PartSortExec` 虽然实现了相关 hook，但没机会再参与一次 pushdown

因此下一个实验方向应改为：

- 在 `WindowedSortPhysicalRule` 后面，再显式补一次 `FilterPushdown::new_post_optimization()`
- 然后用 debug 镜像验证 `ORDER BY start_time DESC LIMIT 10` 的 scan `dyn_filters` 是否不再是 `empty`

这轮实验已经完成，结果是：

- 额外 post pushdown 之后，`start_time` 路径的 scan 侧确实出现了 dyn filter
- 但出现的是：`DynamicFilter [ empty ]`
- 也就是说，问题不再是“pushdown 根本没跑”，而是“pushdown 跑了，但被推下去的 filter 本身还是 empty”

### 关于 PartSortExec empty dyn filter 的当前根因判断

目前最强的根因判断是：

- `PartSortExec` 共享的是一个全局 `TopKDynamicFilters`
- 但执行过程中会多次创建局部 `TopK`
- 每个局部 `TopK` 只在自己的 heap threshold 比当前全局 threshold 更优时，才会更新 shared filter
- 一旦 shared filter 只在很早阶段更新过一次，后面很多局部 `TopK` 虽然仍在做 row replacement，但不再满足“比当前全局 threshold 更优”的条件
- 结果就是 shared filter 长时间停留在 empty / 宽松状态

这和目前观察到的现象一致：

- `PartSortExec` 的 `row_replacements` 一直在增加
- 但最终下推到 scan 的 dyn filter 仍然是 empty

关键代码链路：

1. `PartSortExec::create_filter()` 初始创建 `DynamicFilterPhysicalExpr::new(vec![expr], lit(true))`
2. `PartSortStream::push_buffer()` 在 `Top` 分支里调用 `top.insert_batch(batch)`
3. DataFusion `TopK::insert_batch()` 只有在 `replacements > 0` 时才调用 `update_filter()`
4. `TopK::update_filter()` 只有当新 threshold 比 shared filter 里的旧 threshold 更优时才真正更新表达式
5. `PartSortStream::sort_top_buffer()` 又会重建新的局部 `TopK`

目前更像是：

- `PartSortExec` 不是没有 dyn filter 传播能力
- 而是它的 dyn filter 更新模型和“多次局部 TopK + 共享一个全局 filter”的执行模型不够匹配

## 下一阶段的激进实验

为了尽快回答 benchmark 和 blog 里更实际的问题，下一阶段不直接修 `PartSortExec`，而是先做一个更激进、也更容易解释的实验：

### 实验目标

比较两种策略：

1. **当前 main**
   - non-time-index：走 `SortExec: TopK` + dyn filter
   - time-index：优先走 `WindowedSortExec` / `PartSortExec`

2. **禁用 `WindowedSortPhysicalRule` 的实验版**
   - 让 time-index 路径也尽量回到普通 `TopK` 路径
   - 观察 time-index 排序是否也能拿到更有效的 dyn filter

### 先做 debug 验证

正式 release benchmark 之前，先做一轮 debug 验证，确认：

- 禁用 `WindowedSortPhysicalRule` 后
- `ORDER BY start_time DESC LIMIT 10`
- 是否真的回落到 `SortExec: TopK`
- scan 侧是否出现非空 dyn filter

### 再做 release 对比

如果 debug 验证成立，再补一轮 release 对比，重点只比两组：

1. 当前 main
2. 禁用 windowed sort 的实验版

这样最终更容易回答：

- 当前实现为什么在 time-index 路径上没有吃到 dyn filter 收益
- 如果直接放弃 windowed sort，time-index 路径是否反而能从 TopK dyn filter 中获益

### no-windowedsort debug 验证结果

这轮 debug 验证已经确认：

- 单纯禁用 `WindowedSortPhysicalRule` 后，`ORDER BY start_time DESC LIMIT 10` 的计划会从
  - `WindowedSortExec -> PartSortExec`
  变成
  - `SortExec: TopK`
- scan 侧也从 `DynamicFilter [ empty ]` 变成了非空 dyn filter：
  - `DynamicFilter [ start_time@0 IS NULL OR start_time@0 > 1753660799000000000 ]`

对应的 explain 结果文件：

- `results/20260401-topk-min/no_windowedsort_start_time_explain_run2.txt`

这次最关键的观察：

- `finish_time: 1.92s`
- `total_do_get_cost: 183ms`

而之前仍走 `WindowedSortExec / PartSortExec` 的版本，大致是：

- `finish_time: 6.37s ~ 6.79s`
- dyn filter 为 `DynamicFilter [ empty ]`

所以目前可以比较稳地说：

- time-index 路径不是天然不能从 dyn filter 受益
- 至少在当前这组数据上，回退到普通 `TopK + dyn filter` 后，`start_time DESC LIMIT 10` 反而明显更快

## 正式 release benchmark 的新范围

在正式 benchmark 里，不应该只比较一个查询和两个版本，而应该比较：

### 三个版本

1. **当前 main**
   - non-time-index：`SortExec: TopK + dyn filter`
   - time-index：`WindowedSortExec / PartSortExec`

2. **禁用 `WindowedSortPhysicalRule` 的版本**
   - 让 time-index 路径尽量走普通 `TopK + dyn filter`

3. **禁用 dyn filter、但保留 windowed sort 的版本**
   - 作为“只有 windowed sort，没有 dyn filter”对照组

### 两类排序列

1. **time-index 列**：`start_time`
2. **non-time-index 列**：`end_time`

### 多个 limit 数量级

建议至少覆盖：

- `LIMIT 5`
- `LIMIT 10`
- `LIMIT 100`
- `LIMIT 1000`

### 目标

这轮 release benchmark 要回答的不只是“哪个版本更快”，而是更细一点：

1. 对 `start_time`（time-index）来说：
   - `WindowedSortExec` 在哪些 limit 下仍然更有优势？
   - 普通 `TopK + dyn filter` 在哪些 limit 下反而更好？

2. 对 `end_time`（non-time-index）来说：
   - dyn filter 的收益会不会随着 limit 从 `5 -> 1000` 而明显衰减？

3. 对三个版本整体来说：
   - “只有 windowed sort”
   - “只有 dyn filter / TopK”
   - “当前 main 混合策略”
   分别在哪些场景更优？

### 正式 benchmark 的矩阵

建议最终至少形成这样一个矩阵：

- 列：`start_time` / `end_time`
- limit：`5 / 10 / 100 / 1000`
- 版本：
  - current main
  - no-windowedsort
  - no-dynfilter-but-windowedsort

也就是总共：

- `2 * 4 * 3 = 24` 个主测试点

每个测试点至少：

- 跑 3 次 query
- 保留至少 1 份 `EXPLAIN ANALYZE VERBOSE`
- 提取 scan 侧关键指标

这轮 benchmark 的意义，不再只是给 blog 补一个 headline 数字，而是帮我们判断：

- 当前混合策略是否真的是合理折中
- 还是说应该重新考虑 time-index 路径优先 `WindowedSortExec` 的策略

## 当前 release 镜像准备进度

截至当前，release 版本准备分成三条：

### 1. current main release

状态：**已完成 build/push**

- tag: `localhost:5001/nightly-greptimedb:20260402-topk-main-2b4e12c358-release`
- pushed digest: `sha256:42776e764df2cf00f1a0bfa4f640dfdc572e5b8cf07e8bbe6dbe196c47e1601c`

对应含义：

- 当前 main 的真实 release 基线
- non-time-index 走 `TopK + dyn filter`
- time-index 仍走当前默认策略（windowed sort / part sort）

### 2. no-windowedsort release

状态：**已完成 build/push**

- 目标 tag: `localhost:5001/nightly-greptimedb:20260402-topk-main-2b4e12c358-no-windowedsort-release`
- log: `results/20260401-topk-min/release_build_no_windowedsort.log`
- pushed digest: `sha256:0b8bf9cae0f014d8309bbed1c3af15564dab857204b86f09828e9a4037d2bd0e`

对应含义：

- 禁用 `WindowedSortPhysicalRule`
- 让 time-index 路径尽量回到普通 `TopK + dyn filter`

### 3. no-dynfilter-but-windowedsort release

状态：**已完成 build/push**

- 目标 tag: `localhost:5001/nightly-greptimedb:20260402-topk-main-2b4e12c358-no-dynfilter-windowedsort-release`
- log: `results/20260401-topk-min/release_build_no_dynfilter_windowedsort.log`
- pushed digest: `sha256:f91986a27030bc66e9c207aec1efa15038ae9d54fbb4f4dcee209b4ceaea4cfc`

对应含义：

- 保留 windowed sort
- 关闭 scan-side dyn filter
- 用来观察“只有 windowed sort，没有 dyn filter”时的行为

## 当前工作树状态

当前工作树代码状态是：

- `no-dynfilter-but-windowedsort`

也就是：

- `src/table/src/table/scan.rs` 的 disable dyn filter 改动已应用
- `state.rs` 已恢复，不是 no-windowedsort 状态

另外两个实验状态已单独 stash，便于后续切换：

- `no-windowedsort-release-prep` / `no-windowedsort-release-state-2`
- `postpushdown-and-partsort-log-experiment`
- `disable-dyn-filter-scan-hook`

## LIMIT 10 release sanity check 结果

这轮 sanity check 已经完成，覆盖了 3 个 release 版本和两类排序列：

- `start_time DESC LIMIT 10`
- `end_time DESC LIMIT 10`

### current main release

- `start_time`
  - `WindowedSortExec -> PartSortExec`
  - scan `dyn_filters = ["DynamicFilter [ empty ]"]`
  - `finish_time: 342ms`
  - `total_do_get_cost: 196ms`
- `end_time`
  - `SortExec: TopK`
  - scan `dyn_filters` 非空
  - `finish_time: 236ms`
  - `total_do_get_cost: 17ms`

对应文件：

- `results/20260401-topk-min/sanity_current_main_release_start_time_explain.txt`
- `results/20260401-topk-min/sanity_current_main_release_end_time_explain.txt`

### no-windowedsort release

- `start_time`
  - `SortExec: TopK`
  - scan `dyn_filters` 非空
  - `finish_time: 255ms`
  - `total_do_get_cost: 35ms`
- `end_time`
  - `SortExec: TopK`
  - scan `dyn_filters` 非空
  - `finish_time: 228ms`
  - `total_do_get_cost: 36ms`

对应文件：

- `results/20260401-topk-min/sanity_no_windowedsort_release_start_time_explain.txt`
- `results/20260401-topk-min/sanity_no_windowedsort_release_end_time_explain.txt`

### no-dynfilter-but-windowedsort release

- `start_time`
  - `WindowedSortExec -> PartSortExec`
  - scan 侧不再出现 dyn filter
  - `finish_time: 343ms`
  - `total_do_get_cost: 192ms`
- `end_time`
  - `SortExec: TopK`
  - scan 侧不再出现 dyn filter
  - `finish_time: 45.1s`
  - `total_do_get_cost: 20ms`

对应文件：

- `results/20260401-topk-min/sanity_no_dynfilter_windowedsort_release_start_time_explain.txt`
- `results/20260401-topk-min/sanity_no_dynfilter_windowedsort_release_end_time_explain.txt`

### 这轮 sanity check 的直接结论

1. `end_time`（non-time-index）对 dyn filter 极其敏感：
   - current main / no-windowedsort 都在亚秒级
   - no-dynfilter-windowedsort 直接退化到 `45.1s`

2. `start_time`（time-index）在 `LIMIT 10` 这个点上：
   - current main 的 `WindowedSortExec / PartSortExec` 并不差
   - 但 no-windowedsort 的 `TopK + 非空 dyn filter` 仍然更快

3. `PartSortExec` 的当前问题更加明确：
   - current main 中它推下去的是 `empty dyn filter`
   - 完全回退到普通 `TopK` 后，`start_time` 也能拿到非空 dyn filter

## 完整 release benchmark matrix 结果

完整 matrix 已跑完，覆盖：

- 版本：`current main` / `no-windowedsort` / `no-dynfilter-windowedsort`
- 排序列：`start_time` / `end_time`
- limit：`5 / 10 / 100 / 1000`
- 每个点 3 次 query + 1 份 `EXPLAIN ANALYZE VERBOSE`

结果文件统一保存在：

- `results/20260401-topk-min/matrix_current_main_release_*`
- `results/20260401-topk-min/matrix_no_windowedsort_release_*`
- `results/20260401-topk-min/matrix_no_dynfilter_windowedsort_release_*`

### current main release

- `start_time LIMIT 5`: p50 `0.360s`, best `0.340s`, worst `0.368s`
- `start_time LIMIT 10`: p50 `0.336s`, best `0.336s`, worst `0.340s`
- `start_time LIMIT 100`: p50 `0.339s`, best `0.338s`, worst `0.346s`
- `start_time LIMIT 1000`: p50 `0.335s`, best `0.333s`, worst `0.347s`
- `end_time LIMIT 5`: p50 `0.216s`, best `0.214s`, worst `0.236s`
- `end_time LIMIT 10`: p50 `0.246s`, best `0.210s`, worst `0.266s`
- `end_time LIMIT 100`: p50 `0.239s`, best `0.237s`, worst `0.256s`
- `end_time LIMIT 1000`: p50 `0.260s`, best `0.240s`, worst `0.269s`

explain 现象：

- `start_time` 仍为 `WindowedSortExec -> PartSortExec`
- scan 侧仍为 `dyn_filters = ["DynamicFilter [ empty ]"]`
- `end_time` 为 `SortExec: TopK`
- scan 侧 dyn filter 非空

### no-windowedsort release

- `start_time LIMIT 5`: p50 `0.387s`, best `0.332s`, worst `0.432s`
- `start_time LIMIT 10`: p50 `0.227s`, best `0.211s`, worst `0.228s`
- `start_time LIMIT 100`: p50 `0.228s`, best `0.221s`, worst `0.247s`
- `start_time LIMIT 1000`: p50 `0.244s`, best `0.236s`, worst `0.245s`
- `end_time LIMIT 5`: p50 `0.241s`, best `0.217s`, worst `0.242s`
- `end_time LIMIT 10`: p50 `0.232s`, best `0.220s`, worst `0.238s`
- `end_time LIMIT 100`: p50 `0.254s`, best `0.247s`, worst `0.280s`
- `end_time LIMIT 1000`: p50 `0.394s`, best `0.379s`, worst `0.531s`

explain 现象：

- `start_time` 回落为 `SortExec: TopK`
- scan 侧 dyn filter 非空：`DynamicFilter [ start_time@0 IS NULL OR start_time@0 > ... ]`
- `end_time` 仍为 `SortExec: TopK`
- scan 侧 dyn filter 非空

### no-dynfilter-but-windowedsort release

- `start_time LIMIT 5`: p50 `0.340s`, best `0.330s`, worst `0.347s`
- `start_time LIMIT 10`: p50 `0.336s`, best `0.334s`, worst `0.342s`
- `start_time LIMIT 100`: p50 `0.328s`, best `0.325s`, worst `0.335s`
- `start_time LIMIT 1000`: p50 `0.338s`, best `0.338s`, worst `0.342s`
- `end_time LIMIT 5`: p50 `31.384s`, best `31.231s`, worst `31.781s`
- `end_time LIMIT 10`: p50 `31.594s`, best `31.503s`, worst `31.647s`
- `end_time LIMIT 100`: p50 `31.640s`, best `31.560s`, worst `31.654s`
- `end_time LIMIT 1000`: p50 `31.757s`, best `31.691s`, worst `31.866s`

explain 现象：

- `start_time` 仍为 `WindowedSortExec -> PartSortExec`
- scan 侧不再出现 dyn filter
- `end_time` 仍为 `SortExec: TopK`
- TopK 自己仍生成 runtime filter，但 scan 侧不消费 dyn filter

### 这轮 matrix 的直接结论

1. `end_time`（non-time-index）对 scan-side dyn filter 的依赖非常强，而且这个结论在 `LIMIT 5 -> 1000` 上都成立。
2. `current main` 和 `no-windowedsort` 在 `end_time` 上都稳定保持亚秒级；`no-dynfilter-windowedsort` 则稳定退化到约 `31.5s`。
3. `start_time`（time-index）在当前数据集上，`WindowedSortExec / PartSortExec` 并没有随更大 LIMIT 展现出更强优势；`no-windowedsort` 在 `LIMIT 10 / 100 / 1000` 上反而更快。
4. `start_time LIMIT 5` 上，`current main` 与 `no-dynfilter-windowedsort` 基本接近，而 `no-windowedsort` 没有形成明显优势，说明非常小的 LIMIT 下 windowed sort 至少没有明显吃亏。
5. `PartSortExec` 的核心问题没有变化：它仍然能产生大量 `row_replacements`，但 scan 侧最终看到的 dyn filter 仍是 `empty`。

### 当前更稳妥的判断

- blog headline 仍应放在 `end_time` 这条 non-time-index 路径上。
- 对 `start_time` 路径，不宜写成“当前 windowed sort 明显更优”；至少在这轮 release matrix 上，数据并不支持这个结论。
- 如果后续还要继续做实现决策，最值得补的是：
  - 为什么 `PartSortExec` 的 shared dyn filter 会停留在 `empty`
  - 是否值得做第四个版本，验证“保留 windowed sort，但让 PartSortExec 推出非空 dyn filter”

## 下一步

现在应把任务管理状态更新为：

- full matrix benchmark 标记为 completed
- tradeoff analysis 标记为 in_progress
- 再根据这轮 matrix 决定是否需要第四个 release variant

## 策略 tradeoff 分析

这轮 3-variant release matrix 已经足够回答一个更具体的问题：当前 `current main` 的混合策略，到底在哪些场景占优，哪些场景暴露了明显短板。

### 1. non-time-index：dyn filter 明显比 windowed sort 重要

对 `ORDER BY end_time DESC LIMIT k` 来说：

- `current main` 与 `no-windowedsort` 都稳定在亚秒级
- `no-dynfilter-windowedsort` 稳定在 `31s+`
- 而且这个趋势在 `LIMIT 5 / 10 / 100 / 1000` 上都没有反转

这说明：

- 对 non-time-index TopK 路径来说，主要收益并不是来自 windowed sort
- 关键仍然是 `SortExec: TopK` 产生非空 runtime threshold，并被 scan 侧消费成 dyn filter
- 只要 scan 不消费这个 dyn filter，TopK 虽然还会在算子内部维护 threshold，但执行时间会回到“几乎全扫”的量级

因此 blog 的 headline 继续围绕 `end_time` 这条路径是稳妥的。

### 2. time-index：当前 windowed sort 没有兑现预期中的优势

对 `ORDER BY start_time DESC LIMIT k` 来说，这轮数据更像是在说明：

- `current main` 很稳定，但并不领先
- `no-windowedsort` 在 `LIMIT 10 / 100 / 1000` 上都更快
- `LIMIT 5` 上三者接近，没有证据表明 windowed sort 在超小 limit 下有明显收益

配合 explain 可以得出更明确的解释：

- `current main` 仍走 `WindowedSortExec -> PartSortExec`
- 但 scan 侧看到的 dyn filter 仍然是 `DynamicFilter [ empty ]`
- `no-windowedsort` 回退到普通 `SortExec: TopK` 后，scan 侧反而拿到了非空 dyn filter

也就是说，当前 time-index 路径的问题不是“没有 TopK 语义”，而是：

- `PartSortExec` 这条实现没有把不断收紧的 threshold 变成真正可消费的 scan-side dyn filter

### 3. 当前 mixed strategy 的真实画像

这轮 matrix 后，对三个策略可以更直白地总结成：

1. `current main`
   - 优点：`end_time` 很快，`start_time` 也不差，整体较稳
   - 缺点：time-index 路径没有兑现 dyn filter 收益，`PartSortExec` 的 dyn filter 仍是 empty

2. `no-windowedsort`
   - 优点：`start_time` 和 `end_time` 都能走普通 `TopK + 非空 dyn filter`
   - 缺点：放弃了 windowed sort 的原始设计意图；这轮数据里没看到损失，但还不能证明所有数据分布都如此

3. `no-dynfilter-windowedsort`
   - 优点：保留了 windowed sort 路径，`start_time` 仍然可接受
   - 缺点：`end_time` 直接退化到几十秒，说明对 headline query family 不可接受

所以目前最合理的结论不是“windowed sort 没价值”，而是：

- 当前实现下，windowed sort 的潜在价值还没有通过有效 dyn filter 传播兑现出来

### 4. 是否需要第四个版本

现在已经有了一个更清晰的第四版本目标，不再是模糊地“继续试试看”，而是一个更具体的实现假设：

- 在 `PartSortExec` 内，额外维护一个全生命周期的 `TopK`
- 这个 `TopK` 不参与当前分段 / 局部 top 的执行控制
- 它只负责维护一个持续收紧的全局 threshold
- 再用这个 threshold 去更新 shared dyn filter

这样做想解决的核心问题是：

- 现有执行过程中会多次创建局部 `TopK`
- shared dyn filter 的更新时机和局部 `TopK` 生命周期不匹配
- 导致 scan 侧最后拿到的仍是 empty / 过宽的 filter

如果额外维护一个“全生命周期 TopK for dyn filter”，那第四个版本要验证的就会很明确：

- 是否能保留 `WindowedSortExec / PartSortExec` 的执行形态
- 同时让 `start_time` 路径也稳定下推出非空 dyn filter
- 再和 `no-windowedsort` 对比，看它是否能在 time-index 路径上真正赢回来

### 5. 当前建议

当前最自然的后续顺序是：

1. 保留这轮 3-variant matrix 作为 benchmark 主体证据
2. 把 `subtask_08` 明确成“实现并验证 PartSortExec 全生命周期 TopK dyn filter 版本是否值得做第四个 release variant”
3. 如果实现成立，再决定是否补第四个 release benchmark，而不是先在 blog 里写强结论

## Task 8 当前实现进展

已经按“额外维护一个全生命周期 TopK”这个方向做了最小实现验证，改动集中在：

- `src/query/src/part_sort.rs`

当前实现方式是：

- 保留原来的局部 `TopK` 作为 `PartSortBuffer::Top`，继续负责当前 `PartSortExec` 的缓冲与最终输出
- 额外在 `PartSortStream` 上挂一个长期存活的 `dyn_filter_topk`
- 每次 `push_buffer()` 时：
  - 先把 batch 喂给局部 `TopK`
  - 再把同一批数据喂给长期 `dyn_filter_topk`
- 两者共享同一个 `TopKDynamicFilters`

这里顺序很重要：

- 一开始把长期 `TopK` 放在前面更新，会导致 shared dyn filter 在同一批数据内提前收紧
- 局部 `TopK` 再读这同一批数据时，会把本来应该保留用于最终结果的行过滤掉
- 调整成“先局部、后长期”后，`part_sort` 相关测试恢复通过

新增/验证过的测试：

- `cargo test -p query test_long_lived_topk_updates_dyn_filter_across_resets`
- `cargo test -p query part_sort`

这轮实现目前只能说明：

- “额外维护一个全生命周期 TopK”这个代码方向在单元测试层面是可行的
- 并且能覆盖一个关键场景：局部 `TopK` 重置后，shared dyn filter 仍能靠长期 `TopK` 继续收紧

但它还不能说明：

- 当前线上 `PartSortExec` empty dyn filter 的根因就一定是这个问题
- 或者这个改动在真实 benchmark 上一定能把 `start_time` 路径变成非空 dyn filter

所以下一步仍然需要：

1. 用 debug 镜像验证 `ORDER BY start_time DESC LIMIT 10` 的 explain 是否从 `DynamicFilter [ empty ]` 变成非空 dyn filter
2. 如果 debug 成立，再决定是否补第四个 release variant benchmark

## Task 8 调查结果：empty dyn filter 的真实根因

后续 debug 调查表明，`PartSortExec` 的问题并不主要在 `gather_filters_for_pushdown()` 本身，而是在 `with_new_children()` 的生命周期语义。

### 关键现象回顾

在给 `PartSortExec` 增加长期 `dyn_filter_topk`、并在 explain 中直接打印 `PartSortExec.filter` 之后，可以明确看到：

- `PartSortExec` 自己持有的 dyn filter 已经会更新为非空
- 但 scan 侧 `dyn_filters` 仍然显示为 `empty`

这说明：

- `PartSortExec` 内部 filter 更新本身是有效的
- 问题发生在“下推到 scan 后，为什么 scan 仍然绑定的是旧 filter”

### 真正根因

最终定位到：

- `src/query/src/part_sort.rs`
- `PartSortExec::with_new_children()`

原来的实现会在 `with_new_children()` 中直接调用 `PartSortExec::try_new(...)`，从而重建一个新的 `PartSortExec`，并连带重建一份新的空 dyn filter。

而 DataFusion 的 filter pushdown optimizer 在处理 child 更新时，会调用 `with_new_children()`。这样就形成了断链：

1. optimizer 在 pushdown 阶段，把旧 `PartSortExec.filter` 推给 scan
2. optimizer 又因为 child 被改写，调用 `with_new_children()`
3. `PartSortExec` 被替换成一个新的 exec
4. 新 exec 持有的是一份新的空 filter
5. 真正执行时更新的是新 filter
6. scan 侧仍然绑定旧 filter

这就解释了之前的怪现象：

- `PartSortExec` 自己显示非空 dyn filter
- scan 侧第二个 dyn filter 仍然一直是 empty

### 修复方式

修复后的 `PartSortExec::with_new_children()` 不再重建 filter，而是：

- 保留当前 exec 的 live filter
- 只替换新的 child
- 重新计算 `properties`

这和 DataFusion 原生 `SortExec` 的语义一致：

- `with_new_children()`：保留动态 filter
- `reset_state()`：才重建动态 filter

### v5 debug 验证结果

使用修复后的 debug v5 镜像：

- image tag: `20260403-topk-partsort-lifetime-topk-debug-v5`
- digest: `sha256:29006cbca15cd7180f218bf8ebcaa14711991f5fc19c5f9b63cc21c22c09396f`

对查询：

- `EXPLAIN ANALYZE VERBOSE SELECT * FROM langchain_traces ORDER BY start_time DESC LIMIT 10`

结果文件：

- `results/20260401-topk-min/debug_partsort_lifetime_topk_v5_start_time_limit10_explain.txt`

关键信号：

- `PartSortExec: ... dyn_filter=DynamicFilter [ start_time@0 IS NULL OR start_time@0 > 1753660799000000000 ]`
- scan 侧：
  - `dyn_filters = ["DynamicFilter [ empty ]", "DynamicFilter [ start_time@0 IS NULL OR start_time@0 > 1753660799000000000 ]"]`

也就是说：

- 旧 `SortExec` 留下来的 empty dyn filter 仍然存在（这目前是预期现象）
- 但 `PartSortExec` 自己那份第二个 dyn filter 终于不再是 empty，而是会随执行更新

### 当前结论

现在已经可以相当有把握地说：

- “保留 windowed sort，同时让 PartSortExec 推出可更新的非空 dyn filter” 这条方向是成立的
- 至少在 debug explain 层面，关键链路已经被修通

因此，`subtask_08` 的结论已经接近明确：

- **值得做第四个 release variant**

下一步不再是继续猜测根因，而是：

1. 基于当前修复构建一个 release variant
2. 对 `start_time` 做最小 sanity check
3. 如果收益明确，再把它纳入完整 release 比较或 blog 结论候选

## 第四个 release variant 结果

基于上述修复，又构建了第四个 release variant：

- image tag: `20260403-topk-partsort-lifetime-topk-release-v1`
- digest: `sha256:398acb3393953af958122aabd6edda860dc4c7ed59774a40f4ab32ba85b9fdf5`

这个版本包含：

- `PartSortExec` 长生命周期 `dyn_filter_topk`
- 局部 `TopK` 与全局 pushdown filter 分离
- `WindowedSortPhysicalRule` 后重新执行 post filter pushdown
- `PartSortExec::with_new_children()` 保留 live dyn filter，不再在优化阶段重建 filter

### release sanity check

先跑了 `LIMIT 10` 的 sanity check。

#### `ORDER BY start_time DESC LIMIT 10`

- query runs: `0.609860s / 0.585814s / 0.581059s`
- explain finish_time: `530ms`
- explain 结果：
  - `WindowedSortExec -> PartSortExec`
  - `PartSortExec.dyn_filter` 非空
  - scan 侧 `dyn_filters = ["DynamicFilter [ empty ]", "DynamicFilter [ start_time@0 IS NULL OR start_time@0 > ... ]"]`

#### `ORDER BY end_time DESC LIMIT 10`

- query runs: `0.232075s / 0.235072s / 0.255080s`
- explain finish_time: `186ms`
- explain 结果：
  - `SortExec: TopK`
  - scan 侧 dyn filter 为非空

这个 sanity check 已经证明：

- 第四个 variant 确实把 `PartSortExec` 的第二个 dyn filter 修通了
- 但 `start_time` 路径性能看上去并没有优于现有 release 版本

### 第四个 variant 完整 matrix

随后跑了完整 matrix：

- variant: `partsort_lifetime_topk_release_v1`
- columns: `start_time`, `end_time`
- limits: `5 / 10 / 100 / 1000`
- 每点 3 次 query + 1 次 explain

结果文件前缀：

- `results/20260401-topk-min/matrix_partsort_lifetime_topk_release_v1_*`

#### `start_time`

- `LIMIT 5`: p50 `0.586244s`, best `0.530560s`, worst `0.587927s`
- `LIMIT 10`: p50 `0.587040s`, best `0.576632s`, worst `0.591889s`
- `LIMIT 100`: p50 `0.568906s`, best `0.567115s`, worst `0.591001s`
- `LIMIT 1000`: p50 `0.583684s`, best `0.561384s`, worst `0.589705s`

explain 一致表现：

- `WindowedSortExec -> PartSortExec`
- `PartSortExec.dyn_filter` 为非空
- scan 侧 dyn filters 为：
  - `DynamicFilter [ empty ]`
  - `DynamicFilter [ start_time@0 IS NULL OR start_time@0 > ... ]`

#### `end_time`

- `LIMIT 5`: p50 `0.232006s`, best `0.223660s`, worst `0.235652s`
- `LIMIT 10`: p50 `0.239240s`, best `0.235828s`, worst `0.239294s`
- `LIMIT 100`: p50 `0.278432s`, best `0.255436s`, worst `0.286866s`
- `LIMIT 1000`: p50 `0.269732s`, best `0.261617s`, worst `0.277826s`

explain 一致表现：

- `SortExec: TopK`
- scan 侧两份 `end_time` dyn filter 都是非空

### 与已有三个 release variant 对比后的结论

这第四个 variant 回答了两个不同层面的问题：

#### 1. 机制层面：修通了

这次修复已经明确证明：

- `PartSortExec` 的 live dyn filter 可以真正传到 scan 侧
- 之前的问题确实是优化阶段 `with_new_children()` 重建 filter 导致 scan / exec 断链

换言之：

- “为什么第二个 dyn filter 一直是 empty” 这个问题已经被定位并修复

#### 2. benchmark 层面：当前实现仍不占优

但在这套 release benchmark 上：

- `current_main_release` 的 `start_time` 大约在 `0.33s`
- `no_windowedsort_release` 的 `start_time` 大约在 `0.23s ~ 0.24s`
- `partsort_lifetime_topk_release_v1` 的 `start_time` 大约在 `0.57s ~ 0.59s`

也就是说：

- 机制修通了
- 但性能并没有赢回来，反而更慢

### 当前判断

因此当前最稳妥的工程结论是：

- **在 window sort 的 TopK 场景下，当前实现更适合直接禁用 windowed sort，让查询回退到普通 `SortExec: TopK`**
- **window sort 的全量排序路径仍可保留**，因为问题集中在 `window sort + TopK(limit)` 这个组合上，而不是 window sort 整体语义都不可接受

换句话说：

- “window sort 全量排序还保留”
- “window sort TopK 先禁用”

这比继续把当前 `PartSortExec + dyn filter` 版本直接当成最终 release 方案要稳妥得多。
