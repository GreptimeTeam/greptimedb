# 任务：补做 dyn filter / TopK 最小复测 benchmark

## 目标

在现有 k8s 集群上补一轮**最小复测**，验证 GreptimeDB 在 `langchain_traces` 数据集上的 dyn filter / `TopK` headline path 仍然成立。

本任务不追求完整大盘 benchmark，也不追求覆盖所有查询组合；重点是以较小成本补足博客最需要的最新证据：

- non-time-index 排序列 `end_time`
- DataFusion `SortExec: TopK` 持续更新 dynamic filter
- GreptimeDB table scan 消费更新后的 filter，并在 file / row-group 层剪枝

本任务产出不是博客正文，而是“**最小可引用复测结论 + 原始证据包**”。

## 交付物

1. 一份最小复测记录文档，包含环境、镜像 tag、commit、查询、结果表。
2. 一份 explain / analyze 证据集，至少覆盖 baseline 与 candidate 的 headline case。
3. 一段可直接供博客使用的 benchmark 摘要，明确哪些数字可对外引用。

## 已知环境与输入

- 构建/推镜像脚本：`/home/discord9/langchain-traces-benchmark/build_push.sh`
- Helm values：`/home/discord9/langchain-traces-benchmark/deployments/greptimedb-cluster/values.yaml`
- 本地 registry：`192.168.50.81:5001`
- 现成查询集：`/home/discord9/langchain-traces-benchmark/topk_dyn_filter_test.sql`

从已有文件可确认：

1. `build_push.sh` 会在 `/home/discord9/greptimedb_for_gc` 构建二进制并打包镜像，再推到本地 registry。
2. `values.yaml` 已经指向 `192.168.50.81:5001/nightly-greptimedb:...` 这类镜像配置。
3. `topk_dyn_filter_test.sql` 已区分两类路径：
   - time-index + `PartSortExec`
   - non-time-index + DataFusion `SortExec: TopK`

## 基于现有 k8s 集群的执行方式

本次复测默认复用现成的 k8s 部署流程，不单独设计新的部署链路。执行上以 `/home/discord9/langchain-traces-benchmark/` 目录下的现有脚本和 Make 目标为准。

### 统一流程

1. 准备好 baseline / candidate 对应的 GreptimeDB 二进制源码目录。
2. 调整 `/home/discord9/langchain-traces-benchmark/build_push.sh` 中使用的二进制或源码路径，确保它指向当前要构建的版本。
3. 运行 `build_push.sh` 构建并推送镜像。
4. 更新 `/home/discord9/langchain-traces-benchmark/deployments/greptimedb-cluster/values.yaml` 中的镜像名 / tag。
5. 在 `/home/discord9/langchain-traces-benchmark/` 下执行 `make deploy-cluster` 完成部署。
6. 等待集群健康后，再执行查询与采集结果。

### 这轮复测的约束

- baseline 和 candidate 都必须走同一套 build / push / deploy 流程。
- 除镜像版本外，尽量不要改动 `values.yaml` 中其他会影响结果的配置。
- 如果确实修改了部署参数，必须记录原因，否则结果口径降级。
- 每次切换镜像后，都要确认 rollout 完成、服务可用、数据可读，再开始跑查询。

### 需要显式记录的 k8s 操作信息

- `build_push.sh` 当时指向的源码/二进制路径
- 最终推送出的镜像 tag
- `values.yaml` 中实际使用的镜像名
- `make deploy-cluster` 对应的部署时间点
- baseline / candidate 各自部署完成后的确认信息

## 本次最小复测要回答的问题

1. 最新 candidate 上，`ORDER BY end_time DESC LIMIT 10` 是否仍保持显著收益？
2. explain / analyze 是否仍能看到：
   - `SortExec: TopK(..., filter=[...])`
   - scan 节点上的 `dyn_filters`
3. 相比 baseline，scan 阶段耗时是否明显下降？
4. `ORDER BY start_time DESC LIMIT 10` 作为对照组，是否仍然“本来就快、变化不大”？
5. 能否形成一段足够稳妥、可直接进入博客的“最新最小复测”口径？

## benchmark 范围

### 必测查询

只保留两类最关键查询：

1. **headline case**：`ORDER BY end_time DESC LIMIT 10`
2. **control case**：`ORDER BY start_time DESC LIMIT 10`

如果时间非常充裕，可补：

3. `ORDER BY start_time ASC LIMIT 5`

但该项不是本任务完成前提。

### 必收集证据

每个必测查询至少保留：

1. 真实执行耗时
2. `EXPLAIN ANALYZE VERBOSE` 输出
3. `SortExec: TopK` / `PartSortExec` / scan `dyn_filters` 相关片段
4. 若可见，scan / file-range / row-group pruning 相关指标

## baseline / candidate 定义

### baseline

满足以下任一即可：

1. 当前线上或已知稳定 tag
2. dyn filter 关键改动之前的 commit
3. 已在 PR / 历史 artifact 中出现过的参考镜像

要求：必须明确记录 commit 或镜像 tag，不能只写“旧版本”。

### candidate

使用当前准备验证的最新代码构建镜像。

要求：

1. 记录源码路径
2. 记录 commit id
3. 记录镜像 tag

## 执行步骤

### 任务 1：确定 baseline / candidate

- 选定 baseline commit/tag 与 candidate commit/tag。
- 确认两者差异主要集中在 dyn filter / TopK 相关逻辑，而不是大范围无关优化。

完成标准：两端版本均可明确到 commit 或 image tag。

### 任务 2：构建并推送镜像

- 按 `build_push.sh` 逻辑构建 baseline 与 candidate 镜像。
- 推送至 `192.168.50.81:5001`。
- tag 命名需清晰区分 baseline 与 candidate。
- 构建前确认 `build_push.sh` 中引用的二进制/源码路径已经切到正确版本。

完成标准：两个镜像都能在 registry 中被拉取，并有清晰 tag。

### 任务 3：部署 baseline 并采集结果

- 更新 `deployments/greptimedb-cluster/values.yaml` 到 baseline 镜像。
- 在 `/home/discord9/langchain-traces-benchmark/` 下执行 `make deploy-cluster`。
- 确认服务健康、数据可读。
- 运行必测查询。
- 每个重点查询至少重复 3 次，记录首次与稳定态。

完成标准：baseline 有完整可回溯结果。

### 任务 4：部署 candidate 并采集结果

- 更新 `deployments/greptimedb-cluster/values.yaml` 到 candidate 镜像。
- 在 `/home/discord9/langchain-traces-benchmark/` 下再次执行 `make deploy-cluster`。
- 在相同集群形态、相同数据集上切换到 candidate。
- 确认无脏状态残留影响结果。
- 使用与 baseline 完全相同的查询集与顺序。
- 每个重点查询至少重复 3 次。

完成标准：candidate 有完整可回溯结果。

### 任务 5：整理最小对比表

- 逐查询整理 baseline / candidate：
  - 端到端耗时
  - scan 相关耗时
  - 是否出现 `SortExec: TopK(..., filter=[...])`
  - scan 节点是否出现 `dyn_filters`
  - control case 是否仅表现为“本来就快、变化不大”

完成标准：有一张可直接供博客引用的最小结果表。

### 任务 6：形成发布口径

- 输出一段可直接贴进博客的 benchmark 摘要。
- 强调这是**最新最小复测**，不是完整大盘 benchmark。
- 若绝对值波动大，则保留趋势与证据链，不写过强数字结论。

完成标准：复测完成后，博客才可以自然写成“已完成最新复测”，同时口径依然稳妥。

## 记录格式要求

每轮 benchmark 至少记录：

- 日期
- 集群环境说明
- baseline commit/tag
- candidate commit/tag
- 镜像 tag
- 查询 SQL
- 执行次数
- p50 / 最好值 / 最差值
- explain 关键片段
- 备注（是否 warm cache、是否存在异常波动）

## 风险与控制

### 风险 1：共享集群噪声过大

- 控制：每个重点查询至少跑 3 次，并同时记录首轮与稳定态。

### 风险 2：baseline / candidate 差异不纯

- 控制：记录 commit 范围；若差异过大，降低结论强度。

### 风险 3：数据状态变化影响结果

- 控制：保证同一数据集，不在两轮之间变更数据内容；如有 flush / compaction，单独记录。

### 风险 4：control case 容易被误写成 headline

- 控制：始终明确 `start_time` 路径只是对照组，不把 time-index 路径收益写成主结论。

### 风险 5：镜像切换正确，但部署实际没生效

- 控制：每次 `make deploy-cluster` 后都记录实际使用的镜像名/tag，并确认 rollout 与服务健康状态。

## 明确禁止

- 不要把历史 PR 数字和这轮最新复测混写成同一组结果。
- 不要省略 baseline / candidate 的 commit 或 tag。
- 不要把 time-index 路径的弱收益写成 headline。
- 不要在文档中复制任何对象存储或认证密钥。
- 不要在复测尚未执行完成前，把计划文档写成“结果已确认”。

## 验收标准

- headline case 能明确证明或否定：最新 candidate 上 non-time-index `SortExec: TopK` + table-scan pruning 是否仍成立。
- baseline / candidate 对比可回溯到 commit、镜像 tag、查询与原始输出。
- explain 证据能支撑“producer = `SortExec: TopK`，consumer = table scan”。
- control case 被清楚标成对照组，而非 headline case。
- 最终结果能直接服务博客写作，且口径明确为“最新最小复测”。
