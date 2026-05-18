# RFC: Remote Dynamic Filter Propagation (Phase 1)

## 状态

- Status: Draft
- Scope: Phase 1 最小可用版本
- Related:
  - `remote-dyn-filter-task-index.md`
  - `remote-dyn-filter-task-01-wire-abi.md`
  - `remote-dyn-filter-task-02-region-rpc.md`
  - `remote-dyn-filter-task-03-frontend-producer.md`
  - `remote-dyn-filter-task-04-datanode-apply.md`
  - `remote-dyn-filter-task-05-observability-fallback.md`
  - `remote-dyn-filter-task-06-validation.md`
  - `remote-dyn-filter-task-07-large-build-membership.md`

## 背景

GreptimeDB 现有动态过滤在单节点或本地执行链路中可以减少 probe side 扫描量，但在 distributed query 下，join build side 产生的动态过滤不会自动传播到远端 datanode scan。结果是：

- frontend 已经知道某个 dyn filter 可以收紧 probe side
- 远端 datanode scan 仍按较宽 predicate 扫描
- 优化收益局限在 frontend 本地，不能转化为分布式 pruning 收益

本 RFC 的目标是把 frontend 产生的 dyn filter 增量传播到 datanode table scan，同时保持一个关键约束：

> Remote dyn filter 是优化项，不是查询正确性的前提。

任何编码失败、RPC 失败、状态丢失、乱序、清理异常，都只能关闭优化，不能破坏查询结果正确性。

## 目标

Phase 1 目标：

1. 建立 frontend -> datanode 的最小控制面，使 dyn filter update 能跨节点传播。
2. 在 frontend 为 join 产生的 alive dyn filter 建立 query-scoped registry，并持续观察其更新。
3. 在 datanode 维护 `query_id + filter_id` 对应的 query-scoped remote filter state，并把 update 应用到 scan predicate。
4. 先支持分布式 `IN` filter 语义，为后续 `MIN_MAX` / `BLOOM` 预留协议与实现扩展位。
5. 在异常路径下保证安全降级与可观测。

## 非目标

Phase 1 不做：

- 独立 gRPC service 或 Arrow Flight 控制面
- batched update / streaming update / ack / heartbeat
- Bloom 或大 build-side membership 的最终方案
- 以 remote dyn filter 为前提的查询级阻塞或一致性协议
- 把 routing 元数据混入 filter state identity

## 端到端主链路

Phase 1 的主链路如下：

1. **join 产生 alive dyn filter**
   - join 是 dyn filter producer。
   - frontend 持有 alive `DynamicFilterPhysicalExpr`，后续 update 由它驱动。

2. **MergeScanExec 建立桥接关系**
   - `MergeScanExec` 识别哪些 alive dyn filter 需要传播到哪些 remote subscriber regions/scans。
   - `MergeScanExec` 为这些 dyn filter 生成稳定 `filter_id`。
   - `MergeScanExec` 把 alive dyn filter 注册到 query-scoped registry，并建立 `query_id + filter_id -> subscribers` 的映射。

3. **初始 remote read 完成 register / 建链**
   - `MergeScanExec::to_stream` 发起 remote read。
   - 初始 remote read 负责把“该 query 下会存在这个 remote dyn filter”带到 datanode。
   - datanode 在 scan 构建阶段建立本地注册项，并安装 placeholder / remote wrapper。

4. **registry 持续观察 dyn filter 更新**
   - frontend registry 通过 `wait_update` 或 generation 变化监听 alive dyn filter。
   - 每次有新快照时，registry 生成新 epoch，并构造 `DynFilterUpdate`。

5. **后续 update / unregister 通过 unary RPC 下发**
   - frontend registry 使用 Task 02 定义的 unary RPC 下发 `update / unregister`。
   - datanode 根据 `query_id + filter_id` 找到共享 remote filter state。
   - 若 epoch 更新更大则应用；重复/过期则幂等忽略。

6. **query 结束或无人使用时清理**
   - frontend `is_used()` 可作为“本 query 内已无人使用”的正常注销信号。
   - query finish / cancel 触发 registry 进入 closing，并执行 unregister / cleanup tail。
   - datanode TTL 和 cancel hook 作为兜底回收，不替代显式 unregister。

## 核心设计决策

### 1. ABI 与 payload

Phase 1 采用 Task 01 已定义的最小 ABI：

- `DynFilterUpdate`
  - `protocol_version`
  - `query_id`
  - `filter_id`
  - `epoch`
  - `is_complete`
  - `payload`

- `DynFilterPayload`
  - Phase 1 当前只正式落地 `Datafusion(Vec<u8>)`
  - 最小可交付以 `InListExpr` 为主
  - 不支持的 runtime-only expr（如 `HashTableLookupExpr`）必须静默降级为“不远端发送”

### 2. 控制面通道

Phase 1 复用现有 `greptime.v1.region.Region` unary gRPC：

- `RegionRequest.body.remote_dyn_filter`
- `RemoteDynFilterRequest.oneof action`
  - `update`
  - `unregister`

重要边界：

- 初始 remote read 负责 register / scan 建链
- unary RPC 只负责后续 `update / unregister`

### 3. Identity model

Phase 1 固定：

- 逻辑 identity 为 `query_id + filter_id`
- region / scan 只承担 routing 与注册元数据，不是 filter state identity 的一部分

`filter_id` 只要求在单个 query 内稳定且唯一，不要求跨 query 全局唯一。

当前推荐规则：

- `region_id`
- `producer-local ordinal`
- `canonicalized children fingerprint`

明确不放入 identity：

- `partition`
- datanode transport 信息
- 内存地址 / 临时对象 id

### 4. Frontend registry placement

Frontend query-scoped registry 采用 **方案 1**：

- **实现位置**：`src/query/src/dist_plan/remote_dyn_filter_registry.rs` 或等价邻近模块
- **物理存放**：query-engine runtime map
- **管理方式**：`query_id -> Arc<RemoteDynFilterRegistry>`

这样做的原因：

- registry 是 query execution runtime object，而不是 session metadata
- 它包含 watcher task、cleanup tail、fanout 状态
- 不适合直接塞进 `QueryContext.mutable_session_data`
- 也不应挂在单个 `MergeScanExec` 上，否则 watcher 与 cleanup 责任会分散

### 5. Registry lifecycle

registry 生命周期分三态：

- `Active`
  - 允许注册 subscriber
  - watcher 正常运行
  - 允许发送 update

- `Closing`
  - query finish / cancel 后进入
  - 停止新注册
  - 发送最终 unregister / complete
  - 等待少量 in-flight RPC 收尾

- `Closed`
  - watcher 停止
  - entry 与 subscriber map 清理完成
  - 可从 query-engine runtime map 中移除

Phase 1 允许 registry 比查询主执行略长一点，但只用于善后，不作为长期全局状态。

## 组件职责

### Frontend

#### Join producer

- 产生 alive dyn filter
- 驱动本地 dyn filter 更新

#### MergeScanExec

- 建立 producer -> remote subscriber 的桥接关系
- 生成稳定 `filter_id`
- 注册 alive dyn filter 到 query-scoped registry
- 在 `to_stream()` 时完成初始 register / remote read 建链

#### QueryRemoteDynFilterRegistryManager

- 维护 `query_id -> Arc<RemoteDynFilterRegistry>` map
- 提供：
  - `get_or_init(query_id)`
  - `begin_closing(query_id)`
  - `reap_closed(query_id)`

#### RemoteDynFilterRegistry

- 持有 query 级状态：
  - `query_id`
  - lifecycle state
  - `entries: filter_id -> RegisteredRemoteDynFilter`
  - cleanup tail deadline
- 负责 watcher 启停、fanout、cleanup

#### RegisteredRemoteDynFilter

最小字段建议：

- `filter_id`
- `alive_dyn_filter: Arc<DynamicFilterPhysicalExpr>`
- `last_epoch`
- `last_observed_generation`
- `subscribers`
- `watch_task_handle`

### Datanode

#### Region unary RPC handler

- 接收 `RemoteDynFilterRequest`
- 校验并按 `action` 分发
- 真实 query-scoped state apply 逻辑由 datanode runtime 实现承接

#### Query-scoped remote filter state

- 按 `query_id + filter_id` 建共享 state
- 处理 epoch 幂等与乱序
- 挂接本地 remote wrapper / scan consumer

#### Remote wrapper / apply path

- 将远端 update 转为本地 dyn filter snapshot
- 应用到现有 predicate / scan 更新路径
- 支持 remap / stable children / generation 语义

## 失败与降级语义

所有错误都必须保持“优化失败但查询继续”：

- payload 编码失败 -> 不远端发送，仅本地 dyn filter
- RPC 失败 -> 记录并降级，不中断查询
- update 早到 / 目标缺失 -> 明确为缓冲、丢弃+指标、或前端重试中的一种
- payload 解码失败 / remap 失败 -> 只关闭远端优化
- registry 或 datanode 状态异常 -> 允许失去 pruning 收益，但不能影响正确结果

## 观测与保护

Phase 1 需要至少具备：

- metrics
  - update send/apply/drop
  - stale epoch drop
  - decode fail
  - frontend/datanode registry register/unregister
  - cleanup / complete / TTL
- tracing fields
  - `query_id`
  - `filter_id`
  - `epoch`
  - 必要时的 region / subscriber 元数据
- resource guards
  - payload size budget
  - cardinality threshold
  - 节流 / 去抖

## Alternatives considered

### A. 把 registry 直接挂在 `MergeScanExec`

不选，原因：

- 同一 query 可能存在多个 bridge / exec 实例
- watcher 与 cleanup 会分散
- 不利于 query 结束后的统一善后

### B. 把 registry 直接塞进 `QueryContext.mutable_session_data`

不选，原因：

- 语义不对：registry 是 execution runtime object，不是 session metadata
- 并发状态、watcher task、cleanup tail 不适合放进 session-style rwlock 数据

### C. 直接上长期全局 manager

Phase 1 不选，原因：

- 过重
- 容易提前把 Task 05 的 TTL / 跨 query sweep / 全局观测复杂度引进来
- 目前 query-engine runtime map 已足够满足 query-scoped 生命周期管理

## 分阶段任务映射

- **Task 01**：定义 ABI 与 payload 边界
- **Task 02**：打通 unary RPC 控制面
- **Task 03**：frontend producer / registry / dispatch scheduling
- **Task 04**：datanode apply / remote wrapper / query-scoped state
- **Task 05**：metrics / tracing / cleanup guard / fallback polish
- **Task 06**：端到端验证与回归基线
- **Task 07**：large build-side membership 的后续扩展（Bloom 等）

## Open Questions

1. datanode 对“update 早于 scan register 到达”在 Phase 1 采用哪种固定策略？
2. `children fingerprint` 的 canonicalization 是否需要单独抽成公共 helper？
3. `is_complete` 与最终 unregister 在 frontend 和 datanode 两侧的最佳时序如何定义得更严格？
4. query-engine runtime map 的 sweep/reap 是否在 Phase 1 就需要后台定期任务，还是按 query finish / explicit reap 足够？
5. Task 07 的 Bloom payload 是否与 `MIN_MAX` 组合发送，还是独立演进？

## Rollout 建议

1. 先冻结 RFC 中的 identity、registry placement 和 lifecycle 决议。
2. 按 Task 01 -> 06 顺序落地 Phase 1。
3. 用 Task 06 建立回归基线后，再讨论 batched update、ack、heartbeat、Bloom membership 等 Phase 2/3 演进。
