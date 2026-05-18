# Remote Dyn Filter Implementation Tasks

基于 `/home/discord9/greptimedb_for_build/docs/dyn-filter-propagation-plan-zh.md` 拆分的实现任务文档集合。

统一设计说明见：`remote-dyn-filter-rfc.md`

## 目标

- 将 frontend 产生的 dyn filter 增量传播到 datanode table scan。
- 先用 Phase 1 跑通分布式 `IN` filter 语义，再为后续传输优化与过滤类型扩展留出协议空间。
- 所有任务都以“不影响查询正确性，只影响优化收益”为硬约束。

## 任务清单

1. `remote-dyn-filter-task-01-wire-abi.md`
   - 定义 Dyn Filter 最小 wire contract：identity、payload 边界、版本/epoch 语义与降级规则。
2. `remote-dyn-filter-task-02-region-rpc.md`
   - 在 `RegionRequest.body` 中新增 `remote_dyn_filter` 总入口，并用 `oneof action` 承载 `update / unregister`，打通 frontend -> datanode unary 控制面。
3. `remote-dyn-filter-task-03-frontend-producer.md`
   - 在 frontend 侧识别可分发 dyn filter，完成 filter_id、epoch、目标 region 的生成与下发。
4. `remote-dyn-filter-task-03-minimal-shrink.md`
   - 收缩已经越界的 Task 03：先重写 subtask 06-08 文档，再按最小边界整理 frontend/datanode 相关代码责任。
5. `remote-dyn-filter-task-04-datanode-apply.md`
   - 在 datanode 侧维护 query-scoped filter state，并将 update 应用到 scan predicate。
6. `remote-dyn-filter-task-05-observability-fallback.md`
   - 增加 metrics、tracing、TTL 清理、错误降级与控制面保护。
7. `remote-dyn-filter-task-06-validation.md`
   - 完成功能、一致性、可靠性、性能验证，并为 Phase 2/3 演进保留回归基线。
8. `remote-dyn-filter-task-07-large-build-membership.md`
   - 为 large build-side equal join 的 lookup-like membership 设计远端可传输表示，重点覆盖 `HashTableLookupExpr` 无法直接序列化的问题。

## 建议执行顺序

1. 先完成 ABI 定义，避免 RPC 和状态机实现反复返工。
2. 然后基于 `RegionRequest.body.remote_dyn_filter` 打通 unary RPC 请求链路，让 frontend 和 datanode 拥有最小控制面，并把 dyn filter 操作扩展点统一收敛到 `RemoteDynFilterRequest.oneof action`。
3. 若 Task 03 的实现已经漂出最小边界，先执行 `remote-dyn-filter-task-03-minimal-shrink.md`，再继续推进 Task 03 / Task 04。
4. 接着分别实现 frontend 生产者和 datanode 消费者，完成端到端语义闭环。
5. 最后补齐观测性、清理、降级和测试，确保该优化在失败时能自动失效而不是破坏查询。

## 完整工作流程（Phase 1）

1. **frontend 发现可下推 dyn filter**
   - 在 `MergeScanExec` 所在查询树上，DataFusion 调用 `gather_filters_for_pushdown` 时会把当前可下推的 dyn filters 暴露给下游 scan。
   - 这一刻是 frontend 侧为 remote dyn filter 建立注册关系的最佳时机：记录 `query_id + filter_id` 这一逻辑 filter identity，并维护它当前订阅了哪些 remote region/scan，同时保存对应本地 `DynamicFilterPhysicalExpr` 的引用。
2. **初始 remote read 负责把“会有这个 dyn filter”这件事带到 datanode**
   - 后续 `MergeScanExec::to_stream` 发起 remote read 时，仍然带着同一个 `query_id` 与目标 region/scan 身份。
   - datanode 在接收初始请求并构建 scan 时，为这些即将接收远端更新的 dyn filters 建立注册项，并安装 placeholder / remote wrapper。
3. **frontend 持续观察 dyn filter 更新并转成远端 update**
   - frontend 注册中心通过本地 dyn filter 的更新通知（例如 `wait_update` / generation 变化）感知新快照。
   - 每次有新快照时，frontend 将其编码为 `DynFilterUpdate`，并通过 Task 02 定义的 unary RPC 发送给对应 datanode。
4. **datanode 注册中心接收 update 并刷新本地 dyn filter**
   - datanode 根据 `query_id + filter_id` 找到共享的 remote filter state，并把 update 分发给当前挂接在该 state 上的本地 consumer / wrapper 视图。
   - 若 epoch 更大则应用更新，若重复/过期则按幂等规则安全忽略。
   - scan 在后续 pruning / predicate 计算时读取 wrapper 当前快照，从而渐进增强裁剪收益。
5. **frontend 负责判定“这个 dyn filter 在本查询里是否已无人使用”**
   - 对 frontend 本地持有的 `DynamicFilterPhysicalExpr`，`is_used()` 可作为“该查询内是否仍有 consumer 在使用这个 dyn filter”的判据。
   - 一旦 frontend 确认该 dyn filter 在本查询中已无人使用，就可以把它从 frontend 注册中心注销，并通知相关 datanode 注销对应注册项。
   - datanode 侧也可以把本地 remote wrapper / `DynamicFilterPhysicalExpr` 的 `is_used()` 作为“当前节点上是否还有 scan consumer”的快速判据，用于 eager local cleanup；但它只是正常清理的补充信号，不替代显式 unregister / cancel / TTL。
6. **异常和兜底清理**
   - `is_used()` 触发的是正常路径下的前端注销信号；异常断连、query cancel、frontend 崩溃等场景仍需依赖显式 cancel/complete 和 datanode TTL 兜底。

## 实施前先澄清的契约

- 初始 remote read 请求必须携带与后续 dyn filter update 相同的 `query_id + filter_id` 逻辑 identity；region/scan 只承担注册与路由元数据，不再是 filter state identity 的一部分。
- datanode 需要明确定义“update 早于 scan 注册到达”时的行为，Phase 1 应至少固定为缓冲、显式丢弃加指标、或前端重试中的一种。
- `is_final` 仅表示后续不会再有更强 update，不等于可立即删除最终 filter state；真正回收仍以 query finish / cancel / TTL 为准。
- query-scoped filter state 的生命周期与清理责任以 Task 04 为主，Task 05 只补充观测、预算和兜底机制。

## Phase 对应关系

- Task 01-06 覆盖 Phase 1 的最小可用版本。
- Task 05 和 Task 06 同时为 Phase 2 的 batched update、ack、watermark、heartbeat 做准备。
- Phase 3 的 `MIN_MAX` / `BLOOM` 只需要在 Task 01 预留 ABI 扩展点，再在后续增量任务中落地。
- Task 07 是一个独立的后续任务，面向“大 build side equal join membership”的远端表示设计，更接近 Phase 2/3 的增强项。
