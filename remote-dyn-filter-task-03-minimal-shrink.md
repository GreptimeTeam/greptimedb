# Task 03A - Frontend Dyn Filter 最小化收缩与 subtask 06-08 重写

## Goal

把当前已经失控的 Task 03 收缩回“**frontend producer / bridge 最小闭环**”边界：

- 先重写 `remote-dyn-filter-task-03-frontend-producer.md` 中 **subtask 06 / 07 / 08** 的目标、结论与验收口径；
- 再按新的最小边界整理代码；
- 明确哪些责任必须留在 Task 03，哪些应移回 Task 04 / Task 05。

这个任务的核心不是“继续堆实现”，而是**先纠偏，再收口，再继续**。

## Why this task exists

- subtask 06 已经把 datanode placeholder arrival/state 提前拉进了 Task 03，但真正的 datanode apply / remote wrapper / successful-path lifecycle 仍属于 Task 04。
- 当前未提交的 subtask 07-08 又进一步把 `bridge + registry + watcher + fanout + transport + failure semantics` 混在一起，导致 Task 03 从“bridge 层”滑成“半成品 runtime/control-plane”。
- 如果不先收缩边界，后续 Task 04/05 只会建立在一个更难收拾的前提上。

## Touchpoints

- `remote-dyn-filter-task-03-frontend-producer.md`
- `remote-dyn-filter-task-04-datanode-apply.md`
- `src/query/src/dist_plan/merge_scan.rs`
- `src/query/src/dist_plan/dyn_filter_bridge.rs`
- `src/query/src/dist_plan/remote_dyn_filter_registry.rs`
- `src/query/src/query_engine/state.rs`
- `src/query/src/region_query.rs`
- `src/frontend/src/instance/region_query.rs`
- `src/client/src/region.rs`
- `src/datanode/src/region_server.rs`
- `src/datanode/src/region_server/registrations.rs`
- `src/common/query/src/request.rs`
- `src/common/query/src/request/initial_remote_dyn_filter_reg.rs`

## Scope

### In Scope

1. 重写 Task 03 的 subtask 06-08 文档边界与验收标准。
2. 将 Task 03 收缩为“frontend producer / bridge 最小闭环”：
   - 识别 distributed join producer
   - 生成稳定 `filter_id`
   - 在 frontend 侧维护最小 query-scoped registry state
   - 在首次 remote read 中携带 per-region initial-register metadata
3. 清理或下沉当前已经越界的实现：
   - bridge 启动 watcher
   - registry 直接做 RPC fanout
   - registry 直接承担 transport/payload/runtime lifecycle
   - datanode placeholder state 的过早 ownership
4. 把需要继续存在的 helper 收缩为最小、被当前边界真正需要的形态。
5. 为后续 Task 04/05 留出清晰 handoff，而不是继续在 Task 03 内偷长功能。

### Out of Scope

- 在这个任务里补全 datanode apply / remote wrapper / physical-plan hook
- 在这个任务里补全 query finish/cancel cleanup tail
- 在这个任务里落地真正的后台 watcher / throttle / retry / backoff runtime
- 在这个任务里推进 `HashTableLookupExpr` / large membership / bloom 等后续能力
- 为了“以后可能会用到”继续增加 speculative lifecycle 或 transport 抽象

## Minimal Boundary After Shrink

Task 03 收缩后只保留以下责任：

1. **producer 识别**
   - frontend 只在 distributed join 场景识别可传播的 alive dyn filter。

2. **identity 生成**
   - `filter_id` 继续由 frontend 基于稳定 metadata 生成；
   - 不依赖内存地址；
   - 不把 `partition`、transport 信息、runtime 临时对象 id 混进 identity。

3. **frontend bridge / state**
   - `MergeScanExec` 只负责 capture、register、attach initial metadata；
   - query-scoped registry 只保留当前边界真正需要的 state；
   - registry 不再直接长成 runtime scheduler / transport dispatcher。

4. **initial register handoff**
   - initial-register metadata 仍可随首次 remote read 带到 datanode；
   - 但 Task 03 不再宣称自己拥有 datanode successful-path lifecycle 或 apply/runtime state。

5. **optimization-only safety**
   - 任何失败都只能导致“远端优化未生效”，不能影响 query correctness；
   - 不能用“degrade to local-only”的名义偷偷引入无界重试或后台循环。

## Subtask Rewrite Targets

### Rewrite subtask 06

重写后的 subtask 06 只负责：

- 定义并发送 per-region initial-register metadata；
- 允许 datanode 解码并保存**最小 arrival 信息**，仅作为 Task 04 的 handoff；
- 为 metadata 增加明确边界：count / bytes / duplicate 处理口径；
- 当前默认边界已落地为：最多 `64` 条 registration、最多 `64 KiB` child-expr proto bytes、同一 payload 内 `filter_id` 不可重复；
- frontend attach 与 datanode arrival 复用同一校验，越界 payload 统一按 drop-and-warn 处理；
- 明确写清：
  - successful-path cleanup **不是** subtask 06 责任；
  - placeholder / wrapper 的正式 lifecycle **不是** Task 03 责任。

不应再把以下内容写成 subtask 06 已完成：

- datanode placeholder registry 的长期 ownership
- query finish/cancel/TTL cleanup 设计已定
- scan-side consumer 已经 build-link 完成

### Rewrite subtask 07

重写后的 subtask 07 只负责：

- 收缩 frontend registry 到 state-first、可测试的最小形态；
- 如果 generation / throttle helper 仍需要存在，必须保持为**纯逻辑 helper**；
- 不再让 registry 自己承担 watcher runtime、cleanup tail、transport fanout。

不应再把以下内容写成 subtask 07 范围：

- query 运行期后台 watcher 主循环
- `Closing -> Closed` 的完整 lifecycle 语义
- query finish/cancel 后的 cleanup tail
- 依赖远端 sink 已存在的 fanout 行为

### Rewrite subtask 08

重写后的 subtask 08 只负责：

- 收敛 Task 03 需要的最薄 transport 接口面；
- 明确 update / unregister 真正落地前提：Task 04 必须已有可消费的 datanode sink 与生命周期边界；
- 把 failure semantics 定义成**有限、可验证、不会误导的行为**。

不应再把以下内容写成 subtask 08 已完成：

- bridge 主动 `spawn` fanout watcher
- registry 直接发送 update / unregister 并承担 retry 语义
- 以“degrade to local-only”为名的无限同代重试
- 在 datanode sink 仍是 placeholder 时就宣称 fanout 闭环成立

## Code Cleanup Principles

1. **先删职责，再谈重构**
   - 优先删除越界责任，而不是继续包装它。

2. **registry 保持 state-first**
   - registry 可以有最小 state 与纯逻辑 helper；
   - 不应同时拥有 runtime task management、transport、cleanup tail。

3. **bridge 保持 bridge**
   - `MergeScanExec` / bridge 负责 register 与 initial handoff；
   - 不负责后台调度。

4. **Task 04 拥有 datanode consumer/runtime**
   - 任何涉及 remote wrapper、apply path、successful-path lifecycle 的实现，都应回到 Task 04 文档和代码中定义。

5. **删掉当前边界无法被真实验证的抽象**
   - 如果一个接口目前只在测试里自洽、在生产路径里并不成立，应优先删掉或下沉，而不是保留“以后可能会用”。

## Suggested Execution Order

1. 先重写 `remote-dyn-filter-task-03-frontend-producer.md` 中 subtask 06-08 的目标、范围、验收标准与结论口径。
2. 再对照新边界整理代码：
   - 删除越界 watcher / fanout / lifecycle
   - 收缩 registry / bridge / state ownership
   - 把 datanode 侧提前落入 Task 03 的责任退回 Task 04 handoff
3. 补齐新的 focused tests，只验证收缩后仍成立的最小边界。
4. 最后再决定：哪些能力进入 Task 04，哪些推迟到 Task 05。

## Deliverables

- 更新后的 `remote-dyn-filter-task-03-frontend-producer.md`（重点是 subtask 06-08）
- 必要时同步调整 `.tmp/tasks/task-03-frontend-producer/subtask_06.json`
- 必要时同步调整 `.tmp/tasks/task-03-frontend-producer/subtask_07.json`
- 必要时同步调整 `.tmp/tasks/task-03-frontend-producer/subtask_08.json`
- 收缩后的 frontend-side code changes
- 与 Task 04 对齐后的责任边界说明

## Validation

- Task 03 文档不再把 watcher/runtime/fanout/lifecycle 混成一个任务。
- `MergeScanExec` / bridge 不再直接启动后台 fanout loop。
- frontend registry 不再直接承担 transport dispatcher 责任。
- datanode 侧在 Task 03 范围内不再宣称自己已拥有正式 apply/runtime lifecycle。
- 保留下来的 helper 都能被当前边界真实验证，而不是依赖未来任务兜底。
- 所有失败路径继续满足“只影响优化收益，不影响 query correctness”。

## Risks / Notes

- 这个任务会让 Task 03 暂时“能力变少”，但这是预期结果；目标是先恢复边界一致性。
- 如果发现某段代码只有在 Task 04 存在时才有意义，应优先移动或删除，而不是继续在 Task 03 保留 placeholder。
- 文档重写必须先于代码整理，否则很容易一边删代码、一边沿用旧边界，导致第二轮返工。
