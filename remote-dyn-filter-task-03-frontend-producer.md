# Task 03 - Frontend Dyn Filter 生产与建链

## Goal

在 frontend 执行侧识别 join 产生的 alive dyn filter，生成稳定 `filter_id`，并通过 `MergeScanExec -> query-scoped state -> initial remote read metadata` 建立最小 frontend producer / bridge 闭环。

Task 03 收缩后只负责：

- producer 识别
- stable `filter_id`
- frontend query-scoped state
- 首次 remote read 的 initial-register handoff

持续 update / unregister runtime、datanode apply、successful-path lifecycle cleanup 不再由本任务承诺闭环。

## Touchpoints

- `src/query/src/dist_plan/merge_scan.rs`
- dyn filter 生产者相关执行模块（join / topk 第一阶段至少覆盖 join）
- frontend query 执行生命周期管理模块

## Subtask 01 conclusion - join producer and alive dyn filter access points

- Phase 1 的 producer 关注范围固定为 **distributed join**，不在本任务内扩展到 TopK。
- 从当前测试与接线情况看，alive dyn filter 的真正 owner 在 **DataFusion join dynamic filter path**，而不是 `MergeScanExec` 本身：
  - `src/query/src/datafusion.rs` 中的 `test_join_dynamic_filter_pushdown_reaches_region_scan()` 证明 `HashJoinExec` 在 optimizer 之后会把 dyn filter pushdown 到 region scan。
  - 在该测试里，`PartitionMode::CollectLeft` 下左侧 scan 没有收到 filter，右侧 scan 收到了 dyn filter，说明当前 Phase 1 join coverage 至少已经验证了 probe-side / remote scan 接收路径。
- `MergeScanExec` 的职责不是生产 dyn filter，而是作为 **frontend producer 与 remote subscriber 之间的 bridge**：
  - 它持有 `regions`、`query_ctx`、`plan` 和 distributed scan fanout 信息。
  - `to_stream()` 在发起 remote read 时已经把 `query_context` 与 tracing header 带入 `RegionRequestHeader`，是后续 register / remote read 建链的自然挂点。
- 因此，Task 03 的 frontend 接入点可分为两层：
  - **producer owner**：DataFusion join path 持有 alive dyn filter
  - **bridge layer**：`MergeScanExec` 负责把该 alive dyn filter 与 remote subscriber regions/scans 建立映射，并接到 query-scoped registry
- 当前 subtask 01 只要求识别 owner 与 bridge，不要求在本子任务内完成 registry、watcher 或 RPC fanout 的代码实现。

## Subtask 02 conclusion - stable `filter_id` helper

- 已在 `src/query/src/dist_plan/merge_scan.rs` 中补上 Phase 1 `filter_id` helper：`build_remote_dyn_filter_id(...)`。
- 当前 identity 继续遵循 Phase 1 边界：`region_id + producer-local ordinal + canonicalized children fingerprint`。
- `children fingerprint` 的实现不依赖内存地址，也不直接使用 `Debug` / `Display` 字符串；而是对 child physical expr 做 DataFusion proto 序列化后再生成稳定 hash，降低 alias / 格式化漂移风险。
- `partition` 没有进入 helper 的输入，因此不会被混入 `filter_id` identity；partition 仍应只留在后续 subscriber / fanout 映射中。
- 已补最小单测覆盖：等价 children 稳定、identity 输入变化会改变 `filter_id`、以及 identity 不包含 partition 维度。

## Subtask 03 conclusion - query-scoped registry skeleton

- 已新增 `src/query/src/dist_plan/remote_dyn_filter_registry.rs`，先把 Task 03 需要的 query-scoped registry 模型落成在 `dist_plan` 邻近模块。
- 当前 skeleton 已覆盖 Phase 1 最小结构：
  - `QueryRemoteDynFilterRegistryManager`
  - `RemoteDynFilterRegistry`
  - `RegisteredRemoteDynFilter`
  - `RemoteDynFilterSubscriber`
- `RemoteDynFilterRegistry` 现在显式表达三态生命周期：`Active -> Closing -> Closed`，并提供 `register_remote_dyn_filter(...)`、`register_subscriber(...)`、`begin_closing(...)`、`mark_closed()`、`reap_closed_entries()` 等最小入口，把 register / watcher / cleanup 的职责边界先固定下来。
- `RegisteredRemoteDynFilter` 先保存了 Phase 1 后续子任务会继续使用的核心状态：alive `DynamicFilterPhysicalExpr` 引用、`last_epoch`、`last_observed_generation`、subscriber 列表，以及 watcher 是否已启动的本地标志。
- 当前实现还**没有**把 registry manager 挂到 query-engine runtime map，也**没有**让 `MergeScanExec` 开始真实注册或启动 watcher；这些仍留给 subtask 04/05 继续接线，避免在本子任务里把生命周期接入和 bridge 注册提前揉在一起。
- 已补 focused unit tests，覆盖：同一 `query_id` 复用同一个 registry、filter/subscriber 注册行为、closing 后拒绝新注册、watcher 启动去重，以及 closed registry 被 manager 回收。

## Subtask 04 conclusion - attach registry to query lifecycle

- 已在 `src/query/src/query_engine/state.rs` 上挂载 query-scoped `DynFilterRegistryManager`，把 registry 的 runtime ownership 从 `dist_plan` skeleton 接到 query-engine state。
- `QueryEngineState` 现已提供 subtask 04 约定的最小访问入口：
  - `dyn_filter_registry_manager()`
  - `get_or_init_remote_dyn_filter_registry(...)`
  - `begin_closing_remote_dyn_filter_registry(...)`
  - `reap_closed_remote_dyn_filter_registry(...)`
- 这些 helper 已统一通过 `QueryContext::remote_query_id_value()` 获取 typed `QueryId`，因此 registry placement 现在满足“**query-engine runtime map keyed by query_id**”，而不是挂在单个 `MergeScanExec` 或 `QueryContext.mutable_session_data` 上。
- 当前 subtask 04 的边界是**完成 runtime ownership 与 lifecycle access path**，而不是在本子任务里把 query finish/cancel cleanup hook 全量接入：
  - closing / reap helper 已暴露
  - 真正的 finish/cancel hook 仍留给后续子任务在 distributed query lifecycle 上统一接线
- 已补 focused unit tests，覆盖：
  - 同一个 `QueryContext` 复用同一个 query-scoped registry
  - 不同 `QueryContext` 拿到不同 registry
  - `begin_closing(...)` / `reap_closed(...)` helper 能驱动 registry 生命周期切换与回收
  - `QueryContext` 的 `remote_query_id` contract 与 registry key 绑定正确
- 结论：subtask 04 的 acceptance criteria 已满足；后续子任务只需要在此基础上把 `MergeScanExec` register / watcher / RPC fanout 正式接入。

## Subtask 05 conclusion - register alive dyn filters and subscribers through `MergeScanExec`

- `MergeScanExec` 现在通过 `handle_child_pushdown_result(...)` 捕获 parent pushdown 下来的 alive dyn filter，并保留它们在 `parent_filters` 里的原始 index 作为 `producer_local_ordinal`。
- `MergeScanExec::to_stream` 在每个 remote region 的 read path 上都会把这些 captured filters bridge 到 query-scoped frontend registry：
  - 使用 `build_remote_dyn_filter_id(region_id, producer_local_ordinal, children)` 生成稳定 `filter_id`
  - 调 `register_remote_dyn_filter(...)` 注册 entry
  - 调 `register_subscriber(...)` 注册 subscriber region 映射
- 当前 subscriber metadata 仍保持 Phase 1 最小集：`region_id`。这里不额外发明 scan id；`MergeScanExec` 只负责 register / bridge，而不是后续长跑 update loop。
- 已补 focused tests，覆盖：
  - 只捕获 `DynamicFilterPhysicalExpr`
  - `producer_local_ordinal` 与 parent filter list index 一致
  - 同一 `filter_id` 重复 bridge 时复用已有 registry entry，而不是重复创建
- 结论：subtask 05 的 acceptance criteria 已满足；后续子任务只需要把首次 remote read 的 initial register 一起带到 datanode 并完成 pre-scan build-link。

## Subtask 06 conclusion - send initial register during `MergeScanExec::to_stream`

- `MergeScanExec::to_stream` 可以为每个 remote region 构造 initial-register metadata，并通过 per-region `QueryContext.extensions` 随首次 remote read 带到 datanode。
- 该 metadata 仍采用两层编码：
  - 外层 envelope：JSON
  - 内层 child expr：DataFusion physical expr proto bytes
- datanode 在 Task 03 范围内只需要完成**最小 arrival/handoff**：
  - 能读取 initial-register metadata
  - 能做最小 duplicate / 越界校验处理
  - 能在 failed remote read 时清理本次 region 对应的 arrival state
- initial-register metadata 现已收敛到明确默认边界：
  - 最多 `64` 条 registration
  - 所有 child expr DataFusion proto bytes 总量最多 `64 KiB`
  - 同一 payload 内 `filter_id` 不允许重复
- 边界外 payload 的 Task 03 语义已明确：
  - frontend attach 阶段校验失败时，直接 drop 该 region 的 initial-register metadata 并记录 warning
  - datanode arrival 阶段再次复用同一校验；若 payload 非法则忽略，不把它升级成长期 consumer/runtime 语义
- subtask 06 **不再宣称**以下责任已经由 Task 03 收敛：
  - datanode placeholder state 的长期 ownership
  - successful remote read / stream 正常结束后的 lifecycle cleanup
  - scan-side consumer 已真正 build-link 完成
  - remote wrapper / apply path 已准备就绪
- 已补 focused tests，覆盖：
  - initial-register metadata JSON round-trip
  - child expr proto round-trip
  - region query context 确实携带 per-region initial-register metadata
  - datanode 能从 query context 读取 initial-register metadata
  - failed remote read 能清理本次 region 对应的 arrival state
- 结论：subtask 06 只负责“首次 remote read 传输 initial-register metadata + datanode 最小 arrival/handoff + failed-read cleanup”；真正的 datanode consumer/runtime/lifecycle 继续由 Task 04 接管。

## Subtask 07 rewrite target - compact frontend registry back to minimal state

- subtask 07 重写后只负责把 frontend query-scoped registry 收缩回 **state-first** 的最小边界。
- registry 可以继续持有当前边界真正需要的最小状态：
  - `query_id -> filter_id` query-scoped ownership
  - alive dyn filter 引用
  - subscriber region 映射
  - 少量纯逻辑 helper（若当前边界确实需要）
- registry **不应**再在 subtask 07 内承担：
  - 后台 watcher 主循环
  - cleanup tail
  - transport dispatcher
  - 为未来任务预留但当前无法验证的 lifecycle 抽象
- 若 generation / throttle helper 仍保留，必须是**纯逻辑、可单测、与 transport 解耦**的最小组件，而不是 runtime scheduler 的半成品接口。
- 结论口径应改为：subtask 07 的目标是**收缩 registry 边界**，而不是在 Task 03 中完成完整 watcher/runtime。

## Subtask 08 rewrite target - reduce transport scope to thin frontend plumbing

- subtask 08 重写后只负责 Task 03 需要的最薄 transport 接口面：
  - frontend/query/client 层具备发送 remote dyn filter control request 的最小 plumbing
  - update / unregister 的真正 runtime 闭环仍依赖 Task 04 的 datanode sink 与 lifecycle 边界
- subtask 08 **不应**再把以下内容写成已完成：
  - bridge 主动 `spawn` fanout watcher
  - registry 直接发送 update / unregister 并承担 retry 语义
  - 以“degrade to local-only”为名的无限同代重试
  - 在 datanode sink 仍是 placeholder 时就宣称 fanout 闭环已成立
- failure semantics 必须保持有限、清晰、可验证：失败只能表示“远端优化没有生效”，不能引入无界后台循环或误导性的成功语义。
- 结论口径应改为：subtask 08 只收敛 frontend transport surface，不在 Task 03 内提前实现完整 fanout runtime。

## Subtask 10 follow-up - repair multi-`MergeScanExec` cleanup and `filter_id` producer scope

- 触发原因：二次 review 发现当前修复仍有一个 query-scoped ownership 边界问题：frontend registry 以 `QueryId` 为 key 存放，但 cleanup tracking 仍可能挂在单个 `MergeScanExec` / exec-local stream tracker 上。
- 需要修复的 cleanup 边界：
  - 同一个 query 内存在多个独立构造的 `MergeScanExec` 时，任意一个 exec 的 stream drop 都不能提前 `remove(query_id)` 删除共享 registry。
  - cleanup ownership 必须提升到 query / manager scope，或采用等价的 query-scoped lease/refcount；不能继续依赖单个 `MergeScanExec` 的局部 tracker 判断“最后一个使用者”。
  - 该修复仍属于 Task 03 frontend query-scoped state 的 leak / premature-remove 修复，不应重新引入 watcher loop、fanout scheduler、transport retry 或 datanode apply/runtime lifecycle。
- 需要修复的 `filter_id` identity 边界：
  - 当前 Phase 1 规则 `region_id + producer-local ordinal + canonicalized children fingerprint` 对单个 logical producer scope 是稳定的，但不足以区分同一 query 中多个独立 `MergeScanExec` producer scope。
  - 后续实现必须引入 query 内稳定的 producer scope 维度，使不同独立 `MergeScanExec` 即使拥有相同 `region_id`、producer-local ordinal 与 children fingerprint，也不会生成同一个 `filter_id`。
  - clone / rewrite 出来的同一个 logical `MergeScanExec` 必须保留同一个 producer scope，因此仍应生成相同 `filter_id`。
  - producer scope 不能来自内存地址、runtime object id、partition 或 datanode transport metadata；这些仍不属于 `filter_id` identity。
- 必补验证：
  - 同一 `QueryId` 下两个独立 cleanup lease：drop 第一个不删除 registry，drop 最后一个才删除。
  - 同一 producer scope + 相同 `region_id` / ordinal / children：`filter_id` 稳定相同。
  - 不同 producer scope + 相同 `region_id` / ordinal / children：`filter_id` 必须不同。
  - cloned / replanned `MergeScanExec` 保留 producer scope，`filter_id` 不漂移。
- 结论：subtask 10 是 Task 03 的 review follow-up，用来修正 query-scoped registry 与 producer identity 的边界；它不扩大 Task 03 到 Task 04 的 datanode apply/runtime/lifecycle 范围。

## Dependencies

- Task 01: ABI 已定义。
- Task 02: unary RPC 已能发送 update。

## Scope

### In Scope

1. 在 distributed join 场景中识别简单等值 join key 的 dyn filter 生产点。
2. 为每个可分发 filter 生成稳定 `filter_id`，优先通过 frontend-side helper 基于 MergeScan / distributed plan 中稳定可见的信息生成。
3. 在 `MergeScanExec` 侧把 alive dyn filter 注册到 frontend query-scoped registry，并建立 `query_id + filter_id -> {remote subscriber regions/scans}` 的映射关系。
4. 在 `MergeScanExec::to_stream` 发起首次 remote read 时，为每个 remote region 携带 per-region initial-register metadata。
5. 在 Task 03 范围内，只要求 datanode 能完成 initial-register metadata 的最小 arrival/handoff，而不要求正式 apply/runtime 闭环。
6. 保留 frontend query-scoped registry 的最小 state 形态；它不应在本任务内继续长成 watcher/runtime/transport 组合体。
7. 对不可序列化、超边界或不支持远端传播的 filter 直接降级为本地 dyn filter，不影响 query correctness。

### Out of Scope

- datanode apply 侧状态写入与消费逻辑
- datanode successful-path cleanup / query finish-cancel lifecycle
- 后台 watcher 主循环、fanout runtime、cleanup tail、retry/backoff 策略
- registry 直接承担 transport dispatcher 或完整 control-plane 调度
- 基于 `is_used()` 的完整 unregister runtime 闭环
- join/projection/alias remap 的完整远端 consumer 语义
- dedicated control-plane transport beyond Task 02 unary RPC
- bloom / other non-IN payload encodings in Phase 1
- 全量 observability/fallback 策略收尾

## Protocol / Design Decisions

- frontend 负责识别可远端下发的 dyn filter producer。
- join 是 dyn filter producer；`MergeScanExec` 是 producer 与 remote subscriber 之间的注册/建链桥接层，而不是后台调度层。
- query-scoped registry 采用 **方案 1**：实现放在 `src/query/src/dist_plan/remote_dyn_filter_registry.rs`（或等价 `dist_plan` 邻近模块），实例物理存放在 query-engine runtime map 中，并以 `query_id` 为 key 管理；它是 query-scoped state，但不直接塞进 `QueryContext.mutable_session_data`。
- Task 03 中的 query-scoped registry 应保持 **state-first**：持有最小 query/filter/subscriber state，但不在本任务内承担完整 watcher/runtime/transport 责任。
- `filter_id` 必须由 frontend 基于稳定 plan/runtime metadata 生成，不能依赖内存地址。
- Phase 1 中 `filter_id` 采用 query 内稳定、局部唯一的规则：`region_id + producer-local ordinal + canonicalized children fingerprint`。
- `partition` 不进入 `filter_id`。如果同一 remote subscriber 下多个 partition 可共享同一个 dyn filter 状态，`partition` 只应留在 subscriber / fanout 映射中，而不应拆分 identity。
- `children fingerprint` 必须来自 canonicalized children 表示，而不是直接使用 `Debug` / `Display` 输出字符串，避免 alias、格式化或 rewrite 导致漂移。
- `MergeScanExec::to_stream` 只负责初始 remote read / register handoff，不在本任务内承担后续持续 update / unregister 的主循环。
- initial-register metadata 是 Task 03 到 Task 04 的 handoff，不等于 datanode consumer/runtime 已闭环。
- Task 03 中任何失败都只能导致“远端优化未生效”，不能引入误导性的 success 语义或无界后台行为。
- 不可序列化或超阈值 filter 必须降级为本地 dyn filter，而不是升级为查询失败。

### Decision Note - `filter_id` identity boundary

- `filter_id` 在 Phase 1 中只要求 **query 内稳定且唯一**，不追求跨 query 或跨实现版本的全局稳定。
- `filter_id` 用于标识“同一个远端共享 dyn filter 状态”，而不是表达完整 fanout 路径。
- 因此：
  - 放入 identity：`region_id`、producer-local ordinal、canonicalized children fingerprint
  - 不放入 identity：`partition`、datanode transport 信息、内存地址、runtime 临时对象 id
- 如果未来确认远端状态需要跨 region 共享，再重新收敛 identity；Phase 1 先优先保证稳定、可实现、低碰撞。

### Decision Note - Task 03 / Task 04 boundary

- Task 03 保留 frontend producer / bridge / initial handoff。
- Task 04 接管 datanode consumer/runtime、apply path、successful-path cleanup 与后续 lifecycle。
- 若某个实现只有在 datanode sink、remote wrapper 或 query cleanup 已存在时才成立，则它不应继续留在 Task 03 作为“半成品 placeholder”。

## Execution Order

1. 在 plan/build 阶段识别 join 产生的可远端下发 alive dyn filter，并确定其对应的 remote subscriber regions；producer owner 在 DataFusion join path，`MergeScanExec` 只承担 bridge 职责。
2. 在 `MergeScanExec` 侧为这些 alive dyn filter 生成稳定 `filter_id`，并把它们注册到 frontend query-scoped state。
3. 在 `MergeScanExec::to_stream` 发起首次 remote read 时，携带 per-region initial-register metadata，把“该 query 下存在这个 remote dyn filter”这件事带到 datanode。
4. 让 datanode 在 Task 03 范围内只完成最小 arrival/handoff，而不提前宣称 apply/runtime/lifecycle 已闭环。
5. 对 frontend registry、bridge、transport surface 做最小化收缩：删除越界 watcher/runtime/fanout/lifecycle 责任。
6. 对 unsupported / oversized / runtime-only expr 维持 local-only degradation，不影响 query correctness。

## Validation

- `filter_id` identity 继续稳定，且不把 `partition` 等 fanout 维度混进 identity。
- frontend 能在首次 remote read 中稳定携带 per-region initial-register metadata。
- datanode 能在 Task 03 范围内读取 initial-register metadata，并完成最小 arrival/handoff 行为。
- frontend query-scoped registry 保持 state-first，而不是继续承担 watcher/runtime/transport 组合责任。
- 不可下发的 filter 会自动降级，不影响本地执行路径。
- Task 03 文档不再把 datanode apply、successful-path cleanup、完整 fanout runtime 描述成已在本任务中收敛。

## Risks / Caveats

- `filter_id` 不能依赖不稳定的内存地址或临时物理表达式指针；若不同逻辑 filter 可能碰撞，修正 `filter_id` 生成规则，而不是把 region 再塞回 identity key。
- 不要把 `partition` 混入 `filter_id`；如果 partition 只是执行态 fanout 维度，把它放进 identity 会错误拆分本应共享的远端状态。
- 目标 region 绑定必须来自分布式 plan 的确定信息，不能靠运行时模糊匹配。
- 不要把无法序列化的 runtime-only expr 升级成查询错误；Phase 1 的默认策略是静默放弃远端传播并保留本地正确性。
- 不要在 datanode sink 仍未成型时，把 frontend 侧 transport/plumbing 包装成“完整 fanout 闭环已成立”。
- 如果某段实现只有在 Task 04 的 datanode runtime/lifecycle 存在时才有意义，应优先移动或删除，而不是继续以 placeholder 形式留在 Task 03。
