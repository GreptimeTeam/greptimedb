# Task 04 - Datanode Filter State 管理与 Scan 应用

## 任务目标

在 datanode 侧维护 query-scoped dyn filter state，按 `query_id + filter_id` 做幂等更新，并把远端 update 安全应用到现有 scan predicate 更新路径；region/scan 只承担本地 subscriber registration 元数据。

Task 04 必须把“remote dyn filter consumer 侧的 remap 能力”视为正式实现要求，而不是附带优化：datanode 侧需要一个类似 `RemoteDynamicFilterPhysicalExpr` 的 runtime wrapper，由它承接来自 frontend 的 update，并向本地物理执行计划暴露 `current()` / `update()` / `mark_complete()` 风格的动态 expr 语义，同时支持 `with_new_children` / child remap，保证同一个远端 filter 能在不同 scan / projection / file reader 视图下被安全复用。

## 主要落点

- `src/datanode/src/region_server.rs`
- `src/table/src/predicate.rs`
- `src/table/src/table/scan.rs`
- `src/mito2/src/read/scan_region.rs`
- 可能新增 query-scoped runtime state 模块

## 前置依赖

- Task 01: ABI 与 payload / remote-wrapper 语义稳定。
- Task 02: datanode 已能接收 RPC。

## 实现范围

1. 建立两层注册结构：
   - 共享 filter state：键为 `query_id + filter_id`
   - 本地 subscriber 注册：记录哪些 region/scan consumer 正在订阅这个共享 filter state
   - 该注册结构应优先在初始 remote read / scan 构建阶段建立，使后续 unary update 只需要查共享 state 并更新现有 wrapper。
2. 校验 epoch 语义：更大则应用、相同则幂等、过小则丢弃。
3. 将 `IN` wire payload 解码为本地 predicate 可识别的 dyn filter expr snapshot，并挂入 remote dynamic wrapper。
4. 将更新接入现有 `predicate` / `scan_region` 路径，而不是重写扫描裁剪机制。
5. 明确定义“update 先于 scan 注册到达”时的策略，Phase 1 至少固定一种行为：缓冲等待、显式丢弃并打指标、或要求前端按可重试错误重发。
6. 处理异常路径：query 已结束、scan 不存在、`query_id` 缺失或关联键不完整、payload 解码失败、wrapper remap 失败、类型不兼容。
7. 在 query 正常结束、取消或 TTL 超时后回收状态，并保留 complete 后的最终 filter state 直到确实没有本地 consumer 使用它。
   - 若 frontend 明确发来“该 dyn filter 已无人使用”的注销信号，datanode 也应及时移除对应注册项。
   - 若 datanode 本地 remote wrapper / `DynamicFilterPhysicalExpr` 已通过 `is_used()` 类语义确认没有 scan consumer，也可作为 eager local cleanup 信号。
8. 确保 remote wrapper 固定原始 children 集合，并在不同 consumer 调用 `with_new_children` 时生成各自 remapped 视图，而不是共享一个会被后续 remap 覆盖的单一实例。
9. 在 datanode 将接收到的逻辑计划转成物理计划时，为预期接收远端 dyn filter 的 scan 安装 placeholder / remote wrapper，使后续 update 到达时能够挂接到正确的 consumer 上。
   - **这一步是 subtask 06 之后的正式落点**：subtask 06 只保证 initial-register metadata 到达 datanode 并落入 placeholder state，**不**提前声明 scan-side consumer 已完成 build-link。
10. 接管 placeholder registry 的 successful-path lifecycle：remote read 正常完成、stream drop、query cancel、query finish、TTL 超时都必须能回收 query-scoped placeholder / wrapper state，避免 datanode 常驻泄漏。
11. 为 remote wrapper 保持 generation / snapshot version 语义，使 scan 侧能判断 update 是否改变了当前过滤快照。
12. 保证 remote wrapper 暴露的 dyn filter 在 update 前后维持稳定的 boolean 返回类型与 nullability 契约；不兼容 update 只能安全降级。

## 推荐子步骤

1. 先复用现有本地 dyn filter 容器能力，只在最外层补 query-scoped 状态管理和远端写入口。
   - 把“初始 remote read 注册 scan / 后续 unary RPC 更新已有注册项”固定为首选契约，避免在首次 update 时才临时创建 consumer。
2. 把 epoch 校验与状态更新封装成纯逻辑单元，便于单测覆盖乱序和重复更新。
3. 先实现 remote wrapper：固定 children、支持 `current()`、`update()`、`mark_complete()`，并验证 remap 后的 expr 仍与本地 schema 一致。
4. 为 scan 提供“收到远端 update 后刷新 predicate 快照”的清晰边界，避免并发下直接改底层状态导致竞态。
5. 让 `is_complete` 只承担“不会再有后续 update”的语义，不在收到 complete marker 时立即回收最终 filter state。
6. 将 query finish / cancel / TTL 作为兜底状态回收入口，并把 registry 生命周期接口固定下来；同时补上 successful remote read / stream 正常结束 / stream drop 的 cleanup，避免 subtask 06 留下的 placeholder state 长驻。
7. 在 datanode physical-plan optimizer / scan build hook 上完成真正的 consumer 安装，而不是依赖 `table_provider(...)` 一类过早的弱信号。
8. 为 remote wrapper 增加 generation/version 与类型不变式检查，确保 remap 或后续 update 不会把 bool filter 变成不兼容 expr。

## 验收标准

- datanode 能接收并应用远端 `IN` filter 更新，且实际扫描量下降。
- 乱序和重复 update 不会破坏状态机。
- scan 可以在没有 dyn filter 时先启动，update 到达后继续渐进增强 pruning，不会因为等待控制面而阻塞执行。
- “update 早到”路径有明确且可测试的行为定义。
- frontend 发来的 unregister / end-of-use 信号能让 datanode 及时移除对应 subscriber；当共享 filter state 已无本地 consumer 时，datanode 也能借助本地 `is_used()` 类语义主动回收，而 query cancel / TTL 仍可作为兜底。
- remote wrapper 能在多个 scan consumer 上正确 remap children，不会因为 projection / schema 变化把 filter 应用到错误列索引。
- remote wrapper 能稳定暴露 generation/version 变化，并保持 boolean / nullability 契约不被后续 update 破坏。
- 找不到目标 scan 或解码失败时只降级，不影响查询正确性。
- query 结束后不会遗留 filter state。

## 风险与注意点

- 不要绕开已有 predicate/scan 更新能力重新造一套 scan 侧状态系统。
- 状态表生命周期必须和 query 生命周期绑定，否则会出现内存泄漏。
- 不要把 remote wrapper 做成“单实例共享 remapped children”的可变对象；不同 consumer 必须拥有各自 remapped 视图，否则极易出现“更新成功但裁剪错误”的隐蔽问题。
