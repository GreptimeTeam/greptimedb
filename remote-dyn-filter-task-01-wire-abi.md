# Task 01 - Dyn Filter Wire ABI 与序列化边界

## 任务目标

定义一个可版本化、可幂等重放、可降级的 `DynFilterUpdate` 最小 wire contract，让 frontend 能把运行时 dyn filter 状态投影为 datanode 可解释的消息体；Task 01 只固定“传什么”和“什么情况下允许传”，不把 transport、producer 调度或 datanode runtime 混进同一任务。

## 主要落点

- `src/common/query/src/request.rs`
- 可能新增 `src/common/query/src/dyn_filter.rs` 或相近模块
- 可能涉及 protobuf / serde / flight request 相关公共定义

## 前置依赖

- 无，作为后续 RPC 和状态机实现的基础任务优先完成。

## 当前实现状态（截至本轮代码修改）

按当前已经收紧后的任务边界，**Task 01 在其自身范围内已经完成**。当前已落地的内容可归为三块：

- `query_id` / `remote_query_id` 的 Phase 1 contract 与传播基础设施
- `DynFilterUpdate` ABI 的核心共享类型骨架
- `DynFilterPayload::Datafusion(Vec<u8>)` 的共享编码边界与基础 helper

### 已完成

1. `remote_query_id` 作为 Phase 1 query-scoped execution id 的载体已经落地到 `QueryContext.extensions["remote_query_id"]`。
2. fresh local `QueryContext` 创建路径现在都会自动补齐 `remote_query_id`。
3. `QueryContext <-> api::v1::QueryContext` 现有序列化链路已经可以携带该 extension，无需 proto schema 改动。
4. frontend 正常 distributed read 主路径会把 `query_context` 带入 `RegionRequestHeader`，因此 datanode 能恢复 `remote_query_id`。
5. `src/common/query/src/request.rs` 中已经新增 `DynFilterUpdate`、`DynFilterPayload` 等 Task 01 ABI 核心类型。
6. ABI 当前采用：
   - `#[non_exhaustive] enum DynFilterPayload { Datafusion(Vec<u8>) }`
   作为 Phase 1 当前唯一 payload 变体，不再保留冗余的 `filter_kind` 字段。
7. `is_complete` 命名已经替代 `is_final`，与 DataFusion `mark_complete()` 语义对齐。
8. `DynFilterPayload::Datafusion(Vec<u8>)` 的共享 encode/decode helper 已经落地到 `common-query`：
   - 能把 `Arc<dyn PhysicalExpr>` 编码成 payload bytes
   - 能在给定 `TaskContext + Schema` 的前提下解码回 `Arc<dyn PhysicalExpr>`
   - 会拒绝 `HashTableLookupExpr` 这类不应进入 Phase 1 payload 的 runtime-only expr
   - 会在解码后校验 `Column(name, index)` 与输入 schema 的一致性
   - 会在 encode / decode 两侧都执行 payload bytes budget 检查

### 不再计入 Task 01 的后续实现

下面这些工作依赖 Task 01 已定义好的 contract，但**不再属于 Task 01 自身的未完成项**：

1. `filter_id` 的 concrete frontend helper 落地
   - Task 01 只定义它必须在单个 `query_id` 内稳定且可重放；实际生成逻辑归 Task 03。
2. `epoch` / `is_complete` 的 producer 递增与 consumer 执行
   - Task 01 只定义 contract-level 语义；真实发送与消费分别归 Task 03 / Task 04。
3. “可序列化 / 需静默降级”的 frontend 判定逻辑
   - Task 01 只定义 Phase 1 支持边界；例如 `HashTableLookupExpr` 必须静默降级为“不远端发送”，具体判定逻辑归 Task 03。

以下能力依赖 Task 01 已定义好的 ABI，但其真实实现归属后续任务：

- **Task 02**：`DynFilterUpdate` 的真实发送/接收控制面链路
- **Task 04**：datanode 侧 remote dynamic wrapper / update apply / remap runtime

因此，当前代码状态更准确地说是：

- **Task 01 / query-id groundwork：已完成**
- **Task 01 / DynFilterUpdate ABI 核心类型骨架：已完成**
- **Task 01 / DynFilterUpdate 共享消息 schema、编码边界与基础 encode/decode helper：已完成**
- **Task 02 / DynFilterUpdate 真实发送链路：未开始或未实现**
- **Task 04 / datanode consumer runtime：未开始或未实现**

## 实现范围

1. 定义 `DynFilterUpdate` 核心字段:
   - `protocol_version`
   - `query_id`（建议实现为 dedicated query-scoped execution id，而不是复用现有 `process_id`）
   - `filter_id`
   - `epoch`
   - `is_complete`（语义上对应 DataFusion `mark_complete()`；表示后续不会再有新 update）
   - `payload`
2. 第一阶段只支持 `IN` payload，但 ABI 必须保留 `MIN_MAX` / `BLOOM` 扩展位。
3. 对于 `DynFilterPayload::Datafusion(Vec<u8>)`，列引用由 expr tree 内部的 `Column` 节点承担，而不是额外的 top-level binding 字段。
4. 明确可序列化判定逻辑：仅底表列上的简单等值 join dyn filter 可下发。
5. 明确降级规则：无法投影为 ABI 的 filter 仅本地使用，不进入远端传播路径。

## Phase 1 默认落地的精确定义

为了让实现可以直接开始，Phase 1 先把以下设计钉死，不再保持开放状态。

### 1. `DynFilterUpdate` 的 wire shape

Phase 1 先使用“结构化字段 + payload enum”的方案：

- 路由、生命周期、关联字段继续放在 `DynFilterUpdate` 外层。
- 具体过滤内容放进 `payload`。
- Phase 1 的 payload 先只承载 DataFusion physical expr protobuf bytes，但类型本身保持为可扩展 enum。

推荐形式：

```rust
#[non_exhaustive]
enum DynFilterPayload {
    Datafusion(Vec<u8>), // DataFusion physical expr protobuf bytes
}
```

建议最小结构：

- `protocol_version: u32`
- `query_id: String`（从 `QueryContext.extensions["remote_query_id"]` 读取）
- `filter_id: String`
- `epoch: u64`
- `is_complete: bool`
- `payload: DynFilterPayload`

其中 Phase 1 的约束是：

- `DynFilterPayload::Datafusion(Vec<u8>)`
  - 只用于 **simple/snapshotted expr subset**
  - 当前最小交付以 `InListExpr` 为主
  - `MIN_MAX BinaryExpr` 与少量简单组合 expr 只代表后续可扩展编码空间，不视为本任务的端到端承诺

receiver 侧兼容性规则也在 Task 01 固定：

- 若 `protocol_version` 不受支持，或 payload 变体对当前节点未知，则该 update 必须被安全拒绝/丢弃并触发优化降级
- 该类拒绝只能让 remote dyn filter 失效，不能影响查询结果正确性

对 `DynFilterPayload::Datafusion(Vec<u8>)` 来说，列信息的 authoritative source 是 payload 内部的 DataFusion expr tree：

- `Column` 节点本身携带 `name + index`
- `InListExpr` / `BinaryExpr` / `CaseExpr` 等会递归携带其子 expr
- 多列引用（例如后续可能出现的 `struct(...)` key）也应该由 expr tree 本身表达

因此，Phase 1 先不在 `DynFilterUpdate` 顶层重复建模 `column_binding`，避免与 expr tree 内部列引用形成双重状态源。

### 1.0 Task 01 与 Task 02 的边界

为了避免和 Task 02 混淆，Task 01 在这里固定边界如下：

- **Task 01 负责**
  - `DynFilterUpdate` 长什么样
  - `DynFilterPayload` 如何表达与如何编码
  - `query_id` / `filter_id` / `epoch` / `is_complete` 的 contract-level 语义
  - 哪些上游 expr 形态允许进入 payload，哪些必须降级为“不远端发送”
- **Task 02 负责**
  - 通过什么 unary RPC 把 `DynFilterUpdate` 发出去
  - frontend/client 怎么调用该 RPC
  - datanode `region_server` 怎么接收、返回错误、做鉴权/路由

也就是说：

- Task 01 更像“消息体和消息语义本身”
- Task 02 更像“承载这条消息体的控制面通道”

如果没有 Task 01，Task 02 不知道该发什么；如果没有 Task 02，Task 01 定义好的消息也没有真实控制面可走。

与 Task 04 的边界也需要同时明确：

- Task 01 只定义 consumer 必须能依赖的消息语义，例如：幂等 epoch 规则、payload 必须能解码为稳定 expr subset、以及不支持 payload 时必须安全降级。
- Task 04 负责把这些约束真正落到 datanode runtime：包括 RemoteDynamicFilter wrapper、child remap、scan/apply 路径和状态生命周期。

结论：

- Phase 1 可以优先复用 `DynFilterPayload::Datafusion(Vec<u8>)` 来承载简单可序列化的 expr snapshot。
- 但不能把它理解成“arbitrary DataFusion physical expr 都能安全远端传输”。
- `HashTableLookupExpr` 等 runtime-only expr 仍然不能指望该分支解决。
- Phase 1 先不引入 `Custom`，避免在 wire 尚未真正稳定前过早扩展 payload 面。

### 1.1 `DynFilterPayload::Datafusion` 的边界

这个分支的主要价值，是最大程度复用 DataFusion 已有的 physical expr protobuf 编解码逻辑，减少我们自己维护 expr AST wire format 的成本。

但 Phase 1 必须明确限制：

1. 它传输的是 snapshot 后的 physical expr bytes，不是远端可持续 update 的动态 wrapper。
2. `HashTableLookupExpr` 在 DataFusion proto 路径上会退化掉，因此不能被视作有效 payload。
3. `DynFilterPayload::Datafusion(Vec<u8>)` 在当前最小交付里主要承载小 build side `InListExpr`；`MIN_MAX` 或简单组合 expr 只作为后续扩展时可复用的编码空间，不是本任务的端到端承诺。
4. payload 内部若引用多个 `Column`，这是 expr tree 自身的语义，不应被错误压扁成单列 binding。
5. 对 `HashTableLookupExpr` 等运行时专属 expr，Phase 1 采用静默降级：不远端发送，而不是把它当成查询级错误。

### 1.2 为什么 Phase 1 先不引入 `Custom`

在 wire 还没有真正投入跨节点兼容场景之前，先只保留 `Datafusion` 变体更简单。

但需要明确兼容性边界：

- **当前阶段**：`Custom` 等新变体可以先不实现，因为还没有稳定的跨版本/跨节点协议承诺。
- **未来阶段**：一旦真实 RPC/wire 稳定并开始跨版本传输，再新增 `Custom` 或 Bloom 一类 payload，就应视为协议扩展事件。
- 这意味着后续若新增 `Custom`，需要配合：
  - `protocol_version` 演进；或
  - 显式 capability / feature gating；或
  - mixed-version 节点兼容策略。

### 2. `filter_id` 的生成与唯一性范围

Phase 1 先规定：

- `filter_id` 由 frontend 生成。
- 唯一性范围是 **单个 `query_id` 内唯一**，不要求跨 query 全局唯一。
- 同一个 dyn filter 在整个查询生命周期内复用同一个 `filter_id`。
- 重试、重复发送、final update 都必须沿用同一个 `filter_id`。

推荐生成规则：

- 使用 plan/build 阶段稳定信息生成字符串 id，例如：
  - `<source_stage_or_plan_node>/<join_side>/<target_region_group>/<join_key_ordinal>`
- 不要使用内存地址、裸指针、临时 expr pointer、随机数作为 `filter_id`。
- Phase 1 优先把它做成 frontend-side helper：输入是 MergeScan / distributed plan 中稳定可见的信息，加上对应 dyn filter expr 的稳定位置，而不是依赖 datanode 物理计划生成后的临时节点身份。

### 3. `filter_id` 是逻辑 filter identity，target 只保留为路由/注册元数据

Phase 1 改为固定以下约束：

- `query_id + filter_id` 唯一标识一条 remote dyn filter 逻辑更新流。
- `target_region_id` / `target_scan_id` 若存在，只承担 transport routing 或 consumer registration 元数据，不再是 filter state identity 的一部分。
- 同一个 datanode 上若有多个 scan/region consumer 订阅同一 remote dyn filter，应共享同一个 query-local filter state，并各自持有 remapped consumer 视图。

因此，Phase 1 的核心关联键正式固定为：

- `query_id + filter_id`

### 4. `epoch` / `is_complete` 的幂等语义

Phase 1 先规定以下规则：

1. `epoch` 由 frontend 对同一个 `query_id + filter_id` 单调递增生成。
2. datanode 处理规则：
   - `incoming_epoch > current_epoch` -> 应用更新并覆盖状态
   - `incoming_epoch == current_epoch` -> 视为幂等重放，允许忽略或视为成功
   - `incoming_epoch < current_epoch` -> 丢弃为过期 update
3. `is_complete = true` 的含义仅是：
   - 不再期待更高 epoch 的后续 update
   - **不等于立即删除 filter state**
4. 若收到 `is_complete = true` 后又收到更高 epoch：
   - Phase 1 视为协议错误或异常重放，记录指标并丢弃
   - 不要尝试重新打开已 complete 的 filter state

### 5. Phase 1 `DynFilterPayload::Datafusion`（承载 `InListExpr`）规范

为了避免两端归一化不一致，Phase 1 先锁定：

1. **NULL 语义**
   - 若 `DynFilterPayload::Datafusion` 承载的是 `InListExpr`，其值集合默认不包含 NULL。
   - 若 build side key 含 NULL，Phase 1 直接忽略这些 NULL，不把它们编码进 remote payload。
2. **类型归一化**
   - `DynFilterPayload::Datafusion` 中承载的 expr 必须在 frontend 侧已经完成类型归一化。
   - datanode 不做“猜测式”隐式转换，而是依赖 expr 自身与本地 schema / wrapper remap 的一致性。
3. **去重**
   - 若底层语义是 `IN`，frontend 发送前先做去重。
   - payload 中 values 不要求保留原始顺序。
4. **顺序**
   - 可选地按稳定编码排序，便于测试和重放；至少不要让语义依赖顺序。
5. **大小上限**
   - 单次 `DynFilterPayload::Datafusion` update 必须受 cardinality / bytes budget 限制。
   - 一旦超过 Phase 1 上限，直接降级为“不远端发送”，而不是发送超大 payload。

这意味着 Phase 1 的 `DynFilterPayload::Datafusion` remote payload 是：

- 非 NULL
- 已归一化
- 已去重
- 有大小上限

的稳定集合，而不是原始 build-side 明细流。

## 关于 `query_id` 字段的现实约束

当前仓库里并没有一个已经稳定打通 frontend -> datanode 远端请求链路的现成 `query_id` 字段，可以直接拿来当 remote dyn filter 的 query-scoped identity。

已确认的相近概念：

1. `QueryContext.process_id`
   - `src/session/src/context.rs` 中确实存在 `process_id: u32`。
   - 它主要用于 frontend process manager / kill query / process list。
2. `api::v1::QueryContext`
   - 当前 protobuf 转换并没有把 `process_id` 编进 `api::v1::QueryContext`。
   - 也就是说它今天不是一个天然能跨 frontend -> datanode 传播的 query-scoped id。
3. `request_id`
   - 现有 `request_id` 更多用于 Flight DoPut 这类请求/响应配对，不等于一次查询生命周期 id。

因此，Task 01 里写的 `query_id` 不应被理解成“代码里已经有这个字段”。当前更合理的方向是：

- 引入一个 dedicated query-scoped execution identifier；
- 不把现有 `process_id` 当作 primary wire identity 复用；
- `process_id` 继续保留给 process-list / kill / connection 语义。

在真正落实现有代码前，文档里的 `query_id` 应当被视为“设计占位符”，不是现成 API 名称。

## 推荐的 `query_id` 方案

推荐把 `query_id` 实现为 frontend 在一次分布式查询执行开始时生成的全局唯一字符串 ID，优先选择：

1. **UUIDv7（首选）**
   - 优点：
     - 多 frontend 下天然全局唯一，不依赖 `server_addr + process_id` 拼接。
     - 时间有序，日志和调试时更容易按查询开始时间排序。
     - Rust 生态成熟，实现风险低。
   - 适合作为 protobuf / RPC header 中的 `string query_id`。
2. **UUIDv4（可接受 fallback）**
   - 若当前依赖或运行环境不便引入 v7，可先用 v4 起步。
   - 缺点是无时间有序性，日志可读性和索引局部性较差。

当前不推荐作为 primary query id 的方案：

- 裸 `u32 process_id`
- `server_addr + process_id` 复合字符串
- DataFusion `task_id`
- Flight `request_id`

原因分别是：

- `process_id` 语义偏 connection / process 管理，且今天并未统一跨协议、跨节点传播。
- `server_addr + process_id` 更适合 kill/process-list 显示，不适合作为长期内部协议主键。
- `task_id` 与 `request_id` 都不是 query-scoped distributed execution identity。

## `query_id` 的生成与传播建议

推荐的实现路径：

1. **生成时机**
   - 在 frontend 侧、一次真实查询执行开始时生成一次。
   - 同一 distributed query 的所有 remote dyn filter update 与 region read 请求共用同一个 `query_id`。
2. **Phase 1 主传播路径：先走 `QueryContext.extensions`**
   - 暂时不修改 protobuf schema，也不引入新的 typed proto field。
   - 把 `query_id` 放进 `QueryContext.extensions`，例如预留一个稳定 key：`remote_query_id`。
   - 通过现有 `QueryContext -> api::v1::QueryContext -> RegionRequestHeader.query_context` 路径传到 datanode。
   - 因为当前 `extensions` 已经走现有序列化/反序列化链路，所以这条路径可以先落地、后续再决定是否升级成显式字段。
3. **消费侧**
   - datanode 通过 `RegionRequestHeader` 重建 `QueryContext` 时读取同一个 `query_id`。
   - remote dyn filter state 以 `query_id + filter_id` 作为核心逻辑关联键；target 只承担路由/注册元数据。
4. **与其他 id 的关系**
   - `process_id`：继续用于 kill/process-list/connection 语义。
   - `filter_id`：在单个 query 内标识某个 dyn filter。
   - `query_id`：跨 frontend/datanode 关联一次 distributed query execution。

## 为什么 Phase 1 先放在 `QueryContext.extensions`

当前只有 join dyn filter 这一个场景急需 query-scoped distributed identity，而直接改 protobuf schema 会引入兼容性和演进成本。对于这个阶段，更合理的权衡是：

1. **先复用已存在的可传播通道**
   - `QueryContext.extensions` 已经被现有 QueryContext protobuf 转换保留。
2. **把兼容性风险降到最低**
   - 不需要立刻修改 proto、生成代码、处理 mixed-version 节点兼容问题。
3. **为后续收敛保留空间**
   - 若最终 remote dyn filter 只有少数场景使用该字段，可以继续保留在 extensions。
   - 若后续 query-scoped execution id 被更多功能复用，再从 extensions 提升为 typed field 也更有依据。

因此，Task 01 当前推荐的是“两阶段策略”而不是一步到位：

- **Phase 1**：`QueryContext.extensions["remote_query_id"] = <uuidv7>`
- **Phase 2+（可选）**：待功能稳定后，再评估是否把它迁移为显式 proto 字段。

到这里为止，Task 01 对 `query_id` 只保留 contract 层结论：

- carrier 固定为 `QueryContext.extensions["remote_query_id"]`
- 同一 distributed query 的 remote read 与后续 dyn filter update 必须复用同一个值
- datanode 只消费该值，不回退到 `process_id`，也不在缺失时补生成新 id
- `remote_query_id + filter_id` 是 Phase 1 的最小逻辑关联键；target 信息若存在，只用于路由/注册元数据
- header 传播、请求构造、runtime 读取、清理和相关测试的实现细节，归后续 Task 02/03/04/06

## Equal Join 需要先识别的上游表达式形态

基于当前 GreptimeDB 接入方式和 DataFusion hash join 实现，equal join 场景里真正传到 scan 侧的并不是一个抽象的“join filter”标签，而是 `DynamicFilterPhysicalExpr` 持有的运行时 physical expr 快照。对 ABI 设计最关键的是以下几种形态：

1. 单列等值 join，小 build side
   - membership 形态是 `InListExpr`。
   - 典型语义是 `col IN (...)`。
2. 单列等值 join，大 build side
   - membership 形态是 `HashTableLookupExpr`。
   - 这是对 build side 哈希表的运行时引用，不适合跨节点直接序列化。
3. 单列等值 join，带统计边界
   - DataFusion 还会附加 `BinaryExpr` 形式的范围条件，即 `col >= min AND col <= max`。
   - 最终常见形态是 `bounds AND membership`。
4. 多列等值 join
   - DataFusion 会先把 probe side key 组装成 `struct(...)`，再做 `InListExpr` 或 `HashTableLookupExpr` membership 判定。
   - 这种复合 key 形态不适合直接作为 Phase 1 wire ABI。
5. partitioned hash join
   - 可能再外包一层 `CaseExpr`，按分区路由到不同 partition 的 bounds + membership 组合。
   - 这同样超出 Phase 1 可直接远端运输的稳定边界。

因此，Task 01 的 ABI 设计要以“从这些上游 physical expr 形态中投影出稳定子集”为目标，而不是假设 equal join 天然只会生成简单 `IN` 列表。

## 关于 DataFusion physical expr protobuf 的使用边界

DataFusion 现有的 physical expr protobuf 序列化能力，可以作为 Task 01 的一个**窄范围实现辅助**，但不应该成为 remote dyn filter 的正式 wire contract。

可以考虑直接复用 protobuf 的场景：

- 已经 snapshot 成静态 expr 的简单过滤表达式
- `InListExpr`
- `BinaryExpr` 形式的 `MIN_MAX` 范围条件
- 不依赖 runtime-only 对象的 `CaseExpr` / `HashExpr` 组合

不应把 protobuf 当成主协议的原因：

1. 它序列化的是 expr snapshot，而不是可持续 update 的 dynamic wrapper。
2. `HashTableLookupExpr` 在 DataFusion proto 层会被显式替换成 `lit(true)`，不会保留 large build-side membership 优化语义。
3. expr tree 本身不携带 remote update 所需的生命周期与路由信息，例如 `query_id`、`filter_id`、`epoch`、`is_complete` 以及必要的路由/注册元数据等。

因此更合适的分工是：

- remote wire contract 仍然由 `DynFilterUpdate` ABI 负责。
- 若某个 payload 已经被约束在“simple/snapshotted expr subset”内，可以把 DataFusion protobuf 当成内部编码捷径，而不是外部协议本体。

## Large Build-Side Equal Join 的 Phase 1 支持边界

Task 01 只需要把 Phase 1 的 contract 边界固定清楚：

- 小 build side、可 snapshot 的 `InListExpr` / simple expr subset 可以进入 `DynFilterPayload::Datafusion`
- `HashTableLookupExpr` 等 large build-side membership 形态必须降级为“不远端发送”
- large build-side membership 的精确阈值、上游触发条件和后续远端替代表达，归 Task 07 处理

## 推荐子步骤

1. 抽象 wire 层枚举和 payload 结构，避免 datanode 依赖 frontend 内部表达式结构。
2. 为 `query_id` 选定最终格式和生成策略，优先采用 UUIDv7 字符串。
3. 明确 Phase 1 使用 `QueryContext.extensions["remote_query_id"]` 作为传播载体，而不是先改 proto schema。
4. 为 `filter_id`、`target_region_id`、`epoch`、`is_complete` 制定文档化语义，保证消息关联、重复发送和乱序处理可实现。
5. 编写 ABI 编解码单测，验证版本号、payload bytes budget、malformed bytes 与类型不匹配时的行为。
6. 增加“能下发 / 不能下发”的判定测试，锁定 `InListExpr` / simple snapshot expr 可发送，而 `HashTableLookupExpr` / 多列复杂形态默认降级。
7. 把 DataFusion physical-expr protobuf 的使用边界写清楚：它只是 simple snapshot expr 的内部编码 helper，不是远端 dyn filter 协议本体。

## 验收标准

- wire 消息类型稳定且不泄漏物理表达式实现细节。
- query-scoped execution id 的来源和传播方式已被明确：Phase 1 先通过 `QueryContext.extensions` 引入 dedicated `query_id`，而不是直接复用 `process_id` 或先改 proto schema。
- `IN` filter 能完整表达 payload 内部的列引用与值集合，且不会额外复制一份可能失真的 top-level binding。
- equal join 的上游 physical expr 形态已经被归类清楚，并且只有可稳定投影的子集会进入远端 ABI。
- DataFusion physical-expr protobuf 是否可复用的边界已被文档化：它只适合作为 simple snapshot expr 的内部编码 helper。
- 不支持的复杂 dyn filter 会被明确降级，而不是在远端执行时报错。
- ABI 结构能够无破坏扩展到 `MIN_MAX` / `BLOOM`。

## 风险与注意点

- 不要把物理列索引当作唯一绑定键，否则跨节点重建后容易错位。
- 不要为了“通用性”做成通用表达式传输系统，Phase 1 必须收敛范围。
- 不要假设 equal join 天然只会得到可序列化的 `InListExpr`；对 large build-side、复杂多列或 partition 路由形态必须安全降级。
- 不要误把 DataFusion expr protobuf 当成“统一远端 dyn filter 协议”：它对 simple expr 有帮助，但会丢失动态更新语义，并且无法保留 `HashTableLookupExpr`。
- 不要为了省字段而复用现有 `process_id` 充当 remote query id；二者生命周期和跨节点语义不同。
- 不要在 Phase 1 为了 typed field“整洁性”过早修改 proto schema；先用 `extensions` 落地，再按实际复用范围决定是否提升为显式字段。
- payload 大小、值类型归一化和序列化成本要在接口层提前限制。
