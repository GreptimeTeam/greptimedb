# Task 02 - Region Unary RPC 控制面链路

## Goal

为远程 dyn filter 建立最小可用的 frontend -> datanode unary RPC 控制面链路。Phase 1 复用现有 `greptime.v1.region.Region` unary gRPC 服务，不新增独立 gRPC service，也不使用 Arrow Flight。

## Touchpoints

- `src/client/src/region.rs`
- `src/datanode/src/region_server.rs`
- Region gRPC / service 定义所在模块
- 与 `src/common/query/src/request.rs` 相邻的请求结构定义

## Dependencies

- 依赖 Task 01 提供稳定的 `DynFilterUpdate` ABI。

## Scope

### In Scope

1. 在 `greptime.v1.region.RegionRequest.body` 中新增单一总入口：`remote_dyn_filter: RemoteDynFilterRequest`。
2. `RemoteDynFilterRequest` 作为远程 dyn filter 控制面的统一 envelope，内部使用 `oneof action` 区分具体操作；Phase 1 仅定义：
   - `update`
   - `unregister`
3. 在 frontend/client 侧补齐 `RemoteDynFilterRequest` 的封装、序列化与发送逻辑。
4. 在 datanode `region_server` 中新增 special-case 接收入口、鉴权/校验与错误映射，处理方式与现有 `Sync` / `ListMetadata` 一致。
5. 明确 RPC 返回语义：成功、幂等重放、安全忽略、缺失 `query_id`、可降级失败。
6. 确保 RPC 失败不会让主查询失败，调用方只能记录并降级。
7. `query_id` 在 Phase 1 中显式放入 `RemoteDynFilterRequest.query_id`，不依赖 `RegionRequestHeader.query_context` 透传。
8. 初始 remote read 继续负责建立 datanode 侧 scan/consumer 注册关系；Task 2 的 unary RPC 只负责后续 `update / unregister` 控制面。
9. `DynFilterUpdate` 的 payload 继续承接 Task 01 已定义的最小 ABI；Task 02 只负责其真实发送/接收通路。
10. `unregister` 语义必须与 `is_complete` 明确区分：
    - `is_complete`：不会再有更强更新
    - `unregister`：该 query 在该远端已不再需要该 filter

### Out of Scope

- dedicated gRPC service
- Arrow Flight 控制面
- datanode 侧真实 dyn filter 状态管理与 apply 语义
- scan wrapper 注入
- plan remap
- DN 本地消费侧替换逻辑
- batched / streaming update

## Protocol / Design Decisions

- `query_id` 在 Phase 1 中显式放入 `RemoteDynFilterRequest.query_id`，不依赖 `RegionRequestHeader.query_context` 透传。
- `query_id + filter_id` 是远程 dyn filter state 的逻辑 identity。
- `RegionRequest.body` 顶层只增加一个统一入口 `remote_dyn_filter`。
- 具体操作通过 `RemoteDynFilterRequest.oneof action` 扩展。

## Execution Order

1. 对齐现有 Region RPC 风格，保持错误处理模式一致；由于 `RegionRequest` 现有处理链路主要消费 body，Phase 1 的 `query_id` 直接放在 `RemoteDynFilterRequest` 中。
2. 将 `RegionRequest.body` 顶层扩展固定为单一入口 `remote_dyn_filter`，避免后续每新增一个 dyn filter 操作都重复污染顶层 `body`。
3. 将 dyn filter 的操作扩展点收敛到 `RemoteDynFilterRequest.oneof action`，而不是使用“单 message + op enum + 大量共享 optional 字段”的设计。
4. 固定 scan 注册契约：由初始 remote read 显式建立 datanode 注册，后续 unary RPC 只负责 `update / unregister`。
   - 不再保留“首次 update 懒注册”作为 Phase 1 选项，避免 frontend 与 datanode 对 consumer 生命周期理解不一致。
   - region RPC 仍可作为 transport 路由入口，但 `target_region_id` 不再是 dyn filter state identity 的一部分。
5. datanode 侧抽出独立 handler，使后续从 unary 演进到 batched 或 stream 时可以复用状态处理逻辑；Phase 1 允许先以 placeholder 形式接线。
6. 补客户端重试与超时策略，但默认保持保守，避免控制面风暴。
7. 增加 RPC 层单测 / 集成测试，覆盖解码失败、权限失败、`query_id` 缺失等路径。
8. 为 `query_id` 明确 extensions / header 级测试，至少验证：
    - 多 frontend 同时发起查询时 `query_id` 不冲突。
    - `QueryContext.extensions["remote_query_id"]` 能随 remote read 和后续 dyn filter update 一致传到 datanode。
    - `process_id` 仍保留原有 kill/process-list 语义，不与新的 distributed `query_id` 混淆。

## Expected Proto Shape

```proto
message RegionRequest {
  RegionRequestHeader header = 1;

  oneof body {
    // existing variants...
    RemoteDynFilterRequest remote_dyn_filter = N;
  }
}

message RemoteDynFilterRequest {
  string query_id = 1;
  oneof action {
    RemoteDynFilterUpdate update = 2;
    RemoteDynFilterUnregister unregister = 3;
  }
}

message RemoteDynFilterUpdate {
  string filter_id = 1;
  bytes payload = 2;
  uint64 generation = 3;
  bool is_complete = 4;
}

message RemoteDynFilterUnregister {
  string filter_id = 1;
}
```

### 字段约定

- `query_id` 在 Phase 1 中显式放入 `RemoteDynFilterRequest.query_id`，作为控制面状态的 query 维度标识。
- `filter_id` 在 Phase 1 先保持为稳定、简单的字符串标识，不把 proto 直接绑定到某个 Rust 内部 id 类型。
- `payload` 直接承接 Task 01 已定义的 `DynFilterPayload::Datafusion(Vec<u8>)`。
- `generation` 用于幂等与乱序保护。
- `is_complete` 只表示该 filter 不会再有更强更新，不表示 consumer 生命周期结束。

## Datanode Handler Shape

`remote_dyn_filter` 应作为 `RegionServer` 的 special-case request 处理，方式与现有 `Sync` / `ListMetadata` 类似。

推荐分发逻辑：

- `Body::RemoteDynFilter(req)` -> `handle_remote_dyn_filter_request(req)`

总入口 handler 负责：

1. 提取 `query_id`
2. 根据 `action` 分发到：
   - `handle_remote_dyn_filter_update`
   - `handle_remote_dyn_filter_unregister`

其中：

- `update` handler 在 Phase 1 至少负责校验 `query_id / filter_id / payload / generation` 与接线；真实写入 DN shared filter state 由后续状态管理任务负责。
- `unregister` handler 在 Phase 1 至少负责校验与占位接线；真实删除/注销 `query_id + filter_id` 状态由后续状态管理任务负责。

Task 2 只要求打通最小接收、分发和状态更新入口，不在本任务内引入 scan wrapper 注入、plan remap 或 DN 本地消费侧替换逻辑。

## Cross-Repo Update Flow

由于 Task 2 需要修改 `greptime.v1.region.RegionRequest.body`，因此通常不能只在当前仓库内完成；需要先更新 `greptime-proto` 仓库，再把当前仓库的 proto 依赖切到新的 commit。

推荐流程如下：

1. 在 `greptime-proto` 仓库中创建新分支。
2. 修改 region proto：
   - 在 `RegionRequest.body` 中新增 `remote_dyn_filter: RemoteDynFilterRequest`
   - 新增 `RemoteDynFilterRequest`
   - 新增 `RemoteDynFilterUpdate`
   - 新增 `RemoteDynFilterUnregister`
3. 在 `greptime-proto` 仓库中执行生成流程（例如 `make all`），确保生成代码与校验全部通过。
4. 提交并 push `greptime-proto` 分支，得到新的 proto commit。
5. 回到当前仓库，在 `Cargo.toml` 中将 `greptime-proto` 依赖更新到新的 commit；必要时同步更新 lockfile。
6. 之后再在当前仓库内实现 Task 2 的 client / server 逻辑。

### 任务边界说明

- `greptime-proto` 仓库负责协议定义与生成代码。
- 当前仓库负责消费新 proto，并实现 frontend -> datanode 的 unary RPC 收发与分发逻辑。
- 若 proto 尚未合入或依赖 commit 尚未更新，则不要在当前仓库中先行写死临时本地结构体替代正式 proto。

## Validation

- frontend 能向指定 datanode 发送 dyn filter update，并在 `RemoteDynFilterRequest` 中稳定携带 `query_id`。
- frontend 能通过同一总入口发送 dyn filter unregister。
- datanode 能识别并 special-case 分发 `RemoteDynFilter` 请求。
- 缺失 `query_id` / `action` 等基础字段时能返回明确错误。
- 当前阶段若真实状态管理尚未引入，handler 可以明确返回 placeholder / `NotYetImplemented`，但链路与协议形状必须稳定。
- RPC 故障不会影响查询结果正确性。
- 后续演进到 batched update 或 stream 时无需推翻 handler 核心语义。
- 后续若新增 `register / cancel / heartbeat / batched update` 等操作，只需扩展 `RemoteDynFilterRequest.oneof action`，不需要再次修改 `RegionRequest.body` 顶层设计。

## Risks / Caveats

- 不要把更新通道塞回一次性 `QueryRequest`，否则生命周期和职责会继续耦合。
- 错误码必须能表达“优化失败但查询继续”的语义。
- 不要使用“单 message + op enum + 大量共享 optional 字段”的设计，否则后续会把不同 action 的字段语义搅在一起。
- 若同一 query 面向多个 region，高频单发请求会放大开销，接口设计要为批量化留口子。
