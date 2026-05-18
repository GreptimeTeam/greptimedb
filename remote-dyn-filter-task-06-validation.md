# Task 06 - 端到端验证与回归基线

## 任务目标

建立 remote dyn filter 的功能、一致性、可靠性和性能验证矩阵，证明该机制在带来 pruning 收益的同时，不会因异常路径破坏查询正确性。

## 主要落点

- `tests/`
- `tests-integration/`
- 分布式 query / datanode / region 相关测试目录
- 基准或 benchmark 脚本目录

## 前置依赖

- Task 01-05 完成最小可用实现。

## 实现范围

1. 功能测试:
    - distributed hash join，小 build side 对大 probe side 产生远端 pruning
    - 无法序列化表达式时自动降级且结果正确
    - 初始 remote read 与后续 dyn filter update 使用同一个 `remote_query_id + filter_id` 逻辑 identity，并稳定绑定到对应 subscriber scan 集合
2. 一致性测试:
    - update 乱序
    - 重复 epoch
    - complete marker 早到 / 晚到
    - 仅部分 region 收到 update
    - `remote_query_id` / `filter_id` 缺失或关联键不完整时安全降级，不串线到其他 query state
    - datanode 本地 `is_used()` 类 eager cleanup 与 frontend unregister / TTL 兜底不会互相打架或造成误删
3. 可靠性测试:
   - frontend 中途停止发送
   - datanode 重启或 scan 提前结束
   - query cancel 后状态释放
4. 性能验证:
   - 高频小 update 与低频批量 update 对比
   - 不同 `IN` cardinality 阈值收益曲线
5. 建立回归基线，为后续 Phase 2/3 演进提供对照数据。

## 推荐子步骤

1. 先补最小集成测试，证明“能工作且失败不影响结果”。
2. 再补乱序 / 幂等 / 清理类单测，把状态机语义锁住。
3. 最后增加 benchmark 或 profiling 脚本，比较不同更新策略的控制面开销。
4. 为日志和 metrics 增加断言，确保观测口径不会回归。

## 验收标准

- 至少一个 distributed join 集成测试证明远端 pruning 生效。
- 所有异常路径测试都证明查询结果仍然正确。
- 状态机相关测试覆盖乱序、重复、complete marker、超时回收。
- 有可重复运行的性能对照基线，能支撑是否继续推进 stream/batch 演进。

## 风险与注意点

- 只测 happy path 没有意义，remote dyn filter 的价值在于异常时也必须安全退化。
- 性能测试必须同时看收益和控制面成本，不能只看扫描量下降。
- Phase 2/3 若没有基线，后续优化很难判断是真提升还是只是在搬移开销。
