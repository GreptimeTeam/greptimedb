# Task 05 - 观测性、降级与生命周期保护

## 任务目标

补齐 remote dyn filter 机制的可观测性、资源保护和错误降级能力，确保它始终是“可失效的优化项”，而不是新的稳定性风险源。

说明：query-scoped filter state 的生命周期契约与实际清理由 Task 04 主持，Task 05 只负责为这些行为补充指标、日志、预算和兜底策略。

## 主要落点

- frontend / datanode metrics 模块
- tracing / logging 模块
- query lifecycle cleanup 相关逻辑
- RPC 调用保护、内存预算与节流配置模块

## 前置依赖

- Task 02-04 至少具备基本端到端语义。

## 实现范围

1. 增加 metrics:
    - update 发送次数
    - update 应用次数
    - 过期 epoch 丢弃次数
    - payload 解码失败次数
    - complete marker / cleanup 次数
    - frontend registry 注册 / 注销次数
    - datanode registry 注册 / 注销次数
2. 增加 tracing 字段：`query_id`、`filter_id`、`epoch`，以及必要时的 route/subscriber region 元数据。
3. 为 Task 04 已定义的 TTL 清理和 query cancel cleanup 补充指标、日志、超时配置和告警信号。
4. 增加 cardinality、payload 大小、内存预算保护。
5. 明确降级语义：控制面失败、目标缺失、类型不匹配、状态丢失都只关闭优化，不中断查询。
6. 为 batched update / heartbeat / ack 预留配置和抽象边界。
7. 为 frontend 基于 `is_used()` 触发的 unregister 路径，以及 datanode 基于本地 `is_used()` 类语义触发的 eager cleanup 路径，补充日志、指标和异常回收兜底，避免“逻辑上已无人使用但状态未清理”不可观测。

## 推荐子步骤

1. 先定义最关键的 metrics 名称和标签，避免后续改动观测口径。
2. 在 frontend 和 datanode 两端都打 tracing，便于串联一次 update 的全链路。
3. 将清理逻辑做成统一入口，避免 query finish、cancel、timeout 各自分叉。
4. 把降级事件写入日志或指标，让“优化没生效”的原因可追踪。
5. 为“update 早到被缓冲/丢弃/重试”的策略增加可观测性字段，方便后续判断是否需要演进到 stream 或更强注册机制。
6. 区分“frontend 正常 unregister”与“query cancel / TTL 兜底回收”，避免把两种清理路径混为一谈。

## 验收标准

- 能看见每个 filter 的发送、接收、应用、丢弃和清理情况。
- query 结束、取消、异常断连后，状态能被可靠回收。
- 当控制面出错时，查询结果保持正确且问题可观测。
- 内存与频率保护能阻止异常 workload 打爆控制面或状态表。

## 风险与注意点

- 没有指标的优化很难上线调参，Task 05 不能被视作“后补文档工作”。
- TTL 只是兜底，不应替代 complete marker 和 query cancel hook。
- tracing 不能打印过大的 payload 内容，避免日志膨胀与敏感数据泄露。
