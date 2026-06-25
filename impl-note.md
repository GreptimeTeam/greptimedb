# Flow `EVAL INTERVAL` 稳定调度实现备注

## 本次需要特别知道的事项

- `flow.scheduled_runtime_millis` 只在调度尝试执行期间临时写入 `TaskState.query_ctx`，执行结束后恢复原来的 `QueryContextRef`。这样是为了避免后台调度循环的逻辑时间泄漏到后续手动 `flush_flow`。
- 当前 `expire_after` 的保留窗口下界仍按墙钟时间计算，不按调度运行时间计算；这属于产品语义待明确事项，本次没有改变。
- `remove_flow` / replace 这类路径通常会中止整个任务；如果异步任务正好在 `execute_once_unlocked(...).await` 中被中止，临时 `QueryContext` 恢复代码不会继续执行。由于任务会被移除，评审认为不阻塞；后续可以考虑取消安全的恢复保护。
- 调度循环的长时间睡眠仍主要依赖任务中止来打断，未改成 `select!` 监听关闭信号；这和旧自适应循环行为一致，本次未扩大范围。
- 指标和 sqlness 端到端用例暂未补：建议后续再加调度滞后、跳过的运行时间数量、尝试状态，以及 SQL/TQL `now()` 调度运行时间的端到端覆盖。
- `/home/discord9/greptimedb-flow-stable-schedule/.cargo/config.toml` 是本地 target-dir 配置，不属于本次实现内容，后续提交或 PR 不应带入。
- 调度配置现在是内部类型化状态：`FlowInfoValue.eval_schedule: Option<FlowScheduleConfig>`。`eval_interval_start`、`eval_interval_anchor` 等名字目前不作为用户 `WITH` flow options 暴露；用户传入时走普通 unknown option allowlist 错误。未来如果要暴露 `eval_interval_start`，应在输入层解析后写入 `FlowScheduleConfig`，不要再把原始字符串持久化到 `FlowInfoValue.options`。
- meta 到 flownode 的 `CreateRequest` 目前没有 typed schedule 字段，因此只在这个 RPC 边界临时使用 `__greptime_internal_eval_schedule` JSON key；flownode 收到后立即移除并反序列化。该 key 不应进入 metadata，也不应展示给用户。
- 旧 metadata 缺少 `eval_schedule` 时，会用 `eval_interval_secs` 和 `created_time` 按默认值派生 deterministic schedule，避免恢复时按当前墙钟漂移。不兼容本 PR 内早期中间态的字符串 schedule options。

## 关键验证结果

- 阶段 A / 阶段 B / 阶段 C 均已通过 `@oracle` 审查，阶段 C 最终结论为通过，无必须修复项。
- 类型化调度配置改造后复验已通过：`cargo test -p common-meta --lib create_flow`（39 passed）、`cargo test -p operator --lib validate_and_normalize_flow_options`（6 passed）、`cargo test -p operator --lib schedule`（8 passed）、`cargo test -p flow --lib eval_schedule`（36 passed）、`cargo test -p flow --lib batching_mode::task`（43 passed）、`cargo check -p flow --lib`、`cargo check -p common-meta --lib`、`cargo check -p operator --lib`、`git diff --check`。
