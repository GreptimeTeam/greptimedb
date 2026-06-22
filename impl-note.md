## 第 1 阶段实现备注

本节记录 RDF Bloom 远程动态过滤第 1 阶段中，和原始预期或计划不同、后续实现需要知道的事项。

### 和原计划不同的实现点

- GreptimeDB 侧没有依赖 DataFusion 的 join-map trait。生产代码只使用 `HashTableLookupExpr` 的公开接口：`on_columns()`、`seeds()`、`visit_distinct_join_hashes(...)`。
- 第 1 阶段最初只切了 `common-query` 内的 payload、probe、encoder 模型；后续又继续补上了 FE/DN typed payload wiring 和 initial snapshot Bloom 初始化。
- 新增了 `DynFilterPayload::from_datafusion_expr_with_registered_children(...)`。Bloom 编码必须显式传入注册子表达式和输入 schema，不能只靠旧的 `from_datafusion_expr(...)`，否则无法安全生成 `join_key_child_indices` 和 `HashExpr` 兼容性指纹。
- Bloom 转换比最初计划更保守：只有恰好一个 `HashTableLookupExpr` 作为正向 `AND` 合取项时才转换。`OR`、`NOT`、其他包装表达式、多 lookup、子表达式不匹配等情况都会回退。
- 注册子表达式路径的回退逻辑改成 fail-open：遇到不安全 lookup 位置时，丢弃包含 lookup 的顶层 `AND` 合取项；如果整个表达式都不安全，则编码 `lit(true)`。这样避免 `NOT(lookup)` 被错误降级成 `NOT(true) == false`，从而产生 false negative。
- Bloom payload 现在强制要求非空 `join_key_child_indices` 和非空 `residual_datafusion_physical_expr`。纯 lookup 场景的 residual 也必须编码成 `lit(true)`，不能用“空 residual 表示 true”的隐式约定。
- `decode_expr_with_registered_children(...)` 会直接调用 `bloom.validate()`，因为 Bloom payload 可能来自 JSON 反序列化，不一定经过 proto 转换入口。
- Encoder 最终没有收集全部 distinct hash 到 `Vec` 后再构建 Bloom；后续又按性能考虑去掉了“两次遍历”。现在通过 DataFusion 新增的 `HashTableLookupExpr::distinct_join_hash_count()` 先拿 distinct count 做 sizing，再单次调用 `visit_distinct_join_hashes(...)` 流式写入 bitset。
- `JoinHashBloomProbeExpr` 的 eval 不再手写 `with_hashes` 路径；现在通过 DataFusion 自己的 `HashExpr` 计算 join hash，再用 Bloom bitset 做 membership probe，降低和 DataFusion hash 语义漂移的风险。
- Bloom payload 新增 `hash_compat_fingerprint`（proto tag 14）。编码端用相同 seeds/children 在规范 `RecordBatch` 上直接评估 DataFusion `HashExpr`，再用本地稳定 digest 生成非零指纹；解码端本地重算，指纹为 0、重算失败或不匹配时直接返回 residual-only，不构造 Bloom probe。
- 指纹 digest 不使用 `ahash`/`DefaultHasher`，而是写入固定 magic、seeds、child data type 稳定 tag 和 `HashExpr` 输出 hash 序列后再 finalize。这样能覆盖同版本不同平台的实际 hash 漂移检测，也避免 Int32/Int64 等值场景只看 hash 输出时不可区分的问题。
- Initial snapshot 现在也会优先尝试 Bloom，而不再只等 gRPC update。这个决策牺牲了对“不认识 `JoinHashBloom` JSON variant 的老 DN”的滚动兼容；如果需要跨版本滚动升级，后续必须加 capability/version gate 或回退到 Datafusion-only initial snapshot。
- gRPC update 采用“双写”：`typed_payload` 放完整 `DynFilterPayload`，deprecated legacy `payload` 继续放非空 Datafusion fallback。这样新 DN 优先读 typed，老 DN 忽略未知 tag 5 后仍能读 legacy bytes。
- fanout 编码一开始误用过 `current.children()`；这是错的，因为当前表达式的 children 不是注册 children。Bloom 编码必须用 `DynamicFilterPhysicalExpr::children()` 的注册 children，否则 `join_key_child_indices` 映射会失败或退回 Datafusion。
- fanout 的 generation watermark 改成“编码成功后再前进”；否则 Bloom/fallback 编码失败会跳过该 generation。为了避免 reconcile tick 对已发送 generation 反复做 Bloom 编码，又加了只读预检。
- DN pending/runtime 状态从裸 `Vec<u8>` 改为保存 `DynFilterPayload`，runtime 显式保存注册 children，并统一走 `decode_expr_with_registered_children(...)`。这比从 runtime filter 现取 children 更清楚，也能支持 buffered Bloom snapshot/update。
- `JoinHashBloomPayload::try_build_from_hashes(...)` 是安全构建入口；当前还保留了原始 `build_from_hashes(...)`，后续可以考虑改成私有或仅测试使用。
- 集成测试里不能用小 build-side 数据来证明 Bloom 链路：DataFusion 默认会先生成 `InListExpr`（`IN (SET)`），不会经过 `HashTableLookupExpr`。Bloom FE→DN E2E 需要让 distinct key 数超过 `hash_join_inlist_pushdown_max_distinct_values`，并且用稀疏整数 key 避免 `ArrayMap`，这样才会走 `HashMap` lookup → `JoinHashBloom` typed payload → DN `bloom_probe`。
- 现在的 FE→DN 集成测试证明的是 `JoinHashBloom` typed payload 能到 DN 并安装进 region `SeqScan` 的 `dyn_filters`，不是证明 Mito scan 已经逐行执行 Bloom。代码路径里 scan 只接受 `DynamicFilterPhysicalExpr` 包装并保存到 `Predicate::dyn_filters`；SST/memtable 侧主要把它交给 `PruningPredicate` 做统计剪枝。自定义 `bloom_probe` 对 `PruningPredicate` 来说是 unsupported，通常会被当成 `true` 保守跳过，剩下的 residual bounds 仍可用于剪枝。若要让 Bloom 在 DN 侧真正减少返回行，还需要在 scan/prefilter 路径增加对 dynamic filter current expr 的 batch-level evaluation。

### 当前限制和后续注意事项

- FE/DN wiring 阶段应优先使用 `from_datafusion_expr_with_registered_children(...)`。旧的 `from_datafusion_expr(...)` 仍会在表达式树内直接替换 `HashTableLookupExpr`，不适合任意包含 lookup 的非单调表达式。
- Bloom sizing 当前是保守常量策略，不是最终 false-positive-rate 调优方案。
- `compute_bloom_sizing(...)` 在某些 shrink-to-fit 情况下可能先给出超过硬上限的大小，再由 `try_build_from_hashes(...)` 回退；这是安全的，但后续可以简化。
- 已补一个 FE→DN Bloom typed payload 集成测试，通过 `EXPLAIN ANALYZE VERBOSE` 中 region `SeqScan` 的 `dyn_filters` 包含 `bloom_probe` 验证 payload 安装链路。后续还需要补 Mito scan batch-level Bloom evaluation、多列、null key、typed payload 权威优先级、跨 FE/DN schema 对齐等测试。
- 当前 fingerprint 只是 guardrail，不是跨版本稳定 hash 协议；它只检测规范样本上真实发生的本地 `HashExpr` 漂移。未覆盖值域（例如更多 null/NaN/复杂类型）仍可能需要后续扩展。
- Fingerprint 规范 batch 覆盖常见标量 key 类型；unsupported key 类型会触发 encoder fallback 或 decoder residual-only。nullable 的 unsupported 非 key schema 列用 typed null array 填充，避免无关列禁用 Bloom。
- `RemoteDynFilterUpdate.payload` 现在在 proto 里标了 deprecated，但过渡期仍必须写；因此 query/datanode 编译会出现 deprecated field warning，这是预期的兼容性代价，不应现在删除 legacy bytes。
- 本地 target 盘一度打满；清理了当前 worktree 的 Rust incremental build cache 后，datanode targeted tests 才能链接运行。
- 本阶段验证因为本地 path patch 会触发 `Cargo.lock` 归一化；验证后已回滚 `Cargo.lock`。`.cargo/config.toml` 仍是本地 scaffolding，不应提交。
- DataFusion 依赖 lane 也新增了 `HashTableLookupExpr::distinct_join_hash_count()`；最终集成时需要确保 GreptimeDB 指向包含该 API 的 DataFusion commit。

### 已验证命令

- `cargo fmt --all -- --check`
- `cargo check -p common-query --offline`
- `cargo test -p common-query request --lib --offline`（最近一次 87 passed, 10 filtered）
- `cargo test -p query remote_dyn_filter --lib --offline`（最近一次 46 passed, 415 filtered）
- `cargo test -p datanode remote_dyn_filter --lib --offline`（最近一次 29 passed, 76 filtered）
- `cargo check -p datanode --offline`（通过，仅依赖/legacy payload deprecation warnings）
- `cargo test -p common-query dyn_filter --lib --offline`
- DataFusion lane: `cargo test -p datafusion-physical-plan test_visit_distinct_join_hashes_hash_map --lib`
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test -p tests-integration test_remote_dyn_filter_bloom_typed_payload_e2e --lib -- --nocapture`（1 passed, 186 filtered）

## 第 2 阶段修正备注

### 和前一版预期不同的实现点

- 旧备注里“集成测试通过 `SeqScan` 的 `dyn_filters` 包含 `bloom_probe` 验证链路”的判断已经不再适用。现在 Bloom exact 部分刻意不下推到 scan，而是通过 `non_pushdown(...)` 包装留在 DN 的 `FilterExec`；`SeqScan` 最多只接收 residual/no-op 的 `DynamicFilterPhysicalExpr` 作为统计剪枝提示。
- DN 侧每个注册的 remote filter id 会创建两个运行时 wrapper：一个可下推的 pushdown wrapper，另一个 exact wrapper。即使初始 payload 是 `lit(true)` 或普通 Datafusion payload，也要保留 exact wrapper；否则后续 gRPC 更新变成 Bloom 时，计划里没有位置能执行行级过滤。
- invalid update 不再做“防御性 fail-open 更新”。oversized payload、typed payload 解码失败且无 legacy fallback、payload 解码失败、`DynamicFilterPhysicalExpr::update(...)` 失败，都不修改现有 runtime filter，也不推进 generation。只有成功解码并接受的新 payload 才能更新 pushdown/exact 两个 wrapper。
- `exact = lit(true)` 只表示“当前这个有效 payload 没有可执行的 exact Bloom 部分”，例如普通 Datafusion payload，或 Bloom payload 的 fingerprint 为空、不匹配、重算失败。它不是 invalid update 的替代状态。
- `DynamicFilterPhysicalExpr::update(...)` 的两步更新理论上不是完全事务化：如果 pushdown 更新成功但 exact 更新失败，generation 不推进但 pushdown 已经变更。当前按用户选择不加 rollback 防御，因为该失败对本路径构造出的表达式基本不可达；继续加补偿逻辑反而会扩大复杂度。
- 同 generation 的 `is_complete=true` 更新也必须先完成 payload 校验和解码，才能把 pushdown/exact wrapper 标记 complete。否则一个坏 payload 虽然不推进 generation，也可能提前关闭 runtime filter，和“invalid update 不改状态”的语义冲突。

### 当前限制和后续注意事项

- 现在的 Bloom 集成测试除计划形态外，还会解析 `EXPLAIN ANALYZE VERBOSE` 指标，要求至少一个带 `bloom_probe` 的 `non_pushdown` `FilterExec` 的 `output_rows` 小于其子 `SeqScan`，从而证明 Bloom exact 过滤发生在 scan 之外。
- 这个 E2E 仍受异步 gRPC update 时序影响。当前 fixture 保持 build 侧 302 个 key（刚好超过 in-list 阈值、编码较快），把 probe 侧放大到 8192 个稀疏 SST key 并跨分区分布，让 Bloom update 有时间在 probe scan 完成前到达；不要简单增大 build 侧，否则反而可能延迟 Bloom 生成。

### 本次补充验证命令

- `cargo fmt --all -- --check`
- `git diff --check`
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test -p datanode registrations --lib --offline`（最近一次 11 passed, 102 filtered）
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test -p datanode remote_dyn_filter --lib --offline`（最近一次 29 passed, 84 filtered）
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test -p common-query request --lib --offline`（最近一次 87 passed, 10 filtered）
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test -p query remote_dyn_filter --lib --offline`（最近一次 46 passed, 415 filtered）
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test -p tests-integration test_remote_dyn_filter_bloom_typed_payload_e2e --lib -- --nocapture`（最近一次 1 passed, 186 filtered）
