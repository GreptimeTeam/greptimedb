# JSON2 Skipping Index 需求与验收

## 目标与范围

沿用 [PR #9324](https://github.com/GreptimeTeam/greptimedb/pull/9324) 的 JSON2 路径索引能力，为显式 type hint 支持 Bloom Filter skipping index，加速等值和 `IN` 查询。

```sql
CREATE TABLE logs (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (
        service.name STRING SKIPPING INDEX,
        status BIGINT SKIPPING INDEX WITH (
            granularity = '8192', false_positive_rate = '0.01'
        )
    )
);
```

- 复用普通列 skipping index 的参数、默认值与校验规则；同一路径允许同时配置 inverted 和 skipping index。
- 创建表、修改 type hints、`SHOW CREATE TABLE`、元数据持久化及重启恢复均保留配置。
- flush、compaction 按当前 hints 构建路径索引；查询按各 SST 的路径与类型兼容性使用索引。
- 本期不覆盖自动展开路径、其他 skipping 算法及已有 SST 的手动索引重建；修改 hints 不立即回填旧 SST。

## 简要方案

1. 扩展 proto `JsonTypeHint`，增加可选 skipping 配置（类型、粒度、误判率），缺省表示未启用；保留已有字段编号。
2. 同步内部 hint 类型、SQL 解析/展示及 DDL 转换；复用现有 `IndexTarget::JsonPath` 和路径取值逻辑，扩展 Bloom Filter 构建、查询与缓存标识。
3. 开发时将 `greptime-proto` 依赖指向 `/home/fys/projects/greptime-proto` 联调；提交前替换为正式 proto revision，不提交本机绝对路径。

## 验收标准

- [x] 默认/自定义参数可用；重复声明、非法参数和不支持的类型或路径明确报错；DDL 展示可重建相同配置。
- [x] proto 与元数据往返、ALTER hints、重启后配置不丢失；旧数据缺少新字段时仍可读取，普通列索引和 JSON2 inverted index 无回归。
- [x] flush、compaction 后路径索引均可用；通过裁剪统计或定向单测证明 `=`、`IN` 实际跳过不匹配的数据块。
- [x] 开关索引的查询结果一致，覆盖 NULL、缺失路径、嵌套路径、多路径及两类索引共存；允许误判，不允许漏查。
- [x] 新旧 SST 混读、hint 增删或类型变化时，无索引或类型不兼容的 SST 安全回退扫描。
- [x] 相关 Rust 单测、JSON2 SQLness 回归及 proto/持久化兼容性用例通过。

## 实施顺序

已按以下顺序完成：proto 与元数据 → SQL 与 DDL → 索引构建与查询 → 回归及兼容性验证。

## 进展

- [完成] proto 与元数据：新增可选配置、生成 Rust bindings，接入本地依赖及双向 DDL 转换。
- [完成] SQL 与 DDL：支持参数校验、展示、ALTER 和两类索引共存。
- [完成] 索引构建与查询：支持两种 batch 格式、路径谓词、SST 类型兼容检查及独立路径缓存。
- [完成] 验证：相关 Rust 回归 281 项通过；相关 crate 的 Clippy（all-targets）通过；JSON SQLness 在 standalone/distributed 全部通过；改动前本地 binary → 当前 binary 的 standalone 升级兼容性通过。裁剪计数已验证：flush 后 4 行，compaction 后 5 行，类型变化并 compaction 后 3 行。最终 binary 重建及 SQL/兼容性复跑均通过。

提交准备：proto 已提交至 fys 分支，GreptimeDB 改为固定引用 `58216917781a3adfefd1eb94f517a6007785d0f6`，移除本机路径依赖；后续上游合入时更新依赖地址/revision。

## 自查两轮

- 第一轮：检查构建/裁剪、路径缓存隔离、新旧 SST 类型兼容与 proto 字段兼容，未发现新的漏查问题。
- 第二轮：检查参数往返、边界及测试覆盖，发现配置精度偏差：同样设置 `false_positive_rate='0.00930001'`，CREATE 显示 `0.0093`，ALTER 后变为 `0.0092`。已用当前 binary 的临时 SQLness 用例复现；proto 转换再次浮点截断导致，作为非阻塞的后续改进。
- Review 结论：用户确认精度问题不阻塞本次交付；普通列已有相同行为，后续可统一改进公共转换与边界测试，本次不扩大范围。
