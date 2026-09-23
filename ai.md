---
Feature Name: JSON2 Type Hint Indexes
Date: 2026-09-22
Status: Draft
---

# 摘要

本文提议允许 JSON2 type hint 为其对象路径声明倒排、全文或跳数索引。type hint 已
经是 JSON2 列下一个具有类型、可空的 Arrow 叶子字段，因此可将带索引的 hint 视作标
量索引输入：每个表行对应一个值（或 null）。索引算法及其磁盘 payload 无需理解 JSON。

需要泛化的是索引目标的身份。当前三类索引都仅以 `ColumnId` 标识目标；而 JSON2 叶
子应由“根 JSON2 列 ID + 对象键路径 + 叶子类型”标识。本文扩展已有 `IndexTarget`，
并使建索引、查找和文件元数据均基于它工作。

数组穿透明确不在范围内。type hint 路径只包含对象键，且不得跨越数组。因此被索引的
hint 每行始终只有一个标量值，无需引入多值索引语义、行展开或数组感知的谓词改写。

# 背景与动机

JSON2 会保留原始 JSON 文档，同时将选定路径物化为有类型的 Arrow 字段。用户常按
`resource.service.name` 等日志、Trace 属性过滤，在属性文本中检索消息，或筛选低
基数标识符。这些负载与普通列索引解决的问题相同，但 JSON2 hint 当前没有可用的索引
声明：

- `INVERTED INDEX` 已可被解析并写入 `JsonTypeHint`，但 Mito 的索引构建和应用路径
  并未消费它。
- `FULLTEXT INDEX` 与 `SKIPPING INDEX` 在 hint 解析阶段即被拒绝。
- Puffin blob 名称和索引元数据仅编码列 ID，无法区分同一 JSON2 列上的两个索引路径。

若让 JSON 本身感知索引，就会使每个后端与 JSON 布局和数组语义耦合。复用已物化的
type-hint 叶子可避免这一耦合。

# 目标与非目标

目标：

- 在 JSON2 type-hint 叶子上声明倒排、全文和跳数索引。
- 复用对应普通列索引的现有后端、选项、裁剪行为和查询函数。
- 支持穿过 JSON 对象的嵌套路径，包含带引号的键段。
- 保证既有普通列索引和 JSON2 schema 仍可读取。

非目标：

- 为任意未 hint 的 JSON 路径或 JSON2 remainder 字段建索引。
- 穿透、展开或索引 JSON 数组，包括 `[n]`、`[*]` 和对象数组。
- 增加新的 SQL 查询函数或 JSON 专用索引格式。
- 为已有 SST 补建 type-hint 索引，包括手动重建和异步补建流程。本次仅覆盖 flush
  或 compaction 新生成 SST 时的索引构建及查询应用。

# 用户语法

扩展已有 JSON2 选项语法。`path` 为现有的点分隔标识符路径；带引号标识符保持既有语义。

```text
json2_option := max_auto_expanded_paths '=' unsigned_integer
              | type_hint

type_hint := path data_type type_hint_index*

type_hint_index := 'INVERTED' 'INDEX'
                 | 'FULLTEXT' 'INDEX' [ 'WITH' '(' fulltext_option_list ')' ]
                 | 'SKIPPING' 'INDEX' [ 'WITH' '(' skipping_option_list ')' ]
```

同一种索引在一个 hint 中至多出现一次。同一 hint 可同时声明多种索引，与普通列的行
为一致。`SHOW CREATE TABLE` 沿用当前规范顺序：`FULLTEXT`、`SKIPPING`、`INVERTED`。

```sql
CREATE TABLE logs (
  ts TIMESTAMP TIME INDEX,
  attrs JSON2 (
    "service.name" STRING INVERTED INDEX,
    trace.id STRING SKIPPING INDEX WITH(
      type = 'BLOOM', granularity = 1024, false_positive_rate = 0.01
    ),
    log.message STRING FULLTEXT INDEX WITH(
      analyzer = 'English', backend = 'tantivy', case_sensitive = false
    ),
    http.status_code BIGINT INVERTED INDEX SKIPPING INDEX
  )
) ENGINE = mito;
```

当 `ALTER TABLE ... MODIFY COLUMN ... JSON2(...)` 提供 JSON2 选项时，也接受同一
语法。修改 hint 的索引配置后，之后 flush 或 compaction 生成的 SST 使用新定义。
已有 SST 保持不变；缺少匹配索引时正常扫描。本次不提供已有 SST 的手动重建或异步
补建能力，也不因索引配置变更主动触发 compaction。

## 合法目标

目标须同时满足：

1. 根节点为 JSON2 列，且路径是非空的对象键序列。
2. 路径是显式 type hint，而非自动扩展路径；路径任意一段都不能是内部保留字段名
  `!__remainder__!`。
3. hint 叶子类型是所选既有索引后端支持的标量类型。全文索引仅支持 `STRING`，与
  `matches` 和 `matches_term` 一致；倒排与跳数索引复用其当前类型校验。
4. 路径不包含数组下标、通配符、范围，也不得存在数组下方的路径段。值与 hint 类型
  不一致时，保持现有写入时报错行为；缺失值按 null 索引。

解析器应对重复索引种类、不支持的选项、不支持的类型/索引组合，以及任何试图表达数
组路径的写法返回无效 SQL 错误。Pipeline YAML 表示也应暴露相同的三种索引配置及校
验，而不是继续保留当前只支持倒排的 `index` 标记。

# 查询语义

索引仅用于裁剪优化：相关 Puffin blob 缺失、过旧或不可用时，查询结果不能改变。

仅当规划器能够证明表达式精确对应某个标量路径时，带索引的 hint 才可使用。第一版支持的形式如下：

| 索引 | 可使用的谓词 |
| --- | --- |
| 倒排 | 当前普通倒排索引列已接受的 `=`、有序比较、`BETWEEN`、`IN` 和正则形式 |
| 全文 | `matches(j.path, query)`、`matches_term(j.path, term)`，包含当前支持的 `lower` 形式 |
| 跳数（Bloom） | 当前普通跳数索引列已接受的等值和 `IN` 形式 |

复合 JSON 语法（`attrs.http.status_code`）和 `json_get(attrs, '$.http.status_code')`
仅在路径是字面量、只经过对象、且与目标精确相等时可使用索引。第一版不会识别 cast
或任意表达式，除非已有普通列索引提取器已识别等价形式。穿过数组的路径永远不可使用
索引，即便查询本身可以求值。

查询规划器已将复合 JSON 访问降为 `json_get`。三类索引 applier builder 应增加共享
的 `expr_to_index_target` 辅助函数：识别这个 scalar function，解析根列和字面量路
径，校验路径存在于本次查询所用的当前最新 region schema 的显式 type hints 中，并
返回 `IndexTarget` 和具体类型。各 builder 还必须确认该 hint 启用了对应索引，不能
仅因为旧 SST 上存在索引就使用它。普通 `Expr::Column` 也通过同一辅助函数映射到
`IndexTarget::ColumnId`。

# 设计

## Type-hint 元数据

`datatypes::json::JsonTypeHint` 位于 JSON2 Arrow extension metadata 中，是持久化
兼容性边界。应以能表达三种索引及其现有选项类型的索引选项结构，替代单一
`inverted_index: bool` 表示。反序列化器必须接受历史的 `inverted_index` 布尔值，
并将 `true` 转换为启用倒排的选项。新增字段必须可选或带 serde 默认值。

SQL AST、与 `JsonSettings` 的双向转换、展示格式化器、解析器和 pipeline 解析器均
使用这一表示，从而避免形成两套略有差异的 JSON2 索引规则。

## 索引目标

扩展 `src/index/src/target.rs` 中已有的目标抽象：

```rust
enum IndexTarget {
    ColumnId(ColumnId),
    JsonPath {
        column_id: ColumnId,
        path: Vec<String>,
        data_type: ConcreteDataType,
    },
}
```

JSON 目标的身份包含叶子的具体类型。同一路径的 `Int32` 与 `Int64` 是不同目标，使
类型一致成为索引查找的必要条件。这里只包含叶子类型，不包含 nullable、索引开关、
全文 analyzer 等其他 hint 或索引配置。

创建或解码 JSON target 时，均须校验路径非空且任意一段都不等于 `!__remainder__!`。
持久化 target 的解码不依赖最新 schema，也不要求它仍属于最新 type hints。旧 SST
中的 target 可以继续保留；hint 被删除、类型改变或对应索引被关闭时，由查询端的可
用性校验阻止使用旧索引。读取到违反路径约束的 target 时，不使用该索引裁剪。

编码键必须稳定、带版本且无歧义。保持 `ColumnId` 的既有十进制编码；JSON 目标使用
`j1:<column-id>:<base64url(serde_json((path, data_type)))>`，其中 JSON 使用紧凑
UTF-8 编码，base64url 不带 padding。这可避免点、引号和 Unicode 带来的分隔符转义
问题。`data_type` 直接复用 `ConcreteDataType` 的 serde 表示；SST Parquet metadata
与 region manifest 中的 `RegionMetadata` 已使用这一持久化表示，无需另造类型编码。

除了反序列化兼容，target key 还要求同一目标生成稳定的字节。应增加固定编码样例测
试，覆盖路径、不同标量类型及类型参数，防止序列化格式调整无意改变 key。`decode`
必须能够还原编码后的目标；读取 blob metadata 后，后续代码使用有类型结构，而不是
解析字符串。

Puffin blob 名称继续采用 `<index-kind>-<target-key>`。既有列键不变，旧 blob 仍可读取。系统表输出将目标解码为目标类型和 JSON。例如，一个 JSON 叶子显示为：

```json
{"json_path":{"column":7,"path":["resource","service.name"],"data_type":{"String":{}}}}
```

## 索引构建

构造 indexer 时，region metadata 除普通列目标外，还应从每个 JSON2 列的 `JsonSettings` 产出 JSON 目标。三个既有 creator 持有 `IndexTarget` 描述符，而非只有 `ColumnId`。

索引在 flush 或 compaction 生成新 SST 时构建，不为已有 SST 单独补建。构建时必须确保输出数据中实际物化叶子的类型与 target 的 `data_type` 一致。

对于 `ColumnId` 目标，保持现有取值逻辑。对于 `JsonPath` 目标，共享 resolver 沿根 Struct array 按 `path` 寻找已显式物化的对象字段，并返回最终可空 Arrow array。该 resolver 每个 batch 行产生一个值；缺失的 hinted 叶子由 JSON2 现有逻辑补为 null。随后三个 creator 以既有值编码消费这个标量 array：

- 倒排索引编码有类型的值，写入现有 FST/bitmap payload；
- 全文索引消费解析后的 `STRING` 叶子，输入现有 Tantivy 或 Bloom 后端；
- 跳数索引通过现有 Bloom value codec 编码解析后的标量。

因此后端不需要 JSON 专用重写；新增部分仅是目标选择和 Arrow 叶子提取。稀疏主键的处理仍只适用于普通 tag 列；JSON2 目标是物化的 field 值，走现有 field-value 路径。

`IndexOutput` 与 `FileMeta` 当前通过 `ColumnIndexMetadata` 记录逐列索引可用性。不得将 JSON 叶子塞入 `column_id`。本次不接入已有 SST 的手动或异步补建流程，因此不再以补建需求为由预设新增 target-index 元数据集合。实现前需确认新 SST 的索引产出、文件级可用性记录及查询路径是否需要扩展这些结构；如确有必要，仅增加这些路径所需的元数据，并保持旧 manifest 可读。Puffin target key 仍是索引目标身份的依据。

## 索引应用

每个 applier builder 按 `IndexTarget` 收集谓词。JSON 目标从当前 hint 定义获取 `data_type`，将其同时用于谓词值编码和 target key 生成。打开 blob 时，以 creator 产出的同一 target key 查找；若旧 SST 上同一路径的索引类型不同，则无法匹配该 key，回退扫描，不使用旧类型索引裁剪。普通列仍在键旁保留预期具体类型，并沿用与 SST `RegionMetadata` 比较类型的现有检查，不改变既有列键。

全文请求 map 也改为以 `IndexTarget` 为键。它在发起 blob 查找前必须确认解析的目标启用了全文选项，避免将未建索引 JSON 路径上的 `matches` 谓词当成已索引。

未知 target 编码、blob 缺失、类型不匹配和目标解析失败均遵循现有保守行为：放弃该 SST 上的索引并正常扫描，绝不能丢弃行。

# 兼容性与发布

JSON settings 和 Puffin 名称是持久化格式；若新 SST 的索引管理确需扩展 `FileMeta`，也属于持久化格式变更。因此该能力在 SQL 表面和兼容性方案稳定前，应置于 `experimental_` 配置开关之后发布。

- 新 reader 同时理解十进制列目标和 `j1` JSON 路径目标。
- 新 writer 对普通列继续写入旧十进制 target key。
- 含 `inverted_index` 的旧 JSON2 type-hint metadata 仍可读取。
- 若需要新增 `FileMeta` 字段，必须使用 serde 默认值等兼容方式，保证旧 manifest 仍可读取。
- 已有 SST 缺少对应 type-hint 索引时正常扫描；不会因新增或修改索引声明而触发历史 SST 补建。
- 不理解 JSON 路径 blob 的二进制必须忽略它，而不能把它当作列索引解释。在默认打开开关前，必须有明确的混合版本兼容性用例。

实现时必须依照本地 runbook 在 `tests/compatibility/` 下覆盖 JSON settings、索引目标编码，以及实际涉及的 manifest 格式变更。

# 增量实施计划

本轮先实现倒排索引，分为三个阶段。每个阶段完成后，须经用户验证确认，才继续下一阶段。跳数与全文索引的接入安排另行讨论。

1. **语法解析**：完成倒排索引声明的解析、校验和展示，通过 SQLness case 验证 CREATE、SHOW CREATE 及非法声明；合法 ALTER 的解析和格式化由解析器测试验证。本阶段不修改 protobuf，不验证 ALTER 的索引配置传递，也不接入索引构建或查询应用。
2. **写入实现**：增加携带 `ConcreteDataType` 的 `IndexTarget::JsonPath`、稳定编解码和 JSON2 标量叶子提取，使 flush / compaction 新生成的 SST 正确创建索引元信息及倒排索引文件；补充固定 key 样例及实际涉及的持久化兼容性验证，不为已有 SST 补建索引。
3. **查询应用**：识别精确路径谓词，按当前显式 hints、索引开关和类型选择可用 target，并应用倒排索引裁剪。通过 SQLness case 验证索引应用及查询结果，覆盖缺失值、未索引路径、旧 SST 无匹配索引和 hint 变更后的回退行为。

已知后续事项：当前 ALTER 的 protobuf `JsonTypeHint` 不携带索引配置，接收端将倒排标记设为 false。完整支持 ALTER 时需扩展协议和传递链路；届时可先将 proto 依赖指向本地仓库进行开发，本阶段不处理。

# 考虑过的替代方案

## 将每个带索引 hint 视作合成物理列

这能继续使用当前 `ColumnId` target，但需要分配并持久化虚拟列 ID、在 metadata 暴露它们，并使其与嵌套 Arrow 字段保持同步。这会将实现细节泄露到 schema，改动比 target 抽象更大。

## 索引 JSON2 根值

对序列化 JSON 建索引，无法可预测地复用标量比较、全文或 Bloom 语义；它也无法区分路径，并会要求每个后端解析 JSON。

## 第一版支持数组

数组会引入每行多个值、重复 term 和 null 的处理，以及存在性匹配还是位置匹配等查询语义问题，实质改变建索引和过滤语义。仅对象路径的第一版可在不作这些承诺的前提下覆盖常见属性查询场景。
