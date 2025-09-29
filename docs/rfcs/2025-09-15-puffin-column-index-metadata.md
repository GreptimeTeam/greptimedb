### Puffin 列索引元信息枚举设计

#### 背景
- 需要类似 `all_ssts_from_manifest` 的全局枚举能力，遍历每个 puffin 文件，收集“每列索引”的元信息。
- 不同索引类型（BloomFilter/Skipping、Inverted、Fulltext Tantivy、Fulltext Bloom）格式异构，需保留各自特有字段。
- 目标是低成本 IO：尽量只读文件元数据与小尾部元数据，避免解压和大范围读。

#### 目标
- 高效列举全部 puffin 索引，按索引目标输出类型化的元信息。
- 输出所需的完整元信息（按需读取尾部或属性）。
- 兼容缺失/旧版本文件；稳定输出。

#### 非目标
- 不执行索引应用/查询。
- 不解码索引正文数据。

---

### 数据来源与键约定
- Puffin 文件元数据 FileMetadata：列出所有 blob 的 `blob_type`、`length`、`properties`。
- Blob 键名约定：
  - Skipping BloomFilter：`"greptime-bloom-filter-v1-{col_id}"`（当前仅列 id；更通用的 target 预留）
  - Inverted Index（单 blob 多列）：`"greptime-inverted-index-v1"`
  - Fulltext Tantivy（目录）：`"greptime-fulltext-index-v1-{col_id}`（当前仅列 id；更通用的 target 预留）
  - Fulltext Bloom（粗粒度）：`"greptime-fulltext-index-bloom-{col_id}`（当前仅列 id；更通用的 target 预留）
- 类型特有元数据：
  - Bloom/Fulltext Bloom：尾部 `BloomFilterMeta`（rows_per_segment、segment_count、row_count、locs 等）
  - Inverted：尾部 `InvertedIndexMetas`（map: 列名→meta，含 offsets/sizes/bitmap_type/segment_row_count/total_row_count）
  - Fulltext Tantivy：`BlobMetadata.properties[KEY_FULLTEXT_CONFIG]`（Analyzer、case_sensitive、granularity 等）

#### 目标编码（target key）规范建议（Reserved）
当前项目仅支持 ColumnId 作为目标；以下编码规范用于预留，将来扩展时按此实现：
- 列 id：直接使用十进制列 id 字符串。
- 组合列：`colid1,colid2,...`（升序、去重）；或以 `+` 连接，经 URL-safe base64 包一层，避免分隔符冲突。
- JSON 路径：`colid#jsonptr`，其中 `jsonptr` 为 RFC 6901 JSON Pointer 的 URL-safe 编码。
- 表达式：`expr:{hash}`，`hash` 为表达式规范化字符串的稳定哈希（如 xxh3_64），规范化字符串可以放在 `properties` 中以便反查。
- 约束：整个 target key 应 URL-safe，最长不超过 256 字节。

---

### 总体方案
- 以 `Engine::all_ssts_from_manifest()` 为基线，遍历所有带 `index_file_path` 的条目，为每个 puffin 文件：
  1) 打开 `PuffinReader` 并一次性读取 `FileMetadata`（可启用元数据缓存；带 `with_file_size_hint`）。
  2) 遍历 `blobs`：
     - 以前缀匹配分类到 Skipping BF、Fulltext Tantivy、Fulltext Bloom。
     - 检测是否存在 Inverted 统一 blob。
  3) Inverted：若存在，读取一次尾部 `InvertedIndexMetas`，逐列产出。
  4) Bloom/Fulltext Bloom：为每个目标读取一次尾部 `BloomFilterMeta`（小范围随机读），并填充段元信息。
  5) Fulltext Tantivy：直接从 `properties` 读取配置并产出（0 额外 IO）。

---

### 输出数据模型
```rust
pub struct IndexFileLocator {
  pub table_dir: String,
  pub region_id: RegionId,
  pub file_id: FileId,
  pub node_id: Option<u64>,
}

pub enum IndexKind {
  BloomSkipping {
    blob_size: u64,
    rows_per_segment: Option<u32>,
    segment_count: Option<u32>,
    row_count: Option<u64>,
  },
  FulltextBloom {
    blob_size: u64,
    rows_per_segment: Option<u32>,
    segment_count: Option<u32>,
    row_count: Option<u64>,
    config_json: Option<String>,
  },
  FulltextTantivy {
    config_json: Option<String>,
  },
  Inverted {
    blob_size: u64,              // 单一 blob 的总大小
    bitmap_type: String,
    base_offset: u64,
    inverted_index_size: u32,
    relative_fst_offset: u32,
    fst_size: u32,
    relative_null_bitmap_offset: u32,
    null_bitmap_size: u32,
    segment_row_count: u32,
    total_row_count: u64,
  },
}

pub enum IndexTarget {
  ColumnId(ColumnId),            // 单列（当前仅实现此分支）
  Columns(Vec<ColumnId>),        // 组合列（预留）
  JsonPath {                     // JSON 路径（预留）
    column: ColumnId,
    path: String,                // 规范化后的 JSONPath/Pointer
  },
  Expression {                   // 表达式或派生目标（预留）
    expr: String,                // 可用逻辑表达式或规范化签名
  },
}

pub struct IndexMeta {
  pub locator: IndexFileLocator,
  pub target: IndexTarget,
  pub kind: IndexKind,
}
```

---

### 紧凑表结构（JSON 承载异构字段）
为避免列过多、同时保留异构信息，推荐默认采用“紧凑模式”：核心字段 + JSON 承载目标和变体元信息。

```rust
pub struct IndexRowCompact {
  // 定位信息
  pub table_dir: String,
  pub region_id: u64,
  pub file_id: String,
  pub node_id: Option<u64>,

  // 索引类型与目标
  pub index_type: String,      // "bloom_skipping" | "fulltext_bloom" | "fulltext_tantivy" | "inverted"
  pub target_type: String,     // "column" | "columns" | "json_path" | "expression"
  pub target_key: String,      // 稳定键（blob 后缀或 inverted meta 名称）

  // 通用存储信息
  pub blob_size: Option<u64>,

  // JSON 列（字符串或原生 JSON 皆可）
  pub target_json: Option<String>,   // { columns:[1,2], json_path:"/a/b", expr_hash:123, expr:"..." }
  pub meta_json: Option<String>,     // 见下方各变体 JSON 结构
}
```

变体 `meta_json` 建议结构：
- Bloom/Fulltext Bloom：
```json
{
  "bloom": {
    "rows_per_segment": 10240,
    "segment_count": 128,
    "row_count": 123456
  },
  "fulltext": {               // 仅 fulltext_bloom 存在
    "analyzer": "english",
    "case_sensitive": false,
    "granularity": 10240,
    "false_positive_rate": 0.01,
    "raw_config": "{...}"    // 原始配置 JSON 字符串（可选）
  }
}
```
- Fulltext Tantivy：
```json
{
  "fulltext": {
    "analyzer": "chinese",
    "case_sensitive": true,
    "raw_config": "{...}"
  }
}
```
- Inverted：
```json
{
  "inverted": {
    "bitmap_type": "Roaring",
    "base_offset": 123,
    "inverted_index_size": 456,
    "relative_fst_offset": 789,
    "fst_size": 50,
    "relative_null_bitmap_offset": 26,
    "null_bitmap_size": 26,
    "segment_row_count": 1000,
    "total_row_count": 100000
  }
}
```

实现建议：
- 列类型：若运行时/查询引擎支持 JSON 类型（如 Arrow JSON/Greptime JSON），可用原生 JSON；否则以 UTF-8 文本存放 JSON。
- Schema 演进：新增字段仅在 `meta_json/target_json` 内扩展，核心列稳定；必要时再为高频查询“实体化”少量派生列。
- 视图策略：
  - 默认提供紧凑表 `system.puffin_index_meta`（上述结构）。
  - 可选提供平铺视图 `system.puffin_index_meta`，从紧凑表按需展开（或离线物化），服务于特定查询（例如只查 `bf_segment_count`）。

与“平铺输出”的关系：
- 紧凑模式作为默认；平铺模式保留作为派生视图或导出选项。
- 两者字段含义一致，唯一区别在承载形式（列 vs JSON）。

---

### 字段对齐（与代码/Proto）
为确保实现一致性，`meta_json/target_json` 的字段命名与类型对齐现有代码/Proto：

- Bloom 与 Fulltext Bloom（`meta_json.bloom`）
  - rows_per_segment: u32 ← greptime_proto::v1::index::BloomFilterMeta.rows_per_segment
  - segment_count: u32 ← BloomFilterMeta.segment_count
  - row_count: u64 ← BloomFilterMeta.row_count

- Fulltext 配置（`meta_json.fulltext`）
  - analyzer: string ← index::fulltext_index::Config.analyzer（取值："english" | "chinese"）
  - case_sensitive: bool ← Config.case_sensitive
  - granularity: u32（仅 fulltext_bloom 有意义；来自 Fulltext Bloom 的配置）
  - false_positive_rate: f64（仅 fulltext_bloom 有意义；来自 Fulltext Bloom 的配置）
  - raw_config: string（可选）← Puffin properties[KEY_FULLTEXT_CONFIG] 的原始 JSON 字符串

- Inverted（`meta_json.inverted`）
  - bitmap_type: string ← greptime_proto::v1::index::InvertedIndexMeta.bitmap_type / index::bitmap::BitmapType（当前取值："Roaring"）
  - base_offset: u64 ← InvertedIndexMeta.base_offset
  - inverted_index_size: u32 ← InvertedIndexMeta.inverted_index_size
  - relative_fst_offset: u32 ← InvertedIndexMeta.relative_fst_offset
  - fst_size: u32 ← InvertedIndexMeta.fst_size
  - relative_null_bitmap_offset: u32 ← InvertedIndexMeta.relative_null_bitmap_offset
  - null_bitmap_size: u32 ← InvertedIndexMeta.null_bitmap_size
  - segment_row_count: u32 ← greptime_proto::v1::index::InvertedIndexMetas.segment_row_count（聚合级字段）
  - total_row_count: u64 ← InvertedIndexMetas.total_row_count（聚合级字段）
  - 说明：每个目标（target_key）一行，base_offset 等为该目标专属；segment_row_count/total_row_count 来源于 metas 顶层。

- 目标信息（`target_json`）
  - 单列/组合：{ "columns": [ColumnId, ...] }（ColumnId 为无符号整数，顺序升序、去重）
  - JSON 路径：{ "column": ColumnId, "json_path": "<RFC6901 JSON Pointer>" }
  - 表达式：{ "expr_hash": u64, "expr": "<规范化表达式>" }（expr 可选，hash 稳定用于键名生成）

- 核心字段
  - index_type: "bloom_skipping" | "fulltext_bloom" | "fulltext_tantivy" | "inverted"
  - target_type: "column" | "columns" | "json_path" | "expression"
  - target_key: 与 blob 键后缀或 inverted meta 名称一致的规范化键；生成与解析遵循“目标编码规范建议”。

---

### 枚举 API
```rust
pub async fn iterate_all_index_metas(
  engine: &Engine,
  max_file_concurrency: usize, // puffin 文件级并发
) -> impl Stream<Item = IndexMeta>;
```

---

### 关键流程
- 文件级：
  - 构建 `PuffinReader`（复用 `PuffinManagerFactory` + `RegionFilePathFactory`），传入 `index_file_size` 作为 hint；可注入 `PuffinMetadataCache`。
  - 调用 `reader.metadata()` 获取 `FileMetadata`。
- Inverted：
  - 若存在 `"greptime-inverted-index-v1"`：`reader.blob(key).reader()` → `InvertedIndexBlobReader::metadata()`；
  - 解析 `metas.metas` 中每个 `name`（约定为 target 的规范化名称，如列 id 字符串、组合/表达式编码）→ `IndexTarget`，逐目标生成 `IndexMeta::Inverted`。
- Bloom/Fulltext Bloom：
  - `reader.blob(target_key).reader()` → `BloomFilterReaderImpl::metadata()` 填充 rows/segments 信息，并由 key 后缀解析 `IndexTarget`。
- Fulltext Tantivy：
  - 从 `BlobMetadata.properties.get(KEY_FULLTEXT_CONFIG)` 读取配置 JSON，填入 `IndexMeta::FulltextTantivy`，并由 key 后缀解析 `IndexTarget`。

---

### 并发与缓存
- 并发：
  - 文件级并发（例如 16/32）。
- 缓存：
  - 启用 `PuffinMetadataCache`

### TODO
- 目标与键编码
  - 提供 `encode_target_key(IndexTarget) -> String` 与 `decode_target_key(&str) -> IndexTarget`，实现并单测 target_key 规范（列、组合、JSON 路径、表达式）。
  - 约束校验：长度上限、URL-safe 校验、组合列去重与排序。
- 写端补齐与对齐
  - Fulltext/Tantivy 写入：`properties[KEY_FULLTEXT_CONFIG]` 一律落原始 JSON，并保证兼容旧文件缺失此属性的读取路径。
  - Fulltext Bloom 写入：同上，并确保 granularity/false_positive_rate 出现在配置 JSON 中。
  - Bloom/Fulltext Bloom：保持 blob 键名 `{prefix}-{target_key}` 规则，列 id 过渡到通用 `target_key`。
  - Inverted：`InvertedIndexMetas.metas` 的名称统一使用 `target_key`，从列 id 字符串迁移到规范名（兼容读取旧名称）。
- 枚举实现
  - 遍历文件：复用 `Engine::all_ssts_from_manifest()`；建立 `PuffinReader`（带 `with_file_size_hint` 与 `PuffinMetadataCache`）。
  - Inverted：单次 footer 读取 → 逐 `target_key` 生成行。
  - Bloom/Fulltext Bloom：逐 `target_key` 读取 `BloomFilterMeta` → 填充段信息与 `blob_size`。
  - Fulltext Tantivy：从 properties 解析配置 → 生成行；目录型 blob 的 `blob_size` 可为空。
- JSON Schema 与版本
  - 为 `meta_json/target_json` 定义 schema 版本号（如 `meta_schema_version`），放入 `meta_json` 顶层，便于未来演进。
  - 提供 schema 校验与降级逻辑（缺字段按空处理）。
- 系统表/视图
  - 创建 `system.puffin_index_meta`（JSON 主承载）DDL 与注册逻辑。
  - 可选提供平铺只读视图，映射常用字段（如 `bloom.segment_count`）。
- 指标与日志
  - 指标：枚举耗时、文件/目标计数、每类型计数、读取字节与次数、失败数。
  - 日志：按文件聚合错误（如 blob 不存在、元数据解析失败），避免刷屏。
- 错误处理与降级
  - blob 不存在：跳过目标并计数；文件损坏：记录并继续下一个文件。
  - 配置缺失：`meta_json.fulltext` 可为空；字段缺失置空。
- 并发与缓存
  - 参数化并发度（文件级），合理默认值（如 16/32）。
  - 启用 `PuffinMetadataCache`，必要时增加 LRU 上限配置。
- 兼容性与回填
  - 旧文件：列 id 作为 `target_key`；无 `properties` 的 fulltext 仅输出 analyzer/case_sensitive 默认推断或置空。
  - 升级策略：新旧名称兼容期内同时支持两种 `target_key` 解析。
- 安全与可见性
  - 系统表权限与租户隔离；表目录脱敏策略（如必要）。
- 测试
  - 单测：四类型最小/组合/缺失属性/损坏文件；目标编码往返；JSON schema 校验。
  - 集成：大表多文件并发遍历；缓存命中；系统表查询覆盖。
  - 基准：不同并发度、不同对象存储延迟下的枚举耗时与字节数。