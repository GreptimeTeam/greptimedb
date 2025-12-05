# RFC: Vector Index with USearch (HNSW)

- Feature Name: `vector-index-usearch`
- Start Date: 2024-12-04
- RFC PR: (leave this empty)
- Issue: (leave this empty)

## Summary

Integrate USearch library to enable Approximate Nearest Neighbor (ANN) search for vector columns in GreptimeDB, replacing the current O(N) brute-force approach with O(log N) HNSW-based indexing.

## Motivation

The current vector search implementation uses [nalgebra](https://docs.rs/nalgebra/latest/nalgebra/) for brute-force distance calculation. While simple and accurate, this approach has O(N) complexity, making it impractical for tables with millions of rows.

Vector similarity search is fundamental to:
- Retrieval-Augmented Generation (RAG) pipelines
- Semantic search applications
- Recommendation systems
- Anomaly detection via embedding comparison

To support these use cases at scale, GreptimeDB needs an efficient vector index structure.

### Goals

1. **Performance**: Achieve sub-linear search complexity through HNSW indexing
2. **Transparency**: Automatically optimize eligible queries without SQL changes
3. **Compatibility**: Preserve existing vector functions as fallback
4. **Persistence**: Store indexes in SST files via Puffin blob format
5. **Configurability**: Expose HNSW parameters for tuning recall vs. performance

### Non-Goals

1. Exact nearest neighbor search (covered by existing brute-force)
2. GPU-accelerated vector operations
3. Distributed vector index across datanodes
4. Real-time index updates (follows SST lifecycle)

## Why USearch

We choose [USearch](https://github.com/unum-cloud/usearch) for the following reasons:

1. **Official Rust bindings**: USearch provides first-class Rust support via the `usearch` crate
2. **Production-proven**: Used by [DuckDB](https://duckdb.org/docs/extensions/vss.html) and [ClickHouse](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/annindexes) for vector search
3. **High performance**: HNSW algorithm with SIMD optimization (AVX-512, NEON)
4. **Flexible persistence**: `save_to_buffer`/`load_from_buffer` API fits our Puffin blob storage
5. **Apache 2.0 license**: Compatible with GreptimeDB

Alternatives like FAISS lack official Rust bindings, and Hnswlib has maintenance concerns.

## Design Overview

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Query Layer                            │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           VectorSearchOptimizer (new)               │   │
│  │  Detects: ORDER BY vec_*_distance() LIMIT k         │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Storage Layer                           │
│  ┌──────────────────────┐    ┌──────────────────────────┐  │
│  │   VectorIndexer      │    │  VectorIndexApplier      │  │
│  │   (write path)       │    │  (read path)             │  │
│  └──────────────────────┘    └──────────────────────────┘  │
│                              │                              │
│                              ▼                              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              USearch Index (HNSW)                    │   │
│  │              Stored in Puffin Blob                   │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### USearch Library

| Property | Value |
|----------|-------|
| Crate | `usearch = "2.21.4"` |
| Algorithm | HNSW (Hierarchical Navigable Small World) |
| Binding | C++ via cxx |
| Metrics | Cosine, L2 squared, Inner Product, Hamming |
| Quantization | f32, f64, f16, i8 |

## Detailed Design

### 1. Index Configuration

```rust
/// Vector index configuration stored in column metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexConfig {
    /// HNSW connectivity parameter (M)
    /// Higher values improve recall but increase memory and build time
    /// Typical range: 8-64, default: 16
    pub connectivity: usize,

    /// HNSW expansion factor for index construction (efConstruction)
    /// Higher values improve index quality but increase build time
    /// Typical range: 64-512, default: 128
    pub expansion_add: usize,

    /// HNSW expansion factor for search (ef)
    /// Higher values improve recall but increase search latency
    /// Typical range: 32-256, default: 64
    pub expansion_search: usize,

    /// Distance metric (must match query function)
    pub metric: VectorMetric,

    /// Optional memory limit for index cache
    pub memory_limit_bytes: Option<usize>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum VectorMetric {
    Cosine,
    L2Squared,
    InnerProduct,
}

impl Default for VectorIndexConfig {
    fn default() -> Self {
        Self {
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
            metric: VectorMetric::Cosine,
            memory_limit_bytes: None,
        }
    }
}
```

### 2. DDL Syntax

```sql
-- Create table with vector index
CREATE TABLE embeddings (
    ts TIMESTAMP TIME INDEX,
    id STRING PRIMARY KEY,
    vec VECTOR(384) VECTOR INDEX WITH (
        type = 'hnsw',
        metric = 'cosine',
        connectivity = 16,
        expansion_add = 128,
        expansion_search = 64
    )
);

-- Add index to existing column
ALTER TABLE embeddings
ADD VECTOR INDEX idx_vec ON vec WITH (
    type = 'hnsw',
    metric = 'l2sq'
);

-- Drop vector index
ALTER TABLE embeddings DROP VECTOR INDEX idx_vec;
```

### 3. Index Building (Write Path)

The `VectorIndexer` integrates with the existing indexer lifecycle in mito2.

#### Key Design Decisions

1. **Row ID Mapping**: HNSW keys are sequential (0, 1, 2, ...) within an SST. We maintain a mapping to handle NULL values and deletions.

2. **NULL Handling**: NULL vectors are tracked in a bitmap and skipped during indexing. The HNSW key sequence remains contiguous.

3. **Memory Limits**: Index building checks memory usage and fails gracefully if limits are exceeded.

```rust
pub struct VectorIndexer {
    /// Column metadata
    column_id: ColumnId,
    dimensions: u32,

    /// HNSW configuration
    config: VectorIndexConfig,

    /// In-memory index being built
    index: Index,

    /// Sequential HNSW key (0, 1, 2, ...)
    /// Different from row_id due to NULL skipping
    next_hnsw_key: u64,

    /// Total rows processed (including NULLs)
    total_rows: u64,

    /// Bitmap tracking NULL positions (row_id -> is_null)
    /// Used during query to map HNSW results back to row offsets
    null_bitmap: RoaringBitmap,

    /// Memory tracking
    memory_usage: Arc<AtomicUsize>,

    /// Memory limit for index building
    memory_limit: usize,
}

impl VectorIndexer {
    pub fn new(
        column_id: ColumnId,
        dimensions: u32,
        config: VectorIndexConfig,
        memory_limit: usize,
    ) -> Result<Self> {
        let options = IndexOptions {
            dimensions: dimensions as usize,
            metric: config.metric.into(),
            quantization: ScalarKind::F32,
            connectivity: config.connectivity,
            expansion_add: config.expansion_add,
            expansion_search: config.expansion_search,
            multi: false,
        };
        let index = Index::new(&options)?;

        Ok(Self {
            column_id,
            dimensions,
            config,
            index,
            next_hnsw_key: 0,
            total_rows: 0,
            null_bitmap: RoaringBitmap::new(),
            memory_usage: Arc::new(AtomicUsize::new(0)),
            memory_limit,
        })
    }
}

impl Indexer for VectorIndexer {
    /// Called for each row during SST write
    fn update(&mut self, value: &Value) -> Result<()> {
        let current_row = self.total_rows;
        self.total_rows += 1;

        // Handle NULL values
        let vector = match value {
            Value::Binary(bytes) => bytes_to_f32_vec(bytes)?,
            Value::Null => {
                // Track NULL position, don't add to HNSW
                self.null_bitmap.insert(current_row as u32);
                return Ok(());
            }
            _ => return Err(Error::InvalidVectorData),
        };

        // Validate dimension
        if vector.len() != self.dimensions as usize {
            return Err(Error::DimensionMismatch {
                expected: self.dimensions,
                actual: vector.len(),
            });
        }

        // Check memory limit before adding
        let current_memory = self.index.memory_usage();
        if current_memory > self.memory_limit {
            return Err(Error::MemoryLimitExceeded {
                limit: self.memory_limit,
                current: current_memory,
            });
        }

        // Add to HNSW with sequential key
        // Store mapping: hnsw_key -> row_offset implicitly
        // hnsw_key 0 = first non-null row, etc.
        self.index.add(self.next_hnsw_key, &vector)?;
        self.next_hnsw_key += 1;

        self.memory_usage.store(current_memory, Ordering::Relaxed);
        Ok(())
    }

    /// Serialize index to Puffin blob
    fn finish(&mut self) -> Result<Vec<u8>> {
        if self.next_hnsw_key == 0 {
            return Ok(Vec::new());  // No vectors indexed
        }

        let mut buffer = Vec::new();

        // Header
        buffer.extend_from_slice(&VECTOR_INDEX_VERSION.to_le_bytes());

        // Config
        let config_bytes = bincode::serialize(&self.config)?;
        buffer.extend_from_slice(&(config_bytes.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&config_bytes);

        // Metadata: total_rows, indexed_count
        buffer.extend_from_slice(&self.total_rows.to_le_bytes());
        buffer.extend_from_slice(&self.next_hnsw_key.to_le_bytes());

        // NULL bitmap (serialized)
        let bitmap_bytes = self.null_bitmap.serialize::<roaring::Portable>();
        buffer.extend_from_slice(&(bitmap_bytes.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&bitmap_bytes);

        // HNSW index data
        self.index.save_to_buffer(&mut buffer)?;

        Ok(buffer)
    }

    fn abort(&mut self) {
        // Index dropped automatically
    }

    fn memory_usage(&self) -> usize {
        self.memory_usage.load(Ordering::Relaxed)
    }
}
```

#### Row ID Mapping Strategy

Since HNSW uses contiguous keys (0, 1, 2, ...) but SST rows may have NULLs, we need to map HNSW keys back to actual row offsets:

```rust
impl VectorIndexApplier {
    /// Convert HNSW key to SST row offset
    ///
    /// HNSW keys are contiguous (skip NULLs), row offsets include NULLs.
    /// Example: rows [V, NULL, V, V, NULL, V] -> HNSW keys [0, 1, 2, 3]
    ///          HNSW key 2 -> row offset 3
    fn hnsw_key_to_row_offset(&self, hnsw_key: u64) -> u64 {
        if self.null_bitmap.is_empty() {
            return hnsw_key;  // Fast path: no NULLs
        }

        // Count how many NULLs appear before this position
        // Binary search to find the row offset
        let mut row_offset = hnsw_key;
        let mut nulls_before = self.null_bitmap.rank(row_offset as u32);

        // Iterate until we find the correct position
        while nulls_before > 0 {
            row_offset += nulls_before as u64;
            let new_nulls = self.null_bitmap.rank(row_offset as u32);
            if new_nulls == nulls_before as u64 {
                break;
            }
            nulls_before = new_nulls as u32;
        }

        row_offset
    }
}
```

#### Puffin Blob Format

```
┌─────────────────────────────────────────┐
│  Vector Index Blob                      │
├─────────────────────────────────────────┤
│  version: u32 (1)                       │
│  config_len: u32                        │
│  config: VectorIndexConfig (bincode)    │
│  total_rows: u64                        │
│  indexed_count: u64                     │
│  null_bitmap_len: u32                   │
│  null_bitmap: [u8] (Roaring bitmap)     │
│  index_data: [u8] (USearch binary)      │
└─────────────────────────────────────────┘
```

### 4. Query Execution (Read Path)

#### 4.1 Query Pattern Detection

The `VectorSearchOptimizer` identifies queries eligible for ANN optimization:

```rust
impl PhysicalOptimizerRule for VectorSearchOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Pattern: TopK(Sort(Scan))
        // Where Sort is ORDER BY vec_*_distance(column, constant)

        let Some(topk) = plan.as_any().downcast_ref::<TopKExec>() else {
            return Ok(plan);
        };

        let sort_exprs = topk.sort_exprs();
        if sort_exprs.len() != 1 {
            return Ok(plan);
        }

        let (column, query_vector, metric) =
            match extract_vector_distance_expr(&sort_exprs[0].expr) {
                Some(info) => info,
                None => return Ok(plan),
            };

        // Check if column has vector index
        let index_meta = self.get_vector_index_meta(&column)?;
        if index_meta.is_none() {
            return Ok(plan); // Fall back to brute-force
        }

        // Verify metric compatibility
        let index_meta = index_meta.unwrap();
        if !is_metric_compatible(&metric, &index_meta.config.metric) {
            return Ok(plan);
        }

        // Replace with VectorAnnScan
        Ok(Arc::new(VectorAnnScanExec::new(
            column,
            query_vector,
            topk.fetch().unwrap_or(10),
            index_meta,
            topk.input().clone(),
        )))
    }
}

fn extract_vector_distance_expr(
    expr: &Arc<dyn PhysicalExpr>
) -> Option<(Column, Vec<f32>, VectorMetric)> {
    let func = expr.as_any().downcast_ref::<ScalarFunctionExpr>()?;

    let metric = match func.name() {
        "vec_cos_distance" => VectorMetric::Cosine,
        "vec_l2sq_distance" => VectorMetric::L2Squared,
        "vec_dot_product" => VectorMetric::InnerProduct,
        _ => return None,
    };

    // Extract column and constant vector from arguments
    let args = func.args();
    if args.len() != 2 {
        return None;
    }

    // Try both argument orders: (column, const) or (const, column)
    try_extract_column_and_vector(&args[0], &args[1], metric)
        .or_else(|| try_extract_column_and_vector(&args[1], &args[0], metric))
}
```

#### 4.2 Index Loading and Search

```rust
pub struct VectorIndexApplier {
    /// Vector dimensions for validation
    dimensions: u32,

    /// Index configuration
    config: VectorIndexConfig,

    /// Loaded index (lazily initialized)
    index: Option<Index>,

    /// NULL bitmap for row offset mapping
    null_bitmap: RoaringBitmap,

    /// Total rows in SST (including NULLs)
    total_rows: u64,

    /// Index data reference
    blob_reader: Arc<dyn BlobReader>,

    /// Cache for loaded indexes
    cache: Arc<VectorIndexCache>,
}

impl VectorIndexApplier {
    /// Load index from Puffin blob (updated format with NULL bitmap)
    pub fn load(&mut self) -> Result<()> {
        if self.index.is_some() {
            return Ok(());
        }

        // Check cache first
        let cache_key = self.blob_reader.blob_id();
        if let Some(cached) = self.cache.get(&cache_key) {
            self.index = Some(cached.index);
            self.null_bitmap = cached.null_bitmap.clone();
            return Ok(());
        }

        // Read and parse blob data (format includes null_bitmap)
        let data = self.blob_reader.read_all()?;
        // ... parse version, config, total_rows, indexed_count, null_bitmap, index_data
        // (see Puffin Blob Format section)

        Ok(())
    }

    /// Perform ANN search, returns (row_offset, distance) sorted by distance
    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<(u64, f32)>> {
        // Validate query dimension
        if query.len() != self.dimensions as usize {
            return Err(Error::DimensionMismatch {
                expected: self.dimensions as usize,
                query: query.len(),
            });
        }

        let index = self.index.as_ref().ok_or(Error::IndexNotLoaded)?;
        let matches = index.search(query, k)?;

        // Convert HNSW keys to SST row offsets using null_bitmap
        Ok(matches.keys.into_iter()
            .zip(matches.distances.into_iter())
            .map(|(hnsw_key, distance)| {
                let row_offset = self.hnsw_key_to_row_offset(hnsw_key);
                (row_offset, distance)
            })
            .collect())
    }
}
```

#### 4.3 Handling Deletions and Updates

GreptimeDB uses logical deletion (rows marked with `__op_type = DELETE`). Since HNSW indexes are immutable after SST creation, we handle deletions at query time by over-fetching and filtering:

```rust
impl VectorAnnScanExec {
    /// Search with deletion filtering
    fn search_with_deletion_filter(
        &self,
        applier: &VectorIndexApplier,
        query: &[f32],
        k: usize,
        sst_reader: &SstReader,
    ) -> Result<Vec<VectorMatch>> {
        // Over-fetch to account for potential deletions
        let overfetch_k = k * 2;
        let candidates = applier.search(query, overfetch_k)?;

        let mut valid_results = Vec::with_capacity(k);
        for (row_offset, distance) in candidates {
            // Check if row is deleted via __op_type column
            if sst_reader.is_row_deleted(row_offset)? {
                continue;
            }
            valid_results.push(VectorMatch {
                row_offset,
                distance,
                sst_id: sst_reader.sst_id(),
            });
            if valid_results.len() >= k {
                break;
            }
        }
        Ok(valid_results)
    }
}
```

#### 4.4 Multi-SST Query Execution

Each SST has its own row offset space. Results must track `(sst_id, row_offset)` pairs:

```rust
/// Represents a match from vector search
pub struct VectorMatch {
    /// Row offset within the SST (NOT global row ID)
    row_offset: u64,
    /// Distance from query vector
    distance: f32,
    /// SST identifier (required: row_offsets are per-SST)
    sst_id: SstId,
}

pub struct VectorAnnScanExec {
    column: Column,
    query_vector: Vec<f32>,
    k: usize,
    sst_readers: Vec<SstReader>,
}

impl ExecutionPlan for VectorAnnScanExec {
    fn execute(&self, partition: usize, context: Arc<TaskContext>)
        -> Result<SendableRecordBatchStream>
    {
        let mut all_candidates: Vec<VectorMatch> = Vec::new();

        // Search each SST's index independently
        for reader in &self.sst_readers {
            if let Some(mut applier) = reader.vector_index_applier(&self.column)? {
                applier.load()?;
                let candidates = self.search_with_deletion_filter(
                    &applier,
                    &self.query_vector,
                    self.k * 2,  // Over-fetch for merge accuracy
                    reader,
                )?;
                all_candidates.extend(candidates);
            } else {
                // Fallback to brute-force for SSTs without index
                all_candidates.extend(self.brute_force_search(reader)?);
            }
        }

        // Global sort by distance and take top-k
        all_candidates.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        all_candidates.truncate(self.k);

        // Fetch rows using (sst_id, row_offset) pairs
        self.fetch_rows(all_candidates, context)
    }
}
```

### 5. Compaction Handling

HNSW graphs cannot be merged incrementally. During compaction, vector indexes must be fully rebuilt:

```rust
impl CompactionTask {
    fn merge_sst_files(&self, inputs: Vec<SstReader>) -> Result<SstWriter> {
        let mut writer = SstWriter::new(self.output_path)?;

        // Check if any input has vector index
        let has_vector_index = inputs.iter()
            .any(|r| r.has_vector_index(&self.vector_column));

        if has_vector_index {
            // Create new indexer for output SST
            let indexer = VectorIndexer::new(
                self.vector_column.clone(),
                self.vector_config.clone(),
            );
            writer.set_vector_indexer(indexer);
        }

        // Iterate all rows from input SSTs
        let mut row_id = 0u64;
        for input in inputs {
            for row in input.iter()? {
                // Write row to new SST
                writer.write_row(&row)?;

                // Update vector index with new row_id mapping
                if has_vector_index {
                    if let Some(vector_value) = row.get(&self.vector_column) {
                        writer.vector_indexer_mut()
                            .update(row_id, vector_value)?;
                    }
                }
                row_id += 1;
            }
        }

        writer.finish()
    }
}
```

### 6. Memory Management

#### 6.1 Index Cache

```rust
pub struct VectorIndexCache {
    /// LRU cache: blob_id -> loaded Index
    cache: Mutex<LruCache<BlobId, Arc<Index>>>,

    /// Current memory usage
    memory_usage: AtomicUsize,

    /// Maximum memory limit
    memory_limit: usize,
}

impl VectorIndexCache {
    pub fn new(memory_limit: usize) -> Self {
        // Estimate max entries based on typical index size
        let estimated_max_entries = memory_limit / (10 * 1024 * 1024); // ~10MB per index

        Self {
            cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(estimated_max_entries.max(16)).unwrap()
            )),
            memory_usage: AtomicUsize::new(0),
            memory_limit,
        }
    }

    pub fn get(&self, key: &BlobId) -> Option<Arc<Index>> {
        self.cache.lock().get(key).cloned()
    }

    pub fn insert(&self, key: BlobId, index: Index) {
        let index_size = index.memory_usage();
        let index = Arc::new(index);

        let mut cache = self.cache.lock();

        // Evict if necessary
        while self.memory_usage.load(Ordering::Relaxed) + index_size > self.memory_limit {
            if let Some((_, evicted)) = cache.pop_lru() {
                self.memory_usage.fetch_sub(
                    evicted.memory_usage(),
                    Ordering::Relaxed
                );
            } else {
                break;
            }
        }

        cache.put(key, index);
        self.memory_usage.fetch_add(index_size, Ordering::Relaxed);
    }
}
```

#### 6.2 Memory-Mapped Loading (Future Enhancement)

For very large indexes, memory-mapped loading can reduce memory pressure:

```rust
impl VectorIndexApplier {
    /// Load index using memory-mapped file (future enhancement)
    pub fn load_mmap(&mut self, path: &Path) -> Result<()> {
        let index = Index::new(&self.options)?;

        // USearch supports view_from_file for memory-mapped access
        // This loads the index structure but keeps vectors on disk
        unsafe {
            index.view_from_file(path)?;
        }

        self.index = Some(index);
        Ok(())
    }
}
```

### 7. Existing Vector Functions

The existing vector functions remain unchanged and serve as fallback:

| Function | Purpose | Index Relationship |
|----------|---------|-------------------|
| `parse_vec` | String → Binary | Data ingestion |
| `vec_to_string` | Binary → String | Data display |
| `vec_cos_distance` | Cosine distance | **Optimizer trigger** / Fallback |
| `vec_l2sq_distance` | L2 squared distance | **Optimizer trigger** / Fallback |
| `vec_dot_product` | Inner product | **Optimizer trigger** / Fallback |
| `vec_add/sub/mul/div` | Arithmetic | Independent |
| `vec_norm/dim/kth_elem` | Utilities | Independent |
| `scalar_add/mul` | Scalar ops | Independent |
| `elem_sum/product/avg` | Aggregation | Independent |

The distance functions serve dual purposes:
1. **Optimizer trigger**: Query patterns like `ORDER BY vec_cos_distance(col, query) LIMIT k` are detected and rewritten to use ANN scan
2. **Brute-force fallback**: When no index exists or query is ineligible, the original nalgebra implementation executes

## Implementation Plan

### Phase 1: Core Infrastructure
- [ ] Add `usearch` dependency to `src/mito2/Cargo.toml`
- [ ] Implement `VectorIndexConfig` in `src/mito2/src/sst/index/vector/`
- [ ] Implement `VectorIndexer` for write path
- [ ] Add Puffin blob integration for vector index storage

### Phase 2: Query Path
- [ ] Implement `VectorIndexApplier` for read path
- [ ] Implement `VectorIndexCache` with LRU eviction
- [ ] Add `VectorSearchOptimizer` physical optimizer rule
- [ ] Implement `VectorAnnScanExec` execution plan

### Phase 3: Compaction & DDL
- [ ] Update compaction to rebuild vector indexes
- [ ] Add DDL parser support for `VECTOR INDEX`
- [ ] Add `ALTER TABLE ADD/DROP VECTOR INDEX`

### Phase 4: Testing & Documentation
- [ ] Unit tests for indexer and applier
- [ ] Integration tests for end-to-end queries
- [ ] Benchmark suite comparing brute-force vs. HNSW
- [ ] User documentation

## Files to Modify

| Path | Change |
|------|--------|
| `src/mito2/Cargo.toml` | Add `usearch = "2.21.4"` |
| `src/mito2/src/sst/index/mod.rs` | Add `vector` module |
| `src/mito2/src/sst/index/vector/mod.rs` | New: VectorIndexer, VectorIndexApplier |
| `src/mito2/src/sst/index/vector/config.rs` | New: VectorIndexConfig |
| `src/mito2/src/sst/index/vector/cache.rs` | New: VectorIndexCache |
| `src/mito2/src/sst/parquet/writer.rs` | Integrate VectorIndexer |
| `src/mito2/src/sst/parquet/reader.rs` | Load vector index from Puffin |
| `src/mito2/src/compaction/` | Rebuild vector index during compaction |
| `src/query/src/optimizer/` | Add VectorSearchOptimizer |
| `src/query/src/physical_plan/` | Add VectorAnnScanExec |
| `src/sql/src/parsers/` | Parse VECTOR INDEX DDL |
| `src/common/function/src/scalars/vector/` | No changes (fallback preserved) |

## Distributed Vector Index

This section describes how vector index searching works in distributed mode, where data is partitioned across multiple regions on different datanodes.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Frontend                                       │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                  VectorSearchOptimizer                             │  │
│  │  Detects: ORDER BY vec_distance(col, query) LIMIT k               │  │
│  │  Rewrites to: DistributedVectorAnnPlan                            │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                  │                                       │
│                                  ▼                                       │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                     MergeScanExec                                  │  │
│  │  Distributes VectorAnnScan to regions                             │  │
│  │  Merges top-k results from all datanodes                          │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
        │                    │                    │
        ▼                    ▼                    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  Datanode 1  │    │  Datanode 2  │    │  Datanode 3  │
│  ┌────────┐  │    │  ┌────────┐  │    │  ┌────────┐  │
│  │Region 1│  │    │  │Region 2│  │    │  │Region 3│  │
│  │        │  │    │  │        │  │    │  │        │  │
│  │ HNSW   │  │    │  │ HNSW   │  │    │  │ HNSW   │  │
│  │ Index  │  │    │  │ Index  │  │    │  │ Index  │  │
│  └────────┘  │    │  └────────┘  │    │  └────────┘  │
│      │       │    │      │       │    │      │       │
│      ▼       │    │      ▼       │    │      ▼       │
│  Top-k local │    │  Top-k local │    │  Top-k local │
└──────────────┘    └──────────────┘    └──────────────┘
        │                    │                    │
        └────────────────────┼────────────────────┘
                             ▼
                    ┌─────────────────┐
                    │  Global Top-k   │
                    │  Merge at       │
                    │  Frontend       │
                    └─────────────────┘
```

### Distributed Query Flow

#### Step 1: Query Detection and Planning

The `VectorSearchOptimizer` at the frontend detects vector search patterns and creates a distributed plan:

```rust
impl PhysicalOptimizerRule for VectorSearchOptimizer {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, ...) -> Result<Arc<dyn ExecutionPlan>> {
        // Detect: ORDER BY vec_*_distance(col, query) LIMIT k
        let (column, query_vector, metric, k) = extract_vector_search_pattern(&plan)?;

        // Check if column has vector index
        let index_meta = self.get_vector_index_meta(&column)?;

        // Create distributed vector search plan
        // This will be wrapped in MergeScanExec for distribution
        Ok(Arc::new(DistributedVectorAnnPlan::new(
            column,
            query_vector,
            metric,
            k,
            index_meta,
        )))
    }
}
```

#### Step 2: Plan Distribution via MergeScan

The distributed plan integrates with GreptimeDB's existing `MergeScanExec`:

```rust
/// Distributed vector ANN scan that executes on each region
pub struct VectorAnnRegionPlan {
    /// Column containing vectors
    column: Column,
    /// Query vector for similarity search
    query_vector: Vec<f32>,
    /// Distance metric
    metric: VectorMetric,
    /// Number of results to return per region
    /// Over-fetch to improve global recall
    local_k: usize,
    /// Index configuration
    index_config: VectorIndexConfig,
}

impl VectorAnnRegionPlan {
    /// Calculate local_k based on global k and region count
    /// Over-fetching improves recall when merging results
    pub fn calculate_local_k(global_k: usize, region_count: usize) -> usize {
        // Heuristic: fetch more from each region to ensure good global recall
        // For small k: fetch k * multiplier
        // For large k: fetch k + buffer
        let multiplier = match region_count {
            1 => 1,
            2..=4 => 2,
            5..=10 => 3,
            _ => 4,
        };
        (global_k * multiplier).max(global_k + 10)
    }
}
```

#### Step 3: Datanode Local Execution

Each datanode executes the vector search on its local regions:

```rust
impl RegionServer {
    async fn handle_vector_ann_query(
        &self,
        request: VectorAnnRequest,
    ) -> Result<VectorAnnResponse> {
        let region = self.get_region(request.region_id)?;
        let mut all_candidates = Vec::new();

        // Search each SST's vector index in this region
        for sst in region.sst_files() {
            if let Some(applier) = sst.vector_index_applier(&request.column)? {
                applier.load()?;

                // Local ANN search
                let matches = applier.search(&request.query_vector, request.local_k)?;

                for (row_id, distance) in matches {
                    all_candidates.push(VectorMatch {
                        row_id,
                        distance,
                        sst_id: sst.id(),
                    });
                }
            } else {
                // Fallback: brute-force for SSTs without index
                let matches = self.brute_force_search(sst, &request)?;
                all_candidates.extend(matches);
            }
        }

        // Sort and return top local_k
        all_candidates.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        all_candidates.truncate(request.local_k);

        // Fetch actual row data for candidates
        let rows = self.fetch_rows(&all_candidates)?;

        Ok(VectorAnnResponse { rows, distances: all_candidates })
    }
}
```

#### Step 4: Global Merge at Frontend

The frontend merges results from all regions:

```rust
pub struct VectorAnnMergeExec {
    /// Results from all regions
    region_streams: Vec<SendableRecordBatchStream>,
    /// Global k (final result count)
    k: usize,
    /// Distance column index for sorting
    distance_col_idx: usize,
}

impl ExecutionPlan for VectorAnnMergeExec {
    fn execute(&self, partition: usize, context: Arc<TaskContext>)
        -> Result<SendableRecordBatchStream>
    {
        // Collect all results from regions
        let mut all_results: Vec<(RecordBatch, f32)> = Vec::new();

        for stream in &self.region_streams {
            while let Some(batch) = stream.next().await? {
                let distances = batch.column(self.distance_col_idx)
                    .as_any().downcast_ref::<Float32Array>()?;

                for (i, distance) in distances.iter().enumerate() {
                    if let Some(d) = distance {
                        all_results.push((batch.slice(i, 1), d));
                    }
                }
            }
        }

        // Global sort by distance
        all_results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        // Take global top-k
        all_results.truncate(self.k);

        // Combine into final batch
        let final_batch = concat_batches(&all_results)?;
        Ok(Box::pin(MemoryStream::new(vec![final_batch])))
    }
}
```

### Recall Optimization Strategies

Distributed ANN search faces a recall challenge: each region returns its local top-k, but the global top-k may require results ranked lower in individual regions.

#### Strategy 1: Over-fetching

Fetch more candidates from each region than the final k:

```rust
pub struct OverfetchConfig {
    /// Multiplier for local k (local_k = global_k * multiplier)
    pub multiplier: f32,
    /// Minimum over-fetch count
    pub min_overfetch: usize,
    /// Maximum over-fetch count (to limit network traffic)
    pub max_overfetch: usize,
}

impl Default for OverfetchConfig {
    fn default() -> Self {
        Self {
            multiplier: 2.0,
            min_overfetch: 10,
            max_overfetch: 1000,
        }
    }
}

fn calculate_local_k(global_k: usize, region_count: usize, config: &OverfetchConfig) -> usize {
    let base = (global_k as f32 * config.multiplier) as usize;
    base.clamp(global_k + config.min_overfetch, config.max_overfetch)
}
```

#### Strategy 2: Adaptive Expansion

If initial results have poor distance distribution, expand search:

```rust
pub struct AdaptiveVectorSearch {
    /// Initial local k
    initial_k: usize,
    /// Maximum expansion rounds
    max_rounds: usize,
    /// Distance threshold for expansion trigger
    expansion_threshold: f32,
}

impl AdaptiveVectorSearch {
    async fn search(&self, regions: &[RegionId], query: &[f32], global_k: usize)
        -> Result<Vec<VectorMatch>>
    {
        let mut local_k = self.initial_k;
        let mut results = Vec::new();

        for round in 0..self.max_rounds {
            results = self.search_regions(regions, query, local_k).await?;

            if results.len() >= global_k {
                // Check distance distribution
                let distances: Vec<f32> = results.iter().map(|r| r.distance).collect();
                let variance = calculate_variance(&distances[..global_k]);

                // If distances are tightly clustered, likely good recall
                if variance < self.expansion_threshold {
                    break;
                }
            }

            // Expand search
            local_k *= 2;
        }

        results.truncate(global_k);
        Ok(results)
    }
}
```

#### Strategy 3: Distance-based Filtering

Use the k-th distance from first round as a filter for subsequent fetches:

```rust
pub struct DistanceFilteredSearch {
    global_k: usize,
}

impl DistanceFilteredSearch {
    async fn search(&self, regions: &[RegionId], query: &[f32]) -> Result<Vec<VectorMatch>> {
        // Round 1: Get initial candidates with small k
        let initial_k = self.global_k * 2;
        let mut results = self.search_all_regions(regions, query, initial_k).await?;
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());

        if results.len() < self.global_k {
            return Ok(results);
        }

        // Get distance threshold (k-th best distance)
        let threshold = results[self.global_k - 1].distance;

        // Round 2: Fetch all results within threshold from each region
        // This catches candidates that were ranked > initial_k locally
        // but are within the global top-k distance
        let additional = self.search_with_threshold(regions, query, threshold).await?;

        results.extend(additional);
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        results.dedup_by(|a, b| a.row_id == b.row_id);
        results.truncate(self.global_k);

        Ok(results)
    }
}
```

### Region Pruning for Vector Search

Leverage partition metadata to skip irrelevant regions:

```rust
pub struct VectorSearchRegionPruner {
    partition_manager: PartitionRuleManagerRef,
}

impl VectorSearchRegionPruner {
    /// Prune regions based on query predicates
    /// Vector search often has time-based or category-based filters
    async fn prune_regions(
        &self,
        table_id: TableId,
        predicates: &[Expr],
        all_regions: &[RegionId],
    ) -> Result<Vec<RegionId>> {
        // Extract partition-relevant predicates
        let partition_exprs = PredicateExtractor::extract_partition_expressions(
            predicates,
            &self.get_partition_columns(table_id).await?,
        )?;

        if partition_exprs.is_empty() {
            // No pruning possible, search all regions
            return Ok(all_regions.to_vec());
        }

        // Use existing constraint pruner
        let partitions = self.partition_manager
            .find_table_partitions(table_id)
            .await?;

        ConstraintPruner::prune_regions(&partition_exprs, &partitions)
    }
}
```

### Combining with Pre-filters

Vector search often includes scalar predicates (e.g., `WHERE category = 'A'`):

```rust
/// Execution strategy for filtered vector search
pub enum FilteredVectorStrategy {
    /// Filter first, then vector search on filtered rows
    /// Best when filter is highly selective
    PreFilter,

    /// Vector search first, then apply filter
    /// Best when filter has low selectivity
    PostFilter,

    /// Hybrid: use filter to prune, then vector search
    /// Best for moderate selectivity
    Hybrid { selectivity_threshold: f32 },
}

impl FilteredVectorSearch {
    fn choose_strategy(&self, filter_selectivity: f32) -> FilteredVectorStrategy {
        match filter_selectivity {
            s if s < 0.01 => FilteredVectorStrategy::PreFilter,
            s if s > 0.5 => FilteredVectorStrategy::PostFilter,
            _ => FilteredVectorStrategy::Hybrid { selectivity_threshold: 0.1 },
        }
    }

    async fn execute_prefilter(
        &self,
        region: &Region,
        filter: &Expr,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<VectorMatch>> {
        // 1. Apply scalar filter to get candidate row IDs
        let filtered_row_ids = region.evaluate_filter(filter).await?;

        // 2. If few candidates, use brute-force
        if filtered_row_ids.len() < k * 10 {
            return self.brute_force_on_rows(region, &filtered_row_ids, query, k).await;
        }

        // 3. Otherwise, use index with row ID filter
        let applier = region.vector_index_applier()?;
        applier.search_with_filter(query, k, &filtered_row_ids)
    }
}
```

### Protocol Definition

```protobuf
// Vector ANN request sent to datanodes
message VectorAnnRequest {
    RegionRequestHeader header = 1;
    uint64 region_id = 2;
    string column_name = 3;
    repeated float query_vector = 4;
    VectorMetric metric = 5;
    uint32 local_k = 6;
    // Optional: scalar predicates to apply
    bytes filter_expr = 7;
}

message VectorAnnResponse {
    repeated VectorMatch matches = 1;
}

message VectorMatch {
    uint64 row_id = 1;
    float distance = 2;
    // Row data (all columns)
    bytes row_data = 3;
}

enum VectorMetric {
    COSINE = 0;
    L2_SQUARED = 1;
    INNER_PRODUCT = 2;
}
```

### Configuration

```rust
/// Distributed vector search configuration
pub struct DistributedVectorConfig {
    /// Over-fetch multiplier for local k calculation
    pub overfetch_multiplier: f32,

    /// Maximum candidates to fetch from each region
    pub max_local_k: usize,

    /// Enable adaptive expansion
    pub adaptive_expansion: bool,

    /// Maximum expansion rounds
    pub max_expansion_rounds: usize,

    /// Timeout for individual region queries
    pub region_timeout: Duration,

    /// Whether to use parallel region queries
    pub parallel_regions: bool,
}

impl Default for DistributedVectorConfig {
    fn default() -> Self {
        Self {
            overfetch_multiplier: 2.0,
            max_local_k: 1000,
            adaptive_expansion: false,
            max_expansion_rounds: 3,
            region_timeout: Duration::from_secs(30),
            parallel_regions: true,
        }
    }
}
```

### Implementation Plan (Additional Phases)

#### Phase 5: Distributed Vector Search
- [ ] Add `VectorAnnRequest`/`VectorAnnResponse` to region protocol
- [ ] Implement `VectorAnnRegionHandler` in datanode
- [ ] Implement `VectorAnnMergeExec` for frontend result merging
- [ ] Add distributed vector search to `MergeScanExec` integration
- [ ] Implement over-fetching strategy

#### Phase 6: Advanced Distributed Features
- [ ] Implement adaptive expansion for recall improvement
- [ ] Add region pruning for vector search with predicates
- [ ] Implement pre-filter/post-filter strategy selection
- [ ] Add distributed vector search metrics and monitoring

### Files to Modify (Additional)

| Path | Change |
|------|--------|
| `src/api/src/region.proto` | Add VectorAnnRequest/Response messages |
| `src/datanode/src/region_server.rs` | Handle vector ANN requests |
| `src/query/src/dist_plan/merge_scan.rs` | Integrate vector search distribution |
| `src/query/src/dist_plan/vector_merge.rs` | New: VectorAnnMergeExec |
| `src/frontend/src/instance/region_query.rs` | Route vector ANN requests |

## Future Extensions

1. **Quantization**: Support int8/binary quantization for reduced memory
2. **Filtering**: Pre-filtering with predicates before ANN search
3. **Hybrid Search**: Combine vector similarity with full-text search
4. **Index Advisor**: Automatic index recommendation based on query patterns
5. **Cross-Region Index**: Global HNSW index spanning multiple regions (research)

## References

- [USearch GitHub](https://github.com/unum-cloud/usearch)
- [HNSW Paper](https://arxiv.org/abs/1603.09320)
- [GreptimeDB Index Architecture](../developer-guide/index-architecture.md)
- [Puffin Blob Format](../developer-guide/puffin-format.md)
