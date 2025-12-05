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

The `VectorIndexer` integrates with the existing indexer lifecycle in mito2:

```rust
pub struct VectorIndexer {
    /// Column metadata
    column_id: ColumnId,
    dimensions: u32,

    /// HNSW configuration
    config: VectorIndexConfig,

    /// In-memory index being built
    index: Index,

    /// Row key counter (used as HNSW key)
    row_count: u64,

    /// Memory tracking
    memory_usage: Arc<AtomicUsize>,
}

impl Indexer for VectorIndexer {
    /// Called for each row during SST write
    fn update(&mut self, row_id: u64, value: &Value) -> Result<()> {
        let vector = match value {
            Value::Binary(bytes) => bytes_to_f32_vec(bytes)?,
            Value::Null => return Ok(()), // Skip null values
            _ => return Err(Error::InvalidVectorData),
        };

        // Validate dimension
        if vector.len() != self.dimensions as usize {
            return Err(Error::DimensionMismatch {
                expected: self.dimensions,
                actual: vector.len(),
            });
        }

        // Add to HNSW index with row_id as key
        self.index.add(row_id, &vector)?;
        self.row_count += 1;
        self.update_memory_usage();

        Ok(())
    }

    /// Serialize index to Puffin blob
    fn finish(&mut self) -> Result<Vec<u8>> {
        if self.row_count == 0 {
            return Ok(Vec::new());
        }

        let mut buffer = Vec::new();

        // Header: version + config
        buffer.extend_from_slice(&VECTOR_INDEX_VERSION.to_le_bytes());
        let config_bytes = bincode::serialize(&self.config)?;
        buffer.extend_from_slice(&(config_bytes.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&config_bytes);

        // Index data
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

#### Puffin Blob Format

```
┌─────────────────────────────────────────┐
│  Vector Index Blob                      │
├─────────────────────────────────────────┤
│  version: u32 (1)                       │
│  config_len: u32                        │
│  config: VectorIndexConfig (bincode)    │
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
    /// Index configuration
    config: VectorIndexConfig,

    /// Loaded index (lazily initialized)
    index: Option<Index>,

    /// Index data reference
    blob_reader: Arc<dyn BlobReader>,

    /// Cache for loaded indexes
    cache: Arc<VectorIndexCache>,
}

impl VectorIndexApplier {
    /// Load index from Puffin blob
    pub fn load(&mut self) -> Result<()> {
        if self.index.is_some() {
            return Ok(());
        }

        // Check cache first
        let cache_key = self.blob_reader.blob_id();
        if let Some(cached) = self.cache.get(&cache_key) {
            self.index = Some(cached);
            return Ok(());
        }

        // Read blob data
        let data = self.blob_reader.read_all()?;
        if data.is_empty() {
            return Ok(()); // No index (empty SST)
        }

        // Parse header
        let version = u32::from_le_bytes(data[0..4].try_into()?);
        if version != VECTOR_INDEX_VERSION {
            return Err(Error::UnsupportedIndexVersion(version));
        }

        let config_len = u32::from_le_bytes(data[4..8].try_into()?) as usize;
        let config: VectorIndexConfig = bincode::deserialize(&data[8..8+config_len])?;

        // Load USearch index
        let index_data = &data[8+config_len..];
        let options = IndexOptions {
            dimensions: self.dimensions as usize,
            metric: config.metric.into(),
            quantization: ScalarKind::F32,
            connectivity: config.connectivity,
            expansion_add: config.expansion_add,
            expansion_search: config.expansion_search,
            multi: false,
        };

        let index = Index::new(&options)?;
        index.load_from_buffer(index_data)?;

        // Cache the loaded index
        self.cache.insert(cache_key, index.clone());
        self.index = Some(index);

        Ok(())
    }

    /// Perform ANN search, returns row IDs sorted by distance
    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<(u64, f32)>> {
        let index = self.index.as_ref()
            .ok_or(Error::IndexNotLoaded)?;

        let matches = index.search(query, k)?;

        Ok(matches.keys.into_iter()
            .zip(matches.distances.into_iter())
            .collect())
    }
}
```

#### 4.3 Multi-SST Query Execution

When a query spans multiple SST files, each SST's index is searched independently and results are merged:

```rust
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
        let mut all_candidates: Vec<(u64, f32, SstId)> = Vec::new();

        // Search each SST's index
        for reader in &self.sst_readers {
            let applier = reader.vector_index_applier(&self.column)?;

            if let Some(mut applier) = applier {
                applier.load()?;

                // Request more candidates from each SST for better recall
                let candidates = applier.search(
                    &self.query_vector,
                    self.k * 2  // Over-fetch for merge accuracy
                )?;

                for (row_id, distance) in candidates {
                    all_candidates.push((row_id, distance, reader.sst_id()));
                }
            } else {
                // No index: fall back to brute-force for this SST
                let candidates = self.brute_force_search(reader)?;
                all_candidates.extend(candidates);
            }
        }

        // Sort by distance and take top-k
        all_candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        all_candidates.truncate(self.k);

        // Fetch actual rows by row_id
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

## Alternatives Considered

### 1. FAISS
- **Pros**: More index types (IVF, PQ), GPU support
- **Cons**: Heavier dependency, complex C++ build, less Rust-native

### 2. Annoy (Spotify)
- **Pros**: Simple, memory-mapped
- **Cons**: Slower build time, cannot add vectors after build

### 3. Hnswlib
- **Pros**: Reference HNSW implementation
- **Cons**: Less maintained, no official Rust bindings

### 4. Custom HNSW Implementation
- **Pros**: Full control, no external dependency
- **Cons**: Significant engineering effort, unlikely to match USearch performance

**Decision**: USearch provides the best balance of performance, Rust support, and maintenance.

## Future Extensions

1. **Quantization**: Support int8/binary quantization for reduced memory
2. **Filtering**: Pre-filtering with predicates before ANN search
3. **Distributed Index**: Shard vector index across datanodes
4. **Hybrid Search**: Combine vector similarity with full-text search
5. **Index Advisor**: Automatic index recommendation based on query patterns

## References

- [USearch GitHub](https://github.com/unum-cloud/usearch)
- [HNSW Paper](https://arxiv.org/abs/1603.09320)
- [GreptimeDB Index Architecture](../developer-guide/index-architecture.md)
- [Puffin Blob Format](../developer-guide/puffin-format.md)
