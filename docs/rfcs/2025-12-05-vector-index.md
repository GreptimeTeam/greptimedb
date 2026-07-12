---
Feature Name: Vector Index
Tracking Issue: TBD
Date: 2025-12-04
Author: "TBD"
---

# Summary
Introduce a per-SST approximate nearest neighbor (ANN) index for `VECTOR(dim)` columns with a pluggable engine. USearch HNSW is the initial engine, while the design keeps VSAG (default when linked) and future engines selectable at DDL or alter time and encoded in the index metadata. The index is built alongside SST creation and accelerates `ORDER BY vec_*_distance(column, <literal vector>) LIMIT k` queries, falling back to the existing brute-force path when an index is unavailable or ineligible.

# Motivation
Vector distances are currently computed with nalgebra across all rows (O(N)) before sorting, which does not scale to millions of vectors. An on-disk ANN index with sub-linear search reduces latency and compute cost for common RAG, semantic search, and recommendation workloads without changing SQL.

# Details

## Current Behavior
`VECTOR(dim)` values are stored as binary blobs. Queries call `vec_cos_distance`/`vec_l2sq_distance`/`vec_dot_product` via nalgebra for every row and then sort; there is no indexing or caching.

## Index Eligibility and Configuration
Only `VECTOR(dim)` columns can be indexed. A column metadata flag follows the existing column-option pattern with an intentionally small surface area:
- `engine`: `vsag` (default when the binding is built) or `usearch`. If a configured engine is unavailable at runtime, the builder logs and falls back to `usearch` while leaving the option intact for future rebuilds.
- `metric`: `cosine` (default), `l2sq`, or `dot`; mismatches with query functions force brute-force execution.
- `m`: HNSW graph connectivity (higher = denser graph, more memory, better recall), default `16`.
- `ef_construct`: build-time expansion, default `128`.
- `ef_search`: query-time expansion, default `64`; engines may clamp values.

Option semantics mirror HNSW defaults so both USearch and VSAG can honor them; engine-specific tunables stay in reserved key-value pairs inside the blob header for forward compatibility.

DDL reuses column extensions similar to inverted/fulltext indexes:

```sql
CREATE TABLE embeddings (
  ts TIMESTAMP TIME INDEX,
  id STRING PRIMARY KEY,
  vec VECTOR(384) VECTOR INDEX WITH (engine = 'vsag', metric = 'cosine', ef_search = 64)
);
```

Altering column options toggles the flag, can switch engines (for example `usearch` -> `vsag`), and triggers rebuilds through the existing alter/compaction flow. Engine choice stays in table metadata and each blob header; new SSTs use the configured engine while older SSTs remain readable under their recorded engine until compaction or a manual rebuild rewrites them.

## Storage and Format
- One vector index per indexed column per SST, stored as a Puffin blob with type `greptime-vector-index-v1`.
- Each blob records the engine (`usearch`, `vsag`, future values) and engine parameters in the header so readers can select the matching decoder. Mixed-engine SSTs remain readable because the engine id travels with the blob.
- USearch uses `f32` vectors and SST row offsets (`u64`) as keys; nulls and `OpType::Delete` rows are skipped. Row ids are the absolute SST ordinal so readers can derive `RowSelection` directly from parquet row group lengths without extra side tables.
- Blob layout:
  - Header: version, column id, dimension, engine id, metric, `m`, `ef_construct`, `ef_search`, and reserved engine-specific key-value pairs.
  - Counts: total rows written and indexed rows.
  - Payload: USearch binary produced by `save_to_buffer`.
- An empty index (no eligible vectors) results in no available index entry for that column.
- `puffin_manager` registers the blob type so caches and readers discover it alongside inverted/fulltext/bloom blobs in the same index file.

## Row Visibility and Duplicates
- The indexer increments `row_offset` for every incoming row (including skipped/null/delete rows) so offsets stay aligned with parquet ordering across row groups.
- Only `OpType::Put` rows with the expected dimension are inserted; `OpType::Delete` and malformed rows are skipped but still advance `row_offset`, matching the data plane’s visibility rules.
- Multiple versions of the same primary key remain in the graph; the read path intersects search hits with the standard mito2 deduplication/visibility pipeline (sequence-aware dedup, delete filtering, projection) before returning results.
- Searches overfetch beyond `k` to compensate for rows discarded by visibility checks and to avoid reissuing index reads.

## Build Path (mito2 write)
Extend `sst::index::Indexer` to optionally create a `VectorIndexer` when region metadata marks a column as vector-indexed, mirroring how inverted/fulltext/bloom filters attach to `IndexerBuilderImpl` in `mito2`.

The indexer consumes `Batch`/`RecordBatch` data and shares memory tracking and abort semantics with existing indexers:
- Maintain a running `row_offset` that follows SST write order and spans row groups so the search result can be turned into `RowSelection`.
- For each `OpType::Put`, if the vector is non-null and matches the declared dimension, insert into USearch with `row_offset` as the key; otherwise skip.
- Track memory with existing index build metrics; on failure, abort only the vector index while keeping SST writing unaffected.

Engine selection is table-driven: the builder picks the configured engine (default `vsag`, fallback `usearch` if `vsag` is not compiled in) and dispatches to the matching implementation. Unknown engines skip index build with a warning.

On `finish`, serialize the engine-tagged index into the Puffin writer and record `IndexType::Vector` metadata for the column. `IndexOutput` and `FileMeta::indexes/available_indexes` gain a vector entry so manifest updates and `RegionVersion` surface per-column presence, following patterns used by inverted/fulltext/bloom indexes. Planner/metadata validation ensures that mismatched dimensions only reduce the indexed-row count and do not break reads.

## Read Path (mito2 query)
A planner rule in `query` identifies eligible plans on mito2 tables: a single `ORDER BY vec_cos_distance|vec_l2sq_distance|vec_dot_product(<vector column>, <literal vector>)` in ascending order plus a `LIMIT`/`TopK`. The rule rejects plans with multiple sort keys, non-literal query vectors, or additional projections that would change the distance expression and falls back to brute-force in those cases.

For eligible scans, build a `VectorIndexScan` execution node that:
- Consults SST metadata for `IndexType::Vector`, loads the index via Puffin using the existing `mito2::cache::index` infrastructure, and dispatches to the engine declared in the blob header (USearch/VSAG/etc.).
- Runs the engine’s `search` with an overfetch (for example 2×k) to tolerate rows filtered by deletes, dimension mismatches, or late-stage dedup; keys already match SST row offsets produced by the writer.
- Converts hits to `RowSelection` using parquet row group lengths and reuses the parquet reader so visibility, projection, and deduplication logic stay unchanged; distances are recomputed with `vec_*_distance` before the final trim to k to guarantee ordering and to merge distributed partial results deterministically.

Any unsupported shape, load error, or cache miss falls back to the current brute-force execution path.

## Lifecycle and Maintenance
Lifecycle piggybacks on the existing SST/index flow: rebuilds run where other secondary indexes do, graphs are always rebuilt from source rows (no HNSW merge), and cleanup/versioning/caching reuse the existing Puffin and index cache paths.

# Implementation Plan
1. Add the `usearch` dependency (wrapper module in `index` or `mito2`) and map minimal HNSW options; keep an engine trait that allows plugging VSAG without changing the rest of the pipeline.
2. Introduce `IndexType::Vector` and a column metadata key for vector index options (including `engine`); add SQL parser and `SHOW CREATE TABLE` support for `VECTOR INDEX WITH (...)`.
3. Implement `vector_index` build/read modules under `mito2` (and `index` if shared), including Puffin serialization that records engine id, blob-type registration with `puffin_manager`, and integration with the `Indexer` builder, `IndexOutput`, manifest updates, and compaction rebuild.
4. Extend the query planner/execution to detect eligible plans and drive a `RowSelection`-based ANN scan with a fallback path, dispatching by engine at read time and using existing Puffin and index caches.
5. Add unit tests for serialization/search correctness and an end-to-end test covering plan rewrite, cache usage, engine selection, and fallback; add a mixed-engine test to confirm old USearch blobs still serve after a VSAG switch.
6. Follow up with an optional VSAG engine binding (feature flag), validate parity with USearch on dense vectors, exercise alternative algorithms (for example PQ), and flip the default `engine` to `vsag` when the binding is present.

# Alternatives
- **VSAG (follow-up engine):** C++ library with HNSW and additional algorithms (for example SINDI for sparse vectors and PQ) targeting in-memory and disk-friendly search. Provides parameter generators and a roadmap for GPU-assisted build and graph compression. Compared to FAISS it is newer with fewer integrations but bundles sparse/dense coverage and out-of-core focus in one engine. Fits the pluggable-engine design and would become the default `engine = 'vsag'` when linked; USearch remains available for lighter dependencies.
- **FAISS:** Broad feature set (IVF/IVFPQ/PQ/HNSW, GPU acceleration, scalar filtering, pre/post filters) and battle-tested performance across datasets, but it requires a heavier C++/GPU toolchain, has no official Rust binding, and is less disk-centric than VSAG; integrating it would add more build/distribution burden than USearch/VSAG.
- **Do nothing:** Keep brute-force evaluation, which remains O(N) and unacceptable at scale.
