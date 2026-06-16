# Mito Scanner Explain Metrics

This document describes the scanner metrics shown by `EXPLAIN ANALYZE` and
`EXPLAIN ANALYZE VERBOSE` for the mito region engine.

The output is intended for developers and operators debugging the read path. The
verbose scanner metrics are debug-formatted JSON-like text, not a stable public
API. Optional zero-valued fields are usually omitted.

## Keep This Document In Sync

Update this document when changing scanner explain output or metrics in these
areas:

- Scanner display output in `seq_scan.rs`, `series_scan.rs`, or
  `unordered_scan.rs`.
- `ScanMetricsSet`, `PartitionMetrics`, or `FileScanMetrics` formatting in
  `read/scan_util.rs`.
- Reader, filter, fetch, metadata cache, or index apply metrics that are merged
  into scanner explain output.

## Normal Mode

`EXPLAIN ANALYZE` prints the scanner display line and the aggregate DataFusion
execution metrics for the scanner plan node.

The scanner display line includes:

- `partition_count`: the number of partition ranges and the number of memtable,
  SST file, and extension ranges considered by the scanner.
- `selector`: present when a series row selector is attached.
- `distribution`: present when distribution information is attached.

Normal mode does not print `metrics_per_partition`, file metadata, projection,
or filter details from the scanner display.

DataFusion's `metrics=[...]` section aggregates partition metrics reported by
`PartitionMetrics`. Common fields include:

| Metric | Meaning |
| --- | --- |
| `output_rows` | Rows returned by the plan node. |
| `elapsed_compute` | Aggregate scanner compute time reported to DataFusion. |
| `build_parts_cost` | Time spent building SST file ranges. |
| `build_reader_cost` | Time spent building readers or merge readers. |
| `convert_cost` | Time spent converting mito batches into Arrow record batches. |
| `scan_cost` | Time spent scanning data from scanner inputs. |
| `yield_cost` | Time spent waiting while yielding batches downstream. |

Example with synthetic values:

```text
SeqScan: region=0(1, 0), "partition_count":{"count":2, "mem_ranges":1, "files":1, "file_ranges":1} metrics=[output_rows: 128, elapsed_compute: 12ms, build_parts_cost: 1.2ms, build_reader_cost: 2.1ms, convert_cost: 300us, scan_cost: 8.4ms, yield_cost: 900us, ]
```

## Verbose Mode

`EXPLAIN ANALYZE VERBOSE` prints the same aggregate DataFusion metrics and adds
scanner details to the display line:

- `projection`: output column names.
- `filters`: static physical predicate expressions.
- `dyn_filters`: dynamic filter expressions.
- `vector_index_k`: vector index limit when the vector index feature is enabled
  and the scan uses it.
- `files`: SST file metadata, including file id, time range, row count, file
  size, and index size.
- `metrics_per_partition`: per-partition scanner metrics collected by
  `PartitionMetrics`.

The `metrics_per_partition` value is a list of objects. Each object contains the
partition number and a `metrics` object.

Example with synthetic values:

```json
"metrics_per_partition": [
  {
    "partition": 0,
    "metrics": {
      "prepare_scan_cost": "500us",
      "build_reader_cost": "2ms",
      "scan_cost": "8ms",
      "yield_cost": "1ms",
      "total_cost": "12ms",
      "num_rows": 128,
      "num_batches": 4,
      "num_mem_ranges": 1,
      "num_file_ranges": 1,
      "build_parts_cost": "1ms",
      "sst_scan_cost": "6ms",
      "rg_total": 3,
      "rows_before_filter": 4096,
      "num_sst_record_batches": 2,
      "num_sst_batches": 2,
      "num_sst_rows": 96,
      "first_poll": "600us",
      "convert_cost": "300us",
      "rg_bloom_filtered": 1,
      "rows_bloom_filtered": 1024,
      "fetch_metrics": {
        "total_fetch_elapsed": "2ms",
        "page_cache_hit": 2,
        "cache_miss": 1
      },
      "metadata_cache_metrics": {
        "metadata_load_cost": "100us",
        "mem_cache_hit": 1
      },
      "build_ranges_peak_mem_size": 2048,
      "num_peak_range_builders": 1,
      "stream_eof": true
    }
  }
]
```

## Shared Partition Metrics

All three mito scanners use the same `PartitionMetrics` and `ScanMetricsSet`
structure for verbose scanner metrics.

### Timing And Output

| Metric | Scanner | Meaning | When present |
| --- | --- | --- | --- |
| `prepare_scan_cost` | All | Elapsed time between query start and partition metric creation. | Always. |
| `build_reader_cost` | All | Time spent building readers or merge readers. | Always. |
| `scan_cost` | All | Time spent polling scanner inputs for data. | Always. |
| `yield_cost` | All | Time spent after yielding batches to downstream operators. | Always. |
| `convert_cost` | All | Time spent converting mito batches to Arrow record batches. | When conversion time is recorded. |
| `total_cost` | All | Elapsed time from query start until the partition finishes, or until the metrics object is dropped. | Always. |
| `first_poll` | All | Elapsed time from query start until the partition stream is first polled. | Always. |
| `num_rows` | All | Rows returned by this partition. | Always. |
| `num_batches` | All | Batches returned by this partition. | Always. |
| `num_distributor_rows` | SeriesScan | Rows scanned by the series distributor. | Nonzero only. |
| `num_distributor_batches` | SeriesScan | Batches scanned by the series distributor. | Nonzero only. |
| `distributor_scan_cost` | SeriesScan | Time spent by the series distributor scanning input. | Nonzero only. |
| `distributor_yield_cost` | SeriesScan | Time spent by the series distributor sending batches to partition channels. | Nonzero only. |
| `distributor_divider_cost` | SeriesScan | Time spent splitting flat batches into series batches. | Nonzero only. |

### Ranges And SST

| Metric | Scanner | Meaning | When present |
| --- | --- | --- | --- |
| `num_mem_ranges` | All | Memtable ranges scanned by this partition. | Always. |
| `num_file_ranges` | All | SST file ranges scanned by this partition. | Always. |
| `build_parts_cost` | All | Time spent building SST file ranges. | Always. |
| `sst_scan_cost` | All | Time spent scanning SST readers. | Always. |
| `rg_total` | All | Row groups considered before pruning. | Always. |
| `num_sst_record_batches` | All | Arrow record batches read from SST readers. | Always. |
| `num_sst_batches` | All | Mito batches decoded from SST readers. | Always. |
| `num_sst_rows` | All | Rows decoded from SST readers. | Always. |

### Filters And Pruning

| Metric | Scanner | Meaning | When present |
| --- | --- | --- | --- |
| `rg_fulltext_filtered` | All | Row groups filtered by fulltext index. | Nonzero only. |
| `rg_inverted_filtered` | All | Row groups filtered by inverted index. | Nonzero only. |
| `rg_minmax_filtered` | All | Row groups filtered by min-max pruning. | Nonzero only. |
| `rg_bloom_filtered` | All | Row groups filtered by bloom filter index. | Nonzero only. |
| `rg_vector_filtered` | All | Row groups filtered by vector index. | Nonzero only. |
| `rows_before_filter` | All | Rows in candidate row groups before row-level filtering. | Always. |
| `rows_fulltext_filtered` | All | Rows filtered by fulltext index. | Nonzero only. |
| `rows_inverted_filtered` | All | Rows filtered by inverted index. | Nonzero only. |
| `rows_bloom_filtered` | All | Rows filtered by bloom filter index. | Nonzero only. |
| `rows_vector_filtered` | All | Rows filtered by vector index. | Nonzero only. |
| `rows_vector_selected` | All | Rows selected by vector index search. | Nonzero only. |
| `rows_precise_filtered` | All | Rows filtered by precise row-level filters. | Nonzero only. |
| `pruner_cache_hit` | All | Pruner builder cache hits while building file ranges. | Nonzero only. |
| `pruner_cache_miss` | All | Pruner builder cache misses while building file ranges. | Nonzero only. |
| `pruner_prune_cost` | All | Time spent waiting for pruners to build file ranges. | Nonzero only. |

### Index Result Caches

| Metric | Scanner | Meaning | When present |
| --- | --- | --- | --- |
| `fulltext_index_cache_hit` | All | Fulltext index result cache hits. | Nonzero only. |
| `fulltext_index_cache_miss` | All | Fulltext index result cache misses. | Nonzero only. |
| `inverted_index_cache_hit` | All | Inverted index result cache hits. | Nonzero only. |
| `inverted_index_cache_miss` | All | Inverted index result cache misses. | Nonzero only. |
| `bloom_filter_cache_hit` | All | Bloom filter index result cache hits. | Nonzero only. |
| `bloom_filter_cache_miss` | All | Bloom filter index result cache misses. | Nonzero only. |
| `minmax_cache_hit` | All | Min-max pruning cache hits. | Nonzero only. |
| `minmax_cache_miss` | All | Min-max pruning cache misses. | Nonzero only. |

### Memtables

| Metric | Scanner | Meaning | When present |
| --- | --- | --- | --- |
| `mem_scan_cost` | All | Time spent scanning memtables. | Nonzero only. |
| `mem_rows` | All | Rows read from memtables. | Nonzero only. |
| `mem_batches` | All | Batches read from memtables. | Nonzero only. |
| `mem_series` | All | Series read from time-series memtables. | Nonzero only. |
| `mem_prefilter_cost` | All | Time spent applying memtable prefilters. | Nonzero only. |
| `mem_prefilter_rows_filtered` | All | Rows filtered by memtable prefilters. | Nonzero only. |

### SeriesScan Distributor

| Metric | Scanner | Meaning | When present |
| --- | --- | --- | --- |
| `num_series_send_timeout` | SeriesScan | Times the distributor timed out sending to a partition channel. | Nonzero only. |
| `num_series_send_full` | SeriesScan | Times a non-blocking send found a full partition channel. | Nonzero only. |

### Nested Metrics

| Metric | Scanner | Meaning | When present |
| --- | --- | --- | --- |
| `fetch_metrics` | All | Page and row-group fetch metrics from Parquet readers. Common fields include `total_fetch_elapsed`, cache hit and miss counters, page counts, byte counts, store or write-cache fetch elapsed time, `prefilter_cost`, and `prefilter_filtered_rows`. | When fetch or prefilter work is recorded. |
| `metadata_cache_metrics` | All | Parquet metadata cache metrics, including `metadata_load_cost`, memory/file cache hits, cache misses, read count, and bytes read. | When metadata load time is recorded. |
| `inverted_index_apply_metrics` | All | Elapsed time and cache/read metrics for inverted index appliers. | When inverted indexes are applied. |
| `bloom_filter_apply_metrics` | All | Elapsed time and cache/read metrics for bloom filter index appliers. | When bloom filter indexes are applied. |
| `fulltext_index_apply_metrics` | All | Elapsed time and cache/read metrics for fulltext index appliers. | When fulltext indexes are applied. |
| `merge_metrics` | SeqScan, SeriesScan | Merge reader metrics, including merge `scan_cost`, `init_cost`, fetch counters, and `fetch_cost`. | When merge scan cost is recorded. |
| `dedup_metrics` | SeqScan, SeriesScan | Deduplication metrics, including `dedup_cost`, `num_unselected_rows`, and `num_deleted_rows`. | When dedup cost is recorded. |
| `top_file_metrics` | All | Up to ten files with the largest accumulated `build_part_cost + build_reader_cost + scan_cost`. Each file entry may include `build_part_cost`, `num_ranges`, `num_rows`, `build_reader_cost`, and `scan_cost`. | When per-file metrics are collected in verbose mode. |

#### Nested Metric Fields

`fetch_metrics`:

| Field | Meaning | When present |
| --- | --- | --- |
| `total_fetch_elapsed` | Total elapsed time for fetching row groups. | Always when `fetch_metrics` is printed. |
| `page_cache_hit` | Page cache hits. | Nonzero only. |
| `write_cache_hit` | Write cache hits. | Nonzero only. |
| `cache_miss` | Cache misses. | Nonzero only. |
| `pages_to_fetch_mem` | Pages to fetch from memory cache. | Nonzero only. |
| `page_size_to_fetch_mem` | Bytes to fetch from memory cache. | Nonzero only. |
| `pages_to_fetch_write_cache` | Pages to fetch from write cache. | Nonzero only. |
| `page_size_to_fetch_write_cache` | Bytes to fetch from write cache. | Nonzero only. |
| `pages_to_fetch_store` | Pages to fetch from object store. | Nonzero only. |
| `page_size_to_fetch_store` | Bytes to fetch from object store. | Nonzero only. |
| `page_size_needed` | Bytes actually needed by the read. | Nonzero only. |
| `write_cache_fetch_elapsed` | Elapsed time fetching from write cache. | Nonzero only. |
| `store_fetch_elapsed` | Elapsed time fetching from object store. | Nonzero only. |
| `prefilter_cost` | Elapsed time running row-group prefilters. | Nonzero only. |
| `prefilter_filtered_rows` | Rows filtered by row-group prefilters. | Nonzero only. |

`metadata_cache_metrics`:

| Field | Meaning | When present |
| --- | --- | --- |
| `metadata_load_cost` | Time spent loading Parquet metadata. | Always when `metadata_cache_metrics` is printed. |
| `mem_cache_hit` | Parquet metadata memory cache hits. | Nonzero only. |
| `file_cache_hit` | Parquet metadata file cache hits. | Nonzero only. |
| `cache_miss` | Parquet metadata cache misses. | Nonzero only. |
| `num_reads` | Metadata read operations. | Nonzero only. |
| `bytes_read` | Metadata bytes read from storage. | Nonzero only. |

Index apply metrics:

| Parent metric | Field | Meaning | When present |
| --- | --- | --- | --- |
| `inverted_index_apply_metrics` | `apply_elapsed` | Time spent applying inverted indexes. | Always when parent is printed. |
| `inverted_index_apply_metrics` | `blob_cache_miss` | Index blob cache misses. | Nonzero only. |
| `inverted_index_apply_metrics` | `blob_read_bytes` | Bytes read for index blobs. | Nonzero only. |
| `inverted_index_apply_metrics` | `inverted_index_read_metrics` | Nested inverted index read metrics. | Always when parent is printed. |
| `bloom_filter_apply_metrics` | `apply_elapsed` | Time spent applying bloom filter indexes. | Always when parent is printed. |
| `bloom_filter_apply_metrics` | `blob_cache_miss` | Index blob cache misses. | Nonzero only. |
| `bloom_filter_apply_metrics` | `blob_read_bytes` | Bytes read for index blobs. | Nonzero only. |
| `bloom_filter_apply_metrics` | `read_metrics` | Nested bloom filter read metrics. | Always when parent is printed. |
| `fulltext_index_apply_metrics` | `apply_elapsed` | Time spent applying fulltext indexes. | Always when parent is printed. |
| `fulltext_index_apply_metrics` | `blob_cache_miss` | Fulltext index blob cache misses. | Nonzero only. |
| `fulltext_index_apply_metrics` | `dir_cache_hit` | Fulltext index directory cache hits. | Nonzero only. |
| `fulltext_index_apply_metrics` | `dir_cache_miss` | Fulltext index directory cache misses. | Nonzero only. |
| `fulltext_index_apply_metrics` | `dir_init_elapsed` | Time spent initializing fulltext index directory data. | Nonzero only. |
| `fulltext_index_apply_metrics` | `bloom_filter_read_metrics` | Nested bloom filter read metrics used by the fulltext path. | Always when parent is printed. |

Index read metrics used by `inverted_index_read_metrics`, `read_metrics`, and
`bloom_filter_read_metrics`:

| Field | Meaning | When present |
| --- | --- | --- |
| `total_bytes` | Bytes read for index data. | Nonzero only. |
| `cache_hit` | Index data cache hits. | Nonzero only. |
| `total_ranges` | Ranges read for index data. | Nonzero only. |
| `fetch_elapsed` | Elapsed time fetching index data. | Nonzero only. |
| `cache_miss` | Index data cache misses. | Nonzero only. |

`merge_metrics`:

| Field | Meaning | When present |
| --- | --- | --- |
| `scan_cost` | Total scan cost of the merge reader. | Always when `merge_metrics` is printed. |
| `init_cost` | Time spent initializing the merge reader. | Nonzero only. |
| `num_fetch_by_batches` | Number of batch-oriented fetches from sources. | Nonzero only. |
| `num_fetch_by_rows` | Number of row-oriented fetches from sources. | Nonzero only. |
| `fetch_cost` | Time spent fetching batches from sources. | Nonzero only. |

`dedup_metrics`:

| Field | Meaning | When present |
| --- | --- | --- |
| `dedup_cost` | Time spent deduplicating rows. | Always when `dedup_metrics` is printed. |
| `num_unselected_rows` | Rows removed by deduplication or delete filtering. | Nonzero only. |
| `num_deleted_rows` | Deleted rows removed during deduplication. | Nonzero only. |

Each `top_file_metrics` entry:

| Field | Meaning | When present |
| --- | --- | --- |
| `build_part_cost` | Time spent building this file's ranges or parts. | Always for each printed file entry. |
| `num_ranges` | Ranges read from this file. | Nonzero only. |
| `num_rows` | Rows read from this file. | Nonzero only. |
| `build_reader_cost` | Time spent building readers for this file. | Nonzero only. |
| `scan_cost` | Time spent scanning this file. | Nonzero only. |

### Range Cache And Lifecycle

| Metric | Scanner | Meaning | When present |
| --- | --- | --- | --- |
| `range_cache_size` | All | Bytes added to the range cache during the scan. | Nonzero only. |
| `range_cache_hit` | All | Range cache lookup hits. | Nonzero only. |
| `range_cache_miss` | All | Range cache lookup misses. | Nonzero only. |
| `build_ranges_peak_mem_size` | All | Peak memory tracked while building file ranges. | Always. |
| `num_peak_range_builders` | All | Peak number of active file range builders. | Always. |
| `stream_eof` | All | Whether the partition stream reached EOF normally. | Always. |

## Scanner Differences

`SeqScan` scans ranges while preserving the ordering required by the read path.
It may report `merge_metrics` when merge readers are used and `dedup_metrics`
when deduplication removes older versions or deleted rows.

`UnorderedScan` provides no output ordering guarantee. It uses the same
partition metric structure as `SeqScan`, but downstream plan nodes may add sort
operators when a query needs ordered output.

`SeriesScan` returns rows grouped by series. It uses the same partition metric
structure as the other scanners. It may also report distributor metrics in one
extra partition used by the series distributor.
