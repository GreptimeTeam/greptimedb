// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use lazy_static::lazy_static;
use prometheus::*;

/// Stage label.
pub const STAGE_LABEL: &str = "stage";
/// Type label.
pub const TYPE_LABEL: &str = "type";
const CACHE_EVICTION_CAUSE: &str = "cause";
/// Reason to flush.
pub const FLUSH_REASON: &str = "reason";
/// File type label.
pub const FILE_TYPE_LABEL: &str = "file_type";
/// Region worker id label.
pub const WORKER_LABEL: &str = "worker";

lazy_static! {
    /// Global write buffer size in bytes.
    pub static ref WRITE_BUFFER_BYTES: IntGauge =
        register_int_gauge!("greptime_mito_write_buffer_bytes", "mito write buffer bytes").unwrap();
    /// Global memtable dictionary size in bytes.
    pub static ref MEMTABLE_DICT_BYTES: IntGauge =
        register_int_gauge!("greptime_mito_memtable_dict_bytes", "mito memtable dictionary size in bytes").unwrap();
    /// Gauge for open regions in each worker.
    pub static ref REGION_COUNT: IntGaugeVec =
        register_int_gauge_vec!(
            "greptime_mito_region_count",
            "mito region count in each worker",
            &[WORKER_LABEL],
        ).unwrap();
    /// Elapsed time to handle requests.
    pub static ref HANDLE_REQUEST_ELAPSED: HistogramVec = register_histogram_vec!(
            "greptime_mito_handle_request_elapsed",
            "mito handle request elapsed",
            &[TYPE_LABEL],
            vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 60.0, 300.0]
        )
        .unwrap();

    // ------ Flush related metrics
    /// Counter of scheduled flush requests.
    /// Note that the flush scheduler may merge some flush requests.
    pub static ref FLUSH_REQUESTS_TOTAL: IntCounterVec = register_int_counter_vec!(
            "greptime_mito_flush_requests_total",
            "mito flush requests total",
            &[FLUSH_REASON]
        )
        .unwrap();
    /// Counter of scheduled failed flush jobs.
    pub static ref FLUSH_ERRORS_TOTAL: IntCounter =
        register_int_counter!("greptime_mito_flush_errors_total", "mito flush errors total").unwrap();
    /// Elapsed time of a flush job.
    pub static ref FLUSH_ELAPSED: HistogramVec = register_histogram_vec!(
            "greptime_mito_flush_elapsed",
            "mito flush elapsed",
            &[TYPE_LABEL],
            vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
        )
        .unwrap();
    /// Histogram of flushed bytes.
    pub static ref FLUSH_BYTES_TOTAL: IntCounter =
        register_int_counter!("greptime_mito_flush_bytes_total", "mito flush bytes total").unwrap();
    // ------ End of flush related metrics


    // ------ Write related metrics
    /// Number of stalled write requests in each worker.
    pub static ref WRITE_STALL_TOTAL: IntGaugeVec = register_int_gauge_vec!(
            "greptime_mito_write_stall_total",
            "mito stalled write request in each worker",
            &[WORKER_LABEL]
        ).unwrap();
    /// Counter of rejected write requests.
    pub static ref WRITE_REJECT_TOTAL: IntCounter =
        register_int_counter!("greptime_mito_write_reject_total", "mito write reject total").unwrap();
    /// Elapsed time of each write stage.
    pub static ref WRITE_STAGE_ELAPSED: HistogramVec = register_histogram_vec!(
            "greptime_mito_write_stage_elapsed",
            "mito write stage elapsed",
            &[STAGE_LABEL],
            vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
        )
        .unwrap();
    /// Counter of rows to write.
    pub static ref WRITE_ROWS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "greptime_mito_write_rows_total",
        "mito write rows total",
        &[TYPE_LABEL]
    )
    .unwrap();
    // ------ End of write related metrics


    // Compaction metrics
    /// Timer of different stages in compaction.
    pub static ref COMPACTION_STAGE_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_mito_compaction_stage_elapsed",
        "mito compaction stage elapsed",
        &[STAGE_LABEL],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
    /// Timer of whole compaction task.
    pub static ref COMPACTION_ELAPSED_TOTAL: Histogram =
        register_histogram!("greptime_mito_compaction_total_elapsed", "mito compaction total elapsed").unwrap();
    /// Counter of all requested compaction task.
    pub static ref COMPACTION_REQUEST_COUNT: IntCounter =
        register_int_counter!("greptime_mito_compaction_requests_total", "mito compaction requests total").unwrap();
    /// Counter of failed compaction task.
    pub static ref COMPACTION_FAILURE_COUNT: IntCounter =
        register_int_counter!("greptime_mito_compaction_failure_total", "mito compaction failure total").unwrap();
    // ------- End of compaction metrics.

    // Query metrics.
    /// Timer of different stages in query.
    pub static ref READ_STAGE_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_mito_read_stage_elapsed",
        "mito read stage elapsed",
        &[STAGE_LABEL],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
    pub static ref READ_STAGE_FETCH_PAGES: Histogram = READ_STAGE_ELAPSED.with_label_values(&["fetch_pages"]);
    /// Counter of rows read from different source.
    pub static ref READ_ROWS_TOTAL: IntCounterVec =
        register_int_counter_vec!("greptime_mito_read_rows_total", "mito read rows total", &[TYPE_LABEL]).unwrap();
    /// Counter of filtered rows during merge.
    pub static ref MERGE_FILTER_ROWS_TOTAL: IntCounterVec =
        register_int_counter_vec!("greptime_mito_merge_filter_rows_total", "mito merge filter rows total", &[TYPE_LABEL]).unwrap();
    /// Counter of row groups read.
    pub static ref READ_ROW_GROUPS_TOTAL: IntCounterVec =
        register_int_counter_vec!("greptime_mito_read_row_groups_total", "mito read row groups total", &[TYPE_LABEL]).unwrap();
    /// Counter of filtered rows by precise filter.
    pub static ref PRECISE_FILTER_ROWS_TOTAL: IntCounterVec =
        register_int_counter_vec!("greptime_mito_precise_filter_rows_total", "mito precise filter rows total", &[TYPE_LABEL]).unwrap();
    pub static ref READ_ROWS_IN_ROW_GROUP_TOTAL: IntCounterVec =
        register_int_counter_vec!("greptime_mito_read_rows_in_row_group_total", "mito read rows in row group total", &[TYPE_LABEL]).unwrap();
    /// Histogram for the number of SSTs to scan per query.
    pub static ref READ_SST_COUNT: Histogram = register_histogram!(
        "greptime_mito_read_sst_count",
        "Number of SSTs to scan in a scan task",
        vec![1.0, 4.0, 8.0, 16.0, 32.0, 64.0, 256.0, 1024.0],
    ).unwrap();
    /// Histogram for the number of rows returned per query.
    pub static ref READ_ROWS_RETURN: Histogram = register_histogram!(
        "greptime_mito_read_rows_return",
        "Number of rows returned in a scan task",
        exponential_buckets(100.0, 10.0, 8).unwrap(),
    ).unwrap();
    /// Histogram for the number of batches returned per query.
    pub static ref READ_BATCHES_RETURN: Histogram = register_histogram!(
        "greptime_mito_read_batches_return",
        "Number of rows returned in a scan task",
        exponential_buckets(100.0, 10.0, 7).unwrap(),
    ).unwrap();
    // ------- End of query metrics.

    // Cache related metrics.
    /// Cache hit counter.
    pub static ref CACHE_HIT: IntCounterVec = register_int_counter_vec!(
        "greptime_mito_cache_hit",
        "mito cache hit",
        &[TYPE_LABEL]
    )
    .unwrap();
    /// Cache miss counter.
    pub static ref CACHE_MISS: IntCounterVec = register_int_counter_vec!(
        "greptime_mito_cache_miss",
        "mito cache miss",
        &[TYPE_LABEL]
    )
    .unwrap();
    /// Cache size in bytes.
    pub static ref CACHE_BYTES: IntGaugeVec = register_int_gauge_vec!(
        "greptime_mito_cache_bytes",
        "mito cache bytes",
        &[TYPE_LABEL]
    )
    .unwrap();
    /// Download bytes counter in the write cache.
    pub static ref WRITE_CACHE_DOWNLOAD_BYTES_TOTAL: IntCounter = register_int_counter!(
        "mito_write_cache_download_bytes_total",
        "mito write cache download bytes total",
    ).unwrap();
    /// Timer of the downloading task in the write cache.
    pub static ref WRITE_CACHE_DOWNLOAD_ELAPSED: HistogramVec = register_histogram_vec!(
        "mito_write_cache_download_elapsed",
        "mito write cache download elapsed",
        &[TYPE_LABEL],
    ).unwrap();
    /// Upload bytes counter.
    pub static ref UPLOAD_BYTES_TOTAL: IntCounter = register_int_counter!(
        "mito_upload_bytes_total",
        "mito upload bytes total",
    )
    .unwrap();
    /// Cache eviction counter, labeled with cache type and eviction reason.
    pub static ref CACHE_EVICTION: IntCounterVec = register_int_counter_vec!(
        "greptime_mito_cache_eviction",
        "mito cache eviction",
        &[TYPE_LABEL, CACHE_EVICTION_CAUSE]
    ).unwrap();
    // ------- End of cache metrics.

    // Index metrics.
    /// Timer of index application.
    pub static ref INDEX_APPLY_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_index_apply_elapsed",
        "index apply elapsed",
        &[TYPE_LABEL],
    )
    .unwrap();
    /// Gauge of index apply memory usage.
    pub static ref INDEX_APPLY_MEMORY_USAGE: IntGauge = register_int_gauge!(
        "greptime_index_apply_memory_usage",
        "index apply memory usage",
    )
    .unwrap();
    /// Timer of index creation.
    pub static ref INDEX_CREATE_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_index_create_elapsed",
        "index create elapsed",
        &[STAGE_LABEL, TYPE_LABEL],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
    /// Counter of rows indexed.
    pub static ref INDEX_CREATE_ROWS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "greptime_index_create_rows_total",
        "index create rows total",
        &[TYPE_LABEL],
    )
    .unwrap();
    /// Counter of created index bytes.
    pub static ref INDEX_CREATE_BYTES_TOTAL: IntCounterVec = register_int_counter_vec!(
        "greptime_index_create_bytes_total",
        "index create bytes total",
        &[TYPE_LABEL],
    )
    .unwrap();
    /// Gauge of index create memory usage.
    pub static ref INDEX_CREATE_MEMORY_USAGE: IntGaugeVec = register_int_gauge_vec!(
        "greptime_index_create_memory_usage",
        "index create memory usage",
        &[TYPE_LABEL],
    ).unwrap();
    /// Counter of r/w bytes on index related IO operations.
    pub static ref INDEX_IO_BYTES_TOTAL: IntCounterVec = register_int_counter_vec!(
        "greptime_index_io_bytes_total",
        "index io bytes total",
        &[TYPE_LABEL, FILE_TYPE_LABEL]
    )
    .unwrap();
    /// Counter of read bytes on puffin files.
    pub static ref INDEX_PUFFIN_READ_BYTES_TOTAL: IntCounter = INDEX_IO_BYTES_TOTAL
        .with_label_values(&["read", "puffin"]);
    /// Counter of write bytes on puffin files.
    pub static ref INDEX_PUFFIN_WRITE_BYTES_TOTAL: IntCounter = INDEX_IO_BYTES_TOTAL
        .with_label_values(&["write", "puffin"]);
    /// Counter of read bytes on intermediate files.
    pub static ref INDEX_INTERMEDIATE_READ_BYTES_TOTAL: IntCounter = INDEX_IO_BYTES_TOTAL
        .with_label_values(&["read", "intermediate"]);
    /// Counter of write bytes on intermediate files.
    pub static ref INDEX_INTERMEDIATE_WRITE_BYTES_TOTAL: IntCounter = INDEX_IO_BYTES_TOTAL
        .with_label_values(&["write", "intermediate"]);

    /// Counter of r/w operations on index related IO operations, e.g. read, write, seek and flush.
    pub static ref INDEX_IO_OP_TOTAL: IntCounterVec = register_int_counter_vec!(
        "greptime_index_io_op_total",
        "index io op total",
        &[TYPE_LABEL, FILE_TYPE_LABEL]
    )
    .unwrap();
    /// Counter of read operations on puffin files.
    pub static ref INDEX_PUFFIN_READ_OP_TOTAL: IntCounter = INDEX_IO_OP_TOTAL
        .with_label_values(&["read", "puffin"]);
    /// Counter of seek operations on puffin files.
    pub static ref INDEX_PUFFIN_SEEK_OP_TOTAL: IntCounter = INDEX_IO_OP_TOTAL
        .with_label_values(&["seek", "puffin"]);
    /// Counter of write operations on puffin files.
    pub static ref INDEX_PUFFIN_WRITE_OP_TOTAL: IntCounter = INDEX_IO_OP_TOTAL
        .with_label_values(&["write", "puffin"]);
    /// Counter of flush operations on puffin files.
    pub static ref INDEX_PUFFIN_FLUSH_OP_TOTAL: IntCounter = INDEX_IO_OP_TOTAL
        .with_label_values(&["flush", "puffin"]);
    /// Counter of read operations on intermediate files.
    pub static ref INDEX_INTERMEDIATE_READ_OP_TOTAL: IntCounter = INDEX_IO_OP_TOTAL
        .with_label_values(&["read", "intermediate"]);
    /// Counter of seek operations on intermediate files.
    pub static ref INDEX_INTERMEDIATE_SEEK_OP_TOTAL: IntCounter = INDEX_IO_OP_TOTAL
        .with_label_values(&["seek", "intermediate"]);
    /// Counter of write operations on intermediate files.
    pub static ref INDEX_INTERMEDIATE_WRITE_OP_TOTAL: IntCounter = INDEX_IO_OP_TOTAL
        .with_label_values(&["write", "intermediate"]);
    /// Counter of flush operations on intermediate files.
    pub static ref INDEX_INTERMEDIATE_FLUSH_OP_TOTAL: IntCounter = INDEX_IO_OP_TOTAL
        .with_label_values(&["flush", "intermediate"]);
    // ------- End of index metrics.

    /// Partition tree memtable data buffer freeze metrics
    pub static ref PARTITION_TREE_DATA_BUFFER_FREEZE_STAGE_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_partition_tree_buffer_freeze_stage_elapsed",
        "mito partition tree data buffer freeze stage elapsed",
        &[STAGE_LABEL],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0]
    )
    .unwrap();

    /// Partition tree memtable read path metrics
    pub static ref PARTITION_TREE_READ_STAGE_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_partition_tree_read_stage_elapsed",
        "mito partition tree read stage elapsed",
        &[STAGE_LABEL],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0]
    )
    .unwrap();

    // ------- End of partition tree memtable metrics.


    // Manifest related metrics:

    /// Elapsed time of manifest operation. Labeled with "op".
    pub static ref MANIFEST_OP_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_manifest_op_elapsed",
        "mito manifest operation elapsed",
        &["op"]
    ).unwrap();
}
