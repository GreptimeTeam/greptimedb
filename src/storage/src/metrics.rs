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

//! storage metrics

use lazy_static::lazy_static;
use prometheus::*;

/// Reason to flush.
pub const FLUSH_REASON: &str = "reason";

lazy_static! {
    /// Elapsed time of updating manifest when creating regions.
    pub static ref CREATE_REGION_UPDATE_MANIFEST: Histogram =
        register_histogram!("storage_create_region_update_manifest", "storage create region update manifest").unwrap();
    /// Counter of scheduled flush requests.
    pub static ref FLUSH_REQUESTS_TOTAL: IntCounterVec =
        register_int_counter_vec!("storage_flush_requests_total", "storage flush requests total", &[FLUSH_REASON]).unwrap();
    /// Counter of scheduled failed flush jobs.
    pub static ref FLUSH_ERRORS_TOTAL: IntCounter =
        register_int_counter!("storage_flush_errors_total", "storage flush errors total").unwrap();
    //// Elapsed time of a flush job.
    pub static ref FLUSH_ELAPSED: Histogram =
        register_histogram!("storage_flush_elapsed", "storage flush elapsed").unwrap();
    /// Counter of flushed bytes.
    pub static ref FLUSH_BYTES_TOTAL: IntCounter =
        register_int_counter!("storage_flush_bytes_total", "storage flush bytes total").unwrap();
    /// Gauge for open regions
    pub static ref REGION_COUNT: IntGauge =
        register_int_gauge!("storage_region_count", "storage region count").unwrap();
    /// Timer for logstore write
    pub static ref LOG_STORE_WRITE_ELAPSED: Histogram =
        register_histogram!("storage_logstore_write_elapsed", "storage logstore write elapsed").unwrap();
    /// Elapsed time of a compact job.
    pub static ref COMPACT_ELAPSED: Histogram =
        register_histogram!("storage_compact_elapsed", "storage compact elapsed").unwrap();
    /// Elapsed time for merging SST files.
    pub static ref MERGE_ELAPSED: Histogram =
        register_histogram!("storage_compaction_merge_elapsed", "storage compaction merge elapsed").unwrap();
    /// Global write buffer size in bytes.
    pub static ref WRITE_BUFFER_BYTES: IntGauge =
        register_int_gauge!("storage_write_buffer_bytes", "storage write buffer bytes").unwrap();
    /// Elapsed time of inserting memtable.
    pub static ref MEMTABLE_WRITE_ELAPSED: Histogram =
        register_histogram!("storage_memtable_write_elapsed", "storage memtable write elapsed").unwrap();
    /// Elapsed time of preprocessing write batch.
    pub static ref PREPROCESS_ELAPSED: Histogram =
        register_histogram!("storage_write_preprocess_elapsed", "storage write preprocess elapsed").unwrap();
    /// Elapsed time for windowed scan
    pub static ref WINDOW_SCAN_ELAPSED: Histogram =
        register_histogram!("query_scan_window_scan_elapsed", "query scan window scan elapsed").unwrap();
    /// Rows per window during window scan
    pub static ref WINDOW_SCAN_ROWS_PER_WINDOW: Histogram =
        register_histogram!("query_scan_window_scan_window_row_size", "query scan window scan window row size").unwrap();
}
