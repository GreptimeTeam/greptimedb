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

/// Stage label.
pub const STAGE_LABEL: &str = "stage";

/// Global write buffer size in bytes.
pub const WRITE_BUFFER_BYTES: &str = "mito.write_buffer_bytes";
/// Type label.
pub const TYPE_LABEL: &str = "type";
/// Gauge for open regions
pub const REGION_COUNT: &str = "mito.region_count";
/// Elapsed time to handle requests.
pub const HANDLE_REQUEST_ELAPSED: &str = "mito.handle_request.elapsed";

// ------ Flush related metrics
/// Counter of scheduled flush requests.
/// Note that the flush scheduler may merge some flush requests.
pub const FLUSH_REQUESTS_TOTAL: &str = "mito.flush.requests_total";
/// Reason to flush.
pub const FLUSH_REASON: &str = "reason";
/// Counter of scheduled failed flush jobs.
pub const FLUSH_ERRORS_TOTAL: &str = "mito.flush.errors_total";
/// Elapsed time of a flush job.
pub const FLUSH_ELAPSED: &str = "mito.flush.elapsed";
/// Histogram of flushed bytes.
pub const FLUSH_BYTES_TOTAL: &str = "mito.flush.bytes_total";
// ------ End of flush related metrics

// ------ Write related metrics
/// Counter of stalled write requests.
pub const WRITE_STALL_TOTAL: &str = "mito.write.stall_total";
/// Counter of rejected write requests.
pub const WRITE_REJECT_TOTAL: &str = "mito.write.reject_total";
/// Elapsed time of each write stage.
pub const WRITE_STAGE_ELAPSED: &str = "mito.write.stage_elapsed";
/// Counter of rows to write.
pub const WRITE_ROWS_TOTAL: &str = "mito.write.rows_total";
// ------ End of write related metrics

// Compaction metrics
/// Timer of different stages in compaction.
pub const COMPACTION_STAGE_ELAPSED: &str = "mito.compaction.stage_elapsed";
/// Timer of whole compaction task.
pub const COMPACTION_ELAPSED_TOTAL: &str = "mito.compaction.total_elapsed";
/// Counter of all requested compaction task.
pub const COMPACTION_REQUEST_COUNT: &str = "mito.compaction.requests_total";
/// Counter of failed compaction task.
pub const COMPACTION_FAILURE_COUNT: &str = "mito.compaction.failure_total";
