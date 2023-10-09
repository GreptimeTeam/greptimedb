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

/// Global write buffer size in bytes.
pub const WRITE_BUFFER_BYTES: &str = "mito.write_buffer_bytes";
/// Type label.
pub const TYPE_LABEL: &str = "type";
/// Gauge for open regions
pub const REGION_COUNT: &str = "mito.region_count";

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
