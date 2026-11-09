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

//! In-process, SST-only statistics requests. No wire protocol is defined here.

use async_trait::async_trait;
use common_error::ext::BoxedError;
use datatypes::value::Value;

use crate::storage::{ColumnId, RegionId};

/// Half-open event-time range, in milliseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SketchTimeRange {
    /// Inclusive start in Unix milliseconds.
    pub start_ms: i64,
    /// Exclusive end in Unix milliseconds.
    pub end_ms: i64,
}

/// Statistics requested for each named column.
#[derive(Debug, Clone, PartialEq)]
pub enum SketchQuery {
    /// Estimates the number of distinct non-null values.
    DistinctCount,
    /// Finds a non-null value at a fraction in the inclusive range [0, 1].
    Quantile { fraction: f64 },
    /// Bounds the occurrence count of a value, including NULL.
    Frequency { value: Value },
}

/// Selects whole time buckets inside a required range. The implementation validates request limits.
/// A whole bucket may still have missing file statistics, reported separately in its coverage.
#[derive(Debug, Clone)]
pub struct SketchRequest {
    pub time_range: SketchTimeRange,
    /// Column names resolved against the selected Region schema.
    pub columns: Vec<String>,
    /// Operations applied to each selected column.
    pub queries: Vec<SketchQuery>,
}

/// Coverage of one whole time bucket and one query across the selected physical files.
/// Whole means contained in the requested range, not that every file has statistics.
#[derive(Debug, Clone, PartialEq)]
pub struct SketchBucket {
    pub time_range: SketchTimeRange,
    /// Number of compatible physical indexes contributing to this nonempty bucket.
    pub covered_files: u64,
    /// Unavailable or incompatible files whose SST time ranges intersect this bucket.
    pub missing_files: u64,
    /// Rows represented by contributing sketches, including NULLs, without cross-file deduplication.
    pub represented_rows: u64,
}

/// Explicitly unavailable file statistics; unavailable files never count as empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SketchGapReason {
    /// The requested algorithm is not enabled for this column in the current Region schema.
    /// This takes precedence over missing persisted descriptors.
    NotConfigured,
    /// No descriptor exists for this column and algorithm.
    MissingIndex,
    /// The index has no time-bucket directory.
    LegacyFormat,
    /// The persisted scope does not match the current Region scope.
    ScopeMismatch,
    /// Type, encoding, algorithm parameters or bucket width cannot be combined.
    Incompatible,
}

/// File-level coverage gap intersecting the request.
#[derive(Debug, Clone, PartialEq)]
pub struct SketchGap {
    /// Physical index identity, including its immutable index version.
    pub file_id: String,
    /// Why this file cannot contribute to the requested operation.
    pub reason: SketchGapReason,
}

/// Results preserve the different error semantics of the algorithms.
#[derive(Debug, Clone, PartialEq)]
pub enum SketchValue {
    DistinctCount {
        /// Estimated distinct count, excluding NULLs.
        estimate: f64,
        /// Relative standard error as a fraction, not a maximum-error bound.
        relative_standard_error: f64,
    },
    Quantile {
        value: Value,
        /// KLL capacity parameter; this is not a row-error bound.
        capacity: u16,
    },
    Frequency {
        /// Inclusive lower bound on occurrences among represented rows.
        lower: u64,
        /// Inclusive upper bound on occurrences among represented rows.
        upper: u64,
    },
}

/// One column/query result. Partial coverage remains explicit even when a value exists.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSketchResult {
    pub column_name: String,
    pub column_id: ColumnId,
    pub query: SketchQuery,
    /// Nonempty buckets wholly inside the requested range; `missing_files` may be nonzero.
    pub buckets: Vec<SketchBucket>,
    /// Unusable files intersecting the requested range; absence is not treated as zero rows.
    pub gaps: Vec<SketchGap>,
    /// Absent for stat-only requests or when no usable value is available.
    pub value: Option<SketchValue>,
}

/// SST-only response. Memtables and logical deduplication are not included.
#[derive(Debug, Clone, PartialEq)]
pub struct SketchResponse {
    /// One result per requested column and operation.
    pub columns: Vec<ColumnSketchResult>,
}

/// Optional engine capability for reading persisted sketches. Each call takes its own snapshot.
#[async_trait]
pub trait SketchReader: Send + Sync {
    /// Inspects coverage without reading algorithm payloads. Missing sketches remain explicit gaps.
    async fn stat(
        &self,
        region_id: RegionId,
        request: SketchRequest,
    ) -> Result<SketchResponse, BoxedError>;

    /// Merges complete buckets and returns estimates with their coverage. Corrupt payloads are errors.
    async fn query(
        &self,
        region_id: RegionId,
        request: SketchRequest,
    ) -> Result<SketchResponse, BoxedError>;
}
