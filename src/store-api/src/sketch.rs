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
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_time::Timestamp;
use datatypes::value::Value;

use crate::storage::{ColumnId, FileId, IndexVersion, RegionId};

/// Bounded, half-open event-time range. Endpoints retain their timestamp precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SketchTimeRange {
    /// Inclusive start.
    pub start: Timestamp,
    /// Exclusive end.
    pub end: Timestamp,
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
#[derive(Debug, Clone, PartialEq)]
pub struct SketchRequest {
    pub time_range: SketchTimeRange,
    /// Column names resolved against the selected Region schema.
    pub columns: Vec<String>,
    /// Operations applied to each selected column.
    pub queries: Vec<SketchQuery>,
}

impl SketchRequest {
    /// Validates algorithm-independent inputs before file selection or I/O.
    /// Implementations separately enforce resource limits and column/type compatibility.
    pub fn validate(&self) -> Result<(), BoxedError> {
        let reason = if self.time_range.start >= self.time_range.end {
            Some("sketch time range must have start before end")
        } else if self.columns.is_empty() {
            Some("sketch request must select at least one column")
        } else if self.queries.is_empty() {
            Some("sketch request must select at least one query")
        } else if self.queries.iter().any(|query| {
            matches!(query, SketchQuery::Quantile { fraction } if !fraction.is_finite() || !(0.0..=1.0).contains(fraction))
        }) {
            Some("sketch quantile fraction must be finite and in [0, 1]")
        } else {
            None
        };
        match reason {
            Some(reason) => Err(BoxedError::new(PlainError::new(
                reason.to_string(),
                StatusCode::InvalidArguments,
            ))),
            None => Ok(()),
        }
    }
}

/// Coverage of one whole time bucket and one query across the selected physical files.
/// Whole means contained in the requested range, not that every file has statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SketchGap {
    /// Region that owns the physical source file, which may differ from the queried Region.
    pub source_region_id: RegionId,
    pub file_id: FileId,
    /// Immutable generation of the physical index file.
    pub index_version: IndexVersion,
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
    /// In request column order, then query order within each column.
    pub results: Vec<ColumnSketchResult>,
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

#[cfg(test)]
mod tests {
    use common_error::ext::ErrorExt;

    use super::*;

    fn request() -> SketchRequest {
        SketchRequest {
            time_range: SketchTimeRange {
                start: Timestamp::new_microsecond(1001),
                end: Timestamp::new_microsecond(1002),
            },
            columns: vec!["value".to_string()],
            queries: vec![SketchQuery::DistinctCount],
        }
    }

    #[test]
    fn test_validate_request() {
        let valid = request();
        assert!(valid.validate().is_ok());
        let mut empty_columns = valid.clone();
        empty_columns.columns.clear();
        let mut empty_queries = valid.clone();
        empty_queries.queries.clear();
        let mut empty_range = valid.clone();
        empty_range.time_range.end = empty_range.time_range.start;
        let mut reversed_range = valid.clone();
        reversed_range.time_range.end = Timestamp::new_nanosecond(1_000_999);
        for invalid in [empty_columns, empty_queries, empty_range, reversed_range] {
            assert_eq!(
                invalid.validate().unwrap_err().status_code(),
                StatusCode::InvalidArguments
            );
        }
        for fraction in [0.0, 0.5, 1.0] {
            let mut request = valid.clone();
            request.queries = vec![SketchQuery::Quantile { fraction }];
            assert!(request.validate().is_ok());
        }
        for fraction in [f64::NAN, f64::NEG_INFINITY, f64::INFINITY, -0.1, 1.1] {
            let mut request = valid.clone();
            request.queries = vec![SketchQuery::Quantile { fraction }];
            assert_eq!(
                request.validate().unwrap_err().status_code(),
                StatusCode::InvalidArguments
            );
        }
        let mut mixed_units = valid;
        mixed_units.time_range = SketchTimeRange {
            start: Timestamp::new_second(1),
            end: Timestamp::new_nanosecond(1_000_000_001),
        };
        assert!(mixed_units.validate().is_ok());
    }
}
