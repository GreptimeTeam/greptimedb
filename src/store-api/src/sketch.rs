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

//! SST-only statistics requests and region RPC conversion.

mod proto;

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
    /// Estimates the fraction of non-null values strictly less than the supplied value.
    Rank { value: Value },
}

/// Uniform sampling of physical files, without replacement, before index I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SketchSampling {
    pub max_files: u32,
    /// Stable seed shared by coverage and value requests.
    pub seed: u64,
}

/// File sample identity and unscaled row diagnostics for the requested window.
#[derive(Debug, Clone, PartialEq)]
pub struct SketchSample {
    pub candidate_files: u64,
    pub sampled_files: u64,
    /// Identifies the selected immutable index generations.
    pub fingerprint: u64,
    /// Sum of squared per-file represented row counts, including zero contributions.
    pub rows_squared_sum: f64,
    pub max_file_rows: u64,
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
    /// Absent means inspect all candidate files. Missing sampled files are never replaced.
    pub sampling: Option<SketchSampling>,
}

impl SketchRequest {
    /// Validates algorithm-independent inputs before file selection or I/O.
    /// Implementations separately enforce resource limits and column/type compatibility.
    pub fn validate(&self) -> Result<(), BoxedError> {
        let reason = if self.time_range.start >= self.time_range.end {
            Some("sketch time range must have start before end")
        } else if self.sampling.is_some_and(|sampling| !(1..=100_000).contains(&sampling.max_files)) {
            Some("sketch sample file limit must be in 1..=100000")
        } else if self.columns.is_empty() {
            Some("sketch request must select at least one column")
        } else if self.queries.is_empty() {
            Some("sketch request must select at least one query")
        } else if self.queries.iter().any(|query| matches!(query, SketchQuery::Rank { value } if value.is_null())) {
            Some("rank requires a non-null value")
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

/// Proposes and validates split points within one pinned SST snapshot.
/// File limits count unique files across the proposal and validation samples together.
#[derive(Debug, Clone, PartialEq)]
pub struct SketchSplitRequest {
    pub time_range: SketchTimeRange,
    pub column: String,
    /// Desired number of partitions, including both outermost partitions.
    pub parts: u32,
    /// Total files selected for the first sampling round.
    pub initial_files: u32,
    /// Minimum total files before a sampled result may be accepted.
    pub min_files: u32,
    /// Maximum total unique files read across all sampling rounds.
    pub max_files: u32,
    pub seed: u64,
    /// Maximum allowed CDF disagreement; this is not a statistical confidence bound.
    pub max_cdf_delta: f64,
    /// Minimum partition mass relative to the ideal mass of `1 / parts`.
    pub min_partition_ratio: f64,
    /// Maximum partition mass relative to the ideal mass of `1 / parts`.
    pub max_partition_ratio: f64,
}

impl SketchSplitRequest {
    /// Validates controls before taking a snapshot or reading index files.
    pub fn validate(&self) -> Result<(), BoxedError> {
        let reason = if self.time_range.start >= self.time_range.end {
            Some("sketch time range must have start before end")
        } else if self.column.trim().is_empty() {
            Some("sketch split request must select a column")
        } else if !(2..=64).contains(&self.parts) {
            Some("sketch split partition count must be in 2..=64")
        } else if self.initial_files == 0
            || self.initial_files > self.min_files
            || self.min_files > self.max_files
        {
            Some("sketch split file limits must satisfy 0 < initial <= min <= max")
        } else if !self.max_cdf_delta.is_finite() || !(0.0..=1.0).contains(&self.max_cdf_delta) {
            Some("sketch split CDF delta must be finite and in [0, 1]")
        } else if !self.min_partition_ratio.is_finite()
            || self.min_partition_ratio <= 0.0
            || self.min_partition_ratio > 1.0
        {
            Some("sketch split minimum partition ratio must be finite and in (0, 1]")
        } else if !self.max_partition_ratio.is_finite() || self.max_partition_ratio < 1.0 {
            Some("sketch split maximum partition ratio must be finite and at least 1")
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

/// A split boundary and its strict-less-than ranks in the two samples.
#[derive(Debug, Clone, PartialEq)]
pub struct SketchSplitPoint {
    pub value: Value,
    pub candidate_rank: f64,
    /// Absent for a census, which does not manufacture an independent validation sample.
    pub validation_rank: Option<f64>,
}

/// Outcome of a bounded split-point search; only validated or census points are actionable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SketchSplitStatus {
    /// Both samples satisfy the requested stability and partition-mass controls.
    Validated,
    /// Every candidate file was read and the requested partition-mass controls hold.
    Census,
    /// Sampling exhausted its file budget without satisfying the controls.
    Unstable,
    /// The available distribution cannot produce the requested partition masses.
    Infeasible,
    /// Missing or incompatible summaries prevent validation.
    MissingCoverage,
    /// No represented rows are available in the requested window.
    EmptyWindow,
}

/// Split-point result and the actual file work performed in one snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct SketchSplitResponse {
    pub status: SketchSplitStatus,
    pub points: Vec<SketchSplitPoint>,
    /// Number of unique files eligible before sampling.
    pub candidate_files: u64,
    /// Total unique files read, equal to proposal_files plus validation_files.
    pub sampled_files: u64,
    /// Files used to propose boundaries; includes all files for a census.
    pub proposal_files: u64,
    /// Disjoint files used to evaluate boundaries; zero for a census.
    pub validation_files: u64,
    pub rounds: u32,
    pub gaps: Vec<SketchGap>,
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
    Rank {
        /// Fraction of non-null represented rows strictly below the requested value.
        fraction: f64,
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
    /// Present when sampling was requested; estimates and represented rows remain unscaled.
    pub sample: Option<SketchSample>,
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

    /// Proposes and validates split points without changing snapshots between rounds.
    async fn split(
        &self,
        _region_id: RegionId,
        _request: SketchSplitRequest,
    ) -> Result<SketchSplitResponse, BoxedError> {
        Err(BoxedError::new(PlainError::new(
            "sketch split search is unsupported".to_string(),
            StatusCode::Unsupported,
        )))
    }
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
            sampling: None,
        }
    }

    fn split_request() -> SketchSplitRequest {
        SketchSplitRequest {
            time_range: request().time_range,
            column: "value".to_string(),
            parts: 2,
            initial_files: 128,
            min_files: 512,
            max_files: 2048,
            seed: 42,
            max_cdf_delta: 0.05,
            min_partition_ratio: 0.7,
            max_partition_ratio: 1.3,
        }
    }

    #[test]
    fn test_validate_split_request() {
        let valid = split_request();
        assert!(valid.validate().is_ok());
        for (initial_files, min_files, max_files, accepted) in [
            (1, 1, 1, true),
            (100_000, 100_000, 100_000, true),
            (0, 512, 2048, false),
            (513, 512, 2048, false),
            (128, 2049, 2048, false),
            (128, 512, u32::MAX, true),
        ] {
            let request = SketchSplitRequest {
                initial_files,
                min_files,
                max_files,
                ..valid.clone()
            };
            assert_eq!(request.validate().is_ok(), accepted, "{request:?}");
        }
        for (parts, accepted) in [(1, false), (2, true), (64, true), (65, false)] {
            let request = SketchSplitRequest {
                parts,
                ..valid.clone()
            };
            assert_eq!(request.validate().is_ok(), accepted, "{request:?}");
        }
        for value in [f64::NAN, f64::NEG_INFINITY, f64::INFINITY, -0.1, 1.1] {
            let request = SketchSplitRequest {
                max_cdf_delta: value,
                ..valid.clone()
            };
            assert_eq!(
                request.validate().unwrap_err().status_code(),
                StatusCode::InvalidArguments
            );
        }
        for value in [0.0, 1.0] {
            assert!(
                SketchSplitRequest {
                    max_cdf_delta: value,
                    ..valid.clone()
                }
                .validate()
                .is_ok()
            );
        }
        for value in [f64::NAN, f64::NEG_INFINITY, f64::INFINITY, 0.0, -0.1, 1.1] {
            assert!(
                SketchSplitRequest {
                    min_partition_ratio: value,
                    ..valid.clone()
                }
                .validate()
                .is_err()
            );
        }
        for value in [f64::NAN, f64::NEG_INFINITY, f64::INFINITY, 0.9] {
            assert!(
                SketchSplitRequest {
                    max_partition_ratio: value,
                    ..valid.clone()
                }
                .validate()
                .is_err()
            );
        }
        assert!(
            SketchSplitRequest {
                min_partition_ratio: 1.0,
                max_partition_ratio: 1.0,
                ..valid.clone()
            }
            .validate()
            .is_ok()
        );
        for column in ["", " "] {
            assert!(
                SketchSplitRequest {
                    column: column.to_string(),
                    ..valid.clone()
                }
                .validate()
                .is_err()
            );
        }
        for end in [valid.time_range.start, Timestamp::new_nanosecond(1_000_999)] {
            assert!(
                SketchSplitRequest {
                    time_range: SketchTimeRange {
                        end,
                        ..valid.time_range
                    },
                    ..valid.clone()
                }
                .validate()
                .is_err()
            );
        }
    }

    #[test]
    fn test_validate_sampling_limit() {
        for (max_files, valid) in [(0, false), (1, true), (100_000, true), (100_001, false)] {
            let mut request = request();
            request.sampling = Some(SketchSampling {
                max_files,
                seed: 42,
            });
            assert_eq!(request.validate().is_ok(), valid, "{max_files}");
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
