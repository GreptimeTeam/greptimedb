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

use std::fmt::{Display, Formatter};

use common_error::ext::BoxedError;
use common_recordbatch::OrderOption;
use datafusion_expr::expr::Expr;
// Re-export vector types from datatypes to avoid duplication
pub use datatypes::schema::{VectorDistanceMetric, VectorIndexEngineType};
use strum::Display;

use crate::storage::{ColumnId, SequenceNumber};

/// A hint for KNN vector search.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorSearchRequest {
    /// Column ID of the vector column to search.
    pub column_id: ColumnId,
    /// The query vector to search for.
    pub query_vector: Vec<f32>,
    /// Number of nearest neighbors to return.
    pub k: usize,
    /// Distance metric to use (matches the index metric).
    pub metric: VectorDistanceMetric,
    /// Optional limit for the final result size (limit + offset).
    /// Used by query-layer adaptive top-k logic; currently not consumed by storage vector scan.
    pub limit: Option<usize>,
    /// Optional offset for the final result.
    /// Used by query-layer adaptive top-k logic; currently not consumed by storage vector scan.
    pub offset: Option<usize>,
    /// Optional tie-breaker order applied after distance.
    /// Used by query-layer adaptive top-k logic; currently not consumed by storage vector scan.
    pub tie_breakers: Option<Vec<OrderOption>>,
}

/// Search results from vector index.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorSearchMatches {
    /// Keys (row offsets in the index).
    pub keys: Vec<u64>,
    /// Distances from the query vector.
    pub distances: Vec<f32>,
}

/// Predicate for filtering vector search candidates by row offset.
pub trait VectorSearchPredicate: Send + Sync {
    /// Returns true if the row offset should be included.
    fn allows(&self, key: u64) -> bool;
}

/// Trait for vector index engines (HNSW implementations).
///
/// This trait defines the interface for pluggable vector index engines.
/// Implementations (e.g., UsearchEngine) are provided by storage engines like mito2.
pub trait VectorIndexEngine: Send + Sync {
    /// Adds a vector with the given key.
    fn add(&mut self, key: u64, vector: &[f32]) -> Result<(), BoxedError>;

    /// Searches for k nearest neighbors.
    fn search(&self, query: &[f32], k: usize) -> Result<VectorSearchMatches, BoxedError>;

    /// Searches for k nearest neighbors with an optional predicate.
    fn search_with_predicate(
        &self,
        query: &[f32],
        k: usize,
        predicate: Option<&dyn VectorSearchPredicate>,
    ) -> Result<VectorSearchMatches, BoxedError>;

    /// Returns the serialized length.
    fn serialized_length(&self) -> usize;

    /// Serializes the index to a buffer.
    fn save_to_buffer(&self, buffer: &mut [u8]) -> Result<(), BoxedError>;

    /// Reserves capacity for vectors.
    fn reserve(&mut self, capacity: usize) -> Result<(), BoxedError>;

    /// Returns current size (number of vectors).
    fn size(&self) -> usize;

    /// Returns current capacity.
    fn capacity(&self) -> usize;

    /// Returns memory usage in bytes.
    fn memory_usage(&self) -> usize;
}

/// A hint on how to select rows from a time-series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display)]
pub enum TimeSeriesRowSelector {
    /// Only keep the last row of each time-series.
    LastRow,
}

/// A hint on how to distribute time-series data on the scan output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display)]
pub enum TimeSeriesDistribution {
    /// Data are distributed by time window first. The scanner will
    /// return all data within one time window before moving to the next one.
    TimeWindowed,
    /// Data are organized by time-series first. The scanner will return
    /// all data for one time-series before moving to the next one.
    PerSeries,
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct ScanRequest {
    /// Indices of columns to read, `None` to read all columns. This indices is
    /// based on table schema.
    pub projection: Option<Vec<usize>>,
    /// Filters pushed down
    pub filters: Vec<Expr>,
    /// Expected output ordering. This is only a hint and isn't guaranteed.
    pub output_ordering: Option<Vec<OrderOption>>,
    /// limit can be used to reduce the amount scanned
    /// from the datasource as a performance optimization.
    /// If set, it contains the amount of rows needed by the caller,
    /// The data source should return *at least* this number of rows if available.
    pub limit: Option<usize>,
    /// Optional hint to select rows from time-series.
    pub series_row_selector: Option<TimeSeriesRowSelector>,
    /// Optional constraint on the sequence number of the rows to read.
    /// If set, only rows with a sequence number **lesser or equal** to this value
    /// will be returned.
    pub memtable_max_sequence: Option<SequenceNumber>,
    /// Optional constraint on the minimal sequence number in the memtable.
    /// If set, only the memtables that contain sequences **greater than** this value will be scanned
    pub memtable_min_sequence: Option<SequenceNumber>,
    /// Optional constraint on the minimal sequence number in the SST files.
    /// If set, only the SST files that contain sequences greater than this value will be scanned.
    pub sst_min_sequence: Option<SequenceNumber>,
    /// Optional hint for the distribution of time-series data.
    pub distribution: Option<TimeSeriesDistribution>,
    /// Optional hint for KNN vector search. When set, the scan should use
    /// vector index to find the k nearest neighbors.
    pub vector_search: Option<VectorSearchRequest>,
}

impl Display for ScanRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        enum Delimiter {
            None,
            Init,
        }

        impl Delimiter {
            fn as_str(&mut self) -> &str {
                match self {
                    Delimiter::None => {
                        *self = Delimiter::Init;
                        ""
                    }
                    Delimiter::Init => ", ",
                }
            }
        }

        let mut delimiter = Delimiter::None;

        write!(f, "ScanRequest {{ ")?;
        if let Some(projection) = &self.projection {
            write!(f, "{}projection: {:?}", delimiter.as_str(), projection)?;
        }
        if !self.filters.is_empty() {
            write!(
                f,
                "{}filters: [{}]",
                delimiter.as_str(),
                self.filters
                    .iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }
        if let Some(output_ordering) = &self.output_ordering {
            write!(
                f,
                "{}output_ordering: {:?}",
                delimiter.as_str(),
                output_ordering
            )?;
        }
        if let Some(limit) = &self.limit {
            write!(f, "{}limit: {}", delimiter.as_str(), limit)?;
        }
        if let Some(series_row_selector) = &self.series_row_selector {
            write!(
                f,
                "{}series_row_selector: {}",
                delimiter.as_str(),
                series_row_selector
            )?;
        }
        if let Some(sequence) = &self.memtable_max_sequence {
            write!(f, "{}sequence: {}", delimiter.as_str(), sequence)?;
        }
        if let Some(sst_min_sequence) = &self.sst_min_sequence {
            write!(
                f,
                "{}sst_min_sequence: {}",
                delimiter.as_str(),
                sst_min_sequence
            )?;
        }
        if let Some(distribution) = &self.distribution {
            write!(f, "{}distribution: {}", delimiter.as_str(), distribution)?;
        }
        if let Some(vector_search) = &self.vector_search {
            write!(
                f,
                "{}vector_search: column_id={}, k={}, metric={}, limit={:?}, offset={:?}, tie_breakers={:?}",
                delimiter.as_str(),
                vector_search.column_id,
                vector_search.k,
                vector_search.metric,
                vector_search.limit,
                vector_search.offset,
                vector_search.tie_breakers
            )?;
        }
        write!(f, " }}")
    }
}

#[cfg(test)]
mod tests {
    use datafusion_expr::{Operator, binary_expr, col, lit};

    use super::*;

    #[test]
    fn test_display_scan_request() {
        let request = ScanRequest {
            ..Default::default()
        };
        assert_eq!(request.to_string(), "ScanRequest {  }");

        let request = ScanRequest {
            projection: Some(vec![1, 2]),
            filters: vec![
                binary_expr(col("i"), Operator::Gt, lit(1)),
                binary_expr(col("s"), Operator::Eq, lit("x")),
            ],
            limit: Some(10),
            ..Default::default()
        };
        assert_eq!(
            request.to_string(),
            r#"ScanRequest { projection: [1, 2], filters: [i > Int32(1), s = Utf8("x")], limit: 10 }"#
        );

        let request = ScanRequest {
            filters: vec![
                binary_expr(col("i"), Operator::Gt, lit(1)),
                binary_expr(col("s"), Operator::Eq, lit("x")),
            ],
            limit: Some(10),
            ..Default::default()
        };
        assert_eq!(
            request.to_string(),
            r#"ScanRequest { filters: [i > Int32(1), s = Utf8("x")], limit: 10 }"#
        );

        let request = ScanRequest {
            projection: Some(vec![1, 2]),
            limit: Some(10),
            ..Default::default()
        };
        assert_eq!(
            request.to_string(),
            "ScanRequest { projection: [1, 2], limit: 10 }"
        );
    }
}
