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

use common_recordbatch::OrderOption;
use datafusion_expr::expr::Expr;
use strum::Display;

use crate::storage::SequenceNumber;

/// A hint on how to select rows from a time-series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display)]
pub enum TimeSeriesRowSelector {
    /// Only keep the last row of each time-series.
    LastRow,
}

#[derive(Default, Clone, Debug, PartialEq, Eq)]
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
    /// If set, only rows with a sequence number lesser or equal to this value
    /// will be returned.
    pub sequence: Option<SequenceNumber>,
}
