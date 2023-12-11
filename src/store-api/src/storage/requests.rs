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

use std::collections::HashMap;

use common_error::ext::ErrorExt;
use common_query::logical_plan::Expr;
use common_recordbatch::OrderOption;
use datatypes::vectors::VectorRef;

/// Write request holds a collection of updates to apply to a region.
///
/// The implementation of the write request should ensure all operations in
/// the request follows the same schema restriction.
pub trait WriteRequest: Send {
    type Error: ErrorExt + Send + Sync;

    /// Add put operation to the request.
    ///
    /// `data` is the columnar format of the data to put.
    fn put(&mut self, data: HashMap<String, VectorRef>) -> Result<(), Self::Error>;

    /// Delete rows by `keys`.
    ///
    /// `keys` are the row keys, in columnar format, of the rows to delete.
    fn delete(&mut self, keys: HashMap<String, VectorRef>) -> Result<(), Self::Error>;
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
}
