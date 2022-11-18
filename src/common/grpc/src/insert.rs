// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use api::v1::Column;

/// Data struct about data insertion.
#[derive(Default)]
pub struct InsertBatch {
    /// Data is represented here.
    pub columns: Vec<Column>,

    /// The row_count of all columns, which include null and non-null values.
    ///
    /// Note: the row_count of all columns in a InsertExpr must be same.
    pub row_count: u32,
}

/// Data struct about data insertion.
///
/// Note: Compared with InsertBatch, InsertBatchRef does not have ownership of
/// columns
#[derive(Clone)]
pub struct InsertBatchRef<'a> {
    pub columns: &'a [Column],
    pub row_count: u32,
}
