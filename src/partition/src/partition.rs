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

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use datatypes::arrow::array::{BooleanArray, RecordBatch};
use datatypes::prelude::Value;
use serde::{Deserialize, Serialize};
use store_api::storage::RegionNumber;

use crate::error::Result;

pub type PartitionRuleRef = Arc<dyn PartitionRule>;

pub trait PartitionRule: Sync + Send {
    fn as_any(&self) -> &dyn Any;

    fn partition_columns(&self) -> Vec<String>;

    /// Finds the target region by the partition values.
    ///
    /// Note that the `values` should have the same length as the `partition_columns`.
    fn find_region(&self, values: &[Value]) -> Result<RegionNumber>;

    /// Split the record batch into multiple regions by the partition values.
    /// The result is a map from region mask in which the array is true for the rows that match the partition values.
    /// Region with now rows selected may not appear in result map.
    fn split_record_batch(
        &self,
        record_batch: &RecordBatch,
    ) -> Result<HashMap<RegionNumber, RegionMask>>;
}

/// The bound of one partition.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PartitionBound {
    /// Deprecated since 0.9.0.
    Value(Value),
    /// Deprecated since 0.15.0.
    MaxValue,
    Expr(crate::expr::PartitionExpr),
}

pub struct RegionMask {
    array: BooleanArray,
    selected_rows: usize,
}

impl From<BooleanArray> for RegionMask {
    fn from(array: BooleanArray) -> Self {
        let selected_rows = array.true_count();
        Self {
            array,
            selected_rows,
        }
    }
}

impl RegionMask {
    pub fn new(array: BooleanArray, selected_rows: usize) -> Self {
        Self {
            array,
            selected_rows,
        }
    }

    pub fn array(&self) -> &BooleanArray {
        &self.array
    }

    /// All rows are selected.
    pub fn select_all(&self) -> bool {
        self.selected_rows == self.array.len()
    }

    /// No row is selected.
    pub fn select_none(&self) -> bool {
        self.selected_rows == 0
    }

    pub fn selected_rows(&self) -> usize {
        self.selected_rows
    }
}
