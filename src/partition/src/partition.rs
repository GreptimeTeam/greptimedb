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
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use common_meta::rpc::router::Partition as MetaPartition;
use datafusion_expr::Operator;
use datatypes::arrow::array::{BooleanArray, RecordBatch};
use datatypes::prelude::Value;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::RegionNumber;

use crate::error::{self, Error, Result};

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

/// The right bound(exclusive) of partition range.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PartitionBound {
    Value(Value),
    MaxValue,
    Expr(crate::expr::PartitionExpr),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionDef {
    partition_columns: Vec<String>,
    partition_bounds: Vec<PartitionBound>,
}

impl Display for PartitionBound {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Value(v) => write!(f, "{}", v),
            Self::MaxValue => write!(f, "MAXVALUE"),
            Self::Expr(e) => write!(f, "{}", e),
        }
    }
}

impl Display for PartitionDef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.partition_bounds
                .iter()
                .map(|b| format!("{b}"))
                .join(", ")
        )
    }
}

impl PartitionDef {
    pub fn new(partition_columns: Vec<String>, partition_bounds: Vec<PartitionBound>) -> Self {
        Self {
            partition_columns,
            partition_bounds,
        }
    }

    pub fn partition_columns(&self) -> &Vec<String> {
        &self.partition_columns
    }

    pub fn partition_bounds(&self) -> &Vec<PartitionBound> {
        &self.partition_bounds
    }
}

impl TryFrom<&MetaPartition> for PartitionDef {
    type Error = Error;

    fn try_from(partition: &MetaPartition) -> Result<Self> {
        let MetaPartition {
            column_list,
            value_list,
        } = partition;

        let partition_columns = column_list
            .iter()
            .map(|x| String::from_utf8_lossy(x).to_string())
            .collect::<Vec<String>>();

        let partition_bounds = value_list
            .iter()
            .map(|x| serde_json::from_str(&String::from_utf8_lossy(x)))
            .collect::<std::result::Result<Vec<PartitionBound>, serde_json::Error>>()
            .context(error::DeserializeJsonSnafu)?;

        Ok(PartitionDef {
            partition_columns,
            partition_bounds,
        })
    }
}

impl TryFrom<PartitionDef> for MetaPartition {
    type Error = Error;

    fn try_from(partition: PartitionDef) -> Result<Self> {
        let PartitionDef {
            partition_columns: columns,
            partition_bounds: bounds,
        } = partition;

        let column_list = columns
            .into_iter()
            .map(|x| x.into_bytes())
            .collect::<Vec<Vec<u8>>>();

        let value_list = bounds
            .into_iter()
            .map(|x| serde_json::to_string(&x).map(|s| s.into_bytes()))
            .collect::<std::result::Result<Vec<Vec<u8>>, serde_json::Error>>()
            .context(error::SerializeJsonSnafu)?;

        Ok(MetaPartition {
            column_list,
            value_list,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct PartitionExpr {
    pub column: String,
    pub op: Operator,
    pub value: Value,
}

impl PartitionExpr {
    pub fn new(column: impl Into<String>, op: Operator, value: Value) -> Self {
        Self {
            column: column.into(),
            op,
            value,
        }
    }

    pub fn value(&self) -> &Value {
        &self.value
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_def() {
        // PartitionDef -> MetaPartition
        let def = PartitionDef {
            partition_columns: vec!["a".to_string(), "b".to_string()],
            partition_bounds: vec![
                PartitionBound::MaxValue,
                PartitionBound::Value(1_i32.into()),
            ],
        };
        assert_eq!("MAXVALUE, 1", def.to_string());

        let partition: MetaPartition = def.try_into().unwrap();
        assert_eq!(
            r#"{"column_list":["a","b"],"value_list":["\"MaxValue\"","{\"Value\":{\"Int32\":1}}"]}"#,
            serde_json::to_string(&partition).unwrap(),
        );

        // MetaPartition -> PartitionDef
        let partition = &MetaPartition {
            column_list: vec![b"a".to_vec(), b"b".to_vec()],
            value_list: vec![
                b"\"MaxValue\"".to_vec(),
                b"{\"Value\":{\"Int32\":1}}".to_vec(),
            ],
        };
        let def: PartitionDef = partition.try_into().unwrap();
        assert_eq!(
            def.partition_columns,
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(
            def.partition_bounds,
            vec![
                PartitionBound::MaxValue,
                PartitionBound::Value(1_i32.into())
            ]
        );
    }

    #[test]
    fn test_partition_bound() {
        let b1 = PartitionBound::Value(1_i32.into());
        let b2 = PartitionBound::Value(100_i32.into());
        let b3 = PartitionBound::MaxValue;
        assert!(b1 < b2);
        assert!(b2 < b3);
    }
}
