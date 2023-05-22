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

use std::collections::{HashMap, HashSet};

use common_error::ext::ErrorExt;
use common_query::logical_plan::Expr;
use datatypes::vectors::VectorRef;

use crate::storage::{ColumnDescriptor, RegionDescriptor, SequenceNumber};

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

#[derive(Default)]
pub struct ScanRequest {
    /// Max sequence number to read, None for latest sequence.
    ///
    /// Default is None. Only returns data whose sequence number is less than or
    /// equal to the `sequence`.
    pub sequence: Option<SequenceNumber>,
    /// Indices of columns to read, `None` to read all columns.
    pub projection: Option<Vec<usize>>,
    /// Filters pushed down
    pub filters: Vec<Expr>,
    /// Expected output ordering. This is only a hint and isn't guaranteed.
    pub output_ordering: Option<Vec<usize>>,
    /// limit can be used to reduce the amount scanned
    /// from the datasource as a performance optimization.
    /// If set, it contains the amount of rows needed by the caller,
    /// The data source should return *at least* this number of rows if available.
    pub limit: Option<usize>,
}

#[derive(Debug)]
pub struct GetRequest {}

/// Operation to add a column.
#[derive(Debug, Clone)]
pub struct AddColumn {
    /// Descriptor of the column to add.
    pub desc: ColumnDescriptor,
    /// Is the column a key column.
    pub is_key: bool,
}

/// Operation to alter a region.
#[derive(Debug, Clone)]
pub enum AlterOperation {
    /// Add columns to the region.
    AddColumns {
        /// Columns to add.
        columns: Vec<AddColumn>,
    },
    /// Drop columns from the region, only value columns are allowed to drop.
    DropColumns {
        /// Name of columns to drop.
        names: Vec<String>,
    },
}

impl AlterOperation {
    /// Apply the operation to the [RegionDescriptor].
    pub fn apply(&self, descriptor: &mut RegionDescriptor) {
        match self {
            AlterOperation::AddColumns { columns } => {
                Self::apply_add(columns, descriptor);
            }
            AlterOperation::DropColumns { names } => {
                Self::apply_drop(names, descriptor);
            }
        }
    }

    /// Add `columns` to the [RegionDescriptor].
    ///
    /// Value columns would be added to the default column family.
    fn apply_add(columns: &[AddColumn], descriptor: &mut RegionDescriptor) {
        for col in columns {
            if col.is_key {
                descriptor.row_key.columns.push(col.desc.clone());
            } else {
                descriptor.default_cf.columns.push(col.desc.clone());
            }
        }
    }

    /// Drop columns from the [RegionDescriptor] by their `names`.
    ///
    /// Only value columns would be removed, non-value columns in `names` would be ignored.
    fn apply_drop(names: &[String], descriptor: &mut RegionDescriptor) {
        let name_set: HashSet<_> = names.iter().collect();
        // Remove columns in the default cf.
        descriptor
            .default_cf
            .columns
            .retain(|col| !name_set.contains(&col.name));
        // Remove columns in other cfs.
        for cf in &mut descriptor.extra_cfs {
            cf.columns.retain(|col| !name_set.contains(&col.name));
        }
    }
}

/// Alter region request.
#[derive(Debug)]
pub struct AlterRequest {
    /// Operation to do.
    pub operation: AlterOperation,
    /// The version of the schema before applying the alteration.
    pub version: u32,
}

#[cfg(test)]
mod tests {
    use datatypes::prelude::*;

    use super::*;
    use crate::storage::{
        ColumnDescriptorBuilder, ColumnFamilyDescriptorBuilder, ColumnId, RegionDescriptorBuilder,
        RowKeyDescriptorBuilder,
    };

    fn new_column_desc(id: ColumnId) -> ColumnDescriptor {
        ColumnDescriptorBuilder::new(id, id.to_string(), ConcreteDataType::int64_datatype())
            .is_nullable(false)
            .build()
            .unwrap()
    }

    fn new_region_descriptor() -> RegionDescriptor {
        let row_key = RowKeyDescriptorBuilder::default()
            .timestamp(new_column_desc(1))
            .build()
            .unwrap();
        let default_cf = ColumnFamilyDescriptorBuilder::default()
            .push_column(new_column_desc(2))
            .build()
            .unwrap();

        RegionDescriptorBuilder::default()
            .id(1)
            .name("test")
            .row_key(row_key)
            .default_cf(default_cf)
            .compaction_time_window(Some(1677652502))
            .build()
            .unwrap()
    }

    #[test]
    fn test_alter_operation() {
        let mut desc = new_region_descriptor();

        let op = AlterOperation::AddColumns {
            columns: vec![
                AddColumn {
                    desc: new_column_desc(3),
                    is_key: true,
                },
                AddColumn {
                    desc: new_column_desc(4),
                    is_key: false,
                },
            ],
        };
        op.apply(&mut desc);

        assert_eq!(1, desc.row_key.columns.len());
        assert_eq!("3", desc.row_key.columns[0].name);
        assert_eq!(2, desc.default_cf.columns.len());
        assert_eq!("2", desc.default_cf.columns[0].name);
        assert_eq!("4", desc.default_cf.columns[1].name);

        let op = AlterOperation::DropColumns {
            names: vec![String::from("2")],
        };
        op.apply(&mut desc);
        assert_eq!(1, desc.row_key.columns.len());
        assert_eq!(1, desc.default_cf.columns.len());
        assert_eq!("4", desc.default_cf.columns[0].name);

        // Key columns are ignored.
        let op = AlterOperation::DropColumns {
            names: vec![String::from("1"), String::from("3")],
        };
        op.apply(&mut desc);
        assert_eq!(1, desc.row_key.columns.len());
        assert_eq!(1, desc.default_cf.columns.len());
    }
}
