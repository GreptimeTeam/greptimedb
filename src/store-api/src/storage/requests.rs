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

use api::helper::ColumnDataTypeWrapper;
use api::v1::region::{alter_request, AddColumn as PbAddColumn};
use api::v1::SemanticType;
use common_error::ext::ErrorExt;
use common_query::logical_plan::Expr;
use common_recordbatch::OrderOption;
use datatypes::vectors::VectorRef;
use snafu::{OptionExt, ResultExt};

use crate::error::{
    BuildColumnDescriptorSnafu, Error, InvalidDefaultConstraintSnafu, InvalidRawRegionRequestSnafu,
};
use crate::storage::{ColumnDescriptor, ColumnDescriptorBuilder, RegionDescriptor};

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

impl TryFrom<PbAddColumn> for AddColumn {
    type Error = Error;

    fn try_from(add_column: PbAddColumn) -> Result<Self, Self::Error> {
        let column_def = add_column
            .column_def
            .context(InvalidRawRegionRequestSnafu {
                err: "'column_def' is absent",
            })?;
        let column_id = column_def.column_id;

        let column_def = column_def
            .column_def
            .context(InvalidRawRegionRequestSnafu {
                err: "'column_def' is absent",
            })?;

        let data_type = column_def.data_type;
        let data_type_ext = column_def.datatype_extension.clone();
        let data_type = ColumnDataTypeWrapper::try_new(data_type, data_type_ext)
            .map_err(|_| {
                InvalidRawRegionRequestSnafu {
                    err: format!("unknown raw column datatype: {data_type}"),
                }
                .build()
            })?
            .into();

        let constraint = column_def.default_constraint.as_slice();
        let constraint = if constraint.is_empty() {
            None
        } else {
            Some(
                constraint
                    .try_into()
                    .context(InvalidDefaultConstraintSnafu {
                        constraint: String::from_utf8_lossy(constraint),
                    })?,
            )
        };

        let desc = ColumnDescriptorBuilder::new(column_id, column_def.name.clone(), data_type)
            .is_nullable(column_def.is_nullable)
            .is_time_index(column_def.semantic_type() == SemanticType::Timestamp)
            .default_constraint(constraint)
            .build()
            .context(BuildColumnDescriptorSnafu)?;

        Ok(AddColumn {
            desc,
            is_key: column_def.semantic_type() == SemanticType::Tag,
            // TODO(ruihang & yingwen): support alter column's "location"
        })
    }
}

impl TryFrom<alter_request::Kind> for AlterOperation {
    type Error = Error;

    fn try_from(kind: alter_request::Kind) -> Result<Self, Self::Error> {
        let operation = match kind {
            alter_request::Kind::AddColumns(x) => {
                let columns = x
                    .add_columns
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<Result<Vec<_>, Self::Error>>()?;
                AlterOperation::AddColumns { columns }
            }
            alter_request::Kind::DropColumns(x) => {
                let names = x.drop_columns.into_iter().map(|x| x.name).collect();
                AlterOperation::DropColumns { names }
            }
        };
        Ok(operation)
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
    use api::v1::region::{
        AddColumn as PbAddColumn, AddColumns, DropColumn, DropColumns, RegionColumnDef,
    };
    use api::v1::{ColumnDataType, ColumnDef};
    use datatypes::prelude::*;
    use datatypes::schema::ColumnDefaultConstraint;

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

    #[test]
    fn test_try_from_raw_alter_kind() {
        let kind = alter_request::Kind::AddColumns(AddColumns {
            add_columns: vec![
                PbAddColumn {
                    column_def: Some(RegionColumnDef {
                        column_def: Some(ColumnDef {
                            name: "my_tag".to_string(),
                            data_type: ColumnDataType::Int32 as _,
                            is_nullable: false,
                            default_constraint: vec![],
                            semantic_type: SemanticType::Tag as _,
                            comment: String::new(),
                            ..Default::default()
                        }),
                        column_id: 1,
                    }),
                    location: None,
                },
                PbAddColumn {
                    column_def: Some(RegionColumnDef {
                        column_def: Some(ColumnDef {
                            name: "my_field".to_string(),
                            data_type: ColumnDataType::String as _,
                            is_nullable: true,
                            default_constraint: ColumnDefaultConstraint::Value("hello".into())
                                .try_into()
                                .unwrap(),
                            semantic_type: SemanticType::Field as _,
                            comment: String::new(),
                            ..Default::default()
                        }),
                        column_id: 2,
                    }),
                    location: None,
                },
            ],
        });

        let AlterOperation::AddColumns { columns } = AlterOperation::try_from(kind).unwrap() else {
            unreachable!()
        };
        assert_eq!(2, columns.len());

        let desc = &columns[0].desc;
        assert_eq!(desc.id, 1);
        assert_eq!(&desc.name, "my_tag");
        assert_eq!(desc.data_type, ConcreteDataType::int32_datatype());
        assert!(!desc.is_nullable());
        assert!(!desc.is_time_index());
        assert_eq!(desc.default_constraint(), None);
        assert!(columns[0].is_key);

        let desc = &columns[1].desc;
        assert_eq!(desc.id, 2);
        assert_eq!(&desc.name, "my_field");
        assert_eq!(desc.data_type, ConcreteDataType::string_datatype());
        assert!(desc.is_nullable());
        assert!(!desc.is_time_index());
        assert_eq!(
            desc.default_constraint(),
            Some(&ColumnDefaultConstraint::Value("hello".into()))
        );
        assert!(!columns[1].is_key);

        let kind = alter_request::Kind::DropColumns(DropColumns {
            drop_columns: vec![
                DropColumn {
                    name: "c1".to_string(),
                },
                DropColumn {
                    name: "c2".to_string(),
                },
            ],
        });

        let AlterOperation::DropColumns { names } = AlterOperation::try_from(kind).unwrap() else {
            unreachable!()
        };
        assert_eq!(names, vec!["c1", "c2"]);
    }
}
