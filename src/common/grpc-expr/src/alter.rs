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

use api::v1::add_column_location::LocationType;
use api::v1::alter_expr::Kind;
use api::v1::{
    column_def, AddColumnLocation as Location, AlterExpr, CreateTableExpr, DropColumns,
    RenameTable, SemanticType,
};
use common_query::AddColumnLocation;
use datatypes::schema::{ColumnSchema, RawSchema};
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::TableId;
use table::requests::{AddColumnRequest, AlterKind, AlterTableRequest};

use crate::error::{
    InvalidColumnDefSnafu, MissingFieldSnafu, MissingTimestampColumnSnafu, Result,
    UnknownLocationTypeSnafu,
};

const LOCATION_TYPE_FIRST: i32 = LocationType::First as i32;
const LOCATION_TYPE_AFTER: i32 = LocationType::After as i32;

/// Convert an [`AlterExpr`] to an [`AlterTableRequest`]
pub fn alter_expr_to_request(table_id: TableId, expr: AlterExpr) -> Result<AlterTableRequest> {
    let catalog_name = expr.catalog_name;
    let schema_name = expr.schema_name;
    let kind = expr.kind.context(MissingFieldSnafu { field: "kind" })?;
    let alter_kind = match kind {
        Kind::AddColumns(add_columns) => {
            let add_column_requests = add_columns
                .add_columns
                .into_iter()
                .map(|ac| {
                    let column_def = ac.column_def.context(MissingFieldSnafu {
                        field: "column_def",
                    })?;

                    let schema = column_def::try_as_column_schema(&column_def).context(
                        InvalidColumnDefSnafu {
                            column: &column_def.name,
                        },
                    )?;
                    Ok(AddColumnRequest {
                        column_schema: schema,
                        is_key: column_def.semantic_type == SemanticType::Tag as i32,
                        location: parse_location(ac.location)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            AlterKind::AddColumns {
                columns: add_column_requests,
            }
        }
        Kind::DropColumns(DropColumns { drop_columns }) => AlterKind::DropColumns {
            names: drop_columns.into_iter().map(|c| c.name).collect(),
        },
        Kind::RenameTable(RenameTable { new_table_name }) => {
            AlterKind::RenameTable { new_table_name }
        }
    };

    let request = AlterTableRequest {
        catalog_name,
        schema_name,
        table_name: expr.table_name,
        table_id,
        alter_kind,
        table_version: None,
    };
    Ok(request)
}

pub fn create_table_schema(expr: &CreateTableExpr, require_time_index: bool) -> Result<RawSchema> {
    let column_schemas = expr
        .column_defs
        .iter()
        .map(|x| {
            column_def::try_as_column_schema(x).context(InvalidColumnDefSnafu { column: &x.name })
        })
        .collect::<Result<Vec<ColumnSchema>>>()?;

    // allow external table schema without the time index
    if require_time_index {
        ensure!(
            column_schemas
                .iter()
                .any(|column| column.name == expr.time_index),
            MissingTimestampColumnSnafu {
                msg: format!("CreateExpr: {expr:?}")
            }
        );
    }

    let column_schemas = column_schemas
        .into_iter()
        .map(|column_schema| {
            if column_schema.name == expr.time_index {
                column_schema.with_time_index(true)
            } else {
                column_schema
            }
        })
        .collect::<Vec<_>>();

    Ok(RawSchema::new(column_schemas))
}

fn parse_location(location: Option<Location>) -> Result<Option<AddColumnLocation>> {
    match location {
        Some(Location {
            location_type: LOCATION_TYPE_FIRST,
            ..
        }) => Ok(Some(AddColumnLocation::First)),
        Some(Location {
            location_type: LOCATION_TYPE_AFTER,
            after_column_name,
        }) => Ok(Some(AddColumnLocation::After {
            column_name: after_column_name,
        })),
        Some(Location { location_type, .. }) => UnknownLocationTypeSnafu { location_type }.fail(),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use api::v1::{AddColumn, AddColumns, ColumnDataType, ColumnDef, DropColumn, SemanticType};
    use datatypes::prelude::ConcreteDataType;

    use super::*;

    #[test]
    fn test_alter_expr_to_request() {
        let expr = AlterExpr {
            catalog_name: "".to_string(),
            schema_name: "".to_string(),
            table_name: "monitor".to_string(),

            kind: Some(Kind::AddColumns(AddColumns {
                add_columns: vec![AddColumn {
                    column_def: Some(ColumnDef {
                        name: "mem_usage".to_string(),
                        data_type: ColumnDataType::Float64 as i32,
                        is_nullable: false,
                        default_constraint: vec![],
                        semantic_type: SemanticType::Field as i32,
                        comment: String::new(),
                        ..Default::default()
                    }),
                    location: None,
                }],
            })),
        };

        let alter_request = alter_expr_to_request(1, expr).unwrap();
        assert_eq!(alter_request.catalog_name, "");
        assert_eq!(alter_request.schema_name, "");
        assert_eq!("monitor".to_string(), alter_request.table_name);
        let add_column = match alter_request.alter_kind {
            AlterKind::AddColumns { mut columns } => columns.pop().unwrap(),
            _ => unreachable!(),
        };

        assert!(!add_column.is_key);
        assert_eq!("mem_usage", add_column.column_schema.name);
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            add_column.column_schema.data_type
        );
        assert_eq!(None, add_column.location);
    }

    #[test]
    fn test_alter_expr_with_location_to_request() {
        let expr = AlterExpr {
            catalog_name: "".to_string(),
            schema_name: "".to_string(),
            table_name: "monitor".to_string(),

            kind: Some(Kind::AddColumns(AddColumns {
                add_columns: vec![
                    AddColumn {
                        column_def: Some(ColumnDef {
                            name: "mem_usage".to_string(),
                            data_type: ColumnDataType::Float64 as i32,
                            is_nullable: false,
                            default_constraint: vec![],
                            semantic_type: SemanticType::Field as i32,
                            comment: String::new(),
                            ..Default::default()
                        }),
                        location: Some(Location {
                            location_type: LocationType::First.into(),
                            after_column_name: "".to_string(),
                        }),
                    },
                    AddColumn {
                        column_def: Some(ColumnDef {
                            name: "cpu_usage".to_string(),
                            data_type: ColumnDataType::Float64 as i32,
                            is_nullable: false,
                            default_constraint: vec![],
                            semantic_type: SemanticType::Field as i32,
                            comment: String::new(),
                            ..Default::default()
                        }),
                        location: Some(Location {
                            location_type: LocationType::After.into(),
                            after_column_name: "ts".to_string(),
                        }),
                    },
                ],
            })),
        };

        let alter_request = alter_expr_to_request(1, expr).unwrap();
        assert_eq!(alter_request.catalog_name, "");
        assert_eq!(alter_request.schema_name, "");
        assert_eq!("monitor".to_string(), alter_request.table_name);

        let mut add_columns = match alter_request.alter_kind {
            AlterKind::AddColumns { columns } => columns,
            _ => unreachable!(),
        };

        let add_column = add_columns.pop().unwrap();
        assert!(!add_column.is_key);
        assert_eq!("cpu_usage", add_column.column_schema.name);
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            add_column.column_schema.data_type
        );
        assert_eq!(
            Some(AddColumnLocation::After {
                column_name: "ts".to_string()
            }),
            add_column.location
        );

        let add_column = add_columns.pop().unwrap();
        assert!(!add_column.is_key);
        assert_eq!("mem_usage", add_column.column_schema.name);
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            add_column.column_schema.data_type
        );
        assert_eq!(Some(AddColumnLocation::First), add_column.location);
    }

    #[test]
    fn test_drop_column_expr() {
        let expr = AlterExpr {
            catalog_name: "test_catalog".to_string(),
            schema_name: "test_schema".to_string(),
            table_name: "monitor".to_string(),

            kind: Some(Kind::DropColumns(DropColumns {
                drop_columns: vec![DropColumn {
                    name: "mem_usage".to_string(),
                }],
            })),
        };

        let alter_request = alter_expr_to_request(1, expr).unwrap();
        assert_eq!(alter_request.catalog_name, "test_catalog");
        assert_eq!(alter_request.schema_name, "test_schema");
        assert_eq!("monitor".to_string(), alter_request.table_name);

        let mut drop_names = match alter_request.alter_kind {
            AlterKind::DropColumns { names } => names,
            _ => unreachable!(),
        };
        assert_eq!(1, drop_names.len());
        assert_eq!("mem_usage".to_string(), drop_names.pop().unwrap());
    }
}
