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

use api::v1::alter_expr::Kind;
use api::v1::{AlterExpr, DropColumns};
use snafu::OptionExt;
use table::requests::{AddColumnRequest, AlterKind, AlterTableRequest};

use crate::column::column_def_to_column_schema;
use crate::error::{MissingFieldSnafu, Result};

/// Convert an [`AlterExpr`] to an optional [`AlterTableRequest`]
pub fn alter_expr_to_request(expr: AlterExpr) -> Result<Option<AlterTableRequest>> {
    match expr.kind {
        Some(Kind::AddColumns(add_columns)) => {
            let mut add_column_requests = vec![];
            for add_column_expr in add_columns.add_columns {
                let column_def = add_column_expr.column_def.context(MissingFieldSnafu {
                    field: "column_def",
                })?;

                let schema = column_def_to_column_schema(&column_def)?;
                add_column_requests.push(AddColumnRequest {
                    column_schema: schema,
                    is_key: add_column_expr.is_key,
                })
            }

            Ok(Some(AlterTableRequest {
                catalog_name: expr.catalog_name,
                schema_name: expr.schema_name,
                table_name: expr.table_name,
                alter_kind: AlterKind::AddColumns {
                    columns: add_column_requests,
                },
            }))
        }
        Some(Kind::DropColumns(DropColumns { drop_columns })) => {
            let alter_kind = AlterKind::DropColumns {
                names: drop_columns.into_iter().map(|c| c.name).collect(),
            };

            let request = AlterTableRequest {
                catalog_name: expr.catalog_name,
                schema_name: expr.schema_name,
                table_name: expr.table_name,
                alter_kind,
            };
            Ok(Some(request))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use api::v1::{AddColumn, AddColumns, ColumnDataType, ColumnDef, DropColumn};
    use datatypes::prelude::ConcreteDataType;

    use super::*;

    #[test]
    fn test_alter_expr_to_request() {
        let expr = AlterExpr {
            catalog_name: None,
            schema_name: None,
            table_name: "monitor".to_string(),

            kind: Some(Kind::AddColumns(AddColumns {
                add_columns: vec![AddColumn {
                    column_def: Some(ColumnDef {
                        name: "mem_usage".to_string(),
                        datatype: ColumnDataType::Float64 as i32,
                        is_nullable: false,
                        default_constraint: None,
                    }),
                    is_key: false,
                }],
            })),
        };

        let alter_request = alter_expr_to_request(expr).unwrap().unwrap();
        assert_eq!(None, alter_request.catalog_name);
        assert_eq!(None, alter_request.schema_name);
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
    }

    #[test]
    fn test_drop_column_expr() {
        let expr = AlterExpr {
            catalog_name: Some("test_catalog".to_string()),
            schema_name: Some("test_schema".to_string()),
            table_name: "monitor".to_string(),

            kind: Some(Kind::DropColumns(DropColumns {
                drop_columns: vec![DropColumn {
                    name: "mem_usage".to_string(),
                }],
            })),
        };

        let alter_request = alter_expr_to_request(expr).unwrap().unwrap();
        assert_eq!(Some("test_catalog".to_string()), alter_request.catalog_name);
        assert_eq!(Some("test_schema".to_string()), alter_request.schema_name);
        assert_eq!("monitor".to_string(), alter_request.table_name);

        let mut drop_names = match alter_request.alter_kind {
            AlterKind::DropColumns { names } => names,
            _ => unreachable!(),
        };
        assert_eq!(1, drop_names.len());
        assert_eq!("mem_usage".to_string(), drop_names.pop().unwrap());
    }
}
