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

use api::v1::{alter_expr, AddColumn, AlterExpr, DropColumn};
use sqlparser::ast::{ColumnDef, Ident, ObjectName, TableConstraint};

use crate::error::UnsupportedAlterTableStatementSnafu;
use crate::statements::{sql_column_def_to_grpc_column_def, table_idents_to_full_name};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTable {
    table_name: ObjectName,
    alter_operation: AlterTableOperation,
}

impl AlterTable {
    pub(crate) fn new(table_name: ObjectName, alter_operation: AlterTableOperation) -> Self {
        Self {
            table_name,
            alter_operation,
        }
    }

    pub fn table_name(&self) -> &ObjectName {
        &self.table_name
    }

    pub fn alter_operation(&self) -> &AlterTableOperation {
        &self.alter_operation
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTableOperation {
    /// `ADD <table_constraint>`
    AddConstraint(TableConstraint),
    /// `ADD [ COLUMN ] <column_def>`
    AddColumn { column_def: ColumnDef },
    /// `DROP COLUMN <name>`
    DropColumn { name: Ident },
    /// `RENAME <new_table_name>`
    RenameTable { new_table_name: ObjectName },
}

/// Convert `AlterTable` statement to `AlterExpr` for gRPC
impl TryFrom<AlterTable> for AlterExpr {
    type Error = crate::error::Error;

    fn try_from(value: AlterTable) -> Result<Self, Self::Error> {
        let (catalog_name, schema_name, table_name) = table_idents_to_full_name(&value.table_name)?;

        let kind = match value.alter_operation {
            AlterTableOperation::AddConstraint(_) => {
                return UnsupportedAlterTableStatementSnafu {
                    msg: "ADD CONSTRAINT not supported yet.",
                }
                .fail();
            }
            AlterTableOperation::AddColumn { column_def } => {
                alter_expr::Kind::AddColumns(api::v1::AddColumns {
                    add_columns: vec![AddColumn {
                        column_def: Some(sql_column_def_to_grpc_column_def(column_def)?),
                        is_key: false,
                    }],
                })
            }
            AlterTableOperation::DropColumn { name } => {
                alter_expr::Kind::DropColumns(api::v1::DropColumns {
                    drop_columns: vec![DropColumn { name: name.value }],
                })
            }
            AlterTableOperation::RenameTable { .. } => {
                // TODO update proto to support alter table name
                return UnsupportedAlterTableStatementSnafu {
                    msg: "rename table not supported yet",
                }
                .fail();
            }
        };
        let expr = AlterExpr {
            catalog_name,
            schema_name,
            table_name,
            kind: Some(kind),
        };

        Ok(expr)
    }
}
