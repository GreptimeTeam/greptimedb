use api::v1::{alter_expr, AlterExpr};
use sqlparser::ast::{ColumnDef, ObjectName, TableConstraint};

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
    // TODO(hl): support remove column
}

/// Convert `AlterTable` statement to `AlterExpr` for gRPC
impl TryFrom<AlterTable> for AlterExpr {
    type Error = crate::error::Error;

    fn try_from(value: AlterTable) -> Result<Self, Self::Error> {
        let (catalog, schema, table) = table_idents_to_full_name(&value.table_name).unwrap();

        let kind = match value.alter_operation {
            AlterTableOperation::AddConstraint(_) => {
                return UnsupportedAlterTableStatementSnafu {
                    msg: "ADD CONSTRAINT not supported yet.",
                }
                .fail();
            }
            AlterTableOperation::AddColumn { column_def } => {
                alter_expr::Kind::AddColumn(api::v1::AddColumn {
                    column_def: Some(sql_column_def_to_grpc_column_def(column_def)?),
                })
            }
        };
        let expr = AlterExpr {
            catalog_name: Some(catalog),
            schema_name: Some(schema),
            table_name: table,
            kind: Some(kind),
        };

        Ok(expr)
    }
}
