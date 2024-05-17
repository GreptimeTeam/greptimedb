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

use std::fmt::{Debug, Display};

use common_query::AddColumnLocation;
use sqlparser::ast::{ColumnDef, DataType, Ident, ObjectName, TableConstraint};
use sqlparser_derive::{Visit, VisitMut};

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
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

    pub fn alter_operation_mut(&mut self) -> &mut AlterTableOperation {
        &mut self.alter_operation
    }
}

impl Display for AlterTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let table_name = self.table_name();
        let alter_operation = self.alter_operation();
        write!(f, r#"ALTER TABLE {table_name} {alter_operation}"#)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub enum AlterTableOperation {
    /// `ADD <table_constraint>`
    AddConstraint(TableConstraint),
    /// `ADD [ COLUMN ] <column_def> [location]`
    AddColumn {
        column_def: ColumnDef,
        location: Option<AddColumnLocation>,
    },
    /// `MODIFY <column_name> [target_type]`
    ChangeColumnType {
        column_name: Ident,
        target_type: DataType,
    },
    /// `DROP COLUMN <name>`
    DropColumn { name: Ident },
    /// `RENAME <new_table_name>`
    RenameTable { new_table_name: String },
}

impl Display for AlterTableOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlterTableOperation::AddConstraint(constraint) => write!(f, r#"ADD {constraint}"#),
            AlterTableOperation::AddColumn {
                column_def,
                location,
            } => {
                if let Some(location) = location {
                    write!(f, r#"ADD COLUMN {column_def} {location}"#)
                } else {
                    write!(f, r#"ADD COLUMN {column_def}"#)
                }
            }
            AlterTableOperation::DropColumn { name } => write!(f, r#"DROP COLUMN {name}"#),
            AlterTableOperation::RenameTable { new_table_name } => {
                write!(f, r#"RENAME {new_table_name}"#)
            }
            AlterTableOperation::ChangeColumnType {
                column_name,
                target_type,
            } => {
                write!(f, r#"MODIFY COLUMN {column_name} {target_type}"#)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::statement::Statement;

    #[test]
    fn test_display_alter() {
        let sql = r"alter table monitor add column app string default 'shop' primary key;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::Alter { .. });

        match &stmts[0] {
            Statement::Alter(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
ALTER TABLE monitor ADD COLUMN app STRING DEFAULT 'shop' PRIMARY KEY"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = r"alter table monitor modify column load_15 string;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::Alter { .. });

        match &stmts[0] {
            Statement::Alter(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
ALTER TABLE monitor MODIFY COLUMN load_15 STRING"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = r"alter table monitor drop column load_15;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::Alter { .. });

        match &stmts[0] {
            Statement::Alter(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
ALTER TABLE monitor DROP COLUMN load_15"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = r"alter table monitor rename monitor_new;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::Alter { .. });

        match &stmts[0] {
            Statement::Alter(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
ALTER TABLE monitor RENAME monitor_new"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }
}
