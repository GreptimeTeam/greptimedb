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

#[cfg(feature = "enterprise")]
pub mod trigger;

use std::fmt::{Debug, Display};

use api::v1;
use common_query::AddColumnLocation;
use datatypes::schema::{FulltextOptions, SkippingIndexOptions};
use itertools::Itertools;
use serde::Serialize;
use sqlparser::ast::{ColumnDef, DataType, Expr, Ident, ObjectName, TableConstraint};
use sqlparser_derive::{Visit, VisitMut};

use crate::statements::OptionMap;

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct AlterTable {
    pub table_name: ObjectName,
    pub alter_operation: AlterTableOperation,
    /// Table options in `WITH`. All keys are lowercase.
    pub options: OptionMap,
}

impl AlterTable {
    pub(crate) fn new(
        table_name: ObjectName,
        alter_operation: AlterTableOperation,
        options: OptionMap,
    ) -> Self {
        Self {
            table_name,
            alter_operation,
            options,
        }
    }

    pub fn table_name(&self) -> &ObjectName {
        &self.table_name
    }

    pub fn alter_operation(&self) -> &AlterTableOperation {
        &self.alter_operation
    }

    pub fn options(&self) -> &OptionMap {
        &self.options
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

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum AlterTableOperation {
    /// `ADD <table_constraint>`
    AddConstraint(TableConstraint),
    /// `ADD [ COLUMN ] <column_def> [location]`
    AddColumns {
        add_columns: Vec<AddColumn>,
    },
    /// `MODIFY <column_name> [target_type]`
    ModifyColumnType {
        column_name: Ident,
        target_type: DataType,
    },
    /// `SET <table attrs key> = <table attr value>`
    SetTableOptions {
        options: Vec<KeyValueOption>,
    },
    /// `UNSET <table attrs key>`
    UnsetTableOptions {
        keys: Vec<String>,
    },
    /// `DROP COLUMN <name>`
    DropColumn {
        name: Ident,
    },
    /// `RENAME <new_table_name>`
    RenameTable {
        new_table_name: String,
    },
    SetIndex {
        options: SetIndexOperation,
    },
    UnsetIndex {
        options: UnsetIndexOperation,
    },
    DropDefaults {
        columns: Vec<DropDefaultsOperation>,
    },
    /// `ALTER <column_name> SET DEFAULT <default_value>`
    SetDefaults {
        defaults: Vec<SetDefaultsOperation>,
    },
    /// `REPARTITION (...) INTO (...)`
    Repartition {
        operation: RepartitionOperation,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
/// `ALTER <column_name> DROP DEFAULT`
pub struct DropDefaultsOperation(pub Ident);

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct SetDefaultsOperation {
    pub column_name: Ident,
    pub default_constraint: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct RepartitionOperation {
    pub from_exprs: Vec<Expr>,
    pub into_exprs: Vec<Expr>,
}

impl RepartitionOperation {
    pub fn new(from_exprs: Vec<Expr>, into_exprs: Vec<Expr>) -> Self {
        Self {
            from_exprs,
            into_exprs,
        }
    }
}

impl Display for RepartitionOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let from = self
            .from_exprs
            .iter()
            .map(|expr| expr.to_string())
            .join(", ");
        let into = self
            .into_exprs
            .iter()
            .map(|expr| expr.to_string())
            .join(", ");

        write!(f, "({from}) INTO ({into})")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum SetIndexOperation {
    /// `MODIFY COLUMN <column_name> SET FULLTEXT INDEX [WITH <options>]`
    Fulltext {
        column_name: Ident,
        options: FulltextOptions,
    },
    /// `MODIFY COLUMN <column_name> SET INVERTED INDEX`
    Inverted { column_name: Ident },
    /// `MODIFY COLUMN <column_name> SET SKIPPING INDEX`
    Skipping {
        column_name: Ident,
        options: SkippingIndexOptions,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum UnsetIndexOperation {
    /// `MODIFY COLUMN <column_name> UNSET FULLTEXT INDEX`
    Fulltext { column_name: Ident },
    /// `MODIFY COLUMN <column_name> UNSET INVERTED INDEX`
    Inverted { column_name: Ident },
    /// `MODIFY COLUMN <column_name> UNSET SKIPPING INDEX`
    Skipping { column_name: Ident },
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct AddColumn {
    pub column_def: ColumnDef,
    pub location: Option<AddColumnLocation>,
    pub add_if_not_exists: bool,
}

impl Display for AddColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(location) = &self.location {
            write!(f, "{} {location}", self.column_def)
        } else {
            write!(f, "{}", self.column_def)
        }
    }
}

impl Display for AlterTableOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlterTableOperation::AddConstraint(constraint) => write!(f, r#"ADD {constraint}"#),
            AlterTableOperation::AddColumns { add_columns } => {
                let columns = add_columns
                    .iter()
                    .map(|add_column| format!("ADD COLUMN {add_column}"))
                    .join(", ");
                write!(f, "{columns}")
            }
            AlterTableOperation::DropColumn { name } => write!(f, r#"DROP COLUMN {name}"#),
            AlterTableOperation::RenameTable { new_table_name } => {
                write!(f, r#"RENAME {new_table_name}"#)
            }
            AlterTableOperation::ModifyColumnType {
                column_name,
                target_type,
            } => {
                write!(f, r#"MODIFY COLUMN {column_name} {target_type}"#)
            }
            AlterTableOperation::SetTableOptions { options } => {
                let kvs = options
                    .iter()
                    .map(|KeyValueOption { key, value }| {
                        if !value.is_empty() {
                            format!("'{key}'='{value}'")
                        } else {
                            format!("'{key}'=NULL")
                        }
                    })
                    .join(",");

                write!(f, "SET {kvs}")
            }
            AlterTableOperation::UnsetTableOptions { keys } => {
                let keys = keys.iter().map(|k| format!("'{k}'")).join(",");
                write!(f, "UNSET {keys}")
            }
            AlterTableOperation::Repartition { operation } => {
                write!(f, "REPARTITION {operation}")
            }
            AlterTableOperation::SetIndex { options } => match options {
                SetIndexOperation::Fulltext {
                    column_name,
                    options,
                } => {
                    write!(
                        f,
                        "MODIFY COLUMN {column_name} SET FULLTEXT INDEX WITH(analyzer={0}, case_sensitive={1}, backend={2})",
                        options.analyzer, options.case_sensitive, options.backend
                    )
                }
                SetIndexOperation::Inverted { column_name } => {
                    write!(f, "MODIFY COLUMN {column_name} SET INVERTED INDEX")
                }
                SetIndexOperation::Skipping {
                    column_name,
                    options,
                } => {
                    write!(
                        f,
                        "MODIFY COLUMN {column_name} SET SKIPPING INDEX WITH(granularity={0}, index_type={1})",
                        options.granularity, options.index_type
                    )
                }
            },
            AlterTableOperation::UnsetIndex { options } => match options {
                UnsetIndexOperation::Fulltext { column_name } => {
                    write!(f, "MODIFY COLUMN {column_name} UNSET FULLTEXT INDEX")
                }
                UnsetIndexOperation::Inverted { column_name } => {
                    write!(f, "MODIFY COLUMN {column_name} UNSET INVERTED INDEX")
                }
                UnsetIndexOperation::Skipping { column_name } => {
                    write!(f, "MODIFY COLUMN {column_name} UNSET SKIPPING INDEX")
                }
            },
            AlterTableOperation::DropDefaults { columns } => {
                let columns = columns
                    .iter()
                    .map(|column| format!("MODIFY COLUMN {} DROP DEFAULT", column.0))
                    .join(", ");
                write!(f, "{columns}")
            }
            AlterTableOperation::SetDefaults { defaults } => {
                let defaults = defaults
                    .iter()
                    .map(|column| {
                        format!(
                            "MODIFY COLUMN {} SET DEFAULT {}",
                            column.column_name, column.default_constraint
                        )
                    })
                    .join(", ");
                write!(f, "{defaults}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct KeyValueOption {
    pub key: String,
    pub value: String,
}

impl From<KeyValueOption> for v1::Option {
    fn from(c: KeyValueOption) -> Self {
        v1::Option {
            key: c.key,
            value: c.value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct AlterDatabase {
    pub database_name: ObjectName,
    pub alter_operation: AlterDatabaseOperation,
}

impl AlterDatabase {
    pub(crate) fn new(database_name: ObjectName, alter_operation: AlterDatabaseOperation) -> Self {
        Self {
            database_name,
            alter_operation,
        }
    }

    pub fn database_name(&self) -> &ObjectName {
        &self.database_name
    }

    pub fn alter_operation(&self) -> &AlterDatabaseOperation {
        &self.alter_operation
    }
}

impl Display for AlterDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let database_name = self.database_name();
        let alter_operation = self.alter_operation();
        write!(f, r#"ALTER DATABASE {database_name} {alter_operation}"#)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum AlterDatabaseOperation {
    SetDatabaseOption { options: Vec<KeyValueOption> },
    UnsetDatabaseOption { keys: Vec<String> },
}

impl Display for AlterDatabaseOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlterDatabaseOperation::SetDatabaseOption { options } => {
                let kvs = options
                    .iter()
                    .map(|KeyValueOption { key, value }| {
                        if !value.is_empty() {
                            format!("'{key}'='{value}'")
                        } else {
                            format!("'{key}'=NULL")
                        }
                    })
                    .join(",");

                write!(f, "SET {kvs}")?;

                Ok(())
            }
            AlterDatabaseOperation::UnsetDatabaseOption { keys } => {
                let keys = keys.iter().map(|key| format!("'{key}'")).join(",");
                write!(f, "UNSET {keys}")?;

                Ok(())
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
        let sql = r"ALTER DATABASE db SET 'a' = 'b', 'c' = 'd'";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::AlterDatabase { .. });

        match &stmts[0] {
            Statement::AlterDatabase(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
ALTER DATABASE db SET 'a'='b','c'='d'"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = r"ALTER DATABASE db UNSET 'a', 'c'";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());

        match &stmts[0] {
            Statement::AlterDatabase(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
ALTER DATABASE db UNSET 'a','c'"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql =
            r"alter table monitor add column app string default 'shop' primary key, add foo INT;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::AlterTable { .. });

        match &stmts[0] {
            Statement::AlterTable(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
ALTER TABLE monitor ADD COLUMN app STRING DEFAULT 'shop' PRIMARY KEY, ADD COLUMN foo INT"#,
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
        assert_matches!(&stmts[0], Statement::AlterTable { .. });

        match &stmts[0] {
            Statement::AlterTable(set) => {
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
        assert_matches!(&stmts[0], Statement::AlterTable { .. });

        match &stmts[0] {
            Statement::AlterTable(set) => {
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
        assert_matches!(&stmts[0], Statement::AlterTable { .. });

        match &stmts[0] {
            Statement::AlterTable(set) => {
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

        let sql = "ALTER TABLE monitor MODIFY COLUMN a SET FULLTEXT INDEX WITH(analyzer='English',case_sensitive='false',backend='bloom')";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::AlterTable { .. });

        match &stmts[0] {
            Statement::AlterTable(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
ALTER TABLE monitor MODIFY COLUMN a SET FULLTEXT INDEX WITH(analyzer=English, case_sensitive=false, backend=bloom)"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = "ALTER TABLE monitor MODIFY COLUMN a UNSET FULLTEXT INDEX";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::AlterTable { .. });

        match &stmts[0] {
            Statement::AlterTable(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
ALTER TABLE monitor MODIFY COLUMN a UNSET FULLTEXT INDEX"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = "ALTER TABLE monitor MODIFY COLUMN a SET INVERTED INDEX";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::AlterTable { .. });

        match &stmts[0] {
            Statement::AlterTable(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
ALTER TABLE monitor MODIFY COLUMN a SET INVERTED INDEX"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = "ALTER TABLE monitor MODIFY COLUMN a DROP DEFAULT";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::AlterTable { .. });

        match &stmts[0] {
            Statement::AlterTable(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
ALTER TABLE monitor MODIFY COLUMN a DROP DEFAULT"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = "ALTER TABLE monitor MODIFY COLUMN a SET DEFAULT 'default_for_a'";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::AlterTable { .. });

        match &stmts[0] {
            Statement::AlterTable(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
ALTER TABLE monitor MODIFY COLUMN a SET DEFAULT 'default_for_a'"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }
}
