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

use std::fmt::Display;

use datatypes::data_type::ConcreteDataType;
use sql::statements::concrete_data_type_to_sql_data_type;

use crate::error::{Error, Result};
use crate::ir::alter_expr::{AlterTableOperation, AlterTableOption};
use crate::ir::create_expr::ColumnOption;
use crate::ir::{AlterTableExpr, Column};
use crate::translator::postgres::sql_data_type_to_postgres_data_type;
use crate::translator::DslTranslator;

pub struct AlterTableExprTranslator;

impl DslTranslator<AlterTableExpr, String> for AlterTableExprTranslator {
    type Error = Error;

    fn translate(&self, input: &AlterTableExpr) -> Result<String> {
        Ok(match &input.alter_kinds {
            AlterTableOperation::AddColumn { column, .. } => {
                Self::format_add_column(&input.table_name, column)
            }
            AlterTableOperation::DropColumn { name } => Self::format_drop(&input.table_name, name),
            AlterTableOperation::RenameTable { new_table_name } => {
                Self::format_rename(&input.table_name, new_table_name)
            }
            AlterTableOperation::ModifyDataType { column } => {
                Self::format_modify_data_type(&input.table_name, column)
            }
            AlterTableOperation::SetTableOptions { options } => {
                Self::format_set_table_options(&input.table_name, options)
            }
            AlterTableOperation::UnsetTableOptions { keys } => {
                Self::format_unset_table_options(&input.table_name, keys)
            }
        })
    }
}

impl AlterTableExprTranslator {
    fn format_drop(name: impl Display, column: impl Display) -> String {
        format!("ALTER TABLE {name} DROP COLUMN {column};")
    }

    fn format_rename(name: impl Display, new_name: impl Display) -> String {
        format!("ALTER TABLE {name} RENAME TO {new_name};")
    }

    fn format_add_column(name: impl Display, column: &Column) -> String {
        format!(
            "{};",
            vec![format!(
                "ALTER TABLE {name} ADD COLUMN {}",
                Self::format_column(column)
            ),]
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
        )
    }

    fn format_modify_data_type(name: impl Display, column: &Column) -> String {
        format!(
            "ALTER TABLE {name} MODIFY COLUMN {};",
            Self::format_column(column)
        )
    }

    fn format_column(column: &Column) -> String {
        vec![
            column.name.to_string(),
            Self::format_column_type(&column.column_type),
            Self::format_column_options(&column.options),
        ]
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
    }

    fn format_column_type(column_type: &ConcreteDataType) -> String {
        sql_data_type_to_postgres_data_type(
            // Safety: We don't use the `Dictionary` type
            concrete_data_type_to_sql_data_type(column_type).unwrap(),
        )
    }

    fn acceptable_column_option(option: &ColumnOption) -> bool {
        matches!(
            option,
            ColumnOption::Null
                | ColumnOption::NotNull
                | ColumnOption::DefaultValue(_)
                | ColumnOption::DefaultFn(_)
        )
    }

    fn format_column_options(options: &[ColumnOption]) -> String {
        options
            .iter()
            .filter(|opt| Self::acceptable_column_option(opt))
            .map(|option| option.to_string())
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn format_set_table_options(name: impl Display, options: &[AlterTableOption]) -> String {
        format!(
            "ALTER TABLE {name} SET {};",
            options
                .iter()
                .map(|option| option.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn format_unset_table_options(name: impl Display, keys: &[String]) -> String {
        format!(
            "ALTER TABLE {name} UNSET {};",
            keys.iter()
                .map(|key| format!("'{}'", key))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use common_base::readable_size::ReadableSize;
    use common_query::AddColumnLocation;
    use common_time::Duration;
    use datatypes::data_type::ConcreteDataType;

    use super::AlterTableExprTranslator;
    use crate::ir::alter_expr::{AlterTableOperation, AlterTableOption};
    use crate::ir::create_expr::ColumnOption;
    use crate::ir::{AlterTableExpr, Column};
    use crate::translator::DslTranslator;

    #[test]
    fn test_alter_table_expr() {
        let alter_expr = AlterTableExpr {
            table_name: "test".into(),
            alter_kinds: AlterTableOperation::AddColumn {
                column: Column {
                    name: "host".into(),
                    column_type: ConcreteDataType::string_datatype(),
                    options: vec![ColumnOption::PrimaryKey],
                },
                location: Some(AddColumnLocation::First),
            },
        };

        let output = AlterTableExprTranslator.translate(&alter_expr).unwrap();
        // Ignores the location and primary key option.
        assert_eq!("ALTER TABLE test ADD COLUMN host STRING;", output);

        let alter_expr = AlterTableExpr {
            table_name: "test".into(),
            alter_kinds: AlterTableOperation::RenameTable {
                new_table_name: "foo".into(),
            },
        };

        let output = AlterTableExprTranslator.translate(&alter_expr).unwrap();
        assert_eq!("ALTER TABLE test RENAME TO foo;", output);

        let alter_expr = AlterTableExpr {
            table_name: "test".into(),
            alter_kinds: AlterTableOperation::DropColumn { name: "foo".into() },
        };

        let output = AlterTableExprTranslator.translate(&alter_expr).unwrap();
        assert_eq!("ALTER TABLE test DROP COLUMN foo;", output);

        let alter_expr = AlterTableExpr {
            table_name: "test".into(),
            alter_kinds: AlterTableOperation::ModifyDataType {
                column: Column {
                    name: "host".into(),
                    column_type: ConcreteDataType::string_datatype(),
                    options: vec![],
                },
            },
        };

        let output = AlterTableExprTranslator.translate(&alter_expr).unwrap();
        // Ignores the location and primary key option.
        assert_eq!("ALTER TABLE test MODIFY COLUMN host STRING;", output);
    }

    #[test]
    fn test_alter_table_expr_set_table_options() {
        let alter_expr = AlterTableExpr {
            table_name: "test".into(),
            alter_kinds: AlterTableOperation::SetTableOptions {
                options: vec![
                    AlterTableOption::Ttl(Duration::new_second(60)),
                    AlterTableOption::TwcsTimeWindow(Duration::new_second(60)),
                    AlterTableOption::TwcsMaxOutputFileSize(ReadableSize::from_str("1GB").unwrap()),
                    AlterTableOption::TwcsMaxActiveWindowFiles(10),
                    AlterTableOption::TwcsMaxActiveWindowRuns(10),
                    AlterTableOption::TwcsMaxInactiveWindowFiles(5),
                    AlterTableOption::TwcsMaxInactiveWindowRuns(5),
                ],
            },
        };

        let output = AlterTableExprTranslator.translate(&alter_expr).unwrap();
        let expected = concat!(
            "ALTER TABLE test SET 'ttl' = '60s', ",
            "'compaction.twcs.time_window' = '60s', ",
            "'compaction.twcs.max_output_file_size' = '1.0GiB', ",
            "'compaction.twcs.max_active_window_files' = '10', ",
            "'compaction.twcs.max_active_window_runs' = '10', ",
            "'compaction.twcs.max_inactive_window_files' = '5', ",
            "'compaction.twcs.max_inactive_window_runs' = '5';"
        );
        assert_eq!(expected, output);
    }

    #[test]
    fn test_alter_table_expr_unset_table_options() {
        let alter_expr = AlterTableExpr {
            table_name: "test".into(),
            alter_kinds: AlterTableOperation::UnsetTableOptions {
                keys: vec!["ttl".into(), "compaction.twcs.time_window".into()],
            },
        };

        let output = AlterTableExprTranslator.translate(&alter_expr).unwrap();
        let expected = "ALTER TABLE test UNSET 'ttl', 'compaction.twcs.time_window';";
        assert_eq!(expected, output);
    }
}
