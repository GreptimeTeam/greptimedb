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

use common_query::AddColumnLocation;
use snafu::ResultExt;
use sqlparser::keywords::Keyword;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::alter::{AlterTable, AlterTableOperation};
use crate::statements::statement::Statement;

impl<'a> ParserContext<'a> {
    pub(crate) fn parse_alter(&mut self) -> Result<Statement> {
        let alter_table = self
            .parse_alter_table()
            .context(error::SyntaxSnafu { sql: self.sql })?;
        Ok(Statement::Alter(alter_table))
    }

    fn parse_alter_table(&mut self) -> std::result::Result<AlterTable, ParserError> {
        let parser = &mut self.parser;
        parser.expect_keywords(&[Keyword::ALTER, Keyword::TABLE])?;

        let table_name = parser.parse_object_name()?;

        let alter_operation = if parser.parse_keyword(Keyword::ADD) {
            if let Some(constraint) = parser.parse_optional_table_constraint()? {
                AlterTableOperation::AddConstraint(constraint)
            } else {
                let _ = parser.parse_keyword(Keyword::COLUMN);
                let column_def = parser.parse_column_def()?;
                let location = if parser.parse_keyword(Keyword::FIRST) {
                    Some(AddColumnLocation::First)
                } else if let Token::Word(word) = parser.peek_token().token {
                    if word.value.to_ascii_uppercase() == "AFTER" {
                        let _ = parser.next_token();
                        let name = parser.parse_identifier()?;
                        Some(AddColumnLocation::After {
                            column_name: name.value,
                        })
                    } else {
                        None
                    }
                } else {
                    None
                };
                AlterTableOperation::AddColumn {
                    column_def,
                    location,
                }
            }
        } else if parser.parse_keyword(Keyword::DROP) {
            if parser.parse_keyword(Keyword::COLUMN) {
                let name = self.parser.parse_identifier()?;
                AlterTableOperation::DropColumn { name }
            } else {
                return Err(ParserError::ParserError(format!(
                    "expect keyword COLUMN after ALTER TABLE DROP, found {}",
                    parser.peek_token()
                )));
            }
        } else if parser.parse_keyword(Keyword::RENAME) {
            let new_table_name_obj = parser.parse_object_name()?;
            let new_table_name = match &new_table_name_obj.0[..] {
                [table] => table.value.clone(),
                _ => {
                    return Err(ParserError::ParserError(format!(
                        "expect table name, actual: {new_table_name_obj}"
                    )))
                }
            };
            AlterTableOperation::RenameTable { new_table_name }
        } else {
            return Err(ParserError::ParserError(format!(
                "expect keyword ADD or DROP or RENAME after ALTER TABLE, found {}",
                parser.peek_token()
            )));
        };
        Ok(AlterTable::new(table_name, alter_operation))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_error::ext::ErrorExt;
    use sqlparser::ast::{ColumnOption, DataType};

    use super::*;
    use crate::dialect::GreptimeDbDialect;

    #[test]
    fn test_parse_alter_add_column() {
        let sql = "ALTER TABLE my_metric_1 ADD tagk_i STRING Null;";
        let mut result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        assert_matches!(statement, Statement::Alter { .. });
        match statement {
            Statement::Alter(alter_table) => {
                assert_eq!("my_metric_1", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_matches!(alter_operation, AlterTableOperation::AddColumn { .. });
                match alter_operation {
                    AlterTableOperation::AddColumn {
                        column_def,
                        location,
                    } => {
                        assert_eq!("tagk_i", column_def.name.value);
                        assert_eq!(DataType::String, column_def.data_type);
                        assert!(column_def
                            .options
                            .iter()
                            .any(|o| matches!(o.option, ColumnOption::Null)));
                        assert_eq!(&None, location);
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_alter_add_column_with_first() {
        let sql = "ALTER TABLE my_metric_1 ADD tagk_i STRING Null FIRST;";
        let mut result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        assert_matches!(statement, Statement::Alter { .. });
        match statement {
            Statement::Alter(alter_table) => {
                assert_eq!("my_metric_1", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_matches!(alter_operation, AlterTableOperation::AddColumn { .. });
                match alter_operation {
                    AlterTableOperation::AddColumn {
                        column_def,
                        location,
                    } => {
                        assert_eq!("tagk_i", column_def.name.value);
                        assert_eq!(DataType::String, column_def.data_type);
                        assert!(column_def
                            .options
                            .iter()
                            .any(|o| matches!(o.option, ColumnOption::Null)));
                        assert_eq!(&Some(AddColumnLocation::First), location);
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_alter_add_column_with_after() {
        let sql = "ALTER TABLE my_metric_1 ADD tagk_i STRING Null AFTER ts;";
        let mut result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        assert_matches!(statement, Statement::Alter { .. });
        match statement {
            Statement::Alter(alter_table) => {
                assert_eq!("my_metric_1", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_matches!(alter_operation, AlterTableOperation::AddColumn { .. });
                match alter_operation {
                    AlterTableOperation::AddColumn {
                        column_def,
                        location,
                    } => {
                        assert_eq!("tagk_i", column_def.name.value);
                        assert_eq!(DataType::String, column_def.data_type);
                        assert!(column_def
                            .options
                            .iter()
                            .any(|o| matches!(o.option, ColumnOption::Null)));
                        assert_eq!(
                            &Some(AddColumnLocation::After {
                                column_name: "ts".to_string()
                            }),
                            location
                        );
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_alter_drop_column() {
        let sql = "ALTER TABLE my_metric_1 DROP a";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap_err();
        let err = result.output_msg();
        assert!(err.contains("expect keyword COLUMN after ALTER TABLE DROP"));

        let sql = "ALTER TABLE my_metric_1 DROP COLUMN a";
        let mut result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        assert_matches!(statement, Statement::Alter { .. });
        match statement {
            Statement::Alter(alter_table) => {
                assert_eq!("my_metric_1", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_matches!(alter_operation, AlterTableOperation::DropColumn { .. });
                match alter_operation {
                    AlterTableOperation::DropColumn { name } => {
                        assert_eq!("a", name.value);
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_alter_rename_table() {
        let sql = "ALTER TABLE test_table table_t";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap_err();
        let err = result.output_msg();
        assert!(err.contains("expect keyword ADD or DROP or RENAME after ALTER TABLE"));

        let sql = "ALTER TABLE test_table RENAME table_t";
        let mut result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        assert_matches!(statement, Statement::Alter { .. });
        match statement {
            Statement::Alter(alter_table) => {
                assert_eq!("test_table", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_matches!(alter_operation, AlterTableOperation::RenameTable { .. });
                match alter_operation {
                    AlterTableOperation::RenameTable { new_table_name } => {
                        assert_eq!("table_t", new_table_name);
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}
