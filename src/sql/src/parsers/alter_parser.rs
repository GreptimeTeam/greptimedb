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
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::alter::{AlterTable, AlterTableOperation, ChangeTableOption};
use crate::statements::statement::Statement;

impl ParserContext<'_> {
    pub(crate) fn parse_alter(&mut self) -> Result<Statement> {
        let alter_table = self.parse_alter_table().context(error::SyntaxSnafu)?;
        Ok(Statement::Alter(alter_table))
    }

    fn parse_alter_table(&mut self) -> std::result::Result<AlterTable, ParserError> {
        self.parser
            .expect_keywords(&[Keyword::ALTER, Keyword::TABLE])?;

        let raw_table_name = self.parser.parse_object_name(false)?;
        let table_name = Self::canonicalize_object_name(raw_table_name);

        let alter_operation = if self.parser.parse_keyword(Keyword::ADD) {
            if let Some(constraint) = self.parser.parse_optional_table_constraint()? {
                AlterTableOperation::AddConstraint(constraint)
            } else {
                let _ = self.parser.parse_keyword(Keyword::COLUMN);
                let mut column_def = self.parser.parse_column_def()?;
                column_def.name = Self::canonicalize_identifier(column_def.name);
                let location = if self.parser.parse_keyword(Keyword::FIRST) {
                    Some(AddColumnLocation::First)
                } else if let Token::Word(word) = self.parser.peek_token().token {
                    if word.value.eq_ignore_ascii_case("AFTER") {
                        let _ = self.parser.next_token();
                        let name = Self::canonicalize_identifier(self.parse_identifier()?);
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
        } else if self.parser.parse_keyword(Keyword::DROP) {
            if self.parser.parse_keyword(Keyword::COLUMN) {
                let name = Self::canonicalize_identifier(self.parse_identifier()?);
                AlterTableOperation::DropColumn { name }
            } else {
                return Err(ParserError::ParserError(format!(
                    "expect keyword COLUMN after ALTER TABLE DROP, found {}",
                    self.parser.peek_token()
                )));
            }
        } else if self.consume_token("MODIFY") {
            let _ = self.parser.parse_keyword(Keyword::COLUMN);
            let column_name = Self::canonicalize_identifier(self.parser.parse_identifier(false)?);
            let target_type = self.parser.parse_data_type()?;

            AlterTableOperation::ChangeColumnType {
                column_name,
                target_type,
            }
        } else if self.parser.parse_keyword(Keyword::RENAME) {
            let new_table_name_obj_raw = self.parse_object_name()?;
            let new_table_name_obj = Self::canonicalize_object_name(new_table_name_obj_raw);
            let new_table_name = match &new_table_name_obj.0[..] {
                [table] => table.value.clone(),
                _ => {
                    return Err(ParserError::ParserError(format!(
                        "expect table name, actual: {new_table_name_obj}"
                    )))
                }
            };
            AlterTableOperation::RenameTable { new_table_name }
        } else if self.parser.parse_keyword(Keyword::SET) {
            let options = self
                .parser
                .parse_comma_separated(parse_string_options)?
                .into_iter()
                .map(|(key, value)| ChangeTableOption { key, value })
                .collect();
            AlterTableOperation::ChangeTableOptions { options }
        } else {
            return Err(ParserError::ParserError(format!(
                "expect keyword ADD or DROP or MODIFY or RENAME after ALTER TABLE, found {}",
                self.parser.peek_token()
            )));
        };
        Ok(AlterTable::new(table_name, alter_operation))
    }
}

fn parse_string_options(parser: &mut Parser) -> std::result::Result<(String, String), ParserError> {
    let name = parser.parse_literal_string()?;
    parser.expect_token(&Token::Eq)?;
    let value = if parser.parse_keyword(Keyword::NULL) {
        "".to_string()
    } else if let Ok(v) = parser.parse_literal_string() {
        v
    } else {
        return Err(ParserError::ParserError(format!(
            "Unexpected option value for alter table statements, expect string literal or NULL, got: `{}`",
            parser.next_token()
        )));
    };
    Ok((name, value))
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_error::ext::ErrorExt;
    use sqlparser::ast::{ColumnOption, DataType};

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParseOptions;

    #[test]
    fn test_parse_alter_add_column() {
        let sql = "ALTER TABLE my_metric_1 ADD tagk_i STRING Null;";
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
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
                        assert_eq!(DataType::String(None), column_def.data_type);
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
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
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
                        assert_eq!(DataType::String(None), column_def.data_type);
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
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
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
                        assert_eq!(DataType::String(None), column_def.data_type);
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
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap_err();
        let err = result.output_msg();
        assert!(err.contains("expect keyword COLUMN after ALTER TABLE DROP"));

        let sql = "ALTER TABLE my_metric_1 DROP COLUMN a";
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
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
    fn test_parse_alter_change_column_type() {
        let sql_1 = "ALTER TABLE my_metric_1 MODIFY COLUMN a STRING";
        let result_1 = ParserContext::create_with_dialect(
            sql_1,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap();

        let sql_2 = "ALTER TABLE my_metric_1 MODIFY COLUMN a STRING";
        let mut result_2 = ParserContext::create_with_dialect(
            sql_2,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap();
        assert_eq!(result_1, result_2);
        assert_eq!(1, result_2.len());

        let statement = result_2.remove(0);
        assert_matches!(statement, Statement::Alter { .. });
        match statement {
            Statement::Alter(alter_table) => {
                assert_eq!("my_metric_1", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_matches!(
                    alter_operation,
                    AlterTableOperation::ChangeColumnType { .. }
                );
                match alter_operation {
                    AlterTableOperation::ChangeColumnType {
                        column_name,
                        target_type,
                    } => {
                        assert_eq!("a", column_name.value);
                        assert_eq!(DataType::String(None), *target_type);
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_alter_change_column_alias_type() {
        let sql_1 = "ALTER TABLE my_metric_1 MODIFY COLUMN a MediumText";
        let mut result_1 = ParserContext::create_with_dialect(
            sql_1,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap();

        match result_1.remove(0) {
            Statement::Alter(alter_table) => {
                assert_eq!("my_metric_1", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_matches!(
                    alter_operation,
                    AlterTableOperation::ChangeColumnType { .. }
                );
                match alter_operation {
                    AlterTableOperation::ChangeColumnType {
                        column_name,
                        target_type,
                    } => {
                        assert_eq!("a", column_name.value);
                        assert_eq!(DataType::Text, *target_type);
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }

        let sql_2 = "ALTER TABLE my_metric_1 MODIFY COLUMN a TIMESTAMP_US";
        let mut result_2 = ParserContext::create_with_dialect(
            sql_2,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap();

        match result_2.remove(0) {
            Statement::Alter(alter_table) => {
                assert_eq!("my_metric_1", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_matches!(
                    alter_operation,
                    AlterTableOperation::ChangeColumnType { .. }
                );
                match alter_operation {
                    AlterTableOperation::ChangeColumnType {
                        column_name,
                        target_type,
                    } => {
                        assert_eq!("a", column_name.value);
                        assert!(matches!(target_type, DataType::Timestamp(Some(6), _)));
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
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap_err();
        let err = result.output_msg();
        assert!(err.contains("expect keyword ADD or DROP or MODIFY or RENAME after ALTER TABLE"));

        let sql = "ALTER TABLE test_table RENAME table_t";
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
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

    fn check_parse_alter_table(sql: &str, expected: &[(&str, &str)]) {
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());
        let Statement::Alter(alter) = &result[0] else {
            unreachable!()
        };
        assert_eq!("test_table", alter.table_name.0[0].value);
        let AlterTableOperation::ChangeTableOptions { options } = &alter.alter_operation else {
            unreachable!()
        };
        let res = options
            .iter()
            .map(|o| (o.key.as_str(), o.value.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(expected, &res);
    }

    #[test]
    fn test_parse_alter_column() {
        check_parse_alter_table("ALTER TABLE test_table SET 'a'='A';", &[("a", "A")]);
        check_parse_alter_table(
            "ALTER TABLE test_table SET 'a'='A','b'='B'",
            &[("a", "A"), ("b", "B")],
        );
        check_parse_alter_table(
            "ALTER TABLE test_table SET 'a'='A','b'='B','c'='C';",
            &[("a", "A"), ("b", "B"), ("c", "C")],
        );
        check_parse_alter_table("ALTER TABLE test_table SET 'a'=NULL;", &[("a", "")]);

        ParserContext::create_with_dialect(
            "ALTER TABLE test_table SET a INTEGER",
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap_err();
    }
}
