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

use std::collections::HashMap;

use common_query::AddColumnLocation;
use datatypes::schema::COLUMN_FULLTEXT_CHANGE_OPT_KEY_ENABLE;
use snafu::{ensure, ResultExt};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

use crate::error::{self, InvalidColumnOptionSnafu, Result, SetFulltextOptionSnafu};
use crate::parser::ParserContext;
use crate::parsers::utils::validate_column_fulltext_option;
use crate::statements::alter::{AlterTable, AlterTableOperation, ChangeTableOption};
use crate::statements::statement::Statement;
use crate::util::parse_option_string;

impl ParserContext<'_> {
    pub(crate) fn parse_alter(&mut self) -> Result<Statement> {
        let alter_table = self.parse_alter_table()?;
        Ok(Statement::Alter(alter_table))
    }

    fn parse_alter_table(&mut self) -> Result<AlterTable> {
        self.parser
            .expect_keywords(&[Keyword::ALTER, Keyword::TABLE])
            .context(error::SyntaxSnafu)?;

        let raw_table_name = self
            .parser
            .parse_object_name(false)
            .context(error::SyntaxSnafu)?;
        let table_name = Self::canonicalize_object_name(raw_table_name);

        let alter_operation = match self.parser.peek_token().token {
            Token::Word(w) => {
                if w.value.eq_ignore_ascii_case("MODIFY") {
                    self.parse_alter_table_modify()?
                } else {
                    match w.keyword {
                        Keyword::ADD => self.parse_alter_table_add()?,
                        Keyword::DROP => {
                            let _ = self.parser.next_token();
                            self.parser
                                .expect_keyword(Keyword::COLUMN)
                                .context(error::SyntaxSnafu)?;
                            let name = Self::canonicalize_identifier(
                                self.parse_identifier().context(error::SyntaxSnafu)?,
                            );
                            AlterTableOperation::DropColumn { name }
                        }
                        Keyword::RENAME => {
                            let _ = self.parser.next_token();
                            let new_table_name_obj_raw =
                                self.parse_object_name().context(error::SyntaxSnafu)?;
                            let new_table_name_obj =
                                Self::canonicalize_object_name(new_table_name_obj_raw);
                            let new_table_name = match &new_table_name_obj.0[..] {
                                [table] => table.value.clone(),
                                _ => {
                                    return Err(ParserError::ParserError(format!(
                                        "expect table name, actual: {new_table_name_obj}"
                                    )))
                                    .context(error::SyntaxSnafu)
                                }
                            };
                            AlterTableOperation::RenameTable { new_table_name }
                        }
                        Keyword::SET => {
                            let _ = self.parser.next_token();
                            let options = self
                                .parser
                                .parse_comma_separated(parse_string_options)
                                .context(error::SyntaxSnafu)?
                                .into_iter()
                                .map(|(key, value)| ChangeTableOption { key, value })
                                .collect();
                            AlterTableOperation::ChangeTableOptions { options }
                        }
                        _ => self.expected(
                            "ADD or DROP or MODIFY or RENAME or SET after ALTER TABLE",
                            self.parser.peek_token(),
                        )?,
                    }
                }
            }
            unexpected => self.unsupported(unexpected.to_string())?,
        };
        Ok(AlterTable::new(table_name, alter_operation))
    }

    fn parse_alter_table_add(&mut self) -> Result<AlterTableOperation> {
        let _ = self.parser.next_token();
        if let Some(constraint) = self
            .parser
            .parse_optional_table_constraint()
            .context(error::SyntaxSnafu)?
        {
            Ok(AlterTableOperation::AddConstraint(constraint))
        } else {
            let _ = self.parser.parse_keyword(Keyword::COLUMN);
            let mut column_def = self.parser.parse_column_def().context(error::SyntaxSnafu)?;
            column_def.name = Self::canonicalize_identifier(column_def.name);
            let location = if self.parser.parse_keyword(Keyword::FIRST) {
                Some(AddColumnLocation::First)
            } else if let Token::Word(word) = self.parser.peek_token().token {
                if word.value.eq_ignore_ascii_case("AFTER") {
                    let _ = self.parser.next_token();
                    let name = Self::canonicalize_identifier(
                        self.parse_identifier().context(error::SyntaxSnafu)?,
                    );
                    Some(AddColumnLocation::After {
                        column_name: name.value,
                    })
                } else {
                    None
                }
            } else {
                None
            };
            Ok(AlterTableOperation::AddColumn {
                column_def,
                location,
            })
        }
    }

    fn parse_alter_table_modify(&mut self) -> Result<AlterTableOperation> {
        let _ = self.parser.next_token();
        self.parser
            .expect_keyword(Keyword::COLUMN)
            .context(error::SyntaxSnafu)?;
        let column_name = Self::canonicalize_identifier(
            self.parser
                .parse_identifier(false)
                .context(error::SyntaxSnafu)?,
        );

        if self.parser.parse_keyword(Keyword::SET) {
            self.parser
                .expect_keyword(Keyword::FULLTEXT)
                .context(error::SyntaxSnafu)?;

            let options = self
                .parser
                .parse_options(Keyword::WITH)
                .context(error::SyntaxSnafu)?
                .into_iter()
                .map(parse_option_string)
                .collect::<Result<HashMap<String, String>>>()?;

            for key in options.keys() {
                ensure!(
                    key.to_ascii_lowercase().as_str() == COLUMN_FULLTEXT_CHANGE_OPT_KEY_ENABLE
                        || validate_column_fulltext_option(key.to_ascii_lowercase().as_str()),
                    InvalidColumnOptionSnafu {
                        name: column_name.to_string(),
                        msg: format!("invalid FULLTEXT option: {key}"),
                    }
                );
            }

            Ok(AlterTableOperation::ChangeColumnFulltext {
                column_name,
                options: options.try_into().context(SetFulltextOptionSnafu)?,
            })
        } else {
            let target_type = self.parser.parse_data_type().context(error::SyntaxSnafu)?;
            Ok(AlterTableOperation::ChangeColumnType {
                column_name,
                target_type,
            })
        }
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
    use datatypes::schema::{FulltextAnalyzer, FulltextOptions};
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
        assert_eq!(
            err,
            "sql parser error: Expected COLUMN, found: a at Line: 1, Column 30"
        );

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
        assert_eq!(err, "sql parser error: Expected ADD or DROP or MODIFY or RENAME or SET after ALTER TABLE, found: table_t");

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

        assert_eq!(sql, alter.to_string());
        let res = options
            .iter()
            .map(|o| (o.key.as_str(), o.value.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(expected, &res);
    }

    #[test]
    fn test_parse_alter_column() {
        check_parse_alter_table("ALTER TABLE test_table SET 'a'='A'", &[("a", "A")]);
        check_parse_alter_table(
            "ALTER TABLE test_table SET 'a'='A','b'='B'",
            &[("a", "A"), ("b", "B")],
        );
        check_parse_alter_table(
            "ALTER TABLE test_table SET 'a'='A','b'='B','c'='C'",
            &[("a", "A"), ("b", "B"), ("c", "C")],
        );
        check_parse_alter_table("ALTER TABLE test_table SET 'a'=NULL", &[("a", "")]);

        ParserContext::create_with_dialect(
            "ALTER TABLE test_table SET a INTEGER",
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap_err();
    }

    #[test]
    fn test_parse_alter_column_fulltext() {
        let sql = "ALTER TABLE test_table MODIFY COLUMN a SET FULLTEXT WITH(enable='true',analyzer='English',case_sensitive='false')";
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
                assert_matches!(
                    alter_operation,
                    AlterTableOperation::ChangeColumnFulltext { .. }
                );
                match alter_operation {
                    AlterTableOperation::ChangeColumnFulltext {
                        column_name,
                        options,
                    } => {
                        assert_eq!("a", column_name.value);
                        assert_eq!(
                            FulltextOptions {
                                enable: true,
                                analyzer: FulltextAnalyzer::English,
                                case_sensitive: false
                            },
                            *options
                        );
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }

        let invalid_sql = "ALTER TABLE test_table MODIFY COLUMN a SET FULLTEXT WITH('abcd'='true')";
        let result = ParserContext::create_with_dialect(
            invalid_sql,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap_err();
        let err = result.to_string();
        assert_eq!(
            err,
            "Invalid column option, column name: a, error: invalid FULLTEXT option: abcd"
        );
    }
}
