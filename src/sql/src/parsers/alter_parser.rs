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
use sqlparser::ast::Ident;
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, TokenWithLocation};

use crate::error::{self, InvalidColumnOptionSnafu, Result, SetFulltextOptionSnafu};
use crate::parser::ParserContext;
use crate::parsers::create_parser::INVERTED;
use crate::parsers::utils::{
    validate_column_fulltext_create_option, validate_column_skipping_index_create_option,
};
use crate::statements::alter::{
    AddColumn, AlterDatabase, AlterDatabaseOperation, AlterTable, AlterTableOperation,
    KeyValueOption, SetIndexOperation, UnsetIndexOperation,
};
use crate::statements::statement::Statement;
use crate::util::parse_option_string;

impl ParserContext<'_> {
    pub(crate) fn parse_alter(&mut self) -> Result<Statement> {
        let _ = self.parser.expect_keyword(Keyword::ALTER);
        match self.parser.peek_token().token {
            Token::Word(w) => match w.keyword {
                Keyword::DATABASE => self.parse_alter_database().map(Statement::AlterDatabase),
                Keyword::TABLE => self.parse_alter_table().map(Statement::AlterTable),
                _ => self.expected("DATABASE or TABLE after ALTER", self.parser.peek_token()),
            },
            unexpected => self.unsupported(unexpected.to_string()),
        }
    }

    fn parse_alter_database(&mut self) -> Result<AlterDatabase> {
        self.parser
            .expect_keyword(Keyword::DATABASE)
            .context(error::SyntaxSnafu)?;

        let database_name = self
            .parser
            .parse_object_name(false)
            .context(error::SyntaxSnafu)?;
        let database_name = Self::canonicalize_object_name(database_name);

        match self.parser.peek_token().token {
            Token::Word(w) => {
                if w.value.eq_ignore_ascii_case("UNSET") {
                    let _ = self.parser.next_token();
                    let keys = self
                        .parser
                        .parse_comma_separated(parse_string_option_names)
                        .context(error::SyntaxSnafu)?
                        .into_iter()
                        .map(|name| name.to_string())
                        .collect();
                    Ok(AlterDatabase::new(
                        database_name,
                        AlterDatabaseOperation::UnsetDatabaseOption { keys },
                    ))
                } else if w.keyword == Keyword::SET {
                    let _ = self.parser.next_token();
                    let options = self
                        .parser
                        .parse_comma_separated(parse_string_options)
                        .context(error::SyntaxSnafu)?
                        .into_iter()
                        .map(|(key, value)| KeyValueOption { key, value })
                        .collect();
                    Ok(AlterDatabase::new(
                        database_name,
                        AlterDatabaseOperation::SetDatabaseOption { options },
                    ))
                } else {
                    self.expected(
                        "SET or UNSET after ALTER DATABASE",
                        self.parser.peek_token(),
                    )
                }
            }
            unexpected => self.unsupported(unexpected.to_string()),
        }
    }

    fn parse_alter_table(&mut self) -> Result<AlterTable> {
        self.parser
            .expect_keyword(Keyword::TABLE)
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
                } else if w.value.eq_ignore_ascii_case("UNSET") {
                    self.parse_alter_table_unset()?
                } else {
                    match w.keyword {
                        Keyword::ADD => self.parse_alter_table_add()?,
                        Keyword::DROP => {
                            let _ = self.parser.next_token();
                            self.parser
                                .expect_keyword(Keyword::COLUMN)
                                .context(error::SyntaxSnafu)?;
                            let name = Self::canonicalize_identifier(
                                Self::parse_identifier(&mut self.parser)
                                    .context(error::SyntaxSnafu)?,
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
                                .map(|(key, value)| KeyValueOption { key, value })
                                .collect();
                            AlterTableOperation::SetTableOptions { options }
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

    fn parse_alter_table_unset(&mut self) -> Result<AlterTableOperation> {
        let _ = self.parser.next_token();
        let keys = self
            .parser
            .parse_comma_separated(parse_string_option_names)
            .context(error::SyntaxSnafu)?
            .into_iter()
            .collect();

        Ok(AlterTableOperation::UnsetTableOptions { keys })
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
            self.parser.prev_token();
            let add_columns = self
                .parser
                .parse_comma_separated(parse_add_columns)
                .context(error::SyntaxSnafu)?;
            Ok(AlterTableOperation::AddColumns { add_columns })
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

        match self.parser.peek_token().token {
            Token::Word(w) => {
                if w.value.eq_ignore_ascii_case("UNSET") {
                    // consume the current token.
                    self.parser.next_token();
                    self.parse_alter_column_unset_index(column_name)
                } else if w.keyword == Keyword::SET {
                    // consume the current token.
                    self.parser.next_token();
                    self.parse_alter_column_set_index(column_name)
                } else {
                    let data_type = self.parser.parse_data_type().context(error::SyntaxSnafu)?;
                    Ok(AlterTableOperation::ModifyColumnType {
                        column_name,
                        target_type: data_type,
                    })
                }
            }
            _ => self.expected(
                "SET or UNSET or data type after MODIFY COLUMN",
                self.parser.peek_token(),
            )?,
        }
    }

    fn parse_alter_column_unset_index(
        &mut self,
        column_name: Ident,
    ) -> Result<AlterTableOperation> {
        match self.parser.next_token() {
            TokenWithLocation {
                token: Token::Word(w),
                ..
            } if w.keyword == Keyword::FULLTEXT => {
                self.parser
                    .expect_keyword(Keyword::INDEX)
                    .context(error::SyntaxSnafu)?;
                Ok(AlterTableOperation::UnsetIndex {
                    options: UnsetIndexOperation::Fulltext { column_name },
                })
            }

            TokenWithLocation {
                token: Token::Word(w),
                ..
            } if w.value.eq_ignore_ascii_case(INVERTED) => {
                self.parser
                    .expect_keyword(Keyword::INDEX)
                    .context(error::SyntaxSnafu)?;
                Ok(AlterTableOperation::UnsetIndex {
                    options: UnsetIndexOperation::Inverted { column_name },
                })
            }

            TokenWithLocation {
                token: Token::Word(w),
                ..
            } if w.value.eq_ignore_ascii_case("SKIPPING") => {
                self.parser
                    .expect_keyword(Keyword::INDEX)
                    .context(error::SyntaxSnafu)?;
                Ok(AlterTableOperation::UnsetIndex {
                    options: UnsetIndexOperation::Skipping { column_name },
                })
            }
            _ => self.expected(
                format!(
                    "{:?} OR INVERTED INDEX OR SKIPPING INDEX",
                    Keyword::FULLTEXT
                )
                .as_str(),
                self.parser.peek_token(),
            ),
        }
    }

    fn parse_alter_column_set_index(&mut self, column_name: Ident) -> Result<AlterTableOperation> {
        match self.parser.next_token() {
            TokenWithLocation {
                token: Token::Word(w),
                ..
            } if w.keyword == Keyword::FULLTEXT => {
                self.parser
                    .expect_keyword(Keyword::INDEX)
                    .context(error::SyntaxSnafu)?;
                self.parse_alter_column_fulltext(column_name)
            }

            TokenWithLocation {
                token: Token::Word(w),
                ..
            } if w.value.eq_ignore_ascii_case(INVERTED) => {
                self.parser
                    .expect_keyword(Keyword::INDEX)
                    .context(error::SyntaxSnafu)?;
                Ok(AlterTableOperation::SetIndex {
                    options: SetIndexOperation::Inverted { column_name },
                })
            }

            TokenWithLocation {
                token: Token::Word(w),
                ..
            } if w.value.eq_ignore_ascii_case("SKIPPING") => {
                self.parser
                    .expect_keyword(Keyword::INDEX)
                    .context(error::SyntaxSnafu)?;
                self.parse_alter_column_skipping(column_name)
            }
            _ => self.expected(
                format!("{:?} OR INVERTED OR SKIPPING INDEX", Keyword::FULLTEXT).as_str(),
                self.parser.peek_token(),
            ),
        }
    }

    fn parse_alter_column_fulltext(&mut self, column_name: Ident) -> Result<AlterTableOperation> {
        let mut options = self
            .parser
            .parse_options(Keyword::WITH)
            .context(error::SyntaxSnafu)?
            .into_iter()
            .map(parse_option_string)
            .collect::<Result<HashMap<String, String>>>()?;

        for key in options.keys() {
            ensure!(
                validate_column_fulltext_create_option(key),
                InvalidColumnOptionSnafu {
                    name: column_name.to_string(),
                    msg: format!("invalid FULLTEXT option: {key}"),
                }
            );
        }

        options.insert(
            COLUMN_FULLTEXT_CHANGE_OPT_KEY_ENABLE.to_string(),
            "true".to_string(),
        );

        Ok(AlterTableOperation::SetIndex {
            options: SetIndexOperation::Fulltext {
                column_name,
                options: options.try_into().context(SetFulltextOptionSnafu)?,
            },
        })
    }

    fn parse_alter_column_skipping(&mut self, column_name: Ident) -> Result<AlterTableOperation> {
        let options = self
            .parser
            .parse_options(Keyword::WITH)
            .context(error::SyntaxSnafu)?
            .into_iter()
            .map(parse_option_string)
            .collect::<Result<HashMap<String, String>>>()?;

        for key in options.keys() {
            ensure!(
                validate_column_skipping_index_create_option(key),
                InvalidColumnOptionSnafu {
                    name: column_name.to_string(),
                    msg: format!("invalid SKIPPING INDEX option: {key}"),
                }
            );
        }

        Ok(AlterTableOperation::SetIndex {
            options: SetIndexOperation::Skipping {
                column_name,
                options: options
                    .try_into()
                    .context(error::SetSkippingIndexOptionSnafu)?,
            },
        })
    }
}

/// Parses a string literal and an optional string literal value.
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

fn parse_add_columns(parser: &mut Parser) -> std::result::Result<AddColumn, ParserError> {
    parser.expect_keyword(Keyword::ADD)?;
    let _ = parser.parse_keyword(Keyword::COLUMN);
    let add_if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let mut column_def = parser.parse_column_def()?;
    column_def.name = ParserContext::canonicalize_identifier(column_def.name);
    let location = if parser.parse_keyword(Keyword::FIRST) {
        Some(AddColumnLocation::First)
    } else if let Token::Word(word) = parser.peek_token().token {
        if word.value.eq_ignore_ascii_case("AFTER") {
            let _ = parser.next_token();
            let name =
                ParserContext::canonicalize_identifier(ParserContext::parse_identifier(parser)?);
            Some(AddColumnLocation::After {
                column_name: name.value,
            })
        } else {
            None
        }
    } else {
        None
    };
    Ok(AddColumn {
        column_def,
        location,
        add_if_not_exists,
    })
}

/// Parses a comma separated list of string literals.
fn parse_string_option_names(parser: &mut Parser) -> std::result::Result<String, ParserError> {
    parser.parse_literal_string()
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_error::ext::ErrorExt;
    use datatypes::schema::{FulltextAnalyzer, FulltextBackend, FulltextOptions};
    use sqlparser::ast::{ColumnDef, ColumnOption, ColumnOptionDef, DataType};

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParseOptions;
    use crate::statements::alter::AlterDatabaseOperation;

    #[test]
    fn test_parse_alter_database() {
        let sql = "ALTER DATABASE test_db SET 'a'='A', 'b' = 'B'";
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        assert_matches!(statement, Statement::AlterDatabase { .. });
        match statement {
            Statement::AlterDatabase(alter_database) => {
                assert_eq!("test_db", alter_database.database_name().0[0].value);

                let alter_operation = alter_database.alter_operation();
                assert_matches!(
                    alter_operation,
                    AlterDatabaseOperation::SetDatabaseOption { .. }
                );
                match alter_operation {
                    AlterDatabaseOperation::SetDatabaseOption { options } => {
                        assert_eq!(2, options.len());
                        assert_eq!("a", options[0].key);
                        assert_eq!("A", options[0].value);
                        assert_eq!("b", options[1].key);
                        assert_eq!("B", options[1].value);
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
        let sql = "ALTER DATABASE test_db UNSET 'a', 'b'";
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());
        let statement = result.remove(0);
        assert_matches!(statement, Statement::AlterDatabase { .. });
        match statement {
            Statement::AlterDatabase(alter_database) => {
                assert_eq!("test_db", alter_database.database_name().0[0].value);
                let alter_operation = alter_database.alter_operation();
                assert_matches!(
                    alter_operation,
                    AlterDatabaseOperation::UnsetDatabaseOption { .. }
                );
                match alter_operation {
                    AlterDatabaseOperation::UnsetDatabaseOption { keys } => {
                        assert_eq!(2, keys.len());
                        assert_eq!("a", keys[0]);
                        assert_eq!("b", keys[1]);
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_alter_add_column() {
        let sql = "ALTER TABLE my_metric_1 ADD tagk_i STRING Null;";
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        assert_matches!(statement, Statement::AlterTable { .. });
        match statement {
            Statement::AlterTable(alter_table) => {
                assert_eq!("my_metric_1", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_matches!(alter_operation, AlterTableOperation::AddColumns { .. });
                match alter_operation {
                    AlterTableOperation::AddColumns { add_columns } => {
                        assert_eq!(add_columns.len(), 1);
                        assert_eq!("tagk_i", add_columns[0].column_def.name.value);
                        assert_eq!(DataType::String(None), add_columns[0].column_def.data_type);
                        assert!(add_columns[0]
                            .column_def
                            .options
                            .iter()
                            .any(|o| matches!(o.option, ColumnOption::Null)));
                        assert_eq!(&None, &add_columns[0].location);
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
        assert_matches!(statement, Statement::AlterTable { .. });
        match statement {
            Statement::AlterTable(alter_table) => {
                assert_eq!("my_metric_1", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_matches!(alter_operation, AlterTableOperation::AddColumns { .. });
                match alter_operation {
                    AlterTableOperation::AddColumns { add_columns } => {
                        assert_eq!("tagk_i", add_columns[0].column_def.name.value);
                        assert_eq!(DataType::String(None), add_columns[0].column_def.data_type);
                        assert!(add_columns[0]
                            .column_def
                            .options
                            .iter()
                            .any(|o| matches!(o.option, ColumnOption::Null)));
                        assert_eq!(&Some(AddColumnLocation::First), &add_columns[0].location);
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_alter_add_column_with_after() {
        let sql =
            "ALTER TABLE my_metric_1 ADD tagk_i STRING Null AFTER ts, add column tagl_i String;";
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        assert_matches!(statement, Statement::AlterTable { .. });
        match statement {
            Statement::AlterTable(alter_table) => {
                assert_eq!("my_metric_1", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_matches!(alter_operation, AlterTableOperation::AddColumns { .. });
                match alter_operation {
                    AlterTableOperation::AddColumns { add_columns } => {
                        let expecteds: Vec<(Option<AddColumnLocation>, ColumnDef)> = vec![
                            (
                                Some(AddColumnLocation::After {
                                    column_name: "ts".to_string(),
                                }),
                                ColumnDef {
                                    name: Ident::new("tagk_i"),
                                    data_type: DataType::String(None),
                                    options: vec![ColumnOptionDef {
                                        name: None,
                                        option: ColumnOption::Null,
                                    }],
                                    collation: None,
                                },
                            ),
                            (
                                None,
                                ColumnDef {
                                    name: Ident::new("tagl_i"),
                                    data_type: DataType::String(None),
                                    options: vec![],
                                    collation: None,
                                },
                            ),
                        ];
                        for (add_column, expected) in add_columns
                            .iter()
                            .zip(expecteds)
                            .collect::<Vec<(&AddColumn, (Option<AddColumnLocation>, ColumnDef))>>()
                        {
                            assert_eq!(add_column.column_def, expected.1);
                            assert_eq!(&expected.0, &add_column.location);
                        }
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_add_column_if_not_exists() {
        let sql = "ALTER TABLE test ADD COLUMN IF NOT EXISTS a INTEGER, ADD COLUMN b STRING, ADD COLUMN IF NOT EXISTS c INT;";
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(result.len(), 1);
        let statement = result.remove(0);
        assert_matches!(statement, Statement::AlterTable { .. });
        match statement {
            Statement::AlterTable(alter) => {
                assert_eq!(alter.table_name.0[0].value, "test");
                assert_matches!(
                    alter.alter_operation,
                    AlterTableOperation::AddColumns { .. }
                );
                match alter.alter_operation {
                    AlterTableOperation::AddColumns { add_columns } => {
                        let expected = vec![
                            AddColumn {
                                column_def: ColumnDef {
                                    name: Ident::new("a"),
                                    data_type: DataType::Integer(None),
                                    collation: None,
                                    options: vec![],
                                },
                                location: None,
                                add_if_not_exists: true,
                            },
                            AddColumn {
                                column_def: ColumnDef {
                                    name: Ident::new("b"),
                                    data_type: DataType::String(None),
                                    collation: None,
                                    options: vec![],
                                },
                                location: None,
                                add_if_not_exists: false,
                            },
                            AddColumn {
                                column_def: ColumnDef {
                                    name: Ident::new("c"),
                                    data_type: DataType::Int(None),
                                    collation: None,
                                    options: vec![],
                                },
                                location: None,
                                add_if_not_exists: true,
                            },
                        ];
                        for (idx, add_column) in add_columns.into_iter().enumerate() {
                            assert_eq!(add_column, expected[idx]);
                        }
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
            "Invalid SQL syntax: sql parser error: Expected: COLUMN, found: a at Line: 1, Column: 30"
        );

        let sql = "ALTER TABLE my_metric_1 DROP COLUMN a";
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        assert_matches!(statement, Statement::AlterTable { .. });
        match statement {
            Statement::AlterTable(alter_table) => {
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
    fn test_parse_alter_modify_column_type() {
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
        assert_matches!(statement, Statement::AlterTable { .. });
        match statement {
            Statement::AlterTable(alter_table) => {
                assert_eq!("my_metric_1", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_matches!(
                    alter_operation,
                    AlterTableOperation::ModifyColumnType { .. }
                );
                match alter_operation {
                    AlterTableOperation::ModifyColumnType {
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
            Statement::AlterTable(alter_table) => {
                assert_eq!("my_metric_1", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_matches!(
                    alter_operation,
                    AlterTableOperation::ModifyColumnType { .. }
                );
                match alter_operation {
                    AlterTableOperation::ModifyColumnType {
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
            Statement::AlterTable(alter_table) => {
                assert_eq!("my_metric_1", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_matches!(
                    alter_operation,
                    AlterTableOperation::ModifyColumnType { .. }
                );
                match alter_operation {
                    AlterTableOperation::ModifyColumnType {
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
        assert_eq!(err, "Invalid SQL syntax: sql parser error: Expected ADD or DROP or MODIFY or RENAME or SET after ALTER TABLE, found: table_t");

        let sql = "ALTER TABLE test_table RENAME table_t";
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        assert_matches!(statement, Statement::AlterTable { .. });
        match statement {
            Statement::AlterTable(alter_table) => {
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

    fn check_parse_alter_table_set_options(sql: &str, expected: &[(&str, &str)]) {
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());
        let Statement::AlterTable(alter) = &result[0] else {
            unreachable!()
        };
        assert_eq!("test_table", alter.table_name.0[0].value);
        let AlterTableOperation::SetTableOptions { options } = &alter.alter_operation else {
            unreachable!()
        };

        assert_eq!(sql, alter.to_string());
        let res = options
            .iter()
            .map(|o| (o.key.as_str(), o.value.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(expected, &res);
    }

    fn check_parse_alter_table_unset_options(sql: &str, expected: &[&str]) {
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());
        let Statement::AlterTable(alter) = &result[0] else {
            unreachable!()
        };
        assert_eq!("test_table", alter.table_name.0[0].value);
        let AlterTableOperation::UnsetTableOptions { keys } = &alter.alter_operation else {
            unreachable!()
        };

        assert_eq!(sql, alter.to_string());
        let res = keys.iter().map(|o| o.to_string()).collect::<Vec<_>>();
        assert_eq!(expected, &res);
    }

    #[test]
    fn test_parse_alter_table_set_options() {
        check_parse_alter_table_set_options("ALTER TABLE test_table SET 'a'='A'", &[("a", "A")]);
        check_parse_alter_table_set_options(
            "ALTER TABLE test_table SET 'a'='A','b'='B'",
            &[("a", "A"), ("b", "B")],
        );
        check_parse_alter_table_set_options(
            "ALTER TABLE test_table SET 'a'='A','b'='B','c'='C'",
            &[("a", "A"), ("b", "B"), ("c", "C")],
        );
        check_parse_alter_table_set_options("ALTER TABLE test_table SET 'a'=NULL", &[("a", "")]);

        ParserContext::create_with_dialect(
            "ALTER TABLE test_table SET a INTEGER",
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap_err();
    }

    #[test]
    fn test_parse_alter_table_unset_options() {
        check_parse_alter_table_unset_options("ALTER TABLE test_table UNSET 'a'", &["a"]);
        check_parse_alter_table_unset_options("ALTER TABLE test_table UNSET 'a','b'", &["a", "b"]);
        ParserContext::create_with_dialect(
            "ALTER TABLE test_table UNSET a INTEGER",
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap_err();
    }

    #[test]
    fn test_parse_alter_column_fulltext() {
        let sql = "ALTER TABLE test_table MODIFY COLUMN a SET FULLTEXT INDEX WITH(analyzer='English',case_sensitive='false',backend='bloom')";
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();

        assert_eq!(1, result.len());
        let statement = result.remove(0);
        assert_matches!(statement, Statement::AlterTable { .. });
        match statement {
            Statement::AlterTable(alter_table) => {
                assert_eq!("test_table", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                match alter_operation {
                    AlterTableOperation::SetIndex {
                        options:
                            SetIndexOperation::Fulltext {
                                column_name,
                                options,
                            },
                    } => {
                        assert_eq!("a", column_name.value);
                        assert_eq!(
                            FulltextOptions {
                                enable: true,
                                analyzer: FulltextAnalyzer::English,
                                case_sensitive: false,
                                backend: FulltextBackend::Bloom,
                            },
                            *options
                        );
                    }
                    _ => unreachable!(),
                };
            }
            _ => unreachable!(),
        }

        let sql = "ALTER TABLE test_table MODIFY COLUMN a UNSET FULLTEXT INDEX";
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());
        let statement = result.remove(0);
        assert_matches!(statement, Statement::AlterTable { .. });
        match statement {
            Statement::AlterTable(alter_table) => {
                assert_eq!("test_table", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_eq!(
                    alter_operation,
                    &AlterTableOperation::UnsetIndex {
                        options: UnsetIndexOperation::Fulltext {
                            column_name: Ident {
                                value: "a".to_string(),
                                quote_style: None
                            }
                        }
                    }
                );
            }
            _ => unreachable!(),
        }

        let invalid_sql =
            "ALTER TABLE test_table MODIFY COLUMN a SET FULLTEXT INDEX WITH('abcd'='true')";
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

    #[test]
    fn test_parse_alter_column_inverted() {
        let sql = "ALTER TABLE test_table MODIFY COLUMN a SET INVERTED INDEX";
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();

        assert_eq!(1, result.len());
        let statement = result.remove(0);
        assert_matches!(statement, Statement::AlterTable { .. });
        match statement {
            Statement::AlterTable(alter_table) => {
                assert_eq!("test_table", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                match alter_operation {
                    AlterTableOperation::SetIndex {
                        options: SetIndexOperation::Inverted { column_name },
                    } => assert_eq!("a", column_name.value),
                    _ => unreachable!(),
                };
            }
            _ => unreachable!(),
        }

        let sql = "ALTER TABLE test_table MODIFY COLUMN a UNSET INVERTED INDEX";
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());
        let statement = result.remove(0);
        assert_matches!(statement, Statement::AlterTable { .. });
        match statement {
            Statement::AlterTable(alter_table) => {
                assert_eq!("test_table", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_eq!(
                    alter_operation,
                    &AlterTableOperation::UnsetIndex {
                        options: UnsetIndexOperation::Inverted {
                            column_name: Ident {
                                value: "a".to_string(),
                                quote_style: None
                            }
                        }
                    }
                );
            }
            _ => unreachable!(),
        }

        let invalid_sql = "ALTER TABLE test_table MODIFY COLUMN a SET INVERTED";
        ParserContext::create_with_dialect(
            invalid_sql,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap_err();
    }
}
