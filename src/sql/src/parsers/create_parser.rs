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

use std::cmp::Ordering;
use std::collections::HashMap;

use common_catalog::consts::default_engine;
use itertools::Itertools;
use once_cell::sync::Lazy;
use snafu::{ensure, OptionExt, ResultExt};
use sqlparser::ast::{ColumnOption, ColumnOptionDef, DataType, Value};
use sqlparser::dialect::keywords::Keyword;
use sqlparser::keywords::ALL_KEYWORDS;
use sqlparser::parser::IsOptional::Mandatory;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, TokenWithLocation, Word};
use table::requests::valid_table_option;

use crate::ast::{ColumnDef, Ident, TableConstraint, Value as SqlValue};
use crate::error::{
    self, InvalidColumnOptionSnafu, InvalidTableOptionSnafu, InvalidTimeIndexSnafu,
    MissingTimeIndexSnafu, Result, SyntaxSnafu,
};
use crate::parser::ParserContext;
use crate::statements::create::{
    CreateDatabase, CreateExternalTable, CreateTable, PartitionEntry, Partitions, TIME_INDEX,
};
use crate::statements::statement::Statement;
use crate::statements::{
    get_data_type_by_alias_name, sql_data_type_to_concrete_data_type, sql_value_to_value,
};
use crate::util::parse_option_string;

pub const ENGINE: &str = "ENGINE";
pub const MAXVALUE: &str = "MAXVALUE";

static LESS: Lazy<Token> = Lazy::new(|| Token::make_keyword("LESS"));
static THAN: Lazy<Token> = Lazy::new(|| Token::make_keyword("THAN"));

/// Parses create [table] statement
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_create(&mut self) -> Result<Statement> {
        match self.parser.peek_token().token {
            Token::Word(w) => match w.keyword {
                Keyword::TABLE => self.parse_create_table(),

                Keyword::SCHEMA | Keyword::DATABASE => self.parse_create_database(),

                Keyword::EXTERNAL => self.parse_create_external_table(),

                _ => self.unsupported(w.to_string()),
            },
            unexpected => self.unsupported(unexpected.to_string()),
        }
    }

    fn parse_create_external_table(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();
        self.parser
            .expect_keyword(Keyword::TABLE)
            .context(error::SyntaxSnafu { sql: self.sql })?;
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self
            .parser
            .parse_object_name()
            .context(error::UnexpectedSnafu {
                sql: self.sql,
                expected: "a table name",
                actual: self.peek_token_as_string(),
            })?;
        let (columns, constraints) = self.parse_columns()?;
        let engine = self.parse_table_engine(common_catalog::consts::FILE_ENGINE)?;
        let options = self
            .parser
            .parse_options(Keyword::WITH)
            .context(error::SyntaxSnafu { sql: self.sql })?
            .into_iter()
            .filter_map(|option| {
                if let Some(v) = parse_option_string(option.value) {
                    Some((option.name.value.to_lowercase(), v))
                } else {
                    None
                }
            })
            .collect::<HashMap<String, String>>();
        for key in options.keys() {
            ensure!(
                valid_table_option(key),
                InvalidTableOptionSnafu {
                    key: key.to_string()
                }
            );
        }
        Ok(Statement::CreateExternalTable(CreateExternalTable {
            name: table_name,
            columns,
            constraints,
            options: options.into(),
            if_not_exists,
            engine,
        }))
    }

    fn parse_create_database(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();

        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let database_name = self
            .parser
            .parse_object_name()
            .context(error::UnexpectedSnafu {
                sql: self.sql,
                expected: "a database name",
                actual: self.peek_token_as_string(),
            })?;

        Ok(Statement::CreateDatabase(CreateDatabase {
            name: database_name,
            if_not_exists,
        }))
    }

    fn parse_create_table(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let table_name = self
            .parser
            .parse_object_name()
            .context(error::UnexpectedSnafu {
                sql: self.sql,
                expected: "a table name",
                actual: self.peek_token_as_string(),
            })?;

        let (columns, constraints) = self.parse_columns()?;

        let partitions = self.parse_partitions()?;

        let engine = self.parse_table_engine(default_engine())?;
        let options = self
            .parser
            .parse_options(Keyword::WITH)
            .context(error::SyntaxSnafu { sql: self.sql })?;
        for option in options.iter() {
            ensure!(
                valid_table_option(&option.name.value),
                InvalidTableOptionSnafu {
                    key: option.name.value.to_string()
                }
            );
        }
        let create_table = CreateTable {
            if_not_exists,
            name: table_name,
            columns,
            engine,
            constraints,
            options,
            table_id: 0, // table id is assigned by catalog manager
            partitions,
        };
        validate_create(&create_table)?;

        Ok(Statement::CreateTable(create_table))
    }

    // "PARTITION BY ..." syntax:
    // https://dev.mysql.com/doc/refman/8.0/en/partitioning-columns-range.html
    fn parse_partitions(&mut self) -> Result<Option<Partitions>> {
        if !self.parser.parse_keyword(Keyword::PARTITION) {
            return Ok(None);
        }
        self.parser
            .expect_keywords(&[Keyword::BY, Keyword::RANGE, Keyword::COLUMNS])
            .context(error::UnexpectedSnafu {
                sql: self.sql,
                expected: "BY, RANGE, COLUMNS",
                actual: self.peek_token_as_string(),
            })?;

        let column_list = self
            .parser
            .parse_parenthesized_column_list(Mandatory, false)
            .context(error::SyntaxSnafu { sql: self.sql })?;

        let entries = self.parse_comma_separated(Self::parse_partition_entry)?;

        Ok(Some(Partitions {
            column_list,
            entries,
        }))
    }

    fn parse_partition_entry(&mut self) -> Result<PartitionEntry> {
        self.parser
            .expect_keyword(Keyword::PARTITION)
            .context(error::UnexpectedSnafu {
                sql: self.sql,
                expected: "PARTITION",
                actual: self.peek_token_as_string(),
            })?;

        let name = self
            .parser
            .parse_identifier()
            .context(error::SyntaxSnafu { sql: self.sql })?;

        self.parser
            .expect_keyword(Keyword::VALUES)
            .and_then(|_| self.parser.expect_token(&LESS))
            .and_then(|_| self.parser.expect_token(&THAN))
            .context(error::SyntaxSnafu { sql: self.sql })?;

        let value_list = self.parse_comma_separated(Self::parse_value_list)?;

        Ok(PartitionEntry { name, value_list })
    }

    fn parse_value_list(&mut self) -> Result<SqlValue> {
        let token = self.parser.peek_token().token;
        let value = match token {
            Token::Word(Word { value, .. }) if value == MAXVALUE => {
                let _ = self.parser.next_token();
                SqlValue::Number(MAXVALUE.to_string(), false)
            }
            _ => self
                .parser
                .parse_value()
                .context(error::SyntaxSnafu { sql: self.sql })?,
        };
        Ok(value)
    }

    /// Parse a comma-separated list wrapped by "()", and of which all items accepted by `F`
    fn parse_comma_separated<T, F>(&mut self, mut f: F) -> Result<Vec<T>>
    where
        F: FnMut(&mut ParserContext<'a>) -> Result<T>,
    {
        self.parser
            .expect_token(&Token::LParen)
            .context(error::UnexpectedSnafu {
                sql: self.sql,
                expected: "(",
                actual: self.peek_token_as_string(),
            })?;

        let mut values = vec![];
        while self.parser.peek_token() != Token::RParen {
            values.push(f(self)?);
            if !self.parser.consume_token(&Token::Comma) {
                break;
            }
        }

        self.parser
            .expect_token(&Token::RParen)
            .context(error::UnexpectedSnafu {
                sql: self.sql,
                expected: ")",
                actual: self.peek_token_as_string(),
            })?;

        Ok(values)
    }

    fn parse_columns(&mut self) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>)> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.parser.consume_token(&Token::LParen) || self.parser.consume_token(&Token::RParen) {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Token::Word(_) = self.parser.peek_token().token {
                self.parse_column(&mut columns, &mut constraints)?;
            } else {
                return self.expected(
                    "column name or constraint definition",
                    self.parser.peek_token(),
                );
            }
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after column definition",
                    self.parser.peek_token(),
                );
            }
        }

        Ok((columns, constraints))
    }

    fn parse_column(
        &mut self,
        columns: &mut Vec<ColumnDef>,
        constraints: &mut Vec<TableConstraint>,
    ) -> Result<()> {
        let mut column = self
            .parse_column_def()
            .context(SyntaxSnafu { sql: self.sql })?;

        let mut time_index_opt_idx = None;
        for (index, opt) in column.options.iter().enumerate() {
            if let ColumnOption::DialectSpecific(tokens) = &opt.option {
                if matches!(
                    &tokens[..],
                    [
                        Token::Word(Word {
                            keyword: Keyword::TIME,
                            ..
                        }),
                        Token::Word(Word {
                            keyword: Keyword::INDEX,
                            ..
                        })
                    ]
                ) {
                    ensure!(
                        time_index_opt_idx.is_none(),
                        InvalidColumnOptionSnafu {
                            name: column.name.to_string(),
                            msg: "duplicated time index",
                        }
                    );
                    time_index_opt_idx = Some(index);

                    let constraint = TableConstraint::Unique {
                        name: Some(Ident {
                            value: TIME_INDEX.to_owned(),
                            quote_style: None,
                        }),
                        columns: vec![Ident {
                            value: column.name.value.clone(),
                            quote_style: None,
                        }],
                        is_primary: false,
                    };
                    constraints.push(constraint);
                }
            }
        }

        if let Some(index) = time_index_opt_idx {
            ensure!(
                !column.options.contains(&ColumnOptionDef {
                    option: ColumnOption::Null,
                    name: None,
                }),
                InvalidColumnOptionSnafu {
                    name: column.name.to_string(),
                    msg: "time index column can't be null",
                }
            );

            // The timestamp type may be an alias type, we have to retrieve the actual type.
            let data_type = get_real_timestamp_type(&column.data_type);
            ensure!(
                matches!(data_type, DataType::Timestamp(_, _)),
                InvalidColumnOptionSnafu {
                    name: column.name.to_string(),
                    msg: "time index column data type should be timestamp",
                }
            );

            let not_null_opt = ColumnOptionDef {
                option: ColumnOption::NotNull,
                name: None,
            };

            if !column.options.contains(&not_null_opt) {
                column.options.push(not_null_opt);
            }

            let _ = column.options.remove(index);
        }

        columns.push(column);

        Ok(())
    }

    pub fn parse_column_def(&mut self) -> std::result::Result<ColumnDef, ParserError> {
        let parser = &mut self.parser;

        let name = parser.parse_identifier()?;
        if name.quote_style.is_none() &&
            // "ALL_KEYWORDS" are sorted.
            ALL_KEYWORDS.binary_search(&name.value.to_uppercase().as_str()).is_ok()
        {
            return Err(ParserError::ParserError(format!(
                "Cannot use keyword '{}' as column name. Hint: add quotes to the name.",
                &name.value
            )));
        }

        let data_type = parser.parse_data_type()?;
        let collation = if parser.parse_keyword(Keyword::COLLATE) {
            Some(parser.parse_object_name()?)
        } else {
            None
        };
        let mut options = vec![];
        loop {
            if parser.parse_keyword(Keyword::CONSTRAINT) {
                let name = Some(parser.parse_identifier()?);
                if let Some(option) = Self::parse_optional_column_option(parser)? {
                    options.push(ColumnOptionDef { name, option });
                } else {
                    return parser.expected(
                        "constraint details after CONSTRAINT <name>",
                        parser.peek_token(),
                    );
                }
            } else if let Some(option) = Self::parse_optional_column_option(parser)? {
                options.push(ColumnOptionDef { name: None, option });
            } else {
                break;
            };
        }
        Ok(ColumnDef {
            name,
            data_type,
            collation,
            options,
        })
    }

    fn parse_optional_column_option(
        parser: &mut Parser<'a>,
    ) -> std::result::Result<Option<ColumnOption>, ParserError> {
        if parser.parse_keywords(&[Keyword::CHARACTER, Keyword::SET]) {
            Ok(Some(ColumnOption::CharacterSet(
                parser.parse_object_name()?,
            )))
        } else if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            Ok(Some(ColumnOption::NotNull))
        } else if parser.parse_keywords(&[Keyword::COMMENT]) {
            match parser.next_token() {
                TokenWithLocation {
                    token: Token::SingleQuotedString(value, ..),
                    ..
                } => Ok(Some(ColumnOption::Comment(value))),
                unexpected => parser.expected("string", unexpected),
            }
        } else if parser.parse_keyword(Keyword::NULL) {
            Ok(Some(ColumnOption::Null))
        } else if parser.parse_keyword(Keyword::DEFAULT) {
            Ok(Some(ColumnOption::Default(parser.parse_expr()?)))
        } else if parser.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY]) {
            Ok(Some(ColumnOption::Unique { is_primary: true }))
        } else if parser.parse_keyword(Keyword::UNIQUE) {
            Ok(Some(ColumnOption::Unique { is_primary: false }))
        } else if parser.parse_keywords(&[Keyword::TIME, Keyword::INDEX]) {
            // Use a DialectSpecific option for time index
            Ok(Some(ColumnOption::DialectSpecific(vec![
                Token::Word(Word {
                    value: "TIME".to_string(),
                    quote_style: None,
                    keyword: Keyword::TIME,
                }),
                Token::Word(Word {
                    value: "INDEX".to_string(),
                    quote_style: None,
                    keyword: Keyword::INDEX,
                }),
            ])))
        } else {
            Ok(None)
        }
    }

    fn parse_optional_table_constraint(&mut self) -> Result<Option<TableConstraint>> {
        let name = if self.parser.parse_keyword(Keyword::CONSTRAINT) {
            Some(
                self.parser
                    .parse_identifier()
                    .context(error::SyntaxSnafu { sql: self.sql })?,
            )
        } else {
            None
        };
        match self.parser.next_token() {
            TokenWithLocation {
                token: Token::Word(w),
                ..
            } if w.keyword == Keyword::PRIMARY => {
                self.parser
                    .expect_keyword(Keyword::KEY)
                    .context(error::UnexpectedSnafu {
                        sql: self.sql,
                        expected: "KEY",
                        actual: self.peek_token_as_string(),
                    })?;
                let columns = self
                    .parser
                    .parse_parenthesized_column_list(Mandatory, false)
                    .context(error::SyntaxSnafu { sql: self.sql })?;
                Ok(Some(TableConstraint::Unique {
                    name,
                    columns,
                    is_primary: true,
                }))
            }
            TokenWithLocation {
                token: Token::Word(w),
                ..
            } if w.keyword == Keyword::TIME => {
                self.parser
                    .expect_keyword(Keyword::INDEX)
                    .context(error::UnexpectedSnafu {
                        sql: self.sql,
                        expected: "INDEX",
                        actual: self.peek_token_as_string(),
                    })?;

                let columns = self
                    .parser
                    .parse_parenthesized_column_list(Mandatory, false)
                    .context(error::SyntaxSnafu { sql: self.sql })?;

                ensure!(
                    columns.len() == 1,
                    InvalidTimeIndexSnafu {
                        msg: "it should contain only one column in time index",
                    }
                );

                // TODO(dennis): TableConstraint doesn't support dialect right now,
                // so we use unique constraint with special key to represent TIME INDEX.
                Ok(Some(TableConstraint::Unique {
                    name: Some(Ident {
                        value: TIME_INDEX.to_owned(),
                        quote_style: None,
                    }),
                    columns,
                    is_primary: false,
                }))
            }
            unexpected => {
                if name.is_some() {
                    self.expected("PRIMARY, TIME", unexpected)
                } else {
                    self.parser.prev_token();
                    Ok(None)
                }
            }
        }
    }

    /// Parses the set of valid formats
    fn parse_table_engine(&mut self, default: &str) -> Result<String> {
        if !self.consume_token(ENGINE) {
            return Ok(default.to_string());
        }

        self.parser
            .expect_token(&Token::Eq)
            .context(error::UnexpectedSnafu {
                sql: self.sql,
                expected: "=",
                actual: self.peek_token_as_string(),
            })?;

        let token = self.parser.next_token();
        if let Token::Word(w) = token.token {
            Ok(w.value)
        } else {
            self.expected("'Engine' is missing", token)
        }
    }
}

fn validate_create(create_table: &CreateTable) -> Result<()> {
    if let Some(partitions) = &create_table.partitions {
        validate_partitions(&create_table.columns, partitions)?;
    }
    validate_time_index(create_table)?;

    Ok(())
}

fn validate_time_index(create_table: &CreateTable) -> Result<()> {
    let time_index_constraints: Vec<_> = create_table
        .constraints
        .iter()
        .filter_map(|c| {
            if let TableConstraint::Unique {
                name: Some(ident),
                columns,
                is_primary: false,
            } = c
            {
                if ident.value == TIME_INDEX {
                    Some(columns)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .unique()
        .collect();

    ensure!(!time_index_constraints.is_empty(), MissingTimeIndexSnafu);
    ensure!(
        time_index_constraints.len() == 1,
        InvalidTimeIndexSnafu {
            msg: format!(
                "expected only one time index constraint but actual {}",
                time_index_constraints.len()
            ),
        }
    );
    ensure!(
        time_index_constraints[0].len() == 1,
        InvalidTimeIndexSnafu {
            msg: "it should contain only one column in time index",
        }
    );

    // It's safe to use time_index_constraints[0][0],
    // we already check the bound above.
    let time_index_column_ident = &time_index_constraints[0][0];
    let time_index_column = create_table
        .columns
        .iter()
        .find(|c| c.name.value == *time_index_column_ident.value)
        .with_context(|| InvalidTimeIndexSnafu {
            msg: format!(
                "time index column {} not found in columns",
                time_index_column_ident
            ),
        })?;

    let time_index_data_type = get_real_timestamp_type(&time_index_column.data_type);
    ensure!(
        matches!(time_index_data_type, DataType::Timestamp(_, _)),
        InvalidColumnOptionSnafu {
            name: time_index_column.name.to_string(),
            msg: "time index column data type should be timestamp",
        }
    );

    Ok(())
}

fn get_real_timestamp_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Custom(name, tokens) if name.0.len() == 1 && tokens.is_empty() => {
            if let Some(real_type) = get_data_type_by_alias_name(name.0[0].value.as_str()) {
                real_type
            } else {
                data_type.clone()
            }
        }
        _ => data_type.clone(),
    }
}

fn validate_partitions(columns: &[ColumnDef], partitions: &Partitions) -> Result<()> {
    let partition_columns = ensure_partition_columns_defined(columns, partitions)?;

    ensure_partition_names_no_duplicate(partitions)?;

    ensure_value_list_len_matches_columns(partitions, &partition_columns)?;

    let value_lists = ensure_value_lists_strictly_increased(partitions, partition_columns)?;

    ensure_value_lists_bounded_by_maxvalue(value_lists)?;

    Ok(())
}

/// Ensure that partition ranges fully cover all values.
// Simply check the last partition is bounded by "MAXVALUE"s.
// MySQL does not have this restriction. However, I think we'd better have it because:
//   - It might save user from adding more partitions in the future by hand, which is often
//     a tedious task. Why not provide an extra partition at the beginning and leave all
//     other partition related jobs to us? I think it's a reasonable argument to user.
//   - It might save us from some ugly designs and codings. The "MAXVALUE" bound is natural
//     in dealing with values that are unspecified upfront. Without it, we have to store
//     and use the user defined max bound everywhere, starting from calculating regions by
//     partition rule in Frontend, to automatically split and merge regions in Meta.
fn ensure_value_lists_bounded_by_maxvalue(value_lists: Vec<&Vec<Value>>) -> Result<()> {
    let is_maxvalue_bound = value_lists.last().map(|v| {
        v.iter()
            .all(|x| matches!(x, SqlValue::Number(s, _) if s == MAXVALUE))
    });
    ensure!(
        matches!(is_maxvalue_bound, Some(true)),
        error::InvalidSqlSnafu {
            msg: "Please provide an extra partition that is bounded by 'MAXVALUE'."
        }
    );
    Ok(())
}

/// Ensure that value lists of partitions are strictly increasing.
fn ensure_value_lists_strictly_increased<'a>(
    partitions: &'a Partitions,
    partition_columns: Vec<&'a ColumnDef>,
) -> Result<Vec<&'a Vec<Value>>> {
    let value_lists = partitions
        .entries
        .iter()
        .map(|x| &x.value_list)
        .collect::<Vec<_>>();
    for i in 1..value_lists.len() {
        let mut equal_tuples = 0;
        for (n, (x, y)) in value_lists[i - 1]
            .iter()
            .zip(value_lists[i].iter())
            .enumerate()
        {
            let column = partition_columns[n];
            let is_x_maxvalue = matches!(x, SqlValue::Number(s, _) if s == MAXVALUE);
            let is_y_maxvalue = matches!(y, SqlValue::Number(s, _) if s == MAXVALUE);
            match (is_x_maxvalue, is_y_maxvalue) {
                (true, true) => {
                    equal_tuples += 1;
                }
                (false, false) => {
                    let column_name = &column.name.value;
                    let cdt = sql_data_type_to_concrete_data_type(&column.data_type)?;
                    let x = sql_value_to_value(column_name, &cdt, x)?;
                    let y = sql_value_to_value(column_name, &cdt, y)?;
                    match x.cmp(&y) {
                        Ordering::Less => break,
                        Ordering::Equal => equal_tuples += 1,
                        Ordering::Greater => return error::InvalidSqlSnafu {
                            msg: "VALUES LESS THAN value must be strictly increasing for each partition.",
                        }.fail()
                    }
                }
                (true, false) => return error::InvalidSqlSnafu {
                    msg: "VALUES LESS THAN value must be strictly increasing for each partition.",
                }
                .fail(),
                (false, true) => break,
            }
        }
        ensure!(
            equal_tuples < partition_columns.len(),
            error::InvalidSqlSnafu {
                msg: "VALUES LESS THAN value must be strictly increasing for each partition.",
            }
        );
    }
    Ok(value_lists)
}

/// Ensure that value list's length matches the column list.
fn ensure_value_list_len_matches_columns(
    partitions: &Partitions,
    partition_columns: &Vec<&ColumnDef>,
) -> Result<()> {
    for entry in partitions.entries.iter() {
        ensure!(
            entry.value_list.len() == partition_columns.len(),
            error::InvalidSqlSnafu {
                msg: "Partition value list does not match column list.",
            }
        );
    }
    Ok(())
}

/// Ensure that all columns used in "PARTITION BY RANGE COLUMNS" are defined in create table.
fn ensure_partition_columns_defined<'a>(
    columns: &'a [ColumnDef],
    partitions: &'a Partitions,
) -> Result<Vec<&'a ColumnDef>> {
    partitions
        .column_list
        .iter()
        .map(|x| {
            // Normally the columns in "create table" won't be too many,
            // a linear search to find the target every time is fine.
            columns
                .iter()
                .find(|c| &c.name == x)
                .context(error::InvalidSqlSnafu {
                    msg: format!("Partition column {:?} not defined!", x.value),
                })
        })
        .collect::<Result<Vec<&ColumnDef>>>()
}

/// Ensure that partition names do not duplicate.
fn ensure_partition_names_no_duplicate(partitions: &Partitions) -> Result<()> {
    let partition_names = partitions
        .entries
        .iter()
        .map(|x| &x.name.value)
        .sorted()
        .collect::<Vec<&String>>();
    for w in partition_names.windows(2) {
        ensure!(
            w[0] != w[1],
            error::InvalidSqlSnafu {
                msg: format!("Duplicate partition names: {}", w[0]),
            }
        )
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;

    use common_catalog::consts::FILE_ENGINE;
    use common_error::ext::ErrorExt;
    use sqlparser::ast::ColumnOption::NotNull;

    use super::*;
    use crate::dialect::GreptimeDbDialect;

    #[test]
    fn test_validate_external_table_options() {
        let sql = "CREATE EXTERNAL TABLE city (
            host string,
            ts int64,
            cpu float64 default 0,
            memory float64,
            TIME INDEX (ts),
            PRIMARY KEY(ts, host)
        ) with(location='/var/data/city.csv',format='csv',foo='bar');";

        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        assert!(matches!(
            result,
            Err(error::Error::InvalidTableOption { .. })
        ));
    }

    #[test]
    fn test_parse_create_external_table() {
        struct Test<'a> {
            sql: &'a str,
            expected_table_name: &'a str,
            expected_options: HashMap<String, String>,
            expected_engine: &'a str,
            expected_if_not_exist: bool,
        }

        let tests = [
            Test {
                sql: "CREATE EXTERNAL TABLE city with(location='/var/data/city.csv',format='csv');",
                expected_table_name: "city",
                expected_options: HashMap::from([
                    ("location".to_string(), "/var/data/city.csv".to_string()),
                    ("format".to_string(), "csv".to_string()),
                ]),
                expected_engine: FILE_ENGINE,
                expected_if_not_exist: false,
            },
            Test {
                sql: "CREATE EXTERNAL TABLE IF NOT EXISTS city ENGINE=foo with(location='/var/data/city.csv',format='csv');",
                expected_table_name: "city",
                expected_options: HashMap::from([
                    ("location".to_string(), "/var/data/city.csv".to_string()),
                    ("format".to_string(), "csv".to_string()),
                ]),
                expected_engine: "foo",
                expected_if_not_exist: true,
            },
        ];

        for test in tests {
            let stmts =
                ParserContext::create_with_dialect(test.sql, &GreptimeDbDialect {}).unwrap();
            assert_eq!(1, stmts.len());
            match &stmts[0] {
                Statement::CreateExternalTable(c) => {
                    assert_eq!(c.name.to_string(), test.expected_table_name.to_string());
                    assert_eq!(c.options, test.expected_options.into());
                    assert_eq!(c.if_not_exists, test.expected_if_not_exist);
                    assert_eq!(c.engine, test.expected_engine);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_parse_create_external_table_with_schema() {
        let sql = "CREATE EXTERNAL TABLE city (
            host string,
            ts int64,
            cpu float32 default 0,
            memory float64,
            TIME INDEX (ts),
            PRIMARY KEY(ts, host)
        ) with(location='/var/data/city.csv',format='csv');";

        let options = HashMap::from([
            ("location".to_string(), "/var/data/city.csv".to_string()),
            ("format".to_string(), "csv".to_string()),
        ]);

        let stmts = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(1, stmts.len());
        match &stmts[0] {
            Statement::CreateExternalTable(c) => {
                assert_eq!(c.name.to_string(), "city");
                assert_eq!(c.options, options.into());

                let columns = &c.columns;
                assert_column_def(&columns[0], "host", "STRING");
                assert_column_def(&columns[1], "ts", "BIGINT");
                assert_column_def(&columns[2], "cpu", "FLOAT");
                assert_column_def(&columns[3], "memory", "DOUBLE");

                let constraints = &c.constraints;
                assert_matches!(
                    &constraints[0],
                    TableConstraint::Unique {
                        is_primary: false,
                        ..
                    }
                );
                assert_matches!(
                    &constraints[1],
                    TableConstraint::Unique {
                        is_primary: true,
                        ..
                    }
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_create_database() {
        let sql = "create database";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unexpected token while parsing SQL statement"));

        let sql = "create database prometheus";
        let stmts = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();

        assert_eq!(1, stmts.len());
        match &stmts[0] {
            Statement::CreateDatabase(c) => {
                assert_eq!(c.name.to_string(), "prometheus");
                assert!(!c.if_not_exists);
            }
            _ => unreachable!(),
        }

        let sql = "create database if not exists prometheus";
        let stmts = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();

        assert_eq!(1, stmts.len());
        match &stmts[0] {
            Statement::CreateDatabase(c) => {
                assert_eq!(c.name.to_string(), "prometheus");
                assert!(c.if_not_exists);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_validate_create() {
        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT, ts timestamp TIME INDEX)
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh', 2000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        let _ = result.unwrap();

        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, x) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh', 2000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Partition column \"x\" not defined!"));

        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh', 2000),
  PARTITION r2 VALUES LESS THAN ('sz', 3000),
  PARTITION r1 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Duplicate partition names: r1"));

        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh'),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Partition value list does not match column list"));

        let cases = vec![
            r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('sh', 1000),
  PARTITION r1 VALUES LESS THAN ('hz', 2000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito",
            r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 2000),
  PARTITION r1 VALUES LESS THAN ('hz', 1000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito",
            r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('hz', 1000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito",
            r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, 2000),
  PARTITION r1 VALUES LESS THAN ('sh', 3000),
)
ENGINE=mito",
        ];
        for sql in cases {
            let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("VALUES LESS THAN value must be strictly increasing for each partition"));
        }

        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh', 2000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, 9999),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Please provide an extra partition that is bounded by 'MAXVALUE'."));
    }

    #[test]
    fn test_parse_create_table_with_partitions() {
        let sql = r"
CREATE TABLE monitor (
  host_id    INT,
  idc        STRING,
  ts         TIMESTAMP,
  cpu        DOUBLE DEFAULT 0,
  memory     DOUBLE,
  TIME INDEX (ts),
  PRIMARY KEY (host),
)
PARTITION BY RANGE COLUMNS(idc, host_id) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh', 2000),
  PARTITION r2 VALUES LESS THAN ('sh', 3000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(result.len(), 1);
        match &result[0] {
            Statement::CreateTable(c) => {
                assert!(c.partitions.is_some());

                let partitions = c.partitions.as_ref().unwrap();
                let column_list = partitions
                    .column_list
                    .iter()
                    .map(|x| &x.value)
                    .collect::<Vec<&String>>();
                assert_eq!(column_list, vec!["idc", "host_id"]);

                let entries = &partitions.entries;
                let partition_names = entries
                    .iter()
                    .map(|x| &x.name.value)
                    .collect::<Vec<&String>>();
                assert_eq!(partition_names, vec!["r0", "r1", "r2", "r3"]);

                assert_eq!(
                    entries[0].value_list,
                    vec![
                        SqlValue::SingleQuotedString("hz".to_string()),
                        SqlValue::Number("1000".to_string(), false)
                    ]
                );
                assert_eq!(
                    entries[1].value_list,
                    vec![
                        SqlValue::SingleQuotedString("sh".to_string()),
                        SqlValue::Number("2000".to_string(), false)
                    ]
                );
                assert_eq!(
                    entries[2].value_list,
                    vec![
                        SqlValue::SingleQuotedString("sh".to_string()),
                        SqlValue::Number("3000".to_string(), false)
                    ]
                );
                assert_eq!(
                    entries[3].value_list,
                    vec![
                        SqlValue::Number(MAXVALUE.to_string(), false),
                        SqlValue::Number(MAXVALUE.to_string(), false)
                    ]
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_create_table_with_timestamp_index() {
        let sql1 = r"
CREATE TABLE monitor (
  host_id    INT,
  idc        STRING,
  ts         TIMESTAMP TIME INDEX,
  cpu        DOUBLE DEFAULT 0,
  memory     DOUBLE,
  PRIMARY KEY (host),
)
ENGINE=mito";
        let result1 = ParserContext::create_with_dialect(sql1, &GreptimeDbDialect {}).unwrap();

        if let Statement::CreateTable(c) = &result1[0] {
            assert_eq!(c.constraints.len(), 2);
            let tc = c.constraints[0].clone();
            match tc {
                TableConstraint::Unique {
                    name,
                    columns,
                    is_primary,
                } => {
                    assert_eq!(name.unwrap().to_string(), "__time_index");
                    assert_eq!(columns.len(), 1);
                    assert_eq!(&columns[0].value, "ts");
                    assert!(!is_primary);
                }
                _ => panic!("should be time index constraint"),
            };
        } else {
            panic!("should be create_table statement");
        }

        // `TIME INDEX` should be in front of `PRIMARY KEY`
        // in order to equal the `TIMESTAMP TIME INDEX` constraint options vector
        let sql2 = r"
CREATE TABLE monitor (
  host_id    INT,
  idc        STRING,
  ts         TIMESTAMP NOT NULL,
  cpu        DOUBLE DEFAULT 0,
  memory     DOUBLE,
  TIME INDEX (ts),
  PRIMARY KEY (host),
)
ENGINE=mito";
        let result2 = ParserContext::create_with_dialect(sql2, &GreptimeDbDialect {}).unwrap();

        assert_eq!(result1, result2);

        // TIMESTAMP can be NULL which is not equal to above
        let sql3 = r"
CREATE TABLE monitor (
  host_id    INT,
  idc        STRING,
  ts         TIMESTAMP,
  cpu        DOUBLE DEFAULT 0,
  memory     DOUBLE,
  TIME INDEX (ts),
  PRIMARY KEY (host),
)
ENGINE=mito";

        let result3 = ParserContext::create_with_dialect(sql3, &GreptimeDbDialect {}).unwrap();

        assert_ne!(result1, result3);

        // BIGINT can't be time index any more
        let sql1 = r"
CREATE TABLE monitor (
  host_id    INT,
  idc        STRING,
  b          bigint TIME INDEX,
  cpu        DOUBLE DEFAULT 0,
  memory     DOUBLE,
  PRIMARY KEY (host),
)
ENGINE=mito";
        let result1 = ParserContext::create_with_dialect(sql1, &GreptimeDbDialect {});

        assert!(result1
            .unwrap_err()
            .to_string()
            .contains("time index column data type should be timestamp"));
    }

    #[test]
    fn test_parse_create_table_with_timestamp_index_not_null() {
        let sql = r"
CREATE TABLE monitor (
  host_id    INT,
  idc        STRING,
  ts         TIMESTAMP TIME INDEX,
  cpu        DOUBLE DEFAULT 0,
  memory     DOUBLE,
  TIME INDEX (ts),
  PRIMARY KEY (host),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();

        assert_eq!(result.len(), 1);
        if let Statement::CreateTable(c) = &result[0] {
            let ts = c.columns[2].clone();
            assert_eq!(ts.name.to_string(), "ts");
            assert_eq!(ts.options[0].option, NotNull);
        } else {
            panic!("should be create table statement");
        }

        let sql1 = r"
CREATE TABLE monitor (
  host_id    INT,
  idc        STRING,
  ts         TIMESTAMP NOT NULL TIME INDEX,
  cpu        DOUBLE DEFAULT 0,
  memory     DOUBLE,
  TIME INDEX (ts),
  PRIMARY KEY (host),
)
ENGINE=mito";

        let result1 = ParserContext::create_with_dialect(sql1, &GreptimeDbDialect {}).unwrap();
        assert_eq!(result, result1);

        let sql2 = r"
CREATE TABLE monitor (
  host_id    INT,
  idc        STRING,
  ts         TIMESTAMP TIME INDEX NOT NULL,
  cpu        DOUBLE DEFAULT 0,
  memory     DOUBLE,
  TIME INDEX (ts),
  PRIMARY KEY (host),
)
ENGINE=mito";

        let result2 = ParserContext::create_with_dialect(sql2, &GreptimeDbDialect {}).unwrap();
        assert_eq!(result, result2);

        let sql3 = r"
CREATE TABLE monitor (
  host_id    INT,
  idc        STRING,
  ts         TIMESTAMP TIME INDEX NULL NOT,
  cpu        DOUBLE DEFAULT 0,
  memory     DOUBLE,
  TIME INDEX (ts),
  PRIMARY KEY (host),
)
ENGINE=mito";

        let result3 = ParserContext::create_with_dialect(sql3, &GreptimeDbDialect {});
        assert!(result3.is_err());

        let sql4 = r"
CREATE TABLE monitor (
  host_id    INT,
  idc        STRING,
  ts         TIMESTAMP TIME INDEX NOT NULL NULL,
  cpu        DOUBLE DEFAULT 0,
  memory     DOUBLE,
  TIME INDEX (ts),
  PRIMARY KEY (host),
)
ENGINE=mito";

        let result4 = ParserContext::create_with_dialect(sql4, &GreptimeDbDialect {});
        assert!(result4.is_err());

        let sql = r"
CREATE TABLE monitor (
  host_id    INT,
  idc        STRING,
  ts         TIMESTAMP TIME INDEX DEFAULT CURRENT_TIMESTAMP,
  cpu        DOUBLE DEFAULT 0,
  memory     DOUBLE,
  TIME INDEX (ts),
  PRIMARY KEY (host),
)
ENGINE=mito";

        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();

        if let Statement::CreateTable(c) = &result[0] {
            let tc = c.constraints[0].clone();
            match tc {
                TableConstraint::Unique {
                    name,
                    columns,
                    is_primary,
                } => {
                    assert_eq!(name.unwrap().to_string(), "__time_index");
                    assert_eq!(columns.len(), 1);
                    assert_eq!(&columns[0].value, "ts");
                    assert!(!is_primary);
                }
                _ => panic!("should be time index constraint"),
            }
            let ts = c.columns[2].clone();
            assert_eq!(ts.name.to_string(), "ts");
            assert!(matches!(ts.options[0].option, ColumnOption::Default(..)));
            assert_eq!(ts.options[1].option, NotNull);
        } else {
            unreachable!("should be create table statement");
        }
    }

    #[test]
    fn test_parse_partitions_with_error_syntax() {
        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh', 2000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        assert!(result
            .unwrap_err()
            .output_msg()
            .contains("sql parser error: Expected BY, found: RANGE"));

        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh', 2000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        assert!(result
            .unwrap_err()
            .output_msg()
            .contains("sql parser error: Expected LESS, found: THAN"));

        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS(b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 1000),
  PARTITION r1 VALUES LESS THAN ('sh', 2000),
  PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALU),
)
ENGINE=mito";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        assert!(result
            .unwrap_err()
            .output_msg()
            .contains("Expected a concrete value, found: MAXVALU"));
    }

    fn assert_column_def(column: &ColumnDef, name: &str, data_type: &str) {
        assert_eq!(column.name.to_string(), name);
        assert_eq!(column.data_type.to_string(), data_type);
    }

    #[test]
    pub fn test_parse_create_table() {
        let sql = r"create table demo(
                             host string,
                             ts timestamp,
                             cpu float32 default 0,
                             memory float64,
                             TIME INDEX (ts),
                             PRIMARY KEY(ts, host)) engine=mito
                             with(regions=1);
         ";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(1, result.len());
        match &result[0] {
            Statement::CreateTable(c) => {
                assert!(!c.if_not_exists);
                assert_eq!("demo", c.name.to_string());
                assert_eq!("mito", c.engine);
                assert_eq!(4, c.columns.len());
                let columns = &c.columns;
                assert_column_def(&columns[0], "host", "STRING");
                assert_column_def(&columns[1], "ts", "TIMESTAMP");
                assert_column_def(&columns[2], "cpu", "FLOAT");
                assert_column_def(&columns[3], "memory", "DOUBLE");

                let constraints = &c.constraints;
                assert_matches!(
                    &constraints[0],
                    TableConstraint::Unique {
                        is_primary: false,
                        ..
                    }
                );
                assert_matches!(
                    &constraints[1],
                    TableConstraint::Unique {
                        is_primary: true,
                        ..
                    }
                );
                let options = &c.options;
                assert_eq!(1, options.len());
                assert_eq!("regions", &options[0].name.to_string());
                assert_eq!("1", &options[0].value.to_string());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_invalid_index_keys() {
        let sql = r"create table demo(
                             host string,
                             ts int64,
                             cpu float64 default 0,
                             memory float64,
                             TIME INDEX (ts, host),
                             PRIMARY KEY(ts, host)) engine=mito
                             with(regions=1);
         ";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        assert!(result.is_err());
        assert_matches!(result, Err(crate::error::Error::InvalidTimeIndex { .. }));
    }

    #[test]
    fn test_duplicated_time_index() {
        let sql = r"create table demo(
                             host string,
                             ts timestamp time index,
                             t timestamp time index,
                             cpu float64 default 0,
                             memory float64,
                             TIME INDEX (ts, host),
                             PRIMARY KEY(ts, host)) engine=mito
                             with(regions=1);
         ";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        assert!(result.is_err());
        assert_matches!(result, Err(crate::error::Error::InvalidTimeIndex { .. }));

        let sql = r"create table demo(
                             host string,
                             ts timestamp time index,
                             cpu float64 default 0,
                             t timestamp,
                             memory float64,
                             TIME INDEX (t),
                             PRIMARY KEY(ts, host)) engine=mito
                             with(regions=1);
         ";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        assert!(result.is_err());
        assert_matches!(result, Err(crate::error::Error::InvalidTimeIndex { .. }));
    }

    #[test]
    fn test_invalid_column_name() {
        let sql = "create table foo(user string, i timestamp time index)";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        let err = result.unwrap_err().output_msg();
        assert!(err.contains("Cannot use keyword 'user' as column name"));

        // If column name is quoted, it's valid even same with keyword.
        let sql = r#"
            create table foo("user" string, i timestamp time index)
        "#;
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        let _ = result.unwrap();
    }
}
