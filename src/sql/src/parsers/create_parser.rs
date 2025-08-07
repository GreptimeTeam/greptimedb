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

use std::collections::HashMap;

use common_catalog::consts::default_engine;
use datafusion_common::ScalarValue;
use datatypes::arrow::datatypes::{DataType as ArrowDataType, IntervalUnit};
use datatypes::data_type::ConcreteDataType;
use itertools::Itertools;
use snafu::{ensure, OptionExt, ResultExt};
use sqlparser::ast::{ColumnOption, ColumnOptionDef, DataType, Expr};
use sqlparser::dialect::keywords::Keyword;
use sqlparser::keywords::ALL_KEYWORDS;
use sqlparser::parser::IsOptional::Mandatory;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, TokenWithSpan, Word};
use table::requests::{validate_database_option, validate_table_option};

use crate::ast::{ColumnDef, Ident, ObjectNamePartExt};
use crate::error::{
    self, InvalidColumnOptionSnafu, InvalidDatabaseOptionSnafu, InvalidIntervalSnafu,
    InvalidSqlSnafu, InvalidTableOptionSnafu, InvalidTimeIndexSnafu, MissingTimeIndexSnafu, Result,
    SyntaxSnafu, UnexpectedSnafu, UnsupportedSnafu,
};
use crate::parser::{ParserContext, FLOW};
use crate::parsers::utils::{
    self, validate_column_fulltext_create_option, validate_column_skipping_index_create_option,
};
use crate::statements::create::{
    Column, ColumnExtensions, CreateDatabase, CreateExternalTable, CreateFlow, CreateTable,
    CreateTableLike, CreateView, Partitions, SqlOrTql, TableConstraint, VECTOR_OPT_DIM,
};
use crate::statements::statement::Statement;
use crate::statements::transform::type_alias::get_data_type_by_alias_name;
use crate::statements::{sql_data_type_to_concrete_data_type, OptionMap};
use crate::util::{location_to_index, parse_option_string};

pub const ENGINE: &str = "ENGINE";
pub const MAXVALUE: &str = "MAXVALUE";
pub const SINK: &str = "SINK";
pub const EXPIRE: &str = "EXPIRE";
pub const AFTER: &str = "AFTER";
pub const INVERTED: &str = "INVERTED";
pub const SKIPPING: &str = "SKIPPING";

/// Parses create [table] statement
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_create(&mut self) -> Result<Statement> {
        match self.parser.peek_token().token {
            Token::Word(w) => match w.keyword {
                Keyword::TABLE => self.parse_create_table(),

                Keyword::SCHEMA | Keyword::DATABASE => self.parse_create_database(),

                Keyword::EXTERNAL => self.parse_create_external_table(),

                Keyword::OR => {
                    let _ = self.parser.next_token();
                    self.parser
                        .expect_keyword(Keyword::REPLACE)
                        .context(SyntaxSnafu)?;
                    match self.parser.next_token().token {
                        Token::Word(w) => match w.keyword {
                            Keyword::VIEW => self.parse_create_view(true),
                            Keyword::NoKeyword => {
                                let uppercase = w.value.to_uppercase();
                                match uppercase.as_str() {
                                    FLOW => self.parse_create_flow(true),
                                    _ => self.unsupported(w.to_string()),
                                }
                            }
                            _ => self.unsupported(w.to_string()),
                        },
                        _ => self.unsupported(w.to_string()),
                    }
                }

                Keyword::VIEW => {
                    let _ = self.parser.next_token();
                    self.parse_create_view(false)
                }

                #[cfg(feature = "enterprise")]
                Keyword::TRIGGER => {
                    let _ = self.parser.next_token();
                    self.parse_create_trigger()
                }

                Keyword::NoKeyword => {
                    let _ = self.parser.next_token();
                    let uppercase = w.value.to_uppercase();
                    match uppercase.as_str() {
                        FLOW => self.parse_create_flow(false),
                        _ => self.unsupported(w.to_string()),
                    }
                }
                _ => self.unsupported(w.to_string()),
            },
            unexpected => self.unsupported(unexpected.to_string()),
        }
    }

    /// Parse `CREAVE VIEW` statement.
    fn parse_create_view(&mut self, or_replace: bool) -> Result<Statement> {
        let if_not_exists = self.parse_if_not_exist()?;
        let view_name = self.intern_parse_table_name()?;

        let columns = self.parse_view_columns()?;

        self.parser
            .expect_keyword(Keyword::AS)
            .context(SyntaxSnafu)?;

        let query = self.parse_query()?;

        Ok(Statement::CreateView(CreateView {
            name: view_name,
            columns,
            or_replace,
            query: Box::new(query),
            if_not_exists,
        }))
    }

    fn parse_view_columns(&mut self) -> Result<Vec<Ident>> {
        let mut columns = vec![];
        if !self.parser.consume_token(&Token::LParen) || self.parser.consume_token(&Token::RParen) {
            return Ok(columns);
        }

        loop {
            let name = self.parse_column_name().context(SyntaxSnafu)?;

            columns.push(name);

            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected("',' or ')' after column name", self.parser.peek_token());
            }
        }

        Ok(columns)
    }

    fn parse_create_external_table(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();
        self.parser
            .expect_keyword(Keyword::TABLE)
            .context(SyntaxSnafu)?;
        let if_not_exists = self.parse_if_not_exist()?;
        let table_name = self.intern_parse_table_name()?;
        let (columns, constraints) = self.parse_columns()?;
        if !columns.is_empty() {
            validate_time_index(&columns, &constraints)?;
        }

        let engine = self.parse_table_engine(common_catalog::consts::FILE_ENGINE)?;
        let options = self.parse_create_table_options()?;
        Ok(Statement::CreateExternalTable(CreateExternalTable {
            name: table_name,
            columns,
            constraints,
            options,
            if_not_exists,
            engine,
        }))
    }

    fn parse_create_database(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();
        let if_not_exists = self.parse_if_not_exist()?;
        let database_name = self.parse_object_name().context(error::UnexpectedSnafu {
            expected: "a database name",
            actual: self.peek_token_as_string(),
        })?;
        let database_name = Self::canonicalize_object_name(database_name);

        let options = self
            .parser
            .parse_options(Keyword::WITH)
            .context(SyntaxSnafu)?
            .into_iter()
            .map(parse_option_string)
            .collect::<Result<HashMap<String, String>>>()?;

        for key in options.keys() {
            ensure!(
                validate_database_option(key),
                InvalidDatabaseOptionSnafu {
                    key: key.to_string()
                }
            );
        }
        if let Some(append_mode) = options.get("append_mode") {
            if append_mode == "true" && options.contains_key("merge_mode") {
                return InvalidDatabaseOptionSnafu {
                    key: "merge_mode".to_string(),
                }
                .fail();
            }
        }

        Ok(Statement::CreateDatabase(CreateDatabase {
            name: database_name,
            if_not_exists,
            options: options.into(),
        }))
    }

    fn parse_create_table(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();

        let if_not_exists = self.parse_if_not_exist()?;

        let table_name = self.intern_parse_table_name()?;

        if self.parser.parse_keyword(Keyword::LIKE) {
            let source_name = self.intern_parse_table_name()?;

            return Ok(Statement::CreateTableLike(CreateTableLike {
                table_name,
                source_name,
            }));
        }

        let (columns, constraints) = self.parse_columns()?;
        validate_time_index(&columns, &constraints)?;

        let partitions = self.parse_partitions()?;
        if let Some(partitions) = &partitions {
            validate_partitions(&columns, partitions)?;
        }

        let engine = self.parse_table_engine(default_engine())?;
        let options = self.parse_create_table_options()?;
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

        Ok(Statement::CreateTable(create_table))
    }

    /// "CREATE FLOW" clause
    fn parse_create_flow(&mut self, or_replace: bool) -> Result<Statement> {
        let if_not_exists = self.parse_if_not_exist()?;

        let flow_name = self.intern_parse_table_name()?;

        // make `SINK` case in-sensitive
        if let Token::Word(word) = self.parser.peek_token().token
            && word.value.eq_ignore_ascii_case(SINK)
        {
            self.parser.next_token();
        } else {
            Err(ParserError::ParserError(
                "Expect `SINK` keyword".to_string(),
            ))
            .context(SyntaxSnafu)?
        }
        self.parser
            .expect_keyword(Keyword::TO)
            .context(SyntaxSnafu)?;

        let output_table_name = self.intern_parse_table_name()?;

        let expire_after = if self
            .parser
            .consume_tokens(&[Token::make_keyword(EXPIRE), Token::make_keyword(AFTER)])
        {
            Some(self.parse_interval()?)
        } else {
            None
        };

        let comment = if self.parser.parse_keyword(Keyword::COMMENT) {
            match self.parser.next_token() {
                TokenWithSpan {
                    token: Token::SingleQuotedString(value, ..),
                    ..
                } => Some(value),
                unexpected => {
                    return self
                        .parser
                        .expected("string", unexpected)
                        .context(SyntaxSnafu)
                }
            }
        } else {
            None
        };

        self.parser
            .expect_keyword(Keyword::AS)
            .context(SyntaxSnafu)?;

        let start_loc = self.parser.peek_token().span.start;
        let start_index = location_to_index(self.sql, &start_loc);

        let query = self.parse_statement()?;
        let end_token = self.parser.peek_token();

        let raw_query = if end_token == Token::EOF {
            &self.sql[start_index..]
        } else {
            let end_loc = end_token.span.end;
            let end_index = location_to_index(self.sql, &end_loc);
            &self.sql[start_index..end_index.min(self.sql.len())]
        };
        let raw_query = raw_query.trim_end_matches(";");

        let query = Box::new(SqlOrTql::try_from_statement(query, raw_query)?);

        Ok(Statement::CreateFlow(CreateFlow {
            flow_name,
            sink_table_name: output_table_name,
            or_replace,
            if_not_exists,
            expire_after,
            comment,
            query,
        }))
    }

    /// Parse the interval expr to duration in seconds.
    fn parse_interval(&mut self) -> Result<i64> {
        let interval_expr = self.parser.parse_expr().context(error::SyntaxSnafu)?;
        let interval = utils::parser_expr_to_scalar_value_literal(interval_expr.clone())?
            .cast_to(&ArrowDataType::Interval(IntervalUnit::MonthDayNano))
            .ok()
            .with_context(|| InvalidIntervalSnafu {
                reason: format!("cannot cast {} to interval type", interval_expr),
            })?;
        if let ScalarValue::IntervalMonthDayNano(Some(interval)) = interval {
            Ok(
                interval.nanoseconds / 1_000_000_000
                    + interval.days as i64 * 60 * 60 * 24
                    + interval.months as i64 * 60 * 60 * 24 * 3044 / 1000, // 1 month=365.25/12=30.44 days
                                                                           // this is to keep the same as https://docs.rs/humantime/latest/humantime/fn.parse_duration.html
                                                                           // which we use in database to parse i.e. ttl interval and many other intervals
            )
        } else {
            unreachable!()
        }
    }

    fn parse_if_not_exist(&mut self) -> Result<bool> {
        match self.parser.peek_token().token {
            Token::Word(w) if Keyword::IF != w.keyword => return Ok(false),
            _ => {}
        }

        if self.parser.parse_keywords(&[Keyword::IF, Keyword::NOT]) {
            return self
                .parser
                .expect_keyword(Keyword::EXISTS)
                .map(|_| true)
                .context(UnexpectedSnafu {
                    expected: "EXISTS",
                    actual: self.peek_token_as_string(),
                });
        }

        if self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]) {
            return UnsupportedSnafu { keyword: "EXISTS" }.fail();
        }

        Ok(false)
    }

    fn parse_create_table_options(&mut self) -> Result<OptionMap> {
        let options = self
            .parser
            .parse_options(Keyword::WITH)
            .context(SyntaxSnafu)?
            .into_iter()
            .map(parse_option_string)
            .collect::<Result<HashMap<String, String>>>()?;
        for key in options.keys() {
            ensure!(validate_table_option(key), InvalidTableOptionSnafu { key });
        }
        Ok(options.into())
    }

    /// "PARTITION ON COLUMNS (...)" clause
    fn parse_partitions(&mut self) -> Result<Option<Partitions>> {
        if !self.parser.parse_keyword(Keyword::PARTITION) {
            return Ok(None);
        }
        self.parser
            .expect_keywords(&[Keyword::ON, Keyword::COLUMNS])
            .context(error::UnexpectedSnafu {
                expected: "ON, COLUMNS",
                actual: self.peek_token_as_string(),
            })?;

        let raw_column_list = self
            .parser
            .parse_parenthesized_column_list(Mandatory, false)
            .context(error::SyntaxSnafu)?;
        let column_list = raw_column_list
            .into_iter()
            .map(Self::canonicalize_identifier)
            .collect();

        let exprs = self.parse_comma_separated(Self::parse_partition_entry)?;

        Ok(Some(Partitions { column_list, exprs }))
    }

    fn parse_partition_entry(&mut self) -> Result<Expr> {
        self.parser.parse_expr().context(error::SyntaxSnafu)
    }

    /// Parse a comma-separated list wrapped by "()", and of which all items accepted by `F`
    fn parse_comma_separated<T, F>(&mut self, mut f: F) -> Result<Vec<T>>
    where
        F: FnMut(&mut ParserContext<'a>) -> Result<T>,
    {
        self.parser
            .expect_token(&Token::LParen)
            .context(error::UnexpectedSnafu {
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
                expected: ")",
                actual: self.peek_token_as_string(),
            })?;

        Ok(values)
    }

    /// Parse the columns and constraints.
    fn parse_columns(&mut self) -> Result<(Vec<Column>, Vec<TableConstraint>)> {
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
        columns: &mut Vec<Column>,
        constraints: &mut Vec<TableConstraint>,
    ) -> Result<()> {
        let mut column = self.parse_column_def()?;

        let mut time_index_opt_idx = None;
        for (index, opt) in column.options().iter().enumerate() {
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
                            name: column.name().to_string(),
                            msg: "duplicated time index",
                        }
                    );
                    time_index_opt_idx = Some(index);

                    let constraint = TableConstraint::TimeIndex {
                        column: Ident::new(column.name().value.clone()),
                    };
                    constraints.push(constraint);
                }
            }
        }

        if let Some(index) = time_index_opt_idx {
            ensure!(
                !column.options().contains(&ColumnOptionDef {
                    option: ColumnOption::Null,
                    name: None,
                }),
                InvalidColumnOptionSnafu {
                    name: column.name().to_string(),
                    msg: "time index column can't be null",
                }
            );

            // The timestamp type may be an alias type, we have to retrieve the actual type.
            let data_type = get_unalias_type(column.data_type());
            ensure!(
                matches!(data_type, DataType::Timestamp(_, _)),
                InvalidColumnOptionSnafu {
                    name: column.name().to_string(),
                    msg: "time index column data type should be timestamp",
                }
            );

            let not_null_opt = ColumnOptionDef {
                option: ColumnOption::NotNull,
                name: None,
            };

            if !column.options().contains(&not_null_opt) {
                column.mut_options().push(not_null_opt);
            }

            let _ = column.mut_options().remove(index);
        }

        columns.push(column);

        Ok(())
    }

    /// Parse the column name and check if it's valid.
    fn parse_column_name(&mut self) -> std::result::Result<Ident, ParserError> {
        let name = self.parser.parse_identifier()?;
        if name.quote_style.is_none() &&
        // "ALL_KEYWORDS" are sorted.
            ALL_KEYWORDS.binary_search(&name.value.to_uppercase().as_str()).is_ok()
        {
            return Err(ParserError::ParserError(format!(
                "Cannot use keyword '{}' as column name. Hint: add quotes to the name.",
                &name.value
            )));
        }

        Ok(name)
    }

    pub fn parse_column_def(&mut self) -> Result<Column> {
        let name = self.parse_column_name().context(SyntaxSnafu)?;
        let parser = &mut self.parser;

        ensure!(
            !(name.quote_style.is_none() &&
            // "ALL_KEYWORDS" are sorted.
            ALL_KEYWORDS.binary_search(&name.value.to_uppercase().as_str()).is_ok()),
            InvalidSqlSnafu {
                msg: format!(
                    "Cannot use keyword '{}' as column name. Hint: add quotes to the name.",
                    &name.value
                ),
            }
        );

        let data_type = parser.parse_data_type().context(SyntaxSnafu)?;
        let mut options = vec![];
        let mut extensions = ColumnExtensions::default();
        loop {
            if parser.parse_keyword(Keyword::CONSTRAINT) {
                let name = Some(parser.parse_identifier().context(SyntaxSnafu)?);
                if let Some(option) = Self::parse_optional_column_option(parser)? {
                    options.push(ColumnOptionDef { name, option });
                } else {
                    return parser
                        .expected(
                            "constraint details after CONSTRAINT <name>",
                            parser.peek_token(),
                        )
                        .context(SyntaxSnafu);
                }
            } else if let Some(option) = Self::parse_optional_column_option(parser)? {
                options.push(ColumnOptionDef { name: None, option });
            } else if !Self::parse_column_extensions(parser, &name, &data_type, &mut extensions)? {
                break;
            };
        }

        Ok(Column {
            column_def: ColumnDef {
                name: Self::canonicalize_identifier(name),
                data_type,
                options,
            },
            extensions,
        })
    }

    fn parse_optional_column_option(parser: &mut Parser<'_>) -> Result<Option<ColumnOption>> {
        if parser.parse_keywords(&[Keyword::CHARACTER, Keyword::SET]) {
            Ok(Some(ColumnOption::CharacterSet(
                parser.parse_object_name(false).context(SyntaxSnafu)?,
            )))
        } else if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            Ok(Some(ColumnOption::NotNull))
        } else if parser.parse_keywords(&[Keyword::COMMENT]) {
            match parser.next_token() {
                TokenWithSpan {
                    token: Token::SingleQuotedString(value, ..),
                    ..
                } => Ok(Some(ColumnOption::Comment(value))),
                unexpected => parser.expected("string", unexpected).context(SyntaxSnafu),
            }
        } else if parser.parse_keyword(Keyword::NULL) {
            Ok(Some(ColumnOption::Null))
        } else if parser.parse_keyword(Keyword::DEFAULT) {
            Ok(Some(ColumnOption::Default(
                parser.parse_expr().context(SyntaxSnafu)?,
            )))
        } else if parser.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY]) {
            Ok(Some(ColumnOption::Unique {
                is_primary: true,
                characteristics: None,
            }))
        } else if parser.parse_keyword(Keyword::UNIQUE) {
            Ok(Some(ColumnOption::Unique {
                is_primary: false,
                characteristics: None,
            }))
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

    /// Parse a column option extensions.
    ///
    /// This function will handle:
    /// - Vector type
    /// - Indexes
    fn parse_column_extensions(
        parser: &mut Parser<'_>,
        column_name: &Ident,
        column_type: &DataType,
        column_extensions: &mut ColumnExtensions,
    ) -> Result<bool> {
        if let DataType::Custom(name, tokens) = column_type
            && name.0.len() == 1
            && &name.0[0].to_string_unquoted().to_uppercase() == "VECTOR"
        {
            ensure!(
                tokens.len() == 1,
                InvalidColumnOptionSnafu {
                    name: column_name.to_string(),
                    msg: "VECTOR type should have dimension",
                }
            );

            let dimension =
                tokens[0]
                    .parse::<u32>()
                    .ok()
                    .with_context(|| InvalidColumnOptionSnafu {
                        name: column_name.to_string(),
                        msg: "dimension should be a positive integer",
                    })?;

            let options = OptionMap::from([(VECTOR_OPT_DIM.to_string(), dimension.to_string())]);
            column_extensions.vector_options = Some(options);
        }

        // parse index options in column definition
        let mut is_index_declared = false;

        // skipping index
        if let Token::Word(word) = parser.peek_token().token
            && word.value.eq_ignore_ascii_case(SKIPPING)
        {
            parser.next_token();
            // Consume `INDEX` keyword
            ensure!(
                parser.parse_keyword(Keyword::INDEX),
                InvalidColumnOptionSnafu {
                    name: column_name.to_string(),
                    msg: "expect INDEX after SKIPPING keyword",
                }
            );
            ensure!(
                column_extensions.skipping_index_options.is_none(),
                InvalidColumnOptionSnafu {
                    name: column_name.to_string(),
                    msg: "duplicated SKIPPING index option",
                }
            );

            let options = parser
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

            column_extensions.skipping_index_options = Some(options.into());
            is_index_declared |= true;
        }

        // fulltext index
        if parser.parse_keyword(Keyword::FULLTEXT) {
            // Consume `INDEX` keyword
            ensure!(
                parser.parse_keyword(Keyword::INDEX),
                InvalidColumnOptionSnafu {
                    name: column_name.to_string(),
                    msg: "expect INDEX after FULLTEXT keyword",
                }
            );

            ensure!(
                column_extensions.fulltext_index_options.is_none(),
                InvalidColumnOptionSnafu {
                    name: column_name.to_string(),
                    msg: "duplicated FULLTEXT INDEX option",
                }
            );

            let column_type = get_unalias_type(column_type);
            let data_type = sql_data_type_to_concrete_data_type(&column_type)?;
            ensure!(
                data_type == ConcreteDataType::string_datatype(),
                InvalidColumnOptionSnafu {
                    name: column_name.to_string(),
                    msg: "FULLTEXT index only supports string type",
                }
            );

            let options = parser
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
                        msg: format!("invalid FULLTEXT INDEX option: {key}"),
                    }
                );
            }

            column_extensions.fulltext_index_options = Some(options.into());
            is_index_declared |= true;
        }

        // inverted index
        if let Token::Word(word) = parser.peek_token().token
            && word.value.eq_ignore_ascii_case(INVERTED)
        {
            parser.next_token();
            // Consume `INDEX` keyword
            ensure!(
                parser.parse_keyword(Keyword::INDEX),
                InvalidColumnOptionSnafu {
                    name: column_name.to_string(),
                    msg: "expect INDEX after INVERTED keyword",
                }
            );

            ensure!(
                column_extensions.inverted_index_options.is_none(),
                InvalidColumnOptionSnafu {
                    name: column_name.to_string(),
                    msg: "duplicated INVERTED index option",
                }
            );

            // inverted index doesn't have options, skipping `WITH`
            // try cache `WITH` and throw error
            let with_token = parser.peek_token();
            ensure!(
                with_token.token
                    != Token::Word(Word {
                        value: "WITH".to_string(),
                        keyword: Keyword::WITH,
                        quote_style: None,
                    }),
                InvalidColumnOptionSnafu {
                    name: column_name.to_string(),
                    msg: "INVERTED index doesn't support options",
                }
            );

            column_extensions.inverted_index_options = Some(OptionMap::default());
            is_index_declared |= true;
        }

        Ok(is_index_declared)
    }

    fn parse_optional_table_constraint(&mut self) -> Result<Option<TableConstraint>> {
        match self.parser.next_token() {
            TokenWithSpan {
                token: Token::Word(w),
                ..
            } if w.keyword == Keyword::PRIMARY => {
                self.parser
                    .expect_keyword(Keyword::KEY)
                    .context(error::UnexpectedSnafu {
                        expected: "KEY",
                        actual: self.peek_token_as_string(),
                    })?;
                let raw_columns = self
                    .parser
                    .parse_parenthesized_column_list(Mandatory, false)
                    .context(error::SyntaxSnafu)?;
                let columns = raw_columns
                    .into_iter()
                    .map(Self::canonicalize_identifier)
                    .collect();
                Ok(Some(TableConstraint::PrimaryKey { columns }))
            }
            TokenWithSpan {
                token: Token::Word(w),
                ..
            } if w.keyword == Keyword::TIME => {
                self.parser
                    .expect_keyword(Keyword::INDEX)
                    .context(error::UnexpectedSnafu {
                        expected: "INDEX",
                        actual: self.peek_token_as_string(),
                    })?;

                let raw_columns = self
                    .parser
                    .parse_parenthesized_column_list(Mandatory, false)
                    .context(error::SyntaxSnafu)?;
                let mut columns = raw_columns
                    .into_iter()
                    .map(Self::canonicalize_identifier)
                    .collect::<Vec<_>>();

                ensure!(
                    columns.len() == 1,
                    InvalidTimeIndexSnafu {
                        msg: "it should contain only one column in time index",
                    }
                );

                Ok(Some(TableConstraint::TimeIndex {
                    column: columns.pop().unwrap(),
                }))
            }
            _ => {
                self.parser.prev_token();
                Ok(None)
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

fn validate_time_index(columns: &[Column], constraints: &[TableConstraint]) -> Result<()> {
    let time_index_constraints: Vec<_> = constraints
        .iter()
        .filter_map(|c| match c {
            TableConstraint::TimeIndex { column } => Some(column),
            _ => None,
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

    // It's safe to use time_index_constraints[0][0],
    // we already check the bound above.
    let time_index_column_ident = &time_index_constraints[0];
    let time_index_column = columns
        .iter()
        .find(|c| c.name().value == *time_index_column_ident.value)
        .with_context(|| InvalidTimeIndexSnafu {
            msg: format!(
                "time index column {} not found in columns",
                time_index_column_ident
            ),
        })?;

    let time_index_data_type = get_unalias_type(time_index_column.data_type());
    ensure!(
        matches!(time_index_data_type, DataType::Timestamp(_, _)),
        InvalidColumnOptionSnafu {
            name: time_index_column.name().to_string(),
            msg: "time index column data type should be timestamp",
        }
    );

    Ok(())
}

fn get_unalias_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Custom(name, tokens) if name.0.len() == 1 && tokens.is_empty() => {
            if let Some(real_type) =
                get_data_type_by_alias_name(name.0[0].to_string_unquoted().as_str())
            {
                real_type
            } else {
                data_type.clone()
            }
        }
        _ => data_type.clone(),
    }
}

fn validate_partitions(columns: &[Column], partitions: &Partitions) -> Result<()> {
    let partition_columns = ensure_partition_columns_defined(columns, partitions)?;

    ensure_exprs_are_binary(&partitions.exprs, &partition_columns)?;

    Ok(())
}

/// Ensure all exprs are binary expr and all the columns are defined in the column list.
fn ensure_exprs_are_binary(exprs: &[Expr], columns: &[&Column]) -> Result<()> {
    for expr in exprs {
        // The first level must be binary expr
        if let Expr::BinaryOp { left, op: _, right } = expr {
            ensure_one_expr(left, columns)?;
            ensure_one_expr(right, columns)?;
        } else {
            return error::InvalidSqlSnafu {
                msg: format!("Partition rule expr {:?} is not a binary expr", expr),
            }
            .fail();
        }
    }
    Ok(())
}

/// Check if the expr is a binary expr, an ident or a literal value.
/// If is ident, then check it is in the column list.
/// This recursive function is intended to be used by [ensure_exprs_are_binary].
fn ensure_one_expr(expr: &Expr, columns: &[&Column]) -> Result<()> {
    match expr {
        Expr::BinaryOp { left, op: _, right } => {
            ensure_one_expr(left, columns)?;
            ensure_one_expr(right, columns)?;
            Ok(())
        }
        Expr::Identifier(ident) => {
            let column_name = &ident.value;
            ensure!(
                columns.iter().any(|c| &c.name().value == column_name),
                error::InvalidSqlSnafu {
                    msg: format!(
                        "Column {:?} in rule expr is not referenced in PARTITION ON",
                        column_name
                    ),
                }
            );
            Ok(())
        }
        Expr::Value(_) => Ok(()),
        Expr::UnaryOp { expr, .. } => {
            ensure_one_expr(expr, columns)?;
            Ok(())
        }
        _ => error::InvalidSqlSnafu {
            msg: format!("Partition rule expr {:?} is not a binary expr", expr),
        }
        .fail(),
    }
}

/// Ensure that all columns used in "PARTITION ON COLUMNS" are defined in create table.
fn ensure_partition_columns_defined<'a>(
    columns: &'a [Column],
    partitions: &'a Partitions,
) -> Result<Vec<&'a Column>> {
    partitions
        .column_list
        .iter()
        .map(|x| {
            let x = ParserContext::canonicalize_identifier(x.clone());
            // Normally the columns in "create table" won't be too many,
            // a linear search to find the target every time is fine.
            columns
                .iter()
                .find(|c| *c.name().value == x.value)
                .context(error::InvalidSqlSnafu {
                    msg: format!("Partition column {:?} not defined", x.value),
                })
        })
        .collect::<Result<Vec<&Column>>>()
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;

    use common_catalog::consts::FILE_ENGINE;
    use common_error::ext::ErrorExt;
    use sqlparser::ast::ColumnOption::NotNull;
    use sqlparser::ast::{BinaryOperator, Expr, ObjectName, Value};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::tokenizer::Tokenizer;

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParseOptions;

    #[test]
    fn test_parse_create_table_like() {
        let sql = "CREATE TABLE t1 LIKE t2";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();

        assert_eq!(1, stmts.len());
        match &stmts[0] {
            Statement::CreateTableLike(c) => {
                assert_eq!(c.table_name.to_string(), "t1");
                assert_eq!(c.source_name.to_string(), "t2");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_validate_external_table_options() {
        let sql = "CREATE EXTERNAL TABLE city (
            host string,
            ts timestamp,
            cpu float64 default 0,
            memory float64,
            TIME INDEX (ts),
            PRIMARY KEY(ts, host)
        ) with(location='/var/data/city.csv',format='csv',foo='bar');";

        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
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
            Test {
                sql: "CREATE EXTERNAL TABLE IF NOT EXISTS city ENGINE=foo with(location='/var/data/city.csv',format='csv','compaction.type'='bar');",
                expected_table_name: "city",
                expected_options: HashMap::from([
                    ("location".to_string(), "/var/data/city.csv".to_string()),
                    ("format".to_string(), "csv".to_string()),
                    ("compaction.type".to_string(), "bar".to_string()),
                ]),
                expected_engine: "foo",
                expected_if_not_exist: true,
            },
        ];

        for test in tests {
            let stmts = ParserContext::create_with_dialect(
                test.sql,
                &GreptimeDbDialect {},
                ParseOptions::default(),
            )
            .unwrap();
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
            ts timestamp,
            cpu float32 default 0,
            memory float64,
            TIME INDEX (ts),
            PRIMARY KEY(ts, host),
        ) with(location='/var/data/city.csv',format='csv');";

        let options = HashMap::from([
            ("location".to_string(), "/var/data/city.csv".to_string()),
            ("format".to_string(), "csv".to_string()),
        ]);

        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        match &stmts[0] {
            Statement::CreateExternalTable(c) => {
                assert_eq!(c.name.to_string(), "city");
                assert_eq!(c.options, options.into());

                let columns = &c.columns;
                assert_column_def(&columns[0].column_def, "host", "STRING");
                assert_column_def(&columns[1].column_def, "ts", "TIMESTAMP");
                assert_column_def(&columns[2].column_def, "cpu", "FLOAT");
                assert_column_def(&columns[3].column_def, "memory", "DOUBLE");

                let constraints = &c.constraints;
                assert_eq!(
                    &constraints[0],
                    &TableConstraint::TimeIndex {
                        column: Ident::new("ts"),
                    }
                );
                assert_eq!(
                    &constraints[1],
                    &TableConstraint::PrimaryKey {
                        columns: vec![Ident::new("ts"), Ident::new("host")]
                    }
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_create_database() {
        let sql = "create database";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unexpected token while parsing SQL statement"));

        let sql = "create database prometheus";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();

        assert_eq!(1, stmts.len());
        match &stmts[0] {
            Statement::CreateDatabase(c) => {
                assert_eq!(c.name.to_string(), "prometheus");
                assert!(!c.if_not_exists);
            }
            _ => unreachable!(),
        }

        let sql = "create database if not exists prometheus";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();

        assert_eq!(1, stmts.len());
        match &stmts[0] {
            Statement::CreateDatabase(c) => {
                assert_eq!(c.name.to_string(), "prometheus");
                assert!(c.if_not_exists);
            }
            _ => unreachable!(),
        }

        let sql = "CREATE DATABASE `fOo`";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        match &stmts.last().unwrap() {
            Statement::CreateDatabase(c) => {
                assert_eq!(c.name, vec![Ident::with_quote('`', "fOo")].into());
                assert!(!c.if_not_exists);
            }
            _ => unreachable!(),
        }

        let sql = "CREATE DATABASE prometheus with (ttl='1h');";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        match &stmts[0] {
            Statement::CreateDatabase(c) => {
                assert_eq!(c.name.to_string(), "prometheus");
                assert!(!c.if_not_exists);
                assert_eq!(c.options.get("ttl").unwrap(), "1h");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_create_flow_more_testcases() {
        use pretty_assertions::assert_eq;
        fn parse_create_flow(sql: &str) -> CreateFlow {
            let stmts = ParserContext::create_with_dialect(
                sql,
                &GreptimeDbDialect {},
                ParseOptions::default(),
            )
            .unwrap();
            assert_eq!(1, stmts.len());
            match &stmts[0] {
                Statement::CreateFlow(c) => c.clone(),
                _ => unreachable!(),
            }
        }
        struct CreateFlowWoutQuery {
            /// Flow name
            pub flow_name: ObjectName,
            /// Output (sink) table name
            pub sink_table_name: ObjectName,
            /// Whether to replace existing task
            pub or_replace: bool,
            /// Create if not exist
            pub if_not_exists: bool,
            /// `EXPIRE AFTER`
            /// Duration in second as `i64`
            pub expire_after: Option<i64>,
            /// Comment string
            pub comment: Option<String>,
        }
        let testcases = vec![
            (
                r"
CREATE OR REPLACE FLOW IF NOT EXISTS task_1
SINK TO schema_1.table_1
EXPIRE AFTER INTERVAL '5 minutes'
COMMENT 'test comment'
AS
SELECT max(c1), min(c2) FROM schema_2.table_2;",
                CreateFlowWoutQuery {
                    flow_name: ObjectName::from(vec![Ident::new("task_1")]),
                    sink_table_name: ObjectName::from(vec![
                        Ident::new("schema_1"),
                        Ident::new("table_1"),
                    ]),
                    or_replace: true,
                    if_not_exists: true,
                    expire_after: Some(300),
                    comment: Some("test comment".to_string()),
                },
            ),
            (
                r"
CREATE OR REPLACE FLOW IF NOT EXISTS task_1
SINK TO schema_1.table_1
EXPIRE AFTER INTERVAL '300 s'
COMMENT 'test comment'
AS
SELECT max(c1), min(c2) FROM schema_2.table_2;",
                CreateFlowWoutQuery {
                    flow_name: ObjectName::from(vec![Ident::new("task_1")]),
                    sink_table_name: ObjectName::from(vec![
                        Ident::new("schema_1"),
                        Ident::new("table_1"),
                    ]),
                    or_replace: true,
                    if_not_exists: true,
                    expire_after: Some(300),
                    comment: Some("test comment".to_string()),
                },
            ),
            (
                r"
CREATE OR REPLACE FLOW IF NOT EXISTS task_1
SINK TO schema_1.table_1
EXPIRE AFTER '5 minutes'
COMMENT 'test comment'
AS
SELECT max(c1), min(c2) FROM schema_2.table_2;",
                CreateFlowWoutQuery {
                    flow_name: ObjectName::from(vec![Ident::new("task_1")]),
                    sink_table_name: ObjectName::from(vec![
                        Ident::new("schema_1"),
                        Ident::new("table_1"),
                    ]),
                    or_replace: true,
                    if_not_exists: true,
                    expire_after: Some(300),
                    comment: Some("test comment".to_string()),
                },
            ),
            (
                r"
CREATE OR REPLACE FLOW IF NOT EXISTS task_1
SINK TO schema_1.table_1
EXPIRE AFTER '300 s'
COMMENT 'test comment'
AS
SELECT max(c1), min(c2) FROM schema_2.table_2;",
                CreateFlowWoutQuery {
                    flow_name: ObjectName::from(vec![Ident::new("task_1")]),
                    sink_table_name: ObjectName::from(vec![
                        Ident::new("schema_1"),
                        Ident::new("table_1"),
                    ]),
                    or_replace: true,
                    if_not_exists: true,
                    expire_after: Some(300),
                    comment: Some("test comment".to_string()),
                },
            ),
            (
                r"
CREATE FLOW `task_2`
SINK TO schema_1.table_1
EXPIRE AFTER '1 month 2 days 1h 2 min'
AS
SELECT max(c1), min(c2) FROM schema_2.table_2;",
                CreateFlowWoutQuery {
                    flow_name: ObjectName::from(vec![Ident::with_quote('`', "task_2")]),
                    sink_table_name: ObjectName::from(vec![
                        Ident::new("schema_1"),
                        Ident::new("table_1"),
                    ]),
                    or_replace: false,
                    if_not_exists: false,
                    expire_after: Some(86400 * 3044 / 1000 + 2 * 86400 + 3600 + 2 * 60),
                    comment: None,
                },
            ),
        ];

        for (sql, expected) in testcases {
            let create_task = parse_create_flow(sql);

            let expected = CreateFlow {
                flow_name: expected.flow_name,
                sink_table_name: expected.sink_table_name,
                or_replace: expected.or_replace,
                if_not_exists: expected.if_not_exists,
                expire_after: expected.expire_after,
                comment: expected.comment,
                // ignore query parse result
                query: create_task.query.clone(),
            };

            assert_eq!(create_task, expected, "input sql is:\n{sql}");
            let show_create = create_task.to_string();
            let recreated = parse_create_flow(&show_create);
            assert_eq!(recreated, expected, "input sql is:\n{show_create}");
        }
    }

    #[test]
    fn test_parse_create_flow() {
        let sql = r"
CREATE OR REPLACE FLOW IF NOT EXISTS task_1
SINK TO schema_1.table_1
EXPIRE AFTER INTERVAL '5 minutes'
COMMENT 'test comment'
AS
SELECT max(c1), min(c2) FROM schema_2.table_2;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        let create_task = match &stmts[0] {
            Statement::CreateFlow(c) => c,
            _ => unreachable!(),
        };

        let expected = CreateFlow {
            flow_name: vec![Ident::new("task_1")].into(),
            sink_table_name: vec![Ident::new("schema_1"), Ident::new("table_1")].into(),
            or_replace: true,
            if_not_exists: true,
            expire_after: Some(300),
            comment: Some("test comment".to_string()),
            // ignore query parse result
            query: create_task.query.clone(),
        };
        assert_eq!(create_task, &expected);

        // create flow without `OR REPLACE`, `IF NOT EXISTS`, `EXPIRE AFTER` and `COMMENT`
        let sql = r"
CREATE FLOW `task_2`
SINK TO schema_1.table_1
EXPIRE AFTER '1 month 2 days 1h 2 min'
AS
SELECT max(c1), min(c2) FROM schema_2.table_2;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        let create_task = match &stmts[0] {
            Statement::CreateFlow(c) => c,
            _ => unreachable!(),
        };
        assert!(!create_task.or_replace);
        assert!(!create_task.if_not_exists);
        assert_eq!(
            create_task.expire_after,
            Some(86400 * 3044 / 1000 + 2 * 86400 + 3600 + 2 * 60)
        );
        assert!(create_task.comment.is_none());
        assert_eq!(create_task.flow_name.to_string(), "`task_2`");
    }

    #[test]
    fn test_validate_create() {
        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT, ts timestamp TIME INDEX)
PARTITION ON COLUMNS(c, a) (
    a < 10,
    a > 10 AND a < 20,
    a > 20 AND c < 100,
    a > 20 AND c >= 100
)
ENGINE=mito";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let _ = result.unwrap();

        let sql = r"
CREATE TABLE rcx ( ts TIMESTAMP TIME INDEX, a INT, b STRING, c INT )
PARTITION ON COLUMNS(x) ()
ENGINE=mito";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Partition column \"x\" not defined"));
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
PARTITION ON COLUMNS(idc, host_id) (
  idc <= 'hz' AND host_id < 1000,
  idc > 'hz' AND idc <= 'sh' AND host_id < 2000,
  idc > 'sh' AND host_id < 3000,
  idc > 'sh' AND host_id >= 3000,
)
ENGINE=mito";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
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

                let exprs = &partitions.exprs;

                assert_eq!(
                    exprs[0],
                    Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier("idc".into())),
                            op: BinaryOperator::LtEq,
                            right: Box::new(Expr::Value(
                                Value::SingleQuotedString("hz".to_string()).into()
                            ))
                        }),
                        op: BinaryOperator::And,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier("host_id".into())),
                            op: BinaryOperator::Lt,
                            right: Box::new(Expr::Value(
                                Value::Number("1000".to_string(), false).into()
                            ))
                        })
                    }
                );
                assert_eq!(
                    exprs[1],
                    Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::BinaryOp {
                                left: Box::new(Expr::Identifier("idc".into())),
                                op: BinaryOperator::Gt,
                                right: Box::new(Expr::Value(
                                    Value::SingleQuotedString("hz".to_string()).into()
                                ))
                            }),
                            op: BinaryOperator::And,
                            right: Box::new(Expr::BinaryOp {
                                left: Box::new(Expr::Identifier("idc".into())),
                                op: BinaryOperator::LtEq,
                                right: Box::new(Expr::Value(
                                    Value::SingleQuotedString("sh".to_string()).into()
                                ))
                            })
                        }),
                        op: BinaryOperator::And,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier("host_id".into())),
                            op: BinaryOperator::Lt,
                            right: Box::new(Expr::Value(
                                Value::Number("2000".to_string(), false).into()
                            ))
                        })
                    }
                );
                assert_eq!(
                    exprs[2],
                    Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier("idc".into())),
                            op: BinaryOperator::Gt,
                            right: Box::new(Expr::Value(
                                Value::SingleQuotedString("sh".to_string()).into()
                            ))
                        }),
                        op: BinaryOperator::And,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier("host_id".into())),
                            op: BinaryOperator::Lt,
                            right: Box::new(Expr::Value(
                                Value::Number("3000".to_string(), false).into()
                            ))
                        })
                    }
                );
                assert_eq!(
                    exprs[3],
                    Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier("idc".into())),
                            op: BinaryOperator::Gt,
                            right: Box::new(Expr::Value(
                                Value::SingleQuotedString("sh".to_string()).into()
                            ))
                        }),
                        op: BinaryOperator::And,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier("host_id".into())),
                            op: BinaryOperator::GtEq,
                            right: Box::new(Expr::Value(
                                Value::Number("3000".to_string(), false).into()
                            ))
                        })
                    }
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_create_table_with_quoted_partitions() {
        let sql = r"
CREATE TABLE monitor (
  `host_id`    INT,
  idc        STRING,
  ts         TIMESTAMP,
  cpu        DOUBLE DEFAULT 0,
  memory     DOUBLE,
  TIME INDEX (ts),
  PRIMARY KEY (host),
)
PARTITION ON COLUMNS(IdC, host_id) (
  idc <= 'hz' AND host_id < 1000,
  idc > 'hz' AND idc <= 'sh' AND host_id < 2000,
  idc > 'sh' AND host_id < 3000,
  idc > 'sh' AND host_id >= 3000,
)";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(result.len(), 1);
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
        let result1 = ParserContext::create_with_dialect(
            sql1,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap();

        if let Statement::CreateTable(c) = &result1[0] {
            assert_eq!(c.constraints.len(), 2);
            let tc = c.constraints[0].clone();
            match tc {
                TableConstraint::TimeIndex { column } => {
                    assert_eq!(&column.value, "ts");
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
        let result2 = ParserContext::create_with_dialect(
            sql2,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap();

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

        let result3 = ParserContext::create_with_dialect(
            sql3,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap();

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
        let result1 = ParserContext::create_with_dialect(
            sql1,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        );

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
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();

        assert_eq!(result.len(), 1);
        if let Statement::CreateTable(c) = &result[0] {
            let ts = c.columns[2].clone();
            assert_eq!(ts.name().to_string(), "ts");
            assert_eq!(ts.options()[0].option, NotNull);
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

        let result1 = ParserContext::create_with_dialect(
            sql1,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap();
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

        let result2 = ParserContext::create_with_dialect(
            sql2,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap();
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

        let result3 = ParserContext::create_with_dialect(
            sql3,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        );
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

        let result4 = ParserContext::create_with_dialect(
            sql4,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        );
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

        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();

        if let Statement::CreateTable(c) = &result[0] {
            let tc = c.constraints[0].clone();
            match tc {
                TableConstraint::TimeIndex { column } => {
                    assert_eq!(&column.value, "ts");
                }
                _ => panic!("should be time index constraint"),
            }
            let ts = c.columns[2].clone();
            assert_eq!(ts.name().to_string(), "ts");
            assert!(matches!(ts.options()[0].option, ColumnOption::Default(..)));
            assert_eq!(ts.options()[1].option, NotNull);
        } else {
            unreachable!("should be create table statement");
        }
    }

    #[test]
    fn test_parse_partitions_with_error_syntax() {
        let sql = r"
CREATE TABLE rcx ( ts TIMESTAMP TIME INDEX, a INT, b STRING, c INT )
PARTITION COLUMNS(c, a) (
    a < 10,
    a > 10 AND a < 20,
    a > 20 AND c < 100,
    a > 20 AND c >= 100
)
ENGINE=mito";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result
            .unwrap_err()
            .output_msg()
            .contains("sql parser error: Expected: ON, found: COLUMNS"));
    }

    #[test]
    fn test_parse_partitions_without_rule() {
        let sql = r"
CREATE TABLE rcx ( a INT, b STRING, c INT, d TIMESTAMP TIME INDEX )
PARTITION ON COLUMNS(c, a) ()
ENGINE=mito";
        ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
            .unwrap();
    }

    #[test]
    fn test_parse_partitions_unreferenced_column() {
        let sql = r"
CREATE TABLE rcx ( ts TIMESTAMP TIME INDEX, a INT, b STRING, c INT )
PARTITION ON COLUMNS(c, a) (
    b = 'foo'
)
ENGINE=mito";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert_eq!(
            result.unwrap_err().output_msg(),
            "Invalid SQL, error: Column \"b\" in rule expr is not referenced in PARTITION ON"
        );
    }

    #[test]
    fn test_parse_partitions_not_binary_expr() {
        let sql = r"
CREATE TABLE rcx ( ts TIMESTAMP TIME INDEX, a INT, b STRING, c INT )
PARTITION ON COLUMNS(c, a) (
    b
)
ENGINE=mito";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert_eq!(
            result.unwrap_err().output_msg(),
            r#"Invalid SQL, error: Partition rule expr Identifier(Ident { value: "b", quote_style: None, span: Span(Location(4,5)..Location(4,6)) }) is not a binary expr"#
        );
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
                             PRIMARY KEY(ts, host),
                             ) engine=mito
                             with(ttl='10s');
         ";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());
        match &result[0] {
            Statement::CreateTable(c) => {
                assert!(!c.if_not_exists);
                assert_eq!("demo", c.name.to_string());
                assert_eq!("mito", c.engine);
                assert_eq!(4, c.columns.len());
                let columns = &c.columns;
                assert_column_def(&columns[0].column_def, "host", "STRING");
                assert_column_def(&columns[1].column_def, "ts", "TIMESTAMP");
                assert_column_def(&columns[2].column_def, "cpu", "FLOAT");
                assert_column_def(&columns[3].column_def, "memory", "DOUBLE");

                let constraints = &c.constraints;
                assert_eq!(
                    &constraints[0],
                    &TableConstraint::TimeIndex {
                        column: Ident::new("ts"),
                    }
                );
                assert_eq!(
                    &constraints[1],
                    &TableConstraint::PrimaryKey {
                        columns: vec![Ident::new("ts"), Ident::new("host")]
                    }
                );
                // inverted index is merged into column options
                assert_eq!(1, c.options.len());
                assert_eq!(
                    [("ttl", "10s")].into_iter().collect::<HashMap<_, _>>(),
                    c.options.to_str_map()
                );
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
                             PRIMARY KEY(ts, host)) engine=mito;
         ";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
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
                             PRIMARY KEY(ts, host)) engine=mito;
         ";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result.is_err());
        assert_matches!(result, Err(crate::error::Error::InvalidTimeIndex { .. }));

        let sql = r"create table demo(
                             host string,
                             ts timestamp time index,
                             cpu float64 default 0,
                             t timestamp,
                             memory float64,
                             TIME INDEX (t),
                             PRIMARY KEY(ts, host)) engine=mito;
         ";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result.is_err());
        assert_matches!(result, Err(crate::error::Error::InvalidTimeIndex { .. }));
    }

    #[test]
    fn test_invalid_column_name() {
        let sql = "create table foo(user string, i timestamp time index)";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let err = result.unwrap_err().output_msg();
        assert!(err.contains("Cannot use keyword 'user' as column name"));

        // If column name is quoted, it's valid even same with keyword.
        let sql = r#"
            create table foo("user" string, i timestamp time index)
        "#;
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let _ = result.unwrap();
    }

    #[test]
    fn test_incorrect_default_value_issue_3479() {
        let sql = r#"CREATE TABLE `ExcePTuRi`(
non TIMESTAMP(6) TIME INDEX,
`iUSTO` DOUBLE DEFAULT 0.047318541668048164
)"#;
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());
        match &result[0] {
            Statement::CreateTable(c) => {
                assert_eq!(
                    "`iUSTO` DOUBLE DEFAULT 0.047318541668048164",
                    c.columns[1].to_string()
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_create_view() {
        let sql = "CREATE VIEW test AS SELECT * FROM NUMBERS";

        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        match &result[0] {
            Statement::CreateView(c) => {
                assert_eq!(c.to_string(), sql);
                assert!(!c.or_replace);
                assert!(!c.if_not_exists);
                assert_eq!("test", c.name.to_string());
            }
            _ => unreachable!(),
        }

        let sql = "CREATE OR REPLACE VIEW IF NOT EXISTS test AS SELECT * FROM NUMBERS";

        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        match &result[0] {
            Statement::CreateView(c) => {
                assert_eq!(c.to_string(), sql);
                assert!(c.or_replace);
                assert!(c.if_not_exists);
                assert_eq!("test", c.name.to_string());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_create_view_invalid_query() {
        let sql = "CREATE VIEW test AS DELETE from demo";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result.is_err());
        assert_matches!(result, Err(crate::error::Error::Syntax { .. }));
    }

    #[test]
    fn test_parse_create_table_fulltext_options() {
        let sql1 = r"
CREATE TABLE log (
    ts TIMESTAMP TIME INDEX,
    msg TEXT FULLTEXT INDEX,
)";
        let result1 = ParserContext::create_with_dialect(
            sql1,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap();

        if let Statement::CreateTable(c) = &result1[0] {
            c.columns.iter().for_each(|col| {
                if col.name().value == "msg" {
                    assert!(col
                        .extensions
                        .fulltext_index_options
                        .as_ref()
                        .unwrap()
                        .is_empty());
                }
            });
        } else {
            panic!("should be create_table statement");
        }

        let sql2 = r"
CREATE TABLE log (
    ts TIMESTAMP TIME INDEX,
    msg STRING FULLTEXT INDEX WITH (analyzer='English', case_sensitive='false')
)";
        let result2 = ParserContext::create_with_dialect(
            sql2,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap();

        if let Statement::CreateTable(c) = &result2[0] {
            c.columns.iter().for_each(|col| {
                if col.name().value == "msg" {
                    let options = col.extensions.fulltext_index_options.as_ref().unwrap();
                    assert_eq!(options.len(), 2);
                    assert_eq!(options.get("analyzer").unwrap(), "English");
                    assert_eq!(options.get("case_sensitive").unwrap(), "false");
                }
            });
        } else {
            panic!("should be create_table statement");
        }

        let sql3 = r"
CREATE TABLE log (
    ts TIMESTAMP TIME INDEX,
    msg1 TINYTEXT FULLTEXT INDEX WITH (analyzer='English', case_sensitive='false'),
    msg2 CHAR(20) FULLTEXT INDEX WITH (analyzer='Chinese', case_sensitive='true')
)";
        let result3 = ParserContext::create_with_dialect(
            sql3,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap();

        if let Statement::CreateTable(c) = &result3[0] {
            c.columns.iter().for_each(|col| {
                if col.name().value == "msg1" {
                    let options = col.extensions.fulltext_index_options.as_ref().unwrap();
                    assert_eq!(options.len(), 2);
                    assert_eq!(options.get("analyzer").unwrap(), "English");
                    assert_eq!(options.get("case_sensitive").unwrap(), "false");
                } else if col.name().value == "msg2" {
                    let options = col.extensions.fulltext_index_options.as_ref().unwrap();
                    assert_eq!(options.len(), 2);
                    assert_eq!(options.get("analyzer").unwrap(), "Chinese");
                    assert_eq!(options.get("case_sensitive").unwrap(), "true");
                }
            });
        } else {
            panic!("should be create_table statement");
        }
    }

    #[test]
    fn test_parse_create_table_fulltext_options_invalid_type() {
        let sql = r"
CREATE TABLE log (
    ts TIMESTAMP TIME INDEX,
    msg INT FULLTEXT INDEX,
)";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("FULLTEXT index only supports string type"));
    }

    #[test]
    fn test_parse_create_table_fulltext_options_duplicate() {
        let sql = r"
CREATE TABLE log (
    ts TIMESTAMP TIME INDEX,
    msg STRING FULLTEXT INDEX WITH (analyzer='English', analyzer='Chinese') FULLTEXT INDEX WITH (case_sensitive='false')
)";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("duplicated FULLTEXT INDEX option"));
    }

    #[test]
    fn test_parse_create_table_fulltext_options_invalid_option() {
        let sql = r"
CREATE TABLE log (
    ts TIMESTAMP TIME INDEX,
    msg STRING FULLTEXT INDEX WITH (analyzer='English', invalid_option='Chinese')
)";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid FULLTEXT INDEX option"));
    }

    #[test]
    fn test_parse_create_table_skip_options() {
        let sql = r"
CREATE TABLE log (
    ts TIMESTAMP TIME INDEX,
    msg INT SKIPPING INDEX WITH (granularity='8192', type='bloom'),
)";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();

        if let Statement::CreateTable(c) = &result[0] {
            c.columns.iter().for_each(|col| {
                if col.name().value == "msg" {
                    assert!(!col
                        .extensions
                        .skipping_index_options
                        .as_ref()
                        .unwrap()
                        .is_empty());
                }
            });
        } else {
            panic!("should be create_table statement");
        }

        let sql = r"
        CREATE TABLE log (
            ts TIMESTAMP TIME INDEX,
            msg INT SKIPPING INDEX,
        )";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();

        if let Statement::CreateTable(c) = &result[0] {
            c.columns.iter().for_each(|col| {
                if col.name().value == "msg" {
                    assert!(col
                        .extensions
                        .skipping_index_options
                        .as_ref()
                        .unwrap()
                        .is_empty());
                }
            });
        } else {
            panic!("should be create_table statement");
        }
    }

    #[test]
    fn test_parse_create_view_with_columns() {
        let sql = "CREATE VIEW test () AS SELECT * FROM NUMBERS";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();

        match &result[0] {
            Statement::CreateView(c) => {
                assert_eq!(c.to_string(), "CREATE VIEW test AS SELECT * FROM NUMBERS");
                assert!(!c.or_replace);
                assert!(!c.if_not_exists);
                assert_eq!("test", c.name.to_string());
            }
            _ => unreachable!(),
        }
        assert_eq!(
            "CREATE VIEW test AS SELECT * FROM NUMBERS",
            result[0].to_string()
        );

        let sql = "CREATE VIEW test (n1) AS SELECT * FROM NUMBERS";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();

        match &result[0] {
            Statement::CreateView(c) => {
                assert_eq!(c.to_string(), sql);
                assert!(!c.or_replace);
                assert!(!c.if_not_exists);
                assert_eq!("test", c.name.to_string());
            }
            _ => unreachable!(),
        }
        assert_eq!(sql, result[0].to_string());

        let sql = "CREATE VIEW test (n1, n2) AS SELECT * FROM NUMBERS";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();

        match &result[0] {
            Statement::CreateView(c) => {
                assert_eq!(c.to_string(), sql);
                assert!(!c.or_replace);
                assert!(!c.if_not_exists);
                assert_eq!("test", c.name.to_string());
            }
            _ => unreachable!(),
        }
        assert_eq!(sql, result[0].to_string());

        // Some invalid syntax cases
        let sql = "CREATE VIEW test (n1 AS select * from demo";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result.is_err());

        let sql = "CREATE VIEW test (n1, AS select * from demo";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result.is_err());

        let sql = "CREATE VIEW test n1,n2) AS select * from demo";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result.is_err());

        let sql = "CREATE VIEW test (1) AS select * from demo";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result.is_err());

        // keyword
        let sql = "CREATE VIEW test (n1, select) AS select * from demo";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_column_extensions_vector() {
        let sql = "VECTOR(128)";
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, sql);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(&dialect).with_tokens(tokens);
        let name = Ident::new("vec_col");
        let data_type =
            DataType::Custom(vec![Ident::new("VECTOR")].into(), vec!["128".to_string()]);
        let mut extensions = ColumnExtensions::default();

        let result =
            ParserContext::parse_column_extensions(&mut parser, &name, &data_type, &mut extensions);
        assert!(result.is_ok());
        assert!(extensions.vector_options.is_some());
        let vector_options = extensions.vector_options.unwrap();
        assert_eq!(vector_options.get(VECTOR_OPT_DIM), Some(&"128".to_string()));
    }

    #[test]
    fn test_parse_column_extensions_vector_invalid() {
        let sql = "VECTOR()";
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, sql);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(&dialect).with_tokens(tokens);
        let name = Ident::new("vec_col");
        let data_type = DataType::Custom(vec![Ident::new("VECTOR")].into(), vec![]);
        let mut extensions = ColumnExtensions::default();

        let result =
            ParserContext::parse_column_extensions(&mut parser, &name, &data_type, &mut extensions);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_column_extensions_indices() {
        // Test skipping index
        {
            let sql = "SKIPPING INDEX";
            let dialect = GenericDialect {};
            let mut tokenizer = Tokenizer::new(&dialect, sql);
            let tokens = tokenizer.tokenize().unwrap();
            let mut parser = Parser::new(&dialect).with_tokens(tokens);
            let name = Ident::new("col");
            let data_type = DataType::String(None);
            let mut extensions = ColumnExtensions::default();
            let result = ParserContext::parse_column_extensions(
                &mut parser,
                &name,
                &data_type,
                &mut extensions,
            );
            assert!(result.is_ok());
            assert!(extensions.skipping_index_options.is_some());
        }

        // Test fulltext index with options
        {
            let sql = "FULLTEXT INDEX WITH (analyzer = 'English', case_sensitive = 'true')";
            let dialect = GenericDialect {};
            let mut tokenizer = Tokenizer::new(&dialect, sql);
            let tokens = tokenizer.tokenize().unwrap();
            let mut parser = Parser::new(&dialect).with_tokens(tokens);
            let name = Ident::new("text_col");
            let data_type = DataType::String(None);
            let mut extensions = ColumnExtensions::default();
            let result = ParserContext::parse_column_extensions(
                &mut parser,
                &name,
                &data_type,
                &mut extensions,
            );
            assert!(result.unwrap());
            assert!(extensions.fulltext_index_options.is_some());
            let fulltext_options = extensions.fulltext_index_options.unwrap();
            assert_eq!(
                fulltext_options.get("analyzer"),
                Some(&"English".to_string())
            );
            assert_eq!(
                fulltext_options.get("case_sensitive"),
                Some(&"true".to_string())
            );
        }

        // Test fulltext index with invalid type (should fail)
        {
            let sql = "FULLTEXT INDEX WITH (analyzer = 'English')";
            let dialect = GenericDialect {};
            let mut tokenizer = Tokenizer::new(&dialect, sql);
            let tokens = tokenizer.tokenize().unwrap();
            let mut parser = Parser::new(&dialect).with_tokens(tokens);
            let name = Ident::new("num_col");
            let data_type = DataType::Int(None); // Non-string type
            let mut extensions = ColumnExtensions::default();
            let result = ParserContext::parse_column_extensions(
                &mut parser,
                &name,
                &data_type,
                &mut extensions,
            );
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FULLTEXT index only supports string type"));
        }

        // Test fulltext index with invalid option (won't fail, the parser doesn't check the option's content)
        {
            let sql = "FULLTEXT INDEX WITH (analyzer = 'Invalid', case_sensitive = 'true')";
            let dialect = GenericDialect {};
            let mut tokenizer = Tokenizer::new(&dialect, sql);
            let tokens = tokenizer.tokenize().unwrap();
            let mut parser = Parser::new(&dialect).with_tokens(tokens);
            let name = Ident::new("text_col");
            let data_type = DataType::String(None);
            let mut extensions = ColumnExtensions::default();
            let result = ParserContext::parse_column_extensions(
                &mut parser,
                &name,
                &data_type,
                &mut extensions,
            );
            assert!(result.unwrap());
        }

        // Test inverted index
        {
            let sql = "INVERTED INDEX";
            let dialect = GenericDialect {};
            let mut tokenizer = Tokenizer::new(&dialect, sql);
            let tokens = tokenizer.tokenize().unwrap();
            let mut parser = Parser::new(&dialect).with_tokens(tokens);
            let name = Ident::new("col");
            let data_type = DataType::String(None);
            let mut extensions = ColumnExtensions::default();
            let result = ParserContext::parse_column_extensions(
                &mut parser,
                &name,
                &data_type,
                &mut extensions,
            );
            assert!(result.is_ok());
            assert!(extensions.inverted_index_options.is_some());
        }

        // Test inverted index with options (should fail)
        {
            let sql = "INVERTED INDEX WITH (analyzer = 'English')";
            let dialect = GenericDialect {};
            let mut tokenizer = Tokenizer::new(&dialect, sql);
            let tokens = tokenizer.tokenize().unwrap();
            let mut parser = Parser::new(&dialect).with_tokens(tokens);
            let name = Ident::new("col");
            let data_type = DataType::String(None);
            let mut extensions = ColumnExtensions::default();
            let result = ParserContext::parse_column_extensions(
                &mut parser,
                &name,
                &data_type,
                &mut extensions,
            );
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("INVERTED index doesn't support options"));
        }

        // Test multiple indices
        {
            let sql = "SKIPPING INDEX FULLTEXT INDEX";
            let dialect = GenericDialect {};
            let mut tokenizer = Tokenizer::new(&dialect, sql);
            let tokens = tokenizer.tokenize().unwrap();
            let mut parser = Parser::new(&dialect).with_tokens(tokens);
            let name = Ident::new("col");
            let data_type = DataType::String(None);
            let mut extensions = ColumnExtensions::default();
            let result = ParserContext::parse_column_extensions(
                &mut parser,
                &name,
                &data_type,
                &mut extensions,
            );
            assert!(result.unwrap());
            assert!(extensions.skipping_index_options.is_some());
            assert!(extensions.fulltext_index_options.is_some());
        }
    }

    #[test]
    fn test_parse_interval_cast() {
        let s = "select '10s'::INTERVAL";
        let stmts =
            ParserContext::create_with_dialect(s, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!("SELECT '10 seconds'::INTERVAL", &stmts[0].to_string());
    }
}
