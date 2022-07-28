use snafu::ensure;
use snafu::ResultExt;
use sqlparser::parser::IsOptional::Mandatory;
use sqlparser::{dialect::keywords::Keyword, tokenizer::Token};
use table_engine::engine;

use crate::ast::{ColumnDef, Ident, TableConstraint};
use crate::error;
use crate::error::{InvalidTimeIndexKeysSnafu, SyntaxSnafu};
use crate::parser::ParserContext;
use crate::parser::Result;
use crate::statements::create_table::CreateTable;
use crate::statements::statement::Statement;

const ENGINE: &str = "ENGINE";
const TS_INDEX: &str = "ts_index";

/// Pasre create [table] statement
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_create(&mut self) -> Result<Statement> {
        self.parser
            .expect_keyword(Keyword::TABLE)
            .context(error::SyntaxSnafu { sql: self.sql })?;
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let table_name = self
            .parser
            .parse_object_name()
            .context(error::SyntaxSnafu { sql: self.sql })?;
        let (columns, constraints) = self.parse_columns()?;
        let engine = self.parse_table_engine()?;
        let options = self
            .parser
            .parse_options(Keyword::WITH)
            .context(error::SyntaxSnafu { sql: self.sql })?;

        Ok(Statement::Create(CreateTable {
            if_not_exists,
            name: table_name,
            columns,
            engine,
            constraints,
            options,
        }))
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
            } else if let Token::Word(_) = self.parser.peek_token() {
                columns.push(
                    self.parser
                        .parse_column_def()
                        .context(SyntaxSnafu { sql: self.sql })?,
                );
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

    // Copy from sqlparser by boyan
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
            Token::Word(w) if w.keyword == Keyword::PRIMARY => {
                self.parser
                    .expect_keyword(Keyword::KEY)
                    .context(error::SyntaxSnafu { sql: self.sql })?;
                let columns = self
                    .parser
                    .parse_parenthesized_column_list(Mandatory)
                    .context(error::SyntaxSnafu { sql: self.sql })?;
                Ok(Some(TableConstraint::Unique {
                    name,
                    columns,
                    is_primary: true,
                }))
            }
            Token::Word(w) if w.keyword == Keyword::TIME => {
                self.parser
                    .expect_keyword(Keyword::INDEX)
                    .context(error::SyntaxSnafu { sql: self.sql })?;
                let columns = self
                    .parser
                    .parse_parenthesized_column_list(Mandatory)
                    .context(error::SyntaxSnafu { sql: self.sql })?;

                ensure!(
                    columns.len() == 1,
                    InvalidTimeIndexKeysSnafu { sql: self.sql }
                );

                // TODO(dennis): TableConstraint doesn't support dialect right now,
                // so we use unique constraint with special key to represent TIME INDEX.
                Ok(Some(TableConstraint::Unique {
                    name: Some(Ident {
                        value: TS_INDEX.to_owned(),
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
    fn parse_table_engine(&mut self) -> Result<String> {
        if !self.consume_token(ENGINE) {
            return Ok(engine::DEFAULT_ENGINE.to_string());
        }

        self.parser
            .expect_token(&Token::Eq)
            .context(error::SyntaxSnafu { sql: self.sql })?;

        match self.parser.next_token() {
            Token::Word(w) => Ok(w.value),
            unexpected => self.expected("Engine is missing", unexpected),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use sqlparser::dialect::GenericDialect;

    use super::*;

    fn assert_column_def(column: &ColumnDef, name: &str, data_type: &str) {
        assert_eq!(column.name.to_string(), name);
        assert_eq!(column.data_type.to_string(), data_type);
    }

    #[test]
    pub fn test_parse_create_table() {
        let sql = r"create table demo(
                             host string,
                             ts int64,
                             cpu float64 default 0,
                             memory float64,
                             TIME INDEX (ts),
                             PRIMARY KEY(ts, host)) engine=mito
                             with(regions=1);
         ";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, result.len());
        match &result[0] {
            Statement::Create(c) => {
                assert!(!c.if_not_exists);
                assert_eq!("demo", c.name.to_string());
                assert_eq!("mito", c.engine);
                assert_eq!(4, c.columns.len());
                let columns = &c.columns;
                assert_column_def(&columns[0], "host", "STRING");
                assert_column_def(&columns[1], "ts", "int64");
                assert_column_def(&columns[2], "cpu", "float64");
                assert_column_def(&columns[3], "memory", "float64");
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
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        assert!(result.is_err());
        assert_matches!(
            result,
            Err(crate::error::Error::InvalidTimeIndexKeys { .. })
        );
    }
}
