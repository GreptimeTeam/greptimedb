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

use snafu::ResultExt;
use sqlparser::keywords::Keyword;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::statement::Statement;
use crate::statements::tql::{Tql, TqlEval, TqlExplain};

pub const TQL: &str = "TQL";
const EVAL: &str = "EVAL";
const EVALUATE: &str = "EVALUATE";
const EXPLAIN: &str = "EXPLAIN";
use sqlparser::parser::Parser;

/// TQL extension parser, including:
/// - TQL EVAL <query>
/// - TQL EXPLAIN <query>
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_tql(&mut self) -> Result<Statement> {
        self.parser.next_token();

        match self.parser.peek_token() {
            Token::Word(w) => {
                let uppercase = w.value.to_uppercase();
                match w.keyword {
                    Keyword::NoKeyword
                        if (uppercase == EVAL || uppercase == EVALUATE)
                            && w.quote_style.is_none() =>
                    {
                        self.parser.next_token();
                        self.parse_tql_eval()
                            .context(error::SyntaxSnafu { sql: self.sql })
                    }

                    Keyword::EXPLAIN => {
                        self.parser.next_token();
                        self.parse_tql_explain()
                    }

                    _ => self.unsupported(self.peek_token_as_string()),
                }
            }
            unexpected => self.unsupported(unexpected.to_string()),
        }
    }

    fn parse_tql_eval(&mut self) -> std::result::Result<Statement, ParserError> {
        let parser = &mut self.parser;
        parser.expect_token(&Token::LParen)?;
        let start = Self::parse_string_or_number(parser, Token::Comma)?;
        let end = Self::parse_string_or_number(parser, Token::Comma)?;
        let step = Self::parse_string_or_number(parser, Token::RParen)?;
        let query = Self::parse_tql_query(parser, self.sql, ")")?;

        Ok(Statement::Tql(Tql::Eval(TqlEval {
            start,
            end,
            step,
            query,
        })))
    }

    fn parse_string_or_number(
        parser: &mut Parser,
        token: Token,
    ) -> std::result::Result<String, ParserError> {
        let value = match parser.next_token() {
            Token::Number(n, _) => n,
            Token::DoubleQuotedString(s) | Token::SingleQuotedString(s) => s,
            unexpected => {
                return Err(ParserError::ParserError(format!(
                    "Expect number or string, but is {unexpected:?}"
                )));
            }
        };
        parser.expect_token(&token)?;

        Ok(value)
    }

    fn parse_tql_query(
        parser: &mut Parser,
        sql: &str,
        delimiter: &str,
    ) -> std::result::Result<String, ParserError> {
        let index = sql.to_uppercase().find(delimiter);

        if let Some(index) = index {
            let index = index + delimiter.len() + 1;
            if index >= sql.len() {
                return Err(ParserError::ParserError("empty TQL query".to_string()));
            }

            let query = &sql[index..];

            while parser.next_token() != Token::EOF {
                // consume all tokens
                // TODO(dennis): supports multi TQL statements separated by ';'?
            }

            Ok(query.trim().to_string())
        } else {
            Err(ParserError::ParserError(format!("{delimiter} not found",)))
        }
    }

    fn parse_tql_explain(&mut self) -> Result<Statement> {
        let query = Self::parse_tql_query(&mut self.parser, self.sql, EXPLAIN)
            .context(error::SyntaxSnafu { sql: self.sql })?;

        Ok(Statement::Tql(Tql::Explain(TqlExplain { query })))
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;

    use super::*;
    #[test]
    fn test_parse_tql() {
        let sql = "TQL EVAL (1676887657, 1676887659, '1m') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";

        let mut result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        match statement {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "1676887657");
                assert_eq!(eval.end, "1676887659");
                assert_eq!(eval.step, "1m");
                assert_eq!(eval.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
            }
            _ => unreachable!(),
        }

        let sql = "TQL EVAL (1676887657.1, 1676887659.5, 30.3) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";

        let mut result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        match statement.clone() {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "1676887657.1");
                assert_eq!(eval.end, "1676887659.5");
                assert_eq!(eval.step, "30.3");
                assert_eq!(eval.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
            }
            _ => unreachable!(),
        }

        let sql = "TQL EVALUATE (1676887657.1, 1676887659.5, 30.3) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";

        let mut result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, result.len());

        let statement2 = result.remove(0);
        assert_eq!(statement, statement2);

        let sql = "tql eval ('2015-07-01T20:10:30.781Z', '2015-07-01T20:11:00.781Z', '30s') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";

        let mut result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        match statement {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "2015-07-01T20:10:30.781Z");
                assert_eq!(eval.end, "2015-07-01T20:11:00.781Z");
                assert_eq!(eval.step, "30s");
                assert_eq!(eval.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";

        let mut result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        match statement {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_tql_error() {
        // Invalid duration
        let sql = "TQL EVAL (1676887657, 1676887659, 1m) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap_err();
        assert!(result.to_string().contains("Expected ), found: m"));

        // missing end
        let sql = "TQL EVAL (1676887657, '1m') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap_err();
        assert!(result.to_string().contains("Expected ,, found: )"));
    }
}
