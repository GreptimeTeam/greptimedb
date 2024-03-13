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

use std::sync::Arc;

use chrono::Utc;
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DFSchema, Result as DFResult, ScalarValue, TableReference};
use datafusion_expr::{AggregateUDF, Expr, ScalarUDF, TableSource, WindowUDF};
use datafusion_physical_expr::execution_props::ExecutionProps;
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use snafu::ResultExt;
use sqlparser::keywords::Keyword;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::statement::Statement;
use crate::statements::tql::{Tql, TqlAnalyze, TqlEval, TqlExplain};

pub const TQL: &str = "TQL";
const EVAL: &str = "EVAL";
const EVALUATE: &str = "EVALUATE";
const EXPLAIN: &str = "EXPLAIN";
const VERBOSE: &str = "VERBOSE";

use datatypes::arrow::datatypes::DataType;
use sqlparser::parser::Parser;

use crate::dialect::GreptimeDbDialect;

/// TQL extension parser, including:
/// - `TQL EVAL <query>`
/// - `TQL EXPLAIN [VERBOSE] <query>`
/// - `TQL ANALYZE [VERBOSE] <query>`
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_tql(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();

        match self.parser.peek_token().token {
            Token::Word(w) => {
                let uppercase = w.value.to_uppercase();
                match w.keyword {
                    Keyword::NoKeyword
                        if (uppercase == EVAL || uppercase == EVALUATE)
                            && w.quote_style.is_none() =>
                    {
                        let _ = self.parser.next_token();
                        self.parse_tql_eval().context(error::SyntaxSnafu)
                    }

                    Keyword::EXPLAIN => {
                        let _ = self.parser.next_token();
                        self.parse_tql_explain()
                    }

                    Keyword::ANALYZE => {
                        let _ = self.parser.next_token();
                        self.parse_tql_analyze().context(error::SyntaxSnafu)
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
        let start = Self::parse_string_or_number_or_word(parser, Token::Comma)?;
        let end = Self::parse_string_or_number_or_word(parser, Token::Comma)?;
        let step = Self::parse_string_or_number_or_word(parser, Token::RParen)?;
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
        let value = match parser.next_token().token {
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

    fn parse_string_or_number_or_word(
        parser: &mut Parser,
        token: Token,
    ) -> std::result::Result<String, ParserError> {
        let value = match parser.next_token().token {
            Token::Number(n, _) => n,
            Token::DoubleQuotedString(s) | Token::SingleQuotedString(s) => s,
            Token::Word(w) => Self::parse_tql_word(Token::Word(w), parser).unwrap(),
            unexpected => {
                return Err(ParserError::ParserError(format!(
                    "Expect number or string TODO adjust message, but is {unexpected:?}"
                )));
            }
        };
        parser.expect_token(&token)?;
        Ok(value)
    }

    fn parse_tql_word(w: Token, parser: &mut Parser) -> std::result::Result<String, ParserError> {
        let tokens = Self::collect_tokens(w, parser);
        let empty_df_schema = DFSchema::empty();

        let expr = Parser::new(&GreptimeDbDialect {})
            .with_tokens(tokens)
            .parse_expr()
            .map_err(|err| {
                ParserError::ParserError(format!("Expect number or string, but is {err:?}"))
            })?;

        let sql_to_rel = SqlToRel::new(&DummyContextProvider {});
        let logical_expr = sql_to_rel
            .sql_to_expr(expr.into(), &empty_df_schema, &mut Default::default())
            .unwrap();

        let execution_props = ExecutionProps::new().with_query_execution_start_time(Utc::now());
        let info = SimplifyContext::new(&execution_props).with_schema(Arc::new(empty_df_schema));
        let simplified_expr = ExprSimplifier::new(info).simplify(logical_expr).unwrap();

        let timestamp_nanos = match simplified_expr {
            Expr::Literal(ScalarValue::TimestampNanosecond(Some(v), _)) => v,
            _ => panic!(),
        };

        Ok((timestamp_nanos / 1_000_000_000).to_string())
    }

    fn collect_tokens(w: Token, parser: &mut Parser) -> Vec<Token> {
        let mut tokens = vec![];
        tokens.push(w);
        while parser.peek_token() != Token::Comma {
            let token = parser.next_token();
            tokens.push(token.token);
        }
        tokens
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

            let query = if delimiter == ")" {
                match Self::find_last_balanced_bracket(sql) {
                    Some(to) => &sql[(to + 1)..],
                    None => &sql[index..],
                }
            } else {
                &sql[index..]
            };

            while parser.next_token() != Token::EOF {
                // consume all tokens
                // TODO(dennis): supports multi TQL statements separated by ';'?
            }

            // remove the last ';' or tailing space if exists
            Ok(query.trim().trim_end_matches(';').to_string())
        } else {
            Err(ParserError::ParserError(format!("{delimiter} not found",)))
        }
    }

    fn find_last_balanced_bracket(sql: &str) -> Option<usize> {
        let mut balance = 0;
        for (index, c) in sql.char_indices() {
            match c {
                '(' => balance += 1,
                ')' => {
                    balance -= 1;
                    if balance == 0 {
                        return Some(index);
                    }
                }
                _ => {}
            }
        }
        None
    }

    fn parse_tql_explain(&mut self) -> Result<Statement> {
        let parser = &mut self.parser;
        let is_verbose = if parser.peek_token().token.to_string() == VERBOSE {
            let _ = parser.next_token();
            true
        } else {
            false
        };
        let delimiter = match parser.expect_token(&Token::LParen) {
            Ok(_) => ")",
            Err(_) => {
                if is_verbose {
                    VERBOSE
                } else {
                    EXPLAIN
                }
            }
        };
        let start = Self::parse_string_or_number(parser, Token::Comma).unwrap_or("0".to_string());
        let end = Self::parse_string_or_number(parser, Token::Comma).unwrap_or("0".to_string());
        let step = Self::parse_string_or_number(parser, Token::RParen).unwrap_or("5m".to_string());
        let query =
            Self::parse_tql_query(parser, self.sql, delimiter).context(error::SyntaxSnafu)?;

        Ok(Statement::Tql(Tql::Explain(TqlExplain {
            query,
            start,
            end,
            step,
            is_verbose,
        })))
    }

    fn parse_tql_analyze(&mut self) -> std::result::Result<Statement, ParserError> {
        let parser = &mut self.parser;
        let is_verbose = if parser.peek_token().token.to_string() == VERBOSE {
            let _ = parser.next_token();
            true
        } else {
            false
        };

        parser.expect_token(&Token::LParen)?;
        let start = Self::parse_string_or_number(parser, Token::Comma)?;
        let end = Self::parse_string_or_number(parser, Token::Comma)?;
        let step = Self::parse_string_or_number(parser, Token::RParen)?;
        let query = Self::parse_tql_query(parser, self.sql, ")")?;
        Ok(Statement::Tql(Tql::Analyze(TqlAnalyze {
            start,
            end,
            step,
            query,
            is_verbose,
        })))
    }
}

#[derive(Default)]
struct DummyContextProvider {}

impl ContextProvider for DummyContextProvider {
    fn get_table_provider(&self, _name: TableReference) -> DFResult<Arc<dyn TableSource>> {
        unimplemented!()
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        unimplemented!()
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        unimplemented!()
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        unimplemented!()
    }

    fn options(&self) -> &ConfigOptions {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use common_error::ext::ErrorExt;

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParseOptions;

    #[test]
    fn test_parse_tql_eval_with_functions() {
        let sql = "TQL EVAL (now() - '10 minutes'::interval, now(), '1m') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        let assert_time = Utc::now()
            .timestamp_nanos_opt()
            .map(|x| x / 1_000_000_000)
            .unwrap_or(0);

        let statement = result.remove(0);

        match statement {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, (assert_time - 600).to_string());
                assert_eq!(eval.end, assert_time.to_string());
                assert_eq!(eval.step, "1m");
                assert_eq!(eval.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_tql_eval() {
        let sql = "TQL EVAL (1676887657, 1676887659, '1m') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";

        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
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

        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
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

        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        let statement2 = result.remove(0);
        assert_eq!(statement, statement2);

        let sql = "tql eval ('2015-07-01T20:10:30.781Z', '2015-07-01T20:11:00.781Z', '30s') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";

        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
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
    }

    #[test]
    fn test_parse_tql_explain() {
        let sql = "TQL EXPLAIN http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";

        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        match statement {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert_eq!(explain.start, "0");
                assert_eq!(explain.end, "0");
                assert_eq!(explain.step, "5m");
                assert!(!explain.is_verbose);
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN VERBOSE http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";

        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        match statement {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert_eq!(explain.start, "0");
                assert_eq!(explain.end, "0");
                assert_eq!(explain.step, "5m");
                assert!(explain.is_verbose);
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN (20,100,10) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";

        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        match statement {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert_eq!(explain.start, "20");
                assert_eq!(explain.end, "100");
                assert_eq!(explain.step, "10");
                assert!(!explain.is_verbose);
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN VERBOSE (20,100,10) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";

        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        match statement {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert_eq!(explain.start, "20");
                assert_eq!(explain.end, "100");
                assert_eq!(explain.step, "10");
                assert!(explain.is_verbose);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_tql_analyze() {
        let sql = "TQL ANALYZE (1676887657.1, 1676887659.5, 30.3) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());
        let statement = result.remove(0);
        match statement {
            Statement::Tql(Tql::Analyze(analyze)) => {
                assert_eq!(analyze.start, "1676887657.1");
                assert_eq!(analyze.end, "1676887659.5");
                assert_eq!(analyze.step, "30.3");
                assert_eq!(analyze.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert!(!analyze.is_verbose);
            }
            _ => unreachable!(),
        }

        let sql = "TQL ANALYZE VERBOSE (1676887657.1, 1676887659.5, 30.3) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());
        let statement = result.remove(0);
        match statement {
            Statement::Tql(Tql::Analyze(analyze)) => {
                assert_eq!(analyze.start, "1676887657.1");
                assert_eq!(analyze.end, "1676887659.5");
                assert_eq!(analyze.step, "30.3");
                assert_eq!(analyze.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert!(analyze.is_verbose);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_tql_error() {
        // Invalid duration
        let sql = "TQL EVAL (1676887657, 1676887659, 1m) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap_err();
        assert!(result.output_msg().contains("Expected ), found: m"));

        // missing end
        let sql = "TQL EVAL (1676887657, '1m') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap_err();
        assert!(result.output_msg().contains("Expected ,, found: )"));
    }
}
