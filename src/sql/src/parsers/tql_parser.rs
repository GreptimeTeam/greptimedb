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
use datafusion::execution::context::SessionState;
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DFSchema, Result as DFResult, ScalarValue, TableReference};
use datafusion_expr::{AggregateUDF, Expr, ScalarUDF, TableSource, WindowUDF};
use datafusion_physical_expr::execution_props::ExecutionProps;
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use snafu::{OptionExt, ResultExt};
use sqlparser::keywords::Keyword;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::statement::Statement;
use crate::statements::tql::{Tql, TqlAnalyze, TqlEval, TqlExplain, TqlParameters};

pub const TQL: &str = "TQL";
const EVAL: &str = "EVAL";
const EVALUATE: &str = "EVALUATE";
const VERBOSE: &str = "VERBOSE";

use datatypes::arrow::datatypes::DataType;
use sqlparser::parser::Parser;

use crate::dialect::GreptimeDbDialect;
use crate::parsers::error::{
    ConvertToLogicalExpressionSnafu, EvaluationSnafu, ParserSnafu, SimplificationSnafu, TQLError,
};

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
                let _consume_tql_keyword_token = self.parser.next_token();
                match w.keyword {
                    Keyword::NoKeyword
                        if (uppercase == EVAL || uppercase == EVALUATE)
                            && w.quote_style.is_none() =>
                    {
                        self.parse_tql_params()
                            .map(|params| Statement::Tql(Tql::Eval(TqlEval::from(params))))
                            .context(error::TQLSyntaxSnafu)
                    }

                    Keyword::EXPLAIN => {
                        let is_verbose = self.has_verbose_keyword();
                        if is_verbose {
                            let _consume_verbose_token = self.parser.next_token();
                        }
                        self.parse_tql_params()
                            .map(|mut params| {
                                params.is_verbose = is_verbose;
                                Statement::Tql(Tql::Explain(TqlExplain::from(params)))
                            })
                            .context(error::TQLSyntaxSnafu)
                    }

                    Keyword::ANALYZE => {
                        let is_verbose = self.has_verbose_keyword();
                        if is_verbose {
                            let _consume_verbose_token = self.parser.next_token();
                        }
                        self.parse_tql_params()
                            .map(|mut params| {
                                params.is_verbose = is_verbose;
                                Statement::Tql(Tql::Analyze(TqlAnalyze::from(params)))
                            })
                            .context(error::TQLSyntaxSnafu)
                    }
                    _ => self.unsupported(self.peek_token_as_string()),
                }
            }
            unexpected => self.unsupported(unexpected.to_string()),
        }
    }

    fn parse_tql_params(&mut self) -> std::result::Result<TqlParameters, TQLError> {
        let parser = &mut self.parser;
        let (start, end, step, lookback) = match parser.peek_token().token {
            Token::LParen => {
                let _consume_lparen_token = parser.next_token();
                let start = Self::parse_string_or_number_or_word(parser, Token::Comma)?;
                let end = Self::parse_string_or_number_or_word(parser, Token::Comma)?;
                let delimiter_token = Self::find_next_delimiter_token(parser);
                let (step, lookback) = if Self::is_comma(&delimiter_token) {
                    let step = Self::parse_string_or_number_or_word(parser, Token::Comma)?;
                    let lookback = Self::parse_string_or_number_or_word(parser, Token::RParen).ok();
                    (step, lookback)
                } else {
                    let step = Self::parse_string_or_number_or_word(parser, Token::RParen)?;
                    (step, None)
                };
                (start, end, step, lookback)
            }
            _ => ("0".to_string(), "0".to_string(), "5m".to_string(), None),
        };
        let query = Self::parse_tql_query(parser, self.sql).context(ParserSnafu)?;
        Ok(TqlParameters::new(start, end, step, lookback, query))
    }

    fn find_next_delimiter_token(parser: &mut Parser) -> Token {
        let mut n: usize = 0;
        while !(Self::is_comma(&parser.peek_nth_token(n).token)
            || Self::is_rparen(&parser.peek_nth_token(n).token))
        {
            n += 1;
        }
        parser.peek_nth_token(n).token
    }

    pub fn is_delimiter_token(token: &Token, delimiter_token: &Token) -> bool {
        match token {
            Token::Comma => Self::is_comma(delimiter_token),
            Token::RParen => Self::is_rparen(delimiter_token),
            _ => false,
        }
    }

    #[inline]
    fn is_comma(token: &Token) -> bool {
        matches!(token, Token::Comma)
    }

    #[inline]
    fn is_rparen(token: &Token) -> bool {
        matches!(token, Token::RParen)
    }

    fn has_verbose_keyword(&mut self) -> bool {
        self.peek_token_as_string().eq_ignore_ascii_case(VERBOSE)
    }

    fn parse_string_or_number_or_word(
        parser: &mut Parser,
        delimiter_token: Token,
    ) -> std::result::Result<String, TQLError> {
        let mut tokens = vec![];

        while !Self::is_delimiter_token(&parser.peek_token().token, &delimiter_token) {
            let token = parser.next_token();
            tokens.push(token.token);
        }
        let result = match tokens.len() {
            0 => Err(ParserError::ParserError(
                "Expected at least one token".to_string(),
            ))
            .context(ParserSnafu),
            1 => {
                let value = match tokens[0].clone() {
                    Token::Number(n, _) => n,
                    Token::DoubleQuotedString(s) | Token::SingleQuotedString(s) => s,
                    Token::Word(_) => Self::parse_tokens(tokens)?,
                    unexpected => {
                        return Err(ParserError::ParserError(format!(
                            "Expected number, string or word, but have {unexpected:?}"
                        )))
                        .context(ParserSnafu);
                    }
                };
                Ok(value)
            }
            _ => Self::parse_tokens(tokens),
        };
        parser.expect_token(&delimiter_token).context(ParserSnafu)?;
        result
    }

    fn parse_tokens(tokens: Vec<Token>) -> std::result::Result<String, TQLError> {
        Self::parse_to_expr(tokens)
            .and_then(Self::parse_to_logical_expr)
            .and_then(Self::simplify_expr)
            .and_then(Self::evaluate_expr)
    }

    fn parse_to_expr(tokens: Vec<Token>) -> std::result::Result<sqlparser::ast::Expr, TQLError> {
        Parser::new(&GreptimeDbDialect {})
            .with_tokens(tokens)
            .parse_expr()
            .context(ParserSnafu)
    }

    fn parse_to_logical_expr(expr: sqlparser::ast::Expr) -> std::result::Result<Expr, TQLError> {
        let empty_df_schema = DFSchema::empty();
        SqlToRel::new(&StubContextProvider::default())
            .sql_to_expr(expr.into(), &empty_df_schema, &mut Default::default())
            .context(ConvertToLogicalExpressionSnafu)
    }

    fn simplify_expr(logical_expr: Expr) -> std::result::Result<Expr, TQLError> {
        let empty_df_schema = DFSchema::empty();
        let execution_props = ExecutionProps::new().with_query_execution_start_time(Utc::now());
        let info = SimplifyContext::new(&execution_props).with_schema(Arc::new(empty_df_schema));
        ExprSimplifier::new(info)
            .simplify(logical_expr)
            .context(SimplificationSnafu)
    }

    fn evaluate_expr(simplified_expr: Expr) -> std::result::Result<String, TQLError> {
        match simplified_expr {
            Expr::Literal(ScalarValue::TimestampNanosecond(ts_nanos, _))
            | Expr::Literal(ScalarValue::DurationNanosecond(ts_nanos)) => {
                ts_nanos.map(|v| v / 1_000_000_000)
            }
            Expr::Literal(ScalarValue::TimestampMicrosecond(ts_micros, _))
            | Expr::Literal(ScalarValue::DurationMicrosecond(ts_micros)) => {
                ts_micros.map(|v| v / 1_000_000)
            }
            Expr::Literal(ScalarValue::TimestampMillisecond(ts_millis, _))
            | Expr::Literal(ScalarValue::DurationMillisecond(ts_millis)) => {
                ts_millis.map(|v| v / 1_000)
            }
            Expr::Literal(ScalarValue::TimestampSecond(ts_secs, _))
            | Expr::Literal(ScalarValue::DurationSecond(ts_secs)) => ts_secs,
            _ => None,
        }
        .map(|ts| ts.to_string())
        .context(EvaluationSnafu {
            msg: format!("Failed to extract a timestamp value {simplified_expr:?}"),
        })
    }

    fn parse_tql_query(parser: &mut Parser, sql: &str) -> std::result::Result<String, ParserError> {
        while matches!(parser.peek_token().token, Token::Comma) {
            let _skip_token = parser.next_token();
        }
        let index = parser.next_token().location.column as usize;
        if index == 0 {
            return Err(ParserError::ParserError("empty TQL query".to_string()));
        }

        let query = &sql[index - 1..];
        while parser.next_token() != Token::EOF {
            // consume all tokens
            // TODO(dennis): supports multi TQL statements separated by ';'?
        }
        // remove the last ';' or tailing space if exists
        Ok(query.trim().trim_end_matches(';').to_string())
    }
}

struct StubContextProvider {
    state: SessionState,
}

impl Default for StubContextProvider {
    fn default() -> Self {
        Self {
            state: SessionState::new_with_config_rt(Default::default(), Default::default()),
        }
    }
}

impl ContextProvider for StubContextProvider {
    fn get_table_source(&self, _name: TableReference) -> DFResult<Arc<dyn TableSource>> {
        unimplemented!()
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions().get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions().get(name).cloned()
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

    fn udfs_names(&self) -> Vec<String> {
        self.state.scalar_functions().keys().cloned().collect()
    }

    fn udafs_names(&self) -> Vec<String> {
        self.state.aggregate_functions().keys().cloned().collect()
    }

    fn udwfs_names(&self) -> Vec<String> {
        self.state.window_functions().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use common_error::ext::ErrorExt;

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParseOptions;

    fn parse_into_statement(sql: &str) -> Statement {
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());
        result.remove(0)
    }

    #[test]
    fn test_parse_tql_eval_with_functions() {
        let sql = "TQL EVAL (now() - now(), now() -  (now() - '10 seconds'::interval), '1s') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        let statement = parse_into_statement(sql);
        match statement {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "0");
                assert_eq!(eval.end, "10");
                assert_eq!(eval.step, "1s");
                assert_eq!(eval.lookback, None);
                assert_eq!(eval.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
            }
            _ => unreachable!(),
        }

        let sql = "TQL EVAL ('1970-01-01T00:05:00'::timestamp, '1970-01-01T00:10:00'::timestamp + '10 minutes'::interval, '1m') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "300");
                assert_eq!(eval.end, "1200");
                assert_eq!(eval.step, "1m");
                assert_eq!(eval.lookback, None);
                assert_eq!(eval.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_tql_eval() {
        let sql = "TQL EVAL (1676887657, 1676887659, '1m') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "1676887657");
                assert_eq!(eval.end, "1676887659");
                assert_eq!(eval.step, "1m");
                assert_eq!(eval.lookback, None);
                assert_eq!(eval.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
            }
            _ => unreachable!(),
        }

        let sql = "TQL EVAL (1676887657.1, 1676887659.5, 30.3) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        let statement = parse_into_statement(sql);

        match &statement {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "1676887657.1");
                assert_eq!(eval.end, "1676887659.5");
                assert_eq!(eval.step, "30.3");
                assert_eq!(eval.lookback, None);
                assert_eq!(eval.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
            }
            _ => unreachable!(),
        }

        let sql2 = "TQL EVALUATE (1676887657.1, 1676887659.5, 30.3) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        let statement2 = parse_into_statement(sql2);
        assert_eq!(statement, statement2);

        let sql = "tql eval ('2015-07-01T20:10:30.781Z', '2015-07-01T20:11:00.781Z', '30s') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "2015-07-01T20:10:30.781Z");
                assert_eq!(eval.end, "2015-07-01T20:11:00.781Z");
                assert_eq!(eval.step, "30s");
                assert_eq!(eval.lookback, None);
                assert_eq!(eval.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_tql_with_lookback_values() {
        let sql = "TQL EVAL (1676887657, 1676887659, '1m', '5m') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "1676887657");
                assert_eq!(eval.end, "1676887659");
                assert_eq!(eval.step, "1m".to_string());
                assert_eq!(eval.lookback, Some("5m".to_string()));
                assert_eq!(eval.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
            }
            _ => unreachable!(),
        }

        let sql = "TQL EVAL ('1970-01-01T00:05:00'::timestamp, '1970-01-01T00:10:00'::timestamp + '10 minutes'::interval, '1m', '7m') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "300");
                assert_eq!(eval.end, "1200");
                assert_eq!(eval.step, "1m");
                assert_eq!(eval.lookback, Some("7m".to_string()));
                assert_eq!(eval.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN (20, 100, 10, '3m') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert_eq!(explain.start, "20");
                assert_eq!(explain.end, "100");
                assert_eq!(explain.step, "10");
                assert_eq!(explain.lookback, Some("3m".to_string()));
                assert!(!explain.is_verbose);
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN VERBOSE (20, 100, 10, '3m') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert_eq!(explain.start, "20");
                assert_eq!(explain.end, "100");
                assert_eq!(explain.step, "10");
                assert_eq!(explain.lookback, Some("3m".to_string()));
                assert!(explain.is_verbose);
            }
            _ => unreachable!(),
        }

        let sql = "TQL ANALYZE (1676887657, 1676887659, '1m', '9m') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Analyze(analyze)) => {
                assert_eq!(analyze.start, "1676887657");
                assert_eq!(analyze.end, "1676887659");
                assert_eq!(analyze.step, "1m");
                assert_eq!(analyze.lookback, Some("9m".to_string()));
                assert_eq!(analyze.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert!(!analyze.is_verbose);
            }
            _ => unreachable!(),
        }

        let sql = "TQL ANALYZE VERBOSE (1676887657, 1676887659, '1m', '9m') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Analyze(analyze)) => {
                assert_eq!(analyze.start, "1676887657");
                assert_eq!(analyze.end, "1676887659");
                assert_eq!(analyze.step, "1m");
                assert_eq!(analyze.lookback, Some("9m".to_string()));
                assert_eq!(analyze.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert!(analyze.is_verbose);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_tql_explain() {
        let sql = "TQL EXPLAIN http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert_eq!(explain.start, "0");
                assert_eq!(explain.end, "0");
                assert_eq!(explain.step, "5m");
                assert_eq!(explain.lookback, None);
                assert!(!explain.is_verbose);
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN VERBOSE http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert_eq!(explain.start, "0");
                assert_eq!(explain.end, "0");
                assert_eq!(explain.step, "5m");
                assert_eq!(explain.lookback, None);
                assert!(explain.is_verbose);
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN (20,100,10) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert_eq!(explain.start, "20");
                assert_eq!(explain.end, "100");
                assert_eq!(explain.step, "10");
                assert_eq!(explain.lookback, None);
                assert!(!explain.is_verbose);
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN ('1970-01-01T00:05:00'::timestamp, '1970-01-01T00:10:00'::timestamp + '10 minutes'::interval, 10) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert_eq!(explain.start, "300");
                assert_eq!(explain.end, "1200");
                assert_eq!(explain.step, "10");
                assert_eq!(explain.lookback, None);
                assert!(!explain.is_verbose);
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN VERBOSE (20,100,10) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert_eq!(explain.start, "20");
                assert_eq!(explain.end, "100");
                assert_eq!(explain.step, "10");
                assert_eq!(explain.lookback, None);
                assert!(explain.is_verbose);
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN verbose (20,100,10) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert_eq!(explain.start, "20");
                assert_eq!(explain.end, "100");
                assert_eq!(explain.step, "10");
                assert_eq!(explain.lookback, None);
                assert!(explain.is_verbose);
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN VERBOSE ('1970-01-01T00:05:00'::timestamp, '1970-01-01T00:10:00'::timestamp + '10 minutes'::interval, 10) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert_eq!(explain.start, "300");
                assert_eq!(explain.end, "1200");
                assert_eq!(explain.step, "10");
                assert_eq!(explain.lookback, None);
                assert!(explain.is_verbose);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_tql_analyze() {
        let sql = "TQL ANALYZE (1676887657.1, 1676887659.5, 30.3) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Analyze(analyze)) => {
                assert_eq!(analyze.start, "1676887657.1");
                assert_eq!(analyze.end, "1676887659.5");
                assert_eq!(analyze.step, "30.3");
                assert_eq!(analyze.lookback, None);
                assert_eq!(analyze.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert!(!analyze.is_verbose);
            }
            _ => unreachable!(),
        }

        let sql = "TQL ANALYZE ('1970-01-01T00:05:00'::timestamp, '1970-01-01T00:10:00'::timestamp + '10 minutes'::interval, 10) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Analyze(analyze)) => {
                assert_eq!(analyze.start, "300");
                assert_eq!(analyze.end, "1200");
                assert_eq!(analyze.step, "10");
                assert_eq!(analyze.lookback, None);
                assert_eq!(analyze.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert!(!analyze.is_verbose);
            }
            _ => unreachable!(),
        }

        let sql = "TQL ANALYZE VERBOSE (1676887657.1, 1676887659.5, 30.3) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Analyze(analyze)) => {
                assert_eq!(analyze.start, "1676887657.1");
                assert_eq!(analyze.end, "1676887659.5");
                assert_eq!(analyze.step, "30.3");
                assert_eq!(analyze.lookback, None);
                assert_eq!(analyze.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert!(analyze.is_verbose);
            }
            _ => unreachable!(),
        }

        let sql = "TQL ANALYZE verbose (1676887657.1, 1676887659.5, 30.3) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Analyze(analyze)) => {
                assert_eq!(analyze.start, "1676887657.1");
                assert_eq!(analyze.end, "1676887659.5");
                assert_eq!(analyze.step, "30.3");
                assert_eq!(analyze.lookback, None);
                assert_eq!(analyze.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert!(analyze.is_verbose);
            }
            _ => unreachable!(),
        }

        let sql = "TQL ANALYZE VERBOSE ('1970-01-01T00:05:00'::timestamp, '1970-01-01T00:10:00'::timestamp + '10 minutes'::interval, 10) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Analyze(analyze)) => {
                assert_eq!(analyze.start, "300");
                assert_eq!(analyze.end, "1200");
                assert_eq!(analyze.step, "10");
                assert_eq!(analyze.lookback, None);
                assert_eq!(analyze.query, "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m");
                assert!(analyze.is_verbose);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_tql_with_various_queries() {
        // query has whitespaces and comma
        match parse_into_statement("TQL EVAL (0, 30, '10s')           ,       data + (1 < bool 2);")
        {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "0");
                assert_eq!(eval.end, "30");
                assert_eq!(eval.step, "10s");
                assert_eq!(eval.lookback, None);
                assert_eq!(eval.query, "data + (1 < bool 2)");
            }
            _ => unreachable!(),
        }
        // query starts with a quote
        match parse_into_statement("TQL EVAL (0, 10, '5s') '1+1';") {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "0");
                assert_eq!(eval.end, "10");
                assert_eq!(eval.step, "5s");
                assert_eq!(eval.lookback, None);
                assert_eq!(eval.query, "'1+1'");
            }
            _ => unreachable!(),
        }

        // query starts with number
        match parse_into_statement("TQL EVAL (300, 300, '1s') 10 atan2 20;") {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "300");
                assert_eq!(eval.end, "300");
                assert_eq!(eval.step, "1s");
                assert_eq!(eval.lookback, None);
                assert_eq!(eval.query, "10 atan2 20");
            }
            _ => unreachable!(),
        }

        // query starts with a bracket
        let sql = "TQL EVAL (0, 30, '10s') (sum by(host) (irate(host_cpu_seconds_total{mode!='idle'}[1m0s])) / sum by (host)((irate(host_cpu_seconds_total[1m0s])))) * 100;";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "0");
                assert_eq!(eval.end, "30");
                assert_eq!(eval.step, "10s");
                assert_eq!(eval.lookback, None);
                assert_eq!(eval.query, "(sum by(host) (irate(host_cpu_seconds_total{mode!='idle'}[1m0s])) / sum by (host)((irate(host_cpu_seconds_total[1m0s])))) * 100");
            }
            _ => unreachable!(),
        }

        // query starts with a curly bracket
        match parse_into_statement("TQL EVAL (0, 10, '5s') {__name__=\"test\"}") {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "0");
                assert_eq!(eval.end, "10");
                assert_eq!(eval.step, "5s");
                assert_eq!(eval.lookback, None);
                assert_eq!(eval.query, "{__name__=\"test\"}");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_tql_error() {
        let dialect = &GreptimeDbDialect {};
        let parse_options = ParseOptions::default();

        // invalid duration
        let sql = "TQL EVAL (1676887657, 1676887659, 1m) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        let result =
            ParserContext::create_with_dialect(sql, dialect, parse_options.clone()).unwrap_err();
        assert!(result
            .output_msg()
            .contains("Failed to extract a timestamp value"));

        // missing end
        let sql = "TQL EVAL (1676887657, '1m') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        let result =
            ParserContext::create_with_dialect(sql, dialect, parse_options.clone()).unwrap_err();
        assert!(result
            .output_msg()
            .contains("Failed to extract a timestamp value"));

        // empty TQL query
        let sql = "TQL EVAL (0, 30, '10s')";
        let result =
            ParserContext::create_with_dialect(sql, dialect, parse_options.clone()).unwrap_err();
        assert!(result.output_msg().contains("empty TQL query"));
    }
}
