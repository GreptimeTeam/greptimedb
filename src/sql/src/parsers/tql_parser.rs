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

use datafusion_common::ScalarValue;
use snafu::{OptionExt, ResultExt};
use sqlparser::ast::AnalyzeFormat;
use sqlparser::keywords::Keyword;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;

use crate::dialect::GreptimeDbDialect;
use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::parsers::utils;
use crate::statements::statement::Statement;
use crate::statements::tql::{Tql, TqlAnalyze, TqlEval, TqlExplain, TqlParameters};
use crate::util::location_to_index;

pub const TQL: &str = "TQL";
const EVAL: &str = "EVAL";
const EVALUATE: &str = "EVALUATE";
const VERBOSE: &str = "VERBOSE";
const FORMAT: &str = "FORMAT";

use sqlparser::parser::Parser;

use crate::parsers::error::{
    ConvertToLogicalExpressionSnafu, EvaluationSnafu, ParserSnafu, TQLError,
};

/// TQL extension parser, including:
/// - `TQL EVAL <query>`
/// - `TQL EXPLAIN [VERBOSE] [FORMAT format] <query>`
/// - `TQL ANALYZE [VERBOSE] [FORMAT format] <query>`
impl ParserContext<'_> {
    pub(crate) fn parse_tql(&mut self, require_now_expr: bool) -> Result<Statement> {
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
                        self.parse_tql_params(require_now_expr)
                            .map(|params| Statement::Tql(Tql::Eval(TqlEval::from(params))))
                            .context(error::TQLSyntaxSnafu)
                    }

                    Keyword::EXPLAIN => {
                        let is_verbose = self.has_verbose_keyword();
                        if is_verbose {
                            let _consume_verbose_token = self.parser.next_token();
                        }
                        let format = self.parse_format_option();
                        self.parse_tql_params(require_now_expr)
                            .map(|mut params| {
                                params.is_verbose = is_verbose;
                                params.format = format;
                                Statement::Tql(Tql::Explain(TqlExplain::from(params)))
                            })
                            .context(error::TQLSyntaxSnafu)
                    }

                    Keyword::ANALYZE => {
                        let is_verbose = self.has_verbose_keyword();
                        if is_verbose {
                            let _consume_verbose_token = self.parser.next_token();
                        }
                        let format = self.parse_format_option();
                        self.parse_tql_params(require_now_expr)
                            .map(|mut params| {
                                params.is_verbose = is_verbose;
                                params.format = format;
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

    /// Parse a parenthesized TQL statement and return [`Tql`] and raw text.
    ///
    /// # Parameters
    ///
    /// - `is_lparen_consumed`: whether the leading `(` has already been consumed by the caller.
    /// - `require_now_expr`: whether time params must contain `now()` (same as [`Self::parse_tql`]).
    /// - `only_eval`: if `true`, rejects non-`TQL EVAL/EVALUATE` statements.
    ///
    /// # Examples
    /// - (TQL EVAL (0, 10, '1s') cpu_usage)
    #[allow(dead_code)]
    pub(crate) fn parse_parenthesized_tql(
        &mut self,
        is_lparen_consumed: bool,
        require_now_expr: bool,
        only_eval: bool,
    ) -> Result<(Tql, String)> {
        if !is_lparen_consumed {
            self.parser
                .expect_token(&Token::LParen)
                .context(error::SyntaxSnafu)?;
        }

        let tql_token = self.parser.next_token();
        if tql_token.token == Token::EOF {
            return Err(error::InvalidSqlSnafu {
                msg: "Unexpected end of input while parsing TQL".to_string(),
            }
            .build());
        }

        let start_location = tql_token.span.start;
        let mut paren_depth = 0usize;
        let end_location;

        loop {
            let token_with_span = self.parser.peek_token();

            if token_with_span.token == Token::EOF {
                return Err(error::InvalidSqlSnafu {
                    msg: "Unexpected end of input while parsing TQL".to_string(),
                }
                .build());
            }

            if token_with_span.token == Token::RParen && paren_depth == 0 {
                end_location = token_with_span.span.start;
                break;
            }

            let consumed = self.parser.next_token();
            match consumed.token {
                Token::LParen => paren_depth += 1,
                Token::RParen => paren_depth = paren_depth.saturating_sub(1),
                _ => {}
            }
        }

        let start_index = location_to_index(self.sql, &start_location);
        let end_index = location_to_index(self.sql, &end_location);
        let tql_sql = &self.sql[start_index..end_index];
        let tql_sql = tql_sql.trim();
        let raw_query = tql_sql.trim_end_matches(';');

        let mut parser_ctx = ParserContext::new(&GreptimeDbDialect {}, tql_sql)?;
        let statement = parser_ctx.parse_tql(require_now_expr)?;

        match statement {
            Statement::Tql(tql) => match (only_eval, tql) {
                (true, Tql::Eval(eval)) => Ok((Tql::Eval(eval), raw_query.to_string())),
                (true, _) => Err(error::InvalidSqlSnafu {
                    msg: "Only TQL EVAL is supported".to_string(),
                }
                .build()),
                (false, tql) => Ok((tql, raw_query.to_string())),
            },
            _ => Err(error::InvalidSqlSnafu {
                msg: "Expected TQL statement".to_string(),
            }
            .build()),
        }
    }

    /// `require_now_expr` indicates whether the start&end must contain a `now()` function.
    fn parse_tql_params(
        &mut self,
        require_now_expr: bool,
    ) -> std::result::Result<TqlParameters, TQLError> {
        let parser = &mut self.parser;
        let (start, end, step, lookback) = match parser.peek_token().token {
            Token::LParen => {
                let _consume_lparen_token = parser.next_token();
                let exprs = parser
                    .parse_comma_separated(Parser::parse_expr)
                    .context(ParserSnafu)?;

                let param_count = exprs.len();

                if param_count != 3 && param_count != 4 {
                    return Err(ParserError::ParserError(
                        format!("Expected 3 or 4 expressions in TQL parameters (start, end, step, [lookback]), but found {}", param_count)
                    ))
                    .context(ParserSnafu);
                }

                let mut exprs_iter = exprs.into_iter();
                // Safety: safe to call next and unwrap, because we already check the param_count above.
                let start = Self::parse_expr_to_literal_or_ts(
                    exprs_iter.next().unwrap(),
                    require_now_expr,
                )?;
                let end = Self::parse_expr_to_literal_or_ts(
                    exprs_iter.next().unwrap(),
                    require_now_expr,
                )?;
                let step = Self::parse_expr_to_literal_or_ts(exprs_iter.next().unwrap(), false)?;

                let lookback = exprs_iter
                    .next()
                    .map(|expr| Self::parse_expr_to_literal_or_ts(expr, false))
                    .transpose()?;

                if !parser.consume_token(&Token::RParen) {
                    return Err(ParserError::ParserError(format!(
                        "Expected ')' after TQL parameters, but found: {}",
                        parser.peek_token()
                    )))
                    .context(ParserSnafu);
                }

                (start, end, step, lookback)
            }
            _ => ("0".to_string(), "0".to_string(), "5m".to_string(), None),
        };

        let (query, alias) = Self::parse_tql_query(parser, self.sql).context(ParserSnafu)?;
        Ok(TqlParameters::new(start, end, step, lookback, query, alias))
    }

    pub fn comma_or_rparen(token: &Token) -> bool {
        Self::is_comma(token) || Self::is_rparen(token)
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

    fn parse_format_option(&mut self) -> Option<AnalyzeFormat> {
        if self.peek_token_as_string().eq_ignore_ascii_case(FORMAT) {
            let _consume_format_token = self.parser.next_token();
            // Parse format type
            if let Token::Word(w) = &self.parser.peek_token().token {
                let format_type = w.value.to_uppercase();
                let _consume_format_type_token = self.parser.next_token();
                match format_type.as_str() {
                    "JSON" => Some(AnalyzeFormat::JSON),
                    "TEXT" => Some(AnalyzeFormat::TEXT),
                    "GRAPHVIZ" => Some(AnalyzeFormat::GRAPHVIZ),
                    _ => None, // Invalid format, ignore silently
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Parse the expression to a literal string or a timestamp in seconds.
    fn parse_expr_to_literal_or_ts(
        parser_expr: sqlparser::ast::Expr,
        require_now_expr: bool,
    ) -> std::result::Result<String, TQLError> {
        match parser_expr {
            sqlparser::ast::Expr::Value(v) => match v.value {
                sqlparser::ast::Value::Number(s, _) if !require_now_expr => Ok(s),
                sqlparser::ast::Value::DoubleQuotedString(s)
                | sqlparser::ast::Value::SingleQuotedString(s)
                    if !require_now_expr =>
                {
                    Ok(s)
                }
                unexpected => {
                    if !require_now_expr {
                        Err(ParserError::ParserError(format!(
                            "Expected number, string or word, but have {unexpected:?}"
                        )))
                        .context(ParserSnafu)
                    } else {
                        Err(ParserError::ParserError(format!(
                            "Expected expression containing `now()`, but have {unexpected:?}"
                        )))
                        .context(ParserSnafu)
                    }
                }
            },
            _ => Self::parse_expr_to_ts(parser_expr, require_now_expr),
        }
    }

    /// Parse the expression to a timestamp in seconds.
    fn parse_expr_to_ts(
        parser_expr: sqlparser::ast::Expr,
        require_now_expr: bool,
    ) -> std::result::Result<String, TQLError> {
        let lit = utils::parser_expr_to_scalar_value_literal(parser_expr, require_now_expr)
            .map_err(Box::new)
            .context(ConvertToLogicalExpressionSnafu)?;

        let second = match lit {
            ScalarValue::TimestampNanosecond(ts_nanos, _)
            | ScalarValue::DurationNanosecond(ts_nanos) => ts_nanos.map(|v| v / 1_000_000_000),
            ScalarValue::TimestampMicrosecond(ts_micros, _)
            | ScalarValue::DurationMicrosecond(ts_micros) => ts_micros.map(|v| v / 1_000_000),
            ScalarValue::TimestampMillisecond(ts_millis, _)
            | ScalarValue::DurationMillisecond(ts_millis) => ts_millis.map(|v| v / 1_000),
            ScalarValue::TimestampSecond(ts_secs, _) | ScalarValue::DurationSecond(ts_secs) => {
                ts_secs
            }
            _ => None,
        };

        second.map(|ts| ts.to_string()).context(EvaluationSnafu {
            msg: format!("Failed to extract a timestamp value {lit:?}"),
        })
    }

    /// Parse the TQL query and optional alias from the given [Parser] and SQL string.
    pub fn parse_tql_query(
        parser: &mut Parser,
        sql: &str,
    ) -> std::result::Result<(String, Option<String>), ParserError> {
        while matches!(parser.peek_token().token, Token::Comma) {
            let _skip_token = parser.next_token();
        }
        let start_tql = parser.next_token();
        if start_tql == Token::EOF {
            return Err(ParserError::ParserError("empty TQL query".to_string()));
        }

        let start_location = start_tql.span.start;
        // translate the start location to the index in the sql string
        let index = location_to_index(sql, &start_location);
        assert!(index > 0);

        let mut token = start_tql;
        loop {
            // Find AS keyword, which indicates "<promql> AS <alias"
            if matches!(&token.token, Token::Word(w) if w.keyword == Keyword::AS) {
                let query_end_index = location_to_index(sql, &token.span.start);
                let alias = parser.parse_identifier()?;
                let promql = sql[index - 1..query_end_index]
                    .trim()
                    .trim_end_matches(';')
                    .to_string();
                if promql.is_empty() {
                    return Err(ParserError::ParserError("Empty promql query".to_string()));
                }

                if parser.consume_token(&Token::EOF) || parser.consume_token(&Token::SemiColon) {
                    return Ok((promql, Some(alias.value)));
                } else {
                    return Err(ParserError::ParserError(format!(
                        "Unexpected token after alias: {}",
                        parser.peek_token()
                    )));
                }
            }
            token = parser.next_token();
            if token == Token::EOF {
                break;
            }
        }

        // AS clause not found
        let promql = sql[index - 1..].trim().trim_end_matches(';').to_string();
        if promql.is_empty() {
            return Err(ParserError::ParserError("Empty promql query".to_string()));
        }
        Ok((promql, None))
    }
}

#[cfg(test)]
mod tests {
    use common_error::ext::ErrorExt;
    use sqlparser::tokenizer::Token;

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};

    fn parse_into_statement(sql: &str) -> Statement {
        let mut result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());
        result.remove(0)
    }

    #[test]
    fn test_require_now_expr() {
        let sql = "TQL EVAL (now() - now(), now() -  (now() - '10 seconds'::interval), '1s') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";

        let mut parser = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let statement = parser.parse_tql(true).unwrap();
        match statement {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "0");
                assert_eq!(eval.end, "10");
                assert_eq!(eval.step, "1s");
                assert_eq!(eval.lookback, None);
                assert_eq!(
                    eval.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
            }
            _ => unreachable!(),
        };

        let sql = "TQL EVAL (0, 15, '1s') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";

        let mut parser = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let statement = parser.parse_tql(true);
        assert!(
            statement.is_err()
                && format!("{:?}", statement)
                    .contains("Expected expression containing `now()`, but have "),
            "statement: {:?}",
            statement
        );

        let sql = "TQL EVAL (now() - now(), now() -  (now() - '10 seconds'::interval), '1s') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";

        let mut parser = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let statement = parser.parse_tql(false).unwrap();
        match statement {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "0");
                assert_eq!(eval.end, "10");
                assert_eq!(eval.step, "1s");
                assert_eq!(eval.lookback, None);
                assert_eq!(
                    eval.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
            }
            _ => unreachable!(),
        };

        let sql = "TQL EVAL (0, 15, '1s') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        let mut parser = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let statement = parser.parse_tql(false).unwrap();
        match statement {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "0");
                assert_eq!(eval.end, "15");
                assert_eq!(eval.step, "1s");
                assert_eq!(eval.lookback, None);
                assert_eq!(
                    eval.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
            }
            _ => unreachable!(),
        };
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
                assert_eq!(
                    eval.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
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
                assert_eq!(
                    eval.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
            }
            _ => unreachable!(),
        }

        let sql = "TQL EVAL (now(), now()-'5m', '30s') http_requests_total";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_tql_eval_with_date_trunc() {
        let sql = "TQL EVAL (date_trunc('day', now() - interval '1' day), date_trunc('day', now()), '1h') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        let statement = parse_into_statement(sql);
        match statement {
            Statement::Tql(Tql::Eval(eval)) => {
                // date_trunc('day', now() - interval '1' day) should resolve to start of yesterday
                // date_trunc('day', now()) should resolve to start of today
                // The exact values depend on when the test runs, but we can verify the structure
                assert!(eval.start.parse::<i64>().is_ok());
                assert!(eval.end.parse::<i64>().is_ok());
                assert_eq!(eval.step, "1h");
                assert_eq!(eval.lookback, None);
                assert_eq!(
                    eval.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
            }
            _ => unreachable!(),
        }

        // Test with 4 parameters including lookback
        let sql = "TQL EVAL (date_trunc('hour', now() - interval '6' hour), date_trunc('hour', now()), '30m', '5m') cpu_usage_total";
        let statement = parse_into_statement(sql);
        match statement {
            Statement::Tql(Tql::Eval(eval)) => {
                assert!(eval.start.parse::<i64>().is_ok());
                assert!(eval.end.parse::<i64>().is_ok());
                assert_eq!(eval.step, "30m");
                assert_eq!(eval.lookback, Some("5m".to_string()));
                assert_eq!(eval.query, "cpu_usage_total");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_tql_analyze_with_date_trunc() {
        let sql = "TQL ANALYZE VERBOSE FORMAT JSON (date_trunc('week', now() - interval '2' week), date_trunc('week', now()), '4h', '1h') network_bytes_total";
        let statement = parse_into_statement(sql);
        match statement {
            Statement::Tql(Tql::Analyze(analyze)) => {
                assert!(analyze.start.parse::<i64>().is_ok());
                assert!(analyze.end.parse::<i64>().is_ok());
                assert_eq!(analyze.step, "4h");
                assert_eq!(analyze.lookback, Some("1h".to_string()));
                assert_eq!(analyze.query, "network_bytes_total");
                assert!(analyze.is_verbose);
                assert_eq!(analyze.format, Some(AnalyzeFormat::JSON));
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
                assert_eq!(
                    eval.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
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
                assert_eq!(
                    eval.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
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
                assert_eq!(
                    eval.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
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
                assert_eq!(
                    eval.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
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
                assert_eq!(
                    eval.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN (20, 100, 10, '3m') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(
                    explain.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
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
                assert_eq!(
                    explain.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
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
                assert_eq!(
                    analyze.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
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
                assert_eq!(
                    analyze.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
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
                assert_eq!(
                    explain.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert_eq!(explain.start, "0");
                assert_eq!(explain.end, "0");
                assert_eq!(explain.step, "5m");
                assert_eq!(explain.lookback, None);
                assert!(!explain.is_verbose);
                assert_eq!(explain.format, None);
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN VERBOSE http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(
                    explain.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert_eq!(explain.start, "0");
                assert_eq!(explain.end, "0");
                assert_eq!(explain.step, "5m");
                assert_eq!(explain.lookback, None);
                assert!(explain.is_verbose);
                assert_eq!(explain.format, None);
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN FORMAT JSON http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(
                    explain.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert_eq!(explain.start, "0");
                assert_eq!(explain.end, "0");
                assert_eq!(explain.step, "5m");
                assert_eq!(explain.lookback, None);
                assert!(!explain.is_verbose);
                assert_eq!(explain.format, Some(AnalyzeFormat::JSON));
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN VERBOSE FORMAT JSON http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(
                    explain.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert_eq!(explain.start, "0");
                assert_eq!(explain.end, "0");
                assert_eq!(explain.step, "5m");
                assert_eq!(explain.lookback, None);
                assert!(explain.is_verbose);
                assert_eq!(explain.format, Some(AnalyzeFormat::JSON));
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN FORMAT TEXT (20,100,10) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(
                    explain.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert_eq!(explain.start, "20");
                assert_eq!(explain.end, "100");
                assert_eq!(explain.step, "10");
                assert_eq!(explain.lookback, None);
                assert!(!explain.is_verbose);
                assert_eq!(explain.format, Some(AnalyzeFormat::TEXT));
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN (20,100,10) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(
                    explain.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert_eq!(explain.start, "20");
                assert_eq!(explain.end, "100");
                assert_eq!(explain.step, "10");
                assert_eq!(explain.lookback, None);
                assert!(!explain.is_verbose);
                assert_eq!(explain.format, None);
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN ('1970-01-01T00:05:00'::timestamp, '1970-01-01T00:10:00'::timestamp + '10 minutes'::interval, 10) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(
                    explain.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert_eq!(explain.start, "300");
                assert_eq!(explain.end, "1200");
                assert_eq!(explain.step, "10");
                assert_eq!(explain.lookback, None);
                assert!(!explain.is_verbose);
                assert_eq!(explain.format, None);
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN VERBOSE (20,100,10) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(
                    explain.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert_eq!(explain.start, "20");
                assert_eq!(explain.end, "100");
                assert_eq!(explain.step, "10");
                assert_eq!(explain.lookback, None);
                assert!(explain.is_verbose);
                assert_eq!(explain.format, None);
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN verbose (20,100,10) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(
                    explain.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert_eq!(explain.start, "20");
                assert_eq!(explain.end, "100");
                assert_eq!(explain.step, "10");
                assert_eq!(explain.lookback, None);
                assert!(explain.is_verbose);
                assert_eq!(explain.format, None);
            }
            _ => unreachable!(),
        }

        let sql = "TQL EXPLAIN VERBOSE ('1970-01-01T00:05:00'::timestamp, '1970-01-01T00:10:00'::timestamp + '10 minutes'::interval, 10) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(
                    explain.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert_eq!(explain.start, "300");
                assert_eq!(explain.end, "1200");
                assert_eq!(explain.step, "10");
                assert_eq!(explain.lookback, None);
                assert!(explain.is_verbose);
                assert_eq!(explain.format, None);
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
                assert_eq!(
                    analyze.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert!(!analyze.is_verbose);
                assert_eq!(analyze.format, None);
            }
            _ => unreachable!(),
        }

        let sql = "TQL ANALYZE FORMAT JSON (1676887657.1, 1676887659.5, 30.3) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Analyze(analyze)) => {
                assert_eq!(analyze.start, "1676887657.1");
                assert_eq!(analyze.end, "1676887659.5");
                assert_eq!(analyze.step, "30.3");
                assert_eq!(analyze.lookback, None);
                assert_eq!(
                    analyze.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert!(!analyze.is_verbose);
                assert_eq!(analyze.format, Some(AnalyzeFormat::JSON));
            }
            _ => unreachable!(),
        }

        let sql = "TQL ANALYZE VERBOSE FORMAT JSON (1676887657.1, 1676887659.5, 30.3) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Analyze(analyze)) => {
                assert_eq!(analyze.start, "1676887657.1");
                assert_eq!(analyze.end, "1676887659.5");
                assert_eq!(analyze.step, "30.3");
                assert_eq!(analyze.lookback, None);
                assert_eq!(
                    analyze.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert!(analyze.is_verbose);
                assert_eq!(analyze.format, Some(AnalyzeFormat::JSON));
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
                assert_eq!(
                    analyze.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert!(!analyze.is_verbose);
                assert_eq!(analyze.format, None);
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
                assert_eq!(
                    analyze.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert!(analyze.is_verbose);
                assert_eq!(analyze.format, None);
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
                assert_eq!(
                    analyze.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert!(analyze.is_verbose);
                assert_eq!(analyze.format, None);
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
                assert_eq!(
                    analyze.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert!(analyze.is_verbose);
                assert_eq!(analyze.format, None);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_tql_format() {
        // Test FORMAT JSON for EXPLAIN
        let sql = "TQL EXPLAIN FORMAT JSON http_requests_total";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.format, Some(AnalyzeFormat::JSON));
                assert!(!explain.is_verbose);
            }
            _ => unreachable!(),
        }

        // Test FORMAT TEXT for EXPLAIN
        let sql = "TQL EXPLAIN FORMAT TEXT http_requests_total";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.format, Some(AnalyzeFormat::TEXT));
                assert!(!explain.is_verbose);
            }
            _ => unreachable!(),
        }

        // Test FORMAT GRAPHVIZ for EXPLAIN
        let sql = "TQL EXPLAIN FORMAT GRAPHVIZ http_requests_total";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.format, Some(AnalyzeFormat::GRAPHVIZ));
                assert!(!explain.is_verbose);
            }
            _ => unreachable!(),
        }

        // Test VERBOSE FORMAT JSON for ANALYZE
        let sql = "TQL ANALYZE VERBOSE FORMAT JSON (0,10,'5s') http_requests_total";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Analyze(analyze)) => {
                assert_eq!(analyze.format, Some(AnalyzeFormat::JSON));
                assert!(analyze.is_verbose);
            }
            _ => unreachable!(),
        }

        // Test FORMAT before parameters
        let sql = "TQL EXPLAIN FORMAT JSON (0,10,'5s') http_requests_total";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.format, Some(AnalyzeFormat::JSON));
                assert_eq!(explain.start, "0");
                assert_eq!(explain.end, "10");
                assert_eq!(explain.step, "5s");
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
                assert_eq!(
                    eval.query,
                    "(sum by(host) (irate(host_cpu_seconds_total{mode!='idle'}[1m0s])) / sum by (host)((irate(host_cpu_seconds_total[1m0s])))) * 100"
                );
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
    fn test_parse_tql_with_alias() {
        // Test TQL EVAL with alias
        let sql = "TQL EVAL (0, 30, '10s') http_requests_total AS my_metric";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "0");
                assert_eq!(eval.end, "30");
                assert_eq!(eval.step, "10s");
                assert_eq!(eval.lookback, None);
                assert_eq!(eval.query, "http_requests_total");
                assert_eq!(eval.alias, Some("my_metric".to_string()));
            }
            _ => unreachable!(),
        }

        // Test TQL EVAL with complex query and alias
        let sql = "TQL EVAL (1676887657, 1676887659, '1m') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m AS web_requests";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "1676887657");
                assert_eq!(eval.end, "1676887659");
                assert_eq!(eval.step, "1m");
                assert_eq!(eval.lookback, None);
                assert_eq!(
                    eval.query,
                    "http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m"
                );
                assert_eq!(eval.alias, Some("web_requests".to_string()));
            }
            _ => unreachable!(),
        }

        // Test TQL EVAL with lookback and alias
        let sql = "TQL EVAL (0, 100, '30s', '5m') cpu_usage_total AS cpu_metrics";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.start, "0");
                assert_eq!(eval.end, "100");
                assert_eq!(eval.step, "30s");
                assert_eq!(eval.lookback, Some("5m".to_string()));
                assert_eq!(eval.query, "cpu_usage_total");
                assert_eq!(eval.alias, Some("cpu_metrics".to_string()));
            }
            _ => unreachable!(),
        }

        // Test TQL EXPLAIN with alias
        let sql = "TQL EXPLAIN (20, 100, '10s') memory_usage{app='web'} AS memory_data";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.start, "20");
                assert_eq!(explain.end, "100");
                assert_eq!(explain.step, "10s");
                assert_eq!(explain.lookback, None);
                assert_eq!(explain.query, "memory_usage{app='web'}");
                assert_eq!(explain.alias, Some("memory_data".to_string()));
                assert!(!explain.is_verbose);
                assert_eq!(explain.format, None);
            }
            _ => unreachable!(),
        }

        // Test TQL EXPLAIN VERBOSE with alias
        let sql = "TQL EXPLAIN VERBOSE FORMAT JSON (0, 50, '5s') disk_io_rate AS disk_metrics";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Explain(explain)) => {
                assert_eq!(explain.start, "0");
                assert_eq!(explain.end, "50");
                assert_eq!(explain.step, "5s");
                assert_eq!(explain.lookback, None);
                assert_eq!(explain.query, "disk_io_rate");
                assert_eq!(explain.alias, Some("disk_metrics".to_string()));
                assert!(explain.is_verbose);
                assert_eq!(explain.format, Some(AnalyzeFormat::JSON));
            }
            _ => unreachable!(),
        }

        // Test TQL ANALYZE with alias
        let sql = "TQL ANALYZE (100, 200, '1m') network_bytes_total AS network_stats";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Analyze(analyze)) => {
                assert_eq!(analyze.start, "100");
                assert_eq!(analyze.end, "200");
                assert_eq!(analyze.step, "1m");
                assert_eq!(analyze.lookback, None);
                assert_eq!(analyze.query, "network_bytes_total");
                assert_eq!(analyze.alias, Some("network_stats".to_string()));
                assert!(!analyze.is_verbose);
                assert_eq!(analyze.format, None);
            }
            _ => unreachable!(),
        }

        // Test TQL ANALYZE VERBOSE with alias and lookback
        let sql = "TQL ANALYZE VERBOSE FORMAT TEXT (0, 1000, '2m', '30s') error_rate{service='api'} AS api_errors";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Analyze(analyze)) => {
                assert_eq!(analyze.start, "0");
                assert_eq!(analyze.end, "1000");
                assert_eq!(analyze.step, "2m");
                assert_eq!(analyze.lookback, Some("30s".to_string()));
                assert_eq!(analyze.query, "error_rate{service='api'}");
                assert_eq!(analyze.alias, Some("api_errors".to_string()));
                assert!(analyze.is_verbose);
                assert_eq!(analyze.format, Some(AnalyzeFormat::TEXT));
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_tql_alias_edge_cases() {
        // Test alias with underscore and numbers
        let sql = "TQL EVAL (0, 10, '5s') test_metric AS metric_123";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.query, "test_metric");
                assert_eq!(eval.alias, Some("metric_123".to_string()));
            }
            _ => unreachable!(),
        }

        // Test complex PromQL expression with AS
        let sql = r#"TQL EVAL (0, 30, '10s') (sum by(host) (irate(host_cpu_seconds_total{mode!='idle'}[1m0s])) / sum by (host)((irate(host_cpu_seconds_total[1m0s])))) * 100 AS cpu_utilization;"#;
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(
                    eval.query,
                    "(sum by(host) (irate(host_cpu_seconds_total{mode!='idle'}[1m0s])) / sum by (host)((irate(host_cpu_seconds_total[1m0s])))) * 100"
                );
                assert_eq!(eval.alias, Some("cpu_utilization".to_string()));
            }
            _ => unreachable!(),
        }

        // Test query with semicolon and alias
        let sql = "TQL EVAL (0, 10, '5s') simple_metric AS my_alias";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.query, "simple_metric");
                assert_eq!(eval.alias, Some("my_alias".to_string()));
            }
            _ => unreachable!(),
        }

        // Test without alias (ensure it still works)
        let sql = "TQL EVAL (0, 10, '5s') test_metric_no_alias";
        match parse_into_statement(sql) {
            Statement::Tql(Tql::Eval(eval)) => {
                assert_eq!(eval.query, "test_metric_no_alias");
                assert_eq!(eval.alias, None);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_tql_alias_errors() {
        let dialect = &GreptimeDbDialect {};
        let parse_options = ParseOptions::default();

        // Test AS without alias identifier
        let sql = "TQL EVAL (0, 10, '5s') test_metric AS";
        let result = ParserContext::create_with_dialect(sql, dialect, parse_options.clone());
        assert!(result.is_err(), "Should fail when AS has no identifier");

        // Test AS with invalid characters after alias
        let sql = "TQL EVAL (0, 10, '5s') test_metric AS alias extra_token";
        let result = ParserContext::create_with_dialect(sql, dialect, parse_options.clone());
        assert!(
            result.is_err(),
            "Should fail with unexpected token after alias"
        );

        // Test AS with empty promql query
        let sql = "TQL EVAL (0, 10, '5s') AS alias";
        let result = ParserContext::create_with_dialect(sql, dialect, parse_options.clone());
        assert!(result.is_err(), "Should fail with empty promql query");
    }

    #[test]
    fn test_parse_tql_error() {
        let dialect = &GreptimeDbDialect {};
        let parse_options = ParseOptions::default();

        // invalid duration
        let sql = "TQL EVAL (1676887657, 1676887659, 1m) http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        let result =
            ParserContext::create_with_dialect(sql, dialect, parse_options.clone()).unwrap_err();

        assert!(
            result
                .output_msg()
                .contains("Expected ')' after TQL parameters, but found: m"),
            "{}",
            result.output_msg()
        );

        // missing end
        let sql = "TQL EVAL (1676887657, '1m') http_requests_total{environment=~'staging|testing|development',method!='GET'} @ 1609746000 offset 5m";
        let result =
            ParserContext::create_with_dialect(sql, dialect, parse_options.clone()).unwrap_err();
        assert!(
            result
                .output_msg()
                .contains("Expected 3 or 4 expressions in TQL parameters"),
            "{}",
            result.output_msg()
        );

        // empty TQL query
        let sql = "TQL EVAL (0, 30, '10s')";
        let result =
            ParserContext::create_with_dialect(sql, dialect, parse_options.clone()).unwrap_err();
        assert!(result.output_msg().contains("empty TQL query"));

        // invalid token
        let sql = "tql eval (0, 0, '1s) t;;';";
        let result =
            ParserContext::create_with_dialect(sql, dialect, parse_options.clone()).unwrap_err();
        assert!(
            result
                .output_msg()
                .contains("Expected ')' after TQL parameters, but found: ;"),
            "{}",
            result.output_msg()
        );
    }

    #[test]
    fn test_parse_parenthesized_tql_only_eval_allowed() {
        let sql = "(TQL EXPLAIN http_requests_total)";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let result = ctx.parse_parenthesized_tql(false, false, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_parenthesized_tql_allow_non_eval_when_flag_false() {
        let sql = "(TQL EXPLAIN http_requests_total)";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();

        let (tql, raw) = ctx
            .parse_parenthesized_tql(false, false, false)
            .expect("should allow non-eval when flag is false");

        match tql {
            Tql::Explain(_) => {}
            _ => panic!("Expected TQL Explain variant"),
        }
        assert!(raw.contains("TQL EXPLAIN"));
        assert_eq!(ctx.parser.peek_token().token, Token::RParen);
    }
}
