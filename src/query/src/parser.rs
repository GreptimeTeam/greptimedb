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

use std::time::{Duration, SystemTime};

use chrono::DateTime;
use common_error::ext::PlainError;
use common_error::prelude::BoxedError;
use common_error::status_code::StatusCode;
use common_telemetry::timer;
use promql_parser::parser::EvalStmt;
use snafu::ResultExt;
use sql::dialect::GenericDialect;
use sql::parser::ParserContext;
use sql::statements::statement::Statement;

use crate::error::{
    MultipleStatementsSnafu, ParseFloatSnafu, ParseTimestampSnafu, QueryParseSnafu, Result,
};
use crate::metric::{METRIC_PARSE_PROMQL_ELAPSED, METRIC_PARSE_SQL_ELAPSED};

const DEFAULT_LOOKBACK: u64 = 5 * 60; // 5m

#[derive(Debug, Clone)]
pub enum QueryStatement {
    Sql(Statement),
    Promql(EvalStmt),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromQuery {
    pub query: String,
    pub start: String,
    pub end: String,
    pub step: String,
}

pub struct QueryLanguageParser {}

impl QueryLanguageParser {
    pub fn parse_sql(sql: &str) -> Result<QueryStatement> {
        let _timer = timer!(METRIC_PARSE_SQL_ELAPSED);
        let mut statement = ParserContext::create_with_dialect(sql, &GenericDialect {})
            .map_err(BoxedError::new)
            .context(QueryParseSnafu {
                query: sql.to_string(),
            })?;
        if statement.len() != 1 {
            MultipleStatementsSnafu {
                query: sql.to_string(),
            }
            .fail()
        } else {
            Ok(QueryStatement::Sql(statement.pop().unwrap()))
        }
    }

    // TODO(ruihang): implement this method when parser is ready.
    pub fn parse_promql(query: &PromQuery) -> Result<QueryStatement> {
        let _timer = timer!(METRIC_PARSE_PROMQL_ELAPSED);

        let expr = promql_parser::parser::parse(&query.query)
            .map_err(|msg| BoxedError::new(PlainError::new(msg, StatusCode::InvalidArguments)))
            .context(QueryParseSnafu {
                query: &query.query,
            })?;

        let start = Self::parse_promql_timestamp(&query.start)
            .map_err(BoxedError::new)
            .context(QueryParseSnafu {
                query: &query.query,
            })?;

        let end = Self::parse_promql_timestamp(&query.end)
            .map_err(BoxedError::new)
            .context(QueryParseSnafu {
                query: &query.query,
            })?;

        let step = query
            .step
            .parse::<u64>()
            .map(Duration::from_secs)
            .or_else(|_| promql_parser::util::parse_duration(&query.step))
            .map_err(|msg| BoxedError::new(PlainError::new(msg, StatusCode::InvalidArguments)))
            .context(QueryParseSnafu {
                query: &query.query,
            })?;

        let eval_stmt = EvalStmt {
            expr,
            start,
            end,
            interval: step,
            // TODO(ruihang): provide a way to adjust this parameter.
            lookback_delta: Duration::from_secs(DEFAULT_LOOKBACK),
        };

        Ok(QueryStatement::Promql(eval_stmt))
    }

    fn parse_promql_timestamp(timestamp: &str) -> Result<SystemTime> {
        // try rfc3339 format
        let rfc3339_result = DateTime::parse_from_rfc3339(timestamp)
            .context(ParseTimestampSnafu { raw: timestamp })
            .map(Into::<SystemTime>::into);

        // shorthand
        if rfc3339_result.is_ok() {
            return rfc3339_result;
        }

        // try float format
        timestamp
            .parse::<f64>()
            .context(ParseFloatSnafu { raw: timestamp })
            .map(|float| {
                let duration = Duration::from_secs_f64(float);
                SystemTime::UNIX_EPOCH
                    .checked_add(duration)
                    .unwrap_or(max_system_timestamp())
            })
            // also report rfc3339 error if float parsing fails
            .map_err(|_| rfc3339_result.unwrap_err())
    }
}

fn max_system_timestamp() -> SystemTime {
    SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_secs(std::i64::MAX as u64))
        .unwrap()
}

#[cfg(test)]
mod test {
    use super::*;

    // Detailed logic tests are covered in the parser crate.
    #[test]
    fn parse_sql_simple() {
        let sql = "select * from t1";
        let stmt = QueryLanguageParser::parse_sql(sql).unwrap();
        let expected = String::from("Sql(Query(Query { \
            inner: Query { \
                with: None, body: Select(Select { \
                    distinct: false, \
                    top: None, \
                    projection: \
                    [Wildcard(WildcardAdditionalOptions { opt_exclude: None, opt_except: None })], \
                    into: None, \
                    from: [TableWithJoins { relation: Table { name: ObjectName([Ident { value: \"t1\", quote_style: None }]\
                ), \
                alias: None, \
                args: None, \
                with_hints: [] \
            }, \
            joins: [] }], \
            lateral_views: [], \
            selection: None, \
            group_by: [], \
            cluster_by: [], \
            distribute_by: [], \
            sort_by: [], \
            having: None, \
            qualify: None \
            }), order_by: [], limit: None, offset: None, fetch: None, lock: None }, param_types: [] }))");

        assert_eq!(format!("{stmt:?}"), expected);
    }

    #[test]
    fn parse_promql_timestamp() {
        let cases = vec![
            (
                "1435781451.781",
                SystemTime::UNIX_EPOCH
                    .checked_add(Duration::from_secs_f64(1435781451.781))
                    .unwrap(),
            ),
            ("0.000", SystemTime::UNIX_EPOCH),
            ("00", SystemTime::UNIX_EPOCH),
            (
                // i64::MAX + 1
                "9223372036854775808.000",
                max_system_timestamp(),
            ),
            (
                "2015-07-01T20:10:51.781Z",
                SystemTime::UNIX_EPOCH
                    .checked_add(Duration::from_secs_f64(1435781451.781))
                    .unwrap(),
            ),
            ("1970-01-01T00:00:00.000Z", SystemTime::UNIX_EPOCH),
        ];

        for (input, expected) in cases {
            let result = QueryLanguageParser::parse_promql_timestamp(input).unwrap();

            let result = result
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let expected = expected
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis();

            // assert difference < 0.1 second
            assert!(result.abs_diff(expected) < 100);
        }
    }

    #[test]
    fn parse_promql_simple() {
        let promql = PromQuery {
            query: "http_request".to_string(),
            start: "2022-02-13T17:14:00Z".to_string(),
            end: "2023-02-13T17:14:00Z".to_string(),
            step: "1d".to_string(),
        };

        let expected = String::from(
            "\
            Promql(EvalStmt { \
                expr: VectorSelector(VectorSelector { \
                    name: Some(\"http_request\"), \
                    matchers: Matchers { \
                        matchers: {Matcher { \
                            op: Equal, \
                            name: \"__name__\", \
                            value: \"http_request\" \
                    }} }, \
                    offset: None, at: None }), \
                start: SystemTime { tv_sec: 1644772440, tv_nsec: 0 }, \
                end: SystemTime { tv_sec: 1676308440, tv_nsec: 0 }, \
                interval: 86400s, \
                lookback_delta: 300s \
            })",
        );

        let result = QueryLanguageParser::parse_promql(&promql).unwrap();
        assert_eq!(format!("{result:?}"), expected);
    }
}
