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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::DateTime;
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use promql_parser::parser::ast::{Extension as NodeExtension, ExtensionExpr};
use promql_parser::parser::Expr::Extension;
use promql_parser::parser::{EvalStmt, Expr, ValueType};
use snafu::{OptionExt, ResultExt};
use sql::dialect::GreptimeDbDialect;
use sql::parser::ParserContext;
use sql::statements::statement::Statement;

use crate::error::{
    AddSystemTimeOverflowSnafu, MultipleStatementsSnafu, ParseFloatSnafu, ParseTimestampSnafu,
    QueryParseSnafu, Result, UnimplementedSnafu,
};
use crate::metrics::{METRIC_PARSE_PROMQL_ELAPSED, METRIC_PARSE_SQL_ELAPSED};

const DEFAULT_LOOKBACK: u64 = 5 * 60; // 5m
pub const DEFAULT_LOOKBACK_STRING: &str = "5m";
pub const EXPLAIN_NODE_NAME: &str = "EXPLAIN";
pub const ANALYZE_NODE_NAME: &str = "ANALYZE";

#[derive(Debug, Clone)]
pub enum QueryStatement {
    Sql(Statement),
    Promql(EvalStmt),
}

impl QueryStatement {
    pub fn post_process(&self, params: HashMap<String, String>) -> Result<QueryStatement> {
        match self {
            QueryStatement::Sql(_) => UnimplementedSnafu {
                operation: "sql post process",
            }
            .fail(),
            QueryStatement::Promql(eval_stmt) => {
                let node_name = match params.get("name") {
                    Some(name) => name.as_str(),
                    None => "",
                };
                let extension_node = Self::create_extension_node(node_name, &eval_stmt.expr);
                Ok(QueryStatement::Promql(EvalStmt {
                    expr: Extension(extension_node.unwrap()),
                    start: eval_stmt.start,
                    end: eval_stmt.end,
                    interval: eval_stmt.interval,
                    lookback_delta: eval_stmt.lookback_delta,
                }))
            }
        }
    }

    fn create_extension_node(node_name: &str, expr: &Expr) -> Option<NodeExtension> {
        match node_name {
            ANALYZE_NODE_NAME => Some(NodeExtension {
                expr: Arc::new(AnalyzeExpr { expr: expr.clone() }),
            }),
            EXPLAIN_NODE_NAME => Some(NodeExtension {
                expr: Arc::new(ExplainExpr { expr: expr.clone() }),
            }),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromQuery {
    pub query: String,
    pub start: String,
    pub end: String,
    pub step: String,
}

impl Default for PromQuery {
    fn default() -> Self {
        PromQuery {
            query: String::new(),
            start: String::from("0"),
            end: String::from("0"),
            step: String::from("5m"),
        }
    }
}

pub struct QueryLanguageParser {}

impl QueryLanguageParser {
    pub fn parse_sql(sql: &str) -> Result<QueryStatement> {
        let _timer = METRIC_PARSE_SQL_ELAPSED.start_timer();
        let mut statement = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {})
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

    pub fn parse_promql(query: &PromQuery) -> Result<QueryStatement> {
        let _timer = METRIC_PARSE_PROMQL_ELAPSED.start_timer();

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
        let secs = timestamp
            .parse::<f64>()
            .context(ParseFloatSnafu { raw: timestamp })
            // also report rfc3339 error if float parsing fails
            .map_err(|_| rfc3339_result.unwrap_err())?;

        let duration = Duration::from_secs_f64(secs);
        SystemTime::UNIX_EPOCH
            .checked_add(duration)
            .context(AddSystemTimeOverflowSnafu { duration })
    }
}

macro_rules! define_node_ast_extension {
    ($name:ident, $name_expr:ident, $expr_type:ty, $extension_name:expr) => {
        /// The implementation of the `$name_expr` extension AST node
        #[derive(Debug, Clone)]
        pub struct $name_expr {
            pub expr: $expr_type,
        }

        impl ExtensionExpr for $name_expr {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $extension_name
            }

            fn value_type(&self) -> ValueType {
                self.expr.value_type()
            }

            fn children(&self) -> &[Expr] {
                std::slice::from_ref(&self.expr)
            }
        }

        #[allow(rustdoc::broken_intra_doc_links)]
        #[derive(Debug, Clone)]
        pub struct $name {
            pub expr: Arc<$name_expr>,
        }

        impl $name {
            pub fn new(expr: $expr_type) -> Self {
                Self {
                    expr: Arc::new($name_expr { expr }),
                }
            }
        }
    };
}

define_node_ast_extension!(Analyze, AnalyzeExpr, Expr, ANALYZE_NODE_NAME);
define_node_ast_extension!(Explain, ExplainExpr, Expr, EXPLAIN_NODE_NAME);

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
                    distinct: None, \
                    top: None, \
                    projection: \
                    [Wildcard(WildcardAdditionalOptions { opt_exclude: None, opt_except: None, opt_rename: None, opt_replace: None })], \
                    into: None, \
                    from: [TableWithJoins { relation: Table { name: ObjectName([Ident { value: \"t1\", quote_style: None }]\
                ), \
                alias: None, \
                args: None, \
                with_hints: [], \
                version: None, \
                partitions: [] \
            }, \
            joins: [] }], \
            lateral_views: [], \
            selection: None, \
            group_by: Expressions([]), \
            cluster_by: [], \
            distribute_by: [], \
            sort_by: [], \
            having: None, \
            named_window: [], \
            qualify: None \
            }), order_by: [], limit: None, offset: None, fetch: None, locks: [] } }))");

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

        // i64::MAX + 1
        let timestamp = "9223372036854775808.000";
        let result = QueryLanguageParser::parse_promql_timestamp(timestamp);
        assert_eq!(
            result.unwrap_err().to_string(),
            "Failed to add duration '9223372036854775808s' to SystemTime, overflowed"
        );
    }

    #[test]
    fn parse_promql_simple() {
        let promql = PromQuery {
            query: "http_request".to_string(),
            start: "2022-02-13T17:14:00Z".to_string(),
            end: "2023-02-13T17:14:00Z".to_string(),
            step: "1d".to_string(),
        };

        #[cfg(not(windows))]
        let expected = String::from(
            "\
            Promql(EvalStmt { \
                expr: VectorSelector(VectorSelector { \
                    name: Some(\"http_request\"), \
                    matchers: Matchers { \
                        matchers: [] }, \
                    offset: None, at: None }), \
                start: SystemTime { tv_sec: 1644772440, tv_nsec: 0 }, \
                end: SystemTime { tv_sec: 1676308440, tv_nsec: 0 }, \
                interval: 86400s, \
                lookback_delta: 300s \
            })",
        );

        // Windows has different debug output for SystemTime.
        #[cfg(windows)]
        let expected = String::from(
            "\
            Promql(EvalStmt { \
                expr: VectorSelector(VectorSelector { \
                    name: Some(\"http_request\"), \
                    matchers: Matchers { matchers: [] }, \
                    offset: None, at: None }), \
                start: SystemTime { intervals: 132892460400000000 }, \
                end: SystemTime { intervals: 133207820400000000 }, \
                interval: 86400s, \
                lookback_delta: 300s \
            })",
        );

        let result = QueryLanguageParser::parse_promql(&promql).unwrap();
        assert_eq!(format!("{result:?}"), expected);
    }
}
