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

//! Prometheus exemplar selectors and millisecond API timestamps.

use std::collections::BTreeMap;

use promql_parser::label::{METRIC_NAME, MatchOp, Matcher};
use promql_parser::parser::{Expr, VectorSelector};
use promql_parser::util::{ExprVisitor, walk_expr};
use query::parser::QueryLanguageParser;
use query::promql::label_values::signed_millis_since_epoch;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::error::{InvalidQuerySnafu, Result};
use crate::prom_store::{FIELD_NAME_LABEL, is_database_selection_label};
use crate::prometheus_handler::resolve_schema_from_matchers;

#[derive(Debug, Clone)]
pub struct Selector {
    pub schema: String,
    pub matchers: Vec<Matcher>,
}

#[derive(Debug)]
pub struct Query {
    pub expression: String,
    pub selectors: Vec<Selector>,
    /// Inclusive nanosecond scan bounds covering the requested milliseconds.
    pub bounds: (i64, i64),
}

impl Query {
    pub fn parse(expression: String, start: &str, end: &str, schema: &str) -> Result<Self> {
        let parse = |input| {
            let time = QueryLanguageParser::parse_promql_timestamp(input).map_err(|error| {
                InvalidQuerySnafu {
                    reason: error.to_string(),
                }
                .build()
            })?;
            let millis = signed_millis_since_epoch(time).map_err(|error| {
                InvalidQuerySnafu {
                    reason: error.to_string(),
                }
                .build()
            })?;
            millis.checked_mul(1_000_000).context(InvalidQuerySnafu {
                reason: "timestamp exceeds signed nanosecond range",
            })
        };
        let start = parse(start)?;
        let end = parse(end)?;
        if start > end {
            return invalid("start must not exceed end");
        }
        let expr = promql_parser::parser::parse(&expression)
            .map_err(|reason| InvalidQuerySnafu { reason }.build())?;
        struct Collector<'a> {
            schema: &'a str,
            selectors: Vec<Selector>,
        }
        impl ExprVisitor for Collector<'_> {
            type Error = crate::error::Error;
            fn pre_visit(&mut self, expr: &Expr) -> Result<bool> {
                let selector = match expr {
                    Expr::VectorSelector(vs) => vs,
                    Expr::MatrixSelector(ms) => &ms.vs,
                    Expr::Aggregate(aggregate) => {
                        // promql-parser 0.10's walker omits aggregation parameters.
                        if let Some(param) = &aggregate.param {
                            walk_expr(self, param)?;
                        }
                        return Ok(true);
                    }
                    _ => return Ok(true),
                };
                self.selectors.extend(resolve(selector, self.schema)?);
                Ok(true)
            }
        }
        let mut collector = Collector {
            schema,
            selectors: Vec::new(),
        };
        walk_expr(&mut collector, &expr)?;
        Ok(Self {
            expression,
            selectors: collector.selectors,
            // Include the whole final millisecond without casting the scanned column.
            bounds: (start, end.saturating_add(999_999)),
        })
    }
}

fn resolve(selector: &VectorSelector, default_schema: &str) -> Result<Vec<Selector>> {
    let branches = if selector.matchers.or_matchers.is_empty() {
        vec![&selector.matchers.matchers]
    } else {
        selector.matchers.or_matchers.iter().collect()
    };
    branches
        .into_iter()
        .map(|branch| {
            if branch
                .iter()
                .any(|matcher| matcher.name == FIELD_NAME_LABEL)
            {
                return invalid("__field__ is not supported in exemplar queries");
            }
            let schema =
                resolve_schema_from_matchers(branch)?.unwrap_or_else(|| default_schema.to_string());
            let mut matchers = branch
                .iter()
                .filter(|matcher| !is_database_selection_label(&matcher.name))
                .cloned()
                .collect::<Vec<_>>();
            if let Some(name) = &selector.name {
                matchers.push(Matcher::new(MatchOp::Equal, METRIC_NAME, name));
            }
            Ok(Selector { schema, matchers })
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Series {
    #[serde(rename = "seriesLabels")]
    pub series_labels: BTreeMap<String, String>,
    pub exemplars: Vec<Observation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Observation {
    pub labels: BTreeMap<String, String>,
    pub value: String,
    /// Unix seconds at millisecond precision.
    pub timestamp: f64,
}

fn invalid<T>(reason: &str) -> Result<T> {
    InvalidQuerySnafu { reason }.fail()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_bounds_use_prometheus_milliseconds() {
        for (input, millis) in [
            ("1.234567891", 1235),
            ("1.234e0", 1234),
            ("1720000000.123456789", 1720000000123),
            ("-0.0006", -1),
            ("-0.0004", 0),
            ("-1.0005", -1000),
            ("1970-01-01T00:00:00.0019Z", 1),
            ("1969-12-31T23:59:59.999999999Z", -1),
            ("1970-01-01T01:00:00.0019+01:00", 1),
        ] {
            assert_eq!(
                Query::parse("up".into(), input, input, "public")
                    .unwrap()
                    .bounds,
                (millis * 1_000_000, millis * 1_000_000 + 999_999),
                "{input}",
            );
        }
        assert_eq!(
            Query::parse("up".into(), "0.0012", "0.0011", "public")
                .unwrap()
                .bounds,
            (1_000_000, 1_999_999),
        );
        assert_eq!(
            Query::parse(
                "up".into(),
                "2262-04-11T23:47:16.854Z",
                "2262-04-11T23:47:16.854Z",
                "public"
            )
            .unwrap()
            .bounds,
            (9_223_372_036_854_000_000, i64::MAX),
        );
        assert!(Query::parse("up".into(), "0.002", "0.001", "public").is_err());
        for input in [
            "NaN",
            "inf",
            "",
            "1e100",
            "9223372036.855",
            "-9223372036.855",
        ] {
            assert!(
                Query::parse("up".into(), input, input, "public").is_err(),
                "{input}"
            );
        }
        assert!(
            Query::parse("1".into(), "0", "1", "public")
                .unwrap()
                .selectors
                .is_empty()
        );
    }

    #[test]
    fn aggregation_parameter_selectors() {
        for (expression, expected) in [
            (
                "topk(scalar(limit_metric), requests)",
                vec!["limit_metric", "requests"],
            ),
            (
                "bottomk(scalar(limit_metric), requests)",
                vec!["limit_metric", "requests"],
            ),
            (
                "quantile(scalar(limit_metric), requests)",
                vec!["limit_metric", "requests"],
            ),
            (
                "topk(scalar(topk(scalar(inner_limit), limit_metric)), requests)",
                vec!["inner_limit", "limit_metric", "requests"],
            ),
            ("topk(2, requests)", vec!["requests"]),
            ("count_values(\"kind\", requests)", vec!["requests"]),
            (
                "topk(scalar(requests), requests)",
                vec!["requests", "requests"],
            ),
        ] {
            let query = Query::parse(expression.into(), "0", "1", "public").unwrap();
            let mut names = query
                .selectors
                .iter()
                .flat_map(|selector| &selector.matchers)
                .filter(|matcher| matcher.name == METRIC_NAME)
                .map(|matcher| matcher.value.as_str())
                .collect::<Vec<_>>();
            names.sort_unstable();
            assert_eq!(names, expected, "{expression}");
        }
    }

    #[test]
    fn range_vectors_and_routing_controls() {
        let query = Query::parse(
            "topk(scalar(sum(rate(requests{__schema__=\"a\"}[5m]))), errors{__database__=\"b\"})"
                .into(),
            "0",
            "1",
            "public",
        )
        .unwrap();
        assert_eq!(
            query
                .selectors
                .iter()
                .map(|s| s.schema.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        for query in [
            "up{__schema__=\"\"}",
            "up{__schema__!=\"a\"}",
            "up{__field__=\"x\"}",
            "topk(scalar(up{__schema__=\"\"}), requests)",
            "topk(scalar(up{__schema__!=\"a\"}), requests)",
            "topk(scalar(up{__field__=\"x\"}), requests)",
        ] {
            assert!(Query::parse(query.into(), "0", "1", "public").is_err());
        }
    }
}
