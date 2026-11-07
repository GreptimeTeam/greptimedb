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

//! Prometheus exemplar selectors and exact nanosecond timestamps.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use promql_parser::label::{METRIC_NAME, MatchOp, Matcher};
use promql_parser::parser::{Expr, VectorSelector};
use promql_parser::util::{ExprVisitor, walk_expr};
use serde::{Deserialize, Serialize};

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
    /// Inclusive bounds, floored to nanoseconds before checking their order.
    pub bounds: (i64, i64),
}

impl Query {
    pub fn parse(expression: String, start: &str, end: &str, schema: &str) -> Result<Self> {
        let start = Decimal::timestamp(start)?.round_ns(false)?;
        let end = Decimal::timestamp(end)?.round_ns(false)?;
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
            bounds: (start, end),
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
    pub timestamp: ExactTimestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExactTimestamp(pub i64);

impl Serialize for ExactTimestamp {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let raw = serde_json::value::RawValue::from_string(format_ns(self.0))
            .map_err(serde::ser::Error::custom)?;
        raw.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ExactTimestamp {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
        let value = Decimal::timestamp(raw.get()).map_err(serde::de::Error::custom)?;
        let low = value.round_ns(true).map_err(serde::de::Error::custom)?;
        let high = value.round_ns(false).map_err(serde::de::Error::custom)?;
        if low != high {
            return Err(serde::de::Error::custom(
                "timestamp is finer than one nanosecond",
            ));
        }
        Ok(Self(low))
    }
}

fn format_ns(value: i64) -> String {
    let magnitude = value.unsigned_abs();
    let seconds = magnitude / 1_000_000_000;
    let fraction = magnitude % 1_000_000_000;
    let sign = if value < 0 { "-" } else { "" };
    if fraction == 0 {
        format!("{sign}{seconds}")
    } else {
        format!("{sign}{seconds}.{fraction:09}")
            .trim_end_matches('0')
            .to_string()
    }
}

/// Normalized decimal seconds. Comparison never rounds or allocates exponent-sized strings.
struct Decimal {
    negative: bool,
    digits: String,
    exponent: i64,
}

impl Decimal {
    fn timestamp(input: &str) -> Result<Self> {
        if let Ok(time) = chrono::DateTime::parse_from_rfc3339(input) {
            let nanos = time.timestamp_nanos_opt().ok_or_else(|| {
                InvalidQuerySnafu {
                    reason: "timestamp exceeds signed nanosecond range",
                }
                .build()
            })?;
            return Self::parse(&format_ns(nanos));
        }
        let value = Self::parse(input)?;
        // Flooring can discard a fraction above i64::MAX, matching RFC3339 parsing.
        if value.cmp(&Self::parse("-9223372036.854775808")?).is_lt()
            || value.cmp(&Self::parse("9223372036.854775808")?).is_ge()
        {
            return invalid("timestamp exceeds signed nanosecond range");
        }
        Ok(value)
    }

    fn parse(input: &str) -> Result<Self> {
        let (negative, input) = match input.as_bytes().first() {
            Some(b'-') => (true, &input[1..]),
            Some(b'+') => (false, &input[1..]),
            _ => (false, input),
        };
        let (mantissa, exponent) = match input.split_once(['e', 'E']) {
            Some((mantissa, exponent)) => (
                mantissa,
                exponent.parse::<i64>().map_err(|_| {
                    InvalidQuerySnafu {
                        reason: "invalid timestamp exponent",
                    }
                    .build()
                })?,
            ),
            None => (input, 0),
        };
        let mut digits = String::new();
        let mut fraction = None;
        for character in mantissa.bytes() {
            match character {
                b'0'..=b'9' => {
                    digits.push(character as char);
                    if let Some(count) = &mut fraction {
                        *count += 1;
                    }
                }
                b'.' if fraction.is_none() => fraction = Some(0i64),
                _ => return invalid("invalid numeric timestamp"),
            }
        }
        if digits.is_empty() {
            return invalid("timestamp is required");
        }
        let mut exponent = exponent.checked_sub(fraction.unwrap_or(0)).ok_or_else(|| {
            InvalidQuerySnafu {
                reason: "timestamp exponent overflow",
            }
            .build()
        })?;
        let mut digits = digits.trim_start_matches('0').to_string();
        if digits.is_empty() {
            return Ok(Self {
                negative: false,
                digits: "0".to_string(),
                exponent: 0,
            });
        }
        let trailing = digits.len() - digits.trim_end_matches('0').len();
        exponent = exponent.checked_add(trailing as i64).ok_or_else(|| {
            InvalidQuerySnafu {
                reason: "timestamp exponent overflow",
            }
            .build()
        })?;
        digits.truncate(digits.len() - trailing);
        Ok(Self {
            negative,
            digits,
            exponent,
        })
    }

    fn cmp(&self, other: &Self) -> Ordering {
        if self.negative != other.negative {
            return other.negative.cmp(&self.negative);
        }
        let order = match (self.digits == "0", other.digits == "0") {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            _ => {
                let positions = (self.digits.len() as i128 + self.exponent as i128)
                    .cmp(&(other.digits.len() as i128 + other.exponent as i128));
                positions.then_with(|| {
                    self.digits
                        .bytes()
                        .chain(std::iter::repeat(b'0'))
                        .take(self.digits.len().max(other.digits.len()))
                        .cmp(
                            other
                                .digits
                                .bytes()
                                .chain(std::iter::repeat(b'0'))
                                .take(self.digits.len().max(other.digits.len())),
                        )
                })
            }
        };
        if self.negative {
            order.reverse()
        } else {
            order
        }
    }

    fn round_ns(&self, ceil: bool) -> Result<i64> {
        if self.digits == "0" {
            return Ok(0);
        }
        let position = self.digits.len() as i128 + self.exponent as i128 + 9;
        let integer_len = position.max(0).min(self.digits.len() as i128) as usize;
        let integer = if integer_len == 0 {
            0
        } else {
            self.digits[..integer_len].parse::<i128>().map_err(|_| {
                InvalidQuerySnafu {
                    reason: "timestamp overflow",
                }
                .build()
            })?
        };
        let zeros = (position - integer_len as i128).max(0);
        if zeros > 19 {
            return invalid("timestamp overflow");
        }
        let magnitude = integer * 10i128.pow(zeros as u32);
        let fractional = integer_len < self.digits.len();
        let value = if self.negative {
            -magnitude - i128::from(fractional && !ceil)
        } else {
            magnitude + i128::from(fractional && ceil)
        };
        i64::try_from(value).map_err(|_| {
            InvalidQuerySnafu {
                reason: "timestamp overflow",
            }
            .build()
        })
    }
}

fn invalid<T>(reason: &str) -> Result<T> {
    InvalidQuerySnafu { reason }.fail()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_bounds_and_serialization() {
        for (input, lower, upper) in [
            ("1.234567891", 1234567891, 1234567891),
            ("-1e-10", 0, -1),
            ("1e-10", 1, 0),
            (
                "1720000000.123456789",
                1720000000123456789,
                1720000000123456789,
            ),
            ("1969-12-31T23:59:59.999999999Z", -1, -1),
        ] {
            let value = Decimal::timestamp(input).unwrap();
            assert_eq!(value.round_ns(true).unwrap(), lower);
            assert_eq!(value.round_ns(false).unwrap(), upper);
        }
        for value in [i64::MIN, -1, 0, 1, 1720000000123456789, i64::MAX] {
            let json = serde_json::to_string(&ExactTimestamp(value)).unwrap();
            assert_eq!(
                serde_json::from_str::<ExactTimestamp>(&json).unwrap().0,
                value
            );
            assert!(!json.contains('"'));
        }
        for input in ["1e-10", "-1e-10", "9223372036.8547758079"] {
            assert!(
                serde_json::from_str::<ExactTimestamp>(input).is_err(),
                "{input}"
            );
        }
        for invalid in [
            "NaN",
            "inf",
            "",
            "1e99999999999999999999999",
            "1e100",
            "9223372036.854775808",
            "-9223372036.854775809",
            "-9223372036.8547758081",
        ] {
            assert!(Decimal::timestamp(invalid).is_err(), "{invalid}");
        }
        assert!(
            Query::parse("1".into(), "0", "1", "public")
                .unwrap()
                .selectors
                .is_empty()
        );
    }

    #[test]
    fn query_bounds_drop_subnanosecond_precision() {
        for (numeric, rfc3339, expected) in [
            ("1e-10", "1970-01-01T00:00:00.0000000001Z", 0),
            ("-1e-10", "1969-12-31T23:59:59.9999999999Z", -1),
            ("0.0000000019", "1970-01-01T00:00:00.0000000019Z", 1),
            ("-0.0000000019", "1969-12-31T23:59:59.9999999981Z", -2),
            ("1e-10", "1970-01-01T01:00:00.0000000001+01:00", 0),
            (
                "-9223372036.854775808",
                "1677-09-21T00:12:43.145224192Z",
                i64::MIN,
            ),
            (
                "9223372036.8547758079",
                "2262-04-11T23:47:16.8547758079Z",
                i64::MAX,
            ),
        ] {
            for input in [numeric, rfc3339] {
                assert_eq!(
                    Query::parse("up".into(), input, input, "public")
                        .unwrap()
                        .bounds,
                    (expected, expected),
                    "{input}",
                );
            }
        }
        for (start, end, expected) in [
            ("0.0000000001", "0.0000000002", (0, 0)),
            ("0.0000000002", "0.0000000001", (0, 0)),
            ("-0.0000000001", "-0.0000000002", (-1, -1)),
            ("-0.0000000001", "0.0000000001", (-1, 0)),
        ] {
            assert_eq!(
                Query::parse("up".into(), start, end, "public")
                    .unwrap()
                    .bounds,
                expected,
            );
        }
        assert!(Query::parse("up".into(), "0.000000001", "0.0000000009", "public").is_err());
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
