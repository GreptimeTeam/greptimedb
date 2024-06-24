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

use std::collections::HashMap;
use std::ops::ControlFlow;

use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use sqlparser::ast::{Expr, Interval, Value};

use crate::statements::transform::TransformRule;

lazy_static! {
    /// Matches either one or more digits `(\d+)` or one or more ASCII characters `[a-zA-Z]` or plus/minus signs
    static ref INTERVAL_SHORT_NAME_PATTERN: Regex = Regex::new(r"([+-]?\d+|[a-zA-Z]+|\+|-)").unwrap();

    static ref INTERVAL_SHORT_NAME_MAPPING: HashMap<&'static str, &'static str> = HashMap::from([
        ("y","years"),
        ("mon","months"),
        ("w","weeks"),
        ("d","days"),
        ("h","hours"),
        ("m","minutes"),
        ("s","seconds"),
        ("millis","milliseconds"),
        ("mils","milliseconds"),
        ("ms","microseconds"),
        ("us","microseconds"),
        ("ns","nanoseconds"),
    ]);
}

/// 'Interval' expression transformer
/// - `y` for `years`
/// - `mon` for `months`
/// - `w` for `weeks`
/// - `d` for `days`
/// - `h` for `hours`
/// - `m` for `minutes`
/// - `s` for `seconds`
/// - `millis` for `milliseconds`
/// - `mils` for `milliseconds`
/// - `ms` for `microseconds`
/// - `us` for `microseconds`
/// - `ns` for `nanoseconds`
/// Required for use cases that use the shortened version of Interval declaration,
/// f.e `select interval '1h'` or `select interval '3w'`
pub(crate) struct ExpandIntervalTransformRule;

impl TransformRule for ExpandIntervalTransformRule {
    /// Applies transform rule for `Interval` type by extending the shortened version (e.g. '1h', '2d')
    /// In case when `Interval` has `BinaryOp` value (e.g. query like `SELECT INTERVAL '2h' - INTERVAL '1h'`)
    /// it's AST has `left` part of type `Value::SingleQuotedString` which needs to be handled specifically.
    /// To handle the `right` part which is `Interval` no extra steps are needed.
    fn visit_expr(&self, expr: &mut Expr) -> ControlFlow<()> {
        if let Expr::Interval(interval) = expr {
            match *interval.value.clone() {
                Expr::Value(Value::SingleQuotedString(value))
                | Expr::Value(Value::DoubleQuotedString(value)) => {
                    if let Some(data) = expand_interval_name(&value) {
                        *expr = create_interval_with_expanded_name(
                            interval,
                            single_quoted_string_expr(data),
                        );
                    }
                }
                Expr::BinaryOp { left, op, right } => match *left {
                    Expr::Value(Value::SingleQuotedString(value))
                    | Expr::Value(Value::DoubleQuotedString(value)) => {
                        if let Some(data) = expand_interval_name(&value) {
                            let new_value = Box::new(Expr::BinaryOp {
                                left: single_quoted_string_expr(data),
                                op,
                                right,
                            });
                            *expr = create_interval_with_expanded_name(interval, new_value);
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }
        ControlFlow::<()>::Continue(())
    }
}

fn single_quoted_string_expr(data: String) -> Box<Expr> {
    Box::new(Expr::Value(Value::SingleQuotedString(data)))
}

fn create_interval_with_expanded_name(interval: &Interval, new_value: Box<Expr>) -> Expr {
    Expr::Interval(Interval {
        value: new_value,
        leading_field: interval.leading_field.clone(),
        leading_precision: interval.leading_precision,
        last_field: interval.last_field.clone(),
        fractional_seconds_precision: interval.fractional_seconds_precision,
    })
}

/// Expands a shortened interval name to its full name.
/// Returns an interval's full name (e.g., "years", "hours", "minutes") according to `INTERVAL_SHORT_NAME_MAPPING` mapping
/// If the `interval_str` contains whitespaces, the interval name is considered to be in a full form.
/// Hybrid format "1y 2 days 3h" is not supported.
fn expand_interval_name(interval_str: &str) -> Option<String> {
    return if !interval_str.contains(|c: char| c.is_whitespace()) {
        Some(
            INTERVAL_SHORT_NAME_PATTERN
                .find_iter(interval_str)
                .map(|mat| match INTERVAL_SHORT_NAME_MAPPING.get(mat.as_str()) {
                    Some(&expanded_name) => expanded_name,
                    None => mat.as_str(),
                })
                .join(" "),
        )
    } else {
        None
    };
}

#[cfg(test)]
mod tests {
    use std::ops::ControlFlow;

    use sqlparser::ast::{BinaryOperator, Expr, Interval, Value};

    use crate::statements::transform::expand_interval::{
        expand_interval_name, single_quoted_string_expr, ExpandIntervalTransformRule,
    };
    use crate::statements::transform::TransformRule;

    #[test]
    fn test_transform_interval_basic_conversions() {
        let test_cases = vec![
            ("1y", "1 years"),
            ("4mon", "4 months"),
            ("-3w", "-3 weeks"),
            ("55h", "55 hours"),
            ("3d", "3 days"),
            ("5s", "5 seconds"),
            ("2m", "2 minutes"),
            ("100millis", "100 milliseconds"),
            ("150mils", "150 milliseconds"),
            ("200ms", "200 microseconds"),
            ("350us", "350 microseconds"),
            ("400ns", "400 nanoseconds"),
        ];
        for (input, expected) in test_cases {
            let result = expand_interval_name(input).unwrap();
            assert_eq!(result, expected);
        }

        let test_cases = vec!["1 year 2 months 3 days 4 hours", "-2 months"];
        for input in test_cases {
            assert_eq!(expand_interval_name(input), None);
        }
    }

    #[test]
    fn test_transform_interval_compound_conversions() {
        let test_cases = vec![
            ("2y4mon6w", "2 years 4 months 6 weeks"),
            ("5d3h1m", "5 days 3 hours 1 minutes"),
            (
                "10s312millis789ms",
                "10 seconds 312 milliseconds 789 microseconds",
            ),
            (
                "23mils987us754ns",
                "23 milliseconds 987 microseconds 754 nanoseconds",
            ),
            ("-1d-5h", "-1 days -5 hours"),
            ("-2y-4mon-6w", "-2 years -4 months -6 weeks"),
            ("-5d-3h-1m", "-5 days -3 hours -1 minutes"),
            (
                "-10s-312millis-789ms",
                "-10 seconds -312 milliseconds -789 microseconds",
            ),
            (
                "-23mils-987us-754ns",
                "-23 milliseconds -987 microseconds -754 nanoseconds",
            ),
        ];
        for (input, expected) in test_cases {
            let result = expand_interval_name(input).unwrap();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_visit_expr_when_interval_is_single_quoted_string_expr() {
        let interval_transformation_rule = ExpandIntervalTransformRule {};

        let mut string_expr = Expr::Interval(Interval {
            value: single_quoted_string_expr("5y".to_string()),
            leading_field: None,
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        });

        let control_flow = interval_transformation_rule.visit_expr(&mut string_expr);

        assert_eq!(control_flow, ControlFlow::Continue(()));
        assert_eq!(
            string_expr,
            Expr::Interval(Interval {
                value: Box::new(Expr::Value(Value::SingleQuotedString(
                    "5 years".to_string()
                ))),
                leading_field: None,
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            })
        );
    }

    #[test]
    fn test_visit_expr_when_interval_is_binary_op() {
        let interval_transformation_rule = ExpandIntervalTransformRule {};

        let mut binary_op_expr = Expr::Interval(Interval {
            value: Box::new(Expr::BinaryOp {
                left: single_quoted_string_expr("2d".to_string()),
                op: BinaryOperator::Minus,
                right: Box::new(Expr::Interval(Interval {
                    value: single_quoted_string_expr("1d".to_string()),
                    leading_field: None,
                    leading_precision: None,
                    last_field: None,
                    fractional_seconds_precision: None,
                })),
            }),
            leading_field: None,
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        });

        let control_flow = interval_transformation_rule.visit_expr(&mut binary_op_expr);

        assert_eq!(control_flow, ControlFlow::Continue(()));
        assert_eq!(
            binary_op_expr,
            Expr::Interval(Interval {
                value: Box::new(Expr::BinaryOp {
                    left: single_quoted_string_expr("2 days".to_string()),
                    op: BinaryOperator::Minus,
                    right: Box::new(Expr::Interval(Interval {
                        value: single_quoted_string_expr("1d".to_string()),
                        leading_field: None,
                        leading_precision: None,
                        last_field: None,
                        fractional_seconds_precision: None,
                    })),
                }),
                leading_field: None,
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            })
        );
    }
}
