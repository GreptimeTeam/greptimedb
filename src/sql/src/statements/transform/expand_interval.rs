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
use std::time::Duration as StdDuration;

use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use sqlparser::ast::{DataType, Expr, Interval, Value, ValueWithSpan};

use crate::statements::transform::TransformRule;

lazy_static! {
    /// Matches either one or more digits `(\d+)` or one or more ASCII characters `[a-zA-Z]` or plus/minus signs
    static ref INTERVAL_ABBREVIATION_PATTERN: Regex = Regex::new(r"([+-]?\d+|[a-zA-Z]+|\+|-)").unwrap();

    /// Checks if the provided string starts as ISO_8601 format string (case/sign independent)
    static ref IS_VALID_ISO_8601_PREFIX_PATTERN: Regex = Regex::new(r"^[-]?[Pp]").unwrap();

    static ref INTERVAL_ABBREVIATION_MAPPING: HashMap<&'static str, &'static str> = HashMap::from([
        ("y","years"),
        ("mon","months"),
        ("w","weeks"),
        ("d","days"),
        ("h","hours"),
        ("m","minutes"),
        ("s","seconds"),
        ("millis","milliseconds"),
        ("ms","milliseconds"),
        ("us","microseconds"),
        ("ns","nanoseconds"),
    ]);
}

/// 'INTERVAL' abbreviation transformer
/// - `y` for `years`
/// - `mon` for `months`
/// - `w` for `weeks`
/// - `d` for `days`
/// - `h` for `hours`
/// - `m` for `minutes`
/// - `s` for `seconds`
/// - `millis` for `milliseconds`
/// - `ms` for `milliseconds`
/// - `us` for `microseconds`
/// - `ns` for `nanoseconds`
///
/// Required for scenarios that use the shortened version of `INTERVAL`,
///   f.e `SELECT INTERVAL '1h'` or `SELECT INTERVAL '3w2d'`
pub(crate) struct ExpandIntervalTransformRule;

impl TransformRule for ExpandIntervalTransformRule {
    /// Applies transform rule for `Interval` type by extending the shortened version (e.g. '1h', '2d') or
    /// converting ISO 8601 format strings (e.g., "P1Y2M3D")
    /// In case when `Interval` has `BinaryOp` value (e.g. query like `SELECT INTERVAL '2h' - INTERVAL '1h'`)
    /// it's AST has `left` part of type `Value::SingleQuotedString` which needs to be handled specifically.
    /// To handle the `right` part which is `Interval` no extra steps are needed.
    fn visit_expr(&self, expr: &mut Expr) -> ControlFlow<()> {
        match expr {
            Expr::Interval(interval) => match &*interval.value {
                Expr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString(value),
                    ..
                })
                | Expr::Value(ValueWithSpan {
                    value: Value::DoubleQuotedString(value),
                    ..
                }) => {
                    if let Some(normalized_name) = normalize_interval_name(value) {
                        *expr = update_existing_interval_with_value(
                            interval,
                            single_quoted_string_expr(normalized_name),
                        );
                    }
                }
                Expr::BinaryOp { left, op, right } => match &**left {
                    Expr::Value(ValueWithSpan {
                        value: Value::SingleQuotedString(value),
                        ..
                    })
                    | Expr::Value(ValueWithSpan {
                        value: Value::DoubleQuotedString(value),
                        ..
                    }) => {
                        if let Some(normalized_name) = normalize_interval_name(value) {
                            let new_expr_value = Box::new(Expr::BinaryOp {
                                left: single_quoted_string_expr(normalized_name),
                                op: op.clone(),
                                right: right.clone(),
                            });
                            *expr = update_existing_interval_with_value(interval, new_expr_value);
                        }
                    }
                    _ => {}
                },
                _ => {}
            },
            Expr::Cast {
                expr: cast_exp,
                data_type,
                kind,
                format,
            } => {
                if DataType::Interval == *data_type {
                    match &**cast_exp {
                        Expr::Value(ValueWithSpan {
                            value: Value::SingleQuotedString(value),
                            ..
                        })
                        | Expr::Value(ValueWithSpan {
                            value: Value::DoubleQuotedString(value),
                            ..
                        }) => {
                            let interval_value =
                                normalize_interval_name(value).unwrap_or_else(|| value.to_string());
                            *expr = Expr::Cast {
                                kind: kind.clone(),
                                expr: single_quoted_string_expr(interval_value),
                                data_type: DataType::Interval,
                                format: std::mem::take(format),
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    }
}

fn single_quoted_string_expr(string: String) -> Box<Expr> {
    Box::new(Expr::Value(Value::SingleQuotedString(string).into()))
}

fn update_existing_interval_with_value(interval: &Interval, value: Box<Expr>) -> Expr {
    Expr::Interval(Interval {
        value,
        leading_field: interval.leading_field.clone(),
        leading_precision: interval.leading_precision,
        last_field: interval.last_field.clone(),
        fractional_seconds_precision: interval.fractional_seconds_precision,
    })
}

/// Normalizes an interval expression string into the sql-compatible format.
/// This function handles 2 types of input:
/// 1. Abbreviated interval strings (e.g., "1y2mo3d")
///    Returns an interval's full name (e.g., "years", "hours", "minutes") according to the `INTERVAL_ABBREVIATION_MAPPING`
///    If the `interval_str` contains whitespaces, the interval name is considered to be in a full form.
/// 2. ISO 8601 format strings (e.g., "P1Y2M3D"), case/sign independent
///    Returns a number of milliseconds corresponding to ISO 8601 (e.g., "36525000 milliseconds")
///
/// Note: Hybrid format "1y 2 days 3h" is not supported.
fn normalize_interval_name(interval_str: &str) -> Option<String> {
    if interval_str.contains(char::is_whitespace) {
        return None;
    }

    if IS_VALID_ISO_8601_PREFIX_PATTERN.is_match(interval_str) {
        return parse_iso8601_interval(interval_str);
    }

    expand_interval_abbreviation(interval_str)
}

fn parse_iso8601_interval(signed_iso: &str) -> Option<String> {
    let (is_negative, unsigned_iso) = if let Some(stripped) = signed_iso.strip_prefix('-') {
        (true, stripped)
    } else {
        (false, signed_iso)
    };

    match iso8601::duration(&unsigned_iso.to_uppercase()) {
        Ok(duration) => {
            let millis = StdDuration::from(duration).as_millis();
            let sign = if is_negative { "-" } else { "" };
            Some(format!("{}{} milliseconds", sign, millis))
        }
        Err(_) => None,
    }
}

fn expand_interval_abbreviation(interval_str: &str) -> Option<String> {
    Some(
        INTERVAL_ABBREVIATION_PATTERN
            .find_iter(interval_str)
            .map(|mat| {
                let mat_str = mat.as_str();
                *INTERVAL_ABBREVIATION_MAPPING
                    .get(mat_str)
                    .unwrap_or(&mat_str)
            })
            .join(" "),
    )
}

#[cfg(test)]
mod tests {
    use std::ops::ControlFlow;

    use sqlparser::ast::{BinaryOperator, CastKind, DataType, Expr, Interval, Value};

    use crate::statements::transform::expand_interval::{
        normalize_interval_name, single_quoted_string_expr, ExpandIntervalTransformRule,
    };
    use crate::statements::transform::TransformRule;

    fn create_interval(value: Box<Expr>) -> Expr {
        Expr::Interval(Interval {
            value,
            leading_field: None,
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        })
    }

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
            ("200ms", "200 milliseconds"),
            ("350us", "350 microseconds"),
            ("400ns", "400 nanoseconds"),
        ];
        for (input, expected) in test_cases {
            let result = normalize_interval_name(input).unwrap();
            assert_eq!(result, expected);
        }

        let test_cases = vec!["1 year 2 months 3 days 4 hours", "-2 months"];
        for input in test_cases {
            assert_eq!(normalize_interval_name(input), None);
        }
    }

    #[test]
    fn test_transform_interval_compound_conversions() {
        let test_cases = vec![
            ("2y4mon6w", "2 years 4 months 6 weeks"),
            ("5d3h1m", "5 days 3 hours 1 minutes"),
            (
                "10s312ms789ns",
                "10 seconds 312 milliseconds 789 nanoseconds",
            ),
            (
                "23millis987us754ns",
                "23 milliseconds 987 microseconds 754 nanoseconds",
            ),
            ("-1d-5h", "-1 days -5 hours"),
            ("-2y-4mon-6w", "-2 years -4 months -6 weeks"),
            ("-5d-3h-1m", "-5 days -3 hours -1 minutes"),
            (
                "-10s-312ms-789ns",
                "-10 seconds -312 milliseconds -789 nanoseconds",
            ),
            (
                "-23millis-987us-754ns",
                "-23 milliseconds -987 microseconds -754 nanoseconds",
            ),
        ];
        for (input, expected) in test_cases {
            let result = normalize_interval_name(input).unwrap();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_iso8601_format() {
        assert_eq!(
            normalize_interval_name("P1Y2M3DT4H5M6S"),
            Some("36993906000 milliseconds".to_string())
        );
        assert_eq!(
            normalize_interval_name("p3y3m700dt133h17m36.789s"),
            Some("163343856789 milliseconds".to_string())
        );
        assert_eq!(
            normalize_interval_name("-P1Y2M3DT4H5M6S"),
            Some("-36993906000 milliseconds".to_string())
        );
        assert_eq!(normalize_interval_name("P1_INVALID_ISO8601"), None);
    }

    #[test]
    fn test_visit_expr_when_interval_is_single_quoted_string_abbr_expr() {
        let interval_transformation_rule = ExpandIntervalTransformRule {};

        let mut string_expr = create_interval(single_quoted_string_expr("5y".to_string()));

        let control_flow = interval_transformation_rule.visit_expr(&mut string_expr);

        assert_eq!(control_flow, ControlFlow::Continue(()));
        assert_eq!(
            string_expr,
            Expr::Interval(Interval {
                value: Box::new(Expr::Value(
                    Value::SingleQuotedString("5 years".to_string()).into()
                )),
                leading_field: None,
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            })
        );
    }

    #[test]
    fn test_visit_expr_when_interval_is_single_quoted_string_iso8601_expr() {
        let interval_transformation_rule = ExpandIntervalTransformRule {};

        let mut string_expr =
            create_interval(single_quoted_string_expr("P1Y2M3DT4H5M6S".to_string()));

        let control_flow = interval_transformation_rule.visit_expr(&mut string_expr);

        assert_eq!(control_flow, ControlFlow::Continue(()));
        assert_eq!(
            string_expr,
            Expr::Interval(Interval {
                value: Box::new(Expr::Value(
                    Value::SingleQuotedString("36993906000 milliseconds".to_string()).into()
                )),
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

        let binary_op = Box::new(Expr::BinaryOp {
            left: single_quoted_string_expr("2d".to_string()),
            op: BinaryOperator::Minus,
            right: Box::new(create_interval(single_quoted_string_expr("1d".to_string()))),
        });
        let mut binary_op_expr = create_interval(binary_op);
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

    #[test]
    fn test_visit_expr_when_cast_expr() {
        let interval_transformation_rule = ExpandIntervalTransformRule {};

        let mut cast_to_interval_expr = Expr::Cast {
            expr: single_quoted_string_expr("3y2mon".to_string()),
            data_type: DataType::Interval,
            format: None,
            kind: sqlparser::ast::CastKind::Cast,
        };

        let control_flow = interval_transformation_rule.visit_expr(&mut cast_to_interval_expr);

        assert_eq!(control_flow, ControlFlow::Continue(()));
        assert_eq!(
            cast_to_interval_expr,
            Expr::Cast {
                kind: CastKind::Cast,
                expr: Box::new(Expr::Value(
                    Value::SingleQuotedString("3 years 2 months".to_string()).into()
                )),
                data_type: DataType::Interval,
                format: None,
            }
        );

        let mut cast_to_i64_expr = Expr::Cast {
            expr: single_quoted_string_expr("5".to_string()),
            data_type: DataType::Int64,
            format: None,
            kind: sqlparser::ast::CastKind::Cast,
        };
        let control_flow = interval_transformation_rule.visit_expr(&mut cast_to_i64_expr);
        assert_eq!(control_flow, ControlFlow::Continue(()));
        assert_eq!(
            cast_to_i64_expr,
            Expr::Cast {
                expr: single_quoted_string_expr("5".to_string()),
                data_type: DataType::Int64,
                format: None,
                kind: sqlparser::ast::CastKind::Cast,
            }
        );
    }
}
