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
    /// Matches either one or more digits `(\d+)` or one or more non-digits `(\D+)` characters
    /// Negative sign before digits is matched optionally
    static ref INTERVAL_SHORT_NAME_PATTERN: Regex = Regex::new(r"(-?\d+|\D+)").unwrap();

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
    use crate::statements::transform::expand_interval::expand_interval_name;

    #[test]
    fn test_transform_interval_conversions() {
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
            ("2y4w1h", "2 years 4 weeks 1 hours"),
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
}
