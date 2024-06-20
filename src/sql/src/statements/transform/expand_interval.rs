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
use sqlparser::ast::{Expr, Interval, Value};

use crate::statements::transform::TransformRule;

lazy_static! {
    static ref INTERVAL_SHORT_NAME_MAPPING: HashMap<&'static str, &'static str> = HashMap::from([
        ("y", " years"),
        ("m", " months"),
        ("w", " weeks"),
        ("d", " days"),
        ("h", " hours"),
        ("min", " minutes"),
        ("s", " seconds"),
        ("millis", " milliseconds"),
        ("mils", " milliseconds"),
        ("ms", " microseconds"),
        ("ns", " nanoseconds"),
    ]);
}

/// 'Interval' expression transformer
/// - `y` for `years`
/// - `m` for `months`
/// - `w` for `weeks`
/// - `d` for `days`
/// - `h` for `hours`
/// - `m` for `minutes`
/// - `s` for `seconds`
/// - `millis` for `milliseconds`
/// - `mils` for `milliseconds`
/// - `ms` for `microseconds`
/// - `ns` for `nanoseconds`
/// Required for use cases that use the shortened version of Interval declaration,
/// f.e `select interval '1h'` or `select interval '3w'`
pub(crate) struct ExpandIntervalTransformRule;

impl TransformRule for ExpandIntervalTransformRule {
    fn visit_expr(&self, expr: &mut Expr) -> ControlFlow<()> {
        if let Expr::Interval(Interval {
            value,
            leading_field,
            leading_precision,
            last_field,
            fractional_seconds_precision,
        }) = expr
        {
            if let Expr::Value(Value::SingleQuotedString(item)) = *value.clone() {
                if !item.contains(|c: char| c.is_whitespace()) {
                    *expr = Expr::Interval(Interval {
                        value: Box::new(Expr::Value(Value::SingleQuotedString(
                            expand_interval_name(item.as_str()),
                        ))),
                        leading_field: leading_field.clone(),
                        leading_precision: *leading_precision,
                        last_field: last_field.clone(),
                        fractional_seconds_precision: *fractional_seconds_precision,
                    });
                }
            }
        }
        ControlFlow::<()>::Continue(())
    }
}

/// Removes the first character (assumed to be a sign) and all digits from the `interval_str`.
/// Returns an interval's short name (e.g., "y", "h", "min").
fn get_interval_short_name(interval_str: &str) -> String {
    let mut short_name = String::new();
    for c in interval_str.chars().dropping(1) {
        if !c.is_ascii_digit() {
            short_name.push(c);
        }
    }
    short_name
}

/// Expands a short interval name to its full name.
/// Returns an interval's full name (e.g., "years", "hours", "minutes") for existing mapping
/// or the `interval_str` as is
fn expand_interval_name(interval_str: &str) -> String {
    let short_name = get_interval_short_name(interval_str);
    match INTERVAL_SHORT_NAME_MAPPING.get(short_name.as_str()) {
        Some(extended_name) => interval_str.replace(short_name.as_str(), extended_name),
        None => interval_str.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use crate::statements::transform::expand_interval::{
        expand_interval_name, get_interval_short_name,
    };

    #[test]
    fn test_drop_digits() {
        assert_eq!(get_interval_short_name("100h"), "h");
        assert_eq!(get_interval_short_name("55y"), "y");
        assert_eq!(get_interval_short_name("-2m"), "m");
    }
    #[test]
    fn test_transform_interval_conversions() {
        let test_cases = vec![
            ("1y", "1 years"),
            ("4m", "4 months"),
            ("3w", "3 weeks"),
            ("55h", "55 hours"),
            ("3d", "3 days"),
            ("5s", "5 seconds"),
            ("2min", "2 minutes"),
            ("100millis", "100 milliseconds"),
            ("150mils", "150 milliseconds"),
            ("200ms", "200 microseconds"),
            ("400ns", "400 nanoseconds"),
            ("10x", "10x"),
            ("2 years", "2 years"),
            ("4 months", "4 months"),
            ("7 weeks", "7 weeks"),
        ];
        for (input, expected) in test_cases {
            let result = expand_interval_name(input);
            assert_eq!(result, expected);
        }
    }
}
