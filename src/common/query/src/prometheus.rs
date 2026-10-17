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

use sqlparser::ast::Expr;
use sqlparser::tokenizer::Token;

/// Canonical Prometheus stale-marker NaN bit pattern.
pub const PROMETHEUS_STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// Formats a floating-point value for the Prometheus HTTP API.
pub fn format_prometheus_float(value: f64) -> String {
    if value == f64::INFINITY {
        "+Inf".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_string()
    } else {
        value.to_string()
    }
}

/// Returns whether `value` is the canonical Prometheus stale-marker NaN.
#[inline]
pub fn is_prometheus_stale_nan(value: f64) -> bool {
    value.to_bits() == PROMETHEUS_STALE_NAN_BITS
}

/// Collects metric names from a PromQL expression.
///
/// For binary expressions:
/// - AND, OR, UNLESS: only the left-hand side contributes metric names.
/// - Comparison operators (==, !=, <, >, <=, >=): if not in boolean mode,
///   the left-hand side metric names are preserved (to match Prometheus behavior).
/// - Arithmetic operators (+, -, *, /, %): no metric names are preserved
///   unless the LHS is a metric name expression.
///
/// Note: This function is primarily used for determining which `__name__`
/// labels to attach to results. The actual metric name extraction from table
/// names is handled elsewhere.
pub fn collect_metric_names(expr: &Expr, metric_names: &mut Vec<String>) {
    match expr {
        Expr::Identifier(ident) => {
            if !ident.quote_style.is_empty() {
                metric_names.push(ident.value.clone());
            }
        }
        Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                // table.column or just column - for PromQL, we care about the metric name
                metric_names.push(parts.last().unwrap().value.clone());
            } else {
                metric_names.push(expr.to_string());
            }
        }
        Expr::BinaryOp { left, op, right } => {
            // Determine if this is a comparison operator in vector matching mode
            let is_comparison = matches!(
                op,
                sqlparser::ast::BinaryOperator::Eq
                    | sqlparser::ast::BinaryOperator::NotEq
                    | sqlparser::ast::BinaryOperator::Lt
                    | sqlparser::ast::BinaryOperator::LtEq
                    | sqlparser::ast::BinaryOperator::Gt
                    | sqlparser::ast::BinaryOperator::GtEq
            );

            // For AND, OR, UNLESS - only LHS contributes
            let is_set_operation = matches!(
                op,
                sqlparser::ast::BinaryOperator::And | sqlparser::ast::BinaryOperator::Or
            );

            if is_set_operation {
                // AND, OR: only LHS contributes metric names
                collect_metric_names(left, metric_names);
            } else if is_comparison {
                // For comparison operators, Prometheus preserves the left-hand metric name
                // unless it's a boolean comparison. In GreptimeDB's AST, we don't easily
                // distinguish boolean vs vector mode here, so we follow the rule:
                // Comparison operators preserve LHS metric names.
                collect_metric_names(left, metric_names);
            } else {
                // For arithmetic and other operators, preserve LHS if it contains metric names
                // This is a heuristic - in practice, arithmetic operations often don't
                // have a clear single metric name, but we preserve whatever the LHS has.
                collect_metric_names(left, metric_names);
            }
        }
        Expr::Subquery(_) => {
            // Subqueries don't contribute direct metric names in this context
        }
        _ => {
            // Other expressions don't contribute metric names
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_only_the_canonical_stale_marker() {
        assert!(is_prometheus_stale_nan(f64::from_bits(
            0x7ff0_0000_0000_0002
        )));
        assert!(!is_prometheus_stale_nan(f64::from_bits(
            0x7ff8_0000_0000_0000
        )));
    }

    #[test]
    fn formats_prometheus_float_values() {
        assert_eq!(format_prometheus_float(f64::INFINITY), "+Inf");
        assert_eq!(format_prometheus_float(f64::NEG_INFINITY), "-Inf");
        assert_eq!(format_prometheus_float(f64::NAN), "NaN");
        assert_eq!(format_prometheus_float(1.5), "1.5");
    }
}