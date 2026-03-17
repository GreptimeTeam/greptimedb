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

//! Expression split utilities for partition rules.
//!
//! This module provides a conservative way to split one partition expression `R`
//! by a split expression `S` into:
//! - `left = R AND S`
//! - `right = R AND NOT(S)`
//!
//! The implementation intentionally reuses existing partition components
//! (`Collider`, `simplify`, `PartitionChecker`) and degrades to no-split when an
//! unsupported shape/type is encountered.

use std::collections::{BTreeMap, HashSet};

use datatypes::value::Value;
use snafu::ensure;

use crate::collider::Collider;
use crate::error::{self, Result};
use crate::expr::{Operand, PartitionExpr, RestrictedOp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExprSplitDegradeReason {
    UnsupportedType,
    UnsupportedNotExpansion,
    ColliderRejected,
    ValidationFailed,
    SimplifyFailed,
}

/// Splits one partition expression with a split predicate.
///
/// Returns `(left, right)` on success, where:
/// - `left = R AND S`
/// - `right = R AND NOT(S)`
///
/// Returns [`ExprSplitDegradeReason`] when this cannot safely process the shape/type.
pub fn split_partition_expr(
    base_expr: PartitionExpr,
    split_expr: PartitionExpr,
) -> std::result::Result<(PartitionExpr, PartitionExpr), ExprSplitDegradeReason> {
    let base = base_expr.canonicalize();
    let split = split_expr.canonicalize();

    if validate_supported_expr(&base).is_err() || validate_supported_expr(&split).is_err() {
        return Err(ExprSplitDegradeReason::UnsupportedType);
    }

    let not_split = match negate_split_expr(&split) {
        Ok(expr) => expr,
        Err(_) => {
            return Err(ExprSplitDegradeReason::UnsupportedNotExpansion);
        }
    };

    let left_raw = base.clone().and(split);
    let right_raw = base.clone().and(not_split);

    if Collider::new(std::slice::from_ref(&left_raw)).is_err()
        || Collider::new(std::slice::from_ref(&right_raw)).is_err()
    {
        return Err(ExprSplitDegradeReason::ColliderRejected);
    }

    let left_expr = simplify_and_bounds(left_raw);
    let right_expr = simplify_and_bounds(right_raw);

    Ok((left_expr, right_expr))
}

/// Rewrites `NOT(expr)` into an equivalent `PartitionExpr` without introducing a unary NOT node.
///
/// Why this function exists:
/// - `PartitionExpr` only models binary operators.
/// - Cut logic needs `R AND NOT(S)`.
/// - We therefore rewrite `NOT(S)` into an equivalent binary-expression tree.
///
/// Rewrite rules:
/// - Atomic comparisons:
///   - `=`  <-> `!=`
///   - `<`  <-> `>=`
///   - `<=` <-> `>`
///   - `>`  <-> `<=`
///   - `>=` <-> `<`
/// - Boolean composition:
///   - `NOT(A AND B)` => `NOT(A) OR NOT(B)`
///   - `NOT(A OR B)`  => `NOT(A) AND NOT(B)`
///
/// Failure behavior:
/// - For `AND/OR`, both sides must be `Operand::Expr`; otherwise returns `NoExprOperand`.
/// - Any unsupported shape bubbles up as an error and the caller degrades to no-split.
pub fn negate_split_expr(expr: &PartitionExpr) -> Result<PartitionExpr> {
    match expr.op() {
        RestrictedOp::Eq
        | RestrictedOp::NotEq
        | RestrictedOp::Lt
        | RestrictedOp::LtEq
        | RestrictedOp::Gt
        | RestrictedOp::GtEq => {
            // Atomic negate by operator inversion.
            let op = match expr.op() {
                RestrictedOp::Eq => RestrictedOp::NotEq,
                RestrictedOp::NotEq => RestrictedOp::Eq,
                RestrictedOp::Lt => RestrictedOp::GtEq,
                RestrictedOp::LtEq => RestrictedOp::Gt,
                RestrictedOp::Gt => RestrictedOp::LtEq,
                RestrictedOp::GtEq => RestrictedOp::Lt,
                RestrictedOp::And | RestrictedOp::Or => unreachable!(),
            };
            Ok(PartitionExpr::new(
                expr.lhs().clone(),
                op,
                expr.rhs().clone(),
            ))
        }
        RestrictedOp::And | RestrictedOp::Or => {
            // De Morgan transform on recursive sub-expressions.
            let lhs = match expr.lhs() {
                Operand::Expr(lhs) => lhs,
                other => {
                    return error::NoExprOperandSnafu {
                        operand: other.clone(),
                    }
                    .fail();
                }
            };
            let rhs = match expr.rhs() {
                Operand::Expr(rhs) => rhs,
                other => {
                    return error::NoExprOperandSnafu {
                        operand: other.clone(),
                    }
                    .fail();
                }
            };
            let not_lhs = negate_split_expr(lhs)?;
            let not_rhs = negate_split_expr(rhs)?;
            let op = match expr.op() {
                // NOT(A AND B) => NOT(A) OR NOT(B)
                RestrictedOp::And => RestrictedOp::Or,
                // NOT(A OR B) => NOT(A) AND NOT(B)
                RestrictedOp::Or => RestrictedOp::And,
                _ => unreachable!(),
            };
            Ok(PartitionExpr::new(
                Operand::Expr(not_lhs),
                op,
                Operand::Expr(not_rhs),
            ))
        }
    }
}

pub fn validate_supported_expr(expr: &PartitionExpr) -> Result<()> {
    match expr.op() {
        RestrictedOp::And | RestrictedOp::Or => {
            let lhs = match expr.lhs() {
                Operand::Expr(lhs) => lhs,
                other => {
                    return error::NoExprOperandSnafu {
                        operand: other.clone(),
                    }
                    .fail();
                }
            };
            let rhs = match expr.rhs() {
                Operand::Expr(rhs) => rhs,
                other => {
                    return error::NoExprOperandSnafu {
                        operand: other.clone(),
                    }
                    .fail();
                }
            };
            validate_supported_expr(lhs)?;
            validate_supported_expr(rhs)?;
            Ok(())
        }
        _ => validate_atomic(expr),
    }
}

fn validate_atomic(expr: &PartitionExpr) -> Result<()> {
    let (lhs, rhs) = (expr.lhs(), expr.rhs());
    match (lhs, rhs) {
        (Operand::Column(_), Operand::Value(v)) | (Operand::Value(v), Operand::Column(_)) => {
            ensure!(
                is_supported_value(v),
                error::InvalidExprSnafu { expr: expr.clone() }
            );
            if is_nan_value(v)
                && matches!(
                    expr.op(),
                    RestrictedOp::Lt | RestrictedOp::LtEq | RestrictedOp::Gt | RestrictedOp::GtEq
                )
            {
                return error::InvalidExprSnafu { expr: expr.clone() }.fail();
            }
            Ok(())
        }
        _ => error::InvalidExprSnafu { expr: expr.clone() }.fail(),
    }
}

fn is_supported_value(v: &Value) -> bool {
    matches!(
        v,
        Value::Int8(_)
            | Value::Int16(_)
            | Value::Int32(_)
            | Value::Int64(_)
            | Value::UInt8(_)
            | Value::UInt16(_)
            | Value::UInt32(_)
            | Value::UInt64(_)
            | Value::Float32(_)
            | Value::Float64(_)
            | Value::String(_)
            | Value::Timestamp(_)
            | Value::Date(_)
    )
}

fn is_nan_value(v: &Value) -> bool {
    match v {
        Value::Float32(x) => x.0.is_nan(),
        Value::Float64(x) => x.0.is_nan(),
        _ => false,
    }
}

#[derive(Debug, Clone)]
struct LowerBound {
    value: Value,
    inclusive: bool,
}

#[derive(Debug, Clone)]
struct UpperBound {
    value: Value,
    inclusive: bool,
}

/// Simplifies conjunction-only range predicates by keeping the tightest bounds per column.
///
/// This pass is intentionally conservative and only runs when the whole expression
/// can be flattened into `atom1 AND atom2 AND ...` without any `OR` node.
///
/// Behavior:
/// - For each column, collect all lower-bound predicates (`>` / `>=`) and keep the
///   tightest one.
/// - For each column, collect all upper-bound predicates (`<` / `<=`) and keep the
///   tightest one.
/// - Non-range predicates (for example `=` / `!=`) are preserved as-is.
/// - If the expression contains `OR`, this function returns the original expression.
///
/// Tightness rules:
/// - Upper bound: smaller value is tighter; if equal value, exclusive (`<`) is tighter.
/// - Lower bound: larger value is tighter; if equal value, exclusive (`>`) is tighter.
///
/// Examples:
/// - `a <= 10 AND a < 10` => `a < 10`
/// - `a >= 10 AND a > 10` => `a > 10`
/// - `a < 10 AND a < 5`   => `a < 5`
fn simplify_and_bounds(expr: PartitionExpr) -> PartitionExpr {
    let mut atoms = Vec::new();
    if !collect_and_atoms(&expr, &mut atoms) {
        return expr;
    }

    let mut passthrough = Vec::new();
    let mut lowers: BTreeMap<String, LowerBound> = BTreeMap::new();
    let mut uppers: BTreeMap<String, UpperBound> = BTreeMap::new();
    let mut seen = HashSet::new();

    for atom in atoms {
        let atom = atom.canonicalize();
        let Some((col, op, val)) = atom_col_op_val(&atom) else {
            let key = atom.to_string();
            if seen.insert(key) {
                passthrough.push(atom);
            }
            continue;
        };

        // Keep only the tightest lower/upper range for each column.
        match op {
            RestrictedOp::Lt | RestrictedOp::LtEq => {
                let candidate = UpperBound {
                    value: val,
                    inclusive: matches!(op, RestrictedOp::LtEq),
                };
                match uppers.get_mut(&col) {
                    Some(current) => {
                        if prefer_upper(&candidate, current) {
                            *current = candidate;
                        }
                    }
                    None => {
                        uppers.insert(col, candidate);
                    }
                }
            }
            RestrictedOp::Gt | RestrictedOp::GtEq => {
                let candidate = LowerBound {
                    value: val,
                    inclusive: matches!(op, RestrictedOp::GtEq),
                };
                match lowers.get_mut(&col) {
                    Some(current) => {
                        if prefer_lower(&candidate, current) {
                            *current = candidate;
                        }
                    }
                    None => {
                        lowers.insert(col, candidate);
                    }
                }
            }
            _ => {
                let key = atom.to_string();
                if seen.insert(key) {
                    passthrough.push(atom);
                }
            }
        }
    }

    let mut out = passthrough;
    for (col, lower) in lowers {
        out.push(PartitionExpr::new(
            Operand::Column(col),
            if lower.inclusive {
                RestrictedOp::GtEq
            } else {
                RestrictedOp::Gt
            },
            Operand::Value(lower.value),
        ));
    }
    for (col, upper) in uppers {
        out.push(PartitionExpr::new(
            Operand::Column(col),
            if upper.inclusive {
                RestrictedOp::LtEq
            } else {
                RestrictedOp::Lt
            },
            Operand::Value(upper.value),
        ));
    }

    fold_and_exprs(out).unwrap_or(expr)
}

/// Flattens an expression into atomic terms when it is a pure conjunction tree.
///
/// Returns `false` if any `OR` is encountered, signaling caller to skip this
/// simplification path.
fn collect_and_atoms(expr: &PartitionExpr, out: &mut Vec<PartitionExpr>) -> bool {
    match expr.op() {
        RestrictedOp::And => {
            let lhs = match expr.lhs() {
                Operand::Expr(lhs) => lhs,
                _ => return false,
            };
            let rhs = match expr.rhs() {
                Operand::Expr(rhs) => rhs,
                _ => return false,
            };
            collect_and_atoms(lhs, out) && collect_and_atoms(rhs, out)
        }
        RestrictedOp::Or => false,
        _ => {
            out.push(expr.clone());
            true
        }
    }
}

/// Extracts `(column, op, value)` from a canonicalized atomic expression.
fn atom_col_op_val(expr: &PartitionExpr) -> Option<(String, RestrictedOp, Value)> {
    let lhs = expr.lhs();
    let rhs = expr.rhs();
    match (lhs, rhs) {
        (Operand::Column(col), Operand::Value(v)) => {
            Some((col.clone(), expr.op().clone(), v.clone()))
        }
        _ => None,
    }
}

fn prefer_upper(candidate: &UpperBound, current: &UpperBound) -> bool {
    // "Smaller" upper bound is tighter. For equal value, exclusive is tighter.
    match candidate.value.partial_cmp(&current.value) {
        Some(std::cmp::Ordering::Less) => true,
        Some(std::cmp::Ordering::Equal) => !candidate.inclusive && current.inclusive,
        _ => false,
    }
}

fn prefer_lower(candidate: &LowerBound, current: &LowerBound) -> bool {
    // "Larger" lower bound is tighter. For equal value, exclusive is tighter.
    match candidate.value.partial_cmp(&current.value) {
        Some(std::cmp::Ordering::Greater) => true,
        Some(std::cmp::Ordering::Equal) => !candidate.inclusive && current.inclusive,
        _ => false,
    }
}

/// Folds a list of expressions into a left-associated AND tree.
fn fold_and_exprs(mut exprs: Vec<PartitionExpr>) -> Option<PartitionExpr> {
    if exprs.is_empty() {
        return None;
    }
    let mut iter = exprs.drain(..);
    let mut acc = iter.next()?;
    for next in iter {
        acc = acc.and(next);
    }
    Some(acc)
}

#[cfg(test)]
mod tests {
    use datatypes::value::{OrderedFloat, Value};
    use store_api::storage::RegionNumber;

    use super::*;
    use crate::checker::PartitionChecker;
    use crate::expr::col;
    use crate::multi_dim::MultiDimPartitionRule;

    fn validate_cut_result_with_checker(
        original_rule_exprs: &[PartitionExpr],
        replaced_index: usize,
        left: &Option<PartitionExpr>,
        right: &Option<PartitionExpr>,
        partition_columns: Vec<String>,
        regions: Vec<RegionNumber>,
    ) -> Result<()> {
        ensure!(
            replaced_index < original_rule_exprs.len(),
            error::UnexpectedSnafu {
                err_msg: format!(
                    "replaced index out of bounds: {replaced_index} >= {}",
                    original_rule_exprs.len()
                )
            }
        );

        let mut exprs = original_rule_exprs.to_vec();
        exprs.remove(replaced_index);
        if let Some(left_expr) = left.clone() {
            exprs.push(left_expr);
        }
        if let Some(right_expr) = right.clone() {
            exprs.push(right_expr);
        }

        ensure!(
            !exprs.is_empty(),
            error::UnexpectedSnafu {
                err_msg: "empty rule exprs after split".to_string()
            }
        );

        let final_regions = if regions.len() == exprs.len() {
            regions
        } else {
            (0..exprs.len() as RegionNumber).collect()
        };

        let rule = MultiDimPartitionRule::try_new(partition_columns, final_regions, exprs, false)?;
        let checker = PartitionChecker::try_new(&rule)?;
        checker.check()?;
        Ok(())
    }

    #[test]
    fn test_split_simple_range() {
        // R: a < 10
        let base = col("a").lt(Value::Int64(10));
        // S: a < 5
        let split = col("a").lt(Value::Int64(5));
        let (left, right) = split_partition_expr(base, split).unwrap();
        // left = R AND S = a < 5
        assert_eq!(left.to_string(), "a < 5");
        // right = R AND NOT(S) = a >= 5 AND a < 10
        assert_eq!(right.to_string(), "a >= 5 AND a < 10");
    }

    #[test]
    fn test_split_string_interval() {
        // R: v > 'm' AND v < 'n'
        let base = col("v")
            .gt(Value::String("m".into()))
            .and(col("v").lt(Value::String("n".into())));
        // S: v < 'k'
        let split = col("v").lt(Value::String("k".into()));
        let (left, _right) = split_partition_expr(base, split).unwrap();
        // left = (v > m AND v < n) AND (v < k) -> v > m AND v < k
        assert_eq!(left.to_string(), "v > m AND v < k");
    }

    #[test]
    fn test_split_numeric_interval_mid_split() {
        // R: a > 3 AND a < 10
        let base = col("a")
            .gt(Value::Int64(3))
            .and(col("a").lt(Value::Int64(10)));
        // S: a < 5
        let split = col("a").lt(Value::Int64(5));

        let (left, right) = split_partition_expr(base, split).unwrap();

        // left = (a > 3 AND a < 10) AND (a < 5) -> a > 3 AND a < 5
        assert_eq!(left.to_string(), "a > 3 AND a < 5");
        // right = (a > 3 AND a < 10) AND (a >= 5) -> a >= 5 AND a < 10
        assert_eq!(right.to_string(), "a >= 5 AND a < 10");
    }

    #[test]
    fn test_split_degrade_on_unsupported_type() {
        // intentionally excludes boolean from split-able value types.
        let base = col("a").eq(Value::Boolean(true));
        let split = col("a").eq(Value::Boolean(true));

        let result = split_partition_expr(base, split);
        assert_eq!(result.unwrap_err(), ExprSplitDegradeReason::UnsupportedType);
    }

    #[test]
    fn test_validate_cut_result_with_checker() {
        // Original partition set: a < 10, a >= 10
        let original = vec![
            col("a").lt(Value::Int64(10)),
            col("a").gt_eq(Value::Int64(10)),
        ];
        let left = Some(col("a").lt(Value::Int64(5)));
        let right = Some(
            col("a")
                .gt_eq(Value::Int64(5))
                .and(col("a").lt(Value::Int64(10))),
        );

        validate_cut_result_with_checker(
            &original,
            0,
            &left,
            &right,
            vec!["a".to_string()],
            vec![1, 2, 3],
        )
        .unwrap();
    }

    #[test]
    fn test_simplify_same_upper_bound_prefers_strict() {
        // a <= 10 AND a < 10 => a < 10
        let expr = col("a")
            .lt_eq(Value::Int64(10))
            .and(col("a").lt(Value::Int64(10)));

        let simplified = simplify_and_bounds(expr);
        assert_eq!(simplified.to_string(), "a < 10");
    }

    #[test]
    fn test_simplify_same_lower_bound_prefers_strict() {
        // a >= 10 AND a > 10 => a > 10
        let expr = col("a")
            .gt_eq(Value::Int64(10))
            .and(col("a").gt(Value::Int64(10)));

        let simplified = simplify_and_bounds(expr);
        assert_eq!(simplified.to_string(), "a > 10");
    }

    #[test]
    fn test_negate_split_expr_demorgan_and() {
        // expr: (a < 10) AND (a >= 3)
        let expr = col("a")
            .lt(Value::Int64(10))
            .and(col("a").gt_eq(Value::Int64(3)));
        let not_expr = negate_split_expr(&expr).unwrap();
        // NOT(expr) => (a >= 10) OR (a < 3)
        assert_eq!(not_expr.to_string(), "a >= 10 OR a < 3");
    }

    #[test]
    fn test_negate_split_expr_demorgan_or() {
        // expr: (a = 1) OR (a <> 2)
        let expr = PartitionExpr::new(
            Operand::Expr(col("a").eq(Value::Int64(1))),
            RestrictedOp::Or,
            Operand::Expr(col("a").not_eq(Value::Int64(2))),
        );
        let not_expr = negate_split_expr(&expr).unwrap();
        // NOT(expr) => (a <> 1) AND (a = 2)
        assert_eq!(not_expr.to_string(), "a <> 1 AND a = 2");
    }

    #[test]
    fn test_negate_split_expr_invalid_and_operand() {
        // malformed AND: rhs is a scalar value, not an Expr subtree.
        let malformed = PartitionExpr {
            lhs: Box::new(Operand::Expr(col("a").lt(Value::Int64(10)))),
            op: RestrictedOp::And,
            rhs: Box::new(Operand::Value(Value::Int64(1))),
        };
        assert!(negate_split_expr(&malformed).is_err());
    }

    #[test]
    fn test_validate_supported_expr_value_column_allowed() {
        // Canonicalization can flip to column-value; validator must accept value-column input.
        let expr = PartitionExpr::new(
            Operand::Value(Value::Int64(10)),
            RestrictedOp::Lt,
            Operand::Column("a".to_string()),
        );
        assert!(validate_supported_expr(&expr).is_ok());
    }

    #[test]
    fn test_validate_supported_expr_invalid_atomic_shape() {
        // column-column atomic comparison is out of shape.
        let expr = PartitionExpr::new(
            Operand::Column("a".to_string()),
            RestrictedOp::Eq,
            Operand::Column("b".to_string()),
        );
        assert!(validate_supported_expr(&expr).is_err());
    }

    #[test]
    fn test_validate_supported_expr_nan_range_rejected() {
        // NaN cannot be used in range predicates.
        let expr = col("a").lt(Value::Float64(OrderedFloat(f64::NAN)));
        assert!(validate_supported_expr(&expr).is_err());
    }

    #[test]
    fn test_simplify_and_bounds_or_keeps_original() {
        // OR tree is intentionally not flattened by AND-only simplifier.
        let expr = PartitionExpr::new(
            Operand::Expr(col("a").lt(Value::Int64(10))),
            RestrictedOp::Or,
            Operand::Expr(col("a").gt_eq(Value::Int64(20))),
        );
        let simplified = simplify_and_bounds(expr.clone());
        assert_eq!(simplified.to_string(), expr.to_string());
    }

    #[test]
    fn test_simplify_and_bounds_keep_stronger_when_weaker_seen_later() {
        // upper: stronger bound first, weaker later -> keep stronger (< 5).
        let upper = col("a")
            .lt(Value::Int64(5))
            .and(col("a").lt(Value::Int64(10)));
        assert_eq!(simplify_and_bounds(upper).to_string(), "a < 5");

        // lower: stronger bound first, weaker later -> keep stronger (> 10).
        let lower = col("a")
            .gt(Value::Int64(10))
            .and(col("a").gt(Value::Int64(5)));
        assert_eq!(simplify_and_bounds(lower).to_string(), "a > 10");
    }

    #[test]
    fn test_internal_helpers_uncovered_branches() {
        // Empty AND fold should return None.
        assert!(fold_and_exprs(vec![]).is_none());

        // Any OR in tree disables AND-bound simplification path.
        let mut out = Vec::new();
        let or_expr = PartitionExpr::new(
            Operand::Expr(col("a").lt(Value::Int64(10))),
            RestrictedOp::Or,
            Operand::Expr(col("a").gt_eq(Value::Int64(20))),
        );
        assert!(!collect_and_atoms(&or_expr, &mut out));

        // value-value atom has no (column, op, value) projection.
        let value_value = PartitionExpr::new(
            Operand::Value(Value::Int64(1)),
            RestrictedOp::Eq,
            Operand::Value(Value::Int64(2)),
        );
        assert!(atom_col_op_val(&value_value).is_none());
    }

    #[test]
    fn test_split_nested_base_or_and() {
        // base: (a < 10) OR (a >= 20 AND a < 30)
        let base = PartitionExpr::new(
            Operand::Expr(col("a").lt(Value::Int64(10))),
            RestrictedOp::Or,
            Operand::Expr(
                col("a")
                    .gt_eq(Value::Int64(20))
                    .and(col("a").lt(Value::Int64(30))),
            ),
        );
        // split: a < 5
        let split = col("a").lt(Value::Int64(5));

        let result = split_partition_expr(base, split);
        assert_eq!(
            result.unwrap_err(),
            ExprSplitDegradeReason::ColliderRejected
        );
    }

    #[test]
    fn test_split_nested_split_expr_or_and() {
        // base: a < 10
        let base = col("a").lt(Value::Int64(10));
        // split: (a < 5) OR (a >= 8 AND a < 9)
        let split = PartitionExpr::new(
            Operand::Expr(col("a").lt(Value::Int64(5))),
            RestrictedOp::Or,
            Operand::Expr(
                col("a")
                    .gt_eq(Value::Int64(8))
                    .and(col("a").lt(Value::Int64(9))),
            ),
        );

        let result = split_partition_expr(base, split);
        assert_eq!(
            result.unwrap_err(),
            ExprSplitDegradeReason::ColliderRejected
        );
    }
}
