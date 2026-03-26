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
    EmptyBranch,
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

    if is_empty_and_conjunction(&left_expr) || is_empty_and_conjunction(&right_expr) {
        return Err(ExprSplitDegradeReason::EmptyBranch);
    }

    Ok((left_expr, right_expr))
}

/// Detects whether a pure conjunction expression is definitely unsatisfiable.
///
/// Scope and intent:
/// - This checker is intentionally conservative.
/// - It only analyzes expressions that can be flattened into:
///   `atom1 AND atom2 AND ...`
/// - If any `OR` is present, it returns `false` (unknown / not handled here).
///
/// Strategy:
/// - For each column, keep only the tightest lower bound (`>` / `>=`) and
///   tightest upper bound (`<` / `<=`).
/// - `=` is treated as both lower and upper bound at the same value.
/// - `!=` is tracked per column to catch direct conflicts with `=`.
/// - After bounds are collected, the conjunction is empty iff for any column:
///   - lower value is greater than upper value, or
///   - lower value equals upper value but at least one bound is exclusive.
/// - For discrete domains (`Int*`, `UInt*`), adjacent open bounds with no
///   representable value in between are also treated as empty.
///
/// Notes:
/// - This is still a conservative fast path focused on conjunction emptiness
///   detection for split degradation.
fn is_empty_and_conjunction(expr: &PartitionExpr) -> bool {
    let mut atoms = Vec::new();
    // Only handle conjunction trees. If expression contains OR, skip here.
    if !collect_and_atoms(expr, &mut atoms) {
        return false;
    }

    let mut lowers: BTreeMap<String, LowerBound> = BTreeMap::new();
    let mut uppers: BTreeMap<String, UpperBound> = BTreeMap::new();
    let mut equals: BTreeMap<String, Value> = BTreeMap::new();
    let mut not_equals: BTreeMap<String, HashSet<Value>> = BTreeMap::new();

    for atom in atoms {
        let atom = atom.canonicalize();
        let Some((col, op, val)) = atom_col_op_val(&atom) else {
            continue;
        };

        match op {
            // Keep the tightest upper bound for this column.
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
            // Keep the tightest lower bound for this column.
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
            // Equality means both lower and upper are fixed at the same point.
            RestrictedOp::Eq => {
                if let Some(existing) = equals.get(&col)
                    && existing != &val
                {
                    return true;
                }
                if not_equals
                    .get(&col)
                    .is_some_and(|excluded| excluded.contains(&val))
                {
                    return true;
                }
                equals.insert(col.clone(), val.clone());

                let lower = LowerBound {
                    value: val.clone(),
                    inclusive: true,
                };
                let upper = UpperBound {
                    value: val,
                    inclusive: true,
                };
                match lowers.get_mut(&col) {
                    Some(current) => {
                        if prefer_lower(&lower, current) {
                            *current = lower;
                        }
                    }
                    None => {
                        lowers.insert(col.clone(), lower);
                    }
                }
                match uppers.get_mut(&col) {
                    Some(current) => {
                        if prefer_upper(&upper, current) {
                            *current = upper;
                        }
                    }
                    None => {
                        uppers.insert(col, upper);
                    }
                }
            }
            RestrictedOp::NotEq => {
                if equals.get(&col).is_some_and(|eq| eq == &val) {
                    return true;
                }
                not_equals.entry(col).or_default().insert(val);
            }
            RestrictedOp::And | RestrictedOp::Or => {}
        }
    }

    if uppers
        .iter()
        .any(|(col, upper)| !lowers.contains_key(col) && is_strictly_less_than_domain_min(upper))
    {
        return true;
    }

    if lowers
        .iter()
        .any(|(col, lower)| !uppers.contains_key(col) && is_strictly_greater_than_domain_max(lower))
    {
        return true;
    }

    // Check for contradiction between collected lower/upper bounds per column.
    lowers.into_iter().any(|(col, lower)| {
        let Some(upper) = uppers.get(&col) else {
            return false;
        };

        match lower.value.partial_cmp(&upper.value) {
            Some(std::cmp::Ordering::Greater) => true,
            Some(std::cmp::Ordering::Equal) => {
                if !lower.inclusive || !upper.inclusive {
                    true
                } else {
                    not_equals
                        .get(&col)
                        .is_some_and(|excluded| excluded.contains(&lower.value))
                }
            }
            Some(std::cmp::Ordering::Less) => {
                match (
                    discrete_value_index(&lower.value),
                    discrete_value_index(&upper.value),
                ) {
                    (Some(lower_idx), Some(upper_idx)) => {
                        let min_candidate = if lower.inclusive {
                            Some(lower_idx)
                        } else {
                            lower_idx.checked_add(1)
                        };
                        let max_candidate = if upper.inclusive {
                            Some(upper_idx)
                        } else {
                            upper_idx.checked_sub(1)
                        };
                        match (min_candidate, max_candidate) {
                            (Some(min_val), Some(max_val)) => min_val > max_val,
                            _ => true,
                        }
                    }
                    _ => false,
                }
            }
            _ => false,
        }
    })
}

fn discrete_value_index(v: &Value) -> Option<i128> {
    match v {
        Value::Int8(x) => Some(*x as i128),
        Value::Int16(x) => Some(*x as i128),
        Value::Int32(x) => Some(*x as i128),
        Value::Int64(x) => Some(*x as i128),
        Value::UInt8(x) => Some(*x as i128),
        Value::UInt16(x) => Some(*x as i128),
        Value::UInt32(x) => Some(*x as i128),
        Value::UInt64(x) => Some(*x as i128),
        _ => None,
    }
}

fn is_strictly_less_than_domain_min(bound: &UpperBound) -> bool {
    if bound.inclusive {
        return false;
    }

    is_domain_min_value(&bound.value)
}

fn is_strictly_greater_than_domain_max(bound: &LowerBound) -> bool {
    if bound.inclusive {
        return false;
    }

    is_domain_max_value(&bound.value)
}

fn is_domain_min_value(v: &Value) -> bool {
    match v {
        Value::String(s) => s.is_empty(),
        Value::Float32(v) => v.0 == f32::MIN,
        Value::Float64(v) => v.0 == f64::MIN,
        Value::UInt8(v) => *v == 0,
        Value::UInt16(v) => *v == 0,
        Value::UInt32(v) => *v == 0,
        Value::UInt64(v) => *v == 0,
        Value::Int8(v) => *v == i8::MIN,
        Value::Int16(v) => *v == i16::MIN,
        Value::Int32(v) => *v == i32::MIN,
        Value::Int64(v) => *v == i64::MIN,
        _ => false,
    }
}

fn is_domain_max_value(v: &Value) -> bool {
    match v {
        Value::Float32(v) => v.0 == f32::MAX,
        Value::Float64(v) => v.0 == f64::MAX,
        Value::UInt8(v) => *v == u8::MAX,
        Value::UInt16(v) => *v == u16::MAX,
        Value::UInt32(v) => *v == u32::MAX,
        Value::UInt64(v) => *v == u64::MAX,
        Value::Int8(v) => *v == i8::MAX,
        Value::Int16(v) => *v == i16::MAX,
        Value::Int32(v) => *v == i32::MAX,
        Value::Int64(v) => *v == i64::MAX,
        _ => false,
    }
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
            if is_nan_value(v) || is_infinite_value(v) {
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
    )
}

fn is_nan_value(v: &Value) -> bool {
    match v {
        Value::Float32(x) => x.0.is_nan(),
        Value::Float64(x) => x.0.is_nan(),
        _ => false,
    }
}

fn is_infinite_value(v: &Value) -> bool {
    match v {
        Value::Float32(x) => x.0.is_infinite(),
        Value::Float64(x) => x.0.is_infinite(),
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
    out.extend(lowers.into_iter().map(|(col, lower)| {
        PartitionExpr::new(
            Operand::Column(col),
            if lower.inclusive {
                RestrictedOp::GtEq
            } else {
                RestrictedOp::Gt
            },
            Operand::Value(lower.value),
        )
    }));
    out.extend(uppers.into_iter().map(|(col, upper)| {
        PartitionExpr::new(
            Operand::Column(col),
            if upper.inclusive {
                RestrictedOp::LtEq
            } else {
                RestrictedOp::Lt
            },
            Operand::Value(upper.value),
        )
    }));

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
    exprs.drain(..).reduce(|acc, next| acc.and(next))
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
        exprs.extend(left.iter().cloned());
        exprs.extend(right.iter().cloned());

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
        // S: v < 'm~'
        let split = col("v").lt(Value::String("m~".into()));
        let (left, right) = split_partition_expr(base, split).unwrap();
        // left = (v > m AND v < n) AND (v < m~) -> v > m AND v < m~
        assert_eq!(left.to_string(), "v > m AND v < m~");
        // right = (v > m AND v < n) AND (v >= m~) -> v >= m~ AND v < n
        assert_eq!(right.to_string(), "v >= m~ AND v < n");
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
    fn test_split_degrade_on_empty_branch() {
        // R: a < 10
        let base = col("a").lt(Value::Int64(10));
        // S: a < 20
        let split = col("a").lt(Value::Int64(20));

        // right = (a < 10) AND (a >= 20) is unsatisfiable, should degrade.
        let result = split_partition_expr(base, split);
        assert_eq!(result.unwrap_err(), ExprSplitDegradeReason::EmptyBranch);
    }

    #[test]
    fn test_split_degrade_on_eq_neq_conflict() {
        // R: a = 5
        let base = col("a").eq(Value::Int64(5));
        // S: a = 5
        let split = col("a").eq(Value::Int64(5));

        // right = (a = 5) AND (a <> 5) is unsatisfiable, should degrade.
        let result = split_partition_expr(base, split);
        assert_eq!(result.unwrap_err(), ExprSplitDegradeReason::EmptyBranch);
    }

    #[test]
    fn test_split_degrade_on_discrete_gap_int() {
        // R: a < 5
        let base = col("a").lt(Value::Int64(5));
        // S: a <= 4
        let split = col("a").lt_eq(Value::Int64(4));

        // right = (a < 5) AND (a > 4) has no integer solution, should degrade.
        let result = split_partition_expr(base, split);
        assert_eq!(result.unwrap_err(), ExprSplitDegradeReason::EmptyBranch);
    }

    #[test]
    fn test_split_degrade_on_unsupported_date_type() {
        // Date is intentionally excluded from split-supported value types.
        let base = col("d").lt(Value::Date(5.into()));
        let split = col("d").lt_eq(Value::Date(4.into()));

        let result = split_partition_expr(base, split);
        assert_eq!(result.unwrap_err(), ExprSplitDegradeReason::UnsupportedType);
    }

    #[test]
    fn test_split_degrade_on_unsupported_timestamp_type() {
        // Timestamp is intentionally excluded from split-supported value types.
        let base = col("ts").lt(Value::Timestamp(0.into()));
        let split = col("ts").lt_eq(Value::Timestamp(1.into()));

        let result = split_partition_expr(base, split);
        assert_eq!(result.unwrap_err(), ExprSplitDegradeReason::UnsupportedType);
    }

    #[test]
    fn test_split_degrade_on_singleton_bounds_excluded_by_negated_eq() {
        // R: a >= 5 AND a <= 5
        let base = col("a")
            .gt_eq(Value::Int64(5))
            .and(col("a").lt_eq(Value::Int64(5)));
        // S: a <> 5
        let split = col("a").not_eq(Value::Int64(5));

        // left = (a >= 5 AND a <= 5) AND (a <> 5) is unsatisfiable, should degrade.
        let result = split_partition_expr(base, split);
        assert_eq!(result.unwrap_err(), ExprSplitDegradeReason::EmptyBranch);
    }

    #[test]
    fn test_split_degrade_on_singleton_bounds_excluded_by_eq() {
        // R: a >= 5 AND a <= 5
        let base = col("a")
            .gt_eq(Value::Int64(5))
            .and(col("a").lt_eq(Value::Int64(5)));
        // S: a = 5
        let split = col("a").eq(Value::Int64(5));

        // right = (a >= 5 AND a <= 5) AND (a <> 5) is unsatisfiable, should degrade.
        let result = split_partition_expr(base, split);
        assert_eq!(result.unwrap_err(), ExprSplitDegradeReason::EmptyBranch);
    }

    #[test]
    fn test_split_degrade_on_uint_one_sided_impossible_upper_bound() {
        // R: a < 10 (UInt64 domain)
        let base = col("a").lt(Value::UInt64(10));
        // S: a < 0 (impossible on UInt64)
        let split = col("a").lt(Value::UInt64(0));

        // left = (a < 10) AND (a < 0) is unsatisfiable on UInt64, should degrade.
        let result = split_partition_expr(base, split);
        assert_eq!(result.unwrap_err(), ExprSplitDegradeReason::EmptyBranch);
    }

    #[test]
    fn test_split_degrade_on_uint_one_sided_impossible_lower_bound() {
        // R: a < 10 (UInt64 domain)
        let base = col("a").lt(Value::UInt64(10));
        // S: a > u64::MAX (impossible on UInt64)
        let split = col("a").gt(Value::UInt64(u64::MAX));

        // left = (a < 10) AND (a > u64::MAX) is unsatisfiable on UInt64, should degrade.
        let result = split_partition_expr(base, split);
        assert_eq!(result.unwrap_err(), ExprSplitDegradeReason::EmptyBranch);
    }

    #[test]
    fn test_split_degrade_on_int_one_sided_impossible_upper_bound() {
        // R: a < 10 (Int64 domain)
        let base = col("a").lt(Value::Int64(10));
        // S: a < i64::MIN (impossible on Int64)
        let split = col("a").lt(Value::Int64(i64::MIN));

        // left = (a < 10) AND (a < i64::MIN) is unsatisfiable on Int64, should degrade.
        let result = split_partition_expr(base, split);
        assert_eq!(result.unwrap_err(), ExprSplitDegradeReason::EmptyBranch);
    }

    #[test]
    fn test_split_degrade_on_int_one_sided_impossible_lower_bound() {
        // R: a < 10 (Int64 domain)
        let base = col("a").lt(Value::Int64(10));
        // S: a > i64::MAX (impossible on Int64)
        let split = col("a").gt(Value::Int64(i64::MAX));

        // left = (a < 10) AND (a > i64::MAX) is unsatisfiable on Int64, should degrade.
        let result = split_partition_expr(base, split);
        assert_eq!(result.unwrap_err(), ExprSplitDegradeReason::EmptyBranch);
    }

    #[test]
    fn test_split_degrade_on_string_one_sided_impossible_upper_bound() {
        // R: s < "z" (String domain)
        let base = col("s").lt(Value::String("z".into()));
        // S: s < "" (impossible on String)
        let split = col("s").lt(Value::String("".into()));

        // left = (s < "z") AND (s < "") is unsatisfiable, should degrade.
        let result = split_partition_expr(base, split);
        assert_eq!(result.unwrap_err(), ExprSplitDegradeReason::EmptyBranch);
    }

    #[test]
    fn test_split_degrade_on_float64_one_sided_impossible_upper_bound() {
        // R: a < 10.0 (Float64 domain)
        let base = col("a").lt(Value::Float64(OrderedFloat(10.0)));
        // S: a < f64::MIN (impossible with finite-only float policy)
        let split = col("a").lt(Value::Float64(OrderedFloat(f64::MIN)));

        // left = (a < 10.0) AND (a < f64::MIN) is unsatisfiable, should degrade.
        let result = split_partition_expr(base, split);
        assert_eq!(result.unwrap_err(), ExprSplitDegradeReason::EmptyBranch);
    }

    #[test]
    fn test_split_degrade_on_float64_one_sided_impossible_lower_bound() {
        // R: a < 10.0 (Float64 domain)
        let base = col("a").lt(Value::Float64(OrderedFloat(10.0)));
        // S: a > f64::MAX (impossible with finite-only float policy)
        let split = col("a").gt(Value::Float64(OrderedFloat(f64::MAX)));

        // left = (a < 10.0) AND (a > f64::MAX) is unsatisfiable, should degrade.
        let result = split_partition_expr(base, split);
        assert_eq!(result.unwrap_err(), ExprSplitDegradeReason::EmptyBranch);
    }

    #[test]
    fn test_split_degrade_on_float32_one_sided_impossible_upper_bound() {
        // R: a < 10.0f32 (Float32 domain)
        let base = col("a").lt(Value::Float32(OrderedFloat(10.0)));
        // S: a < f32::MIN (impossible with finite-only float policy)
        let split = col("a").lt(Value::Float32(OrderedFloat(f32::MIN)));

        // left = (a < 10.0f32) AND (a < f32::MIN) is unsatisfiable, should degrade.
        let result = split_partition_expr(base, split);
        assert_eq!(result.unwrap_err(), ExprSplitDegradeReason::EmptyBranch);
    }

    #[test]
    fn test_split_degrade_on_float32_one_sided_impossible_lower_bound() {
        // R: a < 10.0f32 (Float32 domain)
        let base = col("a").lt(Value::Float32(OrderedFloat(10.0)));
        // S: a > f32::MAX (impossible with finite-only float policy)
        let split = col("a").gt(Value::Float32(OrderedFloat(f32::MAX)));

        // left = (a < 10.0f32) AND (a > f32::MAX) is unsatisfiable, should degrade.
        let result = split_partition_expr(base, split);
        assert_eq!(result.unwrap_err(), ExprSplitDegradeReason::EmptyBranch);
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
    fn test_validate_supported_expr_infinite_range_rejected() {
        // Infinity cannot be used in range predicates under finite-only float policy.
        let pos_inf = col("a").gt(Value::Float64(OrderedFloat(f64::INFINITY)));
        let neg_inf = col("a").lt(Value::Float32(OrderedFloat(f32::NEG_INFINITY)));
        assert!(validate_supported_expr(&pos_inf).is_err());
        assert!(validate_supported_expr(&neg_inf).is_err());
    }

    #[test]
    fn test_validate_supported_expr_nan_eq_rejected() {
        let expr = col("a").eq(Value::Float64(OrderedFloat(f64::NAN)));
        assert!(validate_supported_expr(&expr).is_err());
    }

    #[test]
    fn test_validate_supported_expr_infinite_eq_rejected() {
        let pos_inf = col("a").eq(Value::Float64(OrderedFloat(f64::INFINITY)));
        let neg_inf = col("a").not_eq(Value::Float32(OrderedFloat(f32::NEG_INFINITY)));
        assert!(validate_supported_expr(&pos_inf).is_err());
        assert!(validate_supported_expr(&neg_inf).is_err());
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
