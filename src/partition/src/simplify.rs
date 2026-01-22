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

//! Simplification utilities for partition expressions.
//!
//! The main use case is simplifying `MERGE PARTITION` generated expressions:
//! `expr1 OR expr2 OR ...`, where each expr is a conjunction of simple
//! comparisons on partition columns.

use std::collections::BTreeMap;
use std::ops::Bound;

use datatypes::value::Value;

use crate::expr::{Operand, PartitionExpr, RestrictedOp, col};

/// Attempts to simplify a merged partition expression (typically an `OR` of multiple partitions)
/// into an equivalent but shorter expression.
///
/// Falls back to the original expression if the simplifier can't prove equivalence.
pub fn simplify_merged_partition_expr(expr: PartitionExpr) -> PartitionExpr {
    try_simplify_merged_partition_expr(&expr).unwrap_or(expr)
}

fn try_simplify_merged_partition_expr(expr: &PartitionExpr) -> Option<PartitionExpr> {
    // Quick exit: not a disjunction.
    if !contains_or(expr) {
        return None;
    }

    let terms = parse_terms(expr)?;
    let terms = simplify_terms(terms)?;
    build_expr_from_terms(&terms)
}

fn contains_or(expr: &PartitionExpr) -> bool {
    if expr.op == RestrictedOp::Or {
        return true;
    }
    let lhs = match expr.lhs.as_ref() {
        Operand::Expr(e) => Some(e),
        _ => None,
    };
    let rhs = match expr.rhs.as_ref() {
        Operand::Expr(e) => Some(e),
        _ => None,
    };
    lhs.is_some_and(contains_or) || rhs.is_some_and(contains_or)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Term {
    // Only stores constrained columns. Missing column means unbounded.
    constraints: BTreeMap<String, Interval>,
}

impl Term {
    fn is_subset_of(&self, other: &Term) -> bool {
        // If `self` doesn't constrain a column that `other` does, `self` can't be a subset.
        for (col, other_interval) in &other.constraints {
            let Some(self_interval) = self.constraints.get(col) else {
                return false;
            };
            if !self_interval.is_subset_of(other_interval) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Interval {
    lower: Bound<Value>,
    upper: Bound<Value>,
}

impl Interval {
    fn unbounded() -> Self {
        Self {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        }
    }

    fn is_unbounded(&self) -> bool {
        matches!(self.lower, Bound::Unbounded) && matches!(self.upper, Bound::Unbounded)
    }

    fn apply_predicate(&mut self, op: &RestrictedOp, value: &Value) -> Option<()> {
        // Keep simplification conservative for NULL and `<>` constraints.
        if matches!(value, Value::Null) {
            return None;
        }

        match op {
            RestrictedOp::Eq => {
                let v = value.clone();
                // Ensure existing bounds contain `v`.
                if !self.contains_value(&v) {
                    return None;
                }
                self.lower = Bound::Included(v.clone());
                self.upper = Bound::Included(v);
            }
            RestrictedOp::Lt => self.update_upper(Bound::Excluded(value.clone())),
            RestrictedOp::LtEq => self.update_upper(Bound::Included(value.clone())),
            RestrictedOp::Gt => self.update_lower(Bound::Excluded(value.clone())),
            RestrictedOp::GtEq => self.update_lower(Bound::Included(value.clone())),
            RestrictedOp::NotEq | RestrictedOp::And | RestrictedOp::Or => return None,
        }

        if self.is_empty() {
            return None;
        }
        Some(())
    }

    fn contains_value(&self, value: &Value) -> bool {
        // `value` is within [lower, upper] taking inclusiveness into account.
        match &self.lower {
            Bound::Unbounded => {}
            Bound::Included(v) if value < v => return false,
            Bound::Excluded(v) if value <= v => return false,
            _ => {}
        }
        match &self.upper {
            Bound::Unbounded => {}
            Bound::Included(v) if value > v => return false,
            Bound::Excluded(v) if value >= v => return false,
            _ => {}
        }
        true
    }

    fn update_lower(&mut self, new_lower: Bound<Value>) {
        if cmp_lower(&new_lower, &self.lower).is_gt() {
            self.lower = new_lower;
        }
    }

    fn update_upper(&mut self, new_upper: Bound<Value>) {
        if cmp_upper(&new_upper, &self.upper).is_lt() {
            self.upper = new_upper;
        }
    }

    fn is_empty(&self) -> bool {
        let (Bound::Included(lv) | Bound::Excluded(lv), Bound::Included(uv) | Bound::Excluded(uv)) =
            (&self.lower, &self.upper)
        else {
            return false;
        };

        match lv.cmp(uv) {
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Equal => {
                matches!(self.lower, Bound::Excluded(_)) || matches!(self.upper, Bound::Excluded(_))
            }
        }
    }

    fn is_subset_of(&self, other: &Interval) -> bool {
        // self.lower >= other.lower and self.upper <= other.upper
        cmp_lower(&self.lower, &other.lower).is_ge() && cmp_upper(&self.upper, &other.upper).is_le()
    }

    fn union_if_mergeable(&self, other: &Interval) -> Option<UnionInterval> {
        // Ensure `left` starts no later than `right`.
        let (left, right) = match cmp_lower(&self.lower, &other.lower) {
            std::cmp::Ordering::Greater => (other, self),
            _ => (self, other),
        };

        if has_gap(&left.upper, &right.lower) {
            return None;
        }

        let lower = union_lower(&left.lower, &right.lower);
        let upper = union_upper(&left.upper, &right.upper);
        let interval = Interval { lower, upper };
        if interval.is_unbounded() {
            Some(UnionInterval::Unbounded)
        } else {
            Some(UnionInterval::Interval(interval))
        }
    }
}

enum UnionInterval {
    Unbounded,
    Interval(Interval),
}

fn cmp_lower(a: &Bound<Value>, b: &Bound<Value>) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;

    use Bound::*;

    match (a, b) {
        (Unbounded, Unbounded) => Equal,
        (Unbounded, _) => Less,
        (_, Unbounded) => Greater,
        (Included(av), Included(bv)) | (Excluded(av), Excluded(bv)) => av.cmp(bv),
        (Included(av), Excluded(bv)) => match av.cmp(bv) {
            Equal => Less,
            ord => ord,
        },
        (Excluded(av), Included(bv)) => match av.cmp(bv) {
            Equal => Greater,
            ord => ord,
        },
    }
}

fn cmp_upper(a: &Bound<Value>, b: &Bound<Value>) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;

    use Bound::*;

    match (a, b) {
        (Unbounded, Unbounded) => Equal,
        (Unbounded, _) => Greater,
        (_, Unbounded) => Less,
        (Included(av), Included(bv)) | (Excluded(av), Excluded(bv)) => av.cmp(bv),
        (Included(av), Excluded(bv)) => match av.cmp(bv) {
            Equal => Greater,
            ord => ord,
        },
        (Excluded(av), Included(bv)) => match av.cmp(bv) {
            Equal => Less,
            ord => ord,
        },
    }
}

fn has_gap(upper: &Bound<Value>, lower: &Bound<Value>) -> bool {
    use Bound::*;

    match (upper, lower) {
        (Unbounded, _) | (_, Unbounded) => false,
        (Included(u), Included(l)) => u < l,
        (Included(u), Excluded(l)) | (Excluded(u), Included(l)) | (Excluded(u), Excluded(l)) => {
            if u < l {
                return true;
            }
            if u > l {
                return false;
            }
            // u == l: gap only if both ends exclude the boundary
            matches!((upper, lower), (Excluded(_), Excluded(_)))
        }
    }
}

fn union_lower(a: &Bound<Value>, b: &Bound<Value>) -> Bound<Value> {
    use Bound::*;
    match (a, b) {
        (Unbounded, _) | (_, Unbounded) => Unbounded,
        (Included(av), Included(bv)) => {
            if av <= bv {
                Included(av.clone())
            } else {
                Included(bv.clone())
            }
        }
        (Excluded(av), Excluded(bv)) => {
            if av <= bv {
                Excluded(av.clone())
            } else {
                Excluded(bv.clone())
            }
        }
        (Included(av), Excluded(bv)) => match av.cmp(bv) {
            std::cmp::Ordering::Less => Included(av.clone()),
            std::cmp::Ordering::Greater => Excluded(bv.clone()),
            std::cmp::Ordering::Equal => Included(av.clone()),
        },
        (Excluded(av), Included(bv)) => match av.cmp(bv) {
            std::cmp::Ordering::Less => Excluded(av.clone()),
            std::cmp::Ordering::Greater => Included(bv.clone()),
            std::cmp::Ordering::Equal => Included(bv.clone()),
        },
    }
}

fn union_upper(a: &Bound<Value>, b: &Bound<Value>) -> Bound<Value> {
    use Bound::*;
    match (a, b) {
        (Unbounded, _) | (_, Unbounded) => Unbounded,
        (Included(av), Included(bv)) => {
            if av >= bv {
                Included(av.clone())
            } else {
                Included(bv.clone())
            }
        }
        (Excluded(av), Excluded(bv)) => {
            if av >= bv {
                Excluded(av.clone())
            } else {
                Excluded(bv.clone())
            }
        }
        (Included(av), Excluded(bv)) => match av.cmp(bv) {
            std::cmp::Ordering::Greater => Included(av.clone()),
            std::cmp::Ordering::Less => Excluded(bv.clone()),
            std::cmp::Ordering::Equal => Included(av.clone()),
        },
        (Excluded(av), Included(bv)) => match av.cmp(bv) {
            std::cmp::Ordering::Greater => Excluded(av.clone()),
            std::cmp::Ordering::Less => Included(bv.clone()),
            std::cmp::Ordering::Equal => Included(bv.clone()),
        },
    }
}

fn parse_terms(expr: &PartitionExpr) -> Option<Vec<Term>> {
    let mut or_terms = Vec::new();
    collect_or_terms(expr, &mut or_terms)?;

    let mut terms = Vec::with_capacity(or_terms.len());
    for term_expr in or_terms {
        terms.push(parse_one_term(term_expr)?);
    }
    Some(terms)
}

fn collect_or_terms<'a>(expr: &'a PartitionExpr, out: &mut Vec<&'a PartitionExpr>) -> Option<()> {
    if expr.op == RestrictedOp::Or {
        let Operand::Expr(lhs) = expr.lhs.as_ref() else {
            return None;
        };
        let Operand::Expr(rhs) = expr.rhs.as_ref() else {
            return None;
        };
        collect_or_terms(lhs, out)?;
        collect_or_terms(rhs, out)?;
        return Some(());
    }

    out.push(expr);
    Some(())
}

fn parse_one_term(expr: &PartitionExpr) -> Option<Term> {
    let mut preds = Vec::new();
    collect_and_preds(expr, &mut preds)?;

    let mut constraints: BTreeMap<String, Interval> = BTreeMap::new();
    for pred in preds {
        let (col, op, value) = parse_predicate(pred)?;
        let entry = constraints
            .entry(col.to_string())
            .or_insert_with(Interval::unbounded);
        entry.apply_predicate(op, value)?;
    }

    // Keep only effective constraints.
    constraints.retain(|_, interval| !interval.is_unbounded());
    Some(Term { constraints })
}

fn collect_and_preds<'a>(expr: &'a PartitionExpr, out: &mut Vec<&'a PartitionExpr>) -> Option<()> {
    if expr.op == RestrictedOp::And {
        let Operand::Expr(lhs) = expr.lhs.as_ref() else {
            return None;
        };
        let Operand::Expr(rhs) = expr.rhs.as_ref() else {
            return None;
        };
        collect_and_preds(lhs, out)?;
        collect_and_preds(rhs, out)?;
        return Some(());
    }

    if expr.op == RestrictedOp::Or {
        // Avoid `(a OR b) AND c`-style constructs (not representable without parentheses).
        return None;
    }

    out.push(expr);
    Some(())
}

fn parse_predicate(expr: &PartitionExpr) -> Option<(&str, &RestrictedOp, &Value)> {
    match (expr.lhs.as_ref(), expr.rhs.as_ref()) {
        (Operand::Column(col), Operand::Value(v)) => Some((col.as_str(), &expr.op, v)),
        (Operand::Value(v), Operand::Column(col)) => Some((col.as_str(), &expr.op, v)),
        _ => None,
    }
}

fn simplify_terms(mut terms: Vec<Term>) -> Option<Vec<Term>> {
    // Dedup exact duplicates.
    let mut unique = Vec::new();
    for t in terms.drain(..) {
        if !unique.contains(&t) {
            unique.push(t);
        }
    }
    terms = unique;

    loop {
        // Remove subsumed terms (more restrictive) in `OR`.
        let mut to_remove = vec![false; terms.len()];
        for i in 0..terms.len() {
            for j in 0..terms.len() {
                if i == j || to_remove[i] {
                    continue;
                }
                if terms[i].is_subset_of(&terms[j]) {
                    to_remove[i] = true;
                }
            }
        }
        let before = terms.len();
        terms = terms
            .into_iter()
            .enumerate()
            .filter_map(|(idx, t)| (!to_remove[idx]).then_some(t))
            .collect();

        // Try to merge a pair; restart on success.
        let mut merged = None;
        'outer: for i in 0..terms.len() {
            for j in (i + 1)..terms.len() {
                if let Some(t) = try_merge_terms(&terms[i], &terms[j]) {
                    merged = Some((i, j, t));
                    break 'outer;
                }
            }
        }

        if let Some((i, j, new_term)) = merged {
            let mut next = Vec::with_capacity(terms.len() - 1);
            for (idx, t) in terms.into_iter().enumerate() {
                if idx != i && idx != j {
                    next.push(t);
                }
            }
            next.push(new_term);
            terms = next;
            continue;
        }

        // Stable point: no more merges and no more subsumption.
        if terms.len() == before {
            break;
        }
    }

    Some(terms)
}

fn try_merge_terms(a: &Term, b: &Term) -> Option<Term> {
    // Find the only differing column (treat missing as unbounded).
    let mut diff_col: Option<&str> = None;

    let mut cols = std::collections::BTreeSet::new();
    cols.extend(a.constraints.keys().map(|s| s.as_str()));
    cols.extend(b.constraints.keys().map(|s| s.as_str()));

    for col in cols {
        let a_interval = a.constraints.get(col);
        let b_interval = b.constraints.get(col);
        if a_interval == b_interval {
            continue;
        }
        if diff_col.is_some() {
            return None;
        }
        diff_col = Some(col);
    }

    let diff_col = diff_col?;
    let a_interval = a.constraints.get(diff_col)?;
    let b_interval = b.constraints.get(diff_col)?;

    let union = a_interval.union_if_mergeable(b_interval)?;
    let mut constraints = a.constraints.clone();
    match union {
        UnionInterval::Unbounded => {
            constraints.remove(diff_col);
        }
        UnionInterval::Interval(interval) => {
            constraints.insert(diff_col.to_string(), interval);
        }
    }

    Some(Term { constraints })
}

fn build_expr_from_terms(terms: &[Term]) -> Option<PartitionExpr> {
    let mut term_exprs = Vec::with_capacity(terms.len());
    for term in terms {
        let expr = term_to_expr(term)?;
        term_exprs.push(expr);
    }

    // Can't represent a tautology in `PartitionExpr`.
    if term_exprs.is_empty() {
        return None;
    }

    if term_exprs.len() == 1 {
        return Some(term_exprs.pop().unwrap());
    }

    term_exprs.sort_by_key(|a| a.to_string());

    let mut iter = term_exprs.into_iter();
    let mut acc = iter.next()?;
    for next in iter {
        acc = PartitionExpr::new(Operand::Expr(acc), RestrictedOp::Or, Operand::Expr(next));
    }
    Some(acc)
}

fn term_to_expr(term: &Term) -> Option<PartitionExpr> {
    // Empty term would represent a tautology which can't be expressed here.
    if term.constraints.is_empty() {
        return None;
    }

    let mut exprs = Vec::new();
    for (column, interval) in &term.constraints {
        exprs.extend(interval_to_exprs(column, interval)?);
    }

    let mut iter = exprs.into_iter();
    let mut acc = iter.next()?;
    for next in iter {
        acc = acc.and(next);
    }
    Some(acc)
}

fn interval_to_exprs(column: &str, interval: &Interval) -> Option<Vec<PartitionExpr>> {
    use Bound::*;

    if interval.is_unbounded() {
        return Some(vec![]);
    }

    let lower = &interval.lower;
    let upper = &interval.upper;

    match (lower, upper) {
        (Included(lv), Included(uv)) if lv == uv => {
            return Some(vec![col(column).eq(lv.clone())]);
        }
        (Excluded(lv), Excluded(uv)) if lv == uv => return None,
        (Included(lv), Excluded(uv)) if lv == uv => return None,
        (Excluded(lv), Included(uv)) if lv == uv => return None,
        _ => {}
    }

    let mut exprs = Vec::new();
    match lower {
        Unbounded => {}
        Included(v) => exprs.push(col(column).gt_eq(v.clone())),
        Excluded(v) => exprs.push(col(column).gt(v.clone())),
    }
    match upper {
        Unbounded => {}
        Included(v) => exprs.push(col(column).lt_eq(v.clone())),
        Excluded(v) => exprs.push(col(column).lt(v.clone())),
    }

    Some(exprs)
}

#[cfg(test)]
mod tests {
    use datatypes::value::Value;

    use super::*;
    use crate::expr::Operand;

    fn or(lhs: PartitionExpr, rhs: PartitionExpr) -> PartitionExpr {
        PartitionExpr::new(Operand::Expr(lhs), RestrictedOp::Or, Operand::Expr(rhs))
    }

    #[test]
    fn simplify_common_factor_complement() {
        // device_id < 100 AND area < 'South'
        let left = col("device_id")
            .lt(Value::Int32(100))
            .and(col("area").lt(Value::String("South".into())));
        // device_id < 100 AND area >= 'South'
        let right = col("device_id")
            .lt(Value::Int32(100))
            .and(col("area").gt_eq(Value::String("South".into())));
        let merged = or(left, right);
        let simplified = simplify_merged_partition_expr(merged);
        assert_eq!(simplified.to_string(), "device_id < 100");
    }

    #[test]
    fn simplify_adjacent_ranges() {
        // host < 'h0' OR (host >= 'h0' AND host < 'h1') -> host < 'h1'
        let left = col("host").lt(Value::String("h0".into()));
        let right = col("host")
            .gt_eq(Value::String("h0".into()))
            .and(col("host").lt(Value::String("h1".into())));
        let merged = or(left, right);
        let simplified = simplify_merged_partition_expr(merged);
        assert_eq!(simplified.to_string(), "host < h1");
    }

    #[test]
    fn simplify_drop_upper_bound() {
        // a > 10 OR (a <= 10 AND a > 0) -> a > 0
        let left = col("a").gt(Value::Int32(10));
        let right = col("a")
            .lt_eq(Value::Int32(10))
            .and(col("a").gt(Value::Int32(0)));
        let merged = or(left, right);
        let simplified = simplify_merged_partition_expr(merged);
        assert_eq!(simplified.to_string(), "a > 0");
    }

    #[test]
    fn do_not_merge_hole_without_not_eq() {
        // a < 10 OR a > 10 can't be simplified without `a <> 10`.
        let left = col("a").lt(Value::Int32(10));
        let right = col("a").gt(Value::Int32(10));
        let merged = or(left, right);
        let simplified = simplify_merged_partition_expr(merged.clone());
        assert_eq!(simplified, merged);
    }
}
