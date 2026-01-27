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

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use datatypes::value::{OrderedF64, Value};

use crate::collider::{AtomicExpr, Collider, GluonOp, NucleonExpr};
use crate::expr::{Operand, PartitionExpr, RestrictedOp, col};

/// Attempts to simplify a merged partition expression (typically an `OR` of multiple partitions)
/// into an equivalent but shorter expression.
///
/// Falls back to the original expression if the simplifier can't prove equivalence.
pub fn simplify_merged_partition_expr(expr: PartitionExpr) -> PartitionExpr {
    try_simplify_merged_partition_expr(&expr).unwrap_or(expr)
}

type DenormValues = BTreeMap<String, BTreeMap<OrderedF64, Value>>;

fn try_simplify_merged_partition_expr(expr: &PartitionExpr) -> Option<PartitionExpr> {
    let collider = Collider::new(std::slice::from_ref(expr)).ok()?;
    if collider.atomic_exprs.len() <= 1 {
        return None;
    }

    let denorm_values = build_denorm_values(&collider)?;

    let mut terms = Vec::with_capacity(collider.atomic_exprs.len());
    for atomic in &collider.atomic_exprs {
        terms.push(term_from_atomic(atomic, &denorm_values)?);
    }

    let terms = simplify_terms(terms)?;
    build_expr_from_terms(&terms, &denorm_values)
}

fn build_denorm_values(collider: &Collider<'_>) -> Option<DenormValues> {
    let mut values = DenormValues::new();
    for (column, pairs) in &collider.normalized_values {
        let mut map = BTreeMap::new();
        for (value, normalized) in pairs {
            // Keep simplification conservative for NULL semantics.
            if matches!(value, Value::Null) {
                return None;
            }
            map.insert(*normalized, value.clone());
        }
        values.insert(column.clone(), map);
    }
    Some(values)
}

fn term_from_atomic(atomic: &AtomicExpr, denorm_values: &DenormValues) -> Option<Term> {
    let mut constraints = BTreeMap::new();

    let mut i = 0;
    while i < atomic.nucleons.len() {
        let column = atomic.nucleons[i].column();
        if !denorm_values.contains_key(column) {
            return None;
        }
        let start = i;
        while i < atomic.nucleons.len() && atomic.nucleons[i].column() == column {
            i += 1;
        }

        let interval = interval_from_nucleons(&atomic.nucleons[start..i])?;
        if !interval.is_unbounded() {
            constraints.insert(column.to_string(), interval);
        }
    }

    Some(Term { constraints })
}

fn interval_from_nucleons(nucleons: &[NucleonExpr]) -> Option<Interval> {
    let mut interval = Interval::unbounded();
    for nucleon in nucleons {
        interval.apply_nucleon(nucleon.op(), nucleon.value())?;
    }
    Some(interval)
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
    lower: Bound<OrderedF64>,
    upper: Bound<OrderedF64>,
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

    fn apply_nucleon(&mut self, op: &GluonOp, value: OrderedF64) -> Option<()> {
        match op {
            GluonOp::Eq => {
                // Ensure existing bounds contain `value`.
                if !self.contains_value(&value) {
                    return None;
                }
                self.lower = Bound::Included(value);
                self.upper = Bound::Included(value);
            }
            GluonOp::Lt => self.update_upper(Bound::Excluded(value)),
            GluonOp::LtEq => self.update_upper(Bound::Included(value)),
            GluonOp::Gt => self.update_lower(Bound::Excluded(value)),
            GluonOp::GtEq => self.update_lower(Bound::Included(value)),
            GluonOp::NotEq => return None,
        }

        if self.is_empty() {
            return None;
        }
        Some(())
    }

    fn contains_value(&self, value: &OrderedF64) -> bool {
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

    fn update_lower(&mut self, new_lower: Bound<OrderedF64>) {
        if cmp_lower(&new_lower, &self.lower).is_gt() {
            self.lower = new_lower;
        }
    }

    fn update_upper(&mut self, new_upper: Bound<OrderedF64>) {
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

        let lower = left.lower;
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

fn cmp_lower(a: &Bound<OrderedF64>, b: &Bound<OrderedF64>) -> std::cmp::Ordering {
    // Lower bound ordering:
    // - Unbounded is -∞ (smallest)
    // - For the same value, Included is smaller (less restrictive) than Excluded.
    fn lower_key(bound: &Bound<OrderedF64>) -> (u8, Option<OrderedF64>, u8) {
        use Bound::*;

        match bound {
            Unbounded => (0, None, 0),
            Included(v) => (1, Some(*v), 0),
            Excluded(v) => (1, Some(*v), 1),
        }
    }

    lower_key(a).cmp(&lower_key(b))
}

fn cmp_upper(a: &Bound<OrderedF64>, b: &Bound<OrderedF64>) -> std::cmp::Ordering {
    // Upper bound ordering:
    // - Unbounded is +∞ (largest)
    // - For the same value, Excluded is smaller (more restrictive) than Included.
    fn upper_key(bound: &Bound<OrderedF64>) -> (u8, Option<OrderedF64>, u8) {
        use Bound::*;

        match bound {
            Unbounded => (1, None, 0),
            Included(v) => (0, Some(*v), 1),
            Excluded(v) => (0, Some(*v), 0),
        }
    }

    upper_key(a).cmp(&upper_key(b))
}

fn has_gap(upper: &Bound<OrderedF64>, lower: &Bound<OrderedF64>) -> bool {
    use Bound::*;

    match (upper, lower) {
        (Unbounded, _) | (_, Unbounded) => false,
        (Included(u), Included(l)) => u < l,
        (Included(u) | Excluded(u), Included(l) | Excluded(l)) => {
            u < l || (u == l && matches!((upper, lower), (Excluded(_), Excluded(_))))
        }
    }
}

fn union_upper(a: &Bound<OrderedF64>, b: &Bound<OrderedF64>) -> Bound<OrderedF64> {
    match cmp_upper(a, b) {
        std::cmp::Ordering::Less => *b,
        std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => *a,
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

    let mut cols = BTreeSet::new();
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

fn build_expr_from_terms(terms: &[Term], denorm_values: &DenormValues) -> Option<PartitionExpr> {
    let mut term_exprs = Vec::with_capacity(terms.len());
    for term in terms {
        let expr = term_to_expr(term, denorm_values)?;
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

fn term_to_expr(term: &Term, denorm_values: &DenormValues) -> Option<PartitionExpr> {
    // Empty term would represent a tautology which can't be expressed here.
    if term.constraints.is_empty() {
        return None;
    }

    let mut exprs = Vec::new();
    for (column, interval) in &term.constraints {
        exprs.extend(interval_to_exprs(column, interval, denorm_values)?);
    }

    let mut iter = exprs.into_iter();
    let mut acc = iter.next()?;
    for next in iter {
        acc = acc.and(next);
    }
    Some(acc)
}

fn interval_to_exprs(
    column: &str,
    interval: &Interval,
    denorm_values: &DenormValues,
) -> Option<Vec<PartitionExpr>> {
    use Bound::*;

    if interval.is_unbounded() {
        return Some(vec![]);
    }

    let col_values = denorm_values.get(column)?;

    let lower = &interval.lower;
    let upper = &interval.upper;

    match (lower, upper) {
        (Included(lv), Included(uv)) if lv == uv => {
            return Some(vec![col(column).eq(col_values.get(lv)?.clone())]);
        }
        (Excluded(lv), Excluded(uv)) if lv == uv => return None,
        (Included(lv), Excluded(uv)) if lv == uv => return None,
        (Excluded(lv), Included(uv)) if lv == uv => return None,
        _ => {}
    }

    let mut exprs = Vec::new();
    match lower {
        Unbounded => {}
        Included(v) => exprs.push(col(column).gt_eq(col_values.get(v)?.clone())),
        Excluded(v) => exprs.push(col(column).gt(col_values.get(v)?.clone())),
    }
    match upper {
        Unbounded => {}
        Included(v) => exprs.push(col(column).lt_eq(col_values.get(v)?.clone())),
        Excluded(v) => exprs.push(col(column).lt(col_values.get(v)?.clone())),
    }

    Some(exprs)
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use datatypes::value::{OrderedFloat, Value};

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

    #[test]
    fn interval_bound_helpers() {
        use std::cmp::Ordering::*;

        use Bound::*;

        let v0 = OrderedFloat(0.0f64);
        let v1 = OrderedFloat(1.0f64);

        // cmp_lower: Unbounded < Included(v) < Excluded(v) and increasing by value.
        let lower_order = [
            Unbounded,
            Included(v0),
            Excluded(v0),
            Included(v1),
            Excluded(v1),
        ];
        for pair in lower_order.windows(2) {
            assert_eq!(cmp_lower(&pair[0], &pair[1]), Less);
            assert_eq!(cmp_lower(&pair[1], &pair[0]), Greater);
        }
        for bound in &lower_order {
            assert_eq!(cmp_lower(bound, bound), Equal);
        }

        // cmp_upper: Excluded(v) < Included(v) and increasing by value; Unbounded is +∞ (largest).
        let upper_order = [
            Excluded(v0),
            Included(v0),
            Excluded(v1),
            Included(v1),
            Unbounded,
        ];
        for pair in upper_order.windows(2) {
            assert_eq!(cmp_upper(&pair[0], &pair[1]), Less);
            assert_eq!(cmp_upper(&pair[1], &pair[0]), Greater);
        }
        for bound in &upper_order {
            assert_eq!(cmp_upper(bound, bound), Equal);
        }

        // has_gap: Unbounded never contributes a gap.
        assert!(!has_gap(&Unbounded, &Included(v0)));
        assert!(!has_gap(&Excluded(v0), &Unbounded));
        // Separated bounds always have a gap.
        assert!(has_gap(&Included(v0), &Included(v1)));
        assert!(has_gap(&Excluded(v0), &Included(v1)));
        assert!(!has_gap(&Included(v1), &Included(v0)));
        assert!(!has_gap(&Excluded(v1), &Included(v0)));
        // Touching at boundary has a gap only if both ends exclude.
        assert!(!has_gap(&Included(v0), &Included(v0)));
        assert!(!has_gap(&Included(v0), &Excluded(v0)));
        assert!(!has_gap(&Excluded(v0), &Included(v0)));
        assert!(has_gap(&Excluded(v0), &Excluded(v0)));

        // union_upper: choose the less restrictive upper bound (max under cmp_upper).
        assert_eq!(union_upper(&Unbounded, &Included(v0)), Unbounded);
        assert_eq!(union_upper(&Included(v0), &Unbounded), Unbounded);
        assert_eq!(union_upper(&Included(v0), &Included(v1)), Included(v1));
        assert_eq!(union_upper(&Excluded(v1), &Included(v0)), Excluded(v1));
        assert_eq!(union_upper(&Excluded(v0), &Included(v0)), Included(v0));
        assert_eq!(union_upper(&Included(v0), &Excluded(v0)), Included(v0));
        assert_eq!(union_upper(&Excluded(v0), &Excluded(v0)), Excluded(v0));
        assert_eq!(union_upper(&Included(v0), &Included(v0)), Included(v0));
    }
}
