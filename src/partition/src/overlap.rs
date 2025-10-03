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

//! Rule overlap and association utilities.
//!
//! This module provides pure functions to determine overlap relationships between
//! partition expressions and to associate rule sets.

use std::cmp::Ordering;
use std::ops::Bound;

use datatypes::value::OrderedF64;

use crate::collider::{AtomicExpr, Collider, GluonOp, NucleonExpr};
use crate::error::Result;
use crate::expr::PartitionExpr;

/// Check if two atomic expressions can be both satisfied, i.e., whether they
/// overlap on all common columns.
pub fn atomic_exprs_overlap(lhs: &AtomicExpr, rhs: &AtomicExpr) -> bool {
    // Merge-walk over columns since nucleons are sorted by (column, op, value)
    let mut lhs_index = 0;
    let mut rhs_index = 0;

    while lhs_index < lhs.nucleons.len() && rhs_index < rhs.nucleons.len() {
        let lhs_col = lhs.nucleons[lhs_index].column();
        let rhs_col = rhs.nucleons[rhs_index].column();

        match lhs_col.cmp(rhs_col) {
            Ordering::Equal => {
                // advance to the next column boundaries in both atomics
                let mut lhs_next = lhs_index;
                let mut rhs_next = rhs_index;
                while lhs_next < lhs.nucleons.len() && lhs.nucleons[lhs_next].column() == lhs_col {
                    lhs_next += 1;
                }
                while rhs_next < rhs.nucleons.len() && rhs.nucleons[rhs_next].column() == rhs_col {
                    rhs_next += 1;
                }

                let lhs_range = nucleons_to_range(&lhs.nucleons[lhs_index..lhs_next]);
                let rhs_range = nucleons_to_range(&rhs.nucleons[rhs_index..rhs_next]);

                if !lhs_range.overlaps_with(&rhs_range) {
                    return false;
                }

                lhs_index = lhs_next;
                rhs_index = rhs_next;
            }
            Ordering::Less => {
                // column appears only in `a`, skip all nucleons for this column
                let col = lhs_col;
                while lhs_index < lhs.nucleons.len() && lhs.nucleons[lhs_index].column() == col {
                    lhs_index += 1;
                }
            }
            Ordering::Greater => {
                // column appears only in `b`, skip all nucleons for this column
                let col = rhs_col;
                while rhs_index < rhs.nucleons.len() && rhs.nucleons[rhs_index].column() == col {
                    rhs_index += 1;
                }
            }
        }
    }

    true
}

/// Pairwise overlap check between two expression lists.
///
/// Returns true if two [`PartitionExpr`]s are overlapping (any pair of atomics overlaps).
fn expr_pair_overlap(lhs: &PartitionExpr, rhs: &PartitionExpr) -> Result<bool> {
    let binding = [lhs.clone(), rhs.clone()];
    let collider = Collider::new(&binding)?;
    // Split atomic exprs by source index
    let mut lhs_atoms = Vec::new();
    let mut rhs_atoms = Vec::new();
    for atomic in collider.atomic_exprs.iter() {
        if atomic.source_expr_index == 0 {
            lhs_atoms.push(atomic);
        } else {
            rhs_atoms.push(atomic);
        }
    }
    for lhs_atomic in &lhs_atoms {
        for rhs_atomic in &rhs_atoms {
            if atomic_exprs_overlap(lhs_atomic, rhs_atomic) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Associates each expression in `from_exprs` with indices of overlapping expressions in `to_exprs`.
///
/// Output vector length equals `from_exprs.len()`, and each inner vector contains indices into
/// `to_exprs` that overlap with the corresponding `from_exprs[i]`.
pub fn associate_from_to(
    from_exprs: &[PartitionExpr],
    to_exprs: &[PartitionExpr],
) -> Result<Vec<Vec<usize>>> {
    let mut result = Vec::with_capacity(from_exprs.len());
    for from in from_exprs.iter() {
        let mut targets = Vec::new();
        for (i, to) in to_exprs.iter().enumerate() {
            if expr_pair_overlap(from, to)? {
                targets.push(i);
            }
        }
        result.push(targets);
    }
    Ok(result)
}

/// Represents a value range derived from a group of nucleons for the same column
#[derive(Debug, Clone)]
struct ValueRange {
    lower: Bound<OrderedF64>,
    upper: Bound<OrderedF64>,
}

impl ValueRange {
    fn new() -> Self {
        Self {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        }
    }

    fn update_lower(&mut self, new_lower: Bound<OrderedF64>) {
        match (&self.lower, &new_lower) {
            (Bound::Unbounded, _) => self.lower = new_lower,
            (_, Bound::Unbounded) => {}
            (Bound::Included(cur), Bound::Included(new))
            | (Bound::Excluded(cur), Bound::Included(new))
            | (Bound::Included(cur), Bound::Excluded(new))
            | (Bound::Excluded(cur), Bound::Excluded(new)) => {
                if new > cur {
                    self.lower = new_lower;
                }
            }
        }
    }

    fn update_upper(&mut self, new_upper: Bound<OrderedF64>) {
        match (&self.upper, &new_upper) {
            (Bound::Unbounded, _) => self.upper = new_upper,
            (_, Bound::Unbounded) => {}
            (Bound::Included(cur), Bound::Included(new))
            | (Bound::Excluded(cur), Bound::Included(new))
            | (Bound::Included(cur), Bound::Excluded(new))
            | (Bound::Excluded(cur), Bound::Excluded(new)) => {
                if new < cur {
                    self.upper = new_upper;
                }
            }
        }
    }

    fn overlaps_with(&self, other: &Self) -> bool {
        fn no_overlap(upper: &Bound<OrderedF64>, lower: &Bound<OrderedF64>) -> bool {
            match (upper, lower) {
                (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
                // u], [l
                (Bound::Included(u), Bound::Included(l)) => u < l,
                // u], (l) or u), [l or u), (l)
                (Bound::Included(u), Bound::Excluded(l))
                | (Bound::Excluded(u), Bound::Included(l))
                | (Bound::Excluded(u), Bound::Excluded(l)) => u <= l,
            }
        }

        if no_overlap(&self.upper, &other.lower) || no_overlap(&other.upper, &self.lower) {
            return false;
        }
        true
    }
}

/// Convert nucleons for the same column into a ValueRange
fn nucleons_to_range(nucleons: &[NucleonExpr]) -> ValueRange {
    use GluonOp::*;

    let mut range = ValueRange::new();
    for n in nucleons {
        let v = n.value();
        match n.op() {
            Eq => {
                range.lower = Bound::Included(v);
                range.upper = Bound::Included(v);
                break;
            }
            Lt => range.update_upper(Bound::Excluded(v)),
            LtEq => range.update_upper(Bound::Included(v)),
            Gt => range.update_lower(Bound::Excluded(v)),
            GtEq => range.update_lower(Bound::Included(v)),
            NotEq => continue, // handled elsewhere as separate atomics
        }
    }
    range
}

#[cfg(test)]
mod tests {
    use datatypes::value::Value;

    use super::*;
    use crate::expr::{Operand, PartitionExpr, RestrictedOp, col};

    #[test]
    fn test_pair_overlap_simple() {
        let a = col("user_id")
            .gt_eq(Value::Int64(0))
            .and(col("user_id").lt(Value::Int64(100)));
        let b = col("user_id").eq(Value::Int64(50));
        assert!(expr_pair_overlap(&a, &b).unwrap());

        let c = col("user_id")
            .gt_eq(Value::Int64(100))
            .and(col("user_id").lt(Value::Int64(200)));
        assert!(!expr_pair_overlap(&a, &c).unwrap());
    }

    #[test]
    fn test_associate_from_to() {
        // from: [ [0,100), [100,200) ]
        let from = vec![
            col("user_id")
                .gt_eq(Value::Int64(0))
                .and(col("user_id").lt(Value::Int64(100))),
            col("user_id")
                .gt_eq(Value::Int64(100))
                .and(col("user_id").lt(Value::Int64(200))),
        ];
        // to: [ [0,150), [150,300) ]
        let to = vec![
            col("user_id")
                .gt_eq(Value::Int64(0))
                .and(col("user_id").lt(Value::Int64(150))),
            col("user_id")
                .gt_eq(Value::Int64(150))
                .and(col("user_id").lt(Value::Int64(300))),
        ];
        let assoc = associate_from_to(&from, &to).unwrap();
        assert_eq!(assoc.len(), 2);
        // [0,100) overlaps only with [0,150)
        assert_eq!(assoc[0], vec![0]);
        // [100,200) overlaps both [0,150) and [150,300)
        assert_eq!(assoc[1], vec![0, 1]);
    }

    #[test]
    fn test_expr_with_or() {
        // a: (user_id = 10 OR user_id = 20)
        let a = PartitionExpr::new(
            Operand::Expr(col("user_id").eq(Value::Int64(10))),
            RestrictedOp::Or,
            Operand::Expr(col("user_id").eq(Value::Int64(20))),
        );
        let b = col("user_id")
            .gt_eq(Value::Int64(15))
            .and(col("user_id").lt_eq(Value::Int64(25)));
        assert!(expr_pair_overlap(&a, &b).unwrap());
    }

    #[test]
    fn test_adjacent_ranges_non_overlap() {
        // [0, 100) vs [100, 200) -> no overlap
        let from = vec![
            col("k")
                .gt_eq(Value::Int64(0))
                .and(col("k").lt(Value::Int64(100))),
        ];
        let to = vec![
            col("k")
                .gt_eq(Value::Int64(100))
                .and(col("k").lt(Value::Int64(200))),
        ];
        let assoc = associate_from_to(&from, &to).unwrap();
        assert_eq!(assoc[0], Vec::<usize>::new());
    }

    #[test]
    fn test_multi_column_conflict_no_overlap() {
        // Left: a in [0,10) AND b >= 5
        let left = col("a")
            .gt_eq(Value::Int64(0))
            .and(col("a").lt(Value::Int64(10)))
            .and(col("b").gt_eq(Value::Int64(5)));
        // Right: a = 9 AND b < 5 -> conflict on b
        let right = col("a")
            .eq(Value::Int64(9))
            .and(col("b").lt(Value::Int64(5)));
        assert!(!expr_pair_overlap(&left, &right).unwrap());
    }

    #[test]
    fn test_disjoint_columns_overlap() {
        // Different columns don't constrain each other => satisfiable together
        let from = vec![col("a").eq(Value::Int64(1))];
        let to = vec![col("b").eq(Value::Int64(2))];
        let assoc = associate_from_to(&from, &to).unwrap();
        assert_eq!(assoc[0], vec![0]);
    }

    #[test]
    fn test_boundary_inclusive_exclusive() {
        // Left: a <= 10 AND a >= 10 => a = 10
        let left = col("a")
            .lt_eq(Value::Int64(10))
            .and(col("a").gt_eq(Value::Int64(10)));
        // Right: a = 10 -> overlap
        let right_eq = col("a").eq(Value::Int64(10));
        assert!(expr_pair_overlap(&left, &right_eq).unwrap());

        // Left: a < 10, Right: a = 10 -> no overlap
        let left_lt = col("a").lt(Value::Int64(10));
        assert!(!expr_pair_overlap(&left_lt, &right_eq).unwrap());
    }
}
