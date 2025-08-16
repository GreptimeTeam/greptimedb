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

//! [`ConstraintPruner`] prunes partition info based on given expressions.

use std::cmp::Ordering;

use ahash::{HashMap, HashSet};
use common_telemetry::debug;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::{OrderedF64, OrderedFloat, Value};
use partition::collider::{AtomicExpr, Collider, GluonOp, NucleonExpr};
use partition::expr::{Operand, PartitionExpr};
use partition::manager::PartitionInfo;
use store_api::storage::RegionId;
use GluonOp::*;

use crate::error::Result;

pub struct ConstraintPruner;

impl ConstraintPruner {
    /// Prune regions using constraint satisfaction approach
    ///
    /// Takes query expressions and partition info, returns matching region IDs
    pub fn prune_regions(
        query_expressions: &[PartitionExpr],
        partitions: &[PartitionInfo],
        column_datatypes: HashMap<String, ConcreteDataType>,
    ) -> Result<Vec<RegionId>> {
        if query_expressions.is_empty() || partitions.is_empty() {
            // No constraints, return all regions
            return Ok(partitions.iter().map(|p| p.id).collect());
        }

        debug!("query expressions: {query_expressions:?}");

        // Collect all partition expressions for unified normalization
        let mut expression_to_partition = HashMap::default();
        let mut all_partition_expressions = Vec::with_capacity(partitions.len());
        for partition in partitions {
            if let Some(expr) = &partition.partition_expr {
                expression_to_partition.insert(all_partition_expressions.len(), partition.id);
                all_partition_expressions.push(expr.clone());
            }
        }
        if all_partition_expressions.is_empty() {
            return Ok(partitions.iter().map(|p| p.id).collect());
        }

        // Create unified collider with both query and partition expressions for consistent normalization
        let mut all_expressions = query_expressions.to_vec();
        all_expressions.extend(all_partition_expressions.iter().cloned());
        if !Self::normalize_datatype(&mut all_expressions, &column_datatypes) {
            return Ok(partitions.iter().map(|p| p.id).collect());
        }

        let collider = match Collider::new(&all_expressions) {
            Ok(collider) => collider,
            Err(err) => {
                debug!(
                    "Failed to create unified collider: {}, returning all regions conservatively",
                    err
                );
                return Ok(partitions.iter().map(|p| p.id).collect());
            }
        };

        // Extract query atomic expressions (first N expressions in the collider)
        let query_atomics: Vec<&AtomicExpr> = collider
            .atomic_exprs
            .iter()
            .filter(|atomic| atomic.source_expr_index < query_expressions.len())
            .collect();

        let mut candidate_regions = HashSet::default();

        for region_atomics in collider
            .atomic_exprs
            .iter()
            .filter(|atomic| atomic.source_expr_index >= query_expressions.len())
        {
            if Self::atomic_sets_overlap(&query_atomics, region_atomics) {
                let partition_expr_index =
                    region_atomics.source_expr_index - query_expressions.len();
                candidate_regions.insert(expression_to_partition[&partition_expr_index]);
            }
        }

        debug!(
            "Constraint pruning: {} -> {} regions",
            partitions.len(),
            candidate_regions.len()
        );

        Ok(candidate_regions.into_iter().collect())
    }

    fn atomic_sets_overlap(query_atomics: &[&AtomicExpr], partition_atomic: &AtomicExpr) -> bool {
        for query_atomic in query_atomics {
            if Self::atomic_constraint_satisfied(query_atomic, partition_atomic) {
                return true;
            }
        }

        false
    }

    fn normalize_datatype(
        all_expressions: &mut Vec<PartitionExpr>,
        column_datatypes: &HashMap<String, ConcreteDataType>,
    ) -> bool {
        for expr in all_expressions {
            if !Self::normalize_expr_datatype(&mut expr.lhs, &mut expr.rhs, column_datatypes) {
                return false;
            }
        }
        true
    }

    fn normalize_expr_datatype(
        lhs: &mut Operand,
        rhs: &mut Operand,
        column_datatypes: &HashMap<String, ConcreteDataType>,
    ) -> bool {
        match (lhs, rhs) {
            (Operand::Expr(lhs_expr), Operand::Expr(rhs_expr)) => {
                Self::normalize_expr_datatype(
                    &mut lhs_expr.lhs,
                    &mut lhs_expr.rhs,
                    column_datatypes,
                ) && Self::normalize_expr_datatype(
                    &mut rhs_expr.lhs,
                    &mut rhs_expr.rhs,
                    column_datatypes,
                )
            }
            (Operand::Column(col_name), Operand::Value(val))
            | (Operand::Value(val), Operand::Column(col_name)) => {
                let Some(datatype) = column_datatypes.get(col_name) else {
                    debug!("Column {} not found from type set, skip pruning", col_name);
                    return false;
                };

                match datatype {
                    ConcreteDataType::Int8(_)
                    | ConcreteDataType::Int16(_)
                    | ConcreteDataType::Int32(_)
                    | ConcreteDataType::Int64(_) => {
                        let Some(new_lit) = val.as_i64() else {
                            debug!("Value {:?} cannot be converted to i64", val);
                            return false;
                        };
                        *val = Value::Int64(new_lit);
                    }

                    ConcreteDataType::UInt8(_)
                    | ConcreteDataType::UInt16(_)
                    | ConcreteDataType::UInt32(_)
                    | ConcreteDataType::UInt64(_) => {
                        let Some(new_lit) = val.as_u64() else {
                            debug!("Value {:?} cannot be converted to u64", val);
                            return false;
                        };
                        *val = Value::UInt64(new_lit);
                    }

                    ConcreteDataType::Float32(_) | ConcreteDataType::Float64(_) => {
                        let Some(new_lit) = val.as_f64_lossy() else {
                            debug!("Value {:?} cannot be converted to f64", val);
                            return false;
                        };

                        *val = Value::Float64(OrderedFloat(new_lit));
                    }

                    ConcreteDataType::String(_) | ConcreteDataType::Boolean(_) => {
                        // no operation needed
                    }

                    ConcreteDataType::Decimal128(_)
                    | ConcreteDataType::Binary(_)
                    | ConcreteDataType::Date(_)
                    | ConcreteDataType::Timestamp(_)
                    | ConcreteDataType::Time(_)
                    | ConcreteDataType::Duration(_)
                    | ConcreteDataType::Interval(_)
                    | ConcreteDataType::List(_)
                    | ConcreteDataType::Dictionary(_)
                    | ConcreteDataType::Struct(_)
                    | ConcreteDataType::Json(_)
                    | ConcreteDataType::Null(_)
                    | ConcreteDataType::Vector(_) => {
                        debug!("Unsupported data type {datatype}");
                        return false;
                    }
                }

                true
            }
            _ => false,
        }
    }

    /// Check if a single atomic constraint can be satisfied
    fn atomic_constraint_satisfied(
        query_atomic: &AtomicExpr,
        partition_atomic: &AtomicExpr,
    ) -> bool {
        let mut query_index = 0;
        let mut partition_index = 0;

        while query_index < query_atomic.nucleons.len()
            && partition_index < partition_atomic.nucleons.len()
        {
            let query_col = query_atomic.nucleons[query_index].column();
            let partition_col = partition_atomic.nucleons[partition_index].column();

            match query_col.cmp(partition_col) {
                Ordering::Equal => {
                    let mut query_index_for_next_col = query_index;
                    let mut partition_index_for_next_col = partition_index;

                    while query_index_for_next_col < query_atomic.nucleons.len()
                        && query_atomic.nucleons[query_index_for_next_col].column() == query_col
                    {
                        query_index_for_next_col += 1;
                    }
                    while partition_index_for_next_col < partition_atomic.nucleons.len()
                        && partition_atomic.nucleons[partition_index_for_next_col].column()
                            == partition_col
                    {
                        partition_index_for_next_col += 1;
                    }

                    let query_range = Self::nucleons_to_range(
                        &query_atomic.nucleons[query_index..query_index_for_next_col],
                    );
                    let partition_range = Self::nucleons_to_range(
                        &partition_atomic.nucleons[partition_index..partition_index_for_next_col],
                    );

                    debug!("Comparing two ranges, {query_range:?} and {partition_range:?}");

                    query_index = query_index_for_next_col;
                    partition_index = partition_index_for_next_col;

                    if !query_range.overlaps_with(&partition_range) {
                        return false;
                    }
                }
                Ordering::Less => {
                    // Query column comes before partition column - skip query column
                    while query_index < query_atomic.nucleons.len()
                        && query_atomic.nucleons[query_index].column() == query_col
                    {
                        query_index += 1;
                    }
                }
                Ordering::Greater => {
                    // Partition column comes before query column - skip partition column
                    while partition_index < partition_atomic.nucleons.len()
                        && partition_atomic.nucleons[partition_index].column() == partition_col
                    {
                        partition_index += 1;
                    }
                }
            }
        }

        true
    }

    /// Convert a slice of nucleons (all for the same column) into a ValueRange
    fn nucleons_to_range(nucleons: &[NucleonExpr]) -> ValueRange {
        let mut range = ValueRange::new();

        for nucleon in nucleons {
            let value = nucleon.value();
            match nucleon.op() {
                Eq => {
                    // Equality constraint: both min and max are the same value
                    range.min_value = Some((value, true));
                    range.max_value = Some((value, true));
                    break; // Equality is the most restrictive, no need to check others
                }
                Lt => {
                    // Less than: update max_value if it's more restrictive
                    let new_max = (value, false);
                    if range.max_value.is_none() || range.max_value.as_ref().unwrap().0 > value {
                        range.max_value = Some(new_max);
                    }
                }
                LtEq => {
                    // Less than or equal: update max_value if it's more restrictive
                    let new_max = (value, true);
                    if range.max_value.is_none() || range.max_value.as_ref().unwrap().0 > value {
                        range.max_value = Some(new_max);
                    }
                }
                Gt => {
                    // Greater than: update min_value if it's more restrictive
                    let new_min = (value, false);
                    if range.min_value.is_none() || range.min_value.as_ref().unwrap().0 < value {
                        range.min_value = Some(new_min);
                    }
                }
                GtEq => {
                    // Greater than or equal: update min_value if it's more restrictive
                    let new_min = (value, true);
                    if range.min_value.is_none() || range.min_value.as_ref().unwrap().0 < value {
                        range.min_value = Some(new_min);
                    }
                }
                NotEq => {
                    // NotEq should be broke into two other AtomicExprs
                    continue;
                }
            }
        }

        range
    }
}

/// Represents a value range derived from a group of nucleons for the same column
#[derive(Debug, Clone)]
struct ValueRange {
    min_value: Option<(OrderedF64, bool)>, // (value, inclusive)
    max_value: Option<(OrderedF64, bool)>, // (value, inclusive)
}

impl ValueRange {
    fn new() -> Self {
        Self {
            min_value: None,
            max_value: None,
        }
    }

    /// Check if this range overlaps with another range
    fn overlaps_with(&self, other: &ValueRange) -> bool {
        match (
            &self.min_value,
            &self.max_value,
            &other.min_value,
            &other.max_value,
        ) {
            // If either range is completely unconstrained, they always overlap
            (None, None, _, _) | (_, _, None, None) => true,

            (Some(lower), None, None, Some(upper)) | (None, Some(upper), Some(lower), None) => {
                if lower.0 == upper.0 {
                    lower.1 && upper.1
                } else {
                    lower.0 < upper.0
                }
            }

            // Both ranges have specific bounds - check for actual overlap
            _ => {
                // Get effective bounds (use infinity when unspecified)
                let self_min = self
                    .min_value
                    .unwrap_or((OrderedF64::from(-f64::INFINITY), true));
                let self_max = self
                    .max_value
                    .unwrap_or((OrderedF64::from(f64::INFINITY), true));
                let other_min = other
                    .min_value
                    .unwrap_or((OrderedF64::from(-f64::INFINITY), true));
                let other_max = other
                    .max_value
                    .unwrap_or((OrderedF64::from(f64::INFINITY), true));

                // Check if ranges overlap: self_min <= other_max && other_min <= self_max
                let self_min_le_other_max = if self_min.0 < other_max.0 {
                    true
                } else if self_min.0 == other_max.0 {
                    self_min.1 && other_max.1
                } else {
                    false
                };
                let other_min_le_self_max = if other_min.0 < self_max.0 {
                    true
                } else if other_min.0 == self_max.0 {
                    other_min.1 && self_max.1
                } else {
                    false
                };

                self_min_le_other_max && other_min_le_self_max
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datatypes::value::Value;
    use partition::expr::{col, Operand, PartitionExpr, RestrictedOp};
    use store_api::storage::RegionId;

    use super::*;

    fn create_test_partition_info(region_id: u64, expr: Option<PartitionExpr>) -> PartitionInfo {
        PartitionInfo {
            id: RegionId::new(1, region_id as u32),
            partition_expr: expr,
        }
    }

    #[test]
    fn test_constraint_pruning_equality() {
        let partitions = vec![
            // Region 1: user_id >= 0 AND user_id < 100
            create_test_partition_info(
                1,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(0))
                        .and(col("user_id").lt(Value::Int64(100))),
                ),
            ),
            // Region 2: user_id >= 100 AND user_id < 200
            create_test_partition_info(
                2,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(100))
                        .and(col("user_id").lt(Value::Int64(200))),
                ),
            ),
            // Region 3: user_id >= 200 AND user_id < 300
            create_test_partition_info(
                3,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(200))
                        .and(col("user_id").lt(Value::Int64(300))),
                ),
            ),
        ];

        // Query: user_id = 150 (should only match Region 2)
        let query_exprs = vec![col("user_id").eq(Value::Int64(150))];
        let mut column_datatypes = HashMap::default();
        column_datatypes.insert("user_id".to_string(), ConcreteDataType::int64_datatype());
        let pruned =
            ConstraintPruner::prune_regions(&query_exprs, &partitions, column_datatypes).unwrap();

        // Should include Region 2, and potentially others due to conservative approach
        assert!(pruned.contains(&RegionId::new(1, 2)));
    }

    #[test]
    fn test_constraint_pruning_in_list() {
        let partitions = vec![
            // Region 1: user_id >= 0 AND user_id < 100
            create_test_partition_info(
                1,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(0))
                        .and(col("user_id").lt(Value::Int64(100))),
                ),
            ),
            // Region 2: user_id >= 100 AND user_id < 200
            create_test_partition_info(
                2,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(100))
                        .and(col("user_id").lt(Value::Int64(200))),
                ),
            ),
            // Region 3: user_id >= 200 AND user_id < 300
            create_test_partition_info(
                3,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(200))
                        .and(col("user_id").lt(Value::Int64(300))),
                ),
            ),
        ];

        // Query: user_id IN (50, 150, 250) - should match all regions
        let query_exprs = vec![PartitionExpr::new(
            Operand::Expr(PartitionExpr::new(
                Operand::Expr(col("user_id").eq(Value::Int64(50))),
                RestrictedOp::Or,
                Operand::Expr(col("user_id").eq(Value::Int64(150))),
            )),
            RestrictedOp::Or,
            Operand::Expr(col("user_id").eq(Value::Int64(250))),
        )];

        let mut column_datatypes = HashMap::default();
        column_datatypes.insert("user_id".to_string(), ConcreteDataType::int64_datatype());
        let pruned =
            ConstraintPruner::prune_regions(&query_exprs, &partitions, column_datatypes).unwrap();

        // Should include regions that can satisfy any of the values
        assert!(!pruned.is_empty());
    }

    #[test]
    fn test_constraint_pruning_range() {
        let partitions = vec![
            // Region 1: user_id >= 0 AND user_id < 100
            create_test_partition_info(
                1,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(0))
                        .and(col("user_id").lt(Value::Int64(100))),
                ),
            ),
            // Region 2: user_id >= 100 AND user_id < 200
            create_test_partition_info(
                2,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(100))
                        .and(col("user_id").lt(Value::Int64(200))),
                ),
            ),
            // Region 3: user_id >= 200 AND user_id < 300
            create_test_partition_info(
                3,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(200))
                        .and(col("user_id").lt(Value::Int64(300))),
                ),
            ),
        ];

        // Query: user_id >= 150 (should include regions that can satisfy this constraint)
        let query_exprs = vec![col("user_id").gt_eq(Value::Int64(150))];
        let mut column_datatypes = HashMap::default();
        column_datatypes.insert("user_id".to_string(), ConcreteDataType::int64_datatype());
        let pruned =
            ConstraintPruner::prune_regions(&query_exprs, &partitions, column_datatypes).unwrap();

        // With constraint-based approach:
        // Region 1: [0, 100) - user_id >= 150 is not satisfiable
        // Region 2: [100, 200) - user_id >= 150 is satisfiable in range [150, 200)
        // Region 3: [200, 300) - user_id >= 150 is satisfiable (all values >= 200 satisfy user_id >= 150)
        // Conservative approach may include more regions, but should at least include regions 2 and 3
        assert!(pruned.len() >= 2);
        assert!(pruned.contains(&RegionId::new(1, 2))); // Region 2 should be included
        assert!(pruned.contains(&RegionId::new(1, 3))); // Region 3 should be included
    }

    #[test]
    fn test_prune_regions_no_constraints() {
        let partitions = vec![
            create_test_partition_info(1, None),
            create_test_partition_info(2, None),
        ];

        let constraints = vec![];
        let column_datatypes = HashMap::default();
        let pruned =
            ConstraintPruner::prune_regions(&constraints, &partitions, column_datatypes).unwrap();

        // No constraints should return all regions
        assert_eq!(pruned.len(), 2);
    }

    #[test]
    fn test_prune_regions_with_simple_equality() {
        let partitions = vec![
            // Region 1: user_id >= 0 AND user_id < 100
            create_test_partition_info(
                1,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(0))
                        .and(col("user_id").lt(Value::Int64(100))),
                ),
            ),
            // Region 2: user_id >= 100 AND user_id < 200
            create_test_partition_info(
                2,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(100))
                        .and(col("user_id").lt(Value::Int64(200))),
                ),
            ),
            // Region 3: user_id >= 200 AND user_id < 300
            create_test_partition_info(
                3,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(200))
                        .and(col("user_id").lt(Value::Int64(300))),
                ),
            ),
        ];

        // Query: user_id = 150 (should only match Region 2 which contains values [100, 200))
        let query_exprs = vec![col("user_id").eq(Value::Int64(150))];
        let mut column_datatypes = HashMap::default();
        column_datatypes.insert("user_id".to_string(), ConcreteDataType::int64_datatype());
        let pruned =
            ConstraintPruner::prune_regions(&query_exprs, &partitions, column_datatypes).unwrap();

        // user_id = 150 should match Region 2 ([100, 200)) and potentially others due to conservative approach
        assert!(pruned.contains(&RegionId::new(1, 2)));
    }

    #[test]
    fn test_prune_regions_with_or_constraint() {
        let partitions = vec![
            // Region 1: user_id >= 0 AND user_id < 100
            create_test_partition_info(
                1,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(0))
                        .and(col("user_id").lt(Value::Int64(100))),
                ),
            ),
            // Region 2: user_id >= 100 AND user_id < 200
            create_test_partition_info(
                2,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(100))
                        .and(col("user_id").lt(Value::Int64(200))),
                ),
            ),
            // Region 3: user_id >= 200 AND user_id < 300
            create_test_partition_info(
                3,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(200))
                        .and(col("user_id").lt(Value::Int64(300))),
                ),
            ),
        ];

        // Query: user_id = 50 OR user_id = 150 OR user_id = 250 - should match all 3 regions
        let expr1 = col("user_id").eq(Value::Int64(50));
        let expr2 = col("user_id").eq(Value::Int64(150));
        let expr3 = col("user_id").eq(Value::Int64(250));

        let or_expr = PartitionExpr::new(
            Operand::Expr(PartitionExpr::new(
                Operand::Expr(expr1),
                RestrictedOp::Or,
                Operand::Expr(expr2),
            )),
            RestrictedOp::Or,
            Operand::Expr(expr3),
        );

        let query_exprs = vec![or_expr];
        let mut column_datatypes = HashMap::default();
        column_datatypes.insert("user_id".to_string(), ConcreteDataType::int64_datatype());
        let pruned =
            ConstraintPruner::prune_regions(&query_exprs, &partitions, column_datatypes).unwrap();

        // Should match all 3 regions: 50 matches Region 1, 150 matches Region 2, 250 matches Region 3
        assert_eq!(pruned.len(), 3);
        assert!(pruned.contains(&RegionId::new(1, 1)));
        assert!(pruned.contains(&RegionId::new(1, 2)));
        assert!(pruned.contains(&RegionId::new(1, 3)));
    }

    #[test]
    fn test_constraint_pruning_no_match() {
        let partitions = vec![
            // Region 1: user_id >= 0 AND user_id < 100
            create_test_partition_info(
                1,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(0))
                        .and(col("user_id").lt(Value::Int64(100))),
                ),
            ),
            // Region 2: user_id >= 100 AND user_id < 200
            create_test_partition_info(
                2,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(100))
                        .and(col("user_id").lt(Value::Int64(200))),
                ),
            ),
        ];

        // Query: user_id = 300 (should match no regions)
        let query_exprs = vec![col("user_id").eq(Value::Int64(300))];
        let mut column_datatypes = HashMap::default();
        column_datatypes.insert("user_id".to_string(), ConcreteDataType::int64_datatype());
        let pruned =
            ConstraintPruner::prune_regions(&query_exprs, &partitions, column_datatypes).unwrap();

        // Should match no regions since 300 is outside both partition ranges
        assert_eq!(pruned.len(), 0);
    }

    #[test]
    fn test_constraint_pruning_partial_match() {
        let partitions = vec![
            // Region 1: user_id >= 0 AND user_id < 100
            create_test_partition_info(
                1,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(0))
                        .and(col("user_id").lt(Value::Int64(100))),
                ),
            ),
            // Region 2: user_id >= 100 AND user_id < 200
            create_test_partition_info(
                2,
                Some(
                    col("user_id")
                        .gt_eq(Value::Int64(100))
                        .and(col("user_id").lt(Value::Int64(200))),
                ),
            ),
        ];

        // Query: user_id >= 50 (should match both regions partially)
        let query_exprs = vec![col("user_id").gt_eq(Value::Int64(50))];
        let mut column_datatypes = HashMap::default();
        column_datatypes.insert("user_id".to_string(), ConcreteDataType::int64_datatype());
        let pruned =
            ConstraintPruner::prune_regions(&query_exprs, &partitions, column_datatypes).unwrap();

        // Region 1: [0,100) intersects with [50,∞) -> includes [50,100)
        // Region 2: [100,200) is fully contained in [50,∞)
        assert_eq!(pruned.len(), 2);
        assert!(pruned.contains(&RegionId::new(1, 1)));
        assert!(pruned.contains(&RegionId::new(1, 2)));
    }
}
