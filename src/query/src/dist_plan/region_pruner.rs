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

//! Region pruning based on partition constraint satisfaction.
//!
//! This module implements constraint-based region pruning for distributed query execution.
//! It determines which data regions can potentially contain results for a given query by
//! comparing query constraints against partition definitions.
//!
//! ## Key Components
//!
//! - [`ConstraintPruner`]: Main pruning algorithm that uses constraint satisfaction
//! - Takes PartitionExpr list from PredicateExtractor and partition info to return region IDs
//! - Clean separation: PredicateExtractor → Vec&lt;PartitionExpr&gt; → ConstraintPruner → Vec&lt;RegionId&gt;
//!
//! ## Algorithm Overview
//!
//! The pruning algorithm works by:
//! 1. **Unified Normalization**: Create a single Collider with both query and partition expressions
//! 2. **Atomic Decomposition**: Break down complex expressions into atomic constraints (AND of nucleons)
//! 3. **Constraint Matching**: Check if query atomics can be satisfied by partition atomics
//! 4. **Conservative Fallback**: Include regions when constraint evaluation is uncertain
//!
//! ## Constraint Satisfaction Logic
//!
//! - **Query atomics**: Represent OR conditions (any atomic can satisfy the query)
//! - **Partition atomics**: Represent OR conditions (any atomic defines the partition boundary)
//! - **Nucleon compatibility**: Individual column constraints must be logically compatible
//!
//! ## Nucleon Compatibility Rules
//!
//! The algorithm checks if query constraints can be satisfied by partition constraints:
//!
//! | Query | Partition | Compatible |
//! |-------|-----------|------------|
//! | `x = 5` | `x = 5` | ✓ (exact match) |
//! | `x = 5` | `x >= 3` | ✓ (5 satisfies >= 3) |
//! | `x >= 10` | `x < 5` | ✗ (no overlap) |
//! | `x >= 10` | `x >= 5` | ✓ (overlapping ranges) |
//!
//! ## Example
//!
//! Given:
//! - Query: `timestamp >= 100 AND timestamp < 200`
//! - Partition 1: `timestamp >= 0 AND timestamp < 150`
//! - Partition 2: `timestamp >= 150 AND timestamp < 300`
//!
//! Both partitions would be included because:
//! - Partition 1: Range [100,150) overlaps with query [100,200)
//! - Partition 2: Range [150,200) overlaps with query [100,200)

use common_telemetry::debug;
use partition::collider::{AtomicExpr, Collider, GluonOp};
use partition::expr::PartitionExpr;
use partition::manager::PartitionInfo;
use store_api::storage::RegionId;
use GluonOp::*;

use crate::error::Result;

/// Unified constraint-based pruning algorithm
pub struct ConstraintPruner;

impl ConstraintPruner {
    /// Prune regions using constraint satisfaction approach
    /// Takes query expressions and partition info, returns matching region IDs
    pub fn prune_regions(
        query_expressions: &[PartitionExpr],
        partitions: &[PartitionInfo],
    ) -> Result<Vec<RegionId>> {
        Self::prune_regions_impl(query_expressions, partitions)
    }

    /// Internal implementation for region pruning
    fn prune_regions_impl(
        query_expressions: &[PartitionExpr],
        partitions: &[PartitionInfo],
    ) -> Result<Vec<RegionId>> {
        if query_expressions.is_empty() {
            // No constraints, return all regions
            return Ok(partitions.iter().map(|p| p.id).collect());
        }

        // Collect all partition expressions for unified normalization
        let all_partition_expressions: Vec<PartitionExpr> = partitions
            .iter()
            .filter_map(|p| p.partition_expr.clone())
            .collect();

        if all_partition_expressions.is_empty() {
            // No partition expressions to match against, return all regions conservatively
            return Ok(partitions.iter().map(|p| p.id).collect());
        }

        // Create unified collider with both query and partition expressions for consistent normalization
        let mut all_expressions = query_expressions.to_vec();
        all_expressions.extend(all_partition_expressions.iter().cloned());

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

        let mut candidate_regions = Vec::new();

        // Process each partition with direct region mapping
        for partition in partitions {
            if let Some(ref partition_expr) = partition.partition_expr {
                // Find atomic expressions for this specific partition expression
                let partition_expr_index = all_partition_expressions
                    .iter()
                    .position(|expr| expr == partition_expr);

                if let Some(index) = partition_expr_index {
                    let adjusted_index = query_expressions.len() + index;
                    let partition_atomics: Vec<&AtomicExpr> = collider
                        .atomic_exprs
                        .iter()
                        .filter(|atomic| atomic.source_expr_index == adjusted_index)
                        .collect();

                    // Check overlap between query and partition atomic expressions
                    if Self::atomic_sets_overlap(&query_atomics, &partition_atomics) {
                        candidate_regions.push(partition.id);
                    }
                } else {
                    // Partition expression not found in unified list, include conservatively
                    debug!("Partition expression not found in unified list for region {}, including conservatively", partition.id);
                    candidate_regions.push(partition.id);
                }
            } else {
                // No partition expression, include conservatively
                debug!(
                    "No partition expression for region {}, including conservatively",
                    partition.id
                );
                candidate_regions.push(partition.id);
            }
        }

        debug!(
            "Constraint pruning: {} -> {} regions",
            partitions.len(),
            candidate_regions.len()
        );

        Ok(candidate_regions)
    }

    /// Check if query atomic expressions overlap with partition atomic expressions
    /// Each atomic expression represents a constraint that can satisfy the query/partition
    fn atomic_sets_overlap(
        query_atomics: &[&AtomicExpr],
        partition_atomics: &[&AtomicExpr],
    ) -> bool {
        // Query represents OR of atomic expressions (any query atomic can be satisfied)
        // Partition represents OR of atomic expressions (any partition atomic defines the partition)
        // A partition satisfies a query if any query atomic can be satisfied by any partition atomic

        for query_atomic in query_atomics {
            for partition_atomic in partition_atomics {
                if Self::atomic_constraint_satisfied(query_atomic, partition_atomic) {
                    return true;
                }
            }
        }

        // If no atomic expressions can be satisfied, partition cannot satisfy query
        false
    }

    /// Check if a single atomic constraint (AND of nucleons) can be satisfied
    fn atomic_constraint_satisfied(
        query_atomic: &AtomicExpr,
        partition_atomic: &AtomicExpr,
    ) -> bool {
        // Both atomic expressions represent AND conditions
        // Query atomic: must ALL be satisfiable
        // Partition atomic: defines what values are available in this partition

        // For each query nucleon, check if it's compatible with partition nucleons on the same column
        for query_nucleon in &query_atomic.nucleons {
            let column = query_nucleon.column();

            // Find partition nucleons for the same column
            let partition_nucleons_for_column: Vec<_> = partition_atomic
                .nucleons
                .iter()
                .filter(|p_nucleon| p_nucleon.column() == column)
                .collect();

            if !partition_nucleons_for_column.is_empty() {
                // Check if query nucleon is compatible with ALL partition nucleons for this column
                // Since partition nucleons are ANDed together, ALL must be satisfied
                let compatible = partition_nucleons_for_column
                    .iter()
                    .all(|p_nucleon| Self::nucleons_compatible(query_nucleon, p_nucleon));

                if !compatible {
                    return false;
                }
            }
            // If no partition nucleons for this column, assume compatible (no constraint on this column)
        }

        true
    }

    /// Check if two nucleons are compatible (can be satisfied simultaneously)
    fn nucleons_compatible(
        query_nucleon: &partition::collider::NucleonExpr,
        partition_nucleon: &partition::collider::NucleonExpr,
    ) -> bool {
        // Both nucleons operate on normalized values from separate colliders
        // We need to compare the actual original values, not normalized ones
        // For now, use the normalized values but fix the logic
        let query_val = query_nucleon.value();
        let partition_val = partition_nucleon.value();

        match (query_nucleon.op(), partition_nucleon.op()) {
            // Query equality with partition equality - must be exact match
            (Eq, Eq) => query_val == partition_val,

            // Query equality with partition bounds - check if equality value satisfies partition bound
            (Eq, Lt) => query_val < partition_val,
            (Eq, LtEq) => query_val <= partition_val,
            (Eq, Gt) => query_val > partition_val,
            (Eq, GtEq) => query_val >= partition_val,

            // Query bounds with partition equality - check if partition value satisfies query bound
            (Lt, Eq) => partition_val < query_val,
            (LtEq, Eq) => partition_val <= query_val,
            (Gt, Eq) => partition_val > query_val,
            (GtEq, Eq) => partition_val >= query_val,

            // Both bounds - check for range overlap
            (Lt, Gt) | (LtEq, Gt) | (Lt, GtEq) | (LtEq, GtEq) => {
                // Query: < query_val, Partition: > partition_val
                // Compatible if there's overlap: partition_val < query_val
                partition_val < query_val
            }
            (Gt, Lt) | (GtEq, Lt) | (Gt, LtEq) | (GtEq, LtEq) => {
                // Query: > query_val, Partition: < partition_val
                // Compatible if there's overlap: query_val < partition_val
                query_val < partition_val
            }

            // Same direction bounds - check for overlap
            (Lt, Lt) | (LtEq, Lt) | (Lt, LtEq) | (LtEq, LtEq) => {
                // Both upper bounds - compatible (take the smaller bound)
                true
            }
            (Gt, Gt) | (GtEq, Gt) | (Gt, GtEq) | (GtEq, GtEq) => {
                // Both lower bounds - compatible (take the larger bound)
                true
            }

            // NotEq - for simplicity, assume compatible unless exact conflict
            (NotEq, Eq) => query_val != partition_val,
            (Eq, NotEq) => query_val != partition_val,
            (NotEq, NotEq) => true,          // Both are NotEq, compatible
            (NotEq, _) | (_, NotEq) => true, // NotEq with bounds, assume compatible
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
        let pruned = ConstraintPruner::prune_regions(&query_exprs, &partitions).unwrap();

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

        let pruned = ConstraintPruner::prune_regions(&query_exprs, &partitions).unwrap();

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
        let pruned = ConstraintPruner::prune_regions(&query_exprs, &partitions).unwrap();

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
        let pruned = ConstraintPruner::prune_regions(&constraints, &partitions).unwrap();

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
        let pruned = ConstraintPruner::prune_regions(&query_exprs, &partitions).unwrap();

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
        let pruned = ConstraintPruner::prune_regions(&query_exprs, &partitions).unwrap();

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
        let pruned = ConstraintPruner::prune_regions(&query_exprs, &partitions).unwrap();

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
        let pruned = ConstraintPruner::prune_regions(&query_exprs, &partitions).unwrap();

        // Region 1: [0,100) intersects with [50,∞) -> includes [50,100)
        // Region 2: [100,200) is fully contained in [50,∞)
        assert_eq!(pruned.len(), 2);
        assert!(pruned.contains(&RegionId::new(1, 1)));
        assert!(pruned.contains(&RegionId::new(1, 2)));
    }
}
