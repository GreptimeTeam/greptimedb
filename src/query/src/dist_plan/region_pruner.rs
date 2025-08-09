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
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use common_telemetry::debug;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};
use partition::expr::PartitionExpr;
use partition::manager::PartitionInfo;
use store_api::storage::RegionId;

use crate::dist_plan::predicate_extractor::RangeConstraint;
use crate::dist_plan::PredicateExtractor;
use crate::error::Result;

/// Represents the range boundary extracted from a partition expression
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionRange {
    pub column: String,
    pub min: Option<ScalarValue>,
    pub max: Option<ScalarValue>,
    pub min_inclusive: bool,
    pub max_inclusive: bool,
}

impl PartitionRange {
    /// Check if this partition range overlaps with a query constraint
    pub fn overlaps_with(&self, constraint: &RangeConstraint) -> bool {
        if self.column != constraint.column {
            return false;
        }

        // Check if ranges are disjoint
        // Case 1: partition max < constraint min
        if let (Some(ref p_max), Some(ref c_min)) = (&self.max, &constraint.min) {
            match p_max.partial_cmp(c_min) {
                Some(std::cmp::Ordering::Less) => return false,
                Some(std::cmp::Ordering::Equal) => {
                    // Equal values only overlap if both bounds are inclusive
                    if !self.max_inclusive || !constraint.min_inclusive {
                        return false;
                    }
                }
                Some(std::cmp::Ordering::Greater) => {} // Continue checking
                None => {
                    // Incomparable types, conservatively assume overlap
                    debug!("Incomparable types in range overlap check, assuming overlap");
                }
            }
        }

        // Case 2: constraint max < partition min
        if let (Some(ref c_max), Some(ref p_min)) = (&constraint.max, &self.min) {
            match c_max.partial_cmp(p_min) {
                Some(std::cmp::Ordering::Less) => return false,
                Some(std::cmp::Ordering::Equal) => {
                    // Equal values only overlap if both bounds are inclusive
                    if !constraint.max_inclusive || !self.min_inclusive {
                        return false;
                    }
                }
                Some(std::cmp::Ordering::Greater) => {} // Continue checking
                None => {
                    // Incomparable types, conservatively assume overlap
                    debug!("Incomparable types in range overlap check, assuming overlap");
                }
            }
        }

        // If we reach here, ranges overlap
        true
    }
}

/// Core pruning algorithm for range partitions
pub struct RangePartitionPruner;

impl RangePartitionPruner {
    /// Prune regions based on range constraints and partition expressions
    pub fn prune_regions(
        constraints: &[RangeConstraint],
        partitions: &[PartitionInfo],
    ) -> Result<Vec<RegionId>> {
        if constraints.is_empty() {
            // No constraints, return all regions
            return Ok(partitions.iter().map(|p| p.id).collect());
        }

        let mut candidate_regions = Vec::new();

        for partition in partitions {
            if let Some(ref partition_expr) = partition.partition_expr {
                match Self::extract_partition_ranges(partition_expr) {
                    Ok(partition_ranges) => {
                        if Self::ranges_intersect(constraints, &partition_ranges) {
                            candidate_regions.push(partition.id);
                        }
                    }
                    Err(err) => {
                        debug!(
                            "Failed to extract ranges from partition expression for region {}: {}, including region conservatively",
                            partition.id, err
                        );
                        // Conservative: include region if we can't parse the expression
                        candidate_regions.push(partition.id);
                    }
                }
            } else {
                debug!(
                    "No partition expression for region {}, including region conservatively",
                    partition.id
                );
                // Conservative: include regions with no partition expression
                candidate_regions.push(partition.id);
            }
        }

        debug!(
            "Region pruning: {} -> {} regions based on {} constraints",
            partitions.len(),
            candidate_regions.len(),
            constraints.len()
        );

        Ok(candidate_regions)
    }

    /// Extract range boundaries from a partition expression
    /// Convert PartitionExpr to LogicalExpr and use existing constraint extraction
    fn extract_partition_ranges(
        partition_expr: &PartitionExpr,
    ) -> Result<HashMap<String, PartitionRange>> {
        // Convert PartitionExpr to DataFusion LogicalExpr
        let logical_expr = match partition_expr.try_as_logical_expr() {
            Ok(expr) => expr,
            Err(_) => {
                // If conversion fails, fall back to conservative approach
                debug!(
                    "Failed to convert PartitionExpr to LogicalExpr, using conservative approach"
                );
                return Ok(HashMap::new());
            }
        };

        // Extract partition column names from the expression string representation
        let expr_str = partition_expr.to_string();
        debug!("Converting partition expression to ranges: {}", expr_str);

        // For now, extract column names by simple pattern matching
        // This could be enhanced to be more sophisticated
        let partition_columns = Self::extract_column_names_from_expression(&expr_str);

        let mut fields = vec![];
        for col in &partition_columns {
            fields.push(Field::new(col.as_str(), DataType::Int64, true));
        }
        // Add a dummy field if no columns found
        if fields.is_empty() {
            fields.push(Field::new("dummy", DataType::Int64, true));
        }

        let schema = Arc::new(Schema::new(fields));
        let df_schema = DFSchema::try_from(schema.clone()).unwrap();

        // Create a dummy plan with the filter
        let logical_plan =
            LogicalPlanBuilder::from(LogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(df_schema),
            }))
            .filter(logical_expr)
            .unwrap()
            .build()
            .unwrap();

        let constraints = match PredicateExtractor::extract_range_constraints(
            &logical_plan,
            &partition_columns,
        ) {
            Ok(constraints) => constraints,
            Err(_) => {
                // If extraction fails, fall back to conservative approach
                debug!("Failed to extract constraints from partition expression, using conservative approach");
                return Ok(HashMap::new());
            }
        };

        // Convert constraints to partition ranges
        let mut ranges = HashMap::new();
        for constraint in constraints {
            let range = PartitionRange {
                column: constraint.column.clone(),
                min: constraint.min,
                max: constraint.max,
                min_inclusive: constraint.min_inclusive,
                max_inclusive: constraint.max_inclusive,
            };
            ranges.insert(constraint.column, range);
        }

        debug!(
            "Extracted {} partition ranges from expression",
            ranges.len()
        );
        Ok(ranges)
    }

    /// Extract column names from partition expression string
    /// This is a simple pattern-based approach that could be enhanced
    fn extract_column_names_from_expression(expr_str: &str) -> Vec<String> {
        // Simple regex-based extraction - could be improved
        // For now, look for common column patterns
        let mut columns = Vec::new();

        // Common partition column names in time-series databases
        let common_columns = vec![
            "ts",
            "timestamp",
            "time",
            "date",
            "created_at",
            "updated_at",
        ];

        for col in common_columns {
            if expr_str.contains(col) {
                columns.push(col.to_string());
            }
        }

        // If no common columns found, try to extract any identifier-like patterns
        if columns.is_empty() {
            // This is a fallback - in practice, partition columns should be known
            // from the table metadata
            if let Some(start) = expr_str.find(char::is_alphabetic) {
                let remaining = &expr_str[start..];
                if let Some(end) = remaining.find(|c: char| !c.is_alphanumeric() && c != '_') {
                    let potential_column = &remaining[..end];
                    if !potential_column.is_empty() {
                        columns.push(potential_column.to_string());
                    }
                }
            }
        }

        debug!("Extracted column names from '{}': {:?}", expr_str, columns);
        columns
    }

    /// Check if query constraints intersect with partition ranges
    fn ranges_intersect(
        constraints: &[RangeConstraint],
        partition_ranges: &HashMap<String, PartitionRange>,
    ) -> bool {
        // For each constraint, check if it overlaps with the corresponding partition range
        for constraint in constraints {
            if let Some(partition_range) = partition_ranges.get(&constraint.column) {
                if !partition_range.overlaps_with(constraint) {
                    // No overlap found for this column, partition is excluded
                    return false;
                }
            }
            // If no partition range for this column, conservatively assume overlap
        }

        // All constraints overlap with partition ranges
        true
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::ScalarValue;
    use datatypes::value::Value;
    use partition::expr::col;
    use store_api::storage::RegionId;

    use super::*;

    fn create_test_partition_info(region_id: u64, expr: Option<PartitionExpr>) -> PartitionInfo {
        PartitionInfo {
            id: RegionId::new(1, region_id as u32),
            partition_expr: expr,
        }
    }

    #[test]
    fn test_range_overlap_basic() {
        // Partition: [100, 200)
        let partition_range = PartitionRange {
            column: "ts".to_string(),
            min: Some(ScalarValue::Int64(Some(100))),
            max: Some(ScalarValue::Int64(Some(200))),
            min_inclusive: true,
            max_inclusive: false,
        };

        // Query: ts >= 150
        let constraint = RangeConstraint {
            column: "ts".to_string(),
            min: Some(ScalarValue::Int64(Some(150))),
            max: None,
            min_inclusive: true,
            max_inclusive: true,
        };

        assert!(partition_range.overlaps_with(&constraint));
    }

    #[test]
    fn test_range_no_overlap() {
        // Partition: [100, 200)
        let partition_range = PartitionRange {
            column: "ts".to_string(),
            min: Some(ScalarValue::Int64(Some(100))),
            max: Some(ScalarValue::Int64(Some(200))),
            min_inclusive: true,
            max_inclusive: false,
        };

        // Query: ts >= 300 (no overlap)
        let constraint = RangeConstraint {
            column: "ts".to_string(),
            min: Some(ScalarValue::Int64(Some(300))),
            max: None,
            min_inclusive: true,
            max_inclusive: true,
        };

        assert!(!partition_range.overlaps_with(&constraint));
    }

    #[test]
    fn test_extract_simple_partition_range() {
        // Create expression: ts >= 100
        let expr = col("ts").gt_eq(Value::Int64(100));

        // New implementation should extract actual ranges
        let ranges = RangePartitionPruner::extract_partition_ranges(&expr).unwrap();
        assert_eq!(ranges.len(), 1);
        let range = ranges.get("ts").unwrap();
        assert_eq!(range.min, Some(ScalarValue::Int64(Some(100))));
        assert!(range.min_inclusive);
        assert_eq!(range.max, None); // No upper bound
    }

    #[test]
    fn test_extract_compound_partition_range() {
        // Create expression: ts >= 100 AND ts < 200
        let expr = col("ts")
            .gt_eq(Value::Int64(100))
            .and(col("ts").lt(Value::Int64(200)));

        // New implementation should extract actual ranges
        let ranges = RangePartitionPruner::extract_partition_ranges(&expr).unwrap();
        assert_eq!(ranges.len(), 1);
        let range = ranges.get("ts").unwrap();
        assert_eq!(range.min, Some(ScalarValue::Int64(Some(100))));
        assert!(range.min_inclusive);
        assert_eq!(range.max, Some(ScalarValue::Int64(Some(200))));
        assert!(!range.max_inclusive);
    }

    #[test]
    fn test_prune_regions_with_constraints() {
        let partitions = vec![
            // Region 1: ts >= 0 AND ts < 100
            create_test_partition_info(
                1,
                Some(
                    col("ts")
                        .gt_eq(Value::Int64(0))
                        .and(col("ts").lt(Value::Int64(100))),
                ),
            ),
            // Region 2: ts >= 100 AND ts < 200
            create_test_partition_info(
                2,
                Some(
                    col("ts")
                        .gt_eq(Value::Int64(100))
                        .and(col("ts").lt(Value::Int64(200))),
                ),
            ),
            // Region 3: ts >= 200 AND ts < 300
            create_test_partition_info(
                3,
                Some(
                    col("ts")
                        .gt_eq(Value::Int64(200))
                        .and(col("ts").lt(Value::Int64(300))),
                ),
            ),
        ];

        // Query constraint: ts >= 150 AND ts < 250
        let constraints = vec![RangeConstraint {
            column: "ts".to_string(),
            min: Some(ScalarValue::Int64(Some(150))),
            max: Some(ScalarValue::Int64(Some(250))),
            min_inclusive: true,
            max_inclusive: false,
        }];

        let pruned = RangePartitionPruner::prune_regions(&constraints, &partitions).unwrap();

        // With actual range parsing, should only return regions that overlap with [150, 250):
        // Region 1: [0, 100) - no overlap
        // Region 2: [100, 200) - overlap with [150, 250) at [150, 200)
        // Region 3: [200, 300) - overlap with [150, 250) at [200, 250)
        assert_eq!(pruned.len(), 2); // Only regions 2 and 3 returned
        assert!(!pruned.contains(&RegionId::new(1, 1))); // Region 1 excluded
        assert!(pruned.contains(&RegionId::new(1, 2))); // Region 2 included
        assert!(pruned.contains(&RegionId::new(1, 3))); // Region 3 included
    }

    #[test]
    fn test_prune_regions_no_constraints() {
        let partitions = vec![
            create_test_partition_info(1, None),
            create_test_partition_info(2, None),
        ];

        let constraints = vec![];
        let pruned = RangePartitionPruner::prune_regions(&constraints, &partitions).unwrap();

        // No constraints should return all regions
        assert_eq!(pruned.len(), 2);
    }
}
