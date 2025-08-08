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

use std::collections::{HashMap, HashSet};

use common_telemetry::debug;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::{Result as DfResult, ScalarValue};
use datafusion_expr::{Expr, LogicalPlan, Operator};

/// Represents a range constraint extracted from query predicates
#[derive(Debug, Clone, PartialEq)]
pub struct RangeConstraint {
    pub column: String,
    pub min: Option<ScalarValue>, // None = unbounded below
    pub max: Option<ScalarValue>, // None = unbounded above
    pub min_inclusive: bool,
    pub max_inclusive: bool,
}

impl RangeConstraint {
    /// Create a new range constraint
    pub fn new(column: String) -> Self {
        Self {
            column,
            min: None,
            max: None,
            min_inclusive: true,
            max_inclusive: true,
        }
    }

    /// Set lower bound
    pub fn with_min(mut self, value: ScalarValue, inclusive: bool) -> Self {
        self.min = Some(value);
        self.min_inclusive = inclusive;
        self
    }

    /// Set upper bound  
    pub fn with_max(mut self, value: ScalarValue, inclusive: bool) -> Self {
        self.max = Some(value);
        self.max_inclusive = inclusive;
        self
    }

    /// Combine two constraints on the same column
    /// Returns None if constraints are contradictory
    pub fn intersect(&self, other: &RangeConstraint) -> Option<RangeConstraint> {
        if self.column != other.column {
            return None;
        }

        let mut result = RangeConstraint::new(self.column.clone());

        // Determine the more restrictive lower bound
        match (&self.min, &other.min) {
            (Some(a), Some(b)) => {
                match a.partial_cmp(b) {
                    Some(std::cmp::Ordering::Greater) => {
                        result.min = self.min.clone();
                        result.min_inclusive = self.min_inclusive;
                    }
                    Some(std::cmp::Ordering::Less) => {
                        result.min = other.min.clone();
                        result.min_inclusive = other.min_inclusive;
                    }
                    Some(std::cmp::Ordering::Equal) => {
                        result.min = self.min.clone();
                        result.min_inclusive = self.min_inclusive && other.min_inclusive;
                    }
                    None => return None, // Incomparable types
                }
            }
            (Some(_), None) => {
                result.min = self.min.clone();
                result.min_inclusive = self.min_inclusive;
            }
            (None, Some(_)) => {
                result.min = other.min.clone();
                result.min_inclusive = other.min_inclusive;
            }
            (None, None) => {}
        }

        // Determine the more restrictive upper bound
        match (&self.max, &other.max) {
            (Some(a), Some(b)) => {
                match a.partial_cmp(b) {
                    Some(std::cmp::Ordering::Less) => {
                        result.max = self.max.clone();
                        result.max_inclusive = self.max_inclusive;
                    }
                    Some(std::cmp::Ordering::Greater) => {
                        result.max = other.max.clone();
                        result.max_inclusive = other.max_inclusive;
                    }
                    Some(std::cmp::Ordering::Equal) => {
                        result.max = self.max.clone();
                        result.max_inclusive = self.max_inclusive && other.max_inclusive;
                    }
                    None => return None, // Incomparable types
                }
            }
            (Some(_), None) => {
                result.max = self.max.clone();
                result.max_inclusive = self.max_inclusive;
            }
            (None, Some(_)) => {
                result.max = other.max.clone();
                result.max_inclusive = other.max_inclusive;
            }
            (None, None) => {}
        }

        // Check if the constraint is valid (min <= max)
        if let (Some(ref min_val), Some(ref max_val)) = (&result.min, &result.max) {
            match min_val.partial_cmp(max_val) {
                Some(std::cmp::Ordering::Greater) => return None, // min > max, invalid
                Some(std::cmp::Ordering::Equal) => {
                    // min == max, valid only if both bounds are inclusive
                    if !result.min_inclusive || !result.max_inclusive {
                        return None;
                    }
                }
                Some(std::cmp::Ordering::Less) => {} // min < max, valid
                None => return None,                 // Incomparable
            }
        }

        Some(result)
    }
}

/// Extracts range constraints from logical plan filters
pub struct PredicateExtractor;

impl PredicateExtractor {
    /// Extract range constraints for partition columns from logical plan
    pub fn extract_range_constraints(
        plan: &LogicalPlan,
        partition_columns: &[String],
    ) -> DfResult<Vec<RangeConstraint>> {
        let partition_set: HashSet<String> = partition_columns.iter().cloned().collect();
        let mut visitor = PredicateVisitor::new(partition_set);

        plan.visit(&mut visitor)?;

        // Combine constraints for the same column
        let combined = visitor.combine_constraints();

        debug!(
            "Extracted {} range constraints from logical plan for partition columns: {:?}",
            combined.len(),
            partition_columns
        );

        Ok(combined)
    }
}

/// Visitor to traverse logical plan and extract predicates
struct PredicateVisitor {
    partition_columns: HashSet<String>,
    constraints: Vec<RangeConstraint>,
}

impl PredicateVisitor {
    fn new(partition_columns: HashSet<String>) -> Self {
        Self {
            partition_columns,
            constraints: Vec::new(),
        }
    }

    /// Combine constraints for the same column
    fn combine_constraints(self) -> Vec<RangeConstraint> {
        let mut combined: HashMap<String, RangeConstraint> = HashMap::new();

        for constraint in self.constraints {
            match combined.get(&constraint.column) {
                Some(existing) => {
                    if let Some(intersected) = existing.intersect(&constraint) {
                        combined.insert(constraint.column.clone(), intersected);
                    } else {
                        // Constraints are contradictory, remove this column entirely
                        combined.remove(&constraint.column);
                        debug!(
                            "Contradictory constraints found for column {}, removing from pruning",
                            constraint.column
                        );
                    }
                }
                None => {
                    combined.insert(constraint.column.clone(), constraint);
                }
            }
        }

        combined.into_values().collect()
    }

    /// Analyze a predicate expression for range constraints
    fn analyze_predicate(&mut self, expr: &Expr) -> DfResult<()> {
        match expr {
            Expr::BinaryExpr(binary_expr) => {
                match binary_expr.op {
                    Operator::And => {
                        // Process both sides of AND
                        self.analyze_predicate(&binary_expr.left)?;
                        self.analyze_predicate(&binary_expr.right)?;
                    }
                    Operator::Or => {
                        // For OR, we use a conservative approach and don't extract constraints
                        // since we can't guarantee the intersection of conditions
                        debug!("OR predicate found, using conservative approach");
                    }
                    Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq => {
                        self.extract_comparison_constraint(
                            &binary_expr.left,
                            &binary_expr.op,
                            &binary_expr.right,
                        )?;
                    }
                    _ => {
                        // Other operators like arithmetic, we ignore for now
                    }
                }
            }
            Expr::Between(between) => {
                self.extract_between_constraint(
                    &between.expr,
                    &between.low,
                    &between.high,
                    between.negated,
                )?;
            }
            Expr::InList(in_list) => {
                if !in_list.negated {
                    self.extract_in_constraint(&in_list.expr, &in_list.list)?;
                }
            }
            _ => {
                // Other expression types we don't handle for now
            }
        }
        Ok(())
    }

    /// Extract constraint from comparison operators
    fn extract_comparison_constraint(
        &mut self,
        left: &Expr,
        op: &Operator,
        right: &Expr,
    ) -> DfResult<()> {
        // Check if left is column and right is literal
        if let (Expr::Column(col), Some(value)) = (left, Self::try_extract_literal(right)?) {
            if self.partition_columns.contains(&col.name) {
                let constraint =
                    self.create_constraint_from_comparison(&col.name, op, value, false)?;
                if let Some(constraint) = constraint {
                    self.constraints.push(constraint);
                }
            }
        }
        // Check if right is column and left is literal (reversed comparison)
        else if let (Some(value), Expr::Column(col)) = (Self::try_extract_literal(left)?, right) {
            if self.partition_columns.contains(&col.name) {
                let constraint =
                    self.create_constraint_from_comparison(&col.name, op, value, true)?;
                if let Some(constraint) = constraint {
                    self.constraints.push(constraint);
                }
            }
        }

        Ok(())
    }

    /// Create range constraint from comparison
    fn create_constraint_from_comparison(
        &self,
        column: &str,
        op: &Operator,
        value: ScalarValue,
        reversed: bool,
    ) -> DfResult<Option<RangeConstraint>> {
        let effective_op = if reversed {
            // Reverse the operator when column and value positions are swapped
            match op {
                Operator::Lt => Operator::Gt,
                Operator::LtEq => Operator::GtEq,
                Operator::Gt => Operator::Lt,
                Operator::GtEq => Operator::LtEq,
                other => *other,
            }
        } else {
            *op
        };

        let constraint = match effective_op {
            Operator::Eq => RangeConstraint::new(column.to_string())
                .with_min(value.clone(), true)
                .with_max(value, true),
            Operator::NotEq => {
                // NotEq creates a gap, which we can't easily represent with a single range
                // Skip for now (conservative approach)
                return Ok(None);
            }
            Operator::Lt => RangeConstraint::new(column.to_string()).with_max(value, false),
            Operator::LtEq => RangeConstraint::new(column.to_string()).with_max(value, true),
            Operator::Gt => RangeConstraint::new(column.to_string()).with_min(value, false),
            Operator::GtEq => RangeConstraint::new(column.to_string()).with_min(value, true),
            _ => return Ok(None),
        };

        Ok(Some(constraint))
    }

    /// Extract constraint from BETWEEN expression
    fn extract_between_constraint(
        &mut self,
        expr: &Expr,
        low: &Expr,
        high: &Expr,
        negated: bool,
    ) -> DfResult<()> {
        if negated {
            // NOT BETWEEN is harder to represent as ranges, skip for now
            return Ok(());
        }

        if let Expr::Column(col) = expr {
            if self.partition_columns.contains(&col.name) {
                if let (Some(low_val), Some(high_val)) = (
                    Self::try_extract_literal(low)?,
                    Self::try_extract_literal(high)?,
                ) {
                    let constraint = RangeConstraint::new(col.name.clone())
                        .with_min(low_val, true)
                        .with_max(high_val, true);
                    self.constraints.push(constraint);
                }
            }
        }

        Ok(())
    }

    /// Extract constraint from IN expression
    fn extract_in_constraint(&mut self, expr: &Expr, list: &[Expr]) -> DfResult<()> {
        if let Expr::Column(col) = expr {
            if self.partition_columns.contains(&col.name) {
                let mut values = Vec::new();
                for item in list {
                    if let Some(value) = Self::try_extract_literal(item)? {
                        values.push(value);
                    } else {
                        // Non-literal in list, skip entire constraint
                        return Ok(());
                    }
                }

                if !values.is_empty() {
                    // Find min and max values in the IN list using partial_cmp
                    let mut min_val = values[0].clone();
                    let mut max_val = values[0].clone();

                    for value in &values[1..] {
                        match min_val.partial_cmp(value) {
                            Some(std::cmp::Ordering::Greater) => min_val = value.clone(),
                            Some(std::cmp::Ordering::Equal) | Some(std::cmp::Ordering::Less) => {}
                            None => {
                                // Incomparable values, skip constraint
                                return Ok(());
                            }
                        }

                        match max_val.partial_cmp(value) {
                            Some(std::cmp::Ordering::Less) => max_val = value.clone(),
                            Some(std::cmp::Ordering::Equal) | Some(std::cmp::Ordering::Greater) => {
                            }
                            None => {
                                // Incomparable values, skip constraint
                                return Ok(());
                            }
                        }
                    }

                    let constraint = RangeConstraint::new(col.name.clone())
                        .with_min(min_val, true)
                        .with_max(max_val, true);
                    self.constraints.push(constraint);
                }
            }
        }

        Ok(())
    }

    /// Try to extract a scalar value from an expression
    fn try_extract_literal(expr: &Expr) -> DfResult<Option<ScalarValue>> {
        match expr {
            Expr::Literal(value) => Ok(Some(value.clone())),
            _ => Ok(None),
        }
    }
}

impl TreeNodeVisitor<'_> for PredicateVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &LogicalPlan) -> DfResult<TreeNodeRecursion> {
        match node {
            LogicalPlan::Filter(filter) => {
                debug!("Analyzing filter predicate: {}", filter.predicate);
                self.analyze_predicate(&filter.predicate)?;
                Ok(TreeNodeRecursion::Continue)
            }
            _ => Ok(TreeNodeRecursion::Continue),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion_expr::{col, lit, LogicalPlanBuilder};

    use super::*;

    fn create_test_table_scan() -> LogicalPlan {
        use std::sync::Arc;

        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::DefaultTableSource;

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Int64, false),
        ]));

        let empty_table = datafusion::datasource::empty::EmptyTable::new(schema);
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(empty_table)));

        LogicalPlanBuilder::scan("test", table_source, None)
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_extract_simple_range_constraint() {
        let table_scan = create_test_table_scan();

        // timestamp >= 100
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("timestamp").gt_eq(lit(100i64)))
            .unwrap()
            .build()
            .unwrap();

        let constraints =
            PredicateExtractor::extract_range_constraints(&plan, &["timestamp".to_string()])
                .unwrap();

        assert_eq!(constraints.len(), 1);
        let constraint = &constraints[0];
        assert_eq!(constraint.column, "timestamp");
        assert_eq!(constraint.min, Some(ScalarValue::Int64(Some(100))));
        assert!(constraint.min_inclusive);
        assert_eq!(constraint.max, None);
    }

    #[test]
    fn test_extract_range_with_and() {
        let table_scan = create_test_table_scan();

        // timestamp >= 100 AND timestamp < 200
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(
                col("timestamp")
                    .gt_eq(lit(100i64))
                    .and(col("timestamp").lt(lit(200i64))),
            )
            .unwrap()
            .build()
            .unwrap();

        let constraints =
            PredicateExtractor::extract_range_constraints(&plan, &["timestamp".to_string()])
                .unwrap();

        assert_eq!(constraints.len(), 1);
        let constraint = &constraints[0];
        assert_eq!(constraint.column, "timestamp");
        assert_eq!(constraint.min, Some(ScalarValue::Int64(Some(100))));
        assert!(constraint.min_inclusive);
        assert_eq!(constraint.max, Some(ScalarValue::Int64(Some(200))));
        assert!(!constraint.max_inclusive);
    }

    #[test]
    fn test_constraint_intersection() {
        let c1 =
            RangeConstraint::new("col".to_string()).with_min(ScalarValue::Int64(Some(100)), true);
        let c2 =
            RangeConstraint::new("col".to_string()).with_max(ScalarValue::Int64(Some(200)), false);

        let result = c1.intersect(&c2).unwrap();
        assert_eq!(result.min, Some(ScalarValue::Int64(Some(100))));
        assert!(result.min_inclusive);
        assert_eq!(result.max, Some(ScalarValue::Int64(Some(200))));
        assert!(!result.max_inclusive);
    }

    #[test]
    fn test_contradictory_constraints() {
        let c1 =
            RangeConstraint::new("col".to_string()).with_min(ScalarValue::Int64(Some(200)), true);
        let c2 =
            RangeConstraint::new("col".to_string()).with_max(ScalarValue::Int64(Some(100)), false);

        assert!(c1.intersect(&c2).is_none());
    }

    #[test]
    fn test_non_partition_column_ignored() {
        let table_scan = create_test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("value").gt_eq(lit(100i64)))
            .unwrap()
            .build()
            .unwrap();

        let constraints = PredicateExtractor::extract_range_constraints(
            &plan,
            &["timestamp".to_string()], // only timestamp is partition column
        )
        .unwrap();

        assert_eq!(constraints.len(), 0);
    }
}
