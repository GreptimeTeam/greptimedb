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

use std::collections::HashSet;

use common_telemetry::debug;
use datafusion_common::{Result as DfResult, ScalarValue};
use datafusion_expr::{Expr, LogicalPlan, Operator};
use datatypes::value::{OrderedFloat, Value};
use partition::collider::{AtomicExpr, Collider};
use partition::expr::{Operand, PartitionExpr, RestrictedOp};

/// Converts DataFusion expressions to PartitionExpr
pub struct DataFusionExprConverter;

impl DataFusionExprConverter {
    /// Convert DataFusion Expr to PartitionExpr
    pub fn convert(expr: &Expr) -> DfResult<PartitionExpr> {
        match expr {
            Expr::BinaryExpr(binary_expr) => {
                let lhs = Self::convert_to_operand(&binary_expr.left)?;
                let rhs = Self::convert_to_operand(&binary_expr.right)?;
                let op = Self::convert_operator(&binary_expr.op)?;

                Ok(PartitionExpr::new(lhs, op, rhs))
            }
            _ => Err(datafusion_common::DataFusionError::Plan(format!(
                "Unsupported expression type for conversion: {:?}",
                expr
            ))),
        }
    }

    /// Convert DataFusion Expr to Operand
    fn convert_to_operand(expr: &Expr) -> DfResult<Operand> {
        match expr {
            Expr::Column(col) => Ok(Operand::Column(col.name.clone())),
            Expr::Literal(scalar_value) => {
                let value = Self::scalar_value_to_value(scalar_value)?;
                Ok(Operand::Value(value))
            }
            other => {
                let partition_expr = Self::convert(other)?;
                Ok(Operand::Expr(partition_expr))
            }
        }
    }

    /// Convert DataFusion Operator to RestrictedOp
    fn convert_operator(op: &Operator) -> DfResult<RestrictedOp> {
        match op {
            Operator::Eq => Ok(RestrictedOp::Eq),
            Operator::NotEq => Ok(RestrictedOp::NotEq),
            Operator::Lt => Ok(RestrictedOp::Lt),
            Operator::LtEq => Ok(RestrictedOp::LtEq),
            Operator::Gt => Ok(RestrictedOp::Gt),
            Operator::GtEq => Ok(RestrictedOp::GtEq),
            Operator::And => Ok(RestrictedOp::And),
            Operator::Or => Ok(RestrictedOp::Or),
            _ => Err(datafusion_common::DataFusionError::Plan(format!(
                "Unsupported operator: {:?}",
                op
            ))),
        }
    }

    /// Convert DataFusion ScalarValue to datatypes Value
    fn scalar_value_to_value(scalar: &ScalarValue) -> DfResult<Value> {
        let value = match scalar {
            ScalarValue::Boolean(Some(v)) => Value::Boolean(*v),
            ScalarValue::Int8(Some(v)) => Value::Int8(*v),
            ScalarValue::Int16(Some(v)) => Value::Int16(*v),
            ScalarValue::Int32(Some(v)) => Value::Int32(*v),
            ScalarValue::Int64(Some(v)) => Value::Int64(*v),
            ScalarValue::UInt8(Some(v)) => Value::UInt8(*v),
            ScalarValue::UInt16(Some(v)) => Value::UInt16(*v),
            ScalarValue::UInt32(Some(v)) => Value::UInt32(*v),
            ScalarValue::UInt64(Some(v)) => Value::UInt64(*v),
            ScalarValue::Float32(Some(v)) => Value::Float32(OrderedFloat(*v)),
            ScalarValue::Float64(Some(v)) => Value::Float64(OrderedFloat(*v)),
            ScalarValue::Utf8(Some(v)) => Value::String(v.as_str().into()),
            ScalarValue::Binary(Some(v)) => Value::Binary(v.clone().into()),
            ScalarValue::Date32(Some(v)) => Value::Date((*v).into()),
            ScalarValue::Null => Value::Null,
            _ => {
                return Err(datafusion_common::DataFusionError::Plan(format!(
                    "Unsupported scalar value type: {:?}",
                    scalar
                )))
            }
        };
        Ok(value)
    }
}

/// Extracts range constraints from logical plan filters
pub struct PredicateExtractor;

impl PredicateExtractor {
    /// Extract partition expressions for partition columns from logical plan  
    /// This method returns PartitionExpr objects suitable for ConstraintPruner
    pub fn extract_partition_expressions(
        plan: &LogicalPlan,
        partition_columns: &[String],
    ) -> DfResult<Vec<PartitionExpr>> {
        // Collect all filter expressions from the logical plan
        let mut filter_exprs = Vec::new();
        Self::collect_filter_expressions(plan, &mut filter_exprs)?;

        if filter_exprs.is_empty() {
            return Ok(Vec::new());
        }

        // Convert each DataFusion filter expression to PartitionExpr
        let mut partition_exprs = Vec::new();
        let partition_set: HashSet<String> = partition_columns.iter().cloned().collect();

        for filter_expr in filter_exprs {
            match DataFusionExprConverter::convert(&filter_expr) {
                Ok(partition_expr) => {
                    // Check if this expression involves partition columns
                    if Self::expr_involves_partition_columns(&partition_expr, &partition_set) {
                        partition_exprs.push(partition_expr);
                    }
                }
                Err(err) => {
                    debug!(
                        "Failed to convert filter expression to PartitionExpr: {}, skipping",
                        err
                    );
                    continue;
                }
            }
        }

        debug!(
            "Extracted {} partition expressions from logical plan for partition columns: {:?}",
            partition_exprs.len(),
            partition_columns
        );

        Ok(partition_exprs)
    }

    /// Check if a partition expression involves any partition columns
    fn expr_involves_partition_columns(
        expr: &PartitionExpr,
        partition_columns: &HashSet<String>,
    ) -> bool {
        Self::operand_involves_partition_columns(expr.lhs(), partition_columns)
            || Self::operand_involves_partition_columns(expr.rhs(), partition_columns)
    }

    /// Check if an operand involves any partition columns
    fn operand_involves_partition_columns(
        operand: &Operand,
        partition_columns: &HashSet<String>,
    ) -> bool {
        match operand {
            Operand::Column(col) => partition_columns.contains(col),
            Operand::Value(_) => false,
            Operand::Expr(expr) => Self::expr_involves_partition_columns(expr, partition_columns),
        }
    }

    /// Extract atomic expressions for partition columns from logical plan
    /// This is the new enhanced method that returns AtomicExpr
    pub fn extract_atomic_constraints(
        plan: &LogicalPlan,
        partition_columns: &[String],
    ) -> DfResult<Vec<AtomicExpr>> {
        // Get the partition expressions first
        let partition_exprs = Self::extract_partition_expressions(plan, partition_columns)?;

        if partition_exprs.is_empty() {
            return Ok(Vec::new());
        }

        // Use Collider to process the expressions
        match Collider::new(&partition_exprs) {
            Ok(collider) => {
                // Filter atomic expressions to only include relevant partition columns
                let partition_set: HashSet<String> = partition_columns.iter().cloned().collect();
                let filtered_atomics: Vec<AtomicExpr> = collider
                    .atomic_exprs
                    .into_iter()
                    .filter(|atomic| {
                        atomic.nucleons.iter().any(|nucleon| {
                            // Check if any nucleon in this atomic expression involves partition columns
                            partition_set.contains(nucleon.column())
                        })
                    })
                    .collect();

                debug!(
                    "Extracted {} atomic constraints from logical plan for partition columns: {:?}",
                    filtered_atomics.len(),
                    partition_columns
                );

                Ok(filtered_atomics)
            }
            Err(err) => {
                debug!("Failed to create collider from partition expressions: {}, returning empty constraints", err);
                Ok(Vec::new())
            }
        }
    }

    /// Collect all filter expressions from a logical plan
    fn collect_filter_expressions(plan: &LogicalPlan, expressions: &mut Vec<Expr>) -> DfResult<()> {
        if let LogicalPlan::Filter(filter) = plan {
            expressions.push(filter.predicate.clone());
        }

        // Recursively visit children
        for child in plan.inputs() {
            Self::collect_filter_expressions(child, expressions)?;
        }

        Ok(())
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
    fn test_non_partition_column_ignored() {
        let table_scan = create_test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("value").gt_eq(lit(100i64)))
            .unwrap()
            .build()
            .unwrap();

        let atomic_constraints = PredicateExtractor::extract_atomic_constraints(
            &plan,
            &["timestamp".to_string()], // only timestamp is partition column
        )
        .unwrap();

        assert_eq!(atomic_constraints.len(), 0);
    }

    #[test]
    fn test_extract_atomic_constraints_simple() {
        let table_scan = create_test_table_scan();

        // timestamp >= 100
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("timestamp").gt_eq(lit(100i64)))
            .unwrap()
            .build()
            .unwrap();

        let atomic_constraints =
            PredicateExtractor::extract_atomic_constraints(&plan, &["timestamp".to_string()])
                .unwrap();

        assert_eq!(atomic_constraints.len(), 1);
        let atomic = &atomic_constraints[0];
        assert_eq!(atomic.nucleons.len(), 1);
        assert_eq!(atomic.nucleons[0].column(), "timestamp");
    }

    #[test]
    fn test_extract_atomic_constraints_or_expression() {
        let table_scan = create_test_table_scan();

        // timestamp = 100 OR timestamp = 200
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(
                col("timestamp")
                    .eq(lit(100i64))
                    .or(col("timestamp").eq(lit(200i64))),
            )
            .unwrap()
            .build()
            .unwrap();

        let atomic_constraints =
            PredicateExtractor::extract_atomic_constraints(&plan, &["timestamp".to_string()])
                .unwrap();

        // OR expression should result in 2 atomic expressions
        assert_eq!(atomic_constraints.len(), 2);
        for atomic in &atomic_constraints {
            assert_eq!(atomic.nucleons.len(), 1);
            assert_eq!(atomic.nucleons[0].column(), "timestamp");
        }
    }

    #[test]
    fn test_extract_atomic_constraints_complex_and_or() {
        let table_scan = create_test_table_scan();

        // (timestamp >= 100 AND timestamp < 200) OR (timestamp >= 300 AND timestamp < 400)
        let filter = col("timestamp")
            .gt_eq(lit(100i64))
            .and(col("timestamp").lt(lit(200i64)))
            .or(col("timestamp")
                .gt_eq(lit(300i64))
                .and(col("timestamp").lt(lit(400i64))));

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(filter)
            .unwrap()
            .build()
            .unwrap();

        let atomic_constraints =
            PredicateExtractor::extract_atomic_constraints(&plan, &["timestamp".to_string()])
                .unwrap();

        // Should result in 2 atomic expressions (one for each OR branch)
        assert_eq!(atomic_constraints.len(), 2);
        for atomic in &atomic_constraints {
            assert_eq!(atomic.nucleons.len(), 2); // Each should have 2 constraints (AND)
            for nucleon in &atomic.nucleons {
                assert_eq!(nucleon.column(), "timestamp");
            }
        }
    }
}
