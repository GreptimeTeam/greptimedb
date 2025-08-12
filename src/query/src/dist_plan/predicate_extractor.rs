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

//! Predicate extraction for partition pruning.
//!
//! This module extracts predicate expressions from DataFusion logical plans and converts them
//! into atomic constraints suitable for partition pruning. It serves as a bridge between
//! DataFusion's query representation and GreptimeDB's partition constraint system.
//!
//! ## Key Components
//!
//! - [`DataFusionExprConverter`]: Converts DataFusion expressions into PartitionExpr format
//! - [`PredicateExtractor`]: Main entry point for extracting predicates from logical plans
//!
//! ## Core Workflow
//!
//! 1. **Extract Filters**: Traverse the logical plan to collect all filter expressions
//! 2. **Convert Expressions**: Transform DataFusion expressions into PartitionExpr format
//! 3. **Generate Atomics**: Use the Collider to break down complex expressions into atomic constraints
//! 4. **Filter Relevance**: Only return constraints that involve specified partition columns
//!
//! ## Atomic Expressions
//!
//! The module produces [`AtomicExpr`] objects that represent normalized constraint expressions:
//! - Each atomic expression is a conjunction (AND) of nucleons
//! - Multiple atomic expressions represent disjunctions (OR)
//! - Nucleons are individual column constraints (e.g., `column >= value`)
//!
//! ## Example
//!
//! Given a SQL WHERE clause like:
//! ```sql
//! WHERE (timestamp >= 100 AND timestamp < 200) OR timestamp = 300
//! ```
//!
//! This produces two atomic expressions:
//! - Atomic 1: `[timestamp >= 100, timestamp < 200]` (AND of nucleons)
//! - Atomic 2: `[timestamp = 300]` (single nucleon)

use std::collections::HashSet;

use arrow::datatypes::DataType;
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
            Expr::InList(inlist_expr) => {
                // Convert col IN (val1, val2, val3) to col = val1 OR col = val2 OR col = val3
                // Handle negation: col NOT IN (val1, val2) to col != val1 AND col != val2
                let column_operand = Self::convert_to_operand(&inlist_expr.expr)?;

                if inlist_expr.list.is_empty() {
                    return Err(datafusion_common::DataFusionError::Plan(
                        "InList with empty list is not supported".to_string(),
                    ));
                }

                let op = if inlist_expr.negated {
                    RestrictedOp::NotEq
                } else {
                    RestrictedOp::Eq
                };

                let connector_op = if inlist_expr.negated {
                    RestrictedOp::And // NOT IN becomes col != val1 AND col != val2
                } else {
                    RestrictedOp::Or // IN becomes col = val1 OR col = val2
                };

                // Convert each value in the list to an equality/inequality expression
                let mut expressions = Vec::new();
                for value_expr in &inlist_expr.list {
                    let value_operand = Self::convert_to_operand(value_expr)?;
                    expressions.push(PartitionExpr::new(
                        column_operand.clone(),
                        op.clone(),
                        value_operand,
                    ));
                }

                // Chain expressions with OR/AND
                let mut expr_iter = expressions.into_iter();
                let mut result = expr_iter.next().unwrap();
                for expr in expr_iter {
                    result = PartitionExpr::new(
                        Operand::Expr(result),
                        connector_op.clone(),
                        Operand::Expr(expr),
                    );
                }

                Ok(result)
            }
            Expr::Between(between_expr) => {
                // Convert col BETWEEN low AND high to col >= low AND col <= high
                // Handle negation: col NOT BETWEEN low AND high to col < low OR col > high
                let column_operand = Self::convert_to_operand(&between_expr.expr)?;
                let low_operand = Self::convert_to_operand(&between_expr.low)?;
                let high_operand = Self::convert_to_operand(&between_expr.high)?;

                if between_expr.negated {
                    // NOT BETWEEN: col < low OR col > high
                    let left_expr =
                        PartitionExpr::new(column_operand.clone(), RestrictedOp::Lt, low_operand);
                    let right_expr =
                        PartitionExpr::new(column_operand, RestrictedOp::Gt, high_operand);
                    Ok(PartitionExpr::new(
                        Operand::Expr(left_expr),
                        RestrictedOp::Or,
                        Operand::Expr(right_expr),
                    ))
                } else {
                    // BETWEEN: col >= low AND col <= high
                    let left_expr =
                        PartitionExpr::new(column_operand.clone(), RestrictedOp::GtEq, low_operand);
                    let right_expr =
                        PartitionExpr::new(column_operand, RestrictedOp::LtEq, high_operand);
                    Ok(PartitionExpr::new(
                        Operand::Expr(left_expr),
                        RestrictedOp::And,
                        Operand::Expr(right_expr),
                    ))
                }
            }
            Expr::IsNull(expr) => {
                // Convert col IS NULL to a PartitionExpr
                let column_operand = Self::convert_to_operand(expr)?;
                Ok(PartitionExpr::new(
                    column_operand,
                    RestrictedOp::Eq,
                    Operand::Value(Value::Null),
                ))
            }
            Expr::IsNotNull(expr) => {
                // Convert col IS NOT NULL to a PartitionExpr
                let column_operand = Self::convert_to_operand(expr)?;
                Ok(PartitionExpr::new(
                    column_operand,
                    RestrictedOp::NotEq,
                    Operand::Value(Value::Null),
                ))
            }
            Expr::Not(expr) => {
                // Handle NOT expressions by inverting the inner expression
                match expr.as_ref() {
                    Expr::BinaryExpr(binary_expr) => {
                        let lhs = Self::convert_to_operand(&binary_expr.left)?;
                        let rhs = Self::convert_to_operand(&binary_expr.right)?;
                        let inverted_op = Self::invert_operator(&binary_expr.op)?;

                        Ok(PartitionExpr::new(lhs, inverted_op, rhs))
                    }
                    Expr::IsNull(inner_expr) => {
                        // NOT (col IS NULL) becomes col IS NOT NULL
                        let column_operand = Self::convert_to_operand(inner_expr)?;
                        Ok(PartitionExpr::new(
                            column_operand,
                            RestrictedOp::NotEq,
                            Operand::Value(Value::Null),
                        ))
                    }
                    Expr::IsNotNull(inner_expr) => {
                        // NOT (col IS NOT NULL) becomes col IS NULL
                        let column_operand = Self::convert_to_operand(inner_expr)?;
                        Ok(PartitionExpr::new(
                            column_operand,
                            RestrictedOp::Eq,
                            Operand::Value(Value::Null),
                        ))
                    }
                    _ => {
                        debug!(
                            "Unsupported NOT expression for partition pruning: {:?}",
                            expr
                        );
                        Err(datafusion_common::DataFusionError::Plan(format!(
                            "NOT expression with inner type {:?} not supported for partition pruning",
                            expr
                        )))
                    }
                }
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
            Expr::Column(col) => {
                // Handle qualified column names (table.column) by extracting just the column name
                // For partition pruning, we typically only care about the column name itself
                let column_name = if let Some(relation) = &col.relation {
                    debug!(
                        "Using qualified column reference: {}.{}",
                        relation, col.name
                    );
                    col.name.clone()
                } else {
                    col.name.clone()
                };
                Ok(Operand::Column(column_name))
            }
            Expr::Literal(scalar_value) => {
                let value = Self::scalar_value_to_value(scalar_value)?;
                Ok(Operand::Value(value))
            }
            Expr::Alias(alias_expr) => {
                // Unwrap alias to get the actual expression
                Self::convert_to_operand(&alias_expr.expr)
            }
            Expr::Cast(cast_expr) => {
                // For safe casts, unwrap to the inner expression
                // For unsafe casts, skip with debug logging
                if Self::is_safe_cast_for_partition_pruning(&cast_expr.data_type) {
                    Self::convert_to_operand(&cast_expr.expr)
                } else {
                    debug!(
                        "Skipping unsafe cast for partition pruning: {:?}",
                        cast_expr.data_type
                    );
                    Err(datafusion_common::DataFusionError::Plan(format!(
                        "Cast to {:?} not supported for partition pruning",
                        cast_expr.data_type
                    )))
                }
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

    /// Invert a DataFusion Operator for NOT expressions
    fn invert_operator(op: &Operator) -> DfResult<RestrictedOp> {
        match op {
            Operator::Eq => Ok(RestrictedOp::NotEq),
            Operator::NotEq => Ok(RestrictedOp::Eq),
            Operator::Lt => Ok(RestrictedOp::GtEq),
            Operator::LtEq => Ok(RestrictedOp::Gt),
            Operator::Gt => Ok(RestrictedOp::LtEq),
            Operator::GtEq => Ok(RestrictedOp::Lt),
            Operator::And => Ok(RestrictedOp::Or), // De Morgan's law: NOT (A AND B) = (NOT A) OR (NOT B)
            Operator::Or => Ok(RestrictedOp::And), // De Morgan's law: NOT (A OR B) = (NOT A) AND (NOT B)
            _ => Err(datafusion_common::DataFusionError::Plan(format!(
                "Cannot invert operator: {:?}",
                op
            ))),
        }
    }

    /// Convert DataFusion ScalarValue to datatypes Value
    fn scalar_value_to_value(scalar: &ScalarValue) -> DfResult<Value> {
        let value = match scalar {
            ScalarValue::Boolean(Some(v)) => Value::Boolean(*v),
            ScalarValue::Boolean(None) => Value::Null,
            ScalarValue::Int8(Some(v)) => Value::Int8(*v),
            ScalarValue::Int8(None) => Value::Null,
            ScalarValue::Int16(Some(v)) => Value::Int16(*v),
            ScalarValue::Int16(None) => Value::Null,
            ScalarValue::Int32(Some(v)) => Value::Int32(*v),
            ScalarValue::Int32(None) => Value::Null,
            ScalarValue::Int64(Some(v)) => Value::Int64(*v),
            ScalarValue::Int64(None) => Value::Null,
            ScalarValue::UInt8(Some(v)) => Value::UInt8(*v),
            ScalarValue::UInt8(None) => Value::Null,
            ScalarValue::UInt16(Some(v)) => Value::UInt16(*v),
            ScalarValue::UInt16(None) => Value::Null,
            ScalarValue::UInt32(Some(v)) => Value::UInt32(*v),
            ScalarValue::UInt32(None) => Value::Null,
            ScalarValue::UInt64(Some(v)) => Value::UInt64(*v),
            ScalarValue::UInt64(None) => Value::Null,
            ScalarValue::Float32(Some(v)) => Value::Float32(OrderedFloat(*v)),
            ScalarValue::Float32(None) => Value::Null,
            ScalarValue::Float64(Some(v)) => Value::Float64(OrderedFloat(*v)),
            ScalarValue::Float64(None) => Value::Null,
            ScalarValue::Utf8(Some(v)) => Value::String(v.as_str().into()),
            ScalarValue::Utf8(None) => Value::Null,
            ScalarValue::Binary(Some(v)) => Value::Binary(v.clone().into()),
            ScalarValue::Binary(None) => Value::Null,
            ScalarValue::Date32(Some(v)) => Value::Date((*v).into()),
            ScalarValue::Date32(None) => Value::Null,
            ScalarValue::Date64(Some(v)) => Value::Timestamp((*v * 1000).into()), // Convert to microseconds
            ScalarValue::Date64(None) => Value::Null,

            // Timestamp variants with different precisions
            ScalarValue::TimestampSecond(Some(v), _) => Value::Timestamp((*v * 1_000_000).into()),
            ScalarValue::TimestampSecond(None, _) => Value::Null,
            ScalarValue::TimestampMillisecond(Some(v), _) => Value::Timestamp((*v * 1_000).into()),
            ScalarValue::TimestampMillisecond(None, _) => Value::Null,
            ScalarValue::TimestampMicrosecond(Some(v), _) => Value::Timestamp((*v).into()),
            ScalarValue::TimestampMicrosecond(None, _) => Value::Null,
            ScalarValue::TimestampNanosecond(Some(v), _) => Value::Timestamp((*v / 1_000).into()),
            ScalarValue::TimestampNanosecond(None, _) => Value::Null,

            // Time variants
            ScalarValue::Time32Second(Some(v)) => Value::Time((*v as i64 * 1_000_000).into()),
            ScalarValue::Time32Second(None) => Value::Null,
            ScalarValue::Time32Millisecond(Some(v)) => Value::Time((*v as i64 * 1_000).into()),
            ScalarValue::Time32Millisecond(None) => Value::Null,
            ScalarValue::Time64Microsecond(Some(v)) => Value::Time((*v).into()),
            ScalarValue::Time64Microsecond(None) => Value::Null,
            ScalarValue::Time64Nanosecond(Some(v)) => Value::Time((*v / 1_000).into()),
            ScalarValue::Time64Nanosecond(None) => Value::Null,

            // Duration variants
            ScalarValue::DurationSecond(Some(v)) => Value::Duration((*v * 1_000_000).into()),
            ScalarValue::DurationSecond(None) => Value::Null,
            ScalarValue::DurationMillisecond(Some(v)) => Value::Duration((*v * 1_000).into()),
            ScalarValue::DurationMillisecond(None) => Value::Null,
            ScalarValue::DurationMicrosecond(Some(v)) => Value::Duration((*v).into()),
            ScalarValue::DurationMicrosecond(None) => Value::Null,
            ScalarValue::DurationNanosecond(Some(v)) => Value::Duration((*v / 1_000).into()),
            ScalarValue::DurationNanosecond(None) => Value::Null,

            // Decimal variants
            ScalarValue::Decimal128(Some(v), _precision, scale) => {
                // Convert to string representation for now - more complex conversion would be needed for exact decimal handling
                let scale_factor = 10_i128.pow(*scale as u32);
                let decimal_str = format!(
                    "{:.prec$}",
                    (*v as f64) / (scale_factor as f64),
                    prec = *scale as usize
                );
                Value::String(decimal_str.into())
            }
            ScalarValue::Decimal128(None, _, _) => Value::Null,
            ScalarValue::Decimal256(Some(_), _, _) => {
                debug!("Decimal256 values not fully supported in partition pruning, skipping");
                return Err(datafusion_common::DataFusionError::Plan(
                    "Decimal256 not supported for partition pruning".to_string(),
                ));
            }
            ScalarValue::Decimal256(None, _, _) => Value::Null,

            // Large string variants
            ScalarValue::LargeUtf8(Some(v)) => Value::String(v.as_str().into()),
            ScalarValue::LargeUtf8(None) => Value::Null,
            ScalarValue::LargeBinary(Some(v)) => Value::Binary(v.clone().into()),
            ScalarValue::LargeBinary(None) => Value::Null,

            // Interval variants - convert to duration
            ScalarValue::IntervalYearMonth(Some(v)) => {
                // Approximate: 1 month = 30 days = 30 * 24 * 3600 * 1_000_000 microseconds
                let months = *v;
                let microseconds = months as i64 * 30 * 24 * 3600 * 1_000_000;
                Value::Duration(microseconds.into())
            }
            ScalarValue::IntervalYearMonth(None) => Value::Null,
            ScalarValue::IntervalDayTime(Some(v)) => {
                // IntervalDayTime has days and milliseconds fields
                let days = v.days;
                let milliseconds = v.milliseconds;
                let total_microseconds =
                    (days as i64 * 24 * 3600 * 1_000_000) + (milliseconds as i64 * 1_000);
                Value::Duration(total_microseconds.into())
            }
            ScalarValue::IntervalDayTime(None) => Value::Null,
            ScalarValue::IntervalMonthDayNano(Some(v)) => {
                // v is packed as (months, days, nanoseconds) - simplified conversion
                let months = v.months;
                let days = v.days;
                let nanos = v.nanoseconds;
                let total_microseconds = (months as i64 * 30 * 24 * 3600 * 1_000_000)
                    + (days as i64 * 24 * 3600 * 1_000_000)
                    + (nanos / 1_000);
                Value::Duration(total_microseconds.into())
            }
            ScalarValue::IntervalMonthDayNano(None) => Value::Null,

            ScalarValue::Null => Value::Null,
            _ => {
                debug!(
                    "Unsupported scalar value type for partition pruning: {:?}",
                    scalar
                );
                return Err(datafusion_common::DataFusionError::Plan(format!(
                    "Unsupported scalar value type: {:?}",
                    scalar
                )));
            }
        };
        Ok(value)
    }

    /// Determine if a cast is safe for partition pruning
    /// Safe casts don't change the logical meaning of constraints
    fn is_safe_cast_for_partition_pruning(data_type: &DataType) -> bool {
        match data_type {
            // Integer widening casts are generally safe
            DataType::Int8 => true,
            DataType::Int16 => true,
            DataType::Int32 => true,
            DataType::Int64 => true,
            DataType::UInt8 => true,
            DataType::UInt16 => true,
            DataType::UInt32 => true,
            DataType::UInt64 => true,

            // Float casts are generally safe for equality/inequality comparisons
            DataType::Float32 => true,
            DataType::Float64 => true,

            // String casts might be safe in some cases
            DataType::Utf8 => true,
            DataType::LargeUtf8 => true,

            // Date/time casts might be safe if they don't change precision significantly
            DataType::Date32 => true,
            DataType::Date64 => true,
            DataType::Timestamp(_, _) => true,

            // Boolean casts are straightforward
            DataType::Boolean => true,

            // For other types, be conservative and skip
            _ => false,
        }
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
            Field::new("user_id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let empty_table = datafusion::datasource::empty::EmptyTable::new(schema);
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(empty_table)));

        LogicalPlanBuilder::scan("test", table_source, None)
            .unwrap()
            .build()
            .unwrap()
    }

    struct FilterTestCase {
        name: &'static str,
        filter_expr: Box<dyn Fn() -> Expr>,
        expected_atomics: usize,
        expected_nucleons_per_atomic: Vec<usize>,
        partition_columns: Vec<&'static str>,
    }

    impl FilterTestCase {
        fn new(
            name: &'static str,
            filter_expr: Box<dyn Fn() -> Expr>,
            expected_atomics: usize,
            expected_nucleons_per_atomic: Vec<usize>,
            partition_columns: Vec<&'static str>,
        ) -> Self {
            Self {
                name,
                filter_expr,
                expected_atomics,
                expected_nucleons_per_atomic,
                partition_columns,
            }
        }
    }

    #[test]
    fn test_basic_constraints_extraction() {
        let cases = vec![
            FilterTestCase::new(
                "non_partition_column_ignored",
                Box::new(|| col("value").gt_eq(lit(100i64))),
                0,
                vec![],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "simple_constraint",
                Box::new(|| col("user_id").gt_eq(lit(100i64))),
                1,
                vec![1],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "or_expression",
                Box::new(|| {
                    col("user_id")
                        .eq(lit(100i64))
                        .or(col("user_id").eq(lit(200i64)))
                }),
                2,
                vec![1, 1],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "complex_and_or",
                Box::new(|| {
                    col("user_id")
                        .gt_eq(lit(100i64))
                        .and(col("user_id").lt(lit(200i64)))
                        .or(col("user_id")
                            .gt_eq(lit(300i64))
                            .and(col("user_id").lt(lit(400i64))))
                }),
                2,
                vec![2, 2],
                vec!["user_id"],
            ),
        ];

        for case in cases {
            let table_scan = create_test_table_scan();
            let filter = (case.filter_expr)();

            let plan = LogicalPlanBuilder::from(table_scan)
                .filter(filter)
                .unwrap()
                .build()
                .unwrap();

            let partition_columns: Vec<String> = case
                .partition_columns
                .iter()
                .map(|s| s.to_string())
                .collect();
            let atomic_constraints =
                PredicateExtractor::extract_atomic_constraints(&plan, &partition_columns).unwrap();

            assert_eq!(
                atomic_constraints.len(),
                case.expected_atomics,
                "Test case '{}': expected {} atomic constraints, got {}",
                case.name,
                case.expected_atomics,
                atomic_constraints.len()
            );

            for (i, expected_nucleons) in case.expected_nucleons_per_atomic.iter().enumerate() {
                assert_eq!(
                    atomic_constraints[i].nucleons.len(),
                    *expected_nucleons,
                    "Test case '{}': atomic {} should have {} nucleons, got {}",
                    case.name,
                    i,
                    expected_nucleons,
                    atomic_constraints[i].nucleons.len()
                );

                // Verify all nucleons reference partition columns
                for nucleon in &atomic_constraints[i].nucleons {
                    assert!(
                        case.partition_columns.contains(&nucleon.column()),
                        "Test case '{}': nucleon references non-partition column '{}'",
                        case.name,
                        nucleon.column()
                    );
                }
            }
        }
    }

    #[test]
    fn test_alias_expressions() {
        let cases = vec![
            FilterTestCase::new(
                "simple_alias",
                Box::new(|| col("user_id").alias("uid").eq(lit(100i64))),
                1,
                vec![1],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "nested_alias",
                Box::new(|| col("user_id").alias("uid").alias("u").gt_eq(lit(50i64))),
                1,
                vec![1],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "complex_alias_with_and_or",
                Box::new(|| {
                    col("user_id")
                        .alias("uid")
                        .gt_eq(lit(100i64))
                        .and(col("user_id").alias("u").lt(lit(200i64)))
                        .or(col("user_id").alias("id").eq(lit(300i64)))
                }),
                2,
                vec![2, 1], // First atomic has 2 nucleons (AND), second has 1
                vec!["user_id"],
            ),
        ];

        for case in cases {
            let table_scan = create_test_table_scan();
            let filter = (case.filter_expr)();

            let plan = LogicalPlanBuilder::from(table_scan)
                .filter(filter)
                .unwrap()
                .build()
                .unwrap();

            let partition_columns: Vec<String> = case
                .partition_columns
                .iter()
                .map(|s| s.to_string())
                .collect();
            let atomic_constraints =
                PredicateExtractor::extract_atomic_constraints(&plan, &partition_columns).unwrap();

            assert_eq!(
                atomic_constraints.len(),
                case.expected_atomics,
                "Test case '{}': expected {} atomic constraints, got {}",
                case.name,
                case.expected_atomics,
                atomic_constraints.len()
            );

            for (i, expected_nucleons) in case.expected_nucleons_per_atomic.iter().enumerate() {
                assert_eq!(
                    atomic_constraints[i].nucleons.len(),
                    *expected_nucleons,
                    "Test case '{}': atomic {} should have {} nucleons, got {}",
                    case.name,
                    i,
                    expected_nucleons,
                    atomic_constraints[i].nucleons.len()
                );

                // Verify all nucleons reference partition columns
                for nucleon in &atomic_constraints[i].nucleons {
                    assert_eq!(
                        nucleon.column(),
                        "user_id",
                        "Test case '{}': should unwrap alias to 'user_id', got '{}'",
                        case.name,
                        nucleon.column()
                    );
                }
            }
        }
    }

    #[test]
    fn test_inlist_expressions() {
        let cases = vec![
            FilterTestCase::new(
                "simple_inlist",
                Box::new(|| {
                    col("user_id").in_list(vec![lit(100i64), lit(200i64), lit(300i64)], false)
                }),
                3,
                vec![1, 1, 1], // Three OR conditions, each with 1 nucleon
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "negated_inlist",
                Box::new(|| col("user_id").in_list(vec![lit(100i64), lit(200i64)], true)),
                1,
                vec![2], // One AND condition with 2 nucleons: user_id != 100 AND user_id != 200
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "inlist_with_alias",
                Box::new(|| {
                    col("user_id")
                        .alias("uid")
                        .in_list(vec![lit(100i64), lit(200i64)], false)
                }),
                2,
                vec![1, 1], // Two OR conditions, each with 1 nucleon
                vec!["user_id"],
            ),
        ];

        for case in cases {
            let table_scan = create_test_table_scan();
            let filter = (case.filter_expr)();

            let plan = LogicalPlanBuilder::from(table_scan)
                .filter(filter)
                .unwrap()
                .build()
                .unwrap();

            let partition_columns: Vec<String> = case
                .partition_columns
                .iter()
                .map(|s| s.to_string())
                .collect();
            let atomic_constraints =
                PredicateExtractor::extract_atomic_constraints(&plan, &partition_columns).unwrap();

            assert_eq!(
                atomic_constraints.len(),
                case.expected_atomics,
                "Test case '{}': expected {} atomic constraints, got {}",
                case.name,
                case.expected_atomics,
                atomic_constraints.len()
            );

            for (i, expected_nucleons) in case.expected_nucleons_per_atomic.iter().enumerate() {
                assert_eq!(
                    atomic_constraints[i].nucleons.len(),
                    *expected_nucleons,
                    "Test case '{}': atomic {} should have {} nucleons, got {}",
                    case.name,
                    i,
                    expected_nucleons,
                    atomic_constraints[i].nucleons.len()
                );

                for nucleon in &atomic_constraints[i].nucleons {
                    assert_eq!(
                        nucleon.column(),
                        "user_id",
                        "Test case '{}': nucleon should reference 'user_id'",
                        case.name
                    );
                }
            }
        }
    }

    #[test]
    fn test_between_expressions() {
        let cases = vec![
            FilterTestCase::new(
                "simple_between",
                Box::new(|| col("user_id").between(lit(100i64), lit(200i64))),
                1,
                vec![2], // One AND with 2 nucleons: user_id >= 100 AND user_id <= 200
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "negated_between",
                Box::new(|| {
                    Expr::Between(datafusion_expr::Between {
                        expr: Box::new(col("user_id")),
                        negated: true,
                        low: Box::new(lit(100i64)),
                        high: Box::new(lit(200i64)),
                    })
                }),
                2,
                vec![1, 1], // Two OR conditions: user_id < 100 OR user_id > 200
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "between_with_alias",
                Box::new(|| {
                    col("user_id")
                        .alias("uid")
                        .between(lit(100i64), lit(200i64))
                }),
                1,
                vec![2], // One AND with 2 nucleons
                vec!["user_id"],
            ),
        ];

        for case in cases {
            let table_scan = create_test_table_scan();
            let filter = (case.filter_expr)();

            let plan = LogicalPlanBuilder::from(table_scan)
                .filter(filter)
                .unwrap()
                .build()
                .unwrap();

            let partition_columns: Vec<String> = case
                .partition_columns
                .iter()
                .map(|s| s.to_string())
                .collect();
            let atomic_constraints =
                PredicateExtractor::extract_atomic_constraints(&plan, &partition_columns).unwrap();

            assert_eq!(
                atomic_constraints.len(),
                case.expected_atomics,
                "Test case '{}': expected {} atomic constraints, got {}",
                case.name,
                case.expected_atomics,
                atomic_constraints.len()
            );

            for (i, expected_nucleons) in case.expected_nucleons_per_atomic.iter().enumerate() {
                assert_eq!(
                    atomic_constraints[i].nucleons.len(),
                    *expected_nucleons,
                    "Test case '{}': atomic {} should have {} nucleons, got {}",
                    case.name,
                    i,
                    expected_nucleons,
                    atomic_constraints[i].nucleons.len()
                );

                for nucleon in &atomic_constraints[i].nucleons {
                    assert_eq!(
                        nucleon.column(),
                        "user_id",
                        "Test case '{}': nucleon should reference 'user_id'",
                        case.name
                    );
                }
            }
        }
    }

    #[test]
    fn test_null_expressions() {
        let cases = vec![
            FilterTestCase::new(
                "is_null",
                Box::new(|| col("user_id").is_null()),
                1,
                vec![1], // user_id = NULL
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "is_not_null",
                Box::new(|| col("user_id").is_not_null()),
                1,
                vec![1], // user_id != NULL
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "null_with_alias",
                Box::new(|| col("user_id").alias("uid").is_null()),
                1,
                vec![1], // aliased null check
                vec!["user_id"],
            ),
        ];

        for case in cases {
            let table_scan = create_test_table_scan();
            let filter = (case.filter_expr)();

            let plan = LogicalPlanBuilder::from(table_scan)
                .filter(filter)
                .unwrap()
                .build()
                .unwrap();

            let partition_columns: Vec<String> = case
                .partition_columns
                .iter()
                .map(|s| s.to_string())
                .collect();
            let atomic_constraints =
                PredicateExtractor::extract_atomic_constraints(&plan, &partition_columns).unwrap();

            assert_eq!(
                atomic_constraints.len(),
                case.expected_atomics,
                "Test case '{}': expected {} atomic constraints, got {}",
                case.name,
                case.expected_atomics,
                atomic_constraints.len()
            );

            for (i, expected_nucleons) in case.expected_nucleons_per_atomic.iter().enumerate() {
                assert_eq!(
                    atomic_constraints[i].nucleons.len(),
                    *expected_nucleons,
                    "Test case '{}': atomic {} should have {} nucleons, got {}",
                    case.name,
                    i,
                    expected_nucleons,
                    atomic_constraints[i].nucleons.len()
                );

                for nucleon in &atomic_constraints[i].nucleons {
                    assert_eq!(
                        nucleon.column(),
                        "user_id",
                        "Test case '{}': nucleon should reference 'user_id'",
                        case.name
                    );
                }
            }
        }
    }

    #[test]
    fn test_cast_expressions() {
        use datafusion::arrow::datatypes::DataType;

        let cases = vec![
            FilterTestCase::new(
                "safe_cast",
                Box::new(|| {
                    Expr::Cast(datafusion_expr::Cast {
                        expr: Box::new(col("user_id")),
                        data_type: DataType::Int64,
                    })
                    .eq(lit(100i64))
                }),
                1,
                vec![1], // Should unwrap cast
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "cast_with_alias",
                Box::new(|| {
                    Expr::Cast(datafusion_expr::Cast {
                        expr: Box::new(col("user_id").alias("uid")),
                        data_type: DataType::Int64,
                    })
                    .eq(lit(100i64))
                }),
                1,
                vec![1], // Should unwrap both cast and alias
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "unsafe_cast",
                Box::new(|| {
                    Expr::Cast(datafusion_expr::Cast {
                        expr: Box::new(col("user_id")),
                        data_type: DataType::List(std::sync::Arc::new(
                            datafusion::arrow::datatypes::Field::new("item", DataType::Int32, true),
                        )),
                    })
                    .eq(lit(100i64))
                }),
                0,
                vec![], // Should reject unsafe cast
                vec!["user_id"],
            ),
        ];

        for case in cases {
            let table_scan = create_test_table_scan();
            let filter = (case.filter_expr)();

            let plan = LogicalPlanBuilder::from(table_scan)
                .filter(filter)
                .unwrap()
                .build()
                .unwrap();

            let partition_columns: Vec<String> = case
                .partition_columns
                .iter()
                .map(|s| s.to_string())
                .collect();
            let atomic_constraints =
                PredicateExtractor::extract_atomic_constraints(&plan, &partition_columns).unwrap();

            assert_eq!(
                atomic_constraints.len(),
                case.expected_atomics,
                "Test case '{}': expected {} atomic constraints, got {}",
                case.name,
                case.expected_atomics,
                atomic_constraints.len()
            );

            for (i, expected_nucleons) in case.expected_nucleons_per_atomic.iter().enumerate() {
                assert_eq!(
                    atomic_constraints[i].nucleons.len(),
                    *expected_nucleons,
                    "Test case '{}': atomic {} should have {} nucleons, got {}",
                    case.name,
                    i,
                    expected_nucleons,
                    atomic_constraints[i].nucleons.len()
                );

                for nucleon in &atomic_constraints[i].nucleons {
                    assert_eq!(
                        nucleon.column(),
                        "user_id",
                        "Test case '{}': nucleon should reference 'user_id'",
                        case.name
                    );
                }
            }
        }
    }

    #[test]
    fn test_not_expressions() {
        let cases = vec![
            FilterTestCase::new(
                "not_equality",
                Box::new(|| Expr::Not(Box::new(col("user_id").eq(lit(100i64))))),
                1,
                vec![1], // NOT (user_id = 100) becomes user_id != 100
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "not_comparison",
                Box::new(|| Expr::Not(Box::new(col("user_id").lt(lit(100i64))))),
                1,
                vec![1], // NOT (user_id < 100) becomes user_id >= 100
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "not_is_null",
                Box::new(|| Expr::Not(Box::new(col("user_id").is_null()))),
                1,
                vec![1], // NOT (user_id IS NULL) becomes user_id IS NOT NULL
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "not_with_alias",
                Box::new(|| Expr::Not(Box::new(col("user_id").alias("uid").eq(lit(100i64))))),
                1,
                vec![1], // Should handle alias unwrapping with negation
                vec!["user_id"],
            ),
        ];

        for case in cases {
            let table_scan = create_test_table_scan();
            let filter = (case.filter_expr)();

            let plan = LogicalPlanBuilder::from(table_scan)
                .filter(filter)
                .unwrap()
                .build()
                .unwrap();

            let partition_columns: Vec<String> = case
                .partition_columns
                .iter()
                .map(|s| s.to_string())
                .collect();
            let atomic_constraints =
                PredicateExtractor::extract_atomic_constraints(&plan, &partition_columns).unwrap();

            assert_eq!(
                atomic_constraints.len(),
                case.expected_atomics,
                "Test case '{}': expected {} atomic constraints, got {}",
                case.name,
                case.expected_atomics,
                atomic_constraints.len()
            );

            for (i, expected_nucleons) in case.expected_nucleons_per_atomic.iter().enumerate() {
                assert_eq!(
                    atomic_constraints[i].nucleons.len(),
                    *expected_nucleons,
                    "Test case '{}': atomic {} should have {} nucleons, got {}",
                    case.name,
                    i,
                    expected_nucleons,
                    atomic_constraints[i].nucleons.len()
                );

                for nucleon in &atomic_constraints[i].nucleons {
                    assert_eq!(
                        nucleon.column(),
                        "user_id",
                        "Test case '{}': nucleon should reference 'user_id'",
                        case.name
                    );
                }
            }
        }
    }

    #[test]
    fn test_scalar_value_conversions() {
        use datafusion_common::ScalarValue;

        let cases = vec![
            FilterTestCase::new(
                "timestamp_scalar",
                Box::new(|| {
                    let timestamp_val =
                        ScalarValue::TimestampMillisecond(Some(1672574400000), None);
                    col("timestamp").eq(Expr::Literal(timestamp_val))
                }),
                1,
                vec![1],
                vec!["timestamp"],
            ),
            FilterTestCase::new(
                "decimal_scalar",
                Box::new(|| {
                    let decimal_val = ScalarValue::Decimal128(Some(12345), 5, 2); // 123.45 with precision=5, scale=2
                    col("user_id").eq(Expr::Literal(decimal_val))
                }),
                1,
                vec![1],
                vec!["user_id"],
            ),
        ];

        for case in cases {
            let table_scan = create_test_table_scan();
            let filter = (case.filter_expr)();

            let plan = LogicalPlanBuilder::from(table_scan)
                .filter(filter)
                .unwrap()
                .build()
                .unwrap();

            let partition_columns: Vec<String> = case
                .partition_columns
                .iter()
                .map(|s| s.to_string())
                .collect();
            let atomic_constraints =
                PredicateExtractor::extract_atomic_constraints(&plan, &partition_columns).unwrap();

            assert_eq!(
                atomic_constraints.len(),
                case.expected_atomics,
                "Test case '{}': expected {} atomic constraints, got {}",
                case.name,
                case.expected_atomics,
                atomic_constraints.len()
            );

            for (i, expected_nucleons) in case.expected_nucleons_per_atomic.iter().enumerate() {
                assert_eq!(
                    atomic_constraints[i].nucleons.len(),
                    *expected_nucleons,
                    "Test case '{}': atomic {} should have {} nucleons, got {}",
                    case.name,
                    i,
                    expected_nucleons,
                    atomic_constraints[i].nucleons.len()
                );

                for nucleon in &atomic_constraints[i].nucleons {
                    assert!(
                        case.partition_columns.contains(&nucleon.column()),
                        "Test case '{}': nucleon should reference a partition column",
                        case.name
                    );
                }
            }
        }
    }

    #[test]
    fn test_edge_cases() {
        use datafusion::arrow::datatypes::DataType;
        use datafusion::common::Column;

        let cases = vec![
            FilterTestCase::new(
                "qualified_column_name",
                Box::new(|| {
                    let qualified_col = Expr::Column(Column::new(Some("test"), "user_id"));
                    qualified_col.eq(lit(100i64))
                }),
                1,
                vec![1], // Should extract column name from qualified reference
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "comprehensive_combinations",
                Box::new(|| {
                    let in_expr = col("user_id")
                        .alias("uid")
                        .in_list(vec![lit(100i64), lit(200i64)], false);
                    let cast_expr = Expr::Cast(datafusion_expr::Cast {
                        expr: Box::new(col("user_id")),
                        data_type: DataType::Int64,
                    });
                    let between_expr = cast_expr.between(lit(300i64), lit(400i64));
                    in_expr.or(between_expr)
                }),
                3,
                vec![1, 1, 2], // IN creates 2 OR conditions, BETWEEN creates 1 AND condition
                vec!["user_id"],
            ),
        ];

        for case in cases {
            let table_scan = create_test_table_scan();
            let filter = (case.filter_expr)();

            let plan = LogicalPlanBuilder::from(table_scan)
                .filter(filter)
                .unwrap()
                .build()
                .unwrap();

            let partition_columns: Vec<String> = case
                .partition_columns
                .iter()
                .map(|s| s.to_string())
                .collect();
            let atomic_constraints =
                PredicateExtractor::extract_atomic_constraints(&plan, &partition_columns).unwrap();

            // For edge cases, just verify we get some constraints and they reference correct columns
            assert!(
                !atomic_constraints.is_empty(),
                "Test case '{}': should produce at least some atomic constraints",
                case.name
            );

            for atomic in &atomic_constraints {
                for nucleon in &atomic.nucleons {
                    assert!(
                        case.partition_columns.contains(&nucleon.column()),
                        "Test case '{}': nucleon should reference a partition column, got '{}'",
                        case.name,
                        nucleon.column()
                    );
                }
            }
        }
    }
}
