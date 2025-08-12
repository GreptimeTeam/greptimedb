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
//! 3. **Filter Relevance**: Only return expressions that involve specified partition columns
//!
//! ## Clean Separation of Concerns
//!
//! This module ONLY handles DataFusion → PartitionExpr conversion.
//! The proper flow is: LogicalPlan → PredicateExtractor → Vec&lt;PartitionExpr&gt; → ConstraintPruner → Vec&lt;RegionId&gt;
//!
//! ## Expression Conversion
//!
//! The module converts DataFusion expressions to PartitionExpr format:
//! - Handles complex expressions like InList, Between, Alias, Cast, NOT
//! - Preserves logical structure for downstream processing
//! - Does NOT generate atomic constraints - that's ConstraintPruner's responsibility
//!
//! ## Example
//!
//! Given a SQL WHERE clause like:
//! ```sql
//! WHERE (timestamp >= 100 AND timestamp < 200) OR timestamp = 300
//! ```
//!
//! This produces a single PartitionExpr that preserves the logical structure:
//! ```
//! PartitionExpr(
//!   (timestamp >= 100 AND timestamp < 200) OR timestamp = 300
//! )
//! ```

use std::collections::HashSet;

use arrow::datatypes::DataType;
use common_telemetry::debug;
use datafusion_common::Result as DfResult;
use datafusion_expr::{Expr, LogicalPlan, Operator};
use datatypes::value::Value;
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
                // let value = Self::scalar_value_to_value(scalar_value)?;
                let value = Value::try_from(scalar_value.clone()).unwrap();
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
    use std::sync::Arc;

    use common_time::Timestamp;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Column;
    use datafusion::datasource::DefaultTableSource;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{col, lit, LogicalPlanBuilder};
    use datatypes::value::Value;
    use partition::expr::{Operand, PartitionExpr, RestrictedOp};

    use super::*;

    fn create_test_table_scan() -> LogicalPlan {
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
        filter_expr: Expr,
        expected_partition_exprs: Vec<PartitionExpr>,
        partition_columns: Vec<&'static str>,
    }

    impl FilterTestCase {
        fn new(
            name: &'static str,
            filter_expr: Expr,
            expected_partition_exprs: Vec<PartitionExpr>,
            partition_columns: Vec<&'static str>,
        ) -> Self {
            Self {
                name,
                filter_expr,
                expected_partition_exprs,
                partition_columns,
            }
        }
    }

    /// Helper to check partition expressions for a set of test cases.
    fn check_partition_expressions(cases: Vec<FilterTestCase>) {
        for case in cases {
            let table_scan = create_test_table_scan();
            let filter = case.filter_expr.clone();

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
            let partition_exprs =
                PredicateExtractor::extract_partition_expressions(&plan, &partition_columns)
                    .unwrap();
            let expected = case.expected_partition_exprs.clone();
            assert_eq!(
                partition_exprs, expected,
                "Test case '{}': expected partition expressions {:?}, got {:?}",
                case.name, expected, partition_exprs
            );
        }
    }

    #[test]
    fn test_basic_constraints_extraction() {
        let cases = vec![
            FilterTestCase::new(
                "non_partition_column_ignored",
                col("value").gt_eq(lit(100i64)),
                vec![],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "simple_constraint",
                col("user_id").gt_eq(lit(100i64)),
                vec![PartitionExpr::new(
                    Operand::Column("user_id".to_string()),
                    RestrictedOp::GtEq,
                    Operand::Value(Value::Int64(100)),
                )],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "or_expression",
                col("user_id")
                    .eq(lit(100i64))
                    .or(col("user_id").eq(lit(200i64))),
                vec![PartitionExpr::new(
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("user_id".to_string()),
                        RestrictedOp::Eq,
                        Operand::Value(Value::Int64(100)),
                    )),
                    RestrictedOp::Or,
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("user_id".to_string()),
                        RestrictedOp::Eq,
                        Operand::Value(Value::Int64(200)),
                    )),
                )],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "complex_and_or",
                col("user_id")
                    .gt_eq(lit(100i64))
                    .and(col("user_id").lt(lit(200i64)))
                    .or(col("user_id")
                        .gt_eq(lit(300i64))
                        .and(col("user_id").lt(lit(400i64)))),
                vec![PartitionExpr::new(
                    Operand::Expr(PartitionExpr::new(
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("user_id".to_string()),
                            RestrictedOp::GtEq,
                            Operand::Value(Value::Int64(100)),
                        )),
                        RestrictedOp::And,
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("user_id".to_string()),
                            RestrictedOp::Lt,
                            Operand::Value(Value::Int64(200)),
                        )),
                    )),
                    RestrictedOp::Or,
                    Operand::Expr(PartitionExpr::new(
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("user_id".to_string()),
                            RestrictedOp::GtEq,
                            Operand::Value(Value::Int64(300)),
                        )),
                        RestrictedOp::And,
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("user_id".to_string()),
                            RestrictedOp::Lt,
                            Operand::Value(Value::Int64(400)),
                        )),
                    )),
                )],
                vec!["user_id"],
            ),
        ];
        check_partition_expressions(cases);
    }

    #[test]
    fn test_alias_expressions() {
        let cases = vec![
            FilterTestCase::new(
                "simple_alias",
                col("user_id").alias("uid").eq(lit(100i64)),
                vec![PartitionExpr::new(
                    Operand::Column("user_id".to_string()),
                    RestrictedOp::Eq,
                    Operand::Value(Value::Int64(100)),
                )],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "nested_alias",
                col("user_id").alias("uid").alias("u").gt_eq(lit(50i64)),
                vec![PartitionExpr::new(
                    Operand::Column("user_id".to_string()),
                    RestrictedOp::GtEq,
                    Operand::Value(Value::Int64(50)),
                )],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "complex_alias_with_and_or",
                col("user_id")
                    .alias("uid")
                    .gt_eq(lit(100i64))
                    .and(col("user_id").alias("u").lt(lit(200i64)))
                    .or(col("user_id").alias("id").eq(lit(300i64))),
                vec![PartitionExpr::new(
                    Operand::Expr(PartitionExpr::new(
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("user_id".to_string()),
                            RestrictedOp::GtEq,
                            Operand::Value(Value::Int64(100)),
                        )),
                        RestrictedOp::And,
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("user_id".to_string()),
                            RestrictedOp::Lt,
                            Operand::Value(Value::Int64(200)),
                        )),
                    )),
                    RestrictedOp::Or,
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("user_id".to_string()),
                        RestrictedOp::Eq,
                        Operand::Value(Value::Int64(300)),
                    )),
                )],
                vec!["user_id"],
            ),
        ];
        check_partition_expressions(cases);
    }

    #[test]
    fn test_inlist_expressions() {
        let cases = vec![
            FilterTestCase::new(
                "simple_inlist",
                col("user_id").in_list(vec![lit(100i64), lit(200i64), lit(300i64)], false),
                vec![PartitionExpr::new(
                    Operand::Expr(PartitionExpr::new(
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("user_id".to_string()),
                            RestrictedOp::Eq,
                            Operand::Value(Value::Int64(100)),
                        )),
                        RestrictedOp::Or,
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("user_id".to_string()),
                            RestrictedOp::Eq,
                            Operand::Value(Value::Int64(200)),
                        )),
                    )),
                    RestrictedOp::Or,
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("user_id".to_string()),
                        RestrictedOp::Eq,
                        Operand::Value(Value::Int64(300)),
                    )),
                )],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "negated_inlist",
                col("user_id").in_list(vec![lit(100i64), lit(200i64)], true),
                vec![PartitionExpr::new(
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("user_id".to_string()),
                        RestrictedOp::NotEq,
                        Operand::Value(Value::Int64(100)),
                    )),
                    RestrictedOp::And,
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("user_id".to_string()),
                        RestrictedOp::NotEq,
                        Operand::Value(Value::Int64(200)),
                    )),
                )],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "inlist_with_alias",
                col("user_id")
                    .alias("uid")
                    .in_list(vec![lit(100i64), lit(200i64)], false),
                vec![PartitionExpr::new(
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("user_id".to_string()),
                        RestrictedOp::Eq,
                        Operand::Value(Value::Int64(100)),
                    )),
                    RestrictedOp::Or,
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("user_id".to_string()),
                        RestrictedOp::Eq,
                        Operand::Value(Value::Int64(200)),
                    )),
                )],
                vec!["user_id"],
            ),
        ];
        check_partition_expressions(cases);
    }

    #[test]
    fn test_between_expressions() {
        let cases = vec![
            FilterTestCase::new(
                "simple_between",
                col("user_id").between(lit(100i64), lit(200i64)),
                vec![PartitionExpr::new(
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("user_id".to_string()),
                        RestrictedOp::GtEq,
                        Operand::Value(Value::Int64(100)),
                    )),
                    RestrictedOp::And,
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("user_id".to_string()),
                        RestrictedOp::LtEq,
                        Operand::Value(Value::Int64(200)),
                    )),
                )],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "negated_between",
                Expr::Between(datafusion_expr::Between {
                    expr: Box::new(col("user_id")),
                    negated: true,
                    low: Box::new(lit(100i64)),
                    high: Box::new(lit(200i64)),
                }),
                vec![PartitionExpr::new(
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("user_id".to_string()),
                        RestrictedOp::Lt,
                        Operand::Value(Value::Int64(100)),
                    )),
                    RestrictedOp::Or,
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("user_id".to_string()),
                        RestrictedOp::Gt,
                        Operand::Value(Value::Int64(200)),
                    )),
                )],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "between_with_alias",
                col("user_id")
                    .alias("uid")
                    .between(lit(100i64), lit(200i64)),
                vec![PartitionExpr::new(
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("user_id".to_string()),
                        RestrictedOp::GtEq,
                        Operand::Value(Value::Int64(100)),
                    )),
                    RestrictedOp::And,
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("user_id".to_string()),
                        RestrictedOp::LtEq,
                        Operand::Value(Value::Int64(200)),
                    )),
                )],
                vec!["user_id"],
            ),
        ];
        check_partition_expressions(cases);
    }

    #[test]
    fn test_null_expressions() {
        let cases = vec![
            FilterTestCase::new(
                "is_null",
                col("user_id").is_null(),
                vec![PartitionExpr::new(
                    Operand::Column("user_id".to_string()),
                    RestrictedOp::Eq,
                    Operand::Value(Value::Null),
                )],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "is_not_null",
                col("user_id").is_not_null(),
                vec![PartitionExpr::new(
                    Operand::Column("user_id".to_string()),
                    RestrictedOp::NotEq,
                    Operand::Value(Value::Null),
                )],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "null_with_alias",
                col("user_id").alias("uid").is_null(),
                vec![PartitionExpr::new(
                    Operand::Column("user_id".to_string()),
                    RestrictedOp::Eq,
                    Operand::Value(Value::Null),
                )],
                vec!["user_id"],
            ),
        ];
        check_partition_expressions(cases);
    }

    #[test]
    fn test_cast_expressions() {
        let cases = vec![
            FilterTestCase::new(
                "safe_cast",
                Expr::Cast(datafusion_expr::Cast {
                    expr: Box::new(col("user_id")),
                    data_type: DataType::Int64,
                })
                .eq(lit(100i64)),
                vec![PartitionExpr::new(
                    Operand::Column("user_id".to_string()),
                    RestrictedOp::Eq,
                    Operand::Value(Value::Int64(100)),
                )],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "cast_with_alias",
                Expr::Cast(datafusion_expr::Cast {
                    expr: Box::new(col("user_id").alias("uid")),
                    data_type: DataType::Int64,
                })
                .eq(lit(100i64)),
                vec![PartitionExpr::new(
                    Operand::Column("user_id".to_string()),
                    RestrictedOp::Eq,
                    Operand::Value(Value::Int64(100)),
                )],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "unsafe_cast",
                Expr::Cast(datafusion_expr::Cast {
                    expr: Box::new(col("user_id")),
                    data_type: DataType::List(std::sync::Arc::new(
                        datafusion::arrow::datatypes::Field::new("item", DataType::Int32, true),
                    )),
                })
                .eq(lit(100i64)),
                vec![],
                vec!["user_id"],
            ),
        ];
        check_partition_expressions(cases);
    }

    #[test]
    fn test_not_expressions() {
        let cases = vec![
            FilterTestCase::new(
                "not_equality",
                Expr::Not(Box::new(col("user_id").eq(lit(100i64)))),
                vec![PartitionExpr::new(
                    Operand::Column("user_id".to_string()),
                    RestrictedOp::NotEq,
                    Operand::Value(Value::Int64(100)),
                )],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "not_comparison",
                Expr::Not(Box::new(col("user_id").lt(lit(100i64)))),
                vec![PartitionExpr::new(
                    Operand::Column("user_id".to_string()),
                    RestrictedOp::GtEq,
                    Operand::Value(Value::Int64(100)),
                )],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "not_is_null",
                Expr::Not(Box::new(col("user_id").is_null())),
                vec![PartitionExpr::new(
                    Operand::Column("user_id".to_string()),
                    RestrictedOp::NotEq,
                    Operand::Value(Value::Null),
                )],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "not_with_alias",
                Expr::Not(Box::new(col("user_id").alias("uid").eq(lit(100i64)))),
                vec![PartitionExpr::new(
                    Operand::Column("user_id".to_string()),
                    RestrictedOp::NotEq,
                    Operand::Value(Value::Int64(100)),
                )],
                vec!["user_id"],
            ),
        ];
        check_partition_expressions(cases);
    }

    #[test]
    fn test_scalar_value_conversions() {
        let cases = vec![
            FilterTestCase::new(
                "timestamp_scalar",
                {
                    let timestamp_val =
                        ScalarValue::TimestampMillisecond(Some(1672574400000), None);
                    col("timestamp").eq(Expr::Literal(timestamp_val))
                },
                vec![PartitionExpr::new(
                    Operand::Column("timestamp".to_string()),
                    RestrictedOp::Eq,
                    Operand::Value(Value::Timestamp(Timestamp::new_millisecond(1672574400000))),
                )],
                vec!["timestamp"],
            ),
            FilterTestCase::new(
                "float_scalar",
                {
                    let float_val = ScalarValue::Float64(Some(123.45));
                    col("user_id").eq(Expr::Literal(float_val))
                },
                vec![PartitionExpr::new(
                    Operand::Column("user_id".to_string()),
                    RestrictedOp::Eq,
                    Operand::Value(Value::Float64(123.45.into())),
                )],
                vec!["user_id"],
            ),
        ];
        check_partition_expressions(cases);
    }

    #[test]
    fn test_edge_cases() {
        let cases = vec![
            FilterTestCase::new(
                "qualified_column_name",
                {
                    let qualified_col = Expr::Column(Column::new(Some("test"), "user_id"));
                    qualified_col.eq(lit(100i64))
                },
                vec![PartitionExpr::new(
                    Operand::Column("user_id".to_string()),
                    RestrictedOp::Eq,
                    Operand::Value(Value::Int64(100)),
                )],
                vec!["user_id"],
            ),
            FilterTestCase::new(
                "comprehensive_combinations",
                {
                    let in_expr = col("user_id")
                        .alias("uid")
                        .in_list(vec![lit(100i64), lit(200i64)], false);
                    let cast_expr = Expr::Cast(datafusion_expr::Cast {
                        expr: Box::new(col("user_id")),
                        data_type: DataType::Int64,
                    });
                    let between_expr = cast_expr.between(lit(300i64), lit(400i64));
                    in_expr.or(between_expr)
                },
                vec![PartitionExpr::new(
                    Operand::Expr(PartitionExpr::new(
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("user_id".to_string()),
                            RestrictedOp::Eq,
                            Operand::Value(Value::Int64(100)),
                        )),
                        RestrictedOp::Or,
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("user_id".to_string()),
                            RestrictedOp::Eq,
                            Operand::Value(Value::Int64(200)),
                        )),
                    )),
                    RestrictedOp::Or,
                    Operand::Expr(PartitionExpr::new(
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("user_id".to_string()),
                            RestrictedOp::GtEq,
                            Operand::Value(Value::Int64(300)),
                        )),
                        RestrictedOp::And,
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("user_id".to_string()),
                            RestrictedOp::LtEq,
                            Operand::Value(Value::Int64(400)),
                        )),
                    )),
                )],
                vec!["user_id"],
            ),
        ];
        check_partition_expressions(cases);
    }
}
