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

//! Provides a Collider tool to convert [`PartitionExpr`] into a form that is easier to operate by program.
//!
//! This mod provides the following major structs:
//!
//! - [`Collider`]: The main struct that converts [`PartitionExpr`].
//! - [`AtomicExpr`]: An "atomic" Expression, which isn't composed (OR-ed) of other expressions.
//! - [`NucleonExpr`]: A simplified expression representation.
//! - [`GluonOp`]: Further restricted operation set.
//!
//! On the naming aspect, "collider" is a high-energy machine that cracks particles, "atomic" is a typical
//! non-divisible particle before ~100 years ago, "nucleon" is what composes an atom and "gluon" is the
//! force inside nucleons.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{BinaryExpr, col, lit};
use datatypes::arrow::datatypes::Schema;
use datatypes::value::{OrderedF64, OrderedFloat, Value};

use crate::error;
use crate::error::Result;
use crate::expr::{Operand, PartitionExpr, RestrictedOp};

const ZERO: OrderedF64 = OrderedFloat(0.0f64);
pub(crate) const NORMALIZE_STEP: OrderedF64 = OrderedFloat(1.0f64);
pub(crate) const CHECK_STEP: OrderedF64 = OrderedFloat(0.5f64);

/// Represents an "atomic" Expression, which isn't composed (OR-ed) of other expressions.
#[allow(unused)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomicExpr {
    /// A (ordered) list of simplified expressions. They are [`RestrictedOp::And`]'ed together.
    pub nucleons: Vec<NucleonExpr>,
    /// Index to reference the [`PartitionExpr`] that this [`AtomicExpr`] is derived from.
    /// This index is used with `exprs` field in [`MultiDimPartitionRule`](crate::multi_dim::MultiDimPartitionRule).
    pub source_expr_index: usize,
}

impl AtomicExpr {
    pub fn to_physical_expr(&self, schema: &Schema) -> Arc<dyn PhysicalExpr> {
        let mut exprs = Vec::with_capacity(self.nucleons.len());
        for nucleon in &self.nucleons {
            exprs.push(nucleon.to_physical_expr(schema));
        }
        let result: Arc<dyn PhysicalExpr> = exprs
            .into_iter()
            .reduce(|l, r| Arc::new(BinaryExpr::new(l, Operator::And, r)))
            .unwrap();
        result
    }
}

impl PartialOrd for AtomicExpr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.nucleons.cmp(&other.nucleons))
    }
}

/// A simplified expression representation.
///
/// This struct is used to compose [`AtomicExpr`], hence "nucleon".
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NucleonExpr {
    column: String,
    op: GluonOp,
    /// Normalized [`Value`].
    value: OrderedF64,
}

impl NucleonExpr {
    pub fn to_physical_expr(&self, schema: &Schema) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            col(&self.column, schema).unwrap(),
            self.op.to_operator(),
            lit(*self.value.as_ref()),
        ))
    }

    /// Get the column name
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Get the normalized value
    pub fn value(&self) -> OrderedF64 {
        self.value
    }

    /// Get the operation
    pub fn op(&self) -> &GluonOp {
        &self.op
    }

    pub fn new(column: impl Into<String>, op: GluonOp, value: OrderedF64) -> Self {
        Self {
            column: column.into(),
            op,
            value,
        }
    }
}

/// Further restricted operation set.
///
/// Conjunction operations are removed from [`RestrictedOp`].
/// This enumeration is used to bind elements in [`NucleonExpr`], hence "gluon".
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GluonOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl GluonOp {
    pub fn to_operator(&self) -> Operator {
        match self {
            GluonOp::Eq => Operator::Eq,
            GluonOp::NotEq => Operator::NotEq,
            GluonOp::Lt => Operator::Lt,
            GluonOp::LtEq => Operator::LtEq,
            GluonOp::Gt => Operator::Gt,
            GluonOp::GtEq => Operator::GtEq,
        }
    }
}

/// Collider is used to collide a list of [`PartitionExpr`] into a list of [`AtomicExpr`]
///
/// It also normalizes the values of the columns in the expressions.
#[allow(unused)]
pub struct Collider<'a> {
    source_exprs: &'a [PartitionExpr],

    pub atomic_exprs: Vec<AtomicExpr>,
    /// A map of column name to a list of `(value, normalized value)` pairs.
    ///
    /// The normalized value is used for comparison. The normalization process keeps the order of the values.
    pub normalized_values: HashMap<String, Vec<(Value, OrderedF64)>>,
}

impl<'a> Collider<'a> {
    pub fn new(source_exprs: &'a [PartitionExpr]) -> Result<Self> {
        // first walk to collect all values
        let mut values: HashMap<String, Vec<Value>> = HashMap::new();
        for expr in source_exprs {
            Self::collect_column_values_from_expr(expr, &mut values)?;
        }

        // normalize values, assumes all values on a column are the same type
        let mut normalized_values: HashMap<String, HashMap<Value, OrderedF64>> =
            HashMap::with_capacity(values.len());
        for (column, mut column_values) in values {
            column_values.sort_unstable();
            column_values.dedup(); // Remove duplicates

            // allowed because we have carefully implemented `Hash` to eliminate the mutable
            #[allow(clippy::mutable_key_type)]
            let mut value_map = HashMap::with_capacity(column_values.len());
            let mut start_value = ZERO;
            for value in column_values {
                value_map.insert(value, start_value);
                start_value += NORMALIZE_STEP;
            }
            normalized_values.insert(column, value_map);
        }

        // second walk to get atomic exprs
        let mut atomic_exprs = Vec::with_capacity(source_exprs.len());
        for (index, expr) in source_exprs.iter().enumerate() {
            Self::collide_expr(expr, index, &normalized_values, &mut atomic_exprs)?;
        }
        // sort nucleon exprs
        for expr in &mut atomic_exprs {
            expr.nucleons.sort_unstable();
        }

        // convert normalized values to a map
        let normalized_values = normalized_values
            .into_iter()
            .map(|(col, values)| {
                let mut values = values.into_iter().collect::<Vec<_>>();
                values.sort_unstable_by_key(|(_, v)| *v);
                (col, values)
            })
            .collect();

        Ok(Self {
            source_exprs,
            atomic_exprs,
            normalized_values,
        })
    }

    /// Helper to collect values with their associated columns from an expression
    fn collect_column_values_from_expr(
        expr: &PartitionExpr,
        values: &mut HashMap<String, Vec<Value>>,
    ) -> Result<()> {
        // Handle binary operations between column and value
        match (&*expr.lhs, &*expr.rhs) {
            (Operand::Column(col), Operand::Value(val))
            | (Operand::Value(val), Operand::Column(col)) => {
                values.entry(col.clone()).or_default().push(val.clone());
                Ok(())
            }
            (Operand::Expr(left_expr), Operand::Expr(right_expr)) => {
                Self::collect_column_values_from_expr(left_expr, values)?;
                Self::collect_column_values_from_expr(right_expr, values)
            }
            // Other combinations don't directly contribute column-value pairs
            _ => error::InvalidExprSnafu { expr: expr.clone() }.fail(),
        }
    }

    /// Collide a [`PartitionExpr`] into multiple [`AtomicExpr`]s.
    ///
    /// Split the [`PartitionExpr`] on every [`RestrictedOp::Or`] (disjunction), each branch is an [`AtomicExpr`].
    /// Since [`PartitionExpr`] doesn't allow parentheses, Expression like `(a = 1 OR b = 2) AND c = 3` won't occur.
    /// We can safely split on every [`RestrictedOp::Or`].
    fn collide_expr(
        expr: &PartitionExpr,
        index: usize,
        normalized_values: &HashMap<String, HashMap<Value, OrderedF64>>,
        result: &mut Vec<AtomicExpr>,
    ) -> Result<()> {
        match expr.op {
            RestrictedOp::Or => {
                // Split on OR operation - each side becomes a separate atomic expression

                // Process left side
                match &*expr.lhs {
                    Operand::Expr(left_expr) => {
                        Self::collide_expr(left_expr, index, normalized_values, result)?;
                    }
                    _ => {
                        // Single operand - this shouldn't happen with OR
                        // OR should always connect two sub-expressions
                        return error::InvalidExprSnafu { expr: expr.clone() }.fail();
                    }
                }

                // Process right side
                match &*expr.rhs {
                    Operand::Expr(right_expr) => {
                        Self::collide_expr(right_expr, index, normalized_values, result)?;
                    }
                    _ => {
                        // Single operand - this shouldn't happen with OR
                        // OR should always connect two sub-expressions
                        return error::InvalidExprSnafu { expr: expr.clone() }.fail();
                    }
                }
            }
            RestrictedOp::And => {
                // For AND operations, we need to combine nucleons
                let mut nucleons = Vec::new();
                Self::collect_nucleons_from_expr(expr, &mut nucleons, normalized_values)?;

                result.push(AtomicExpr {
                    nucleons,
                    source_expr_index: index,
                });
            }
            _ => {
                // For other operations, create a single atomic expression
                let mut nucleons = Vec::new();
                Self::collect_nucleons_from_expr(expr, &mut nucleons, normalized_values)?;

                result.push(AtomicExpr {
                    nucleons,
                    source_expr_index: index,
                });
            }
        }
        Ok(())
    }

    /// Collect nucleons from an expression (handles AND operations recursively)
    fn collect_nucleons_from_expr(
        expr: &PartitionExpr,
        nucleons: &mut Vec<NucleonExpr>,
        normalized_values: &HashMap<String, HashMap<Value, OrderedF64>>,
    ) -> Result<()> {
        match expr.op {
            RestrictedOp::And => {
                // For AND operations, collect nucleons from both sides
                Self::collect_nucleons_from_operand(&expr.lhs, nucleons, normalized_values)?;
                Self::collect_nucleons_from_operand(&expr.rhs, nucleons, normalized_values)?;
            }
            _ => {
                // For non-AND operations, try to create a nucleon directly
                nucleons.push(Self::try_create_nucleon(
                    &expr.lhs,
                    &expr.op,
                    &expr.rhs,
                    normalized_values,
                )?);
            }
        }
        Ok(())
    }

    /// Collect nucleons from an operand
    fn collect_nucleons_from_operand(
        operand: &Operand,
        nucleons: &mut Vec<NucleonExpr>,
        normalized_values: &HashMap<String, HashMap<Value, OrderedF64>>,
    ) -> Result<()> {
        match operand {
            Operand::Expr(expr) => {
                Self::collect_nucleons_from_expr(expr, nucleons, normalized_values)
            }
            _ => {
                // Only `Operand::Expr` can be conjuncted by AND.
                error::NoExprOperandSnafu {
                    operand: operand.clone(),
                }
                .fail()
            }
        }
    }

    /// Try to create a nucleon from operands
    fn try_create_nucleon(
        lhs: &Operand,
        op: &RestrictedOp,
        rhs: &Operand,
        normalized_values: &HashMap<String, HashMap<Value, OrderedF64>>,
    ) -> Result<NucleonExpr> {
        let gluon_op = match op {
            RestrictedOp::Eq => GluonOp::Eq,
            RestrictedOp::NotEq => GluonOp::NotEq,
            RestrictedOp::Lt => GluonOp::Lt,
            RestrictedOp::LtEq => GluonOp::LtEq,
            RestrictedOp::Gt => GluonOp::Gt,
            RestrictedOp::GtEq => GluonOp::GtEq,
            RestrictedOp::And | RestrictedOp::Or => {
                // These should be handled elsewhere
                return error::UnexpectedSnafu {
                    err_msg: format!("Conjunction operation {:?} should be handled elsewhere", op),
                }
                .fail();
            }
        };

        match (lhs, rhs) {
            (Operand::Column(col), Operand::Value(val)) => {
                if let Some(column_values) = normalized_values.get(col)
                    && let Some(&normalized_val) = column_values.get(val)
                {
                    return Ok(NucleonExpr {
                        column: col.clone(),
                        op: gluon_op,
                        value: normalized_val,
                    });
                }
            }
            (Operand::Value(val), Operand::Column(col)) => {
                if let Some(column_values) = normalized_values.get(col)
                    && let Some(&normalized_val) = column_values.get(val)
                {
                    // Flip the operation for value op column
                    let flipped_op = match gluon_op {
                        GluonOp::Lt => GluonOp::Gt,
                        GluonOp::LtEq => GluonOp::GtEq,
                        GluonOp::Gt => GluonOp::Lt,
                        GluonOp::GtEq => GluonOp::LtEq,
                        op => op, // Eq and NotEq remain the same
                    };
                    return Ok(NucleonExpr {
                        column: col.clone(),
                        op: flipped_op,
                        value: normalized_val,
                    });
                }
            }
            _ => {}
        }

        // Other combinations not supported for nucleons
        error::InvalidExprSnafu {
            expr: PartitionExpr::new(lhs.clone(), op.clone(), rhs.clone()),
        }
        .fail()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::expr::col;

    #[test]
    fn test_collider_basic_value_normalization() {
        // Test with different value types in different columns
        let exprs = vec![
            // Integer values
            col("age").eq(Value::UInt32(25)),
            col("age").eq(Value::UInt32(30)),
            col("age").eq(Value::UInt32(25)), // Duplicate should be handled
            // String values
            col("name").eq(Value::String("alice".into())),
            col("name").eq(Value::String("bob".into())),
            // Boolean values
            col("active").eq(Value::Boolean(true)),
            col("active").eq(Value::Boolean(false)),
            // Float values
            col("score").eq(Value::Float64(OrderedFloat(95.5))),
            col("score").eq(Value::Float64(OrderedFloat(87.2))),
        ];

        let collider = Collider::new(&exprs).expect("Failed to create collider");

        // Check that we have the right number of columns
        assert_eq!(collider.normalized_values.len(), 4);

        // Check age column - should have 2 unique values (25, 30)
        let age_values = &collider.normalized_values["age"];
        assert_eq!(age_values.len(), 2);
        assert_eq!(
            age_values,
            &[
                (Value::UInt32(25), OrderedFloat(0.0f64)),
                (Value::UInt32(30), OrderedFloat(1.0f64))
            ]
        );

        // Check name column - should have 2 values
        let name_values = &collider.normalized_values["name"];
        assert_eq!(name_values.len(), 2);
        assert_eq!(
            name_values,
            &[
                (Value::String("alice".into()), OrderedFloat(0.0f64)),
                (Value::String("bob".into()), OrderedFloat(1.0f64))
            ]
        );

        // Check active column - should have 2 values
        let active_values = &collider.normalized_values["active"];
        assert_eq!(active_values.len(), 2);
        assert_eq!(
            active_values,
            &[
                (Value::Boolean(false), OrderedFloat(0.0f64)),
                (Value::Boolean(true), OrderedFloat(1.0f64))
            ]
        );

        // Check score column - should have 2 values
        let score_values = &collider.normalized_values["score"];
        assert_eq!(score_values.len(), 2);
        assert_eq!(
            score_values,
            &[
                (Value::Float64(OrderedFloat(87.2)), OrderedFloat(0.0f64)),
                (Value::Float64(OrderedFloat(95.5)), OrderedFloat(1.0f64))
            ]
        );
    }

    #[test]
    fn test_collider_simple_expressions() {
        // Test simple equality
        let exprs = vec![col("id").eq(Value::UInt32(1))];

        let collider = Collider::new(&exprs).unwrap();
        assert_eq!(collider.atomic_exprs.len(), 1);
        assert_eq!(collider.atomic_exprs[0].nucleons.len(), 1);
        assert_eq!(collider.atomic_exprs[0].source_expr_index, 0);

        // Test simple AND
        let exprs = vec![
            col("id")
                .eq(Value::UInt32(1))
                .and(col("status").eq(Value::String("active".into()))),
        ];

        let collider = Collider::new(&exprs).unwrap();
        assert_eq!(collider.atomic_exprs.len(), 1);
        assert_eq!(collider.atomic_exprs[0].nucleons.len(), 2);

        // Test simple OR - should create 2 atomic expressions
        let expr = PartitionExpr::new(
            Operand::Expr(col("id").eq(Value::UInt32(1))),
            RestrictedOp::Or,
            Operand::Expr(col("id").eq(Value::UInt32(2))),
        );
        let exprs = vec![expr];

        let collider = Collider::new(&exprs).unwrap();
        assert_eq!(collider.atomic_exprs.len(), 2);
        assert_eq!(collider.atomic_exprs[0].nucleons.len(), 1);
        assert_eq!(collider.atomic_exprs[1].nucleons.len(), 1);
    }

    #[test]
    fn test_collider_complex_nested_expressions() {
        // Test: (id = 1 AND status = 'active') OR (id = 2 AND status = 'inactive') OR (id = 3)
        let branch1 = col("id")
            .eq(Value::UInt32(1))
            .and(col("status").eq(Value::String("active".into())));
        let branch2 = col("id")
            .eq(Value::UInt32(2))
            .and(col("status").eq(Value::String("inactive".into())));
        let branch3 = col("id").eq(Value::UInt32(3));

        let expr = PartitionExpr::new(
            Operand::Expr(PartitionExpr::new(
                Operand::Expr(branch1),
                RestrictedOp::Or,
                Operand::Expr(branch2),
            )),
            RestrictedOp::Or,
            Operand::Expr(branch3),
        );

        let exprs = vec![expr];
        let collider = Collider::new(&exprs).unwrap();

        assert_eq!(collider.atomic_exprs.len(), 3);

        let total_nucleons: usize = collider
            .atomic_exprs
            .iter()
            .map(|ae| ae.nucleons.len())
            .sum();
        assert_eq!(total_nucleons, 5);
    }

    #[test]
    fn test_collider_deep_nesting() {
        // Test deeply nested AND operations: a = 1 AND b = 2 AND c = 3 AND d = 4
        let expr = col("a")
            .eq(Value::UInt32(1))
            .and(col("b").eq(Value::UInt32(2)))
            .and(col("c").eq(Value::UInt32(3)))
            .and(col("d").eq(Value::UInt32(4)));

        let exprs = vec![expr];
        let collider = Collider::new(&exprs).unwrap();

        assert_eq!(collider.atomic_exprs.len(), 1);
        assert_eq!(collider.atomic_exprs[0].nucleons.len(), 4);

        // All nucleons should have Eq operation
        for nucleon in &collider.atomic_exprs[0].nucleons {
            assert_eq!(nucleon.op, GluonOp::Eq);
        }
    }

    #[test]
    fn test_collider_multiple_expressions() {
        // Test multiple separate expressions
        let exprs = vec![
            col("id").eq(Value::UInt32(1)),
            col("name").eq(Value::String("alice".into())),
            col("score").gt_eq(Value::Float64(OrderedFloat(90.0))),
        ];

        let collider = Collider::new(&exprs).unwrap();

        // Should create 3 atomic expressions (one for each input expression)
        assert_eq!(collider.atomic_exprs.len(), 3);

        // Each should have exactly 1 nucleon
        for atomic_expr in &collider.atomic_exprs {
            assert_eq!(atomic_expr.nucleons.len(), 1);
        }

        // Check that source indices are correct
        let indices: Vec<usize> = collider
            .atomic_exprs
            .iter()
            .map(|ae| ae.source_expr_index)
            .collect();
        assert!(indices.contains(&0));
        assert!(indices.contains(&1));
        assert!(indices.contains(&2));
    }

    #[test]
    fn test_collider_value_column_order() {
        // Test expressions where value comes before column (should flip operation)
        let expr1 = PartitionExpr::new(
            Operand::Value(Value::UInt32(10)),
            RestrictedOp::Lt,
            Operand::Column("age".to_string()),
        ); // 10 < age should become age > 10

        let expr2 = PartitionExpr::new(
            Operand::Value(Value::UInt32(20)),
            RestrictedOp::GtEq,
            Operand::Column("score".to_string()),
        ); // 20 >= score should become score <= 20

        let exprs = vec![expr1, expr2];
        let collider = Collider::new(&exprs).unwrap();

        assert_eq!(collider.atomic_exprs.len(), 2);

        // Check that operations were flipped correctly
        let operations: Vec<GluonOp> = collider
            .atomic_exprs
            .iter()
            .map(|ae| ae.nucleons[0].op.clone())
            .collect();

        assert!(operations.contains(&GluonOp::Gt)); // 10 < age -> age > 10
        assert!(operations.contains(&GluonOp::LtEq)); // 20 >= score -> score <= 20
    }

    #[test]
    fn test_collider_complex_or_with_different_columns() {
        // Test: (name = 'alice' AND age = 25) OR (status = 'active' AND score > 90)
        let branch1 = col("name")
            .eq(Value::String("alice".into()))
            .and(col("age").eq(Value::UInt32(25)));

        let branch2 = col("status")
            .eq(Value::String("active".into()))
            .and(PartitionExpr::new(
                Operand::Column("score".to_string()),
                RestrictedOp::Gt,
                Operand::Value(Value::Float64(OrderedFloat(90.0))),
            ));

        let expr = PartitionExpr::new(
            Operand::Expr(branch1),
            RestrictedOp::Or,
            Operand::Expr(branch2),
        );

        let exprs = vec![expr];
        let collider = Collider::new(&exprs).expect("Failed to create collider");

        // Should create 2 atomic expressions
        assert_eq!(collider.atomic_exprs.len(), 2);

        // Each atomic expression should have 2 nucleons
        for atomic_expr in &collider.atomic_exprs {
            assert_eq!(atomic_expr.nucleons.len(), 2);
        }

        // Should have normalized values for all 4 columns
        assert_eq!(collider.normalized_values.len(), 4);
        assert!(collider.normalized_values.contains_key("name"));
        assert!(collider.normalized_values.contains_key("age"));
        assert!(collider.normalized_values.contains_key("status"));
        assert!(collider.normalized_values.contains_key("score"));
    }

    #[test]
    fn test_try_create_nucleon_edge_cases() {
        let normalized_values = HashMap::new();

        // Test with AND operation
        let result = Collider::try_create_nucleon(
            &col("a"),
            &RestrictedOp::And,
            &Operand::Value(Value::UInt32(1)),
            &normalized_values,
        );
        assert!(result.is_err());

        // Test with OR operation
        let result = Collider::try_create_nucleon(
            &col("a"),
            &RestrictedOp::Or,
            &Operand::Value(Value::UInt32(1)),
            &normalized_values,
        );
        assert!(result.is_err());

        // Test with Column-Column
        let result = Collider::try_create_nucleon(
            &col("a"),
            &RestrictedOp::Eq,
            &col("b"),
            &normalized_values,
        );
        assert!(result.is_err());

        // Test with Value-Value
        let result = Collider::try_create_nucleon(
            &Operand::Value(Value::UInt32(1)),
            &RestrictedOp::Eq,
            &Operand::Value(Value::UInt32(2)),
            &normalized_values,
        );
        assert!(result.is_err());

        // Test empty expression list
        let exprs = vec![];
        let collider = Collider::new(&exprs).unwrap();
        assert_eq!(collider.atomic_exprs.len(), 0);
        assert_eq!(collider.normalized_values.len(), 0);
    }
}
