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
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use datafusion_common::{ScalarValue, ToDFSchema};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::Expr;
use datafusion_physical_expr::{create_physical_expr, PhysicalExpr};
use datatypes::arrow;
use datatypes::value::{
    duration_to_scalar_value, time_to_scalar_value, timestamp_to_scalar_value, OrderedF64,
    OrderedFloat, Value,
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use sql::statements::value_to_sql_value;
use sqlparser::ast::{BinaryOperator as ParserBinaryOperator, Expr as ParserExpr, Ident};

use crate::error;
use crate::error::Result;

const ZERO: OrderedF64 = OrderedFloat(0.0f64);
const NORMALIZE_STEP: OrderedF64 = OrderedFloat(1.0f64);

/// Struct for partition expression. This can be converted back to sqlparser's [Expr].
/// by [`Self::to_parser_expr`].
///
/// [Expr]: sqlparser::ast::Expr
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PartitionExpr {
    pub(crate) lhs: Box<Operand>,
    pub(crate) op: RestrictedOp,
    pub(crate) rhs: Box<Operand>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Operand {
    Column(String),
    Value(Value),
    Expr(PartitionExpr),
}

pub fn col(column_name: impl Into<String>) -> Operand {
    Operand::Column(column_name.into())
}

impl From<Value> for Operand {
    fn from(value: Value) -> Self {
        Operand::Value(value)
    }
}

impl Operand {
    pub fn try_as_logical_expr(&self) -> error::Result<Expr> {
        match self {
            Self::Column(c) => Ok(datafusion_expr::col(format!(r#""{}""#, c))),
            Self::Value(v) => {
                let scalar_value = match v {
                    Value::Boolean(v) => ScalarValue::Boolean(Some(*v)),
                    Value::UInt8(v) => ScalarValue::UInt8(Some(*v)),
                    Value::UInt16(v) => ScalarValue::UInt16(Some(*v)),
                    Value::UInt32(v) => ScalarValue::UInt32(Some(*v)),
                    Value::UInt64(v) => ScalarValue::UInt64(Some(*v)),
                    Value::Int8(v) => ScalarValue::Int8(Some(*v)),
                    Value::Int16(v) => ScalarValue::Int16(Some(*v)),
                    Value::Int32(v) => ScalarValue::Int32(Some(*v)),
                    Value::Int64(v) => ScalarValue::Int64(Some(*v)),
                    Value::Float32(v) => ScalarValue::Float32(Some(v.0)),
                    Value::Float64(v) => ScalarValue::Float64(Some(v.0)),
                    Value::String(v) => ScalarValue::Utf8(Some(v.as_utf8().to_string())),
                    Value::Binary(v) => ScalarValue::Binary(Some(v.to_vec())),
                    Value::Date(v) => ScalarValue::Date32(Some(v.val())),
                    Value::Null => ScalarValue::Null,
                    Value::Timestamp(t) => timestamp_to_scalar_value(t.unit(), Some(t.value())),
                    Value::Time(t) => time_to_scalar_value(*t.unit(), Some(t.value())).unwrap(),
                    Value::IntervalYearMonth(v) => ScalarValue::IntervalYearMonth(Some(v.to_i32())),
                    Value::IntervalDayTime(v) => ScalarValue::IntervalDayTime(Some((*v).into())),
                    Value::IntervalMonthDayNano(v) => {
                        ScalarValue::IntervalMonthDayNano(Some((*v).into()))
                    }
                    Value::Duration(d) => duration_to_scalar_value(d.unit(), Some(d.value())),
                    Value::Decimal128(d) => {
                        let (v, p, s) = d.to_scalar_value();
                        ScalarValue::Decimal128(v, p, s)
                    }
                    other => {
                        return error::UnsupportedPartitionExprValueSnafu {
                            value: other.clone(),
                        }
                        .fail()
                    }
                };
                Ok(datafusion_expr::lit(scalar_value))
            }
            Self::Expr(e) => e.try_as_logical_expr(),
        }
    }

    pub fn lt(self, rhs: impl Into<Self>) -> PartitionExpr {
        PartitionExpr::new(self, RestrictedOp::Lt, rhs.into())
    }

    pub fn gt_eq(self, rhs: impl Into<Self>) -> PartitionExpr {
        PartitionExpr::new(self, RestrictedOp::GtEq, rhs.into())
    }

    pub fn eq(self, rhs: impl Into<Self>) -> PartitionExpr {
        PartitionExpr::new(self, RestrictedOp::Eq, rhs.into())
    }

    pub fn not_eq(self, rhs: impl Into<Self>) -> PartitionExpr {
        PartitionExpr::new(self, RestrictedOp::NotEq, rhs.into())
    }

    pub fn gt(self, rhs: impl Into<Self>) -> PartitionExpr {
        PartitionExpr::new(self, RestrictedOp::Gt, rhs.into())
    }

    pub fn lt_eq(self, rhs: impl Into<Self>) -> PartitionExpr {
        PartitionExpr::new(self, RestrictedOp::LtEq, rhs.into())
    }
}

impl Display for Operand {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Column(v) => write!(f, "{v}"),
            Self::Value(v) => write!(f, "{v}"),
            Self::Expr(v) => write!(f, "{v}"),
        }
    }
}

/// A restricted set of [Operator](datafusion_expr::Operator) that can be used in
/// partition expressions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum RestrictedOp {
    // Evaluate to binary
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,

    // Conjunction
    And,
    Or,
}

impl RestrictedOp {
    pub fn try_from_parser(op: &ParserBinaryOperator) -> Option<Self> {
        match op {
            ParserBinaryOperator::Eq => Some(Self::Eq),
            ParserBinaryOperator::NotEq => Some(Self::NotEq),
            ParserBinaryOperator::Lt => Some(Self::Lt),
            ParserBinaryOperator::LtEq => Some(Self::LtEq),
            ParserBinaryOperator::Gt => Some(Self::Gt),
            ParserBinaryOperator::GtEq => Some(Self::GtEq),
            ParserBinaryOperator::And => Some(Self::And),
            ParserBinaryOperator::Or => Some(Self::Or),
            _ => None,
        }
    }

    pub fn to_parser_op(&self) -> ParserBinaryOperator {
        match self {
            Self::Eq => ParserBinaryOperator::Eq,
            Self::NotEq => ParserBinaryOperator::NotEq,
            Self::Lt => ParserBinaryOperator::Lt,
            Self::LtEq => ParserBinaryOperator::LtEq,
            Self::Gt => ParserBinaryOperator::Gt,
            Self::GtEq => ParserBinaryOperator::GtEq,
            Self::And => ParserBinaryOperator::And,
            Self::Or => ParserBinaryOperator::Or,
        }
    }
}
impl Display for RestrictedOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::NotEq => write!(f, "<>"),
            Self::Lt => write!(f, "<"),
            Self::LtEq => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::GtEq => write!(f, ">="),
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
        }
    }
}

impl PartitionExpr {
    pub fn new(lhs: Operand, op: RestrictedOp, rhs: Operand) -> Self {
        Self {
            lhs: Box::new(lhs),
            op,
            rhs: Box::new(rhs),
        }
    }

    /// Convert [Self] back to sqlparser's [Expr]
    ///
    /// [Expr]: ParserExpr
    pub fn to_parser_expr(&self) -> ParserExpr {
        // Safety: Partition rule won't contains unsupported value type.
        // Otherwise it will be rejected by the parser.
        let lhs = match &*self.lhs {
            Operand::Column(c) => ParserExpr::Identifier(Ident::new(c.clone())),
            Operand::Value(v) => ParserExpr::Value(value_to_sql_value(v).unwrap()),
            Operand::Expr(e) => e.to_parser_expr(),
        };

        let rhs = match &*self.rhs {
            Operand::Column(c) => ParserExpr::Identifier(Ident::new(c.clone())),
            Operand::Value(v) => ParserExpr::Value(value_to_sql_value(v).unwrap()),
            Operand::Expr(e) => e.to_parser_expr(),
        };

        ParserExpr::BinaryOp {
            left: Box::new(lhs),
            op: self.op.to_parser_op(),
            right: Box::new(rhs),
        }
    }

    pub fn try_as_logical_expr(&self) -> error::Result<Expr> {
        let lhs = self.lhs.try_as_logical_expr()?;
        let rhs = self.rhs.try_as_logical_expr()?;

        let expr = match &self.op {
            RestrictedOp::And => datafusion_expr::and(lhs, rhs),
            RestrictedOp::Or => datafusion_expr::or(lhs, rhs),
            RestrictedOp::Gt => lhs.gt(rhs),
            RestrictedOp::GtEq => lhs.gt_eq(rhs),
            RestrictedOp::Lt => lhs.lt(rhs),
            RestrictedOp::LtEq => lhs.lt_eq(rhs),
            RestrictedOp::Eq => lhs.eq(rhs),
            RestrictedOp::NotEq => lhs.not_eq(rhs),
        };
        Ok(expr)
    }

    pub fn try_as_physical_expr(
        &self,
        schema: &arrow::datatypes::SchemaRef,
    ) -> error::Result<Arc<dyn PhysicalExpr>> {
        let df_schema = schema
            .clone()
            .to_dfschema_ref()
            .context(error::ToDFSchemaSnafu)?;
        let execution_props = &ExecutionProps::default();
        let expr = self.try_as_logical_expr()?;
        create_physical_expr(&expr, &df_schema, execution_props)
            .context(error::CreatePhysicalExprSnafu)
    }

    pub fn and(self, rhs: PartitionExpr) -> PartitionExpr {
        PartitionExpr::new(Operand::Expr(self), RestrictedOp::And, Operand::Expr(rhs))
    }
}

impl Display for PartitionExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.lhs, self.op, self.rhs)
    }
}

#[allow(unused)]
pub(crate) struct AtomicExpr {
    /// A (ordered) list of simplified expressions. They are [`RestrictedOp::And`]'ed together.
    nucleons: Vec<NucleonExpr>,
    /// Index to reference the [`PartitionExpr`] that this [`AtomicExpr`] is derived from.
    /// This index is used with `exprs` field in [`MultiDimPartitionRule`](crate::multi_dim::MultiDimPartitionRule).
    source_expr_index: usize,
}

/// A simplified expression representation.
///
/// This struct is used to compose [`AtomicExpr`], hence "nucleon".
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct NucleonExpr {
    column: String,
    op: GluonOp,
    /// Normalized [`Value`].
    value: OrderedF64,
}

/// Further restricted operation set.
///
/// Conjunction operations are removed from [`RestrictedOp`].
/// This enumeration is used to bind elements in [`NucleonExpr`], hence "gluon".
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum GluonOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// Collider is used to colide a list of [`PartitionExpr`] into a list of [`AtomicExpr`]
///
/// It also normalizes the values of the columns in the expressions.
#[allow(unused)]
pub struct Collider<'a> {
    source_exprs: &'a [PartitionExpr],

    atomic_exprs: Vec<AtomicExpr>,
    normalized_values: HashMap<String, Vec<(Value, OrderedF64)>>,
}

impl<'a> Collider<'a> {
    pub fn new(source_exprs: &'a [PartitionExpr]) -> Result<Self> {
        // first walk to collect all values
        let mut values: HashMap<String, Vec<Value>> = HashMap::new();
        for expr in source_exprs {
            Self::collect_column_values_from_expr(expr, &mut values);
        }

        // normalize values, assumes all values on a column are the same type
        let mut normalized_values: HashMap<String, HashMap<Value, OrderedF64>> =
            HashMap::with_capacity(values.len());
        for (column, mut column_values) in values {
            column_values.sort_unstable();
            column_values.dedup(); // Remove duplicates
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
            let exprs = Self::collide_expr(expr, index, &normalized_values);
            atomic_exprs.extend(exprs);
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
    ) {
        // Handle binary operations between column and value
        match (&*expr.lhs, &*expr.rhs) {
            (Operand::Column(col), Operand::Value(val))
            | (Operand::Value(val), Operand::Column(col)) => {
                values.entry(col.clone()).or_default().push(val.clone());
            }
            (Operand::Expr(left_expr), Operand::Expr(right_expr)) => {
                Self::collect_column_values_from_expr(left_expr, values);
                Self::collect_column_values_from_expr(right_expr, values);
            }
            (Operand::Column(_), Operand::Expr(expr))
            | (Operand::Expr(expr), Operand::Column(_)) => {
                Self::collect_column_values_from_expr(expr, values);
            }
            // (Operand::Value(_), Operand::Expr(expr)) | (Operand::Expr(expr), Operand::Value(_)) => {
            //     Self::collect_column_values_from_expr(expr, values);
            // }
            _ => {
                // Other combinations don't directly contribute column-value pairs
                // todo: throw an error
            }
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
    ) -> Vec<AtomicExpr> {
        // todo: share a mutable vec as result set
        match expr.op {
            RestrictedOp::Or => {
                // Split on OR operation - each side becomes a separate atomic expression
                let mut result = Vec::new();

                // Process left side
                match &*expr.lhs {
                    Operand::Expr(left_expr) => {
                        result.extend(Self::collide_expr(left_expr, index, normalized_values));
                    }
                    _ => {
                        // Single operand - this shouldn't happen with OR
                        // OR should always connect two sub-expressions
                        // todo: throw an error
                    }
                }

                // Process right side
                match &*expr.rhs {
                    Operand::Expr(right_expr) => {
                        result.extend(Self::collide_expr(right_expr, index, normalized_values));
                    }
                    _ => {
                        // Single operand - this shouldn't happen with OR
                        // OR should always connect two sub-expressions
                        // todo: throw an error
                    }
                }

                result
            }
            RestrictedOp::And => {
                // For AND operations, we need to combine nucleons
                let mut nucleons = Vec::new();
                Self::collect_nucleons_from_expr(expr, &mut nucleons, normalized_values);

                vec![AtomicExpr {
                    nucleons,
                    source_expr_index: index,
                }]
            }
            _ => {
                // For other operations, create a single atomic expression
                let mut nucleons = Vec::new();
                Self::collect_nucleons_from_expr(expr, &mut nucleons, normalized_values);

                vec![AtomicExpr {
                    nucleons,
                    source_expr_index: index,
                }]
            }
        }
    }

    /// Collect nucleons from an expression (handles AND operations recursively)
    fn collect_nucleons_from_expr(
        expr: &PartitionExpr,
        nucleons: &mut Vec<NucleonExpr>,
        normalized_values: &HashMap<String, HashMap<Value, OrderedF64>>,
    ) {
        match expr.op {
            RestrictedOp::And => {
                // For AND operations, collect nucleons from both sides
                Self::collect_nucleons_from_operand(&*expr.lhs, nucleons, normalized_values);
                Self::collect_nucleons_from_operand(&*expr.rhs, nucleons, normalized_values);
            }
            _ => {
                // For non-AND operations, try to create a nucleon directly
                if let Some(nucleon) =
                    Self::try_create_nucleon(&*expr.lhs, &expr.op, &*expr.rhs, normalized_values)
                {
                    nucleons.push(nucleon);
                }
            }
        }
    }

    /// Collect nucleons from an operand
    fn collect_nucleons_from_operand(
        operand: &Operand,
        nucleons: &mut Vec<NucleonExpr>,
        normalized_values: &HashMap<String, HashMap<Value, OrderedF64>>,
    ) {
        match operand {
            Operand::Expr(expr) => {
                Self::collect_nucleons_from_expr(expr, nucleons, normalized_values);
            }
            _ => {
                // Single operands don't form nucleons by themselves
            }
        }
    }

    /// Try to create a nucleon from operands
    fn try_create_nucleon(
        lhs: &Operand,
        op: &RestrictedOp,
        rhs: &Operand,
        normalized_values: &HashMap<String, HashMap<Value, OrderedF64>>,
    ) -> Option<NucleonExpr> {
        let gluon_op = match op {
            RestrictedOp::Eq => GluonOp::Eq,
            RestrictedOp::NotEq => GluonOp::NotEq,
            RestrictedOp::Lt => GluonOp::Lt,
            RestrictedOp::LtEq => GluonOp::LtEq,
            RestrictedOp::Gt => GluonOp::Gt,
            RestrictedOp::GtEq => GluonOp::GtEq,
            RestrictedOp::And | RestrictedOp::Or => {
                // These should be handled elsewhere
                // todo: throw an error
                return None;
            }
        };

        match (lhs, rhs) {
            (Operand::Column(col), Operand::Value(val)) => {
                if let Some(column_values) = normalized_values.get(col) {
                    if let Some(&normalized_val) = column_values.get(val) {
                        return Some(NucleonExpr {
                            column: col.clone(),
                            op: gluon_op,
                            value: normalized_val,
                        });
                    }
                }
            }
            (Operand::Value(val), Operand::Column(col)) => {
                if let Some(column_values) = normalized_values.get(col) {
                    if let Some(&normalized_val) = column_values.get(val) {
                        // Flip the operation for value op column
                        let flipped_op = match gluon_op {
                            GluonOp::Lt => GluonOp::Gt,
                            GluonOp::LtEq => GluonOp::GtEq,
                            GluonOp::Gt => GluonOp::Lt,
                            GluonOp::GtEq => GluonOp::LtEq,
                            op => op, // Eq and NotEq remain the same
                        };
                        return Some(NucleonExpr {
                            column: col.clone(),
                            op: flipped_op,
                            value: normalized_val,
                        });
                    }
                }
            }
            _ => {
                // Other combinations not supported for nucleons
                // todo: throw an error
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_expr() {
        let cases = [
            (
                Operand::Column("a".to_string()),
                RestrictedOp::Eq,
                Operand::Value(Value::UInt32(10)),
                "a = 10",
            ),
            (
                Operand::Column("a".to_string()),
                RestrictedOp::NotEq,
                Operand::Value(Value::UInt32(10)),
                "a <> 10",
            ),
            (
                Operand::Column("a".to_string()),
                RestrictedOp::Lt,
                Operand::Value(Value::UInt32(10)),
                "a < 10",
            ),
            (
                Operand::Column("a".to_string()),
                RestrictedOp::LtEq,
                Operand::Value(Value::UInt32(10)),
                "a <= 10",
            ),
            (
                Operand::Column("a".to_string()),
                RestrictedOp::Gt,
                Operand::Value(Value::UInt32(10)),
                "a > 10",
            ),
            (
                Operand::Column("a".to_string()),
                RestrictedOp::GtEq,
                Operand::Value(Value::UInt32(10)),
                "a >= 10",
            ),
            (
                Operand::Column("a".to_string()),
                RestrictedOp::And,
                Operand::Column("b".to_string()),
                "a AND b",
            ),
            (
                Operand::Column("a".to_string()),
                RestrictedOp::Or,
                Operand::Column("b".to_string()),
                "a OR b",
            ),
            (
                Operand::Column("a".to_string()),
                RestrictedOp::Or,
                Operand::Expr(PartitionExpr::new(
                    Operand::Column("c".to_string()),
                    RestrictedOp::And,
                    Operand::Column("d".to_string()),
                )),
                "a OR c AND d",
            ),
        ];

        for case in cases {
            let expr = PartitionExpr::new(case.0, case.1.clone(), case.2);
            assert_eq!(case.3, expr.to_string());
        }
    }

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
        let exprs = vec![col("id")
            .eq(Value::UInt32(1))
            .and(col("status").eq(Value::String("active".into())))];

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

        // Test with AND operation (should return None)
        let result = Collider::try_create_nucleon(
            &col("a"),
            &RestrictedOp::And,
            &Operand::Value(Value::UInt32(1)),
            &normalized_values,
        );
        assert!(result.is_none());

        // Test with OR operation (should return None)
        let result = Collider::try_create_nucleon(
            &col("a"),
            &RestrictedOp::Or,
            &Operand::Value(Value::UInt32(1)),
            &normalized_values,
        );
        assert!(result.is_none());

        // Test with Column-Column (should return None)
        let result = Collider::try_create_nucleon(
            &col("a"),
            &RestrictedOp::Eq,
            &col("b"),
            &normalized_values,
        );
        assert!(result.is_none());

        // Test with Value-Value (should return None)
        let result = Collider::try_create_nucleon(
            &Operand::Value(Value::UInt32(1)),
            &RestrictedOp::Eq,
            &Operand::Value(Value::UInt32(2)),
            &normalized_values,
        );
        assert!(result.is_none());

        // Test empty expression list
        let exprs = vec![];
        let collider = Collider::new(&exprs).unwrap();
        assert_eq!(collider.atomic_exprs.len(), 0);
        assert_eq!(collider.normalized_values.len(), 0);
    }
}
