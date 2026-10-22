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
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use api::v1::meta::Partition;
use datafusion_common::{ScalarValue, ToDFSchema};
use datafusion_expr::Expr;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_optimizer::simplify_expressions::ExprSimplifier;
use datafusion_physical_expr::{PhysicalExpr, create_physical_expr};
use datatypes::arrow;
use datatypes::value::{
    Value, duration_to_scalar_value, time_to_scalar_value, timestamp_to_scalar_value,
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use sql::statements::value_to_sql_value;
use sqlparser::ast::{
    BinaryOperator as ParserBinaryOperator, Expr as ParserExpr, Function, FunctionArg,
    FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName,
};

use crate::error;
use crate::function::IcebergHash;
use crate::partition::PartitionBound;

/// Struct for partition expression. This can be converted back to sqlparser's [Expr].
/// by [`Self::to_parser_expr`].
///
/// [Expr]: sqlparser::ast::Expr
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PartitionExpr {
    pub lhs: Box<Operand>,
    pub op: RestrictedOp,
    pub rhs: Box<Operand>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Operand {
    Column(String),
    Value(Value),
    Expr(PartitionExpr),
    IcebergHash(IcebergHash),
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
                    Value::Time(t) => time_to_scalar_value(*t.unit(), Some(t.value()))
                        .context(error::ConvertPartitionExprValueSnafu { value: v.clone() })?,
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
                        .fail();
                    }
                };
                Ok(datafusion_expr::lit(scalar_value))
            }
            Self::Expr(e) => e.try_as_logical_expr(),
            Self::IcebergHash(f) => Ok(f.to_logical_expr()),
        }
    }

    /// Whether this operand is a partition dimension that values are compared against.
    fn is_dimension(&self) -> bool {
        matches!(self, Self::Column(_) | Self::IcebergHash(_))
    }

    fn to_parser_expr(&self) -> ParserExpr {
        // Safety: Partition rule won't contains unsupported value type.
        // Otherwise it will be rejected by the parser.
        match self {
            Self::Column(c) => ParserExpr::Identifier(Ident::new(c.clone())),
            Self::Value(v) => ParserExpr::Value(value_to_sql_value(v).unwrap().into()),
            Self::Expr(e) => e.to_parser_expr(),
            Self::IcebergHash(f) => ParserExpr::Function(Function {
                name: ObjectName::from(vec![Ident::new(IcebergHash::NAME)]),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        ParserExpr::Identifier(Ident::new(f.column())),
                    ))],
                    clauses: vec![],
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
            }),
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
            Self::IcebergHash(v) => write!(f, "{v}"),
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

    fn invert_for_swap(&self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::NotEq => Self::NotEq,
            Self::Lt => Self::Gt,
            Self::LtEq => Self::GtEq,
            Self::Gt => Self::Lt,
            Self::GtEq => Self::LtEq,
            Self::And => Self::And,
            Self::Or => Self::Or,
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
        .canonicalize()
    }

    /// Canonicalize to `Column op Value` form when possible for consistent equality checks.
    pub fn canonicalize(self) -> Self {
        let lhs = Self::canonicalize_operand(*self.lhs);
        let rhs = Self::canonicalize_operand(*self.rhs);
        let mut expr = Self {
            lhs: Box::new(lhs),
            op: self.op,
            rhs: Box::new(rhs),
        };

        if matches!(&*expr.lhs, Operand::Value(_)) && expr.rhs.is_dimension() {
            std::mem::swap(&mut expr.lhs, &mut expr.rhs);
            expr.op = expr.op.invert_for_swap();
        }

        expr
    }

    fn canonicalize_operand(operand: Operand) -> Operand {
        match operand {
            Operand::Expr(expr) => Operand::Expr(expr.canonicalize()),
            other => other,
        }
    }

    /// Convert [Self] back to sqlparser's [Expr]
    ///
    /// [Expr]: ParserExpr
    pub fn to_parser_expr(&self) -> ParserExpr {
        ParserExpr::BinaryOp {
            left: Box::new(self.lhs.to_parser_expr()),
            op: self.op.to_parser_op(),
            right: Box::new(self.rhs.to_parser_expr()),
        }
    }

    pub fn try_as_logical_expr(&self) -> error::Result<Expr> {
        // Row routing compares `Value`s, which order NULL before every other value.
        // Batch routing and region filters treat a NULL result as false, so only the
        // comparisons that hold for NULL need an explicit `IS NULL`.
        let comparison = match (self.lhs.as_ref(), self.rhs.as_ref()) {
            (column, Operand::Value(value)) if column.is_dimension() => {
                Some((column, value, self.op.clone()))
            }
            (Operand::Value(value), column) if column.is_dimension() => {
                Some((column, value, self.op.invert_for_swap()))
            }
            _ => None,
        };
        if let Some((column, value, op)) = comparison {
            let column = column.try_as_logical_expr()?;
            if matches!(value, Value::Null) {
                return match op {
                    RestrictedOp::Eq | RestrictedOp::LtEq => Ok(column.is_null()),
                    RestrictedOp::NotEq | RestrictedOp::Gt => Ok(column.is_not_null()),
                    RestrictedOp::Lt => Ok(datafusion_expr::lit(false)),
                    RestrictedOp::GtEq => Ok(datafusion_expr::lit(true)),
                    RestrictedOp::And | RestrictedOp::Or => {
                        error::InvalidExprSnafu { expr: self.clone() }.fail()
                    }
                };
            }
            let bound = Operand::Value(value.clone()).try_as_logical_expr()?;
            return match op {
                RestrictedOp::Eq => Ok(column.eq(bound)),
                RestrictedOp::NotEq => Ok(column.clone().not_eq(bound).or(column.is_null())),
                RestrictedOp::Lt => Ok(column.clone().lt(bound).or(column.is_null())),
                RestrictedOp::LtEq => Ok(column.clone().lt_eq(bound).or(column.is_null())),
                RestrictedOp::Gt => Ok(column.gt(bound)),
                RestrictedOp::GtEq => Ok(column.gt_eq(bound)),
                RestrictedOp::And | RestrictedOp::Or => {
                    error::InvalidExprSnafu { expr: self.clone() }.fail()
                }
            };
        }

        // Normal cases handling, without NULL
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

    /// Get the left-hand side operand
    pub fn lhs(&self) -> &Operand {
        &self.lhs
    }

    /// Get the right-hand side operand
    pub fn rhs(&self) -> &Operand {
        &self.rhs
    }

    /// Get the operation
    pub fn op(&self) -> &RestrictedOp {
        &self.op
    }

    pub fn try_as_physical_expr(
        &self,
        schema: &arrow::datatypes::SchemaRef,
    ) -> error::Result<Arc<dyn PhysicalExpr>> {
        plan_physical_expr(self.try_as_logical_expr()?, schema)
    }

    pub fn and(self, rhs: PartitionExpr) -> PartitionExpr {
        PartitionExpr::new(Operand::Expr(self), RestrictedOp::And, Operand::Expr(rhs))
    }

    /// Serializes `PartitionExpr` to json string.
    ///
    /// Wraps `PartitionBound::Expr` for compatibility.
    pub fn as_json_str(&self) -> error::Result<String> {
        serde_json::to_string(&PartitionBound::Expr(self.clone()))
            .context(error::SerializeJsonSnafu)
    }

    /// Deserializes `PartitionExpr` from json string.
    ///
    /// Deserializes to `PartitionBound` for compatibility.
    pub fn from_json_str(s: &str) -> error::Result<Option<Self>> {
        if s.is_empty() {
            return Ok(None);
        }

        let bound: PartitionBound = serde_json::from_str(s).context(error::DeserializeJsonSnafu)?;
        match bound {
            PartitionBound::Expr(expr) => Ok(Some(expr.canonicalize())),
            _ => Ok(None),
        }
    }

    /// Converts [Self] to [Partition].
    pub fn as_pb_partition(&self) -> error::Result<Partition> {
        Ok(Partition {
            expression: self.as_json_str()?,
            ..Default::default()
        })
    }

    /// Collects all column names referenced by this expression.
    pub fn collect_column_names(&self, columns: &mut HashSet<String>) {
        Self::collect_operand_columns(&self.lhs, columns);
        Self::collect_operand_columns(&self.rhs, columns);
    }

    fn collect_operand_columns(operand: &Operand, columns: &mut HashSet<String>) {
        match operand {
            Operand::Column(c) => {
                columns.insert(c.clone());
            }
            Operand::Expr(e) => {
                e.collect_column_names(columns);
            }
            Operand::IcebergHash(f) => {
                columns.insert(f.column().to_string());
            }
            Operand::Value(_) => {}
        }
    }
}

/// Plans a logical partition expression for evaluation against `schema`.
pub(crate) fn plan_physical_expr(
    expr: Expr,
    schema: &arrow::datatypes::SchemaRef,
) -> error::Result<Arc<dyn PhysicalExpr>> {
    let df_schema = schema
        .clone()
        .to_dfschema_ref()
        .context(error::ToDFSchemaSnafu)?;
    // Persisted partition bounds keep the unit (and type) they were created
    // with, so after a schema change like widening the time index unit the
    // expression may mix types with the columns it is evaluated against.
    // Coerce it against the schema so comparisons share a common type, the
    // same way `PredicateGroup` does for region partition filters.
    let simplifier = ExprSimplifier::new(
        SimplifyContext::builder()
            .with_schema(df_schema.clone())
            .build(),
    );
    let expr = simplifier
        .coerce(expr, &df_schema)
        .context(error::CoercePartitionExprSnafu)?;
    create_physical_expr(
        &expr,
        &df_schema,
        &ExecutionProps::default(),
        &PhysicalPlanningContext::default(),
    )
    .context(error::CreatePhysicalExprSnafu)
}

impl Display for PartitionExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.lhs, self.op, self.rhs)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common_meta::ddl::alter_table::validate_region_partition_bounds;
    use common_meta::error::Error;
    use common_meta::rpc::router::{Region, RegionRoute};
    use datatypes::data_type::ConcreteDataType;
    use store_api::storage::RegionId;

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
    fn test_try_as_logical_expr_null_equality() {
        let eq_expr = PartitionExpr::new(
            Operand::Column("a".to_string()),
            RestrictedOp::Eq,
            Operand::Value(Value::Null),
        );
        assert_eq!(
            eq_expr.try_as_logical_expr().unwrap().to_string(),
            "a IS NULL"
        );

        let neq_expr = PartitionExpr::new(
            Operand::Column("a".to_string()),
            RestrictedOp::NotEq,
            Operand::Value(Value::Null),
        );
        assert_eq!(
            neq_expr.try_as_logical_expr().unwrap().to_string(),
            "a IS NOT NULL"
        );
    }

    #[test]
    fn test_try_as_logical_expr_null_range_comparison() {
        // Test Lt with column on LHS
        let lt_expr = PartitionExpr::new(
            Operand::Column("a".to_string()),
            RestrictedOp::Lt,
            Operand::Value(Value::Int64(10)),
        );
        assert_eq!(
            lt_expr.try_as_logical_expr().unwrap().to_string(),
            "a < Int64(10) OR a IS NULL"
        );

        // Test Lt with column on RHS
        let lt_expr_rhs_column = PartitionExpr::new(
            Operand::Value(Value::Int64(10)),
            RestrictedOp::Lt,
            Operand::Column("a".to_string()),
        );
        assert_eq!(
            lt_expr_rhs_column
                .try_as_logical_expr()
                .unwrap()
                .to_string(),
            "a > Int64(10)"
        );

        // Test Gt with column on LHS
        let gt_expr = PartitionExpr::new(
            Operand::Column("a".to_string()),
            RestrictedOp::Gt,
            Operand::Value(Value::Int64(10)),
        );
        assert_eq!(
            gt_expr.try_as_logical_expr().unwrap().to_string(),
            "a > Int64(10)"
        );

        // Test Gt with column on RHS
        let gt_expr_rhs_column = PartitionExpr::new(
            Operand::Value(Value::Int64(10)),
            RestrictedOp::Gt,
            Operand::Column("a".to_string()),
        );
        assert_eq!(
            gt_expr_rhs_column
                .try_as_logical_expr()
                .unwrap()
                .to_string(),
            "a < Int64(10) OR a IS NULL"
        );

        // Test GtEq with column on LHS
        let gteq_expr = PartitionExpr::new(
            Operand::Column("a".to_string()),
            RestrictedOp::GtEq,
            Operand::Value(Value::Int64(10)),
        );
        assert_eq!(
            gteq_expr.try_as_logical_expr().unwrap().to_string(),
            "a >= Int64(10)"
        );

        // Test LtEq with column on LHS
        let lteq_expr = PartitionExpr::new(
            Operand::Column("a".to_string()),
            RestrictedOp::LtEq,
            Operand::Value(Value::Int64(10)),
        );
        assert_eq!(
            lteq_expr.try_as_logical_expr().unwrap().to_string(),
            "a <= Int64(10) OR a IS NULL"
        );

        let gteq_expr_rhs_column = PartitionExpr::new(
            Operand::Value(Value::Int64(10)),
            RestrictedOp::GtEq,
            Operand::Column("a".to_string()),
        );
        assert_eq!(
            gteq_expr_rhs_column
                .try_as_logical_expr()
                .unwrap()
                .to_string(),
            "a <= Int64(10) OR a IS NULL"
        );

        let lteq_expr_rhs_column = PartitionExpr::new(
            Operand::Value(Value::Int64(10)),
            RestrictedOp::LtEq,
            Operand::Column("a".to_string()),
        );
        assert_eq!(
            lteq_expr_rhs_column
                .try_as_logical_expr()
                .unwrap()
                .to_string(),
            "a >= Int64(10)"
        );

        let and_expr = PartitionExpr::new(
            Operand::Expr(PartitionExpr::new(
                Operand::Column("a".to_string()),
                RestrictedOp::LtEq,
                Operand::Value(Value::Int64(10)),
            )),
            RestrictedOp::And,
            Operand::Expr(PartitionExpr::new(
                Operand::Column("b".to_string()),
                RestrictedOp::Gt,
                Operand::Value(Value::Int64(5)),
            )),
        );
        assert_eq!(
            and_expr.try_as_logical_expr().unwrap().to_string(),
            "(a <= Int64(10) OR a IS NULL) AND b > Int64(5)"
        );

        let and_expr = PartitionExpr::new(
            Operand::Expr(PartitionExpr::new(
                Operand::Column("a".to_string()),
                RestrictedOp::LtEq,
                Operand::Value(Value::Int64(10)),
            )),
            RestrictedOp::And,
            Operand::Expr(PartitionExpr::new(
                Operand::Column("a".to_string()),
                RestrictedOp::Gt,
                Operand::Value(Value::Int64(5)),
            )),
        );
        assert_eq!(
            and_expr.try_as_logical_expr().unwrap().to_string(),
            "(a <= Int64(10) OR a IS NULL) AND a > Int64(5)"
        );

        let and_expr_strict_lower = PartitionExpr::new(
            Operand::Expr(PartitionExpr::new(
                Operand::Column("a".to_string()),
                RestrictedOp::Lt,
                Operand::Value(Value::Int64(10)),
            )),
            RestrictedOp::And,
            Operand::Expr(PartitionExpr::new(
                Operand::Column("a".to_string()),
                RestrictedOp::GtEq,
                Operand::Value(Value::Int64(5)),
            )),
        );
        assert_eq!(
            and_expr_strict_lower
                .try_as_logical_expr()
                .unwrap()
                .to_string(),
            "(a < Int64(10) OR a IS NULL) AND a >= Int64(5)"
        );

        let and_expr_rhs_column = PartitionExpr::new(
            Operand::Expr(PartitionExpr::new(
                Operand::Value(Value::Int64(10)),
                RestrictedOp::GtEq,
                Operand::Column("a".to_string()),
            )),
            RestrictedOp::And,
            Operand::Expr(PartitionExpr::new(
                Operand::Value(Value::Int64(5)),
                RestrictedOp::Lt,
                Operand::Column("a".to_string()),
            )),
        );
        assert_eq!(
            and_expr_rhs_column
                .try_as_logical_expr()
                .unwrap()
                .to_string(),
            "(a <= Int64(10) OR a IS NULL) AND a > Int64(5)"
        );

        let or_expr_same_column = PartitionExpr::new(
            Operand::Expr(PartitionExpr::new(
                Operand::Column("a".to_string()),
                RestrictedOp::LtEq,
                Operand::Value(Value::Int64(10)),
            )),
            RestrictedOp::Or,
            Operand::Expr(PartitionExpr::new(
                Operand::Column("a".to_string()),
                RestrictedOp::Gt,
                Operand::Value(Value::Int64(5)),
            )),
        );
        assert_eq!(
            or_expr_same_column
                .try_as_logical_expr()
                .unwrap()
                .to_string(),
            "a <= Int64(10) OR a IS NULL OR a > Int64(5)"
        );
    }

    #[test]
    fn test_try_as_logical_expr_rhs_column_without_canonicalize() {
        let gt_expr_rhs_column = PartitionExpr {
            lhs: Box::new(Operand::Value(Value::Int64(10))),
            op: RestrictedOp::Gt,
            rhs: Box::new(Operand::Column("a".to_string())),
        };
        assert_eq!(
            gt_expr_rhs_column
                .try_as_logical_expr()
                .unwrap()
                .to_string(),
            "a < Int64(10) OR a IS NULL"
        );

        let gteq_expr_rhs_column = PartitionExpr {
            lhs: Box::new(Operand::Value(Value::Int64(10))),
            op: RestrictedOp::GtEq,
            rhs: Box::new(Operand::Column("a".to_string())),
        };
        assert_eq!(
            gteq_expr_rhs_column
                .try_as_logical_expr()
                .unwrap()
                .to_string(),
            "a <= Int64(10) OR a IS NULL"
        );
    }

    #[test]
    fn test_serde_partition_expr() {
        let expr = PartitionExpr::new(
            Operand::Column("a".to_string()),
            RestrictedOp::Eq,
            Operand::Value(Value::UInt32(10)),
        );
        let json = expr.as_json_str().unwrap();
        assert_eq!(
            json,
            "{\"Expr\":{\"lhs\":{\"Column\":\"a\"},\"op\":\"Eq\",\"rhs\":{\"Value\":{\"UInt32\":10}}}}"
        );

        let json = r#"{"Expr":{"lhs":{"Column":"a"},"op":"GtEq","rhs":{"Value":{"UInt32":10}}}}"#;
        let expr2 = PartitionExpr::from_json_str(json).unwrap().unwrap();
        let expected = PartitionExpr::new(
            Operand::Column("a".to_string()),
            RestrictedOp::GtEq,
            Operand::Value(Value::UInt32(10)),
        );
        assert_eq!(expr2, expected);

        // empty string
        let json = "";
        let expr3 = PartitionExpr::from_json_str(json).unwrap();
        assert!(expr3.is_none());

        // variants other than Expr
        let json = r#""MaxValue""#;
        let expr4 = PartitionExpr::from_json_str(json).unwrap();
        assert!(expr4.is_none());

        let json = r#"{"Value":{"UInt32":10}}"#;
        let expr5 = PartitionExpr::from_json_str(json).unwrap();
        assert!(expr5.is_none());
    }

    #[test]
    fn test_serde_iceberg_hash_partition_expr() {
        let hash = Operand::IcebergHash(IcebergHash::new("host"));
        let expr = hash.clone().lt(Value::Int32(0));
        let json = expr.as_json_str().unwrap();
        assert_eq!(
            json,
            r#"{"Expr":{"lhs":{"IcebergHash":"host"},"op":"Lt","rhs":{"Value":{"Int32":0}}}}"#
        );
        assert_eq!(PartitionExpr::from_json_str(&json).unwrap().unwrap(), expr);

        let swapped =
            r#"{"Expr":{"lhs":{"Value":{"Int32":0}},"op":"Gt","rhs":{"IcebergHash":"host"}}}"#;
        assert_eq!(
            PartitionExpr::from_json_str(swapped).unwrap().unwrap(),
            expr
        );
        assert_eq!(expr.to_string(), "iceberg_hash(host) < 0");
    }

    #[test]
    fn test_collect_column_names() {
        // Simple expression: col_a = 1 should give {col_a}
        let expr = col("a").eq(Value::Int64(1));
        let mut columns = HashSet::new();
        expr.collect_column_names(&mut columns);
        assert_eq!(columns.len(), 1);
        assert!(columns.contains("a"));

        // Compound AND with same column: col_a >= 0 AND col_a < 10 should give {col_a}
        let expr = col("a")
            .gt_eq(Value::Int64(0))
            .and(col("a").lt(Value::Int64(10)));
        let mut columns = HashSet::new();
        expr.collect_column_names(&mut columns);
        assert_eq!(columns.len(), 1);
        assert!(columns.contains("a"));

        // Multiple columns: col_a >= 0 AND col_b < 10 should give {col_a, col_b}
        let expr = col("a")
            .gt_eq(Value::Int64(0))
            .and(col("b").lt(Value::Int64(10)));
        let mut columns = HashSet::new();
        expr.collect_column_names(&mut columns);
        assert_eq!(columns.len(), 2);
        assert!(columns.contains("a"));
        assert!(columns.contains("b"));

        // Nested expression: (col_a >= 0 AND col_b < 10) AND col_c = 5
        let expr = col("a")
            .gt_eq(Value::Int64(0))
            .and(col("b").lt(Value::Int64(10)))
            .and(col("c").eq(Value::Int64(5)));
        let mut columns = HashSet::new();
        expr.collect_column_names(&mut columns);
        assert_eq!(columns.len(), 3);
        assert!(columns.contains("a"));
        assert!(columns.contains("b"));
        assert!(columns.contains("c"));
    }

    /// `common-meta` mirrors these types to validate ALTERs. If the mirror drifts,
    /// it can't parse the expressions and skips validation, which fails these cases.
    #[test]
    fn test_serialized_expressions_validate_against_type_changes() {
        let overflow_ms = serde_json::from_str::<Value>(
            r#"{"Timestamp":{"value":32503680000000,"unit":"Millisecond"}}"#,
        )
        .unwrap();
        let start_ms =
            serde_json::from_str::<Value>(r#"{"Timestamp":{"value":0,"unit":"Millisecond"}}"#)
                .unwrap();
        let ts_ms = ConcreteDataType::timestamp_millisecond_datatype();
        let ts_ns = ConcreteDataType::timestamp_nanosecond_datatype();

        let routes = |expr: &PartitionExpr| {
            vec![RegionRoute {
                region: Region {
                    id: RegionId::new(1024, 1),
                    partition_expr: expr.as_json_str().unwrap(),
                    ..Default::default()
                },
                ..Default::default()
            }]
        };
        let changed = |column: &str, old_type: ConcreteDataType, new_type: ConcreteDataType| {
            HashMap::from([(column.to_string(), (old_type, new_type))])
        };

        // Fails to compile on a new variant, which the mirror must learn too.
        fn walk(expr: &PartitionExpr) {
            walk_operand(&expr.lhs);
            walk_operand(&expr.rhs);
            match expr.op {
                RestrictedOp::Eq
                | RestrictedOp::NotEq
                | RestrictedOp::Lt
                | RestrictedOp::LtEq
                | RestrictedOp::Gt
                | RestrictedOp::GtEq
                | RestrictedOp::And
                | RestrictedOp::Or => {}
            }
        }
        fn walk_operand(operand: &Operand) {
            match operand {
                Operand::Column(_) | Operand::Value(_) | Operand::IcebergHash(_) => {}
                Operand::Expr(expr) => walk(expr),
            }
        }

        let simple = col("ts").lt(overflow_ms.clone());
        let nested_and = col("ts")
            .gt_eq(start_ms.clone())
            .and(col("ts").lt(overflow_ms.clone()));
        let nested_or = PartitionExpr::new(
            Operand::Expr(col("host").eq(Value::String("a".into()))),
            RestrictedOp::Or,
            Operand::Expr(col("ts").lt(overflow_ms.clone())),
        );
        let with_hash = col("ts")
            .lt(overflow_ms.clone())
            .and(Operand::IcebergHash(IcebergHash::new("host")).lt(Value::Int32(0)));
        for expr in [&simple, &nested_and, &nested_or, &with_hash] {
            walk(expr);
            let err = validate_region_partition_bounds(
                &changed("ts", ts_ms.clone(), ts_ns.clone()),
                &routes(expr),
            )
            .unwrap_err();
            assert!(
                matches!(
                    err,
                    Error::PartitionExprIncompatible { ref column, .. } if column == "ts"
                ),
                "unexpected error: {err}"
            );
        }

        // A representable bound passes.
        let compatible = col("ts").lt(start_ms);
        validate_region_partition_bounds(&changed("ts", ts_ms, ts_ns), &routes(&compatible))
            .unwrap();

        let narrowing = col("v").lt(Value::Int64(3_000_000_000));
        let err = validate_region_partition_bounds(
            &changed(
                "v",
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::int32_datatype(),
            ),
            &routes(&narrowing),
        )
        .unwrap_err();
        assert!(
            matches!(
                err,
                Error::PartitionExprIncompatible { ref column, .. } if column == "v"
            ),
            "unexpected error: {err}"
        );

        // The hash of a column depends on its type.
        let hashed = Operand::IcebergHash(IcebergHash::new("v")).lt(Value::Int32(0));
        let err = validate_region_partition_bounds(
            &changed(
                "v",
                ConcreteDataType::int32_datatype(),
                ConcreteDataType::int64_datatype(),
            ),
            &routes(&hashed),
        )
        .unwrap_err();
        assert!(
            matches!(
                err,
                Error::PartitionExprIncompatible { ref column, .. } if column == "v"
            ),
            "unexpected error: {err}"
        );
    }
}
