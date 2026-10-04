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

use arrow_schema::DataType;
use datafusion_common::{DFSchema, ExprSchema, Result, ScalarValue};
use datafusion_expr::expr::BinaryExpr;
use datafusion_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion_expr::{Expr, Operator};
use datafusion_pg_catalog::pg_catalog::oid_field::{OID_ALIAS_KEY, kind};
use sqlparser::ast::BinaryOperator;

/// Rewrites PostgreSQL's regproc zero sentinel before DataFusion type coercion.
#[derive(Debug)]
pub(crate) struct PgOidAliasExprPlanner;

impl ExprPlanner for PgOidAliasExprPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        let RawBinaryExpr {
            op,
            mut left,
            mut right,
        } = expr;

        let operator = match op {
            BinaryOperator::Eq => Operator::Eq,
            BinaryOperator::NotEq => Operator::NotEq,
            _ => return Ok(PlannerResult::Original(RawBinaryExpr { op, left, right })),
        };

        let (column, zero_on_left) = match (&left, &right) {
            (Expr::Literal(value, _), Expr::Column(column)) if is_integral_zero(value) => {
                (column, true)
            }
            (Expr::Column(column), Expr::Literal(value, _)) if is_integral_zero(value) => {
                (column, false)
            }
            _ => return Ok(PlannerResult::Original(RawBinaryExpr { op, left, right })),
        };

        // A raw SQL column is resolved against the schema before the default
        // coercion planner runs. Do not infer alias semantics from casts or any
        // other expression shape.
        let Ok(field) = schema.field_from_column(column) else {
            return Ok(PlannerResult::Original(RawBinaryExpr { op, left, right }));
        };
        if field.metadata().get(OID_ALIAS_KEY).map(String::as_str) != Some(kind::REGPROC) {
            return Ok(PlannerResult::Original(RawBinaryExpr { op, left, right }));
        }

        let Some(sentinel) = regproc_zero_sentinel(field.data_type()) else {
            return Ok(PlannerResult::Original(RawBinaryExpr { op, left, right }));
        };

        let sentinel = Expr::Literal(sentinel, None);
        if zero_on_left {
            left = sentinel;
        } else {
            right = sentinel;
        }

        Ok(PlannerResult::Planned(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            operator,
            Box::new(right),
        ))))
    }
}

fn is_integral_zero(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Int8(Some(0))
            | ScalarValue::Int16(Some(0))
            | ScalarValue::Int32(Some(0))
            | ScalarValue::Int64(Some(0))
            | ScalarValue::UInt8(Some(0))
            | ScalarValue::UInt16(Some(0))
            | ScalarValue::UInt32(Some(0))
            | ScalarValue::UInt64(Some(0))
    )
}

fn regproc_zero_sentinel(data_type: &DataType) -> Option<ScalarValue> {
    match data_type {
        DataType::Utf8 => Some(ScalarValue::Utf8(Some("-".to_string()))),
        DataType::LargeUtf8 => Some(ScalarValue::LargeUtf8(Some("-".to_string()))),
        DataType::Utf8View => Some(ScalarValue::Utf8View(Some("-".to_string()))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::{Field, Fields};
    use datafusion_common::Column;
    use datafusion_expr::ExprSchemable;
    use datafusion_expr::expr::Cast;
    use datafusion_expr::simplify::SimplifyContext;
    use datafusion_optimizer::simplify_expressions::ExprSimplifier;

    use super::*;

    fn schema(data_type: DataType, alias: Option<&str>) -> DFSchema {
        let mut field = Field::new("typreceive", data_type, true);
        if let Some(alias) = alias {
            field = field.with_metadata(HashMap::from([(
                OID_ALIAS_KEY.to_string(),
                alias.to_string(),
            )]));
        }
        DFSchema::from_unqualified_fields(Fields::from(vec![field]), HashMap::new()).unwrap()
    }

    fn column() -> Expr {
        Expr::Column(Column::new_unqualified("typreceive"))
    }

    fn plan(expr: RawBinaryExpr, schema: &DFSchema) -> PlannerResult<RawBinaryExpr> {
        PgOidAliasExprPlanner.plan_binary_op(expr, schema).unwrap()
    }

    fn assert_planned_sentinel(
        planned: PlannerResult<RawBinaryExpr>,
        operator: Operator,
        zero_on_left: bool,
        sentinel: ScalarValue,
    ) -> Expr {
        let PlannerResult::Planned(Expr::BinaryExpr(expr)) = planned else {
            panic!("expected a planned binary expression");
        };
        assert_eq!(expr.op, operator);
        let literal = Expr::Literal(sentinel, None);
        if zero_on_left {
            assert_eq!(expr.left.as_ref(), &literal);
            assert_eq!(expr.right.as_ref(), &column());
        } else {
            assert_eq!(expr.left.as_ref(), &column());
            assert_eq!(expr.right.as_ref(), &literal);
        }
        Expr::BinaryExpr(expr)
    }

    #[test]
    fn rewrites_zero_regproc_comparisons_in_both_operand_orders() {
        let schema = schema(DataType::Utf8, Some(kind::REGPROC));

        for (sql_operator, operator) in [
            (BinaryOperator::Eq, Operator::Eq),
            (BinaryOperator::NotEq, Operator::NotEq),
        ] {
            for zero_on_left in [true, false] {
                let zero = Expr::Literal(ScalarValue::Int64(Some(0)), None);
                let (left, right) = if zero_on_left {
                    (zero, column())
                } else {
                    (column(), zero)
                };
                let planned = assert_planned_sentinel(
                    plan(
                        RawBinaryExpr {
                            op: sql_operator.clone(),
                            left,
                            right,
                        },
                        &schema,
                    ),
                    operator,
                    zero_on_left,
                    ScalarValue::Utf8(Some("-".to_string())),
                );
                assert!(planned.nullable(&schema).unwrap());
            }
        }
    }

    #[test]
    fn rewrites_every_integral_zero_with_the_column_string_storage_type() {
        let zero_literals = [
            ScalarValue::Int8(Some(0)),
            ScalarValue::Int16(Some(0)),
            ScalarValue::Int32(Some(0)),
            ScalarValue::Int64(Some(0)),
            ScalarValue::UInt8(Some(0)),
            ScalarValue::UInt16(Some(0)),
            ScalarValue::UInt32(Some(0)),
            ScalarValue::UInt64(Some(0)),
        ];
        let string_types = [
            (DataType::Utf8, ScalarValue::Utf8(Some("-".to_string()))),
            (
                DataType::LargeUtf8,
                ScalarValue::LargeUtf8(Some("-".to_string())),
            ),
            (
                DataType::Utf8View,
                ScalarValue::Utf8View(Some("-".to_string())),
            ),
        ];

        for (data_type, sentinel) in string_types {
            let schema = schema(data_type, Some(kind::REGPROC));
            for zero in &zero_literals {
                assert_planned_sentinel(
                    plan(
                        RawBinaryExpr {
                            op: BinaryOperator::Eq,
                            left: column(),
                            right: Expr::Literal(zero.clone(), None),
                        },
                        &schema,
                    ),
                    Operator::Eq,
                    false,
                    sentinel.clone(),
                );
            }
        }
    }

    #[test]
    fn leaves_non_matching_comparisons_untouched() {
        let regproc = schema(DataType::Utf8, Some(kind::REGPROC));
        let int32_regproc = schema(DataType::Int32, Some(kind::REGPROC));
        let untagged = schema(DataType::Utf8, None);
        let regtype = schema(DataType::Utf8, Some(kind::REGTYPE));

        let cases = [
            (
                RawBinaryExpr {
                    op: BinaryOperator::Eq,
                    left: column(),
                    right: Expr::Literal(ScalarValue::Int64(Some(1)), None),
                },
                &regproc,
            ),
            (
                RawBinaryExpr {
                    op: BinaryOperator::Eq,
                    left: column(),
                    right: Expr::Literal(ScalarValue::Int64(None), None),
                },
                &regproc,
            ),
            (
                RawBinaryExpr {
                    op: BinaryOperator::Lt,
                    left: column(),
                    right: Expr::Literal(ScalarValue::Int64(Some(0)), None),
                },
                &regproc,
            ),
            (
                RawBinaryExpr {
                    op: BinaryOperator::Eq,
                    left: column(),
                    right: Expr::Literal(ScalarValue::Int64(Some(0)), None),
                },
                &int32_regproc,
            ),
            (
                RawBinaryExpr {
                    op: BinaryOperator::Eq,
                    left: column(),
                    right: Expr::Literal(ScalarValue::Int64(Some(0)), None),
                },
                &untagged,
            ),
            (
                RawBinaryExpr {
                    op: BinaryOperator::Eq,
                    left: column(),
                    right: Expr::Literal(ScalarValue::Int64(Some(0)), None),
                },
                &regtype,
            ),
        ];

        for (expr, schema) in cases {
            assert!(matches!(plan(expr, schema), PlannerResult::Original(_)));
        }
    }

    #[test]
    fn leaves_casts_and_the_adbc_array_receiver_predicate_untouched() {
        let schema = schema(DataType::Utf8, Some(kind::REGPROC));
        let cast = Expr::Cast(Cast::new(Box::new(column()), DataType::Utf8));
        let expr = RawBinaryExpr {
            op: BinaryOperator::NotEq,
            left: cast,
            right: Expr::Literal(ScalarValue::Utf8(Some("array_recv".to_string())), None),
        };

        assert!(matches!(plan(expr, &schema), PlannerResult::Original(_)));
    }

    #[test]
    fn type_coercion_keeps_regproc_as_a_string_after_the_rewrite() {
        let schema = Arc::new(schema(DataType::Utf8, Some(kind::REGPROC)));
        let planned = assert_planned_sentinel(
            plan(
                RawBinaryExpr {
                    op: BinaryOperator::NotEq,
                    left: column(),
                    right: Expr::Literal(ScalarValue::Int64(Some(0)), None),
                },
                &schema,
            ),
            Operator::NotEq,
            false,
            ScalarValue::Utf8(Some("-".to_string())),
        );
        let simplifier = ExprSimplifier::new(
            SimplifyContext::builder()
                .with_schema(schema.clone())
                .build(),
        );
        let coerced = simplifier.coerce(planned, &schema).unwrap();

        assert!(!format!("{coerced}").contains("CAST"));
        assert!(!format!("{coerced:?}").contains("Int64"));
    }
}
