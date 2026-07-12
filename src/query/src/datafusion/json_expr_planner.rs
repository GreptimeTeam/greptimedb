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

use std::sync::{Arc, LazyLock};

use arrow_schema::Field;
use arrow_schema::extension::ExtensionType;
use common_function::scalars::json::json_get::JsonGetWithType;
use common_function::scalars::udf::create_udf;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::{Column, DFSchema, Result, ScalarValue, TableReference};
use datafusion_expr::expr::{BinaryExpr, ScalarFunction};
use datafusion_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion_expr::{Expr, ExprSchemable, Operator, ScalarUDF};
use datatypes::extension::json::JsonExtensionType;
use either::Either;
use sqlparser::ast::BinaryOperator;

/// Rewrites JSON-aware SQL expressions into DataFusion expressions.
///
/// This planner handles two cases:
/// - Rewrites compound identifiers on JSON extension columns into `json_get` function.
///   For example, `select a.b.c` => `select json_get(a, "b.c")`.
/// - Pushes an "expected type" argument into the `json_get` function when it participates in a
///   binary operator. So that `json_get` knows the wanted data type when dealing with variant
///   JSON values.
///   For example, `select json_get(a, "b.c") + 1` => `select json_get(a, "b.c", NULL::Int64) + 1`.
#[derive(Debug)]
pub(crate) struct JsonExprPlanner;

impl ExprPlanner for JsonExprPlanner {
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

        if extract_untyped_json_get(&mut left).is_none()
            && extract_untyped_json_get(&mut right).is_none()
        {
            return Ok(PlannerResult::Original(RawBinaryExpr { op, left, right }));
        }

        let Some(expr_op) = parse_sql_op(&op) else {
            return Ok(PlannerResult::Original(RawBinaryExpr { op, left, right }));
        };

        let left_type = left.get_type(schema)?;
        let right_type = right.get_type(schema)?;
        let left = push_json_get_type_arg(left, right_type)?;
        let right = push_json_get_type_arg(right, left_type)?;
        match (left, right) {
            (Either::Left(left), Either::Left(right)) => {
                Ok(PlannerResult::Original(RawBinaryExpr { op, left, right }))
            }
            (left, right) => Ok(PlannerResult::Planned(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left.into_inner()),
                expr_op,
                Box::new(right.into_inner()),
            )))),
        }
    }

    fn plan_compound_identifier(
        &self,
        field: &Field,
        qualifier: Option<&TableReference>,
        nested_names: &[String],
    ) -> Result<PlannerResult<Vec<Expr>>> {
        if field.extension_type_name() != Some(JsonExtensionType::NAME) {
            return Ok(PlannerResult::Original(Vec::new()));
        }

        static JSON_GET_UDF: LazyLock<Arc<ScalarUDF>> =
            LazyLock::new(|| Arc::new(create_udf(Arc::new(JsonGetWithType::default()))));

        let json_get = JSON_GET_UDF.clone();
        let path = nested_names.join(".");
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(
                json_get,
                vec![
                    Expr::Column(Column::from((qualifier, field))),
                    Expr::Literal(ScalarValue::Utf8(Some(path)), None),
                ],
            ),
        )))
    }
}

fn extract_untyped_json_get(expr: &mut Expr) -> Option<&mut ScalarFunction> {
    match expr {
        Expr::ScalarFunction(f)
            if f.func.name().eq_ignore_ascii_case(JsonGetWithType::NAME) && f.args.len() == 2 =>
        {
            Some(f)
        }
        _ => None,
    }
}

fn push_json_get_type_arg(mut expr: Expr, mut data_type: DataType) -> Result<Either<Expr, Expr>> {
    let Some(json_get) = extract_untyped_json_get(&mut expr) else {
        return Ok(Either::Left(expr));
    };

    if data_type.is_string() {
        data_type = DataType::Utf8View;
    }
    let with_type = ScalarValue::try_new_null(&data_type).map(|x| Expr::Literal(x, None))?;
    json_get.args.push(with_type);

    Ok(Either::Right(expr))
}

fn parse_sql_op(op: &BinaryOperator) -> Option<Operator> {
    match *op {
        BinaryOperator::Plus => Some(Operator::Plus),
        BinaryOperator::Minus => Some(Operator::Minus),
        BinaryOperator::Multiply => Some(Operator::Multiply),
        BinaryOperator::Divide => Some(Operator::Divide),
        BinaryOperator::Modulo => Some(Operator::Modulo),
        BinaryOperator::Gt => Some(Operator::Gt),
        BinaryOperator::GtEq => Some(Operator::GtEq),
        BinaryOperator::Lt => Some(Operator::Lt),
        BinaryOperator::LtEq => Some(Operator::LtEq),
        BinaryOperator::Eq => Some(Operator::Eq),
        BinaryOperator::NotEq => Some(Operator::NotEq),
        BinaryOperator::And => Some(Operator::And),
        BinaryOperator::Or => Some(Operator::Or),
        BinaryOperator::BitwiseAnd => Some(Operator::BitwiseAnd),
        BinaryOperator::BitwiseOr => Some(Operator::BitwiseOr),
        BinaryOperator::BitwiseXor => Some(Operator::BitwiseXor),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::Fields;
    use datatypes::extension::json::JsonMetadata;

    use super::*;

    fn json_get_expr(base: Expr, path: &str) -> Expr {
        let json_get = Arc::new(create_udf(Arc::new(JsonGetWithType::default())));
        Expr::ScalarFunction(ScalarFunction::new_udf(
            json_get,
            vec![
                base,
                Expr::Literal(ScalarValue::Utf8(Some(path.to_string())), None),
            ],
        ))
    }

    fn json_field(name: &str) -> Field {
        Field::new(name, DataType::Binary, true)
            .with_extension_type(JsonExtensionType::new(Arc::new(JsonMetadata::default())))
    }

    #[test]
    fn test_plan_binary_op() -> Result<()> {
        let planner = JsonExprPlanner;
        let schema = DFSchema::from_unqualified_fields(
            Fields::from(vec![Field::new("value", DataType::Int64, true)]),
            Default::default(),
        )?;

        let planned = planner.plan_binary_op(
            RawBinaryExpr {
                op: BinaryOperator::Eq,
                left: json_get_expr(
                    Expr::Literal(ScalarValue::Binary(Some(b"{\"a\": 1}".to_vec())), None),
                    "a",
                ),
                right: Expr::Column(Column::new_unqualified("value")),
            },
            &schema,
        )?;

        match planned {
            PlannerResult::Planned(Expr::BinaryExpr(expr)) => {
                assert_eq!(expr.op, Operator::Eq);

                match expr.left.as_ref() {
                    Expr::ScalarFunction(func) => {
                        assert_eq!(func.func.name(), JsonGetWithType::NAME);
                        assert_eq!(func.args.len(), 3);
                        assert_eq!(func.args[2], Expr::Literal(ScalarValue::Int64(None), None));
                    }
                    other => panic!("expected json_get on left side, got {other:?}"),
                }

                assert_eq!(
                    expr.right.as_ref(),
                    &Expr::Column(Column::new_unqualified("value"))
                );
            }
            other => panic!("expected planned binary expression, got {other:?}"),
        }

        let original = planner.plan_binary_op(
            RawBinaryExpr {
                op: BinaryOperator::StringConcat,
                left: Expr::Column(Column::new_unqualified("value")),
                right: Expr::Literal(ScalarValue::Utf8(Some("x".to_string())), None),
            },
            &schema,
        )?;

        match original {
            PlannerResult::Original(expr) => {
                assert!(matches!(expr.op, BinaryOperator::StringConcat));
                assert_eq!(expr.left, Expr::Column(Column::new_unqualified("value")));
                assert_eq!(
                    expr.right,
                    Expr::Literal(ScalarValue::Utf8(Some("x".to_string())), None)
                );
            }
            other => panic!(
                "expected original expression for unsupported operator, got {:?}",
                other,
            ),
        }

        Ok(())
    }

    #[test]
    fn test_plan_compound_identifier() -> Result<()> {
        let planner = JsonExprPlanner;
        let qualifier = TableReference::bare("events");
        let nested_names = vec!["payload".to_string(), "cpu".to_string()];

        let planned = planner.plan_compound_identifier(
            &json_field("labels"),
            Some(&qualifier),
            &nested_names,
        )?;

        match planned {
            PlannerResult::Planned(Expr::ScalarFunction(func)) => {
                assert_eq!(func.func.name(), JsonGetWithType::NAME);
                assert_eq!(func.args.len(), 2);
                assert_eq!(
                    func.args[0],
                    Expr::Column(Column::new(Some(qualifier.clone()), "labels"))
                );
                assert_eq!(
                    func.args[1],
                    Expr::Literal(ScalarValue::Utf8(Some("payload.cpu".to_string())), None)
                );
            }
            other => panic!("expected json_get scalar function, got {other:?}"),
        }

        let original = planner.plan_compound_identifier(
            &Field::new("plain", DataType::Utf8, true),
            Some(&qualifier),
            &nested_names,
        )?;

        match original {
            PlannerResult::Original(exprs) => assert!(exprs.is_empty()),
            other => panic!(
                "expected original empty result for non-json field, got {:?}",
                other,
            ),
        }

        Ok(())
    }
}
