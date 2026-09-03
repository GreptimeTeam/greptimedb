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
use common_function::scalars::json::json_get::JsonGetWithType;
use common_function::scalars::udf::create_udf;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::{Column, DFSchema, DataFusionError, Result, ScalarValue, TableReference};
use datafusion_expr::expr::{BinaryExpr, ScalarFunction};
use datafusion_expr::planner::{
    ExprPlanner, PlannerResult, RawAggregateExpr, RawBinaryExpr, RawFieldAccessExpr, RawScalarExpr,
    RawWindowExpr,
};
use datafusion_expr::type_coercion::functions::{UDFCoercionExt, fields_with_udf};
use datafusion_expr::{
    Expr, ExprSchemable, GetFieldAccess, Operator, ScalarUDF, WindowFunctionDefinition,
};
use datatypes::extension::json::is_json2_extension_type;
use sqlparser::ast::BinaryOperator;

/// Rewrites JSON-aware SQL expressions into DataFusion expressions.
///
/// This planner handles three cases:
/// - Rewrites compound identifiers on JSON extension columns into `json_get` function.
///   For example, `select a.b.c` => `select json_get(a, "b.c")`.
/// - Extends a JSON path with list indexes and fields following an index.
///   For example, `select a.b[0].c` => `select json_get(a, "b[0][\"c\"]")`.
/// - Pushes an "expected type" argument into the `json_get` function when it participates in a
///   binary operator. So that `json_get` knows the wanted data type when dealing with variant
///   JSON values.
///   For example, `select json_get(a, "b.c") + 1` => `select json_get(a, "b.c", NULL::Int64) + 1`.
/// - Infers the expected type from scalar, aggregate, and window function signatures.
///   For example, `select abs(a.b.c)` => `select abs(json_get(a, "b.c", NULL::Float64))`.
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

        if !is_untyped_json_get(&left) && !is_untyped_json_get(&right) {
            return Ok(PlannerResult::Original(RawBinaryExpr { op, left, right }));
        }

        let Some(expr_op) = parse_sql_op(&op) else {
            return Ok(PlannerResult::Original(RawBinaryExpr { op, left, right }));
        };

        let left_type = left.get_type(schema)?;
        let right_type = right.get_type(schema)?;
        let left_changed = push_json_get_type_arg(&mut left, &right_type)?;
        let right_changed = push_json_get_type_arg(&mut right, &left_type)?;
        if left_changed || right_changed {
            Ok(PlannerResult::Planned(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left),
                expr_op,
                Box::new(right),
            ))))
        } else {
            Ok(PlannerResult::Original(RawBinaryExpr { op, left, right }))
        }
    }

    /// Extends the path of an untyped `json_get` with one field access.
    ///
    /// For `j.o.l[1].inner.l[2]`, `plan_compound_identifier` first produces
    /// `json_get(j, "o.l")`. DataFusion then calls this method successively
    /// with a list index, two named fields, and another list index, producing
    /// the final path `o.l[1]["inner"]["l"][2]`.
    fn plan_field_access(
        &self,
        mut expr: RawFieldAccessExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawFieldAccessExpr>> {
        // See `normalize_field_access_after_subscript` for the reason why we construct the
        // "suffix" like this.
        let suffix = match &expr.field_access {
            GetFieldAccess::ListIndex { key } => {
                // DataFusion parses ordinary integer literals within the i64 range as Int64.
                let Expr::Literal(ScalarValue::Int64(Some(index)), _) = key.as_ref() else {
                    return Ok(PlannerResult::Original(expr));
                };
                format!("[{index}]")
            }
            GetFieldAccess::NamedStructField { name } => {
                let Some(name) = name.try_as_str().flatten() else {
                    return Ok(PlannerResult::Original(expr));
                };
                // Encode the field name as a JSON string before embedding it in the
                // bracket accessor. This preserves dots as literal field-name characters
                // and escapes quotes, backslashes, and control characters correctly.
                let name = serde_json::to_string(name)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                format!("[{name}]")
            }
            GetFieldAccess::ListRange { .. } => return Ok(PlannerResult::Original(expr)),
        };
        let Some(json_get) = extract_untyped_json_get(&mut expr.expr) else {
            return Ok(PlannerResult::Original(expr));
        };
        let Some(Expr::Literal(ScalarValue::Utf8(Some(path)), _)) = json_get.args.get_mut(1) else {
            return Ok(PlannerResult::Original(expr));
        };

        path.push_str(&suffix);
        Ok(PlannerResult::Planned(expr.expr))
    }

    fn plan_compound_identifier(
        &self,
        field: &Field,
        qualifier: Option<&TableReference>,
        nested_names: &[String],
    ) -> Result<PlannerResult<Vec<Expr>>> {
        if !is_json2_extension_type(field) {
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

    /// Rewrites JSON2 arguments without taking over the final function planning.
    ///
    /// `Original` carries the possibly modified raw expression to subsequent planners and then
    /// DataFusion's default function construction. Returning `Planned` would short-circuit both.
    fn plan_scalar(&self, mut expr: RawScalarExpr) -> Result<PlannerResult<RawScalarExpr>> {
        push_function_arg_types(expr.func.as_ref(), &mut expr.args)?;
        Ok(PlannerResult::Original(expr))
    }

    /// Rewrites JSON2 arguments while preserving subsequent aggregate planning.
    fn plan_aggregate(
        &self,
        mut expr: RawAggregateExpr,
    ) -> Result<PlannerResult<RawAggregateExpr>> {
        push_function_arg_types(expr.func.as_ref(), &mut expr.args)?;
        Ok(PlannerResult::Original(expr))
    }

    /// Rewrites JSON2 arguments while preserving subsequent window planning.
    fn plan_window(&self, mut expr: RawWindowExpr) -> Result<PlannerResult<RawWindowExpr>> {
        match &expr.func_def {
            WindowFunctionDefinition::AggregateUDF(func) => {
                push_function_arg_types(func.as_ref(), &mut expr.args)?;
            }
            WindowFunctionDefinition::WindowUDF(func) => {
                push_function_arg_types(func.as_ref(), &mut expr.args)?;
            }
        }
        Ok(PlannerResult::Original(expr))
    }
}

enum JsonGetTypeResolution {
    Fallback,
    Typed(Vec<(usize, DataType)>),
}

/// Infers static output types for untyped `json_get` arguments from a function signature.
///
/// DataFusion requires every expression to have one Arrow data type during planning. A JSON path
/// may contain heterogeneous values across rows, but it cannot expose those values as different
/// Arrow types in one result column. Preserving their runtime types would require a single
/// Variant-like data type and Variant-aware functions instead. Maybe we can wait for
/// https://github.com/apache/datafusion/issues/16116
///
/// This helper uses the function's coercion rules to select a supported output type, then appends
/// a typed NULL argument to each relevant `json_get`. The typed argument makes `json_get` project
/// compatible JSON values to that type and return NULL for incompatible values. Functions that
/// accept json_get's default `Utf8View` output keep the two-argument form so later rewrites can
/// still push down an outer cast.
fn push_function_arg_types<F>(func: &F, args: &mut [Expr]) -> Result<()>
where
    F: UDFCoercionExt,
{
    if !args.iter().any(is_untyped_json_get) {
        return Ok(());
    }

    let fields = args.iter().map(function_arg_field).collect::<Vec<_>>();
    match infer_json_get_types(func, args, &fields) {
        JsonGetTypeResolution::Fallback => {
            let Some(data_type) = fallback_json_get_type(func, args, &fields) else {
                return Ok(());
            };
            for arg in args.iter_mut() {
                if is_untyped_json_get(arg) {
                    let _ = push_json_get_type_arg(arg, &data_type)?;
                }
            }
        }
        JsonGetTypeResolution::Typed(types) => {
            for (index, data_type) in types {
                let _ = push_json_get_type_arg(&mut args[index], &data_type)?;
            }
        }
    }
    Ok(())
}

fn infer_json_get_types<F>(func: &F, args: &[Expr], fields: &[Arc<Field>]) -> JsonGetTypeResolution
where
    F: UDFCoercionExt,
{
    // Only untyped json_get arguments use Null placeholders; preserve every other known argument
    // type. fields_with_udf performs contextual coercion rather than reverse inference from a
    // signature alone. Numeric signatures may preserve all-Null inputs, while Comparable
    // signatures may default them to Utf8. For example, retaining the Float64 peer in
    // coalesce(json_get(...), 1.0) lets DataFusion resolve json_get to Float64 instead of Utf8.
    //
    // This is a best-effort probe: a failure does not mean the actual function call is invalid, so
    // try concrete JSON types before leaving final validation to DataFusion's default planner.
    let Ok(coerced) = fields_with_udf(fields, func) else {
        return JsonGetTypeResolution::Fallback;
    };

    let mut inferred_types = Vec::with_capacity(coerced.len());
    for (index, (arg, field)) in args.iter().zip(coerced).enumerate() {
        if !is_untyped_json_get(arg) || field.data_type().is_null() {
            continue;
        }
        let Some(data_type) = json_get_output_type(field.data_type()) else {
            return JsonGetTypeResolution::Fallback;
        };
        inferred_types.push((index, data_type));
    }
    if inferred_types.is_empty() {
        JsonGetTypeResolution::Fallback
    } else {
        JsonGetTypeResolution::Typed(inferred_types)
    }
}

fn fallback_json_get_type<F>(func: &F, args: &[Expr], fields: &[Arc<Field>]) -> Option<DataType>
where
    F: UDFCoercionExt,
{
    // Prefer json_get's default Utf8View type. If the function rejects strings but accepts numeric
    // values, prefer Float64 so both integers and fractions remain usable.
    let mut candidate_fields = fields.to_vec();
    for data_type in [
        DataType::Utf8View,
        DataType::Float64,
        DataType::Int64,
        DataType::Boolean,
    ] {
        for (index, arg) in args.iter().enumerate() {
            if is_untyped_json_get(arg) {
                candidate_fields[index] = Arc::new(
                    fields[index]
                        .as_ref()
                        .clone()
                        .with_data_type(data_type.clone()),
                );
            }
        }
        if fields_with_udf(&candidate_fields, func).is_ok() {
            return Some(data_type);
        }
    }
    None
}

fn function_arg_field(expr: &Expr) -> Arc<Field> {
    let data_type = if is_untyped_json_get(expr) {
        DataType::Null
    } else if let Some(data_type) = extract_json_get_type(expr) {
        data_type
    } else {
        // Treat unresolved expressions as untyped NULL. This lets signatures such as `power`
        // infer a JSON type, while functions such as `coalesce` can leave it untyped for default
        // planning. This is only best-effort: overloaded or user-defined functions may select a
        // different signature for NULL than for the expression's actual type.
        // TODO(LFC): Use the input schema once DataFusion passes it to ExprPlanner::plan_*().
        expr.get_type(&DFSchema::empty()).unwrap_or(DataType::Null)
    };
    Arc::new(Field::new("", data_type, true))
}

fn json_get_output_type(data_type: &DataType) -> Option<DataType> {
    let output_type = match data_type {
        DataType::Boolean => DataType::Boolean,
        data_type if data_type.is_integer() => DataType::Int64,
        data_type if data_type.is_floating() => DataType::Float64,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => DataType::Float64,
        data_type if data_type.is_string() => DataType::Utf8View,
        _ => return None,
    };
    Some(output_type)
}

macro_rules! is_untyped_json_get_func {
    ($func:expr) => {
        $func
            .func
            .name()
            .eq_ignore_ascii_case(JsonGetWithType::NAME)
            && $func.args.len() == 2
    };
}

macro_rules! is_typed_json_get_func {
    ($func:expr) => {
        $func
            .func
            .name()
            .eq_ignore_ascii_case(JsonGetWithType::NAME)
            && $func.args.len() == 3
    };
}

fn extract_untyped_json_get(expr: &mut Expr) -> Option<&mut ScalarFunction> {
    match expr {
        Expr::ScalarFunction(f) if is_untyped_json_get_func!(f) => Some(f),
        _ => None,
    }
}

fn extract_json_get_type(expr: &Expr) -> Option<DataType> {
    match expr {
        Expr::ScalarFunction(f) if is_typed_json_get_func!(f) => f
            .args
            .get(2)
            .and_then(|x| x.as_literal())
            .map(|x| x.data_type()),
        _ => None,
    }
}

fn is_untyped_json_get(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::ScalarFunction(f) if is_untyped_json_get_func!(f)
    )
}

fn push_json_get_type_arg(expr: &mut Expr, data_type: &DataType) -> Result<bool> {
    let Some(json_get) = extract_untyped_json_get(expr) else {
        return Ok(false);
    };

    // The two-argument form already returns Utf8View. Keep it so JsonGetRewriter can still absorb
    // a cast added by subsequent function coercion.
    if data_type.is_string() {
        return Ok(false);
    }
    let with_type = ScalarValue::try_new_null(data_type).map(|x| Expr::Literal(x, None))?;
    json_get.args.push(with_type);
    Ok(true)
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
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion_expr::WindowFrame;
    use datafusion_functions::core::coalesce;
    use datafusion_functions::math::{abs, power};
    use datatypes::extension::json::Json2ExtensionType;

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
    fn test_plan_list_index() -> Result<()> {
        let planner = JsonExprPlanner;
        let planned = planner.plan_field_access(
            RawFieldAccessExpr {
                field_access: GetFieldAccess::ListIndex {
                    key: Box::new(Expr::Literal(ScalarValue::Int64(Some(0)), None)),
                },
                expr: json_get_expr(Expr::Column(Column::new_unqualified("j")), "list"),
            },
            &DFSchema::empty(),
        )?;
        let PlannerResult::Planned(Expr::ScalarFunction(func)) = planned else {
            unreachable!()
        };
        assert_eq!(func.func.name(), JsonGetWithType::NAME);
        assert_eq!(func.args.len(), 2);
        assert_eq!(
            func.args[1],
            Expr::Literal(ScalarValue::Utf8(Some("list[0]".to_string())), None)
        );
        Ok(())
    }

    #[test]
    fn test_plan_field_after_list_index() -> Result<()> {
        let planner = JsonExprPlanner;
        let planned = planner.plan_field_access(
            RawFieldAccessExpr {
                field_access: GetFieldAccess::NamedStructField {
                    name: ScalarValue::Utf8(Some("a.b".to_string())),
                },
                expr: json_get_expr(Expr::Column(Column::new_unqualified("j")), "list[0]"),
            },
            &DFSchema::empty(),
        )?;
        let PlannerResult::Planned(Expr::ScalarFunction(func)) = planned else {
            unreachable!()
        };
        assert_eq!(
            func.args[1],
            Expr::Literal(
                ScalarValue::Utf8(Some("list[0][\"a.b\"]".to_string())),
                None
            )
        );
        Ok(())
    }

    #[test]
    fn test_plan_compound_identifier() -> Result<()> {
        let planner = JsonExprPlanner;
        let qualifier = TableReference::bare("events");
        let nested_names = vec!["payload".to_string(), "cpu".to_string()];

        let planned = planner.plan_compound_identifier(
            &Field::new("labels", DataType::Struct(Fields::empty()), true)
                .with_extension_type(Json2ExtensionType::default()),
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

    #[test]
    fn test_plan_functions() -> Result<()> {
        let planner = JsonExprPlanner;
        let json_get = || json_get_expr(Expr::Column(Column::new_unqualified("j")), "a.b");

        let PlannerResult::Original(scalar) = planner.plan_scalar(RawScalarExpr {
            func: abs(),
            args: vec![json_get()],
        })?
        else {
            unreachable!();
        };
        assert_eq!(
            Some(DataType::Float64),
            extract_json_get_type(&scalar.args[0])
        );

        let PlannerResult::Original(scalar) = planner.plan_scalar(RawScalarExpr {
            func: power(),
            args: vec![
                json_get(),
                Expr::Column(Column::new_unqualified("exponent")),
            ],
        })?
        else {
            unreachable!();
        };
        assert_eq!(
            Some(DataType::Float64),
            extract_json_get_type(&scalar.args[0])
        );

        let PlannerResult::Original(aggregate) = planner.plan_aggregate(RawAggregateExpr {
            func: sum_udaf(),
            args: vec![json_get()],
            distinct: false,
            filter: None,
            order_by: vec![],
            null_treatment: None,
        })?
        else {
            unreachable!();
        };
        assert_eq!(
            Some(DataType::Float64),
            extract_json_get_type(&aggregate.args[0])
        );

        let PlannerResult::Original(count) = planner.plan_aggregate(RawAggregateExpr {
            func: count_udaf(),
            args: vec![json_get()],
            distinct: false,
            filter: None,
            order_by: vec![],
            null_treatment: None,
        })?
        else {
            unreachable!();
        };
        assert_eq!(None, extract_json_get_type(&count.args[0]));

        let PlannerResult::Original(window) = planner.plan_window(RawWindowExpr {
            func_def: WindowFunctionDefinition::AggregateUDF(sum_udaf()),
            args: vec![json_get()],
            partition_by: vec![],
            order_by: vec![],
            window_frame: WindowFrame::new(None),
            filter: None,
            null_treatment: None,
            distinct: false,
        })?
        else {
            unreachable!();
        };
        assert_eq!(
            Some(DataType::Float64),
            extract_json_get_type(&window.args[0])
        );
        Ok(())
    }

    #[test]
    fn test_plan_function_with_mixed_json_get_types() -> Result<()> {
        let planner = JsonExprPlanner;
        let json_get = || json_get_expr(Expr::Column(Column::new_unqualified("j")), "a.b");
        let mut typed = json_get();
        push_json_get_type_arg(&mut typed, &DataType::Float64)?;

        let PlannerResult::Original(scalar) = planner.plan_scalar(RawScalarExpr {
            func: coalesce(),
            args: vec![json_get(), typed],
        })?
        else {
            unreachable!();
        };
        assert_eq!(
            Some(DataType::Float64),
            extract_json_get_type(&scalar.args[0])
        );
        assert_eq!(
            Some(DataType::Float64),
            extract_json_get_type(&scalar.args[1])
        );
        Ok(())
    }
}
