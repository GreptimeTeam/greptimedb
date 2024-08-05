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

#![warn(unused_imports)]

use std::sync::Arc;

use common_error::ext::BoxedError;
use common_telemetry::debug;
use datafusion_physical_expr::PhysicalExpr;
use datatypes::data_type::ConcreteDataType as CDT;
use snafu::{OptionExt, ResultExt};
use substrait_proto::proto::expression::field_reference::ReferenceType::DirectReference;
use substrait_proto::proto::expression::reference_segment::ReferenceType::StructField;
use substrait_proto::proto::expression::{IfThen, RexType, ScalarFunction};
use substrait_proto::proto::function_argument::ArgType;
use substrait_proto::proto::Expression;

use crate::error::{
    DatafusionSnafu, DatatypesSnafu, Error, EvalSnafu, ExternalSnafu, InvalidQuerySnafu,
    NotImplementedSnafu, PlanSnafu, UnexpectedSnafu,
};
use crate::expr::{
    BinaryFunc, DfScalarFunction, RawDfScalarFn, ScalarExpr, TypedExpr, UnaryFunc,
    UnmaterializableFunc, VariadicFunc,
};
use crate::repr::{ColumnType, RelationDesc, RelationType};
use crate::transform::literal::{
    from_substrait_literal, from_substrait_type, to_substrait_literal,
};
use crate::transform::{substrait_proto, FunctionExtensions};

// TODO(discord9): refactor plan to substrait convert of `arrow_cast` function thus remove this function
/// ref to `arrow_schema::datatype` for type name
fn typename_to_cdt(name: &str) -> Result<CDT, Error> {
    let ret = match name {
        "Int8" => CDT::int8_datatype(),
        "Int16" => CDT::int16_datatype(),
        "Int32" => CDT::int32_datatype(),
        "Int64" => CDT::int64_datatype(),
        "UInt8" => CDT::uint8_datatype(),
        "UInt16" => CDT::uint16_datatype(),
        "UInt32" => CDT::uint32_datatype(),
        "UInt64" => CDT::uint64_datatype(),
        "Float32" => CDT::float32_datatype(),
        "Float64" => CDT::float64_datatype(),
        "Boolean" => CDT::boolean_datatype(),
        "String" => CDT::string_datatype(),
        "Date" | "Date32" | "Date64" => CDT::date_datatype(),
        "Timestamp" => CDT::timestamp_second_datatype(),
        "Timestamp(Second, None)" => CDT::timestamp_second_datatype(),
        "Timestamp(Millisecond, None)" => CDT::timestamp_millisecond_datatype(),
        "Timestamp(Microsecond, None)" => CDT::timestamp_microsecond_datatype(),
        "Timestamp(Nanosecond, None)" => CDT::timestamp_nanosecond_datatype(),
        "Time32(Second)" | "Time64(Second)" => CDT::time_second_datatype(),
        "Time32(Millisecond)" | "Time64(Millisecond)" => CDT::time_millisecond_datatype(),
        "Time32(Microsecond)" | "Time64(Microsecond)" => CDT::time_microsecond_datatype(),
        "Time32(Nanosecond)" | "Time64(Nanosecond)" => CDT::time_nanosecond_datatype(),
        _ => NotImplementedSnafu {
            reason: format!("Unrecognized typename: {}", name),
        }
        .fail()?,
    };
    Ok(ret)
}

/// Convert [`ScalarFunction`] to corresponding Datafusion's [`PhysicalExpr`]
pub(crate) async fn from_scalar_fn_to_df_fn_impl(
    f: &ScalarFunction,
    input_schema: &RelationDesc,
    extensions: &FunctionExtensions,
) -> Result<Arc<dyn PhysicalExpr>, Error> {
    let e = Expression {
        rex_type: Some(RexType::ScalarFunction(f.clone())),
    };
    let schema = input_schema.to_df_schema()?;

    let df_expr =
        // TODO(discord9): consider coloring everything async....
        substrait::df_logical_plan::consumer::from_substrait_rex(
            &datafusion::prelude::SessionContext::new(),
            &e,
            &schema,
            &extensions.inner_ref(),
        )
        .await
    ;
    let expr = df_expr.context({
        DatafusionSnafu {
            context: "Failed to convert substrait scalar function to datafusion scalar function",
        }
    })?;
    let phy_expr =
        datafusion::physical_expr::create_physical_expr(&expr, &schema, &Default::default())
            .context(DatafusionSnafu {
                context: "Failed to create physical expression from logical expression",
            })?;
    Ok(phy_expr)
}

/// Return an [`Expression`](wrapped in a [`FunctionArgument`]) that references the i-th column of the input relation
pub(crate) fn proto_col(i: usize) -> substrait_proto::proto::FunctionArgument {
    use substrait_proto::proto::expression;
    let expr = Expression {
        rex_type: Some(expression::RexType::Selection(Box::new(
            expression::FieldReference {
                reference_type: Some(expression::field_reference::ReferenceType::DirectReference(
                    expression::ReferenceSegment {
                        reference_type: Some(
                            expression::reference_segment::ReferenceType::StructField(Box::new(
                                expression::reference_segment::StructField {
                                    field: i as i32,
                                    child: None,
                                },
                            )),
                        ),
                    },
                )),
                root_type: None,
            },
        ))),
    };
    substrait_proto::proto::FunctionArgument {
        arg_type: Some(substrait_proto::proto::function_argument::ArgType::Value(
            expr,
        )),
    }
}

fn is_proto_literal(arg: &substrait_proto::proto::FunctionArgument) -> bool {
    use substrait_proto::proto::expression;
    matches!(
        arg.arg_type.as_ref().unwrap(),
        ArgType::Value(Expression {
            rex_type: Some(expression::RexType::Literal(_)),
        })
    )
}

fn build_proto_lit(
    lit: substrait_proto::proto::expression::Literal,
) -> substrait_proto::proto::FunctionArgument {
    use substrait_proto::proto;
    proto::FunctionArgument {
        arg_type: Some(ArgType::Value(Expression {
            rex_type: Some(proto::expression::RexType::Literal(lit)),
        })),
    }
}

/// rewrite ScalarFunction's arguments to Columns 0..n so nested exprs are still handled by us instead of datafusion
///
/// specially, if a argument is a literal, the replacement will not happen
fn rewrite_scalar_function(
    f: &ScalarFunction,
    arg_typed_exprs: &[TypedExpr],
) -> Result<ScalarFunction, Error> {
    let mut f_rewrite = f.clone();
    for (idx, raw_expr) in f_rewrite.arguments.iter_mut().enumerate() {
        // only replace it with col(idx) if it is not literal
        // will try best to determine if it is literal, i.e. for function like `cast(<literal>)` will try
        // in both world to understand if it results in a literal
        match (
            is_proto_literal(raw_expr),
            arg_typed_exprs[idx].expr.is_literal(),
        ) {
            (false, false) => *raw_expr = proto_col(idx),
            (true, _) => (),
            (false, true) => {
                if let ScalarExpr::Literal(val, ty) = &arg_typed_exprs[idx].expr {
                    let df_val = val
                        .try_to_scalar_value(ty)
                        .map_err(BoxedError::new)
                        .context(ExternalSnafu)?;
                    let lit_sub = to_substrait_literal(&df_val)?;
                    // put const-folded literal back to df to simplify stuff
                    *raw_expr = build_proto_lit(lit_sub);
                } else {
                    UnexpectedSnafu {
                        reason: format!(
                            "Expect value to be literal, but found {:?}",
                            arg_typed_exprs[idx].expr
                        ),
                    }
                    .fail()?
                }
            }
        }
    }
    Ok(f_rewrite)
}

impl TypedExpr {
    pub async fn from_substrait_to_datafusion_scalar_func(
        f: &ScalarFunction,
        arg_typed_exprs: Vec<TypedExpr>,
        extensions: &FunctionExtensions,
    ) -> Result<TypedExpr, Error> {
        let (arg_exprs, arg_types): (Vec<_>, Vec<_>) = arg_typed_exprs
            .clone()
            .into_iter()
            .map(|e| (e.expr, e.typ))
            .unzip();
        debug!("Before rewrite: {:?}", f);
        let f_rewrite = rewrite_scalar_function(f, &arg_typed_exprs)?;
        debug!("After rewrite: {:?}", f_rewrite);
        let input_schema = RelationType::new(arg_types).into_unnamed();
        let raw_fn =
            RawDfScalarFn::from_proto(&f_rewrite, input_schema.clone(), extensions.clone())?;

        let df_func = DfScalarFunction::try_from_raw_fn(raw_fn).await?;
        let expr = ScalarExpr::CallDf {
            df_scalar_fn: df_func,
            exprs: arg_exprs,
        };
        // df already know it's own schema, so not providing here
        let ret_type = expr.typ(&[])?;
        Ok(TypedExpr::new(expr, ret_type))
    }

    /// Convert ScalarFunction into Flow's ScalarExpr
    pub async fn from_substrait_scalar_func(
        f: &ScalarFunction,
        input_schema: &RelationDesc,
        extensions: &FunctionExtensions,
    ) -> Result<TypedExpr, Error> {
        let fn_name =
            extensions
                .get(&f.function_reference)
                .with_context(|| NotImplementedSnafu {
                    reason: format!(
                        "Aggregated function not found: function reference = {:?}",
                        f.function_reference
                    ),
                })?;
        let arg_len = f.arguments.len();
        let arg_typed_exprs: Vec<TypedExpr> = {
            let mut rets = Vec::new();
            for arg in f.arguments.iter() {
                let ret = match &arg.arg_type {
                    Some(ArgType::Value(e)) => {
                        TypedExpr::from_substrait_rex(e, input_schema, extensions).await
                    }
                    _ => not_impl_err!("Aggregated function argument non-Value type not supported"),
                }?;
                rets.push(ret);
            }
            rets
        };

        // literal's type is determined by the function and type of other args
        let (arg_exprs, arg_types): (Vec<_>, Vec<_>) = arg_typed_exprs
            .clone()
            .into_iter()
            .map(
                |TypedExpr {
                     expr: arg_val,
                     typ: arg_type,
                 }| {
                    if arg_val.is_literal() {
                        (arg_val, None)
                    } else {
                        (arg_val, Some(arg_type.scalar_type))
                    }
                },
            )
            .unzip();

        match arg_len {
            1 if UnaryFunc::is_valid_func_name(fn_name) => {
                let func = UnaryFunc::from_str_and_type(fn_name, None)?;
                let arg = arg_exprs[0].clone();
                let ret_type = ColumnType::new_nullable(func.signature().output.clone());

                Ok(TypedExpr::new(arg.call_unary(func), ret_type))
            }
            2 if fn_name == "arrow_cast" => {
                let cast_to = arg_exprs[1]
                    .clone()
                    .as_literal()
                    .and_then(|lit| lit.as_string())
                    .with_context(|| InvalidQuerySnafu {
                        reason: "array_cast's second argument must be a literal string",
                    })?;
                let cast_to = typename_to_cdt(&cast_to)?;
                let func = UnaryFunc::Cast(cast_to.clone());
                let arg = arg_exprs[0].clone();
                // constant folding here since some datafusion function require it for constant arg(i.e. `DATE_BIN`)
                if arg.is_literal() {
                    let res = func.eval(&[], &arg).context(EvalSnafu)?;
                    Ok(TypedExpr::new(
                        ScalarExpr::Literal(res, cast_to.clone()),
                        ColumnType::new_nullable(cast_to),
                    ))
                } else {
                    let ret_type = ColumnType::new_nullable(func.signature().output.clone());

                    Ok(TypedExpr::new(arg.call_unary(func), ret_type))
                }
            }
            2 if BinaryFunc::is_valid_func_name(fn_name) => {
                let (func, signature) =
                    BinaryFunc::from_str_expr_and_type(fn_name, &arg_exprs, &arg_types[0..2])?;

                // constant folding here
                let is_all_literal = arg_exprs.iter().all(|arg| arg.is_literal());
                if is_all_literal {
                    let res = func
                        .eval(&[], &arg_exprs[0], &arg_exprs[1])
                        .context(EvalSnafu)?;

                    // if output type is null, it should be inferred from the input types
                    let con_typ = signature.output.clone();
                    let typ = ColumnType::new_nullable(con_typ.clone());
                    return Ok(TypedExpr::new(ScalarExpr::Literal(res, con_typ), typ));
                }

                let mut arg_exprs = arg_exprs;
                for (idx, arg_expr) in arg_exprs.iter_mut().enumerate() {
                    if let ScalarExpr::Literal(val, typ) = arg_expr {
                        let dest_type = signature.input[idx].clone();

                        // cast val to target_type
                        let dest_val = if !dest_type.is_null() {
                            datatypes::types::cast(val.clone(), &dest_type)
                        .with_context(|_|
                            DatatypesSnafu{
                                extra: format!("Failed to implicitly cast literal {val:?} to type {dest_type:?}")
                            })?
                        } else {
                            val.clone()
                        };
                        *val = dest_val;
                        *typ = dest_type;
                    }
                }

                let ret_type = ColumnType::new_nullable(func.signature().output.clone());
                let ret_expr = arg_exprs[0].clone().call_binary(arg_exprs[1].clone(), func);
                Ok(TypedExpr::new(ret_expr, ret_type))
            }
            _var => {
                if VariadicFunc::is_valid_func_name(fn_name) {
                    let func = VariadicFunc::from_str_and_types(fn_name, &arg_types)?;
                    let ret_type = ColumnType::new_nullable(func.signature().output.clone());
                    let mut expr = ScalarExpr::CallVariadic {
                        func,
                        exprs: arg_exprs,
                    };
                    expr.optimize();
                    Ok(TypedExpr::new(expr, ret_type))
                } else if UnmaterializableFunc::is_valid_func_name(fn_name) {
                    let func = UnmaterializableFunc::from_str_args(fn_name, arg_typed_exprs)?;
                    let ret_type = ColumnType::new_nullable(func.signature().output.clone());
                    Ok(TypedExpr::new(
                        ScalarExpr::CallUnmaterializable(func),
                        ret_type,
                    ))
                } else {
                    let try_as_df = Self::from_substrait_to_datafusion_scalar_func(
                        f,
                        arg_typed_exprs,
                        extensions,
                    )
                    .await?;
                    Ok(try_as_df)
                }
            }
        }
    }

    /// Convert IfThen into Flow's ScalarExpr
    pub async fn from_substrait_ifthen_rex(
        if_then: &IfThen,
        input_schema: &RelationDesc,
        extensions: &FunctionExtensions,
    ) -> Result<TypedExpr, Error> {
        let ifs: Vec<_> = {
            let mut ifs = Vec::new();
            for if_clause in if_then.ifs.iter() {
                let proto_if = if_clause.r#if.as_ref().with_context(|| InvalidQuerySnafu {
                    reason: "IfThen clause without if",
                })?;
                let proto_then = if_clause.then.as_ref().with_context(|| InvalidQuerySnafu {
                    reason: "IfThen clause without then",
                })?;
                let cond =
                    TypedExpr::from_substrait_rex(proto_if, input_schema, extensions).await?;
                let then =
                    TypedExpr::from_substrait_rex(proto_then, input_schema, extensions).await?;
                ifs.push((cond, then));
            }
            ifs
        };
        // if no else is presented
        let els = match if_then
            .r#else
            .as_ref()
            .map(|e| TypedExpr::from_substrait_rex(e, input_schema, extensions))
        {
            Some(fut) => Some(fut.await),
            None => None,
        }
        .transpose()?
        .unwrap_or_else(|| {
            TypedExpr::new(
                ScalarExpr::literal_null(),
                ColumnType::new_nullable(CDT::null_datatype()),
            )
        });

        fn build_if_then_recur(
            mut next_if_then: impl Iterator<Item = (TypedExpr, TypedExpr)>,
            els: TypedExpr,
        ) -> TypedExpr {
            if let Some((cond, then)) = next_if_then.next() {
                // always assume the type of `if`` expr is the same with the `then`` expr
                TypedExpr::new(
                    ScalarExpr::If {
                        cond: Box::new(cond.expr),
                        then: Box::new(then.expr),
                        els: Box::new(build_if_then_recur(next_if_then, els).expr),
                    },
                    then.typ,
                )
            } else {
                els
            }
        }
        let expr_if = build_if_then_recur(ifs.into_iter(), els);
        Ok(expr_if)
    }
    /// Convert Substrait Rex into Flow's ScalarExpr
    #[async_recursion::async_recursion]
    pub async fn from_substrait_rex(
        e: &Expression,
        input_schema: &RelationDesc,
        extensions: &FunctionExtensions,
    ) -> Result<TypedExpr, Error> {
        match &e.rex_type {
            Some(RexType::Literal(lit)) => {
                let lit = from_substrait_literal(lit)?;
                Ok(TypedExpr::new(
                    ScalarExpr::Literal(lit.0, lit.1.clone()),
                    ColumnType::new_nullable(lit.1),
                ))
            }
            Some(RexType::SingularOrList(s)) => {
                let substrait_expr = s.value.as_ref().with_context(|| InvalidQuerySnafu {
                    reason: "SingularOrList expression without value",
                })?;
                // Note that we didn't impl support to in list expr
                if !s.options.is_empty() {
                    return not_impl_err!("In list expression is not supported");
                }
                TypedExpr::from_substrait_rex(substrait_expr, input_schema, extensions).await
            }
            Some(RexType::Selection(field_ref)) => match &field_ref.reference_type {
                Some(DirectReference(direct)) => match &direct.reference_type.as_ref() {
                    Some(StructField(x)) => match &x.child.as_ref() {
                        Some(_) => {
                            not_impl_err!(
                                "Direct reference StructField with child is not supported"
                            )
                        }
                        None => {
                            let column = x.field as usize;
                            let column_type = input_schema.typ().column_types[column].clone();
                            Ok(TypedExpr::new(ScalarExpr::Column(column), column_type))
                        }
                    },
                    _ => not_impl_err!(
                        "Direct reference with types other than StructField is not supported"
                    ),
                },
                _ => not_impl_err!("unsupported field ref type"),
            },
            Some(RexType::ScalarFunction(f)) => {
                TypedExpr::from_substrait_scalar_func(f, input_schema, extensions).await
            }
            Some(RexType::IfThen(if_then)) => {
                TypedExpr::from_substrait_ifthen_rex(if_then, input_schema, extensions).await
            }
            Some(RexType::Cast(cast)) => {
                let input = cast.input.as_ref().with_context(|| InvalidQuerySnafu {
                    reason: "Cast expression without input",
                })?;
                let input = TypedExpr::from_substrait_rex(input, input_schema, extensions).await?;
                let cast_type = from_substrait_type(cast.r#type.as_ref().with_context(|| {
                    InvalidQuerySnafu {
                        reason: "Cast expression without type",
                    }
                })?)?;
                let func = UnaryFunc::from_str_and_type("cast", Some(cast_type.clone()))?;
                Ok(TypedExpr::new(
                    input.expr.call_unary(func),
                    ColumnType::new_nullable(cast_type),
                ))
            }
            Some(RexType::WindowFunction(_)) => PlanSnafu {
                reason:
                    "Window function is not supported yet. Please use aggregation function instead."
                        .to_string(),
            }
            .fail(),
            _ => not_impl_err!("unsupported rex_type"),
        }
    }
}

#[cfg(test)]
mod test {
    use common_time::{DateTime, Interval};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::value::Value;
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::expr::{GlobalId, MapFilterProject};
    use crate::plan::{Plan, TypedPlan};
    use crate::repr::{self, ColumnType, RelationType};
    use crate::transform::test::{create_test_ctx, create_test_query_engine, sql_to_substrait};

    /// test if `WHERE` condition can be converted to Flow's ScalarExpr in mfp's filter
    #[tokio::test]
    async fn test_where_and() {
        let engine = create_test_query_engine();
        let sql = "SELECT number FROM numbers WHERE number >= 1 AND number <= 3 AND number!=2";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan).await;

        // optimize binary and to variadic and
        let filter = ScalarExpr::CallVariadic {
            func: VariadicFunc::And,
            exprs: vec![
                ScalarExpr::Column(0).call_binary(
                    ScalarExpr::Literal(Value::from(1u32), CDT::uint32_datatype()),
                    BinaryFunc::Gte,
                ),
                ScalarExpr::Column(0).call_binary(
                    ScalarExpr::Literal(Value::from(3u32), CDT::uint32_datatype()),
                    BinaryFunc::Lte,
                ),
                ScalarExpr::Column(0).call_binary(
                    ScalarExpr::Literal(Value::from(2u32), CDT::uint32_datatype()),
                    BinaryFunc::NotEq,
                ),
            ],
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![ColumnType::new(CDT::uint32_datatype(), false)])
                .into_named(vec![Some("number".to_string())]),
            plan: Plan::Mfp {
                input: Box::new(
                    Plan::Get {
                        id: crate::expr::Id::Global(GlobalId::User(0)),
                    }
                    .with_types(
                        RelationType::new(vec![ColumnType::new(
                            ConcreteDataType::uint32_datatype(),
                            false,
                        )])
                        .into_named(vec![Some("number".to_string())]),
                    ),
                ),
                mfp: MapFilterProject::new(1)
                    .map(vec![ScalarExpr::Column(0)])
                    .unwrap()
                    .filter(vec![filter])
                    .unwrap()
                    .project(vec![1])
                    .unwrap(),
            },
        };
        assert_eq!(flow_plan.unwrap(), expected);
    }

    /// case: binary functions&constant folding can happen in converting substrait plan
    #[tokio::test]
    async fn test_binary_func_and_constant_folding() {
        let engine = create_test_query_engine();
        let sql = "SELECT 1+1*2-1/1+1%2==3 FROM numbers";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan).await;

        let expected = TypedPlan {
            schema: RelationType::new(vec![ColumnType::new(CDT::boolean_datatype(), true)])
                .into_unnamed(),
            plan: Plan::Constant {
                rows: vec![(
                    repr::Row::new(vec![Value::from(true)]),
                    repr::Timestamp::MIN,
                    1,
                )],
            },
        };

        assert_eq!(flow_plan.unwrap(), expected);
    }

    /// test if the type of the literal is correctly inferred, i.e. in here literal is decoded to be int64, but need to be uint32,
    #[tokio::test]
    async fn test_implicitly_cast() {
        let engine = create_test_query_engine();
        let sql = "SELECT number+1 FROM numbers";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan).await;

        let expected = TypedPlan {
            schema: RelationType::new(vec![ColumnType::new(CDT::uint32_datatype(), true)])
                .into_unnamed(),
            plan: Plan::Mfp {
                input: Box::new(
                    Plan::Get {
                        id: crate::expr::Id::Global(GlobalId::User(0)),
                    }
                    .with_types(
                        RelationType::new(vec![ColumnType::new(
                            ConcreteDataType::uint32_datatype(),
                            false,
                        )])
                        .into_named(vec![Some("number".to_string())]),
                    ),
                ),
                mfp: MapFilterProject::new(1)
                    .map(vec![ScalarExpr::Column(0).call_binary(
                        ScalarExpr::Literal(Value::from(1u32), CDT::uint32_datatype()),
                        BinaryFunc::AddUInt32,
                    )])
                    .unwrap()
                    .project(vec![1])
                    .unwrap(),
            },
        };
        assert_eq!(flow_plan.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_cast() {
        let engine = create_test_query_engine();
        let sql = "SELECT CAST(1 AS INT16) FROM numbers";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan).await;

        let expected = TypedPlan {
            schema: RelationType::new(vec![ColumnType::new(CDT::int16_datatype(), true)])
                .into_unnamed(),
            plan: Plan::Constant {
                // cast of literal is constant folded
                rows: vec![(repr::Row::new(vec![Value::from(1i16)]), i64::MIN, 1)],
            },
        };
        assert_eq!(flow_plan.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_select_add() {
        let engine = create_test_query_engine();
        let sql = "SELECT number+number FROM numbers";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan).await;

        let expected = TypedPlan {
            schema: RelationType::new(vec![ColumnType::new(CDT::uint32_datatype(), true)])
                .into_unnamed(),
            plan: Plan::Mfp {
                input: Box::new(
                    Plan::Get {
                        id: crate::expr::Id::Global(GlobalId::User(0)),
                    }
                    .with_types(
                        RelationType::new(vec![ColumnType::new(
                            ConcreteDataType::uint32_datatype(),
                            false,
                        )])
                        .into_named(vec![Some("number".to_string())]),
                    ),
                ),
                mfp: MapFilterProject::new(1)
                    .map(vec![ScalarExpr::Column(0)
                        .call_binary(ScalarExpr::Column(0), BinaryFunc::AddUInt32)])
                    .unwrap()
                    .project(vec![1])
                    .unwrap(),
            },
        };

        assert_eq!(flow_plan.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_func_sig() {
        fn lit(v: impl ToString) -> substrait_proto::proto::FunctionArgument {
            use substrait_proto::proto::expression;
            let expr = Expression {
                rex_type: Some(expression::RexType::Literal(expression::Literal {
                    nullable: false,
                    type_variation_reference: 0,
                    literal_type: Some(expression::literal::LiteralType::String(v.to_string())),
                })),
            };
            substrait_proto::proto::FunctionArgument {
                arg_type: Some(substrait_proto::proto::function_argument::ArgType::Value(
                    expr,
                )),
            }
        }

        let f = substrait_proto::proto::expression::ScalarFunction {
            function_reference: 0,
            arguments: vec![proto_col(0)],
            options: vec![],
            output_type: None,
            ..Default::default()
        };
        let input_schema =
            RelationType::new(vec![ColumnType::new(CDT::uint32_datatype(), false)]).into_unnamed();
        let extensions = FunctionExtensions::from_iter([(0, "is_null".to_string())]);
        let res = TypedExpr::from_substrait_scalar_func(&f, &input_schema, &extensions)
            .await
            .unwrap();

        assert_eq!(
            res,
            TypedExpr {
                expr: ScalarExpr::Column(0).call_unary(UnaryFunc::IsNull),
                typ: ColumnType {
                    scalar_type: CDT::boolean_datatype(),
                    nullable: true,
                },
            }
        );

        let f = substrait_proto::proto::expression::ScalarFunction {
            function_reference: 0,
            arguments: vec![proto_col(0), proto_col(1)],
            options: vec![],
            output_type: None,
            ..Default::default()
        };
        let input_schema = RelationType::new(vec![
            ColumnType::new(CDT::uint32_datatype(), false),
            ColumnType::new(CDT::uint32_datatype(), false),
        ])
        .into_unnamed();
        let extensions = FunctionExtensions::from_iter([(0, "add".to_string())]);
        let res = TypedExpr::from_substrait_scalar_func(&f, &input_schema, &extensions)
            .await
            .unwrap();

        assert_eq!(
            res,
            TypedExpr {
                expr: ScalarExpr::Column(0)
                    .call_binary(ScalarExpr::Column(1), BinaryFunc::AddUInt32,),
                typ: ColumnType {
                    scalar_type: CDT::uint32_datatype(),
                    nullable: true,
                },
            }
        );

        let f = substrait_proto::proto::expression::ScalarFunction {
            function_reference: 0,
            arguments: vec![proto_col(0), lit("1 second"), lit("2021-07-01 00:00:00")],
            options: vec![],
            output_type: None,
            ..Default::default()
        };
        let input_schema = RelationType::new(vec![
            ColumnType::new(CDT::timestamp_nanosecond_datatype(), false),
            ColumnType::new(CDT::string_datatype(), false),
        ])
        .into_unnamed();
        let extensions = FunctionExtensions::from_iter(vec![(0, "tumble".to_string())]);
        let res = TypedExpr::from_substrait_scalar_func(&f, &input_schema, &extensions)
            .await
            .unwrap();

        assert_eq!(
            res,
            ScalarExpr::CallUnmaterializable(UnmaterializableFunc::TumbleWindow {
                ts: Box::new(
                    ScalarExpr::Column(0)
                        .with_type(ColumnType::new(CDT::timestamp_nanosecond_datatype(), false))
                ),
                window_size: Interval::from_month_day_nano(0, 0, 1_000_000_000),
                start_time: Some(DateTime::new(1625097600000))
            })
            .with_type(ColumnType::new(CDT::timestamp_millisecond_datatype(), true)),
        );

        let f = substrait_proto::proto::expression::ScalarFunction {
            function_reference: 0,
            arguments: vec![proto_col(0), lit("1 second")],
            options: vec![],
            output_type: None,
            ..Default::default()
        };
        let input_schema = RelationType::new(vec![
            ColumnType::new(CDT::timestamp_nanosecond_datatype(), false),
            ColumnType::new(CDT::string_datatype(), false),
        ])
        .into_unnamed();
        let extensions = FunctionExtensions::from_iter(vec![(0, "tumble".to_string())]);
        let res = TypedExpr::from_substrait_scalar_func(&f, &input_schema, &extensions)
            .await
            .unwrap();

        assert_eq!(
            res,
            ScalarExpr::CallUnmaterializable(UnmaterializableFunc::TumbleWindow {
                ts: Box::new(
                    ScalarExpr::Column(0)
                        .with_type(ColumnType::new(CDT::timestamp_nanosecond_datatype(), false))
                ),
                window_size: Interval::from_month_day_nano(0, 0, 1_000_000_000),
                start_time: None
            })
            .with_type(ColumnType::new(CDT::timestamp_millisecond_datatype(), true)),
        )
    }
}
