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

use std::collections::{BTreeMap, HashMap};

use common_decimal::Decimal128;
use common_time::{Date, Timestamp};
use datatypes::arrow::compute::kernels::window;
use datatypes::arrow::ipc::Binary;
use datatypes::data_type::{ConcreteDataType as CDT, DataType};
use datatypes::value::Value;
use hydroflow::futures::future::Map;
use itertools::Itertools;
use snafu::{OptionExt, ResultExt};
use substrait::variation_const::{
    DATE_32_TYPE_VARIATION_REF, DATE_64_TYPE_VARIATION_REF, DEFAULT_TYPE_VARIATION_REF,
    TIMESTAMP_MICRO_TYPE_VARIATION_REF, TIMESTAMP_MILLI_TYPE_VARIATION_REF,
    TIMESTAMP_NANO_TYPE_VARIATION_REF, TIMESTAMP_SECOND_TYPE_VARIATION_REF,
    UNSIGNED_INTEGER_TYPE_VARIATION_REF,
};
use substrait_proto::proto::aggregate_function::AggregationInvocation;
use substrait_proto::proto::aggregate_rel::{Grouping, Measure};
use substrait_proto::proto::expression::field_reference::ReferenceType::DirectReference;
use substrait_proto::proto::expression::literal::LiteralType;
use substrait_proto::proto::expression::reference_segment::ReferenceType::StructField;
use substrait_proto::proto::expression::{
    IfThen, Literal, MaskExpression, RexType, ScalarFunction,
};
use substrait_proto::proto::extensions::simple_extension_declaration::MappingType;
use substrait_proto::proto::extensions::SimpleExtensionDeclaration;
use substrait_proto::proto::function_argument::ArgType;
use substrait_proto::proto::r#type::Kind;
use substrait_proto::proto::read_rel::ReadType;
use substrait_proto::proto::rel::RelType;
use substrait_proto::proto::{self, plan_rel, Expression, Plan as SubPlan, Rel};

use crate::error::{
    DatatypesSnafu, Error, EvalSnafu, InvalidQuerySnafu, NotImplementedSnafu, PlanSnafu,
    TableNotFoundSnafu,
};
use crate::expr::{
    AggregateExpr, AggregateFunc, BinaryFunc, GlobalId, MapFilterProject, SafeMfpPlan, ScalarExpr,
    TypedExpr, UnaryFunc, UnmaterializableFunc, VariadicFunc,
};
use crate::plan::{AccumulablePlan, AggrWithIndex, KeyValPlan, Plan, ReducePlan, TypedPlan};
use crate::repr::{self, ColumnType, RelationDesc, RelationType};
use crate::transform::{substrait_proto, FlownodeContext, FunctionExtensions};

impl TypedExpr {
    async fn from_substrait_agg_grouping(
        ctx: &mut FlownodeContext,
        groupings: &[Grouping],
        typ: &RelationDesc,
        extensions: &FunctionExtensions,
    ) -> Result<Vec<TypedExpr>, Error> {
        let _ = ctx;
        let mut group_expr = vec![];
        match groupings.len() {
            1 => {
                for e in &groupings[0].grouping_expressions {
                    let x = TypedExpr::from_substrait_rex(e, typ, extensions).await?;
                    group_expr.push(x);
                }
            }
            _ => {
                return not_impl_err!(
                    "Grouping sets not support yet, use union all with group by instead."
                );
            }
        };
        Ok(group_expr)
    }
}

impl AggregateExpr {
    /// Convert list of `Measure` into Flow's AggregateExpr
    ///
    /// Return both the AggregateExpr and a MapFilterProject that is the final output of the aggregate function
    async fn from_substrait_agg_measures(
        ctx: &mut FlownodeContext,
        measures: &[Measure],
        typ: &RelationDesc,
        extensions: &FunctionExtensions,
    ) -> Result<(Vec<AggregateExpr>, MapFilterProject), Error> {
        let _ = ctx;
        let mut all_aggr_exprs = vec![];
        let mut post_maps = vec![];

        for m in measures {
            let filter = match m
                .filter
                .as_ref()
                .map(|fil| TypedExpr::from_substrait_rex(fil, typ, extensions))
            {
                Some(fut) => Some(fut.await),
                None => None,
            }
            .transpose()?;

            let (aggr_expr, post_mfp) = match &m.measure {
                Some(f) => {
                    let distinct = match f.invocation {
                        _ if f.invocation == AggregationInvocation::Distinct as i32 => true,
                        _ if f.invocation == AggregationInvocation::All as i32 => false,
                        _ => false,
                    };
                    AggregateExpr::from_substrait_agg_func(
                        f, typ, extensions, &filter, // TODO(discord9): impl order_by
                        &None, distinct,
                    )
                    .await
                }
                None => not_impl_err!("Aggregate without aggregate function is not supported"),
            }?;
            // permute col index refer to the output of post_mfp,
            // so to help construct a mfp at the end
            let mut post_map = post_mfp.unwrap_or(ScalarExpr::Column(0));
            let cur_arity = all_aggr_exprs.len();
            let remap = (0..aggr_expr.len()).map(|i| i + cur_arity).collect_vec();
            post_map.permute(&remap)?;

            all_aggr_exprs.extend(aggr_expr);
            post_maps.push(post_map);
        }

        let input_arity = all_aggr_exprs.len();
        let aggr_arity = post_maps.len();
        let post_mfp_final = MapFilterProject::new(all_aggr_exprs.len())
            .map(post_maps)?
            .project(input_arity..input_arity + aggr_arity)?;

        Ok((all_aggr_exprs, post_mfp_final))
    }

    /// Convert AggregateFunction into Flow's AggregateExpr
    ///
    /// the returned value is a tuple of AggregateExpr and a optional ScalarExpr that if exist is the final output of the aggregate function
    /// since aggr functions like `avg` need to be transform to `sum(x)/cast(count(x) as x_type)`
    pub async fn from_substrait_agg_func(
        f: &proto::AggregateFunction,
        input_schema: &RelationDesc,
        extensions: &FunctionExtensions,
        filter: &Option<TypedExpr>,
        order_by: &Option<Vec<TypedExpr>>,
        distinct: bool,
    ) -> Result<(Vec<AggregateExpr>, Option<ScalarExpr>), Error> {
        // TODO(discord9): impl filter
        let _ = filter;
        let _ = order_by;
        let mut args = vec![];
        for arg in &f.arguments {
            let arg_expr = match &arg.arg_type {
                Some(ArgType::Value(e)) => {
                    TypedExpr::from_substrait_rex(e, input_schema, extensions).await
                }
                _ => not_impl_err!("Aggregated function argument non-Value type not supported"),
            }?;
            args.push(arg_expr);
        }

        if args.len() != 1 {
            return not_impl_err!("Aggregated function with multiple arguments is not supported");
        }

        let arg = if let Some(first) = args.first() {
            first
        } else {
            return not_impl_err!("Aggregated function without arguments is not supported");
        };

        let fn_name = extensions
            .get(&f.function_reference)
            .cloned()
            .map(|s| s.to_lowercase());

        match fn_name.as_ref().map(|s| s.as_ref()) {
            Some(Self::AVG_NAME) => AggregateExpr::from_avg_aggr_func(arg),
            Some(function_name) => {
                let func = AggregateFunc::from_str_and_type(
                    function_name,
                    Some(arg.typ.scalar_type.clone()),
                )?;
                let exprs = vec![AggregateExpr {
                    func,
                    expr: arg.expr.clone(),
                    distinct,
                }];
                let ret_mfp = None;
                Ok((exprs, ret_mfp))
            }
            None => not_impl_err!(
                "Aggregated function not found: function anchor = {:?}",
                f.function_reference
            ),
        }
    }
    const AVG_NAME: &'static str = "avg";
    /// convert `avg` function into `sum(x)/cast(count(x) as x_type)`
    fn from_avg_aggr_func(
        arg: &TypedExpr,
    ) -> Result<(Vec<AggregateExpr>, Option<ScalarExpr>), Error> {
        let arg_type = arg.typ.scalar_type.clone();
        let sum = AggregateExpr {
            func: AggregateFunc::from_str_and_type("sum", Some(arg_type.clone()))?,
            expr: arg.expr.clone(),
            distinct: false,
        };
        let sum_out_type = sum.func.signature().output.clone();
        let count = AggregateExpr {
            func: AggregateFunc::Count,
            expr: arg.expr.clone(),
            distinct: false,
        };
        let count_out_type = count.func.signature().output.clone();
        let avg_output = ScalarExpr::Column(0).call_binary(
            ScalarExpr::Column(1).call_unary(UnaryFunc::Cast(sum_out_type.clone())),
            BinaryFunc::div(sum_out_type.clone())?,
        );
        // make sure we wouldn't divide by zero
        let zero = ScalarExpr::literal(count_out_type.default_value(), count_out_type.clone());
        let non_zero = ScalarExpr::If {
            cond: Box::new(ScalarExpr::Column(1).call_binary(zero.clone(), BinaryFunc::NotEq)),
            then: Box::new(avg_output),
            els: Box::new(ScalarExpr::literal(Value::Null, sum_out_type.clone())),
        };
        let ret_aggr_exprs = vec![sum, count];
        let ret_mfp = Some(non_zero);
        Ok((ret_aggr_exprs, ret_mfp))
    }
}

impl KeyValPlan {
    /// Generate KeyValPlan from AggregateExpr and group_exprs
    ///
    /// will also change aggregate expr to use column ref if necessary
    fn from_substrait_gen_key_val_plan(
        aggr_exprs: &mut [AggregateExpr],
        group_exprs: &[TypedExpr],
        input_arity: usize,
    ) -> Result<KeyValPlan, Error> {
        let group_expr_val = group_exprs
            .iter()
            .cloned()
            .map(|expr| expr.expr.clone())
            .collect_vec();
        let output_arity = group_expr_val.len();
        let key_plan = MapFilterProject::new(input_arity)
            .map(group_expr_val)?
            .project(input_arity..input_arity + output_arity)?;

        // val_plan is extracted from aggr_exprs to give aggr function it's necessary input
        // and since aggr func need inputs that is column ref, we just add a prefix mfp to transform any expr that is not into a column ref
        let val_plan = {
            let need_mfp = aggr_exprs.iter().any(|agg| agg.expr.as_column().is_none());
            if need_mfp {
                // create mfp from aggr_expr, and modify aggr_expr to use the output column of mfp
                let input_exprs = aggr_exprs
                    .iter_mut()
                    .enumerate()
                    .map(|(idx, aggr)| {
                        let ret = aggr.expr.clone();
                        aggr.expr = ScalarExpr::Column(idx);
                        ret
                    })
                    .collect_vec();
                let aggr_arity = aggr_exprs.len();

                MapFilterProject::new(input_arity)
                    .map(input_exprs)?
                    .project(input_arity..input_arity + aggr_arity)?
            } else {
                // simply take all inputs as value
                MapFilterProject::new(input_arity)
            }
        };
        Ok(KeyValPlan {
            key_plan: key_plan.into_safe(),
            val_plan: val_plan.into_safe(),
        })
    }
}

/// find out the column that should be time index in group exprs(which is all columns that should be keys)
/// TODO(discord9): better ways to assign time index
fn find_time_index_in_group_exprs(group_exprs: &[TypedExpr]) -> Option<usize> {
    group_exprs.iter().position(|expr| {
        matches!(
            &expr.expr,
            ScalarExpr::CallUnary {
                func: UnaryFunc::TumbleWindowFloor { .. },
                expr: _
            }
        )
    })
}

impl TypedPlan {
    /// Convert AggregateRel into Flow's TypedPlan
    ///
    /// The output of aggr plan is:
    ///
    /// <group_exprs>..<aggr_exprs>
    #[async_recursion::async_recursion]
    pub async fn from_substrait_agg_rel(
        ctx: &mut FlownodeContext,
        agg: &proto::AggregateRel,
        extensions: &FunctionExtensions,
    ) -> Result<TypedPlan, Error> {
        let input = if let Some(input) = agg.input.as_ref() {
            TypedPlan::from_substrait_rel(ctx, input, extensions).await?
        } else {
            return not_impl_err!("Aggregate without an input is not supported");
        };

        let group_exprs = {
            let group_exprs = TypedExpr::from_substrait_agg_grouping(
                ctx,
                &agg.groupings,
                &input.schema,
                extensions,
            )
            .await?;

            TypedExpr::expand_multi_value(&input.schema.typ, &group_exprs)?
        };

        let time_index = find_time_index_in_group_exprs(&group_exprs);

        let (mut aggr_exprs, post_mfp) = AggregateExpr::from_substrait_agg_measures(
            ctx,
            &agg.measures,
            &input.schema,
            extensions,
        )
        .await?;

        let key_val_plan = KeyValPlan::from_substrait_gen_key_val_plan(
            &mut aggr_exprs,
            &group_exprs,
            input.schema.typ.column_types.len(),
        )?;

        // output type is group_exprs + aggr_exprs
        let output_type = {
            let mut output_types = Vec::new();
            // give best effort to get column name
            let mut output_names = Vec::new();
            // mark all auto added cols
            let mut auto_cols = vec![];
            // first append group_expr as key, then aggr_expr as value
            for (idx, expr) in group_exprs.iter().enumerate() {
                output_types.push(expr.typ.clone());
                let col_name = match &expr.expr {
                    ScalarExpr::CallUnary {
                        func: UnaryFunc::TumbleWindowFloor { .. },
                        ..
                    } => Some("window_start".to_string()),
                    ScalarExpr::CallUnary {
                        func: UnaryFunc::TumbleWindowCeiling { .. },
                        ..
                    } => {
                        auto_cols.push(idx);
                        Some("window_end".to_string())
                    }
                    ScalarExpr::Column(col) => input.schema.get_name(*col).clone(),
                    _ => None,
                };
                output_names.push(col_name)
            }

            for aggr in &aggr_exprs {
                output_types.push(ColumnType::new_nullable(
                    aggr.func.signature().output.clone(),
                ));
                // TODO(discord9): find a clever way to name them?
                output_names.push(None);
            }
            // TODO(discord9): try best to get time
            if group_exprs.is_empty() {
                RelationType::new(output_types)
            } else {
                RelationType::new(output_types).with_key((0..group_exprs.len()).collect_vec())
            }
            .with_time_index(time_index)
            .with_autos(&auto_cols)
            .into_named(output_names)
        };

        // copy aggr_exprs to full_aggrs, and split them into simple_aggrs and distinct_aggrs
        // also set them input/output column
        let full_aggrs = aggr_exprs;
        let mut simple_aggrs = Vec::new();
        let mut distinct_aggrs = Vec::new();
        for (output_column, aggr_expr) in full_aggrs.iter().enumerate() {
            let input_column = aggr_expr.expr.as_column().with_context(|| PlanSnafu {
                reason: "Expect aggregate argument to be transformed into a column at this point",
            })?;
            if aggr_expr.distinct {
                distinct_aggrs.push(AggrWithIndex::new(
                    aggr_expr.clone(),
                    input_column,
                    output_column,
                ));
            } else {
                simple_aggrs.push(AggrWithIndex::new(
                    aggr_expr.clone(),
                    input_column,
                    output_column,
                ));
            }
        }
        let accum_plan = AccumulablePlan {
            full_aggrs,
            simple_aggrs,
            distinct_aggrs,
        };
        let plan = Plan::Reduce {
            input: Box::new(input),
            key_val_plan,
            reduce_plan: ReducePlan::Accumulable(accum_plan),
        };
        // FIX(discord9): deal with key first
        if post_mfp.is_identity() {
            Ok(TypedPlan {
                schema: output_type,
                plan,
            })
        } else {
            // make post_mfp map identical mapping of keys
            let input = TypedPlan {
                schema: output_type.clone(),
                plan,
            };
            let key_arity = group_exprs.len();
            let mut post_mfp = post_mfp;
            let val_arity = post_mfp.input_arity;
            // offset post_mfp's col ref by `key_arity`
            let shuffle = BTreeMap::from_iter((0..val_arity).map(|v| (v, v + key_arity)));
            let new_arity = key_arity + val_arity;
            post_mfp.permute(shuffle, new_arity)?;
            // add key projection to post mfp
            let (m, f, p) = post_mfp.into_map_filter_project();
            let p = (0..key_arity).chain(p).collect_vec();
            let post_mfp = MapFilterProject::new(new_arity)
                .map(m)?
                .filter(f)?
                .project(p)?;
            Ok(TypedPlan {
                schema: output_type.apply_mfp(&post_mfp.clone().into_safe())?,
                plan: Plan::Mfp {
                    input: Box::new(input),
                    mfp: post_mfp,
                },
            })
        }
    }
}

#[cfg(test)]
mod test {
    use bytes::BytesMut;
    use common_time::{DateTime, Interval};
    use datatypes::prelude::ConcreteDataType;
    use pretty_assertions::{assert_eq, assert_ne};

    use super::*;
    use crate::expr::{DfScalarFunction, RawDfScalarFn};
    use crate::plan::{Plan, TypedPlan};
    use crate::repr::{self, ColumnType, RelationType};
    use crate::transform::test::{create_test_ctx, create_test_query_engine, sql_to_substrait};
    /// TODO(discord9): add more illegal sql tests
    #[tokio::test]
    async fn test_missing_key_check() {
        let engine = create_test_query_engine();
        let sql = "SELECT avg(number) FROM numbers_with_ts GROUP BY tumble(ts, '1 hour'), number";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        assert!(TypedPlan::from_substrait_plan(&mut ctx, &plan)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_df_func_basic() {
        let engine = create_test_query_engine();
        let sql = "SELECT sum(abs(number)) FROM numbers_with_ts GROUP BY tumble(ts, '1 second', '2021-07-01 00:00:00');";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan)
            .await
            .unwrap();

        let aggr_expr = AggregateExpr {
            func: AggregateFunc::SumUInt32,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![
                ColumnType::new(CDT::uint64_datatype(), true), // sum(number)
                ColumnType::new(CDT::datetime_datatype(), false), // window start
                ColumnType::new(CDT::datetime_datatype(), false), // window end
            ])
            .with_key(vec![2])
            .with_time_index(Some(1))
            .with_autos(&[2])
            .into_named(vec![
                None,
                Some("window_start".to_string()),
                Some("window_end".to_string()),
            ]),
            plan: Plan::Mfp {
                input: Box::new(
                    Plan::Reduce {
                        input: Box::new(
                            Plan::Get {
                                id: crate::expr::Id::Global(GlobalId::User(1)),
                            }
                            .with_types(
                                RelationType::new(vec![
                                    ColumnType::new(ConcreteDataType::uint32_datatype(), false),
                                    ColumnType::new(ConcreteDataType::datetime_datatype(), false),
                                ])
                                .into_named(vec![
                                    Some("number".to_string()),
                                    Some("ts".to_string()),
                                ]),
                            ),
                        ),
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(2)
                                .map(vec![
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowFloor {
                                            window_size: Interval::from_month_day_nano(
                                                0,
                                                0,
                                                1_000_000_000,
                                            ),
                                            start_time: Some(DateTime::new(1625097600000)),
                                        },
                                    ),
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowCeiling {
                                            window_size: Interval::from_month_day_nano(
                                                0,
                                                0,
                                                1_000_000_000,
                                            ),
                                            start_time: Some(DateTime::new(1625097600000)),
                                        },
                                    ),
                                ])
                                .unwrap()
                                .project(vec![2, 3])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(2)
                                .map(vec![ScalarExpr::CallDf {
                                    df_scalar_fn: DfScalarFunction::try_from_raw_fn(
                                        RawDfScalarFn {
                                            f: BytesMut::from(
                                                b"\x08\x01\"\x08\x1a\x06\x12\x04\n\x02\x12\0"
                                                    .as_ref(),
                                            ),
                                            input_schema: RelationType::new(vec![ColumnType::new(
                                                ConcreteDataType::uint32_datatype(),
                                                false,
                                            )])
                                            .into_unnamed(),
                                            extensions: FunctionExtensions {
                                                anchor_to_name: BTreeMap::from([
                                                    (0, "tumble".to_string()),
                                                    (1, "abs".to_string()),
                                                    (2, "sum".to_string()),
                                                ]),
                                            },
                                        },
                                    )
                                    .await
                                    .unwrap(),
                                    exprs: vec![ScalarExpr::Column(0)],
                                }])
                                .unwrap()
                                .project(vec![2])
                                .unwrap()
                                .into_safe(),
                        },
                        reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                            full_aggrs: vec![aggr_expr.clone()],
                            simple_aggrs: vec![AggrWithIndex::new(aggr_expr.clone(), 0, 0)],
                            distinct_aggrs: vec![],
                        }),
                    }
                    .with_types(
                        RelationType::new(vec![
                            ColumnType::new(CDT::datetime_datatype(), false), // window start
                            ColumnType::new(CDT::datetime_datatype(), false), // window end
                            ColumnType::new(CDT::uint64_datatype(), true),    //sum(number)
                        ])
                        .with_key(vec![1])
                        .with_time_index(Some(0))
                        .with_autos(&[1])
                        .into_named(vec![
                            Some("window_start".to_string()),
                            Some("window_end".to_string()),
                            None,
                        ]),
                    ),
                ),
                mfp: MapFilterProject::new(3)
                    .map(vec![
                        ScalarExpr::Column(2),
                        ScalarExpr::Column(3),
                        ScalarExpr::Column(0),
                        ScalarExpr::Column(1),
                    ])
                    .unwrap()
                    .project(vec![4, 5, 6])
                    .unwrap(),
            },
        };
        assert_eq!(expected, flow_plan);
    }

    #[tokio::test]
    async fn test_df_func_expr_tree() {
        let engine = create_test_query_engine();
        let sql = "SELECT abs(sum(number)) FROM numbers_with_ts GROUP BY tumble(ts, '1 second', '2021-07-01 00:00:00');";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan)
            .await
            .unwrap();

        let aggr_expr = AggregateExpr {
            func: AggregateFunc::SumUInt32,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![
                ColumnType::new(CDT::uint64_datatype(), true), // sum(number)
                ColumnType::new(CDT::datetime_datatype(), false), // window start
                ColumnType::new(CDT::datetime_datatype(), false), // window end
            ])
            .with_key(vec![2])
            .with_time_index(Some(1))
            .with_autos(&[2])
            .into_named(vec![
                None,
                Some("window_start".to_string()),
                Some("window_end".to_string()),
            ]),
            plan: Plan::Mfp {
                input: Box::new(
                    Plan::Reduce {
                        input: Box::new(
                            Plan::Get {
                                id: crate::expr::Id::Global(GlobalId::User(1)),
                            }
                            .with_types(
                                RelationType::new(vec![
                                    ColumnType::new(ConcreteDataType::uint32_datatype(), false),
                                    ColumnType::new(ConcreteDataType::datetime_datatype(), false),
                                ])
                                .into_named(vec![
                                    Some("number".to_string()),
                                    Some("ts".to_string()),
                                ]),
                            ),
                        ),
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(2)
                                .map(vec![
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowFloor {
                                            window_size: Interval::from_month_day_nano(
                                                0,
                                                0,
                                                1_000_000_000,
                                            ),
                                            start_time: Some(DateTime::new(1625097600000)),
                                        },
                                    ),
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowCeiling {
                                            window_size: Interval::from_month_day_nano(
                                                0,
                                                0,
                                                1_000_000_000,
                                            ),
                                            start_time: Some(DateTime::new(1625097600000)),
                                        },
                                    ),
                                ])
                                .unwrap()
                                .project(vec![2, 3])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(2)
                                .project(vec![0, 1])
                                .unwrap()
                                .into_safe(),
                        },
                        reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                            full_aggrs: vec![aggr_expr.clone()],
                            simple_aggrs: vec![AggrWithIndex::new(aggr_expr.clone(), 0, 0)],
                            distinct_aggrs: vec![],
                        }),
                    }
                    .with_types(
                        RelationType::new(vec![
                            ColumnType::new(CDT::datetime_datatype(), false), // window start
                            ColumnType::new(CDT::datetime_datatype(), false), // window end
                            ColumnType::new(CDT::uint64_datatype(), true),    //sum(number)
                        ])
                        .with_key(vec![1])
                        .with_time_index(Some(0))
                        .with_autos(&[1])
                        .into_named(vec![
                            Some("window_start".to_string()),
                            Some("window_end".to_string()),
                            None,
                        ]),
                    ),
                ),
                mfp: MapFilterProject::new(3)
                    .map(vec![
                        ScalarExpr::Column(2),
                        ScalarExpr::CallDf {
                            df_scalar_fn: DfScalarFunction::try_from_raw_fn(RawDfScalarFn {
                                f: BytesMut::from(b"\"\x08\x1a\x06\x12\x04\n\x02\x12\0".as_ref()),
                                input_schema: RelationType::new(vec![ColumnType::new(
                                    ConcreteDataType::uint64_datatype(),
                                    true,
                                )])
                                .into_unnamed(),
                                extensions: FunctionExtensions {
                                    anchor_to_name: BTreeMap::from([
                                        (0, "abs".to_string()),
                                        (1, "tumble".to_string()),
                                        (2, "sum".to_string()),
                                    ]),
                                },
                            })
                            .await
                            .unwrap(),
                            exprs: vec![ScalarExpr::Column(3)],
                        },
                        ScalarExpr::Column(0),
                        ScalarExpr::Column(1),
                    ])
                    .unwrap()
                    .project(vec![4, 5, 6])
                    .unwrap(),
            },
        };
        assert_eq!(expected, flow_plan);
    }

    /// TODO(discord9): add more illegal sql tests
    #[tokio::test]
    async fn test_tumble_composite() {
        let engine = create_test_query_engine();
        let sql =
            "SELECT number, avg(number) FROM numbers_with_ts GROUP BY tumble(ts, '1 hour'), number";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan)
            .await
            .unwrap();

        let aggr_exprs = vec![
            AggregateExpr {
                func: AggregateFunc::SumUInt32,
                expr: ScalarExpr::Column(0),
                distinct: false,
            },
            AggregateExpr {
                func: AggregateFunc::Count,
                expr: ScalarExpr::Column(0),
                distinct: false,
            },
        ];
        let avg_expr = ScalarExpr::If {
            cond: Box::new(ScalarExpr::Column(4).call_binary(
                ScalarExpr::Literal(Value::from(0i64), CDT::int64_datatype()),
                BinaryFunc::NotEq,
            )),
            then: Box::new(ScalarExpr::Column(3).call_binary(
                ScalarExpr::Column(4).call_unary(UnaryFunc::Cast(CDT::uint64_datatype())),
                BinaryFunc::DivUInt64,
            )),
            els: Box::new(ScalarExpr::Literal(Value::Null, CDT::uint64_datatype())),
        };
        let expected = TypedPlan {
            plan: Plan::Mfp {
                input: Box::new(
                    Plan::Reduce {
                        input: Box::new(
                            Plan::Get {
                                id: crate::expr::Id::Global(GlobalId::User(1)),
                            }
                            .with_types(
                                RelationType::new(vec![
                                    ColumnType::new(ConcreteDataType::uint32_datatype(), false),
                                    ColumnType::new(ConcreteDataType::datetime_datatype(), false),
                                ])
                                .into_named(vec![
                                    Some("number".to_string()),
                                    Some("ts".to_string()),
                                ]),
                            ),
                        ),
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(2)
                                .map(vec![
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowFloor {
                                            window_size: Interval::from_month_day_nano(
                                                0,
                                                0,
                                                3_600_000_000_000,
                                            ),
                                            start_time: None,
                                        },
                                    ),
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowCeiling {
                                            window_size: Interval::from_month_day_nano(
                                                0,
                                                0,
                                                3_600_000_000_000,
                                            ),
                                            start_time: None,
                                        },
                                    ),
                                    ScalarExpr::Column(0),
                                ])
                                .unwrap()
                                .project(vec![2, 3, 4])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(2)
                                .project(vec![0, 1])
                                .unwrap()
                                .into_safe(),
                        },
                        reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                            full_aggrs: aggr_exprs.clone(),
                            simple_aggrs: vec![
                                AggrWithIndex::new(aggr_exprs[0].clone(), 0, 0),
                                AggrWithIndex::new(aggr_exprs[1].clone(), 0, 1),
                            ],
                            distinct_aggrs: vec![],
                        }),
                    }
                    .with_types(
                        RelationType::new(vec![
                            // keys
                            ColumnType::new(CDT::datetime_datatype(), false), // window start(time index)
                            ColumnType::new(CDT::datetime_datatype(), false), // window end(pk)
                            ColumnType::new(CDT::uint32_datatype(), false),   // number(pk)
                            // values
                            ColumnType::new(CDT::uint64_datatype(), true), // avg.sum(number)
                            ColumnType::new(CDT::int64_datatype(), true),  // avg.count(number)
                        ])
                        .with_key(vec![1, 2])
                        .with_time_index(Some(0))
                        .with_autos(&[1])
                        .into_named(vec![
                            Some("window_start".to_string()),
                            Some("window_end".to_string()),
                            Some("number".to_string()),
                            None,
                            None,
                        ]),
                    ),
                ),
                mfp: MapFilterProject::new(5)
                    .map(vec![
                        avg_expr,
                        ScalarExpr::Column(2), // number(pk)
                        ScalarExpr::Column(5), // avg.sum(number)
                        ScalarExpr::Column(0), // window start
                        ScalarExpr::Column(1), // window end
                    ])
                    .unwrap()
                    .project(vec![6, 7, 8, 9])
                    .unwrap(),
            },
            schema: RelationType::new(vec![
                ColumnType::new(CDT::uint32_datatype(), false), // number
                ColumnType::new(CDT::uint64_datatype(), true),  // avg(number)
                ColumnType::new(CDT::datetime_datatype(), false), // window start
                ColumnType::new(CDT::datetime_datatype(), false), // window end
            ])
            .with_key(vec![0, 3])
            .with_time_index(Some(2))
            .with_autos(&[3])
            .into_named(vec![
                Some("number".to_string()),
                None,
                Some("window_start".to_string()),
                Some("window_end".to_string()),
            ]),
        };
        assert_eq!(flow_plan, expected);
    }

    #[tokio::test]
    async fn test_tumble_parse_optional() {
        let engine = create_test_query_engine();
        let sql = "SELECT sum(number) FROM numbers_with_ts GROUP BY tumble(ts, '1 hour')";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan)
            .await
            .unwrap();

        let aggr_expr = AggregateExpr {
            func: AggregateFunc::SumUInt32,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![
                ColumnType::new(CDT::uint64_datatype(), true), // sum(number)
                ColumnType::new(CDT::datetime_datatype(), false), // window start
                ColumnType::new(CDT::datetime_datatype(), false), // window end
            ])
            .with_key(vec![2])
            .with_time_index(Some(1))
            .with_autos(&[2])
            .into_named(vec![
                None,
                Some("window_start".to_string()),
                Some("window_end".to_string()),
            ]),
            plan: Plan::Mfp {
                input: Box::new(
                    Plan::Reduce {
                        input: Box::new(
                            Plan::Get {
                                id: crate::expr::Id::Global(GlobalId::User(1)),
                            }
                            .with_types(
                                RelationType::new(vec![
                                    ColumnType::new(ConcreteDataType::uint32_datatype(), false),
                                    ColumnType::new(ConcreteDataType::datetime_datatype(), false),
                                ])
                                .into_named(vec![
                                    Some("number".to_string()),
                                    Some("ts".to_string()),
                                ]),
                            ),
                        ),
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(2)
                                .map(vec![
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowFloor {
                                            window_size: Interval::from_month_day_nano(
                                                0,
                                                0,
                                                3_600_000_000_000,
                                            ),
                                            start_time: None,
                                        },
                                    ),
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowCeiling {
                                            window_size: Interval::from_month_day_nano(
                                                0,
                                                0,
                                                3_600_000_000_000,
                                            ),
                                            start_time: None,
                                        },
                                    ),
                                ])
                                .unwrap()
                                .project(vec![2, 3])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(2)
                                .project(vec![0, 1])
                                .unwrap()
                                .into_safe(),
                        },
                        reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                            full_aggrs: vec![aggr_expr.clone()],
                            simple_aggrs: vec![AggrWithIndex::new(aggr_expr.clone(), 0, 0)],
                            distinct_aggrs: vec![],
                        }),
                    }
                    .with_types(
                        RelationType::new(vec![
                            ColumnType::new(CDT::datetime_datatype(), false), // window start
                            ColumnType::new(CDT::datetime_datatype(), false), // window end
                            ColumnType::new(CDT::uint64_datatype(), true),    //sum(number)
                        ])
                        .with_key(vec![1])
                        .with_time_index(Some(0))
                        .with_autos(&[1])
                        .into_named(vec![
                            Some("window_start".to_string()),
                            Some("window_end".to_string()),
                            None,
                        ]),
                    ),
                ),
                mfp: MapFilterProject::new(3)
                    .map(vec![
                        ScalarExpr::Column(2),
                        ScalarExpr::Column(3),
                        ScalarExpr::Column(0),
                        ScalarExpr::Column(1),
                    ])
                    .unwrap()
                    .project(vec![4, 5, 6])
                    .unwrap(),
            },
        };
        assert_eq!(flow_plan, expected);
    }

    #[tokio::test]
    async fn test_tumble_parse() {
        let engine = create_test_query_engine();
        let sql = "SELECT sum(number) FROM numbers_with_ts GROUP BY tumble(ts, '1 hour', '2021-07-01 00:00:00')";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan)
            .await
            .unwrap();

        let aggr_expr = AggregateExpr {
            func: AggregateFunc::SumUInt32,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![
                ColumnType::new(CDT::uint64_datatype(), true), // sum(number)
                ColumnType::new(CDT::datetime_datatype(), false), // window start
                ColumnType::new(CDT::datetime_datatype(), false), // window end
            ])
            .with_key(vec![2])
            .with_time_index(Some(1))
            .with_autos(&[2])
            .into_named(vec![
                None,
                Some("window_start".to_string()),
                Some("window_end".to_string()),
            ]),
            plan: Plan::Mfp {
                input: Box::new(
                    Plan::Reduce {
                        input: Box::new(
                            Plan::Get {
                                id: crate::expr::Id::Global(GlobalId::User(1)),
                            }
                            .with_types(
                                RelationType::new(vec![
                                    ColumnType::new(ConcreteDataType::uint32_datatype(), false),
                                    ColumnType::new(ConcreteDataType::datetime_datatype(), false),
                                ])
                                .into_named(vec![
                                    Some("number".to_string()),
                                    Some("ts".to_string()),
                                ]),
                            ),
                        ),
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(2)
                                .map(vec![
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowFloor {
                                            window_size: Interval::from_month_day_nano(
                                                0,
                                                0,
                                                3_600_000_000_000,
                                            ),
                                            start_time: Some(DateTime::new(1625097600000)),
                                        },
                                    ),
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowCeiling {
                                            window_size: Interval::from_month_day_nano(
                                                0,
                                                0,
                                                3_600_000_000_000,
                                            ),
                                            start_time: Some(DateTime::new(1625097600000)),
                                        },
                                    ),
                                ])
                                .unwrap()
                                .project(vec![2, 3])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(2)
                                .project(vec![0, 1])
                                .unwrap()
                                .into_safe(),
                        },
                        reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                            full_aggrs: vec![aggr_expr.clone()],
                            simple_aggrs: vec![AggrWithIndex::new(aggr_expr.clone(), 0, 0)],
                            distinct_aggrs: vec![],
                        }),
                    }
                    .with_types(
                        RelationType::new(vec![
                            ColumnType::new(CDT::datetime_datatype(), false), // window start
                            ColumnType::new(CDT::datetime_datatype(), false), // window end
                            ColumnType::new(CDT::uint64_datatype(), true),    //sum(number)
                        ])
                        .with_key(vec![1])
                        .with_time_index(Some(0))
                        .with_autos(&[1])
                        .into_named(vec![
                            Some("window_start".to_string()),
                            Some("window_end".to_string()),
                            None,
                        ]),
                    ),
                ),
                mfp: MapFilterProject::new(3)
                    .map(vec![
                        ScalarExpr::Column(2),
                        ScalarExpr::Column(3),
                        ScalarExpr::Column(0),
                        ScalarExpr::Column(1),
                    ])
                    .unwrap()
                    .project(vec![4, 5, 6])
                    .unwrap(),
            },
        };
        assert_eq!(flow_plan, expected);
    }

    #[tokio::test]
    async fn test_avg_group_by() {
        let engine = create_test_query_engine();
        let sql = "SELECT avg(number), number FROM numbers GROUP BY number";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan).await;

        let aggr_exprs = vec![
            AggregateExpr {
                func: AggregateFunc::SumUInt32,
                expr: ScalarExpr::Column(0),
                distinct: false,
            },
            AggregateExpr {
                func: AggregateFunc::Count,
                expr: ScalarExpr::Column(0),
                distinct: false,
            },
        ];
        let avg_expr = ScalarExpr::If {
            cond: Box::new(ScalarExpr::Column(2).call_binary(
                ScalarExpr::Literal(Value::from(0i64), CDT::int64_datatype()),
                BinaryFunc::NotEq,
            )),
            then: Box::new(ScalarExpr::Column(1).call_binary(
                ScalarExpr::Column(2).call_unary(UnaryFunc::Cast(CDT::uint64_datatype())),
                BinaryFunc::DivUInt64,
            )),
            els: Box::new(ScalarExpr::Literal(Value::Null, CDT::uint64_datatype())),
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![
                ColumnType::new(CDT::uint64_datatype(), true), // sum(number) -> u64
                ColumnType::new(CDT::uint32_datatype(), false), // number
            ])
            .with_key(vec![1])
            .into_named(vec![None, Some("number".to_string())]),
            plan: Plan::Mfp {
                input: Box::new(
                    Plan::Reduce {
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
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(1)
                                .map(vec![ScalarExpr::Column(0)])
                                .unwrap()
                                .project(vec![1])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(1)
                                .project(vec![0])
                                .unwrap()
                                .into_safe(),
                        },
                        reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                            full_aggrs: aggr_exprs.clone(),
                            simple_aggrs: vec![
                                AggrWithIndex::new(aggr_exprs[0].clone(), 0, 0),
                                AggrWithIndex::new(aggr_exprs[1].clone(), 0, 1),
                            ],
                            distinct_aggrs: vec![],
                        }),
                    }
                    .with_types(
                        RelationType::new(vec![
                            ColumnType::new(ConcreteDataType::uint32_datatype(), false), // key: number
                            ColumnType::new(ConcreteDataType::uint64_datatype(), true),  // sum
                            ColumnType::new(ConcreteDataType::int64_datatype(), true),   // count
                        ])
                        .with_key(vec![0])
                        .into_named(vec![
                            Some("number".to_string()),
                            None,
                            None,
                        ]),
                    ),
                ),
                mfp: MapFilterProject::new(3)
                    .map(vec![
                        avg_expr, // col 3
                        // TODO(discord9): optimize mfp so to remove indirect ref
                        ScalarExpr::Column(3), // col 4
                        ScalarExpr::Column(0), // col 5
                    ])
                    .unwrap()
                    .project(vec![4, 5])
                    .unwrap(),
            },
        };
        assert_eq!(flow_plan.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_avg() {
        let engine = create_test_query_engine();
        let sql = "SELECT avg(number) FROM numbers";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();

        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan)
            .await
            .unwrap();

        let aggr_exprs = vec![
            AggregateExpr {
                func: AggregateFunc::SumUInt32,
                expr: ScalarExpr::Column(0),
                distinct: false,
            },
            AggregateExpr {
                func: AggregateFunc::Count,
                expr: ScalarExpr::Column(0),
                distinct: false,
            },
        ];
        let avg_expr = ScalarExpr::If {
            cond: Box::new(ScalarExpr::Column(1).call_binary(
                ScalarExpr::Literal(Value::from(0i64), CDT::int64_datatype()),
                BinaryFunc::NotEq,
            )),
            then: Box::new(ScalarExpr::Column(0).call_binary(
                ScalarExpr::Column(1).call_unary(UnaryFunc::Cast(CDT::uint64_datatype())),
                BinaryFunc::DivUInt64,
            )),
            els: Box::new(ScalarExpr::Literal(Value::Null, CDT::uint64_datatype())),
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![ColumnType::new(CDT::uint64_datatype(), true)])
                .into_named(vec![None]),
            plan: Plan::Mfp {
                input: Box::new(
                    Plan::Reduce {
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
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(1)
                                .project(vec![])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(1)
                                .project(vec![0])
                                .unwrap()
                                .into_safe(),
                        },
                        reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                            full_aggrs: aggr_exprs.clone(),
                            simple_aggrs: vec![
                                AggrWithIndex::new(aggr_exprs[0].clone(), 0, 0),
                                AggrWithIndex::new(aggr_exprs[1].clone(), 0, 1),
                            ],
                            distinct_aggrs: vec![],
                        }),
                    }
                    .with_types(
                        RelationType::new(vec![
                            ColumnType::new(ConcreteDataType::uint64_datatype(), true), // sum
                            ColumnType::new(ConcreteDataType::int64_datatype(), true),  // count
                        ])
                        .into_named(vec![None, None]),
                    ),
                ),
                mfp: MapFilterProject::new(2)
                    .map(vec![
                        avg_expr,
                        // TODO(discord9): optimize mfp so to remove indirect ref
                        ScalarExpr::Column(2),
                    ])
                    .unwrap()
                    .project(vec![3])
                    .unwrap(),
            },
        };
        assert_eq!(flow_plan, expected);
    }

    #[tokio::test]
    async fn test_sum() {
        let engine = create_test_query_engine();
        let sql = "SELECT sum(number) FROM numbers";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan).await;
        let typ = RelationType::new(vec![ColumnType::new(
            ConcreteDataType::uint64_datatype(),
            true,
        )]);
        let aggr_expr = AggregateExpr {
            func: AggregateFunc::SumUInt32,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![ColumnType::new(CDT::uint64_datatype(), true)])
                .into_unnamed(),
            plan: Plan::Mfp {
                input: Box::new(
                    Plan::Reduce {
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
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(1)
                                .project(vec![])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(1)
                                .project(vec![0])
                                .unwrap()
                                .into_safe(),
                        },
                        reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                            full_aggrs: vec![aggr_expr.clone()],
                            simple_aggrs: vec![AggrWithIndex::new(aggr_expr.clone(), 0, 0)],
                            distinct_aggrs: vec![],
                        }),
                    }
                    .with_types(typ.into_unnamed()),
                ),
                mfp: MapFilterProject::new(1)
                    .map(vec![ScalarExpr::Column(0), ScalarExpr::Column(1)])
                    .unwrap()
                    .project(vec![2])
                    .unwrap(),
            },
        };
        assert_eq!(flow_plan.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_sum_group_by() {
        let engine = create_test_query_engine();
        let sql = "SELECT sum(number), number FROM numbers GROUP BY number";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan)
            .await
            .unwrap();

        let aggr_expr = AggregateExpr {
            func: AggregateFunc::SumUInt32,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![
                ColumnType::new(CDT::uint64_datatype(), true), // col sum(number)
                ColumnType::new(CDT::uint32_datatype(), false), // col number
            ])
            .with_key(vec![1])
            .into_named(vec![None, Some("number".to_string())]),
            plan: Plan::Mfp {
                input: Box::new(
                    Plan::Reduce {
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
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(1)
                                .map(vec![ScalarExpr::Column(0)])
                                .unwrap()
                                .project(vec![1])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(1)
                                .project(vec![0])
                                .unwrap()
                                .into_safe(),
                        },
                        reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                            full_aggrs: vec![aggr_expr.clone()],
                            simple_aggrs: vec![AggrWithIndex::new(aggr_expr.clone(), 0, 0)],
                            distinct_aggrs: vec![],
                        }),
                    }
                    .with_types(
                        RelationType::new(vec![
                            ColumnType::new(CDT::uint32_datatype(), false), // col number
                            ColumnType::new(CDT::uint64_datatype(), true),  // col sum(number)
                        ])
                        .with_key(vec![0])
                        .into_named(vec![Some("number".to_string()), None]),
                    ),
                ),
                mfp: MapFilterProject::new(2)
                    .map(vec![
                        ScalarExpr::Column(1),
                        ScalarExpr::Column(2),
                        ScalarExpr::Column(0),
                    ])
                    .unwrap()
                    .project(vec![3, 4])
                    .unwrap(),
            },
        };

        assert_eq!(flow_plan, expected);
    }

    #[tokio::test]
    async fn test_sum_add() {
        let engine = create_test_query_engine();
        let sql = "SELECT sum(number+number) FROM numbers";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan).await;

        let aggr_expr = AggregateExpr {
            func: AggregateFunc::SumUInt32,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![ColumnType::new(CDT::uint64_datatype(), true)])
                .into_unnamed(),
            plan: Plan::Mfp {
                input: Box::new(
                    Plan::Reduce {
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
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(1)
                                .project(vec![])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(1)
                                .map(vec![ScalarExpr::Column(0)
                                    .call_binary(ScalarExpr::Column(0), BinaryFunc::AddUInt32)])
                                .unwrap()
                                .project(vec![1])
                                .unwrap()
                                .into_safe(),
                        },
                        reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                            full_aggrs: vec![aggr_expr.clone()],
                            simple_aggrs: vec![AggrWithIndex::new(aggr_expr.clone(), 0, 0)],
                            distinct_aggrs: vec![],
                        }),
                    }
                    .with_types(
                        RelationType::new(vec![ColumnType::new(CDT::uint64_datatype(), true)])
                            .into_unnamed(),
                    ),
                ),
                mfp: MapFilterProject::new(1)
                    .map(vec![ScalarExpr::Column(0), ScalarExpr::Column(1)])
                    .unwrap()
                    .project(vec![2])
                    .unwrap(),
            },
        };
        assert_eq!(flow_plan.unwrap(), expected);
    }
}
