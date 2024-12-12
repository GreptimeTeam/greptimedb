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

use itertools::Itertools;
use snafu::OptionExt;
use substrait_proto::proto::aggregate_function::AggregationInvocation;
use substrait_proto::proto::aggregate_rel::{Grouping, Measure};
use substrait_proto::proto::function_argument::ArgType;
use substrait_proto::proto::{self};

use crate::error::{Error, NotImplementedSnafu, PlanSnafu};
use crate::expr::{
    AggregateExpr, AggregateFunc, MapFilterProject, ScalarExpr, TypedExpr, UnaryFunc,
};
use crate::plan::{AccumulablePlan, AggrWithIndex, KeyValPlan, Plan, ReducePlan, TypedPlan};
use crate::repr::{ColumnType, RelationDesc, RelationType};
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
    ) -> Result<Vec<AggregateExpr>, Error> {
        let _ = ctx;
        let mut all_aggr_exprs = vec![];

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

            let aggr_expr = match &m.measure {
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
                    .await?
                }
                None => {
                    return not_impl_err!("Aggregate without aggregate function is not supported")
                }
            };

            all_aggr_exprs.extend(aggr_expr);
        }

        Ok(all_aggr_exprs)
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
    ) -> Result<Vec<AggregateExpr>, Error> {
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
                Ok(exprs)
            }
            None => not_impl_err!(
                "Aggregated function not found: function anchor = {:?}",
                f.function_reference
            ),
        }
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
/// for now, it will found the first column that is timestamp or has a tumble window floor function
fn find_time_index_in_group_exprs(group_exprs: &[TypedExpr]) -> Option<usize> {
    group_exprs.iter().position(|expr| {
        matches!(
            &expr.expr,
            ScalarExpr::CallUnary {
                func: UnaryFunc::TumbleWindowFloor { .. },
                expr: _
            }
        ) || expr.typ.scalar_type.is_timestamp()
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

        let group_exprs =
            TypedExpr::from_substrait_agg_grouping(ctx, &agg.groupings, &input.schema, extensions)
                .await?;

        let time_index = find_time_index_in_group_exprs(&group_exprs);

        let mut aggr_exprs = AggregateExpr::from_substrait_agg_measures(
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

            // first append group_expr as key, then aggr_expr as value
            for expr in group_exprs.iter() {
                output_types.push(expr.typ.clone());
                let col_name = match &expr.expr {
                    ScalarExpr::Column(col) => input.schema.get_name(*col).clone(),
                    // TODO(discord9): impl& use ScalarExpr.display_name, which recursively build expr's name
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

        return Ok(TypedPlan {
            schema: output_type,
            plan,
        });
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use bytes::BytesMut;
    use common_time::{IntervalMonthDayNano, Timestamp};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::value::Value;
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::expr::{BinaryFunc, DfScalarFunction, GlobalId, RawDfScalarFn};
    use crate::plan::{Plan, TypedPlan};
    use crate::repr::{ColumnType, RelationType};
    use crate::transform::test::{create_test_ctx, create_test_query_engine, sql_to_substrait};
    use crate::transform::CDT;

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
            func: AggregateFunc::SumUInt64,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![
                ColumnType::new(CDT::uint64_datatype(), true), // sum(number)
                ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window start
                ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window end
            ])
            .with_key(vec![2])
            .with_time_index(Some(1))
            .into_named(vec![
                Some("SUM(abs(numbers_with_ts.number))".to_string()),
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
                                    ColumnType::new(
                                        ConcreteDataType::timestamp_millisecond_datatype(),
                                        false,
                                    ),
                                ])
                                .into_named(vec![
                                    Some("number".to_string()),
                                    Some("ts".to_string()),
                                ]),
                            )
                            .mfp(MapFilterProject::new(2).into_safe())
                            .unwrap(),
                        ),
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(2)
                                .map(vec![
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowFloor {
                                            window_size: Duration::from_nanos(1_000_000_000),
                                            start_time: Some(Timestamp::new_millisecond(
                                                1625097600000,
                                            )),
                                        },
                                    ),
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowCeiling {
                                            window_size: Duration::from_nanos(1_000_000_000),
                                            start_time: Some(Timestamp::new_millisecond(
                                                1625097600000,
                                            )),
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
                                                b"\x08\x02\"\x08\x1a\x06\x12\x04\n\x02\x12\0"
                                                    .as_ref(),
                                            ),
                                            input_schema: RelationType::new(vec![ColumnType::new(
                                                ConcreteDataType::uint32_datatype(),
                                                false,
                                            )])
                                            .into_unnamed(),
                                            extensions: FunctionExtensions {
                                                anchor_to_name: BTreeMap::from([
                                                    (0, "tumble_start".to_string()),
                                                    (1, "tumble_end".to_string()),
                                                    (2, "abs".to_string()),
                                                    (3, "sum".to_string()),
                                                ]),
                                            },
                                        },
                                    )
                                    .await
                                    .unwrap(),
                                    exprs: vec![ScalarExpr::Column(0)],
                                }
                                .cast(CDT::uint64_datatype())])
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
                            ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window start
                            ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window end
                            ColumnType::new(CDT::uint64_datatype(), true), //sum(number)
                        ])
                        .with_key(vec![1])
                        .with_time_index(Some(0))
                        .into_unnamed(),
                    ),
                ),
                mfp: MapFilterProject::new(3)
                    .map(vec![
                        ScalarExpr::Column(2),
                        ScalarExpr::Column(0),
                        ScalarExpr::Column(1),
                    ])
                    .unwrap()
                    .project(vec![3, 4, 5])
                    .unwrap(),
            },
        };
        assert_eq!(flow_plan, expected);
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
            func: AggregateFunc::SumUInt64,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![
                ColumnType::new(CDT::uint64_datatype(), true), // sum(number)
                ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window start
                ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window end
            ])
            .with_key(vec![2])
            .with_time_index(Some(1))
            .into_named(vec![
                Some("abs(SUM(numbers_with_ts.number))".to_string()),
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
                                    ColumnType::new(
                                        ConcreteDataType::timestamp_millisecond_datatype(),
                                        false,
                                    ),
                                ])
                                .into_named(vec![
                                    Some("number".to_string()),
                                    Some("ts".to_string()),
                                ]),
                            )
                            .mfp(MapFilterProject::new(2).into_safe())
                            .unwrap(),
                        ),
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(2)
                                .map(vec![
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowFloor {
                                            window_size: Duration::from_nanos(1_000_000_000),
                                            start_time: Some(Timestamp::new_millisecond(
                                                1625097600000,
                                            )),
                                        },
                                    ),
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowCeiling {
                                            window_size: Duration::from_nanos(1_000_000_000),
                                            start_time: Some(Timestamp::new_millisecond(
                                                1625097600000,
                                            )),
                                        },
                                    ),
                                ])
                                .unwrap()
                                .project(vec![2, 3])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(2)
                                .map(vec![ScalarExpr::Column(0).cast(CDT::uint64_datatype())])
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
                            ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window start
                            ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window end
                            ColumnType::new(CDT::uint64_datatype(), true), //sum(number)
                        ])
                        .with_key(vec![1])
                        .with_time_index(Some(0))
                        .into_named(vec![None, None, None]),
                    ),
                ),
                mfp: MapFilterProject::new(3)
                    .map(vec![
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
                                        (1, "tumble_start".to_string()),
                                        (2, "tumble_end".to_string()),
                                        (3, "sum".to_string()),
                                    ]),
                                },
                            })
                            .await
                            .unwrap(),
                            exprs: vec![ScalarExpr::Column(2)],
                        },
                        ScalarExpr::Column(0),
                        ScalarExpr::Column(1),
                    ])
                    .unwrap()
                    .project(vec![3, 4, 5])
                    .unwrap(),
            },
        };
        assert_eq!(flow_plan, expected);
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
                func: AggregateFunc::SumUInt64,
                expr: ScalarExpr::Column(0),
                distinct: false,
            },
            AggregateExpr {
                func: AggregateFunc::Count,
                expr: ScalarExpr::Column(1),
                distinct: false,
            },
        ];
        let avg_expr = ScalarExpr::If {
            cond: Box::new(ScalarExpr::Column(4).call_binary(
                ScalarExpr::Literal(Value::from(0i64), CDT::int64_datatype()),
                BinaryFunc::NotEq,
            )),
            then: Box::new(
                ScalarExpr::Column(3)
                    .cast(CDT::float64_datatype())
                    .call_binary(
                        ScalarExpr::Column(4).cast(CDT::float64_datatype()),
                        BinaryFunc::DivFloat64,
                    ),
            ),
            els: Box::new(ScalarExpr::Literal(Value::Null, CDT::float64_datatype())),
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
                                    ColumnType::new(
                                        ConcreteDataType::timestamp_millisecond_datatype(),
                                        false,
                                    ),
                                ])
                                .into_named(vec![
                                    Some("number".to_string()),
                                    Some("ts".to_string()),
                                ]),
                            )
                            .mfp(MapFilterProject::new(2).into_safe())
                            .unwrap(),
                        ),
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(2)
                                .map(vec![
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowFloor {
                                            window_size: Duration::from_nanos(3_600_000_000_000),
                                            start_time: None,
                                        },
                                    ),
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowCeiling {
                                            window_size: Duration::from_nanos(3_600_000_000_000),
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
                                .map(vec![
                                    ScalarExpr::Column(0).cast(CDT::uint64_datatype()),
                                    ScalarExpr::Column(0),
                                ])
                                .unwrap()
                                .project(vec![2, 3])
                                .unwrap()
                                .into_safe(),
                        },
                        reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                            full_aggrs: aggr_exprs.clone(),
                            simple_aggrs: vec![
                                AggrWithIndex::new(aggr_exprs[0].clone(), 0, 0),
                                AggrWithIndex::new(aggr_exprs[1].clone(), 1, 1),
                            ],
                            distinct_aggrs: vec![],
                        }),
                    }
                    .with_types(
                        RelationType::new(vec![
                            // keys
                            ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window start(time index)
                            ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window end(pk)
                            ColumnType::new(CDT::uint32_datatype(), false), // number(pk)
                            // values
                            ColumnType::new(CDT::uint64_datatype(), true), // avg.sum(number)
                            ColumnType::new(CDT::int64_datatype(), true),  // avg.count(number)
                        ])
                        .with_key(vec![1, 2])
                        .with_time_index(Some(0))
                        .into_named(vec![
                            None,
                            None,
                            Some("number".to_string()),
                            None,
                            None,
                        ]),
                    ),
                ),
                mfp: MapFilterProject::new(5)
                    .map(vec![
                        ScalarExpr::Column(2), // number(pk)
                        avg_expr,
                        ScalarExpr::Column(0), // window start
                        ScalarExpr::Column(1), // window end
                    ])
                    .unwrap()
                    .project(vec![5, 6, 7, 8])
                    .unwrap(),
            },
            schema: RelationType::new(vec![
                ColumnType::new(CDT::uint32_datatype(), false), // number
                ColumnType::new(CDT::float64_datatype(), true), // avg(number)
                ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window start
                ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window end
            ])
            .with_key(vec![0, 3])
            .with_time_index(Some(2))
            .into_named(vec![
                Some("numbers_with_ts.number".to_string()),
                Some("AVG(numbers_with_ts.number)".to_string()),
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
            func: AggregateFunc::SumUInt64,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![
                ColumnType::new(CDT::uint64_datatype(), true), // sum(number)
                ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window start
                ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window end
            ])
            .with_key(vec![2])
            .with_time_index(Some(1))
            .into_named(vec![
                Some("SUM(numbers_with_ts.number)".to_string()),
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
                                    ColumnType::new(
                                        ConcreteDataType::timestamp_millisecond_datatype(),
                                        false,
                                    ),
                                ])
                                .into_named(vec![
                                    Some("number".to_string()),
                                    Some("ts".to_string()),
                                ]),
                            )
                            .mfp(MapFilterProject::new(2).into_safe())
                            .unwrap(),
                        ),
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(2)
                                .map(vec![
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowFloor {
                                            window_size: Duration::from_nanos(3_600_000_000_000),
                                            start_time: None,
                                        },
                                    ),
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowCeiling {
                                            window_size: Duration::from_nanos(3_600_000_000_000),
                                            start_time: None,
                                        },
                                    ),
                                ])
                                .unwrap()
                                .project(vec![2, 3])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(2)
                                .map(vec![ScalarExpr::Column(0).cast(CDT::uint64_datatype())])
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
                            ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window start
                            ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window end
                            ColumnType::new(CDT::uint64_datatype(), true), //sum(number)
                        ])
                        .with_key(vec![1])
                        .with_time_index(Some(0))
                        .into_named(vec![None, None, None]),
                    ),
                ),
                mfp: MapFilterProject::new(3)
                    .map(vec![
                        ScalarExpr::Column(2),
                        ScalarExpr::Column(0),
                        ScalarExpr::Column(1),
                    ])
                    .unwrap()
                    .project(vec![3, 4, 5])
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
            func: AggregateFunc::SumUInt64,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![
                ColumnType::new(CDT::uint64_datatype(), true), // sum(number)
                ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window start
                ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window end
            ])
            .with_key(vec![2])
            .with_time_index(Some(1))
            .into_named(vec![
                Some("SUM(numbers_with_ts.number)".to_string()),
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
                                    ColumnType::new(
                                        ConcreteDataType::timestamp_millisecond_datatype(),
                                        false,
                                    ),
                                ])
                                .into_named(vec![
                                    Some("number".to_string()),
                                    Some("ts".to_string()),
                                ]),
                            )
                            .mfp(MapFilterProject::new(2).into_safe())
                            .unwrap(),
                        ),
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(2)
                                .map(vec![
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowFloor {
                                            window_size: Duration::from_nanos(3_600_000_000_000),
                                            start_time: Some(Timestamp::new_millisecond(
                                                1625097600000,
                                            )),
                                        },
                                    ),
                                    ScalarExpr::Column(1).call_unary(
                                        UnaryFunc::TumbleWindowCeiling {
                                            window_size: Duration::from_nanos(3_600_000_000_000),
                                            start_time: Some(Timestamp::new_millisecond(
                                                1625097600000,
                                            )),
                                        },
                                    ),
                                ])
                                .unwrap()
                                .project(vec![2, 3])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(2)
                                .map(vec![ScalarExpr::Column(0).cast(CDT::uint64_datatype())])
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
                            ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window start
                            ColumnType::new(CDT::timestamp_millisecond_datatype(), true), // window end
                            ColumnType::new(CDT::uint64_datatype(), true), //sum(number)
                        ])
                        .with_key(vec![1])
                        .with_time_index(Some(0))
                        .into_unnamed(),
                    ),
                ),
                mfp: MapFilterProject::new(3)
                    .map(vec![
                        ScalarExpr::Column(2),
                        ScalarExpr::Column(0),
                        ScalarExpr::Column(1),
                    ])
                    .unwrap()
                    .project(vec![3, 4, 5])
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
                func: AggregateFunc::SumUInt64,
                expr: ScalarExpr::Column(0),
                distinct: false,
            },
            AggregateExpr {
                func: AggregateFunc::Count,
                expr: ScalarExpr::Column(1),
                distinct: false,
            },
        ];
        let avg_expr = ScalarExpr::If {
            cond: Box::new(ScalarExpr::Column(2).call_binary(
                ScalarExpr::Literal(Value::from(0i64), CDT::int64_datatype()),
                BinaryFunc::NotEq,
            )),
            then: Box::new(
                ScalarExpr::Column(1)
                    .cast(CDT::float64_datatype())
                    .call_binary(
                        ScalarExpr::Column(2).cast(CDT::float64_datatype()),
                        BinaryFunc::DivFloat64,
                    ),
            ),
            els: Box::new(ScalarExpr::Literal(Value::Null, CDT::float64_datatype())),
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![
                ColumnType::new(CDT::float64_datatype(), true), // avg(number: u32) -> f64
                ColumnType::new(CDT::uint32_datatype(), false), // number
            ])
            .with_key(vec![1])
            .into_named(vec![
                Some("AVG(numbers.number)".to_string()),
                Some("numbers.number".to_string()),
            ]),
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
                            )
                            .mfp(
                                MapFilterProject::new(1)
                                    .project(vec![0])
                                    .unwrap()
                                    .into_safe(),
                            )
                            .unwrap(),
                        ),
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(1)
                                .map(vec![ScalarExpr::Column(0)])
                                .unwrap()
                                .project(vec![1])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(1)
                                .map(vec![
                                    ScalarExpr::Column(0).cast(CDT::uint64_datatype()),
                                    ScalarExpr::Column(0),
                                ])
                                .unwrap()
                                .project(vec![1, 2])
                                .unwrap()
                                .into_safe(),
                        },
                        reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                            full_aggrs: aggr_exprs.clone(),
                            simple_aggrs: vec![
                                AggrWithIndex::new(aggr_exprs[0].clone(), 0, 0),
                                AggrWithIndex::new(aggr_exprs[1].clone(), 1, 1),
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
                        ScalarExpr::Column(0),
                        // TODO(discord9): optimize mfp so to remove indirect ref
                    ])
                    .unwrap()
                    .project(vec![3, 4])
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
                func: AggregateFunc::SumUInt64,
                expr: ScalarExpr::Column(0),
                distinct: false,
            },
            AggregateExpr {
                func: AggregateFunc::Count,
                expr: ScalarExpr::Column(1),
                distinct: false,
            },
        ];
        let avg_expr = ScalarExpr::If {
            cond: Box::new(ScalarExpr::Column(1).call_binary(
                ScalarExpr::Literal(Value::from(0i64), CDT::int64_datatype()),
                BinaryFunc::NotEq,
            )),
            then: Box::new(
                ScalarExpr::Column(0)
                    .cast(CDT::float64_datatype())
                    .call_binary(
                        ScalarExpr::Column(1).cast(CDT::float64_datatype()),
                        BinaryFunc::DivFloat64,
                    ),
            ),
            els: Box::new(ScalarExpr::Literal(Value::Null, CDT::float64_datatype())),
        };
        let input = Box::new(
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
        );
        let expected = TypedPlan {
            schema: RelationType::new(vec![ColumnType::new(CDT::float64_datatype(), true)])
                .into_named(vec![Some("AVG(numbers.number)".to_string())]),
            plan: Plan::Mfp {
                input: Box::new(
                    Plan::Reduce {
                        input: Box::new(
                            Plan::Mfp {
                                input: input.clone(),
                                mfp: MapFilterProject::new(1).project(vec![0]).unwrap(),
                            }
                            .with_types(
                                RelationType::new(vec![ColumnType::new(
                                    CDT::uint32_datatype(),
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
                                .map(vec![
                                    ScalarExpr::Column(0).cast(CDT::uint64_datatype()),
                                    ScalarExpr::Column(0),
                                ])
                                .unwrap()
                                .project(vec![1, 2])
                                .unwrap()
                                .into_safe(),
                        },
                        reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                            full_aggrs: aggr_exprs.clone(),
                            simple_aggrs: vec![
                                AggrWithIndex::new(aggr_exprs[0].clone(), 0, 0),
                                AggrWithIndex::new(aggr_exprs[1].clone(), 1, 1),
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
                    ])
                    .unwrap()
                    .project(vec![2])
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

        let aggr_expr = AggregateExpr {
            func: AggregateFunc::SumUInt64,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![ColumnType::new(CDT::uint64_datatype(), true)])
                .into_named(vec![Some("SUM(numbers.number)".to_string())]),
            plan: Plan::Reduce {
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
                    )
                    .mfp(MapFilterProject::new(1).into_safe())
                    .unwrap(),
                ),
                key_val_plan: KeyValPlan {
                    key_plan: MapFilterProject::new(1)
                        .project(vec![])
                        .unwrap()
                        .into_safe(),
                    val_plan: MapFilterProject::new(1)
                        .map(vec![ScalarExpr::Column(0)
                            .call_unary(UnaryFunc::Cast(CDT::uint64_datatype()))])
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
            func: AggregateFunc::SumUInt64,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![
                ColumnType::new(CDT::uint64_datatype(), true), // col sum(number)
                ColumnType::new(CDT::uint32_datatype(), false), // col number
            ])
            .with_key(vec![1])
            .into_named(vec![
                Some("SUM(numbers.number)".to_string()),
                Some("numbers.number".to_string()),
            ]),
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
                            )
                            .mfp(MapFilterProject::new(1).into_safe())
                            .unwrap(),
                        ),
                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(1)
                                .map(vec![ScalarExpr::Column(0)])
                                .unwrap()
                                .project(vec![1])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(1)
                                .map(vec![ScalarExpr::Column(0)
                                    .call_unary(UnaryFunc::Cast(CDT::uint64_datatype()))])
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
                        RelationType::new(vec![
                            ColumnType::new(CDT::uint32_datatype(), false), // col number
                            ColumnType::new(CDT::uint64_datatype(), true),  // col sum(number)
                        ])
                        .with_key(vec![0])
                        .into_named(vec![Some("number".to_string()), None]),
                    ),
                ),
                mfp: MapFilterProject::new(2)
                    .map(vec![ScalarExpr::Column(1), ScalarExpr::Column(0)])
                    .unwrap()
                    .project(vec![2, 3])
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
            func: AggregateFunc::SumUInt64,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            schema: RelationType::new(vec![ColumnType::new(CDT::uint64_datatype(), true)])
                .into_named(vec![Some(
                    "SUM(numbers.number + numbers.number)".to_string(),
                )]),
            plan: Plan::Reduce {
                input: Box::new(
                    Plan::Mfp {
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
                        mfp: MapFilterProject::new(1),
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
                            .call_binary(ScalarExpr::Column(0), BinaryFunc::AddUInt32)
                            .call_unary(UnaryFunc::Cast(CDT::uint64_datatype()))])
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
            },
        };
        assert_eq!(flow_plan.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_cast_max_min() {
        let engine = create_test_query_engine();
        let sql = "SELECT (max(number) - min(number))/30.0, date_bin(INTERVAL '30 second', CAST(ts AS TimestampMillisecond)) as time_window from numbers_with_ts GROUP BY time_window";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan).await;

        let aggr_exprs = vec![
            AggregateExpr {
                func: AggregateFunc::MaxUInt32,
                expr: ScalarExpr::Column(0),
                distinct: false,
            },
            AggregateExpr {
                func: AggregateFunc::MinUInt32,
                expr: ScalarExpr::Column(0),
                distinct: false,
            },
        ];
        let expected = TypedPlan {
            schema: RelationType::new(vec![
                ColumnType::new(CDT::float64_datatype(), true),
                ColumnType::new(CDT::timestamp_millisecond_datatype(), true),
            ])
            .with_key(vec![1])
            .into_named(vec![
                Some(
                    "MAX(numbers_with_ts.number) - MIN(numbers_with_ts.number) / Float64(30)"
                        .to_string(),
                ),
                Some("time_window".to_string()),
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
                                    ColumnType::new(ConcreteDataType::timestamp_millisecond_datatype(), false),
                                ])
                                .into_named(vec![
                                    Some("number".to_string()),
                                    Some("ts".to_string()),
                                ]),
                            )
                            .mfp(MapFilterProject::new(2).into_safe())
                            .unwrap(),
                        ),

                        key_val_plan: KeyValPlan {
                            key_plan: MapFilterProject::new(2)
                                .map(vec![ScalarExpr::CallDf {
                                    df_scalar_fn: DfScalarFunction::try_from_raw_fn(
                                        RawDfScalarFn {
                                            f: BytesMut::from(
                                                b"\x08\x02\"I\x1aG\nE\x8a\x02?\x08\x03\x12+\n\x17interval-month-day-nano\x12\x10\0\xac#\xfc\x06\0\0\0\0\0\0\0\0\0\0\0\x1a\x06\x12\x04:\x02\x10\x02\x1a\x06\x12\x04:\x02\x10\x02\x98\x03\x03\"\n\x1a\x08\x12\x06\n\x04\x12\x02\x08\x01".as_ref(),
                                            ),
                                            input_schema: RelationType::new(vec![ColumnType::new(
                                                ConcreteDataType::interval_month_day_nano_datatype(),
                                                true,
                                            ),ColumnType::new(
                                                ConcreteDataType::timestamp_millisecond_datatype(),
                                                false,
                                            )])
                                            .into_unnamed(),
                                            extensions: FunctionExtensions {
                                                anchor_to_name: BTreeMap::from([
                                                    (0, "subtract".to_string()),
                                                    (1, "divide".to_string()),
                                                    (2, "date_bin".to_string()),
                                                    (3, "max".to_string()),
                                                    (4, "min".to_string()),
                                                ]),
                                            },
                                        },
                                    )
                                    .await
                                    .unwrap(),
                                    exprs: vec![
                                        ScalarExpr::Literal(
                                            Value::IntervalMonthDayNano(IntervalMonthDayNano::new(0, 0, 30000000000)),
                                            CDT::interval_month_day_nano_datatype()
                                        ),
                                        ScalarExpr::Column(1)
                                        ],
                                }])
                                .unwrap()
                                .project(vec![2])
                                .unwrap()
                                .into_safe(),
                            val_plan: MapFilterProject::new(2)
                                .into_safe(),
                        },
                        reduce_plan: ReducePlan::Accumulable(AccumulablePlan {
                            full_aggrs: aggr_exprs.clone(),
                            simple_aggrs: vec![AggrWithIndex::new(aggr_exprs[0].clone(), 0, 0),
                            AggrWithIndex::new(aggr_exprs[1].clone(), 0, 1)],
                            distinct_aggrs: vec![],
                        }),
                    }
                    .with_types(
                        RelationType::new(vec![
                            ColumnType::new(
                                ConcreteDataType::timestamp_millisecond_datatype(),
                                true,
                            ), // time_window
                            ColumnType::new(ConcreteDataType::uint32_datatype(), true), // max
                            ColumnType::new(ConcreteDataType::uint32_datatype(), true), // min
                        ])
                        .with_key(vec![0])
                        .into_unnamed(),
                    ),
                ),
                mfp: MapFilterProject::new(3)
                    .map(vec![
                        ScalarExpr::Column(1)
                            .call_binary(ScalarExpr::Column(2), BinaryFunc::SubUInt32)
                            .cast(CDT::float64_datatype())
                            .call_binary(
                                ScalarExpr::Literal(Value::from(30.0f64), CDT::float64_datatype()),
                                BinaryFunc::DivFloat64,
                            ),
                        ScalarExpr::Column(0),
                    ])
                    .unwrap()
                    .project(vec![3, 4])
                    .unwrap(),
            },
        };

        assert_eq!(flow_plan.unwrap(), expected);
    }
}
