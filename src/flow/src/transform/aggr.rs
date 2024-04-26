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

use common_decimal::Decimal128;
use common_time::{Date, Timestamp};
use datafusion_substrait::variation_const::{
    DATE_32_TYPE_REF, DATE_64_TYPE_REF, DEFAULT_TYPE_REF, TIMESTAMP_MICRO_TYPE_REF,
    TIMESTAMP_MILLI_TYPE_REF, TIMESTAMP_NANO_TYPE_REF, TIMESTAMP_SECOND_TYPE_REF,
    UNSIGNED_INTEGER_TYPE_REF,
};
use datatypes::arrow::compute::kernels::window;
use datatypes::arrow::ipc::Binary;
use datatypes::data_type::ConcreteDataType as CDT;
use datatypes::value::Value;
use hydroflow::futures::future::Map;
use itertools::Itertools;
use snafu::{OptionExt, ResultExt};
use substrait::substrait_proto::proto::aggregate_function::AggregationInvocation;
use substrait::substrait_proto::proto::aggregate_rel::{Grouping, Measure};
use substrait::substrait_proto::proto::expression::field_reference::ReferenceType::DirectReference;
use substrait::substrait_proto::proto::expression::literal::LiteralType;
use substrait::substrait_proto::proto::expression::reference_segment::ReferenceType::StructField;
use substrait::substrait_proto::proto::expression::{
    IfThen, Literal, MaskExpression, RexType, ScalarFunction,
};
use substrait::substrait_proto::proto::extensions::simple_extension_declaration::MappingType;
use substrait::substrait_proto::proto::extensions::SimpleExtensionDeclaration;
use substrait::substrait_proto::proto::function_argument::ArgType;
use substrait::substrait_proto::proto::r#type::Kind;
use substrait::substrait_proto::proto::read_rel::ReadType;
use substrait::substrait_proto::proto::rel::RelType;
use substrait::substrait_proto::proto::{self, plan_rel, Expression, Plan as SubPlan, Rel};

use crate::adapter::error::{
    DatatypesSnafu, Error, EvalSnafu, InvalidQuerySnafu, NotImplementedSnafu, PlanSnafu,
    TableNotFoundSnafu,
};
use crate::expr::{
    AggregateExpr, AggregateFunc, BinaryFunc, GlobalId, MapFilterProject, SafeMfpPlan, ScalarExpr,
    TypedExpr, UnaryFunc, UnmaterializableFunc, VariadicFunc,
};
use crate::plan::{AccumulablePlan, AggrWithIndex, KeyValPlan, Plan, ReducePlan, TypedPlan};
use crate::repr::{self, ColumnType, RelationType};
use crate::transform::{FlowNodeContext, FunctionExtensions};

impl TypedExpr {
    fn from_substrait_agg_grouping(
        ctx: &mut FlowNodeContext,
        groupings: &[Grouping],
        typ: &RelationType,
        extensions: &FunctionExtensions,
    ) -> Result<Vec<TypedExpr>, Error> {
        let _ = ctx;
        let mut group_expr = vec![];
        match groupings.len() {
            1 => {
                for e in &groupings[0].grouping_expressions {
                    let x = TypedExpr::from_substrait_rex(e, typ, extensions)?;
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
    fn from_substrait_agg_measures(
        ctx: &mut FlowNodeContext,
        measures: &[Measure],
        typ: &RelationType,
        extensions: &FunctionExtensions,
    ) -> Result<Vec<AggregateExpr>, Error> {
        let _ = ctx;
        let mut aggr_exprs = vec![];

        for m in measures {
            let filter = &m
                .filter
                .as_ref()
                .map(|fil| TypedExpr::from_substrait_rex(fil, typ, extensions))
                .transpose()?;

            let agg_func = match &m.measure {
                Some(f) => {
                    let distinct = match f.invocation {
                        _ if f.invocation == AggregationInvocation::Distinct as i32 => true,
                        _ if f.invocation == AggregationInvocation::All as i32 => false,
                        _ => false,
                    };
                    AggregateExpr::from_substrait_agg_func(
                        f, typ, extensions, filter, // TODO(discord9): impl order_by
                        &None, distinct,
                    )
                }
                None => not_impl_err!("Aggregate without aggregate function is not supported"),
            }?;
            aggr_exprs.push(agg_func);
        }
        Ok(aggr_exprs)
    }

    /// Convert AggregateFunction into Flow's AggregateExpr
    pub fn from_substrait_agg_func(
        f: &proto::AggregateFunction,
        input_schema: &RelationType,
        extensions: &FunctionExtensions,
        filter: &Option<TypedExpr>,
        order_by: &Option<Vec<TypedExpr>>,
        distinct: bool,
    ) -> Result<AggregateExpr, Error> {
        // TODO(discord9): impl filter
        let _ = filter;
        let _ = order_by;
        let mut args = vec![];
        for arg in &f.arguments {
            let arg_expr = match &arg.arg_type {
                Some(ArgType::Value(e)) => {
                    TypedExpr::from_substrait_rex(e, input_schema, extensions)
                }
                _ => not_impl_err!("Aggregated function argument non-Value type not supported"),
            }?;
            args.push(arg_expr);
        }

        let arg = if let Some(first) = args.first() {
            first
        } else {
            return not_impl_err!("Aggregated function without arguments is not supported");
        };

        let func = match extensions.get(&f.function_reference) {
            Some(function_name) => {
                AggregateFunc::from_str_and_type(function_name, Some(arg.typ.scalar_type.clone()))
            }
            None => not_impl_err!(
                "Aggregated function not found: function anchor = {:?}",
                f.function_reference
            ),
        }?;
        Ok(AggregateExpr {
            func,
            expr: arg.expr.clone(),
            distinct,
        })
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

impl TypedPlan {
    /// Convert AggregateRel into Flow's TypedPlan
    pub fn from_substrait_agg_rel(
        ctx: &mut FlowNodeContext,
        agg: &proto::AggregateRel,
        extensions: &FunctionExtensions,
    ) -> Result<TypedPlan, Error> {
        let input = if let Some(input) = agg.input.as_ref() {
            TypedPlan::from_substrait_rel(ctx, input, extensions)?
        } else {
            return not_impl_err!("Aggregate without an input is not supported");
        };

        let group_expr =
            TypedExpr::from_substrait_agg_grouping(ctx, &agg.groupings, &input.typ, extensions)?;

        let mut aggr_exprs =
            AggregateExpr::from_substrait_agg_measures(ctx, &agg.measures, &input.typ, extensions)?;

        let key_val_plan = KeyValPlan::from_substrait_gen_key_val_plan(
            &mut aggr_exprs,
            &group_expr,
            input.typ.column_types.len(),
        )?;

        let output_type = {
            let mut output_types = Vec::new();
            // first append group_expr as key, then aggr_expr as value
            for expr in &group_expr {
                output_types.push(expr.typ.clone());
            }

            for aggr in &aggr_exprs {
                output_types.push(ColumnType::new_nullable(
                    aggr.func.signature().output.clone(),
                ));
            }
            RelationType::new(output_types)
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
            input: Box::new(input.plan),
            key_val_plan,
            reduce_plan: ReducePlan::Accumulable(accum_plan),
        };
        Ok(TypedPlan {
            typ: output_type,
            plan,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::plan::{Plan, TypedPlan};
    use crate::repr::{self, ColumnType, RelationType};
    use crate::transform::test::{create_test_ctx, create_test_query_engine, sql_to_substrait};

    #[tokio::test]
    async fn test_sum() {
        let engine = create_test_query_engine();
        let sql = "SELECT sum(number) FROM numbers";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan);

        let aggr_expr = AggregateExpr {
            func: AggregateFunc::SumUInt32,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            typ: RelationType::new(vec![ColumnType::new(CDT::uint32_datatype(), true)]),
            plan: Plan::Mfp {
                input: Box::new(Plan::Reduce {
                    input: Box::new(Plan::Get {
                        id: crate::expr::Id::Global(GlobalId::User(0)),
                    }),
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
                }),
                mfp: MapFilterProject::new(1)
                    .map(vec![ScalarExpr::Column(0)])
                    .unwrap()
                    .project(vec![1])
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
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan).unwrap();

        let aggr_expr = AggregateExpr {
            func: AggregateFunc::SumUInt32,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            typ: RelationType::new(vec![
                ColumnType::new(CDT::uint32_datatype(), true),
                ColumnType::new(CDT::uint32_datatype(), false),
            ]),
            plan: Plan::Mfp {
                input: Box::new(Plan::Reduce {
                    input: Box::new(Plan::Get {
                        id: crate::expr::Id::Global(GlobalId::User(0)),
                    }),
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
                }),
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
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan);

        let aggr_expr = AggregateExpr {
            func: AggregateFunc::SumUInt32,
            expr: ScalarExpr::Column(0),
            distinct: false,
        };
        let expected = TypedPlan {
            typ: RelationType::new(vec![ColumnType::new(CDT::uint32_datatype(), true)]),
            plan: Plan::Mfp {
                input: Box::new(Plan::Reduce {
                    input: Box::new(Plan::Get {
                        id: crate::expr::Id::Global(GlobalId::User(0)),
                    }),
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
                }),
                mfp: MapFilterProject::new(1)
                    .map(vec![ScalarExpr::Column(0)])
                    .unwrap()
                    .project(vec![1])
                    .unwrap(),
            },
        };
        assert_eq!(flow_plan.unwrap(), expected);
    }
}
