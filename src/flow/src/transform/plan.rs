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

use std::collections::BTreeMap;

use itertools::Itertools;
use snafu::OptionExt;
use substrait_proto::proto::expression::MaskExpression;
use substrait_proto::proto::read_rel::ReadType;
use substrait_proto::proto::rel::RelType;
use substrait_proto::proto::{plan_rel, Plan as SubPlan, Rel};

use crate::adapter::error::{
    Error, InvalidQuerySnafu, NotImplementedSnafu, PlanSnafu, UnexpectedSnafu,
};
use crate::expr::{MapFilterProject, ScalarExpr, TypedExpr, UnaryFunc};
use crate::plan::{KeyValPlan, Plan, ReducePlan, TypedPlan};
use crate::repr::{self, RelationType};
use crate::transform::{substrait_proto, FlownodeContext, FunctionExtensions};

impl TypedPlan {
    /// Convert Substrait Plan into Flow's TypedPlan
    pub fn from_substrait_plan(
        ctx: &mut FlownodeContext,
        plan: &SubPlan,
    ) -> Result<TypedPlan, Error> {
        // Register function extension
        let function_extension = FunctionExtensions::try_from_proto(&plan.extensions)?;

        // Parse relations
        match plan.relations.len() {
        1 => {
            match plan.relations[0].rel_type.as_ref() {
                Some(rt) => match rt {
                    plan_rel::RelType::Rel(rel) => {
                        Ok(TypedPlan::from_substrait_rel(ctx, rel, &function_extension)?)
                    },
                    plan_rel::RelType::Root(root) => {
                        let input = root.input.as_ref().with_context(|| InvalidQuerySnafu {
                            reason: "Root relation without input",
                        })?;
                        Ok(TypedPlan::from_substrait_rel(ctx, input, &function_extension)?)
                    }
                },
                None => plan_err!("Cannot parse plan relation: None")
            }
        },
        _ => not_impl_err!(
            "Substrait plan with more than 1 relation trees not supported. Number of relation trees: {:?}",
            plan.relations.len()
        )
    }
    }

    /// Convert Substrait Rel into Flow's TypedPlan
    /// TODO: SELECT DISTINCT(does it get compile with something else?)
    pub fn from_substrait_rel(
        ctx: &mut FlownodeContext,
        rel: &Rel,
        extensions: &FunctionExtensions,
    ) -> Result<TypedPlan, Error> {
        match &rel.rel_type {
            Some(RelType::Project(p)) => {
                let input = if let Some(input) = p.input.as_ref() {
                    TypedPlan::from_substrait_rel(ctx, input, extensions)?
                } else {
                    return not_impl_err!("Projection without an input is not supported");
                };

                let mut exprs: Vec<TypedExpr> = vec![];
                for e in &p.expressions {
                    let expr = TypedExpr::from_substrait_rex(e, &input.typ, extensions)?;
                    exprs.push(expr);
                }
                let is_literal = exprs.iter().all(|expr| expr.expr.is_literal());
                if is_literal {
                    let (literals, lit_types): (Vec<_>, Vec<_>) = exprs
                        .into_iter()
                        .map(|TypedExpr { expr, typ }| (expr, typ))
                        .unzip();
                    let typ = RelationType::new(lit_types);
                    let row = literals
                        .into_iter()
                        .map(|lit| lit.as_literal().expect("A literal"))
                        .collect_vec();
                    let row = repr::Row::new(row);
                    let plan = Plan::Constant {
                        rows: vec![(row, repr::Timestamp::MIN, 1)],
                    };
                    Ok(TypedPlan { typ, plan })
                } else {
                    /// if reduce_plan contains the special function like tumble floor/ceiling, add them to the proj_exprs
                    fn rewrite_projection_after_reduce(
                        key_val_plan: KeyValPlan,
                        _reduce_plan: ReducePlan,
                        reduce_output_type: &RelationType,
                        proj_exprs: &mut Vec<TypedExpr>,
                    ) -> Result<(), Error> {
                        // TODO: get keys correctly
                        let key_exprs = key_val_plan
                            .key_plan
                            .projection
                            .clone()
                            .into_iter()
                            .map(|i| {
                                if i < key_val_plan.key_plan.input_arity {
                                    ScalarExpr::Column(i)
                                } else {
                                    key_val_plan.key_plan.expressions
                                        [i - key_val_plan.key_plan.input_arity]
                                        .clone()
                                }
                            })
                            .collect_vec();
                        let mut shift_offset = 0;
                        let special_keys = key_exprs
                            .into_iter()
                            .enumerate()
                            .filter(|(_idx, p)| {
                                if matches!(
                                    p,
                                    ScalarExpr::CallUnary {
                                        func: UnaryFunc::TumbleWindowFloor { .. },
                                        ..
                                    } | ScalarExpr::CallUnary {
                                        func: UnaryFunc::TumbleWindowCeiling { .. },
                                        ..
                                    }
                                ) {
                                    if matches!(
                                        p,
                                        ScalarExpr::CallUnary {
                                            func: UnaryFunc::TumbleWindowFloor { .. },
                                            ..
                                        }
                                    ) {
                                        shift_offset += 1;
                                    }
                                    true
                                } else {
                                    false
                                }
                            })
                            .collect_vec();
                        let spec_key_arity = special_keys.len();
                        if spec_key_arity == 0 {
                            return Ok(());
                        }

                        {
                            // shift proj_exprs to the right by spec_key_arity
                            let max_used_col_in_proj = proj_exprs
                                .iter()
                                .map(|expr| {
                                    expr.expr
                                        .get_all_ref_columns()
                                        .into_iter()
                                        .max()
                                        .unwrap_or_default()
                                })
                                .max()
                                .unwrap_or_default();

                            let shuffle = (0..=max_used_col_in_proj)
                                .map(|col| (col, col + shift_offset))
                                .collect::<BTreeMap<_, _>>();
                            for proj_expr in proj_exprs.iter_mut() {
                                proj_expr.expr.permute_map(&shuffle)?;
                            } // add key to the end
                            for (key_idx, _key_expr) in special_keys {
                                // here we assume the output type of reduce operator is just first keys columns, then append value columns
                                proj_exprs.push(
                                    ScalarExpr::Column(key_idx).with_type(
                                        reduce_output_type.column_types[key_idx].clone(),
                                    ),
                                );
                            }
                        }

                        Ok(())
                    }

                    match input.plan.clone() {
                        Plan::Reduce {
                            key_val_plan,
                            reduce_plan,
                            ..
                        } => {
                            rewrite_projection_after_reduce(
                                key_val_plan,
                                reduce_plan,
                                &input.typ,
                                &mut exprs,
                            )?;
                        }
                        Plan::Mfp { input, mfp: _ } => {
                            if let Plan::Reduce {
                                key_val_plan,
                                reduce_plan,
                                ..
                            } = input.plan
                            {
                                rewrite_projection_after_reduce(
                                    key_val_plan,
                                    reduce_plan,
                                    &input.typ,
                                    &mut exprs,
                                )?;
                            }
                        }
                        _ => (),
                    }
                    input.projection(exprs)
                }
            }
            Some(RelType::Filter(filter)) => {
                let input = if let Some(input) = filter.input.as_ref() {
                    TypedPlan::from_substrait_rel(ctx, input, extensions)?
                } else {
                    return not_impl_err!("Filter without an input is not supported");
                };

                let expr = if let Some(condition) = filter.condition.as_ref() {
                    TypedExpr::from_substrait_rex(condition, &input.typ, extensions)?
                } else {
                    return not_impl_err!("Filter without an condition is not valid");
                };
                input.filter(expr)
            }
            Some(RelType::Read(read)) => {
                if let Some(ReadType::NamedTable(nt)) = &read.as_ref().read_type {
                    let query_ctx = ctx.query_context.clone().context(UnexpectedSnafu {
                        reason: "Query context not found",
                    })?;
                    let table_reference = match nt.names.len() {
                        1 => [
                            query_ctx.current_catalog().to_string(),
                            query_ctx.current_schema().to_string(),
                            nt.names[0].clone(),
                        ],
                        2 => [
                            query_ctx.current_catalog().to_string(),
                            nt.names[0].clone(),
                            nt.names[1].clone(),
                        ],
                        3 => [
                            nt.names[0].clone(),
                            nt.names[1].clone(),
                            nt.names[2].clone(),
                        ],
                        _ => InvalidQuerySnafu {
                            reason: "Expect table to have name",
                        }
                        .fail()?,
                    };
                    let table = ctx.table(&table_reference)?;
                    let get_table = Plan::Get {
                        id: crate::expr::Id::Global(table.0),
                    };
                    let get_table = TypedPlan {
                        typ: table.1.typ().clone(),
                        plan: get_table,
                    };

                    if let Some(MaskExpression {
                        select: Some(projection),
                        ..
                    }) = &read.projection
                    {
                        let column_indices: Vec<usize> = projection
                            .struct_items
                            .iter()
                            .map(|item| item.field as usize)
                            .collect();
                        let input_arity = get_table.typ.column_types.len();
                        let mfp =
                            MapFilterProject::new(input_arity).project(column_indices.clone())?;
                        get_table.mfp(mfp.into_safe())
                    } else {
                        Ok(get_table)
                    }
                } else {
                    not_impl_err!("Only NamedTable reads are supported")
                }
            }
            Some(RelType::Aggregate(agg)) => {
                TypedPlan::from_substrait_agg_rel(ctx, agg, extensions)
            }
            _ => not_impl_err!("Unsupported relation type: {:?}", rel.rel_type),
        }
    }
}

#[cfg(test)]
mod test {
    use datatypes::prelude::ConcreteDataType;

    use super::*;
    use crate::expr::{GlobalId, ScalarExpr};
    use crate::plan::{Plan, TypedPlan};
    use crate::repr::{self, ColumnType, RelationType};
    use crate::transform::test::{create_test_ctx, create_test_query_engine, sql_to_substrait};
    use crate::transform::CDT;

    #[tokio::test]
    async fn test_select() {
        let engine = create_test_query_engine();
        let sql = "SELECT number FROM numbers";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan);

        let expected = TypedPlan {
            typ: RelationType::new(vec![ColumnType::new(CDT::uint32_datatype(), false)]),
            plan: Plan::Mfp {
                input: Box::new(
                    Plan::Get {
                        id: crate::expr::Id::Global(GlobalId::User(0)),
                    }
                    .with_types(RelationType::new(vec![ColumnType::new(
                        ConcreteDataType::uint32_datatype(),
                        false,
                    )])),
                ),
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
