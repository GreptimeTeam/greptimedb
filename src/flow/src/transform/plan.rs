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
use substrait::substrait_proto::proto::expression::MaskExpression;
use substrait::substrait_proto::proto::read_rel::ReadType;
use substrait::substrait_proto::proto::rel::RelType;
use substrait::substrait_proto::proto::{plan_rel, Plan as SubPlan, Rel};

use crate::adapter::error::{Error, InvalidQuerySnafu, NotImplementedSnafu, PlanSnafu};
use crate::expr::{MapFilterProject, TypedExpr};
use crate::plan::{Plan, TypedPlan};
use crate::repr::{self, RelationType};
use crate::transform::{FlowNodeContext, FunctionExtensions};

impl TypedPlan {
    /// Convert Substrait Plan into Flow's TypedPlan
    pub fn from_substrait_plan(
        ctx: &mut FlowNodeContext,
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
        ctx: &mut FlowNodeContext,
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
                    let table_reference = nt.names.clone();
                    let table = ctx.table(&table_reference)?;
                    let get_table = Plan::Get {
                        id: crate::expr::Id::Global(table.0),
                    };
                    let get_table = TypedPlan {
                        typ: table.1,
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
                        get_table.mfp(mfp)
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
                input: Box::new(Plan::Get {
                    id: crate::expr::Id::Global(GlobalId::User(0)),
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
