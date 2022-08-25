use std::{result::Result, sync::Arc};

use api::v1::codec::{physical_plan_node::PhysicalPlanType, PhysicalPlanNode, ProjectionExecNode};
use datafusion::physical_plan::{projection::ProjectionExec, ExecutionPlan, PhysicalExpr};
use snafu::{OptionExt, ResultExt};

use crate::server::grpc::physical_plan::{
    error::{
        EmptyGrpcPhysicalPlanSnafu, Error, MissingFieldSnafu, NewProjectionSnafu,
        UnsupportedDfSnafu,
    },
    expr, AsExcutionPlan,
};

impl AsExcutionPlan for PhysicalPlanNode {
    type Error = Error;

    fn try_into_physical_plan(&self) -> Result<Arc<dyn ExecutionPlan>, Self::Error> {
        let plan = self
            .physical_plan_type
            .as_ref()
            .context(EmptyGrpcPhysicalPlanSnafu {
                name: format!("{:?}", self),
            })?;

        // TODO(fys): impl other physical plan type
        match plan {
            PhysicalPlanType::Projection(projection) => {
                let input = if let Some(input) = projection.input.as_ref() {
                    input.as_ref().try_into_physical_plan()?
                } else {
                    MissingFieldSnafu { field: "input" }.fail()?
                };
                let exprs = projection
                    .expr
                    .iter()
                    .zip(projection.expr_name.iter())
                    .map(|(expr, name)| {
                        Ok((expr::parse_grpc_physical_expr(expr)?, name.to_string()))
                    })
                    .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, String)>, Error>>()?;

                let projection =
                    ProjectionExec::try_new(exprs, input).context(NewProjectionSnafu)?;

                Ok(Arc::new(projection))
            }
        }
    }

    fn try_from_physical_plan(plan: Arc<dyn ExecutionPlan>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let any = plan.as_any();

        if let Some(exec) = any.downcast_ref::<ProjectionExec>() {
            let input = PhysicalPlanNode::try_from_physical_plan(exec.input().to_owned())?;

            let expr = exec
                .expr()
                .iter()
                .map(|expr| expr::parse_df_physical_expr(expr.0.clone()))
                .collect::<Result<Vec<_>, Error>>()?;

            let expr_name = exec.expr().iter().map(|expr| expr.1.clone()).collect();

            Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Projection(Box::new(
                    ProjectionExecNode {
                        input: Some(Box::new(input)),
                        expr,
                        expr_name,
                    },
                ))),
            })
        } else {
            UnsupportedDfSnafu {
                name: format!("{:?}", plan),
            }
            .fail()?
        }
    }
}
