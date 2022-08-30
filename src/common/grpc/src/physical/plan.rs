use std::{ops::Deref, result::Result, sync::Arc};

use api::v1::codec::{
    physical_plan_node::PhysicalPlanType, MockInputExecNode, PhysicalPlanNode, ProjectionExecNode,
};
use arrow::{
    array::{PrimitiveArray, Utf8Array},
    datatypes::{DataType, Field, Schema},
};
use async_trait::async_trait;
use datafusion::{
    execution::runtime_env::RuntimeEnv,
    field_util::SchemaExt,
    physical_plan::{
        memory::MemoryStream, projection::ProjectionExec, ExecutionPlan, PhysicalExpr,
        SendableRecordBatchStream, Statistics,
    },
    record_batch::RecordBatch,
};
use snafu::{OptionExt, ResultExt};

use crate::error::{
    DecodePhysicalPlanNodeSnafu, EmptyGrpcPhysicalPlanSnafu, Error, MissingFieldSnafu,
    NewProjectionSnafu, UnsupportedDfSnafu,
};
use crate::physical::{expr, AsExcutionPlan};

pub struct DefaultAsPlanImpl {
    pub bytes: Vec<u8>,
}

impl AsExcutionPlan for DefaultAsPlanImpl {
    type Error = Error;

    // Vec<u8> -> PhysicalPlanNode -> Arc<dyn ExecutionPlan>
    fn try_into_physical_plan(&self) -> Result<Arc<dyn ExecutionPlan>, Self::Error> {
        let physicalplan_node: PhysicalPlanNode = self
            .bytes
            .deref()
            .try_into()
            .context(DecodePhysicalPlanNodeSnafu)?;
        physicalplan_node.try_into_physical_plan()
    }

    // Arc<dyn ExecutionPlan> -> PhysicalPlanNode -> Vec<u8>
    fn try_from_physical_plan(plan: Arc<dyn ExecutionPlan>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let bytes: Vec<u8> = PhysicalPlanNode::try_from_physical_plan(plan)?.into();
        Ok(DefaultAsPlanImpl { bytes })
    }
}

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
                let input = if let Some(input) = &projection.input {
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
            PhysicalPlanType::Mock(mock) => Ok(Arc::new(MockExecution {
                name: mock.name.to_string(),
            })),
        }
    }

    fn try_from_physical_plan(plan: Arc<dyn ExecutionPlan>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let plan = plan.as_any();

        if let Some(exec) = plan.downcast_ref::<ProjectionExec>() {
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
        } else if let Some(exec) = plan.downcast_ref::<MockExecution>() {
            Ok(PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Mock(MockInputExecNode {
                    name: exec.name.clone(),
                })),
            })
        } else {
            UnsupportedDfSnafu {
                name: format!("{:?}", plan),
            }
            .fail()?
        }
    }
}

#[derive(Debug)]
pub struct MockExecution {
    name: String,
}

impl MockExecution {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait]
impl ExecutionPlan for MockExecution {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        let field1 = Field::new("id", DataType::UInt32, false);
        let field2 = Field::new("name", DataType::LargeUtf8, false);
        let field3 = Field::new("age", DataType::UInt32, false);
        Arc::new(arrow::datatypes::Schema::new(vec![field1, field2, field3]))
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        unimplemented!()
    }

    fn output_ordering(
        &self,
    ) -> Option<&[datafusion::physical_plan::expressions::PhysicalSortExpr]> {
        unimplemented!()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    async fn execute(
        &self,
        _partition: usize,
        _runtime: Arc<RuntimeEnv>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let id_array = Arc::new(PrimitiveArray::from_slice([1u32, 2, 3, 4, 5]));
        let name_array = Arc::new(Utf8Array::<i64>::from_slice([
            "zhangsan", "lisi", "wangwu", "Tony", "Mike",
        ]));
        let age_array = Arc::new(PrimitiveArray::from_slice([25u32, 28, 27, 35, 25]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("name", DataType::LargeUtf8, false),
            Field::new("age", DataType::UInt32, false),
        ]));
        let record_batch =
            RecordBatch::try_new(schema, vec![id_array, name_array, age_array]).unwrap();
        let data: Vec<RecordBatch> = vec![record_batch];
        let projection = Some(vec![0, 1, 2]);
        let stream = MemoryStream::try_new(data, self.schema(), projection).unwrap();
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::codec::PhysicalPlanNode;
    use datafusion::physical_plan::{
        expressions::Column, projection::ProjectionExec, ExecutionPlan,
    };

    use super::{DefaultAsPlanImpl, MockExecution};
    use crate::physical::AsExcutionPlan;

    #[test]
    fn test_convert_df_projection_with_bytes() {
        let projection_exec = mock_df_projection();

        let bytes = DefaultAsPlanImpl::try_from_physical_plan(projection_exec).unwrap();
        let exec = bytes.try_into_physical_plan().unwrap();

        verify_df_porjection(exec);
    }

    #[test]
    fn test_convert_df_with_grpc_projection() {
        let projection_exec = mock_df_projection();

        let projection_node = PhysicalPlanNode::try_from_physical_plan(projection_exec).unwrap();
        let exec = projection_node.try_into_physical_plan().unwrap();

        verify_df_porjection(exec);
    }

    fn mock_df_projection() -> Arc<ProjectionExec> {
        let mock_input = Arc::new(MockExecution {
            name: "mock_input".to_string(),
        });
        let column1 = Arc::new(Column::new("id", 0));
        let column2 = Arc::new(Column::new("name", 1));
        Arc::new(
            ProjectionExec::try_new(
                vec![(column1, "id".to_string()), (column2, "name".to_string())],
                mock_input,
            )
            .unwrap(),
        )
    }

    fn verify_df_porjection(exec: Arc<dyn ExecutionPlan>) {
        let projection_exec = exec.as_any().downcast_ref::<ProjectionExec>().unwrap();
        let mock_input = projection_exec
            .input()
            .as_any()
            .downcast_ref::<MockExecution>()
            .unwrap();

        assert_eq!("mock_input", mock_input.name);
        assert_eq!(2, projection_exec.expr().len());
        assert_eq!("id", projection_exec.expr()[0].1);
        assert_eq!("name", projection_exec.expr()[1].1);
    }
}
