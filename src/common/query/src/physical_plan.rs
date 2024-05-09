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

// use std::any::Any;
// use std::fmt::{self, Debug};
// use std::sync::Arc;

// use common_recordbatch::adapter::{DfRecordBatchStreamAdapter, RecordBatchStreamAdapter};
// use common_recordbatch::{DfSendableRecordBatchStream, SendableRecordBatchStream};
// use datafusion::arrow::datatypes::SchemaRef as DfSchemaRef;
// use datafusion::error::Result as DfResult;
pub use datafusion::execution::context::{SessionContext, TaskContext};
pub use datafusion::physical_plan::ExecutionPlan;
// use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
pub use datafusion::physical_plan::Partitioning;
// use datafusion::physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};
// use datatypes::schema::SchemaRef;
// use snafu::ResultExt;

// use crate::error::{self, Result};

// pub type PhysicalPlanRef = Arc<dyn PhysicalPlan>;

// /// [`PhysicalPlan`] represent nodes in the Physical Plan.
// ///
// /// Each [`PhysicalPlan`] is partition-aware and is responsible for
// /// creating the actual "async" [`SendableRecordBatchStream`]s
// /// of [`RecordBatch`](common_recordbatch::RecordBatch) that incrementally
// /// compute the operator's  output from its input partition.
// pub trait PhysicalPlan: Debug + Send + Sync {
//     /// Returns the physical plan as [`Any`](std::any::Any) so that it can be
//     /// downcast to a specific implementation.
//     fn as_any(&self) -> &dyn Any;

//     /// Get the schema for this physical plan
//     fn schema(&self) -> SchemaRef;

//     /// Return properties of the output of the [PhysicalPlan], such as output
//     /// ordering(s), partitioning information etc.
//     fn properties(&self) -> &PlanProperties;

//     /// Get a list of child physical plans that provide the input for this plan. The returned list
//     /// will be empty for leaf nodes, will contain a single value for unary nodes, or two
//     /// values for binary nodes (such as joins).
//     fn children(&self) -> Vec<PhysicalPlanRef>;

//     /// Returns a new plan where all children were replaced by new plans.
//     /// The size of `children` must be equal to the size of [`PhysicalPlan::children()`].
//     fn with_new_children(&self, children: Vec<PhysicalPlanRef>) -> Result<PhysicalPlanRef>;

//     /// Creates an RecordBatch stream.
//     fn execute(
//         &self,
//         partition: usize,
//         context: Arc<TaskContext>,
//     ) -> Result<SendableRecordBatchStream>;

//     fn metrics(&self) -> Option<MetricsSet> {
//         None
//     }
// }

// /// Adapt DataFusion's [`ExecutionPlan`](DfPhysicalPlan) to GreptimeDB's [`PhysicalPlan`].
// #[derive(Debug)]
// pub struct PhysicalPlanAdapter {
//     schema: SchemaRef,
//     df_plan: Arc<dyn DfPhysicalPlan>,
//     metric: ExecutionPlanMetricsSet,
// }

// impl PhysicalPlanAdapter {
//     pub fn new(schema: SchemaRef, df_plan: Arc<dyn DfPhysicalPlan>) -> Self {
//         Self {
//             schema,
//             df_plan,
//             metric: ExecutionPlanMetricsSet::new(),
//         }
//     }

//     pub fn df_plan(&self) -> Arc<dyn DfPhysicalPlan> {
//         self.df_plan.clone()
//     }
// }

// impl PhysicalPlan for PhysicalPlanAdapter {
//     fn as_any(&self) -> &dyn Any {
//         self
//     }

//     fn schema(&self) -> SchemaRef {
//         self.schema.clone()
//     }

//     fn properties(&self) -> &PlanProperties {
//         self.df_plan.properties()
//     }

//     fn children(&self) -> Vec<PhysicalPlanRef> {
//         self.df_plan
//             .children()
//             .into_iter()
//             .map(|x| Arc::new(PhysicalPlanAdapter::new(self.schema(), x)) as _)
//             .collect()
//     }

//     fn with_new_children(&self, children: Vec<PhysicalPlanRef>) -> Result<PhysicalPlanRef> {
//         let children = children
//             .into_iter()
//             .map(|x| Arc::new(DfPhysicalPlanAdapter(x)) as _)
//             .collect();
//         let plan = self
//             .df_plan
//             .clone()
//             .with_new_children(children)
//             .context(error::GeneralDataFusionSnafu)?;
//         Ok(Arc::new(PhysicalPlanAdapter::new(self.schema(), plan)))
//     }

//     fn execute(
//         &self,
//         partition: usize,
//         context: Arc<TaskContext>,
//     ) -> Result<SendableRecordBatchStream> {
//         let baseline_metric = BaselineMetrics::new(&self.metric, partition);

//         let df_plan = self.df_plan.clone();
//         let stream = df_plan
//             .execute(partition, context)
//             .context(error::GeneralDataFusionSnafu)?;
//         let adapter = RecordBatchStreamAdapter::try_new_with_metrics_and_df_plan(
//             stream,
//             baseline_metric,
//             df_plan,
//         )
//         .context(error::ConvertDfRecordBatchStreamSnafu)?;

//         Ok(Box::pin(adapter))
//     }

//     fn metrics(&self) -> Option<MetricsSet> {
//         self.df_plan.metrics()
//     }
// }

// #[derive(Debug)]
// pub struct DfPhysicalPlanAdapter(pub PhysicalPlanRef);

// impl DfPhysicalPlan for DfPhysicalPlanAdapter {
//     fn as_any(&self) -> &dyn Any {
//         self
//     }

//     fn schema(&self) -> DfSchemaRef {
//         self.0.schema().arrow_schema().clone()
//     }

//     fn children(&self) -> Vec<Arc<dyn DfPhysicalPlan>> {
//         self.0
//             .children()
//             .into_iter()
//             .map(|x| Arc::new(DfPhysicalPlanAdapter(x)) as _)
//             .collect()
//     }

//     fn with_new_children(
//         self: Arc<Self>,
//         children: Vec<Arc<dyn DfPhysicalPlan>>,
//     ) -> DfResult<Arc<dyn DfPhysicalPlan>> {
//         let df_schema = self.schema();
//         let schema: SchemaRef = Arc::new(
//             df_schema
//                 .try_into()
//                 .context(error::ConvertArrowSchemaSnafu)?,
//         );
//         let children = children
//             .into_iter()
//             .map(|x| Arc::new(PhysicalPlanAdapter::new(schema.clone(), x)) as _)
//             .collect();
//         let plan = self.0.with_new_children(children)?;
//         Ok(Arc::new(DfPhysicalPlanAdapter(plan)))
//     }

//     fn execute(
//         &self,
//         partition: usize,
//         context: Arc<TaskContext>,
//     ) -> DfResult<DfSendableRecordBatchStream> {
//         let stream = self.0.execute(partition, context)?;
//         Ok(Box::pin(DfRecordBatchStreamAdapter::new(stream)))
//     }

//     fn metrics(&self) -> Option<MetricsSet> {
//         self.0.metrics()
//     }

//     fn properties(&self) -> &PlanProperties {
//         self.0.properties()
//     }
// }

// impl DisplayAs for DfPhysicalPlanAdapter {
//     fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "{:?}", self.0)
//     }
// }

// #[cfg(test)]
// mod test {
//     use async_trait::async_trait;
//     use common_recordbatch::{RecordBatch, RecordBatches};
//     use datafusion::arrow::datatypes::SchemaRef as DfSchemaRef;
//     use datafusion::datasource::{DefaultTableSource, TableProvider as DfTableProvider, TableType};
//     use datafusion::execution::context::{SessionContext, SessionState};
//     use datafusion::physical_expr::EquivalenceProperties;
//     use datafusion::physical_plan::empty::EmptyExec;
//     use datafusion::physical_plan::{collect, ExecutionMode};
//     use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
//     use datafusion_expr::{Expr, TableSource};
//     use datatypes::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
//     use datatypes::arrow::util::pretty;
//     use datatypes::schema::Schema;
//     use datatypes::vectors::Int32Vector;

//     use super::*;

//     struct MyDfTableProvider;

//     #[async_trait]
//     impl DfTableProvider for MyDfTableProvider {
//         fn as_any(&self) -> &dyn Any {
//             self
//         }

//         fn schema(&self) -> DfSchemaRef {
//             Arc::new(ArrowSchema::new(vec![Field::new(
//                 "a",
//                 DataType::Int32,
//                 false,
//             )]))
//         }

//         fn table_type(&self) -> TableType {
//             TableType::Base
//         }

//         async fn scan(
//             &self,
//             _ctx: &SessionState,
//             _projection: Option<&Vec<usize>>,
//             _filters: &[Expr],
//             _limit: Option<usize>,
//         ) -> DfResult<Arc<dyn DfPhysicalPlan>> {
//             let schema = Arc::new(Schema::try_from(self.schema()).unwrap());
//             let properties = PlanProperties::new(
//                 EquivalenceProperties::new(schema.arrow_schema().clone()),
//                 Partitioning::UnknownPartitioning(1),
//                 ExecutionMode::Bounded,
//             );
//             let my_plan = Arc::new(MyExecutionPlan { schema, properties });
//             let df_plan = DfPhysicalPlanAdapter(my_plan);
//             Ok(Arc::new(df_plan))
//         }
//     }

//     impl MyDfTableProvider {
//         fn table_source() -> Arc<dyn TableSource> {
//             Arc::new(DefaultTableSource {
//                 table_provider: Arc::new(Self),
//             })
//         }
//     }

//     #[derive(Debug, Clone)]
//     struct MyExecutionPlan {
//         schema: SchemaRef,
//         properties: PlanProperties,
//     }

//     impl PhysicalPlan for MyExecutionPlan {
//         fn as_any(&self) -> &dyn Any {
//             self
//         }

//         fn schema(&self) -> SchemaRef {
//             self.schema.clone()
//         }

//         fn properties(&self) -> &PlanProperties {
//             &self.properties
//         }

//         fn children(&self) -> Vec<PhysicalPlanRef> {
//             vec![]
//         }

//         fn with_new_children(&self, _children: Vec<PhysicalPlanRef>) -> Result<PhysicalPlanRef> {
//             Ok(Arc::new(self.clone()))
//         }

//         fn execute(
//             &self,
//             _partition: usize,
//             _context: Arc<TaskContext>,
//         ) -> Result<SendableRecordBatchStream> {
//             let schema = self.schema();
//             let recordbatches = RecordBatches::try_new(
//                 schema.clone(),
//                 vec![
//                     RecordBatch::new(
//                         schema.clone(),
//                         vec![Arc::new(Int32Vector::from_slice(vec![1])) as _],
//                     )
//                     .unwrap(),
//                     RecordBatch::new(
//                         schema,
//                         vec![Arc::new(Int32Vector::from_slice(vec![2, 3])) as _],
//                     )
//                     .unwrap(),
//                 ],
//             )
//             .unwrap();
//             Ok(recordbatches.as_stream())
//         }
//     }

//     // Test our physical plan can be executed by DataFusion, through adapters.
//     #[tokio::test]
//     async fn test_execute_physical_plan() {
//         let ctx = SessionContext::new();
//         let logical_plan =
//             LogicalPlanBuilder::scan("test", MyDfTableProvider::table_source(), None)
//                 .unwrap()
//                 .build()
//                 .unwrap();
//         let physical_plan = ctx
//             .state()
//             .create_physical_plan(&logical_plan)
//             .await
//             .unwrap();
//         let df_recordbatches = collect(physical_plan, Arc::new(TaskContext::from(&ctx)))
//             .await
//             .unwrap();
//         let pretty_print = pretty::pretty_format_batches(&df_recordbatches).unwrap();
//         assert_eq!(
//             pretty_print.to_string(),
//             r#"+---+
// | a |
// +---+
// | 1 |
// | 2 |
// | 3 |
// +---+"#
//         );
//     }

//     #[test]
//     fn test_physical_plan_adapter() {
//         let df_schema = Arc::new(ArrowSchema::new(vec![Field::new(
//             "name",
//             DataType::Utf8,
//             true,
//         )]));

//         let plan = PhysicalPlanAdapter::new(
//             Arc::new(Schema::try_from(df_schema.clone()).unwrap()),
//             Arc::new(EmptyExec::new(df_schema.clone())),
//         );
//         let _ = plan.df_plan.as_any().downcast_ref::<EmptyExec>().unwrap();

//         let df_plan = DfPhysicalPlanAdapter(Arc::new(plan));
//         assert_eq!(df_schema, df_plan.schema());
//     }
// }
