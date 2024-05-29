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

//! Customized `ANALYZE` plan that aware of [MergeScanExec].
//!
//! The code skeleton is taken from `datafusion/physical-plan/src/analyze.rs`

use std::any::Any;
use std::sync::Arc;

use arrow::array::{StringBuilder, UInt32Builder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use common_recordbatch::adapter::{MetricCollector, RecordBatchMetrics};
use common_recordbatch::{DfRecordBatch, DfSendableRecordBatchStream};
use datafusion::error::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    accept, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{internal_err, DataFusionError};
use datafusion_physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use futures::StreamExt;

use crate::dist_plan::MergeScanExec;

const STAGE: &str = "stage";
const NODE: &str = "node";
const PLAN: &str = "plan";

#[derive(Debug)]
pub struct DistAnalyzeExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl DistAnalyzeExec {
    /// Create a new DistAnalyzeExec
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let schema = SchemaRef::new(Schema::new(vec![
            Field::new(STAGE, DataType::UInt32, true),
            Field::new(NODE, DataType::UInt32, true),
            Field::new(PLAN, DataType::Utf8, true),
        ]));
        let properties = Self::compute_properties(&input, schema.clone());
        Self {
            input,
            schema,
            properties,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>, schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        let output_partitioning = Partitioning::UnknownPartitioning(1);
        let exec_mode = input.execution_mode();
        PlanProperties::new(eq_properties, output_partitioning, exec_mode)
    }
}

impl DisplayAs for DistAnalyzeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DistAnalyzeExec",)
            }
        }
    }
}

impl ExecutionPlan for DistAnalyzeExec {
    fn name(&self) -> &'static str {
        "DistAnalyzeExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// AnalyzeExec is handled specially so this value is ignored
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children.pop().unwrap())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<DfSendableRecordBatchStream> {
        if 0 != partition {
            return internal_err!("AnalyzeExec invalid partition. Expected 0, got {partition}");
        }

        // Wrap the input plan using `CoalescePartitionsExec` to poll multiple
        // partitions in parallel
        let coalesce_partition_plan = CoalescePartitionsExec::new(self.input.clone());

        // Create future that computes thefinal output
        let captured_input = self.input.clone();
        let captured_schema = self.schema.clone();

        // Finish the input stream and create the output
        let mut input_stream = coalesce_partition_plan.execute(0, context)?;
        let output = async move {
            let mut total_rows = 0;
            while let Some(batch) = input_stream.next().await.transpose()? {
                total_rows += batch.num_rows();
            }

            create_output_batch(total_rows, captured_input, captured_schema)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::once(output),
        )))
    }
}

/// Build the result [`DfRecordBatch`] of `ANALYZE`
struct AnalyzeOutputBuilder {
    stage_builder: UInt32Builder,
    node_builder: UInt32Builder,
    plan_builder: StringBuilder,
    schema: SchemaRef,
}

impl AnalyzeOutputBuilder {
    fn new(schema: SchemaRef) -> Self {
        Self {
            stage_builder: UInt32Builder::with_capacity(4),
            node_builder: UInt32Builder::with_capacity(4),
            plan_builder: StringBuilder::with_capacity(1, 1024),
            schema,
        }
    }

    fn append_metric(&mut self, stage: u32, node: u32, metric: RecordBatchMetrics) {
        self.stage_builder.append_value(stage);
        self.node_builder.append_value(node);
        self.plan_builder.append_value(metric.to_string());
    }

    fn append_total_rows(&mut self, total_rows: usize) {
        self.stage_builder.append_null();
        self.node_builder.append_null();
        self.plan_builder
            .append_value(format!("Total rows: {}", total_rows));
    }

    fn finish(mut self) -> DfResult<DfRecordBatch> {
        DfRecordBatch::try_new(
            self.schema,
            vec![
                Arc::new(self.stage_builder.finish()),
                Arc::new(self.node_builder.finish()),
                Arc::new(self.plan_builder.finish()),
            ],
        )
        .map_err(DataFusionError::from)
    }
}

/// Creates the output of AnalyzeExec as a RecordBatch
fn create_output_batch(
    total_rows: usize,
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
) -> DfResult<DfRecordBatch> {
    let mut builder = AnalyzeOutputBuilder::new(schema);

    // Treat the current stage as stage 0. Fetch its metrics
    let mut collector = MetricCollector::default();
    // Safety: metric collector won't return error
    accept(input.as_ref(), &mut collector).unwrap();
    let stage_0_metrics = collector.record_batch_metrics;

    // Append the metrics of the current stage
    builder.append_metric(0, 0, stage_0_metrics);

    // Find merge scan and append its sub_stage_metrics
    input.apply(|plan| {
        if let Some(merge_scan) = plan.as_any().downcast_ref::<MergeScanExec>() {
            let sub_stage_metrics = merge_scan.sub_stage_metrics();
            for (node, metric) in sub_stage_metrics.into_iter().enumerate() {
                builder.append_metric(1, node as _, metric);
            }
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    // Write total rows
    builder.append_total_rows(total_rows);

    builder.finish()
}
