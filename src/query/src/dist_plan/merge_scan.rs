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

use std::any::Any;
use std::sync::Arc;

use arrow_schema::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use async_stream::stream;
use client::region_handler::RegionRequestHandlerRef;
use common_base::bytes::Bytes;
use common_error::ext::BoxedError;
use common_meta::table_name::TableName;
use common_query::physical_plan::TaskContext;
use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{
    DfSendableRecordBatchStream, RecordBatch, RecordBatchStreamAdaptor, SendableRecordBatchStream,
};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning};
use datafusion_common::{DataFusionError, Result, Statistics};
use datafusion_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_physical_expr::PhysicalSortExpr;
use datatypes::schema::{Schema, SchemaRef};
use futures_util::StreamExt;
use greptime_proto::v1::region::QueryRequest;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::ConvertSchemaSnafu;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct MergeScanLogicalPlan {
    /// In logical plan phase it only contains one input
    input: LogicalPlan,
    /// If this plan is a placeholder
    is_placeholder: bool,
}

impl UserDefinedLogicalNodeCore for MergeScanLogicalPlan {
    fn name(&self) -> &str {
        Self::name()
    }

    // Prevent further optimization.
    // The input can be retrieved by `self.input()`
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        self.input.schema()
    }

    // Prevent further optimization
    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MergeScan [is_placeholder={}]", self.is_placeholder)
    }

    fn from_template(&self, _exprs: &[datafusion_expr::Expr], _inputs: &[LogicalPlan]) -> Self {
        self.clone()
    }
}

impl MergeScanLogicalPlan {
    pub fn new(input: LogicalPlan, is_placeholder: bool) -> Self {
        Self {
            input,
            is_placeholder,
        }
    }

    pub fn name() -> &'static str {
        "MergeScan"
    }

    /// Create a [LogicalPlan::Extension] node from this merge scan plan
    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    pub fn is_placeholder(&self) -> bool {
        self.is_placeholder
    }

    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }
}

pub struct MergeScanExec {
    table: TableName,
    regions: Vec<RegionId>,
    substrait_plan: Bytes,
    schema: SchemaRef,
    arrow_schema: ArrowSchemaRef,
    request_handler: RegionRequestHandlerRef,
    metric: ExecutionPlanMetricsSet,
}

impl std::fmt::Debug for MergeScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergeScanExec")
            .field("table", &self.table)
            .field("regions", &self.regions)
            .field("schema", &self.schema)
            .finish()
    }
}

impl MergeScanExec {
    pub fn new(
        table: TableName,
        regions: Vec<RegionId>,
        substrait_plan: Bytes,
        arrow_schema: &ArrowSchema,
        request_handler: RegionRequestHandlerRef,
    ) -> Result<Self> {
        let arrow_schema_without_metadata = Self::arrow_schema_without_metadata(arrow_schema);
        let schema_without_metadata =
            Self::arrow_schema_to_schema(arrow_schema_without_metadata.clone())?;
        Ok(Self {
            table,
            regions,
            substrait_plan,
            schema: schema_without_metadata,
            arrow_schema: arrow_schema_without_metadata,
            request_handler,
            metric: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn to_stream(&self, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let substrait_plan = self.substrait_plan.to_vec();
        let regions = self.regions.clone();
        let request_handler = self.request_handler.clone();
        let metric = MergeScanMetric::new(&self.metric);

        let stream = Box::pin(stream!({
            let _finish_timer = metric.finish_time().timer();
            let mut ready_timer = metric.ready_time().timer();
            let mut first_consume_timer = Some(metric.first_consume_time().timer());

            for region_id in regions {
                let request = QueryRequest {
                    region_id: region_id.into(),
                    plan: substrait_plan.clone(),
                };
                let mut stream = request_handler
                    .do_get(request)
                    .await
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?;

                ready_timer.stop();

                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    metric.record_output_batch_rows(batch.num_rows());
                    yield Ok(Self::remove_metadata_from_record_batch(batch));

                    if let Some(first_consume_timer) = first_consume_timer.as_mut().take() {
                        first_consume_timer.stop();
                    }
                }
            }
        }));

        Ok(Box::pin(RecordBatchStreamAdaptor {
            schema: self.schema.clone(),
            stream,
            output_ordering: None,
        }))
    }

    fn remove_metadata_from_record_batch(batch: RecordBatch) -> RecordBatch {
        let arrow_schema = batch.schema.arrow_schema().as_ref();
        let arrow_schema_without_metadata = Self::arrow_schema_without_metadata(arrow_schema);
        let schema_without_metadata =
            Self::arrow_schema_to_schema(arrow_schema_without_metadata).unwrap();
        RecordBatch::new(schema_without_metadata, batch.columns().iter().cloned()).unwrap()
    }

    fn arrow_schema_without_metadata(arrow_schema: &ArrowSchema) -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(
            arrow_schema
                .fields()
                .iter()
                .map(|field| {
                    let field = field.as_ref().clone();
                    let field_without_metadata = field.with_metadata(Default::default());
                    Arc::new(field_without_metadata)
                })
                .collect::<Vec<_>>(),
        ))
    }

    fn arrow_schema_to_schema(arrow_schema: ArrowSchemaRef) -> Result<SchemaRef> {
        let schema = Schema::try_from(arrow_schema).context(ConvertSchemaSnafu)?;
        Ok(Arc::new(schema))
    }
}

impl ExecutionPlan for MergeScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "should not call `with_new_children` on MergeScanExec".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<DfSendableRecordBatchStream> {
        Ok(Box::pin(DfRecordBatchStreamAdapter::new(
            self.to_stream(context)?,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }
}

impl DisplayAs for MergeScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MergeScanExec: peers=[")?;
        for region_id in self.regions.iter() {
            write!(f, "{}, ", region_id)?;
        }
        write!(f, "]")
    }
}

#[derive(Debug, Clone)]
struct MergeScanMetric {
    /// Nanosecond elapsed till the scan operator is ready to emit data
    ready_time: Time,
    /// Nanosecond elapsed till the first record batch emitted from the scan operator gets consumed
    first_consume_time: Time,
    /// Nanosecond elapsed till the scan operator finished execution
    finish_time: Time,
    /// Count of rows fetched from remote
    output_rows: Count,
}

impl MergeScanMetric {
    pub fn new(metric: &ExecutionPlanMetricsSet) -> Self {
        Self {
            ready_time: MetricBuilder::new(metric).subset_time("ready_time", 1),
            first_consume_time: MetricBuilder::new(metric).subset_time("first_consume_time", 1),
            finish_time: MetricBuilder::new(metric).subset_time("finish_time", 1),
            output_rows: MetricBuilder::new(metric).output_rows(1),
        }
    }

    pub fn ready_time(&self) -> &Time {
        &self.ready_time
    }

    pub fn first_consume_time(&self) -> &Time {
        &self.first_consume_time
    }

    pub fn finish_time(&self) -> &Time {
        &self.finish_time
    }

    pub fn record_output_batch_rows(&self, num_rows: usize) {
        self.output_rows.add(num_rows);
    }
}
