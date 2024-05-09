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
use std::fmt::{self, Debug, Formatter};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use common_query::error::ExecuteRepeatedlySnafu;
use common_query::physical_plan::{ExecutionPlan, Partitioning};
use common_recordbatch::{DfRecordBatch, DfSendableRecordBatchStream, SendableRecordBatchStream};
use common_telemetry::tracing::Span;
use common_telemetry::tracing_context::TracingContext;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, PlanProperties,
    RecordBatchStream as DfRecordBatchStream,
};
use datafusion_common::DataFusionError;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalSortExpr};
use datatypes::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datatypes::schema::SchemaRef;
use futures::{Stream, StreamExt};
use snafu::OptionExt;

use crate::table::metrics::MemoryUsageMetrics;

/// Adapt greptime's [SendableRecordBatchStream] to [ExecutionPlan].
pub struct StreamScanAdapter {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    schema: SchemaRef,
    output_ordering: Option<Vec<PhysicalSortExpr>>,
    metric: ExecutionPlanMetricsSet,
    properties: PlanProperties,
}

impl Debug for StreamScanAdapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamScanAdapter")
            .field("stream", &"<SendableRecordBatchStream>")
            .field("schema", &self.schema.arrow_schema().fields)
            .finish()
    }
}

impl StreamScanAdapter {
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        let schema = stream.schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.arrow_schema().clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self {
            stream: Mutex::new(Some(stream)),
            schema,
            output_ordering: None,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
        }
    }

    pub fn with_output_ordering(mut self, output_ordering: Vec<PhysicalSortExpr>) -> Self {
        self.output_ordering = Some(output_ordering);
        self
    }
}

impl ExecutionPlan for StreamScanAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.arrow_schema().clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<DfSendableRecordBatchStream> {
        let tracing_context = TracingContext::from_json(context.session_id().as_str());
        let span = tracing_context.attach(common_telemetry::tracing::info_span!("stream_adapter"));

        let mut stream = self.stream.lock().unwrap();
        let stream = stream.take().context(ExecuteRepeatedlySnafu)?;
        let mem_usage_metrics = MemoryUsageMetrics::new(&self.metric, partition);
        Ok(Box::pin(StreamWithMetricWrapper {
            stream,
            metric: mem_usage_metrics,
            span,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }
}

impl DisplayAs for StreamScanAdapter {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub struct StreamWithMetricWrapper {
    stream: SendableRecordBatchStream,
    metric: MemoryUsageMetrics,
    span: Span,
}

impl Stream for StreamWithMetricWrapper {
    type Item = DfResult<DfRecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let _enter = this.span.enter();
        match this.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(result)) => match result {
                Ok(record_batch) => {
                    let batch_mem_size = record_batch
                        .columns()
                        .iter()
                        .map(|vec_ref| vec_ref.memory_size())
                        .sum::<usize>();
                    // we don't record elapsed time here
                    // since it's calling storage api involving I/O ops
                    this.metric.record_mem_usage(batch_mem_size);
                    this.metric.record_output(record_batch.num_rows());
                    Poll::Ready(Some(Ok(record_batch.into_df_record_batch())))
                }
                Err(e) => Poll::Ready(Some(Err(DataFusionError::External(Box::new(e))))),
            },
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl DfRecordBatchStream for StreamWithMetricWrapper {
    fn schema(&self) -> ArrowSchemaRef {
        self.stream.schema().arrow_schema().clone()
    }
}

#[cfg(test)]
mod test {
    use common_recordbatch::{RecordBatch, RecordBatches};
    use datafusion::prelude::SessionContext;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;
    use futures::TryStreamExt;

    use super::*;

    #[tokio::test]
    async fn test_simple_table_scan() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));

        let batch1 = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice([1, 2])) as _],
        )
        .unwrap();
        let batch2 = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice([3, 4, 5])) as _],
        )
        .unwrap();

        let recordbatches =
            RecordBatches::try_new(schema.clone(), vec![batch1.clone(), batch2.clone()]).unwrap();
        let stream = recordbatches.as_stream();

        let scan = StreamScanAdapter::new(stream);
        let actual: SchemaRef = Arc::new(
            scan.properties
                .eq_properties
                .schema()
                .clone()
                .try_into()
                .unwrap(),
        );
        assert_eq!(actual, schema);

        let stream = scan.execute(0, ctx.task_ctx()).unwrap();
        let recordbatches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batch1.df_record_batch(), &recordbatches[0]);
        assert_eq!(batch2.df_record_batch(), &recordbatches[1]);

        let result = scan.execute(0, ctx.task_ctx());
        assert!(result.is_err());
        match result {
            Err(e) => assert!(e
                .to_string()
                .contains("Not expected to run ExecutionPlan more than once")),
            _ => unreachable!(),
        }
    }
}
