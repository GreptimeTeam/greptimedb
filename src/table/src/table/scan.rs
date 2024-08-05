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
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

use common_error::ext::BoxedError;
use common_recordbatch::{DfRecordBatch, DfSendableRecordBatchStream, SendableRecordBatchStream};
use common_telemetry::tracing::Span;
use common_telemetry::tracing_context::TracingContext;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
    RecordBatchStream as DfRecordBatchStream,
};
use datafusion_common::DataFusionError;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalSortExpr};
use datatypes::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use futures::{Stream, StreamExt};
use store_api::region_engine::{PartitionRange, RegionScannerRef};

use crate::table::metrics::StreamMetrics;

/// A plan to read multiple partitions from a region of a table.
#[derive(Debug)]
pub struct RegionScanExec {
    scanner: Mutex<RegionScannerRef>,
    arrow_schema: ArrowSchemaRef,
    /// The expected output ordering for the plan.
    output_ordering: Option<Vec<PhysicalSortExpr>>,
    metric: ExecutionPlanMetricsSet,
    properties: PlanProperties,
}

impl RegionScanExec {
    pub fn new(scanner: RegionScannerRef) -> Self {
        let arrow_schema = scanner.schema().arrow_schema().clone();
        let scanner_props = scanner.properties();
        let mut num_output_partition = scanner_props.num_partitions();
        // The meaning of word "partition" is different in different context. For datafusion
        // it's about "parallelism" and for storage it's about "data range". Thus here we add
        // a special case to handle the situation where the number of storage partition is 0.
        if num_output_partition == 0 {
            num_output_partition = 1;
        }
        let properties = PlanProperties::new(
            EquivalenceProperties::new(arrow_schema.clone()),
            Partitioning::UnknownPartitioning(num_output_partition),
            ExecutionMode::Bounded,
        );
        Self {
            scanner: Mutex::new(scanner),
            arrow_schema,
            output_ordering: None,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
        }
    }

    /// Set the expected output ordering for the plan.
    pub fn with_output_ordering(mut self, output_ordering: Vec<PhysicalSortExpr>) -> Self {
        self.output_ordering = Some(output_ordering);
        self
    }

    /// Get the partition ranges of the scanner. This method will collapse the ranges into
    /// a single vector.
    pub fn get_partition_ranges(&self) -> Vec<PartitionRange> {
        let scanner = self.scanner.lock().unwrap();
        let raw_ranges = &scanner.properties().partitions;

        // collapse the ranges
        let mut ranges = Vec::with_capacity(raw_ranges.len());
        for partition in raw_ranges {
            ranges.extend_from_slice(partition);
        }

        ranges
    }

    /// Update the partition ranges of underlying scanner.
    pub fn set_partitions(&self, partitions: Vec<Vec<PartitionRange>>) -> Result<(), BoxedError> {
        let mut scanner = self.scanner.lock().unwrap();
        scanner.prepare(partitions)
    }
}

impl ExecutionPlan for RegionScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<DfSendableRecordBatchStream> {
        let tracing_context = TracingContext::from_json(context.session_id().as_str());
        let span =
            tracing_context.attach(common_telemetry::tracing::info_span!("read_from_region"));

        let stream = self
            .scanner
            .lock()
            .unwrap()
            .scan_partition(partition)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let stream_metrics = StreamMetrics::new(&self.metric, partition);
        Ok(Box::pin(StreamWithMetricWrapper {
            stream,
            metric: stream_metrics,
            span,
            await_timer: None,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }
}

impl DisplayAs for RegionScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // The scanner contains all information needed to display the plan.
        match self.scanner.try_lock() {
            Ok(scanner) => scanner.fmt_as(t, f),
            Err(_) => write!(f, "RegionScanExec <locked>"),
        }
    }
}

pub struct StreamWithMetricWrapper {
    stream: SendableRecordBatchStream,
    metric: StreamMetrics,
    span: Span,
    await_timer: Option<Instant>,
}

impl Stream for StreamWithMetricWrapper {
    type Item = DfResult<DfRecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let _enter = this.span.enter();
        let poll_timer = this.metric.poll_timer();
        this.await_timer.get_or_insert(Instant::now());
        let poll_result = this.stream.poll_next_unpin(cx);
        drop(poll_timer);
        match poll_result {
            Poll::Ready(Some(result)) => {
                if let Some(instant) = this.await_timer.take() {
                    let elapsed = instant.elapsed();
                    this.metric.record_await_duration(elapsed);
                }
                match result {
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
                }
            }
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
    use std::sync::Arc;

    use common_recordbatch::{RecordBatch, RecordBatches};
    use datafusion::prelude::SessionContext;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
    use datatypes::vectors::Int32Vector;
    use futures::TryStreamExt;
    use store_api::region_engine::SinglePartitionScanner;

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

        let scanner = Box::new(SinglePartitionScanner::new(stream));
        let plan = RegionScanExec::new(scanner);
        let actual: SchemaRef = Arc::new(
            plan.properties
                .eq_properties
                .schema()
                .clone()
                .try_into()
                .unwrap(),
        );
        assert_eq!(actual, schema);

        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let recordbatches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batch1.df_record_batch(), &recordbatches[0]);
        assert_eq!(batch2.df_record_batch(), &recordbatches[1]);

        let result = plan.execute(0, ctx.task_ctx());
        assert!(result.is_err());
        match result {
            Err(e) => assert!(e
                .to_string()
                .contains("Not expected to run ExecutionPlan more than once")),
            _ => unreachable!(),
        }
    }
}
