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
use common_telemetry::warn;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    RecordBatchStream as DfRecordBatchStream,
};
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, DataFusionError, Statistics};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalSortExpr};
use datatypes::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datatypes::compute::SortOptions;
use futures::{Stream, StreamExt};
use store_api::region_engine::{
    PartitionRange, PrepareRequest, QueryScanContext, RegionScannerRef,
};
use store_api::storage::{ScanRequest, TimeSeriesDistribution};

use crate::table::metrics::StreamMetrics;

/// A plan to read multiple partitions from a region of a table.
#[derive(Debug)]
pub struct RegionScanExec {
    scanner: Arc<Mutex<RegionScannerRef>>,
    arrow_schema: ArrowSchemaRef,
    /// The expected output ordering for the plan.
    output_ordering: Option<Vec<PhysicalSortExpr>>,
    metric: ExecutionPlanMetricsSet,
    properties: PlanProperties,
    append_mode: bool,
    total_rows: usize,
    is_partition_set: bool,
    // TODO(ruihang): handle TimeWindowed dist via this parameter
    distribution: Option<TimeSeriesDistribution>,
    explain_verbose: bool,
}

impl RegionScanExec {
    pub fn new(scanner: RegionScannerRef, request: ScanRequest) -> DfResult<Self> {
        let arrow_schema = scanner.schema().arrow_schema().clone();
        let scanner_props = scanner.properties();
        let mut num_output_partition = scanner_props.num_partitions();
        // The meaning of word "partition" is different in different context. For datafusion
        // it's about "parallelism" and for storage it's about "data range". Thus here we add
        // a special case to handle the situation where the number of storage partition is 0.
        if num_output_partition == 0 {
            num_output_partition = 1;
        }

        let metadata = scanner.metadata();
        let mut pk_names = metadata
            .primary_key_columns()
            .map(|col| col.column_schema.name.clone())
            .collect::<Vec<_>>();
        // workaround for logical table
        if scanner.properties().is_logical_region() {
            pk_names.sort_unstable();
        }
        let pk_columns = pk_names
            .iter()
            .filter_map(
                |col| Some(Arc::new(Column::new_with_schema(col, &arrow_schema).ok()?) as _),
            )
            .collect::<Vec<_>>();
        let mut pk_sort_columns: Vec<PhysicalSortExpr> = pk_names
            .iter()
            .filter_map(|col| {
                Some(PhysicalSortExpr::new(
                    Arc::new(Column::new_with_schema(col, &arrow_schema).ok()?) as _,
                    SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                ))
            })
            .collect::<Vec<_>>();
        let ts_col: Option<PhysicalSortExpr> = try {
            PhysicalSortExpr::new(
                Arc::new(
                    Column::new_with_schema(
                        &metadata.time_index_column().column_schema.name,
                        &arrow_schema,
                    )
                    .ok()?,
                ) as _,
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            )
        };

        let eq_props = match request.distribution {
            Some(TimeSeriesDistribution::PerSeries) => {
                if let Some(ts) = ts_col {
                    pk_sort_columns.push(ts);
                }
                EquivalenceProperties::new_with_orderings(
                    arrow_schema.clone(),
                    vec![pk_sort_columns],
                )
            }
            Some(TimeSeriesDistribution::TimeWindowed) => {
                if let Some(ts_col) = ts_col {
                    pk_sort_columns.insert(0, ts_col);
                }
                EquivalenceProperties::new_with_orderings(
                    arrow_schema.clone(),
                    vec![pk_sort_columns],
                )
            }
            None => EquivalenceProperties::new(arrow_schema.clone()),
        };

        let partitioning = match request.distribution {
            Some(TimeSeriesDistribution::PerSeries) => {
                Partitioning::Hash(pk_columns.clone(), num_output_partition)
            }
            Some(TimeSeriesDistribution::TimeWindowed) | None => {
                Partitioning::UnknownPartitioning(num_output_partition)
            }
        };

        let properties = PlanProperties::new(
            eq_props,
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        let append_mode = scanner_props.append_mode();
        let total_rows = scanner_props.total_rows();
        Ok(Self {
            scanner: Arc::new(Mutex::new(scanner)),
            arrow_schema,
            output_ordering: None,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
            append_mode,
            total_rows,
            is_partition_set: false,
            distribution: request.distribution,
            explain_verbose: false,
        })
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

    /// Similar to [`Self::get_partition_ranges`] but don't collapse the ranges.
    pub fn get_uncollapsed_partition_ranges(&self) -> Vec<Vec<PartitionRange>> {
        let scanner = self.scanner.lock().unwrap();
        scanner.properties().partitions.clone()
    }

    pub fn is_partition_set(&self) -> bool {
        self.is_partition_set
    }

    /// Update the partition ranges of underlying scanner.
    pub fn with_new_partitions(
        &self,
        partitions: Vec<Vec<PartitionRange>>,
        target_partitions: usize,
    ) -> Result<Self, BoxedError> {
        if self.is_partition_set {
            warn!("Setting partition ranges more than once for RegionScanExec");
        }

        let mut properties = self.properties.clone();
        let new_partitioning = match properties.partitioning {
            Partitioning::Hash(ref columns, _) => {
                Partitioning::Hash(columns.clone(), target_partitions)
            }
            _ => Partitioning::UnknownPartitioning(target_partitions),
        };
        properties.partitioning = new_partitioning;

        {
            let mut scanner = self.scanner.lock().unwrap();
            scanner.prepare(
                PrepareRequest::default()
                    .with_ranges(partitions)
                    .with_target_partitions(target_partitions),
            )?;
        }

        Ok(Self {
            scanner: self.scanner.clone(),
            arrow_schema: self.arrow_schema.clone(),
            output_ordering: self.output_ordering.clone(),
            metric: self.metric.clone(),
            properties,
            append_mode: self.append_mode,
            total_rows: self.total_rows,
            is_partition_set: true,
            distribution: self.distribution,
            explain_verbose: self.explain_verbose,
        })
    }

    pub fn distribution(&self) -> Option<TimeSeriesDistribution> {
        self.distribution
    }

    pub fn with_distinguish_partition_range(&self, distinguish_partition_range: bool) {
        let mut scanner = self.scanner.lock().unwrap();
        // set distinguish_partition_range won't fail
        let _ = scanner.prepare(
            PrepareRequest::default().with_distinguish_partition_range(distinguish_partition_range),
        );
    }

    pub fn time_index(&self) -> String {
        self.scanner
            .lock()
            .unwrap()
            .metadata()
            .time_index_column()
            .column_schema
            .name
            .clone()
    }

    pub fn tag_columns(&self) -> Vec<String> {
        self.scanner
            .lock()
            .unwrap()
            .metadata()
            .primary_key_columns()
            .map(|col| col.column_schema.name.clone())
            .collect()
    }

    pub fn set_explain_verbose(&mut self, explain_verbose: bool) {
        self.explain_verbose = explain_verbose;
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

        let ctx = QueryScanContext {
            explain_verbose: self.explain_verbose,
        };
        let stream = self
            .scanner
            .lock()
            .unwrap()
            .scan_partition(&ctx, &self.metric, partition)
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

    fn statistics(&self) -> DfResult<Statistics> {
        let statistics = if self.append_mode && !self.scanner.lock().unwrap().has_predicate() {
            let column_statistics = self
                .arrow_schema
                .fields
                .iter()
                .map(|_| ColumnStatistics {
                    distinct_count: Precision::Exact(self.total_rows),
                    null_count: Precision::Exact(0), // all null rows are counted for append-only table
                    ..Default::default()
                })
                .collect();
            Statistics {
                num_rows: Precision::Exact(self.total_rows),
                total_byte_size: Default::default(),
                column_statistics,
            }
        } else {
            Statistics::new_unknown(&self.arrow_schema)
        };
        Ok(statistics)
    }

    fn name(&self) -> &str {
        "RegionScanExec"
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

    use api::v1::SemanticType;
    use common_recordbatch::{RecordBatch, RecordBatches};
    use datafusion::prelude::SessionContext;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
    use datatypes::vectors::{Int32Vector, TimestampMillisecondVector};
    use futures::TryStreamExt;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::region_engine::SinglePartitionScanner;
    use store_api::storage::RegionId;

    use super::*;

    #[tokio::test]
    async fn test_simple_table_scan() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), false),
            ColumnSchema::new(
                "b",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ]));

        let batch1 = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(Int32Vector::from_slice([1, 2])) as _,
                Arc::new(TimestampMillisecondVector::from_slice([1000, 2000])) as _,
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(Int32Vector::from_slice([3, 4, 5])) as _,
                Arc::new(TimestampMillisecondVector::from_slice([3000, 4000, 5000])) as _,
            ],
        )
        .unwrap();

        let recordbatches =
            RecordBatches::try_new(schema.clone(), vec![batch1.clone(), batch2.clone()]).unwrap();
        let stream = recordbatches.as_stream();

        let mut builder = RegionMetadataBuilder::new(RegionId::new(1234, 5678));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("a", ConcreteDataType::int32_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "b",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .primary_key(vec![1]);
        let region_metadata = Arc::new(builder.build().unwrap());

        let scanner = Box::new(SinglePartitionScanner::new(stream, false, region_metadata));
        let plan = RegionScanExec::new(scanner, ScanRequest::default()).unwrap();
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
        assert!(result.is_ok());
    }
}
