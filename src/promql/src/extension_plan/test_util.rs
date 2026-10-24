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

//! Utils for testing extension plan

use std::collections::VecDeque;
use std::fmt::{self, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use common_query::native_histogram::{NativeHistogram, Span, UNKNOWN_COUNTER_RESET_HINT};
use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
use common_recordbatch::DfRecordBatch as RecordBatch;
use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Schema, SchemaRef, TimestampMillisecondType,
};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datatypes::arrow::array::TimestampMillisecondArray;
use datatypes::arrow_array::StringArray;
use futures::Stream;

pub(crate) const TIME_INDEX_COLUMN: &str = "timestamp";

pub(crate) fn native_histogram(sum: f64) -> NativeHistogram {
    NativeHistogram {
        schema: 0,
        zero_threshold: 0.0,
        sum,
        reset_hint: UNKNOWN_COUNTER_RESET_HINT,
        start_timestamp: None,
        custom_values: Vec::new(),
        positive_spans: vec![Span {
            offset: 0,
            length: 1,
        }],
        negative_spans: Vec::new(),
        count: 1.0,
        zero_count: 0.0,
        positive_buckets: vec![1.0],
        negative_buckets: Vec::new(),
    }
}

pub(crate) fn prepare_test_data() -> DataSourceExec {
    let schema = Arc::new(Schema::new(vec![
        Field::new(TIME_INDEX_COLUMN, TimestampMillisecondType::DATA_TYPE, true),
        Field::new("value", DataType::Float64, true),
        Field::new("path", DataType::Utf8, true),
    ]));
    let timestamp_column = Arc::new(TimestampMillisecondArray::from(vec![
        0, 30_000, 60_000, 90_000, 120_000, // every 30s
        180_000, 240_000, // every 60s
        241_000, 271_000, 291_000, // others
    ])) as _;
    let field_column = Arc::new(Float64Array::from(vec![1.0; 10])) as _;
    let path_column = Arc::new(StringArray::from(vec!["foo"; 10])) as _;
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![timestamp_column, field_column, path_column],
    )
    .unwrap();

    DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&[vec![data]], schema, None).unwrap(),
    ))
}

pub(crate) fn prepare_test_data_with_stale_marker() -> DataSourceExec {
    let schema = Arc::new(Schema::new(vec![
        Field::new(TIME_INDEX_COLUMN, TimestampMillisecondType::DATA_TYPE, true),
        Field::new("value", DataType::Float64, true),
    ]));
    let timestamp_column = Arc::new(TimestampMillisecondArray::from(vec![
        0, 30_000, 60_000, 90_000, 120_000, // every 30s
    ])) as _;
    let field_column = Arc::new(Float64Array::from(vec![
        0.0,
        f64::from_bits(PROMETHEUS_STALE_NAN_BITS),
        6.0,
        f64::from_bits(PROMETHEUS_STALE_NAN_BITS),
        12.0,
    ])) as _;
    let data = RecordBatch::try_new(schema.clone(), vec![timestamp_column, field_column]).unwrap();

    DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&[vec![data]], schema, None).unwrap(),
    ))
}

/// A task context whose memory pool is `pool`.
pub(crate) fn task_context(pool: Arc<dyn MemoryPool>) -> Arc<TaskContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build()
        .expect("the runtime environment builds");
    Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)))
}

/// A physical input whose one partition's stream `make` builds.
pub(crate) fn staged(
    schema: SchemaRef,
    make: impl Fn() -> SendableRecordBatchStream + Send + Sync + 'static,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(StagedPlan {
        properties: Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )),
        make: Box::new(make),
    })
}

/// A physical input, so a test can stage what a memory source cannot: a pending poll, an error
/// after some rows, and a stream whose drop it can see.
struct StagedPlan {
    make: Box<dyn Fn() -> SendableRecordBatchStream + Send + Sync>,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for StagedPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("StagedPlan").finish()
    }
}

impl DisplayAs for StagedPlan {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> fmt::Result {
        Ok(())
    }
}

impl ExecutionPlan for StagedPlan {
    fn name(&self) -> &str {
        "StagedPlan"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // A scan answers the partitions it declares and no others, which is what makes an index
        // past this plan's count an error the node above has to pass on.
        if partition >= self.properties.partitioning.partition_count() {
            return Err(DataFusionError::Execution(format!(
                "StagedPlan has no partition {partition}"
            )));
        }
        Ok((self.make)())
    }
}

/// The stream a [`StagedPlan`] hands over: `batches`, then an optional error, with a pending poll,
/// a drop and a failure a test can observe.
pub(crate) struct StagedStream {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
    handed: usize,
    /// Hand this many batches, then answer [`Poll::Pending`] once.
    pending_after: Option<usize>,
    /// The error to report once the batches are exhausted.
    error: Option<&'static str>,
    /// Set when this stream is dropped, so a test can see the input being released.
    dropped: Option<Arc<AtomicBool>>,
    /// Counts the batches this stream has handed over, so a test can see how far one poll read.
    counted: Option<Arc<AtomicUsize>>,
    /// Set when this stream reports its error, so a test can see it was read to its failure.
    failed: Option<Arc<AtomicBool>>,
}

impl StagedStream {
    pub(crate) fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches: batches.into(),
            handed: 0,
            pending_after: None,
            error: None,
            dropped: None,
            counted: None,
            failed: None,
        }
    }

    pub(crate) fn pending_after(mut self, batches: usize) -> Self {
        self.pending_after = Some(batches);
        self
    }

    pub(crate) fn error(mut self, message: &'static str) -> Self {
        self.error = Some(message);
        self
    }

    /// Sets `flag` when this stream is dropped, so a test can see the input being released.
    pub(crate) fn dropped(mut self, flag: &Arc<AtomicBool>) -> Self {
        self.dropped = Some(Arc::clone(flag));
        self
    }

    /// Counts each batch handed over into `counter`, so a test can see how far one poll read.
    pub(crate) fn counted(mut self, counter: &Arc<AtomicUsize>) -> Self {
        self.counted = Some(Arc::clone(counter));
        self
    }

    /// Sets `flag` when this stream reports its error, so a test can see it was read to its failure.
    pub(crate) fn failed(mut self, flag: &Arc<AtomicBool>) -> Self {
        self.failed = Some(Arc::clone(flag));
        self
    }
}

impl Stream for StagedStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.pending_after == Some(this.handed) {
            this.pending_after = None;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        match this.batches.pop_front() {
            Some(batch) => {
                this.handed += 1;
                if let Some(counter) = &this.counted {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
                Poll::Ready(Some(Ok(batch)))
            }
            None => match this.error.take() {
                Some(message) => {
                    if let Some(failed) = &this.failed {
                        failed.store(true, Ordering::SeqCst);
                    }
                    Poll::Ready(Some(Err(DataFusionError::Execution(message.to_string()))))
                }
                None => Poll::Ready(None),
            },
        }
    }
}

impl RecordBatchStream for StagedStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for StagedStream {
    fn drop(&mut self) {
        if let Some(dropped) = &self.dropped {
            dropped.store(true, Ordering::SeqCst);
        }
    }
}
