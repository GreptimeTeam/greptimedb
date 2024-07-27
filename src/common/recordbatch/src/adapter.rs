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

use std::fmt::Display;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef as DfSchemaRef;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::metrics::{BaselineMetrics, MetricValue};
use datafusion::physical_plan::{
    accept, displayable, ExecutionPlan, ExecutionPlanVisitor, PhysicalExpr,
    RecordBatchStream as DfRecordBatchStream,
};
use datafusion_common::arrow::error::ArrowError;
use datafusion_common::{DataFusionError, ToDFSchema};
use datatypes::arrow::array::Array;
use datatypes::schema::{Schema, SchemaRef};
use futures::ready;
use pin_project::pin_project;
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::filter::batch_filter;
use crate::{
    DfRecordBatch, DfSendableRecordBatchStream, OrderOption, RecordBatch, RecordBatchStream,
    SendableRecordBatchStream, Stream,
};

type FutureStream =
    Pin<Box<dyn std::future::Future<Output = Result<SendableRecordBatchStream>> + Send>>;

/// Casts the `RecordBatch`es of `stream` against the `output_schema`.
#[pin_project]
pub struct RecordBatchStreamTypeAdapter<T, E> {
    #[pin]
    stream: T,
    projected_schema: DfSchemaRef,
    projection: Vec<usize>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    phantom: PhantomData<E>,
}

impl<T, E> RecordBatchStreamTypeAdapter<T, E>
where
    T: Stream<Item = std::result::Result<DfRecordBatch, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    pub fn new(projected_schema: DfSchemaRef, stream: T, projection: Option<Vec<usize>>) -> Self {
        let projection = if let Some(projection) = projection {
            projection
        } else {
            (0..projected_schema.fields().len()).collect()
        };

        Self {
            stream,
            projected_schema,
            projection,
            predicate: None,
            phantom: Default::default(),
        }
    }

    pub fn with_filter(mut self, filters: Vec<Expr>) -> Result<Self> {
        let filters = if let Some(expr) = conjunction(filters) {
            let df_schema = self
                .projected_schema
                .clone()
                .to_dfschema_ref()
                .context(error::PhysicalExprSnafu)?;

            let filters = create_physical_expr(&expr, &df_schema, &ExecutionProps::new())
                .context(error::PhysicalExprSnafu)?;
            Some(filters)
        } else {
            None
        };
        self.predicate = filters;
        Ok(self)
    }
}

impl<T, E> DfRecordBatchStream for RecordBatchStreamTypeAdapter<T, E>
where
    T: Stream<Item = std::result::Result<DfRecordBatch, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    fn schema(&self) -> DfSchemaRef {
        self.projected_schema.clone()
    }
}

impl<T, E> Stream for RecordBatchStreamTypeAdapter<T, E>
where
    T: Stream<Item = std::result::Result<DfRecordBatch, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    type Item = DfResult<DfRecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let batch = futures::ready!(this.stream.poll_next(cx))
            .map(|r| r.map_err(|e| DataFusionError::External(Box::new(e))));

        let projected_schema = this.projected_schema.clone();
        let projection = this.projection.clone();
        let predicate = this.predicate.clone();

        let batch = batch.map(|b| {
            b.and_then(|b| {
                let projected_column = b.project(&projection)?;
                if projected_column.schema().fields.len() != projected_schema.fields.len() {
                   return Err(DataFusionError::ArrowError(ArrowError::SchemaError(format!(
                        "Trying to cast a RecordBatch into an incompatible schema. RecordBatch: {}, Target: {}",
                        projected_column.schema(),
                        projected_schema,
                    )), None));
                }

                let mut columns = Vec::with_capacity(projected_schema.fields.len());
                for (idx,field) in projected_schema.fields.iter().enumerate() {
                    let column = projected_column.column(idx);
                    if column.data_type() != field.data_type() {
                        let output = cast(&column, field.data_type())?;
                        columns.push(output)
                    } else {
                        columns.push(column.clone())
                    }
                }
                let record_batch = DfRecordBatch::try_new(projected_schema, columns)?;
                let record_batch = if let Some(predicate) = predicate {
                    batch_filter(&record_batch, &predicate)?
                } else {
                    record_batch
                };
                Ok(record_batch)
            })
        });

        Poll::Ready(batch)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

/// Greptime SendableRecordBatchStream -> DataFusion RecordBatchStream.
/// The reverse one is [RecordBatchStreamAdapter].
pub struct DfRecordBatchStreamAdapter {
    stream: SendableRecordBatchStream,
}

impl DfRecordBatchStreamAdapter {
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        Self { stream }
    }
}

impl DfRecordBatchStream for DfRecordBatchStreamAdapter {
    fn schema(&self) -> DfSchemaRef {
        self.stream.schema().arrow_schema().clone()
    }
}

impl Stream for DfRecordBatchStreamAdapter {
    type Item = DfResult<DfRecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(recordbatch)) => match recordbatch {
                Ok(recordbatch) => Poll::Ready(Some(Ok(recordbatch.into_df_record_batch()))),
                Err(e) => Poll::Ready(Some(Err(DataFusionError::External(Box::new(e))))),
            },
            Poll::Ready(None) => Poll::Ready(None),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

/// DataFusion [SendableRecordBatchStream](DfSendableRecordBatchStream) -> Greptime [RecordBatchStream].
/// The reverse one is [DfRecordBatchStreamAdapter]
pub struct RecordBatchStreamAdapter {
    schema: SchemaRef,
    stream: DfSendableRecordBatchStream,
    metrics: Option<BaselineMetrics>,
    /// Aggregated plan-level metrics. Resolved after an [ExecutionPlan] is finished.
    metrics_2: Metrics,
}

/// Json encoded metrics. Contains metric from a whole plan tree.
enum Metrics {
    Unavailable,
    Unresolved(Arc<dyn ExecutionPlan>),
    Resolved(RecordBatchMetrics),
}

impl RecordBatchStreamAdapter {
    pub fn try_new(stream: DfSendableRecordBatchStream) -> Result<Self> {
        let schema =
            Arc::new(Schema::try_from(stream.schema()).context(error::SchemaConversionSnafu)?);
        Ok(Self {
            schema,
            stream,
            metrics: None,
            metrics_2: Metrics::Unavailable,
        })
    }

    pub fn try_new_with_metrics_and_df_plan(
        stream: DfSendableRecordBatchStream,
        metrics: BaselineMetrics,
        df_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let schema =
            Arc::new(Schema::try_from(stream.schema()).context(error::SchemaConversionSnafu)?);
        Ok(Self {
            schema,
            stream,
            metrics: Some(metrics),
            metrics_2: Metrics::Unresolved(df_plan),
        })
    }

    pub fn set_metrics2(&mut self, plan: Arc<dyn ExecutionPlan>) {
        self.metrics_2 = Metrics::Unresolved(plan)
    }
}

impl RecordBatchStream for RecordBatchStreamAdapter {
    fn name(&self) -> &str {
        "RecordBatchStreamAdapter"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        match &self.metrics_2 {
            Metrics::Resolved(metrics) => Some(metrics.clone()),
            Metrics::Unavailable | Metrics::Unresolved(_) => None,
        }
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        None
    }
}

impl Stream for RecordBatchStreamAdapter {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let timer = self
            .metrics
            .as_ref()
            .map(|m| m.elapsed_compute().clone())
            .unwrap_or_default();
        let _guard = timer.timer();
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(df_record_batch)) => {
                let df_record_batch = df_record_batch.context(error::PollStreamSnafu)?;
                Poll::Ready(Some(RecordBatch::try_from_df_record_batch(
                    self.schema(),
                    df_record_batch,
                )))
            }
            Poll::Ready(None) => {
                if let Metrics::Unresolved(df_plan) = &self.metrics_2 {
                    let mut metric_collector = MetricCollector::default();
                    accept(df_plan.as_ref(), &mut metric_collector).unwrap();
                    self.metrics_2 = Metrics::Resolved(metric_collector.record_batch_metrics);
                }
                Poll::Ready(None)
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

/// An [ExecutionPlanVisitor] to collect metrics from a [ExecutionPlan].
#[derive(Default)]
pub struct MetricCollector {
    current_level: usize,
    pub record_batch_metrics: RecordBatchMetrics,
}

impl ExecutionPlanVisitor for MetricCollector {
    type Error = !;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> std::result::Result<bool, Self::Error> {
        // skip if no metric available
        let Some(metric) = plan.metrics() else {
            self.record_batch_metrics.plan_metrics.push(PlanMetrics {
                plan: std::any::type_name::<Self>().to_string(),
                level: self.current_level,
                metrics: vec![],
            });
            return Ok(true);
        };

        // scrape plan metrics
        let metric = metric
            .aggregate_by_name()
            .sorted_for_display()
            .timestamps_removed();
        let mut plan_metric = PlanMetrics {
            plan: displayable(plan).one_line().to_string(),
            level: self.current_level,
            metrics: Vec::with_capacity(metric.iter().size_hint().0),
        };
        for m in metric.iter() {
            plan_metric
                .metrics
                .push((m.value().name().to_string(), m.value().as_usize()));

            // aggregate high-level metrics
            match m.value() {
                MetricValue::ElapsedCompute(ec) => {
                    self.record_batch_metrics.elapsed_compute += ec.value()
                }
                MetricValue::CurrentMemoryUsage(m) => {
                    self.record_batch_metrics.memory_usage += m.value()
                }
                _ => {}
            }
        }
        self.record_batch_metrics.plan_metrics.push(plan_metric);

        self.current_level += 1;
        Ok(true)
    }

    fn post_visit(&mut self, _plan: &dyn ExecutionPlan) -> std::result::Result<bool, Self::Error> {
        // the last minus will underflow
        self.current_level = self.current_level.wrapping_sub(1);
        Ok(true)
    }
}

/// [`RecordBatchMetrics`] carrys metrics value
/// from datanode to frontend through gRPC
#[derive(serde::Serialize, serde::Deserialize, Default, Debug, Clone)]
pub struct RecordBatchMetrics {
    // High-level aggregated metrics
    /// CPU consumption in nanoseconds
    pub elapsed_compute: usize,
    /// Memory used by the plan in bytes
    pub memory_usage: usize,
    // Detailed per-plan metrics
    /// An ordered list of plan metrics, from top to bottom in post-order.
    pub plan_metrics: Vec<PlanMetrics>,
}

/// Only display `plan_metrics` with indent `  ` (2 spaces).
impl Display for RecordBatchMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for metric in &self.plan_metrics {
            write!(
                f,
                "{:indent$}{} metrics=[",
                " ",
                metric.plan.trim_end(),
                indent = metric.level * 2,
            )?;
            for (label, value) in &metric.metrics {
                write!(f, "{}: {}, ", label, value)?;
            }
            writeln!(f, "]")?;
        }

        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Default, Debug, Clone)]
pub struct PlanMetrics {
    /// The plan name
    pub plan: String,
    /// The level of the plan, starts from 0
    pub level: usize,
    /// An ordered key-value list of metrics.
    /// Key is metric label and value is metric value.
    pub metrics: Vec<(String, usize)>,
}

enum AsyncRecordBatchStreamAdapterState {
    Uninit(FutureStream),
    Ready(SendableRecordBatchStream),
    Failed,
}

pub struct AsyncRecordBatchStreamAdapter {
    schema: SchemaRef,
    state: AsyncRecordBatchStreamAdapterState,
}

impl AsyncRecordBatchStreamAdapter {
    pub fn new(schema: SchemaRef, stream: FutureStream) -> Self {
        Self {
            schema,
            state: AsyncRecordBatchStreamAdapterState::Uninit(stream),
        }
    }
}

impl RecordBatchStream for AsyncRecordBatchStreamAdapter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        None
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        None
    }
}

impl Stream for AsyncRecordBatchStreamAdapter {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                AsyncRecordBatchStreamAdapterState::Uninit(stream_future) => {
                    match ready!(Pin::new(stream_future).poll(cx)) {
                        Ok(stream) => {
                            self.state = AsyncRecordBatchStreamAdapterState::Ready(stream);
                            continue;
                        }
                        Err(e) => {
                            self.state = AsyncRecordBatchStreamAdapterState::Failed;
                            return Poll::Ready(Some(Err(e)));
                        }
                    };
                }
                AsyncRecordBatchStreamAdapterState::Ready(stream) => {
                    return Poll::Ready(ready!(Pin::new(stream).poll_next(cx)))
                }
                AsyncRecordBatchStreamAdapterState::Failed => return Poll::Ready(None),
            }
        }
    }

    // This is not supported for lazy stream.
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

#[cfg(test)]
mod test {
    use common_error::ext::BoxedError;
    use common_error::mock::MockError;
    use common_error::status_code::StatusCode;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::vectors::Int32Vector;
    use snafu::IntoError;

    use super::*;
    use crate::error::Error;
    use crate::RecordBatches;

    #[tokio::test]
    async fn test_async_recordbatch_stream_adaptor() {
        struct MaybeErrorRecordBatchStream {
            items: Vec<Result<RecordBatch>>,
        }

        impl RecordBatchStream for MaybeErrorRecordBatchStream {
            fn schema(&self) -> SchemaRef {
                unimplemented!()
            }

            fn output_ordering(&self) -> Option<&[OrderOption]> {
                None
            }

            fn metrics(&self) -> Option<RecordBatchMetrics> {
                None
            }
        }

        impl Stream for MaybeErrorRecordBatchStream {
            type Item = Result<RecordBatch>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                if let Some(batch) = self.items.pop() {
                    Poll::Ready(Some(Ok(batch?)))
                } else {
                    Poll::Ready(None)
                }
            }
        }

        fn new_future_stream(
            maybe_recordbatches: Result<Vec<Result<RecordBatch>>>,
        ) -> FutureStream {
            Box::pin(async move {
                maybe_recordbatches
                    .map(|items| Box::pin(MaybeErrorRecordBatchStream { items }) as _)
            })
        }

        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let batch1 = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice([1])) as _],
        )
        .unwrap();
        let batch2 = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice([2])) as _],
        )
        .unwrap();

        let success_stream = new_future_stream(Ok(vec![Ok(batch1.clone()), Ok(batch2.clone())]));
        let adapter = AsyncRecordBatchStreamAdapter::new(schema.clone(), success_stream);
        let collected = RecordBatches::try_collect(Box::pin(adapter)).await.unwrap();
        assert_eq!(
            collected,
            RecordBatches::try_new(schema.clone(), vec![batch2.clone(), batch1.clone()]).unwrap()
        );

        let poll_err_stream = new_future_stream(Ok(vec![
            Ok(batch1.clone()),
            Err(error::ExternalSnafu
                .into_error(BoxedError::new(MockError::new(StatusCode::Unknown)))),
        ]));
        let adapter = AsyncRecordBatchStreamAdapter::new(schema.clone(), poll_err_stream);
        let err = RecordBatches::try_collect(Box::pin(adapter))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::External { .. }),
            "unexpected err {err}"
        );

        let failed_to_init_stream =
            new_future_stream(Err(error::ExternalSnafu
                .into_error(BoxedError::new(MockError::new(StatusCode::Internal)))));
        let adapter = AsyncRecordBatchStreamAdapter::new(schema.clone(), failed_to_init_stream);
        let err = RecordBatches::try_collect(Box::pin(adapter))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::External { .. }),
            "unexpected err {err}"
        );
    }
}
