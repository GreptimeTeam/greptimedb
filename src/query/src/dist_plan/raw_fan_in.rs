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

//! Fan-in for already-authorized native raw-series region reads.

use std::sync::Arc;
use std::task::{Context, Poll};

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_query::request::{QueryRequest, RawSeriesRead, RawSeriesReadColumn};
use common_recordbatch::adapter::{DfRecordBatchStreamAdapter, RecordBatchMetrics};
use common_recordbatch::{
    DfSendableRecordBatchStream, OrderOption, RecordBatch, RecordBatchStream,
    SendableRecordBatchStream,
};
use common_telemetry::tracing_context::TracingContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    apply_expression_roots, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result, Statistics};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_expr::PhysicalExpr;
use datatypes::arrow::datatypes::Schema as ArrowSchema;
use datatypes::series::frame::{
    FrameLabelMode, LabelDecodeColumn, LabelDecodeEncoding, LabelDecodeSpec, LabelDecodeTag,
};
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::SampleRef;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::SeriesOrdering;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use greptime_proto::v1::region::RegionRequestHeader;
use promql::source::merge_ordered_raw_series;
use session::context::QueryContextRef;
use store_api::region_engine::{RawLabelSource, RawSeriesStream, SendableRawSeriesStream};
use store_api::storage::RegionId;
use table::metadata::TableInfo;

use crate::dist_plan::merge_scan::validate_remote_schema;
use crate::region_query::{RegionQueryHandlerRef, RegionQueryTarget};

/// Bind the authorized logical table metadata into its physical raw-series read and decode schema.
pub(crate) fn bind_raw_series_table(
    table: &TableInfo,
    metric_name: &str,
    tag_names: &[String],
    field_names: &[String],
    region_id: RegionId,
) -> Result<(RawSeriesRead, SeriesSchema, LabelDecodeSpec)> {
    let fail = |message: &str| DataFusionError::Plan(message.to_string());
    let schema = &table.meta.schema;
    let ids = table
        .name_to_ids()
        .ok_or_else(|| fail("raw-series table metadata has incomplete column ids"))?;
    let bind = |index: usize| -> Result<RawSeriesReadColumn> {
        let column = schema
            .column_schemas()
            .get(index)
            .ok_or_else(|| fail("raw-series column index is invalid"))?;
        let column_id = *ids
            .get(&column.name)
            .ok_or_else(|| fail("raw-series column id is missing"))?;
        Ok(RawSeriesReadColumn {
            column_id,
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.is_nullable(),
        })
    };
    let time_index = schema
        .timestamp_index()
        .ok_or_else(|| fail("raw-series table has no time index"))?;
    let primary_key = table
        .meta
        .primary_key_indices
        .iter()
        .map(|&index| bind(index))
        .collect::<Result<Vec<_>>>()?;
    let key_indices = table
        .meta
        .primary_key_indices
        .iter()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    let mut logical_tags = Vec::with_capacity(tag_names.len());
    let mut tags = Vec::with_capacity(tag_names.len());
    for name in tag_names {
        let index = schema
            .column_schemas()
            .iter()
            .position(|column| column.name == *name)
            .ok_or_else(|| fail("raw-series tag column is missing"))?;
        if !key_indices.contains(&index) {
            return Err(fail("raw-series tag is not a primary-key column"));
        }
        let column = bind(index)?;
        logical_tags.push(LabelDecodeTag {
            column_id: column.column_id,
            name: column.name.clone(),
        });
        tags.push(column);
    }
    let mut fields = Vec::with_capacity(field_names.len());
    let mut series_fields = Vec::with_capacity(field_names.len());
    for name in field_names {
        let index = schema
            .column_schemas()
            .iter()
            .position(|column| column.name == *name)
            .ok_or_else(|| fail("raw-series field column is missing"))?;
        if key_indices.contains(&index) || index == time_index {
            return Err(fail("raw-series field has an invalid table role"));
        }
        let column = bind(index)?;
        if !column.data_type.is_numeric() {
            return Err(fail("raw-series field has an unsupported data type"));
        }
        series_fields.push(FieldSpec {
            name: Arc::from(column.name.as_str()),
        });
        fields.push(column);
    }
    if fields.is_empty() {
        return Err(fail("raw-series read requires fields"));
    }
    let primary_key_decode = primary_key
        .iter()
        .map(|column| LabelDecodeColumn {
            column_id: column.column_id,
            name: column.name.clone(),
            physical_type: column.data_type.clone(),
        })
        .collect();
    let mut decode = LabelDecodeSpec {
        version: datatypes::series::frame::LABEL_DECODE_VERSION,
        metric_name: metric_name.to_string(),
        encoding: LabelDecodeEncoding::Dense,
        primary_key: primary_key_decode,
        logical_tags,
    };
    decode.canonicalize();
    decode
        .validate()
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;
    let request = RawSeriesRead {
        version: 1,
        region_id,
        metric_name: metric_name.to_string(),
        primary_key,
        time_index: bind(time_index)?,
        tags,
        fields,
    };
    Ok((
        request,
        SeriesSchema {
            fields: series_fields.into(),
            eval_grid: None,
        },
        decode,
    ))
}

/// One frozen destination in an authorized native raw-series fan-in.
pub(crate) type FrozenRawRegion = (RegionId, RegionQueryTarget);

/// A leaf physical operator that fans in regions under an already-authorized read binding.
pub(crate) struct NativeRawFanInExec {
    template: RawSeriesRead,
    expected_raw_schema: Arc<SeriesSchema>,
    expected_decode: LabelDecodeSpec,
    regions: Vec<FrozenRawRegion>,
    plan: datafusion_expr::LogicalPlan,
    handler: RegionQueryHandlerRef,
    query_ctx: QueryContextRef,
    schema: datatypes::arrow::datatypes::SchemaRef,
    remote_schema: datatypes::arrow::datatypes::SchemaRef,
    properties: Arc<PlanProperties>,
}

fn fan_in_frame_schema(
    raw_schema: &SeriesSchema,
    decode: &LabelDecodeSpec,
) -> Result<datatypes::arrow::datatypes::SchemaRef> {
    let basic = datatypes::series::frame::series_frame_schema(
        raw_schema,
        FrameLabelMode::Key,
        Some(decode),
    )
    .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let mut metadata = basic.metadata().clone();
    metadata.insert(
        "greptime.series.ordering".into(),
        "read_key_then_time".into(),
    );
    Ok(Arc::new(ArrowSchema::new_with_metadata(
        basic.fields().clone(),
        metadata,
    )))
}

impl NativeRawFanInExec {
    pub(crate) fn try_new(
        template: RawSeriesRead,
        expected_raw_schema: Arc<SeriesSchema>,
        expected_decode: LabelDecodeSpec,
        regions: Vec<FrozenRawRegion>,
        plan: datafusion_expr::LogicalPlan,
        handler: RegionQueryHandlerRef,
        query_ctx: QueryContextRef,
    ) -> Result<Self> {
        if expected_decode.encoding != datatypes::series::frame::LabelDecodeEncoding::Dense
            || template.version != 1
            || template.metric_name != expected_decode.metric_name
            || template.primary_key.len() != expected_decode.primary_key.len()
        {
            return Err(DataFusionError::Plan(
                "native raw fan-in requires dense keys".into(),
            ));
        }
        let remote_schema = fan_in_frame_schema(&expected_raw_schema, &expected_decode)?;
        let schema = remote_schema.clone();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            template,
            expected_raw_schema,
            expected_decode,
            regions,
            plan,
            handler,
            query_ctx,
            schema,
            remote_schema,
            properties,
        })
    }

    fn stream(
        &self,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> Result<DfSendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "invalid NativeRawFanInExec partition {partition}"
            )));
        }
        if self.regions.is_empty() {
            return Ok(Box::pin(
                datafusion::physical_plan::EmptyRecordBatchStream::new(self.schema.clone()),
            ));
        }
        let expected_schema = self.expected_raw_schema.clone();
        let template = self.template.clone();
        let expected_decode = self.expected_decode.clone();
        let regions = self.regions.clone();
        let plan = self.plan.clone();
        let handler = self.handler.clone();
        let query_ctx = self.query_ctx.clone();
        let task_id = context.task_id().unwrap_or_default();
        let tracing = TracingContext::from_json(context.session_id().as_str());
        let frame_schema = self.remote_schema.clone();
        // Kept until merge completion: conservative accounting for all buffered chunks and merge heads.
        let reservation = MemoryConsumer::new("NativeRawFanInExec").register(context.memory_pool());
        // Every region starts unrequested; the first poll issues all of their opens together.
        let states = (0..regions.len()).map(|_| RegionState::Pending).collect();
        let raw_stream = FanInRawStream {
            expected_schema: self.expected_raw_schema.clone(),
            expected_decode: self.expected_decode.clone(),
            regions,
            states,
            opening: FuturesUnordered::new(),
            merged: None,
            reservation: Some(reservation),
            open_params: Some((
                template,
                expected_schema,
                expected_decode,
                plan,
                handler,
                query_ctx,
                task_id,
                tracing,
                frame_schema,
                context.memory_pool().clone(),
            )),
            finished: false,
        };
        common_query::series_frame::pack_raw_series(Box::pin(raw_stream))
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }
}

/// One region's `do_get` future, paired with the region index it was issued for.
///
/// The future runs to completion in [`FanInRawStream::opening`], so a region is never opened only
/// after an earlier region reached EOF: every selected region is requested at once.
type OpenFuture = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = (
                    usize,
                    std::result::Result<SendableRawSeriesStream, BoxedError>,
                ),
            > + Send,
    >,
>;
type OpenParams = (
    RawSeriesRead,
    Arc<SeriesSchema>,
    LabelDecodeSpec,
    datafusion::logical_expr::LogicalPlan,
    RegionQueryHandlerRef,
    QueryContextRef,
    String,
    TracingContext,
    datatypes::arrow::datatypes::SchemaRef,
    Arc<dyn MemoryPool>,
);

/// A selected region's position in the fan-in.
///
/// Every opened stream is drained on every poll until its own EOF, so an opened stream is never
/// left unpolled while another region is still being opened: that hold-and-wait is what the
/// parallel opening exists to avoid.
enum RegionState {
    /// Not requested yet.
    Pending,
    /// Its `do_get` future is in flight; it lives in [`FanInRawStream::opening`], tagged with this
    /// region's index.
    Opening,
    /// Opened: `buffered` holds the chunks drained so far and `stream` is still producing.
    Live {
        stream: SendableRawSeriesStream,
        buffered: Vec<RawSeriesChunk>,
    },
    /// Reached EOF: the stream was dropped so its datanode read permit came back, and the drained
    /// chunks remain for the merge.
    Done(Vec<RawSeriesChunk>),
}

struct FanInRawStream {
    expected_schema: Arc<SeriesSchema>,
    expected_decode: LabelDecodeSpec,
    regions: Vec<FrozenRawRegion>,
    states: Vec<RegionState>,
    opening: FuturesUnordered<OpenFuture>,
    merged: Option<SendableRawSeriesStream>,
    reservation: Option<MemoryReservation>,
    open_params: Option<OpenParams>,
    finished: bool,
}

/// Hands a region's drained chunks to the merge, then forwards the rest of its live stream.
///
/// The tail keeps producing while the merge runs, so the merge sees a stream of chunks rather than a
/// materialized region; `None` means the region already reached EOF and the replay is complete. The
/// schema and decode declaration are the region's own, so buffered and live chunks pass through the
/// merge under the frame contract they arrived in.
struct BufferedThenLiveRawSeriesStream {
    schema: Arc<SeriesSchema>,
    decode: LabelDecodeSpec,
    chunks: std::vec::IntoIter<RawSeriesChunk>,
    tail: Option<SendableRawSeriesStream>,
}

impl futures_util::Stream for BufferedThenLiveRawSeriesStream {
    type Item = std::result::Result<RawSeriesChunk, BoxedError>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(chunk) = this.chunks.next() {
            return Poll::Ready(Some(Ok(chunk)));
        }
        match this.tail.as_mut() {
            Some(tail) => tail.as_mut().poll_next(cx),
            None => Poll::Ready(None),
        }
    }
}

impl RawSeriesStream for BufferedThenLiveRawSeriesStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }

    /// Delegates to the live tail, which is the region stream the buffered chunks came from; a
    /// completed replay reports the same value the replay of a drained region always did.
    fn label_source(&self) -> &RawLabelSource {
        match self.tail.as_ref() {
            Some(tail) => tail.label_source(),
            None => &store_api::region_engine::NO_BOUND_LABEL_SOURCE,
        }
    }

    /// Delegates to the live tail for the same reason as [`Self::label_source`].
    fn label_decode_spec(&self) -> Option<&LabelDecodeSpec> {
        match self.tail.as_ref() {
            Some(tail) => tail.label_decode_spec(),
            None => Some(&self.decode),
        }
    }

    fn ordering(&self) -> SeriesOrdering {
        SeriesOrdering::KeyThenTime
    }
}

/// Account timestamp/key/label buffers plus samples. Float allowance includes values and validity;
/// histograms include per-position structure and all bucket, custom-value, and span elements.
fn raw_chunk_held_bytes(chunk: &RawSeriesChunk) -> usize {
    let mut samples = 0usize;
    for field in chunk.fields.iter() {
        for i in 0..field.len() {
            samples = samples.saturating_add(match field.sample(i) {
                SampleRef::Float(_) | SampleRef::Missing => 9,
                SampleRef::Histogram(h) => 64usize
                    .saturating_add(h.custom_values.len().saturating_mul(8))
                    .saturating_add(h.positive.span_offsets.len().saturating_mul(8))
                    .saturating_add(h.positive.span_lengths.len().saturating_mul(4))
                    .saturating_add(h.positive.buckets.len().saturating_mul(8))
                    .saturating_add(h.negative.span_offsets.len().saturating_mul(8))
                    .saturating_add(h.negative.span_lengths.len().saturating_mul(4))
                    .saturating_add(h.negative.buckets.len().saturating_mul(8)),
            });
        }
    }
    let labels = chunk.metric.as_ref().map_or(0, |m| {
        m.name().map_or(0, str::len)
            + m.tags()
                .iter()
                .map(|l| l.name.len() + l.value.len())
                .sum::<usize>()
    });
    chunk
        .time
        .len()
        .saturating_mul(8)
        .saturating_add(samples)
        .saturating_add(chunk.primary_key.len())
        .saturating_add(labels)
}

impl FanInRawStream {
    /// Issues a `do_get` for every region still `Pending`, so the whole selected set is requested
    /// at once instead of one region per completed predecessor.
    fn start_pending_opens(&mut self) {
        if !self
            .states
            .iter()
            .any(|state| matches!(state, RegionState::Pending))
        {
            return;
        }
        let Some((
            template,
            expected_schema,
            expected_decode,
            plan,
            handler,
            query_ctx,
            task_id,
            tracing,
            frame_schema,
            pool,
        )) = self.open_params.as_ref().cloned()
        else {
            return;
        };
        for index in 0..self.states.len() {
            if !matches!(self.states[index], RegionState::Pending) {
                continue;
            }
            self.states[index] = RegionState::Opening;
            let (region_id, target) = self.regions[index].clone();
            let template = template.clone();
            let plan = plan.clone();
            let handler = handler.clone();
            let query_ctx = query_ctx.clone();
            let task_id = task_id.clone();
            let tracing = tracing.clone();
            let frame_schema = frame_schema.clone();
            let expected_schema = expected_schema.clone();
            let expected_decode = expected_decode.clone();
            let pool = pool.clone();
            self.opening.push(Box::pin(async move {
                let mut ctx = query_ctx.fork();
                let mut read = template;
                read.region_id = region_id;
                let result = match read.encode() {
                    Ok(encoded) => {
                        ctx.set_extension(RawSeriesRead::EXTENSION_KEY, encoded);
                        let request = QueryRequest {
                            header: Some(RegionRequestHeader {
                                tracing_context: tracing.to_w3c(),
                                dbname: task_id,
                                query_context: Some((&ctx).into()),
                            }),
                            region_id,
                            plan,
                        };
                        open_region(
                            handler.as_ref(),
                            &target,
                            request,
                            &frame_schema,
                            expected_schema,
                            expected_decode,
                            pool,
                        )
                        .await
                    }
                    Err(error) => Err(BoxedError::new(PlainError::new(
                        error.to_string(),
                        StatusCode::Unexpected,
                    ))),
                };
                (index, result)
            }));
        }
    }

    /// Ends the query, dropping every opening future and every live stream so all datanode read
    /// permits come back, and dropping the memory reservation.
    fn abandon(&mut self) {
        self.finished = true;
        self.reservation = None;
        self.merged = None;
        self.opening.clear();
        self.states.clear();
    }

    /// Reserves `bytes` for a chunk entering a region's buffer, or reports an explicit resource
    /// error naming the pool and the byte count.
    fn reserve(&mut self, bytes: usize) -> std::result::Result<(), BoxedError> {
        let Some(reservation) = self.reservation.as_mut() else {
            return Ok(());
        };
        reservation.try_grow(bytes).map_err(|e| {
            BoxedError::new(PlainError::new(
                format!("NativeRawFanInExec could not reserve {bytes} bytes: {e}"),
                StatusCode::RuntimeResourcesExhausted,
            ))
        })
    }

    /// Reports that no region is left `Pending` or `Opening`, so the merge may start.
    fn all_settled(&self) -> bool {
        !self.states.is_empty()
            && self
                .states
                .iter()
                .all(|state| matches!(state, RegionState::Live { .. } | RegionState::Done(_)))
    }

    /// Hands every region to the merge: its drained chunks first, then its still-live stream.
    fn hand_off_to_merge(&mut self) -> std::result::Result<(), BoxedError> {
        let schema = self.expected_schema.clone();
        let decode = self.expected_decode.clone();
        let inputs = std::mem::take(&mut self.states)
            .into_iter()
            .map(|state| {
                let (buffered, tail) = match state {
                    RegionState::Live { stream, buffered } => (buffered, Some(stream)),
                    RegionState::Done(buffered) => (buffered, None),
                    RegionState::Pending | RegionState::Opening => (Vec::new(), None),
                };
                Box::pin(BufferedThenLiveRawSeriesStream {
                    schema: schema.clone(),
                    decode: decode.clone(),
                    chunks: buffered.into_iter(),
                    tail,
                }) as SendableRawSeriesStream
            })
            .collect();
        match merge_ordered_raw_series(inputs) {
            Ok(merged) => {
                self.merged = Some(merged);
                Ok(())
            }
            Err(e) => Err(BoxedError::new(e)),
        }
    }
}

/// Charges each raw frame batch to the query memory pool while it is the in-flight one.
///
/// `unpack_raw_series` decodes a whole batch per poll and keeps the decoded chunks, and the Arrow
/// buffers behind them, until they are taken out; that window is invisible to the fan-in's
/// per-chunk reservation. This adapter charges the batch itself for as long as it is in flight and
/// passes it through unchanged, so the peak charge is about one batch per open region.
struct PoolChargedRecordBatchStream {
    stream: SendableRecordBatchStream,
    reservation: MemoryReservation,
    charged: usize,
}

impl PoolChargedRecordBatchStream {
    /// Drops the in-flight charge. Shrinking cannot be refused, and a stale size would only
    /// over-report held memory, so a failed shrink is not worth reporting.
    fn release(&mut self) {
        if self.charged > 0 {
            let _ = self.reservation.try_resize(0);
            self.charged = 0;
        }
    }

    /// Charges `batch` and reports an explicit resource error if the pool refuses it.
    fn charge(
        &mut self,
        batch: &RecordBatch,
    ) -> std::result::Result<(), common_recordbatch::error::Error> {
        let bytes = batch.df_record_batch().get_array_memory_size();
        if let Err(e) = self.reservation.try_grow(bytes) {
            return Err(common_recordbatch::error::Error::External {
                location: Default::default(),
                source: BoxedError::new(PlainError::new(
                    format!("NativeRawFanInExec could not reserve {bytes} bytes: {e}"),
                    StatusCode::RuntimeResourcesExhausted,
                )),
            });
        }
        self.charged = self.charged.saturating_add(bytes);
        Ok(())
    }
}

impl futures_util::Stream for PoolChargedRecordBatchStream {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                this.release();
                if let Err(error) = this.charge(&batch) {
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(error))) => {
                this.release();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                this.release();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for PoolChargedRecordBatchStream {
    fn schema(&self) -> datatypes::schema::SchemaRef {
        self.stream.schema()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.stream.output_ordering()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.stream.metrics()
    }
}

/// Runs one `do_get` and validates the returned frame against the authorized binding.
async fn open_region(
    handler: &dyn crate::region_query::RegionQueryHandler,
    target: &RegionQueryTarget,
    request: QueryRequest,
    frame_schema: &datatypes::arrow::datatypes::SchemaRef,
    expected_schema: Arc<SeriesSchema>,
    expected_decode: LabelDecodeSpec,
    pool: Arc<dyn MemoryPool>,
) -> std::result::Result<SendableRawSeriesStream, BoxedError> {
    let stream = handler
        .do_get(target, request)
        .await
        .map_err(BoxedError::new)?;
    validate_remote_schema(
        frame_schema,
        stream.schema().arrow_schema().as_ref(),
        "native raw advertised stream",
    )
    .map_err(|e| BoxedError::new(PlainError::new(e.to_string(), StatusCode::Unexpected)))?;
    let schema = stream.schema().arrow_schema().clone();
    // Charge the in-flight raw batch before it is decoded, so the chunks `unpack_raw_series` holds
    // between polls are accounted to the same pool as the chunks the fan-in buffers.
    let charged = Box::pin(PoolChargedRecordBatchStream {
        stream,
        reservation: MemoryConsumer::new("NativeRawFanInExec raw batches").register(&pool),
        charged: 0,
    }) as SendableRecordBatchStream;
    let raw = common_query::series_frame::unpack_raw_series(
        Box::pin(DfRecordBatchStreamAdapter::new(charged)),
        Some(expected_schema),
    )
    .map_err(|e| BoxedError::new(PlainError::new(e.to_string(), StatusCode::Unexpected)))?;
    let actual = datatypes::series::frame::series_frame_label_decode(schema.as_ref())
        .map_err(|e| BoxedError::new(PlainError::new(e.to_string(), StatusCode::Unexpected)))?;
    if actual.as_ref() != Some(&expected_decode) {
        return Err(BoxedError::new(PlainError::new(
            "native raw label decode mismatch".into(),
            StatusCode::Unexpected,
        )));
    }
    Ok(raw)
}

impl futures_util::Stream for FanInRawStream {
    type Item = std::result::Result<RawSeriesChunk, BoxedError>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        const BUDGET: usize = 64;
        if self.finished {
            return Poll::Ready(None);
        }
        loop {
            if let Some(merged) = self.merged.as_mut() {
                let result = merged.as_mut().poll_next(cx);
                // An error and an EOF are both terminal: either way this stream owned every
                // buffered chunk and every merge head, so the reservation is released here rather
                // than whenever the consumer happens to drop the stream.
                if matches!(result, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
                    self.finished = true;
                    self.reservation = None;
                    self.merged = None;
                }
                return result;
            }
            // Opens go out for every pending region before any completion is consumed, so the very
            // first attempt requests the whole selected set concurrently.
            self.start_pending_opens();
            loop {
                match self.opening.poll_next_unpin(cx) {
                    Poll::Ready(Some((index, Ok(stream)))) => {
                        self.states[index] = RegionState::Live {
                            stream,
                            buffered: Vec::new(),
                        };
                    }
                    Poll::Ready(Some((_, Err(error)))) => {
                        // A failed open is terminal: no partial result, every permit returns.
                        self.abandon();
                        return Poll::Ready(Some(Err(error)));
                    }
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }
            // Every live region is drained on every poll, so a region that is still opening never
            // stops an already-opened stream from making progress.
            let mut budget_hit = false;
            for index in 0..self.states.len() {
                // Take the pair out so `self.reservation` stays reachable while the stream borrows.
                let slot = std::mem::replace(&mut self.states[index], RegionState::Pending);
                let (mut stream, mut buffered) = match slot {
                    RegionState::Live { stream, buffered } => (stream, buffered),
                    other => {
                        self.states[index] = other;
                        continue;
                    }
                };
                let mut reached_eof = false;
                let mut polled = 0;
                while polled < BUDGET {
                    match stream.as_mut().poll_next(cx) {
                        Poll::Pending => break,
                        Poll::Ready(None) => {
                            reached_eof = true;
                            break;
                        }
                        Poll::Ready(Some(Err(error))) => {
                            // `abandon` clears the states, so this stream is dropped with them and
                            // its datanode permit returns.
                            self.states[index] = RegionState::Live { stream, buffered };
                            self.abandon();
                            return Poll::Ready(Some(Err(error)));
                        }
                        Poll::Ready(Some(Ok(chunk))) => {
                            polled += 1;
                            let bytes = raw_chunk_held_bytes(&chunk);
                            if let Err(error) = self.reserve(bytes) {
                                self.states[index] = RegionState::Live { stream, buffered };
                                self.abandon();
                                return Poll::Ready(Some(Err(error)));
                            }
                            buffered.push(chunk);
                        }
                    }
                }
                if reached_eof {
                    // Drop the stream at its own EOF: the datanode read permit comes back now
                    // rather than when the whole fan-in finishes.
                    drop(stream);
                    self.states[index] = RegionState::Done(buffered);
                } else {
                    budget_hit |= polled == BUDGET;
                    self.states[index] = RegionState::Live { stream, buffered };
                }
            }
            if self.all_settled() {
                if let Err(error) = self.hand_off_to_merge() {
                    self.abandon();
                    return Poll::Ready(Some(Err(error)));
                }
                continue;
            }
            if budget_hit {
                cx.waker().wake_by_ref();
            }
            return Poll::Pending;
        }
    }
}

impl RawSeriesStream for FanInRawStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        self.merged
            .as_ref()
            .map_or(&self.expected_schema, |stream| stream.schema())
    }
    fn label_source(&self) -> &RawLabelSource {
        &store_api::region_engine::NO_BOUND_LABEL_SOURCE
    }
    fn label_decode_spec(&self) -> Option<&LabelDecodeSpec> {
        Some(&self.expected_decode)
    }
    fn ordering(&self) -> SeriesOrdering {
        SeriesOrdering::KeyThenTime
    }
}

impl std::fmt::Debug for NativeRawFanInExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeRawFanInExec")
            .field("regions", &self.regions.len())
            .finish()
    }
}

impl ExecutionPlan for NativeRawFanInExec {
    fn name(&self) -> &str {
        "NativeRawFanInExec"
    }
    fn schema(&self) -> datatypes::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(std::iter::empty::<Arc<dyn PhysicalExpr>>(), f)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<DfSendableRecordBatchStream> {
        self.stream(context, partition)
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &datafusion::physical_plan::StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(self.schema.as_ref())))
    }
}

impl DisplayAs for NativeRawFanInExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "NativeRawFanInExec: regions={}", self.regions.len())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};

    use futures_util::{Stream, StreamExt};

    use super::*;

    fn table_info(primary_key_indices: Vec<usize>) -> TableInfo {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
        ]));
        let meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(primary_key_indices)
            .value_indices(vec![1, 2])
            .column_ids(vec![41, 42, 43])
            .next_column_id(44)
            .build()
            .unwrap();
        TableInfoBuilder::new("cpu_usage", meta).build().unwrap()
    }

    #[test]
    fn binds_raw_read_from_table_metadata() {
        let table = table_info(vec![0]);
        let (read, schema, decode) = bind_raw_series_table(
            &table,
            "cpu_usage",
            &["host".to_string()],
            &["value".to_string()],
            RegionId::new(4, 9),
        )
        .unwrap();
        assert_eq!(read.primary_key[0].column_id, 41);
        assert_eq!(read.time_index.column_id, 42);
        assert_eq!(read.tags[0].name, "host");
        assert_eq!(read.fields[0].column_id, 43);
        assert_eq!(schema.fields[0].name.as_ref(), "value");
        assert_eq!(
            decode.primary_key[0].physical_type,
            ConcreteDataType::string_datatype()
        );
        assert_eq!(decode.logical_tags[0].column_id, 41);
    }

    #[test]
    fn binds_raw_read_from_tagless_table() {
        let table = table_info(vec![]);
        let (read, schema, decode) = bind_raw_series_table(
            &table,
            "cpu_usage",
            &[],
            &["value".to_string()],
            RegionId::new(4, 9),
        )
        .unwrap();
        assert!(read.primary_key.is_empty());
        assert!(read.tags.is_empty());
        assert!(decode.primary_key.is_empty());
        assert!(decode.logical_tags.is_empty());
        assert_eq!(schema.fields[0].name.as_ref(), "value");
        let frame_schema = fan_in_frame_schema(&schema, &decode).unwrap();
        assert_eq!(
            datatypes::series::frame::series_frame_label_decode(&frame_schema)
                .unwrap()
                .as_ref(),
            Some(&decode)
        );
    }

    struct UnusedHandler;

    #[async_trait::async_trait]
    impl crate::region_query::RegionQueryHandler for UnusedHandler {
        async fn select_target(
            &self,
            _: session::ReadPreference,
            _: RegionId,
        ) -> crate::error::Result<crate::region_query::RegionQueryTarget> {
            unreachable!()
        }
        async fn do_get(
            &self,
            _: &crate::region_query::RegionQueryTarget,
            _: QueryRequest,
        ) -> crate::error::Result<common_recordbatch::SendableRecordBatchStream> {
            unreachable!()
        }
        async fn handle_remote_dyn_filter_update(
            &self,
            _: &crate::region_query::RegionQueryTarget,
            _: String,
            _: api::v1::region::RemoteDynFilterUpdate,
        ) -> crate::error::Result<()> {
            unreachable!()
        }
        async fn handle_remote_dyn_filter_unregister(
            &self,
            _: &crate::region_query::RegionQueryTarget,
            _: String,
            _: api::v1::region::RemoteDynFilterUnregister,
        ) -> crate::error::Result<()> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn empty_fan_in_returns_empty_stream_with_declared_schema() {
        let table = table_info(vec![0]);
        let (template, raw_schema, decode) = bind_raw_series_table(
            &table,
            "cpu_usage",
            &["host".to_string()],
            &["value".to_string()],
            RegionId::new(4, 9),
        )
        .unwrap();
        let exec = NativeRawFanInExec::try_new(
            template,
            Arc::new(raw_schema),
            decode,
            vec![],
            datafusion::logical_expr::LogicalPlanBuilder::empty(true)
                .build()
                .unwrap(),
            Arc::new(UnusedHandler),
            session::context::QueryContext::arc(),
        )
        .unwrap();
        let expected_schema = exec.schema();
        let context = Arc::new(TaskContext::default());

        let mut stream = exec.execute(0, context.clone()).unwrap();
        assert_eq!(stream.schema(), expected_schema);
        assert!(futures_util::StreamExt::next(&mut stream).await.is_none());
        assert!(exec.execute(1, context).is_err());
    }

    #[test]
    fn fan_in_declares_key_schema_and_rejects_empty_wrong_layout() {
        let table = table_info(vec![0]);
        let (_, raw_schema, decode) = bind_raw_series_table(
            &table,
            "cpu_usage",
            &["host".to_string()],
            &["value".to_string()],
            RegionId::new(4, 9),
        )
        .unwrap();
        let expected = fan_in_frame_schema(&raw_schema, &decode).unwrap();
        let actual_decode = datatypes::series::frame::series_frame_label_decode(&expected).unwrap();
        assert_eq!(actual_decode.as_ref(), Some(&decode));

        // Schema validation also applies to an empty stream: no batch is needed to expose a
        // frame whose label layout violates the authorized remote binding.
        let wrong = datatypes::series::frame::series_frame_schema(
            &raw_schema,
            FrameLabelMode::Explicit,
            None,
        )
        .unwrap();
        assert!(validate_remote_schema(&expected, &wrong, "test empty stream").is_err());
    }

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    use arrow::array::Float64Array;
    use arrow::buffer::{Buffer, ScalarBuffer};
    use datatypes::series::raw::RawTimestamps;
    use datatypes::series::samples::{Samples, SeriesFields};

    use async_trait::async_trait;
    use common_meta::peer::Peer;
    use common_recordbatch::{RecordBatch, RecordBatchStream};
    use session::ReadPreference;
    use snafu::IntoError;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct GuardedEmptyStream {
        schema: Arc<datatypes::schema::Schema>,
        fail_next: bool,
        _drop_flag: DropFlag,
        /// The datanode-side permit this stream keeps until it is dropped, if it took one.
        _permit: Option<tokio::sync::OwnedSemaphorePermit>,
    }

    impl Stream for GuardedEmptyStream {
        type Item = common_recordbatch::error::Result<RecordBatch>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            if self.fail_next {
                self.fail_next = false;
                Poll::Ready(Some(Err(common_recordbatch::error::ExternalSnafu
                    .into_error(BoxedError::new(PlainError::new(
                        "late upstream failure".to_string(),
                        StatusCode::RequestOutdated,
                    ))))))
            } else {
                Poll::Ready(None)
            }
        }
    }

    impl RecordBatchStream for GuardedEmptyStream {
        fn schema(&self) -> Arc<datatypes::schema::Schema> {
            self.schema.clone()
        }
        fn output_ordering(&self) -> Option<&[common_recordbatch::OrderOption]> {
            None
        }
        fn metrics(&self) -> Option<common_recordbatch::adapter::RecordBatchMetrics> {
            None
        }
    }

    struct TestRawSeriesStream {
        chunks: std::vec::IntoIter<RawSeriesChunk>,
        schema: Arc<SeriesSchema>,
        decode: LabelDecodeSpec,
        /// Set to fail one poll after the replay is exhausted, so the fan-in can hand the region to
        /// the merge — and emit what it buffered — before the failure arrives from inside the merge.
        late_error: Option<BoxedError>,
        /// Whether the held failure has already stalled one poll.
        stalled: bool,
    }

    impl Stream for TestRawSeriesStream {
        type Item = std::result::Result<RawSeriesChunk, BoxedError>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            if let Some(chunk) = self.chunks.next() {
                return Poll::Ready(Some(Ok(chunk)));
            }
            match self.late_error.take() {
                // No held failure: the replay is the whole stream.
                None => Poll::Ready(None),
                // The failure is held for one poll, so a consumer that drains to a stall owns the
                // stream before it sees the error.
                Some(error) if !self.stalled => {
                    self.stalled = true;
                    self.late_error = Some(error);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Some(error) => Poll::Ready(Some(Err(error))),
            }
        }
    }

    impl RawSeriesStream for TestRawSeriesStream {
        fn schema(&self) -> &Arc<SeriesSchema> {
            &self.schema
        }
        fn label_source(&self) -> &RawLabelSource {
            &store_api::region_engine::NO_BOUND_LABEL_SOURCE
        }
        fn label_decode_spec(&self) -> Option<&LabelDecodeSpec> {
            Some(&self.decode)
        }
        fn ordering(&self) -> SeriesOrdering {
            SeriesOrdering::KeyThenTime
        }
    }

    struct ChunkedRecordBatchStream {
        schema: Arc<Schema>,
        batches: std::vec::IntoIter<RecordBatch>,
        _drop_flag: DropFlag,
        _permit: tokio::sync::OwnedSemaphorePermit,
        /// When set, records every poll under `key` and stays pending on the first one, so a test
        /// can observe how the fan-in interleaves its open streams.
        paced: Option<(Arc<Mutex<Vec<u8>>>, u8)>,
        first_poll: bool,
    }

    impl Stream for ChunkedRecordBatchStream {
        type Item = common_recordbatch::error::Result<RecordBatch>;
        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            if let Some((trace, key)) = self.paced.as_ref() {
                trace.lock().unwrap().push(*key);
            }
            if self.paced.is_some() && self.first_poll {
                self.first_poll = false;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(self.batches.next().map(Ok))
        }
    }

    impl RecordBatchStream for ChunkedRecordBatchStream {
        fn schema(&self) -> Arc<Schema> {
            self.schema.clone()
        }
        fn output_ordering(&self) -> Option<&[common_recordbatch::OrderOption]> {
            None
        }
        fn metrics(&self) -> Option<common_recordbatch::adapter::RecordBatchMetrics> {
            None
        }
    }

    enum HandlerStep {
        Empty(Arc<datatypes::schema::Schema>, Arc<AtomicBool>),
        LateError(Arc<datatypes::schema::Schema>, Arc<AtomicBool>),
        Fail,
        Pending(Arc<AtomicBool>),
        /// A read that has to take a permit released by the streams already handed out, the way a
        /// datanode with `max_concurrent_queries` set does. The permit is held by the returned
        /// stream, so this only succeeds once the fan-in has released what it was given.
        Limited {
            schema: Arc<datatypes::schema::Schema>,
            limiter: Arc<tokio::sync::Semaphore>,
            dropped: Arc<AtomicBool>,
        },
        Chunked {
            chunks: Vec<RawSeriesChunk>,
            limiter: Arc<tokio::sync::Semaphore>,
            dropped: Arc<AtomicBool>,
            /// When set, the returned stream stays pending on its first poll and records each poll
            /// under the given key.
            paced: Option<(Arc<Mutex<Vec<u8>>>, u8)>,
        },
    }

    struct LifecycleHandler {
        steps: Mutex<Vec<HandlerStep>>,
        seen: Mutex<Vec<(RegionId, u64)>>,
        raw_schema: Arc<SeriesSchema>,
        decode: LabelDecodeSpec,
    }

    #[async_trait]
    impl crate::region_query::RegionQueryHandler for LifecycleHandler {
        async fn select_target(
            &self,
            _: ReadPreference,
            region_id: RegionId,
        ) -> crate::error::Result<RegionQueryTarget> {
            Ok(RegionQueryTarget::new(Peer {
                id: region_id.as_u64(),
                addr: "127.0.0.1:1".into(),
            }))
        }
        async fn do_get(
            &self,
            target: &RegionQueryTarget,
            request: QueryRequest,
        ) -> crate::error::Result<common_recordbatch::SendableRecordBatchStream> {
            self.seen
                .lock()
                .unwrap()
                .push((request.region_id, target.peer().id));
            let step = self.steps.lock().unwrap().remove(0);
            match step {
                HandlerStep::Empty(schema, dropped) => Ok(Box::pin(GuardedEmptyStream {
                    schema,
                    fail_next: false,
                    _drop_flag: DropFlag(dropped),
                    _permit: None,
                })),
                HandlerStep::LateError(schema, dropped) => Ok(Box::pin(GuardedEmptyStream {
                    schema,
                    fail_next: true,
                    _drop_flag: DropFlag(dropped),
                    _permit: None,
                })),
                HandlerStep::Chunked {
                    chunks,
                    limiter,
                    dropped,
                    paced,
                } => {
                    let permit = tokio::time::timeout(
                        std::time::Duration::from_millis(50),
                        limiter.acquire_owned(),
                    )
                    .await
                    .map_err(|_| {
                        crate::error::UnimplementedSnafu {
                            operation: "test region read limiter exhausted".to_string(),
                        }
                        .build()
                    })?
                    .map_err(|_| {
                        crate::error::UnimplementedSnafu {
                            operation: "test region read limiter closed".to_string(),
                        }
                        .build()
                    })?;
                    let raw_schema = self.raw_schema.clone();
                    let decode = self.decode.clone();
                    let raw_stream = Box::pin(TestRawSeriesStream {
                        chunks: chunks.into_iter(),
                        schema: raw_schema,
                        decode,
                        late_error: None,
                        stalled: false,
                    }) as SendableRawSeriesStream;
                    let mut packed = common_query::series_frame::pack_raw_series(raw_stream)
                        .map_err(|e| {
                            crate::error::UnimplementedSnafu {
                                operation: e.to_string(),
                            }
                            .build()
                        })?;
                    let output_schema =
                        Arc::new(Schema::try_from(packed.schema().clone()).unwrap());
                    let mut batches = Vec::new();
                    while let Some(batch) = packed.next().await {
                        let batch = batch.map_err(|e| {
                            crate::error::UnimplementedSnafu {
                                operation: e.to_string(),
                            }
                            .build()
                        })?;
                        batches.push(RecordBatch::from_df_record_batch(
                            output_schema.clone(),
                            batch,
                        ));
                    }
                    Ok(Box::pin(ChunkedRecordBatchStream {
                        schema: output_schema,
                        batches: batches.into_iter(),
                        _drop_flag: DropFlag(dropped),
                        _permit: permit,
                        paced,
                        first_poll: true,
                    }))
                }
                HandlerStep::Limited {
                    schema,
                    limiter,
                    dropped,
                } => {
                    let permit = tokio::time::timeout(
                        std::time::Duration::from_millis(50),
                        limiter.acquire_owned(),
                    )
                    .await
                    .map_err(|_| {
                        crate::error::UnimplementedSnafu {
                            operation: "test region read limiter exhausted".to_string(),
                        }
                        .build()
                    })?
                    .map_err(|_| {
                        crate::error::UnimplementedSnafu {
                            operation: "test region read limiter closed".to_string(),
                        }
                        .build()
                    })?;
                    Ok(Box::pin(GuardedEmptyStream {
                        schema,
                        fail_next: false,
                        _drop_flag: DropFlag(dropped),
                        _permit: Some(permit),
                    }))
                }
                HandlerStep::Fail => Err(crate::error::UnimplementedSnafu {
                    operation: "test region request failure".to_string(),
                }
                .build()),
                HandlerStep::Pending(dropped) => {
                    let _guard = DropFlag(dropped);
                    futures_util::future::pending::<
                        crate::error::Result<common_recordbatch::SendableRecordBatchStream>,
                    >()
                    .await
                }
            }
        }
        async fn handle_remote_dyn_filter_update(
            &self,
            _: &RegionQueryTarget,
            _: String,
            _: api::v1::region::RemoteDynFilterUpdate,
        ) -> crate::error::Result<()> {
            unreachable!()
        }
        async fn handle_remote_dyn_filter_unregister(
            &self,
            _: &RegionQueryTarget,
            _: String,
            _: api::v1::region::RemoteDynFilterUnregister,
        ) -> crate::error::Result<()> {
            unreachable!()
        }
    }

    fn lifecycle_exec(
        steps: Vec<HandlerStep>,
        ids: [RegionId; 2],
    ) -> (NativeRawFanInExec, Arc<LifecycleHandler>) {
        lifecycle_exec_many(steps, ids.to_vec())
    }

    fn lifecycle_exec_many(
        steps: Vec<HandlerStep>,
        ids: Vec<RegionId>,
    ) -> (NativeRawFanInExec, Arc<LifecycleHandler>) {
        let (template, raw_schema, decode) = bind_raw_series_table(
            &table_info(vec![0]),
            "cpu_usage",
            &["host".to_string()],
            &["value".to_string()],
            ids[0],
        )
        .unwrap();
        let schema = fan_in_frame_schema(&raw_schema, &decode).unwrap();
        let declared = Arc::new(Schema::try_from(schema).unwrap());
        let handler = Arc::new(LifecycleHandler {
            steps: Mutex::new(steps),
            seen: Mutex::new(vec![]),
            raw_schema: Arc::new(raw_schema.clone()),
            decode: decode.clone(),
        });
        let regions = ids
            .into_iter()
            .map(|id| {
                (
                    id,
                    RegionQueryTarget::new(Peer {
                        id: id.as_u64(),
                        addr: "127.0.0.1:1".into(),
                    }),
                )
            })
            .collect();
        let steps = std::mem::take(&mut *handler.steps.lock().unwrap());
        let steps = steps
            .into_iter()
            .map(|step| match step {
                HandlerStep::Empty(_, flag) => HandlerStep::Empty(declared.clone(), flag),
                HandlerStep::LateError(_, flag) => HandlerStep::LateError(declared.clone(), flag),
                HandlerStep::Limited {
                    limiter, dropped, ..
                } => HandlerStep::Limited {
                    schema: declared.clone(),
                    limiter,
                    dropped,
                },
                other @ HandlerStep::Chunked { .. } => other,
                other => other,
            })
            .collect();
        *handler.steps.lock().unwrap() = steps;
        let exec = NativeRawFanInExec::try_new(
            template,
            Arc::new(raw_schema),
            decode,
            regions,
            datafusion::logical_expr::LogicalPlanBuilder::empty(true)
                .build()
                .unwrap(),
            handler.clone(),
            session::context::QueryContext::arc(),
        )
        .unwrap();
        (exec, handler)
    }

    fn test_chunk(key: u8, count: usize) -> RawSeriesChunk {
        let table = table_info(vec![0]);
        let (_, schema, _) = bind_raw_series_table(
            &table,
            "cpu_usage",
            &["host".to_string()],
            &["value".to_string()],
            RegionId::new(4, 1),
        )
        .unwrap();
        let timestamps = (0..count as i64).collect::<Vec<_>>();
        let values = (0..count).map(|i| i as f64).collect::<Vec<_>>();
        RawSeriesChunk::try_new(
            &schema,
            Buffer::from_slice_ref([key]),
            None,
            RawTimestamps::try_new(ScalarBuffer::from(timestamps)).unwrap(),
            SeriesFields::one(Samples::from_float(Float64Array::from(values))),
            true,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn fan_in_finishes_when_one_permit_serves_three_regions() {
        let limiter = Arc::new(tokio::sync::Semaphore::new(1));
        let ids = [
            RegionId::new(4, 1),
            RegionId::new(4, 2),
            RegionId::new(4, 3),
        ];
        let steps = (1..=3)
            .map(|key| HandlerStep::Chunked {
                chunks: vec![test_chunk(key, 1)],
                limiter: limiter.clone(),
                dropped: Arc::new(AtomicBool::new(false)),
                paced: None,
            })
            .collect();
        let (exec, _) = lifecycle_exec_many(steps, ids.to_vec());
        // Open-all-then-consume retains the sole permit on region one and cannot open region two.
        let mut output = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let mut batches = 0;
        while let Some(batch) = output.next().await {
            batches += 1;
            batch.unwrap();
        }
        assert!(batches > 0, "raw values must survive the frame round trip");
        assert!(limiter.try_acquire().is_ok(), "a read permit leaked");
    }

    /// Both region reads are opened at once and every opened stream is drained on every poll: a
    /// region whose stream is still pending does not stop its sibling from being consumed.
    #[tokio::test]
    async fn opening_runs_in_parallel_and_live_regions_keep_draining() {
        let limiter = Arc::new(tokio::sync::Semaphore::new(2));
        let trace = Arc::new(Mutex::new(vec![]));
        let ids = [RegionId::new(4, 1), RegionId::new(4, 2)];
        let steps = [1u8, 2]
            .into_iter()
            .map(|key| HandlerStep::Chunked {
                chunks: vec![test_chunk(key, 1)],
                limiter: limiter.clone(),
                dropped: Arc::new(AtomicBool::new(false)),
                paced: Some((trace.clone(), key)),
            })
            .collect();
        let (exec, handler) = lifecycle_exec(steps, ids);
        let mut output = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batch = output
            .next()
            .await
            .expect("the fan-in must still produce output")
            .unwrap();
        assert!(batch.num_rows() > 0);
        let polls = trace.lock().unwrap().clone();
        // Both regions were requested before either produced output.
        assert_eq!(handler.seen.lock().unwrap().len(), 2);
        // Neither stream was drained to EOF before the other was polled at all: the first two
        // polls come from different regions.
        assert_ne!(polls[0], polls[1], "poll order: {polls:?}");
        for key in [1u8, 2] {
            assert!(
                polls.iter().filter(|polled| **polled == key).count() > 1,
                "region {key} was not drained: {polls:?}"
            );
        }
        drop(output);
        assert!(limiter.try_acquire().is_ok(), "a read permit leaked");
    }

    #[tokio::test]
    async fn fan_in_reports_a_budget_refusal_and_releases_the_permit() {
        let limiter = Arc::new(tokio::sync::Semaphore::new(1));
        let ids = [RegionId::new(4, 1), RegionId::new(4, 2)];
        let steps = [1, 2]
            .into_iter()
            .map(|key| HandlerStep::Chunked {
                chunks: vec![test_chunk(key, 8)],
                limiter: limiter.clone(),
                dropped: Arc::new(AtomicBool::new(false)),
                paced: None,
            })
            .collect();
        let (exec, _) = lifecycle_exec(steps, ids);
        let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(
                datafusion::execution::memory_pool::GreedyMemoryPool::new(180),
            ))
            .build_arc()
            .unwrap();
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(datafusion::execution::config::SessionConfig::new())
                .with_runtime(runtime),
        );
        let mut output = exec.execute(0, context).unwrap();
        let error = output.next().await.expect("reservation error").unwrap_err();
        let text = format!("{error:?}");
        assert!(
            text.contains("NativeRawFanInExec could not reserve"),
            "unexpected error: {text}"
        );
        assert!(
            text.to_lowercase().contains("pool"),
            "error must identify the pool: {text}"
        );
        assert!(output.next().await.is_none());
        drop(output);
        assert!(limiter.try_acquire().is_ok(), "a read permit leaked");
    }

    /// The raw RecordBatch that `unpack_raw_series` decodes and holds between polls is charged to
    /// the query pool too, not just the chunks that reach the fan-in: a pool that the per-chunk
    /// accounting alone would fit must still refuse the in-flight batch, explicitly.
    #[tokio::test]
    async fn raw_batch_bytes_are_charged_to_the_query_pool() {
        // The pool fits the chunk accounting for the single chunk below but not the raw batch
        // carrying it, so the failure can only come from the batch charge.
        const POOL_BYTES: usize = 100;
        assert!(raw_chunk_held_bytes(&test_chunk(1, 1)) < POOL_BYTES);
        let limiter = Arc::new(tokio::sync::Semaphore::new(1));
        let dropped = Arc::new(AtomicBool::new(false));
        let (exec, _) = lifecycle_exec_many(
            vec![HandlerStep::Chunked {
                chunks: vec![test_chunk(1, 1)],
                limiter: limiter.clone(),
                dropped: dropped.clone(),
                paced: None,
            }],
            vec![RegionId::new(4, 1)],
        );
        let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(
                datafusion::execution::memory_pool::GreedyMemoryPool::new(POOL_BYTES),
            ))
            .build_arc()
            .unwrap();
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(datafusion::execution::config::SessionConfig::new())
                .with_runtime(runtime),
        );
        let mut output = exec.execute(0, context).unwrap();
        let error = output
            .next()
            .await
            .expect("the raw batch charge must be refused")
            .unwrap_err();
        let text = format!("{error:?}");
        assert!(
            text.contains("NativeRawFanInExec could not reserve"),
            "unexpected error: {text}"
        );
        // No partial result and no silent EOF after the refusal.
        assert!(output.next().await.is_none());
        drop(output);
        // The failed query released the region stream and with it its read permit.
        assert!(dropped.load(Ordering::SeqCst));
        assert!(limiter.try_acquire().is_ok(), "a read permit leaked");
    }

    #[tokio::test]
    async fn later_region_request_error_releases_previously_opened_stream() {
        let first_dropped = Arc::new(AtomicBool::new(false));
        let ids = [RegionId::new(4, 1), RegionId::new(4, 2)];
        let (exec, handler) = lifecycle_exec(
            vec![
                HandlerStep::Empty(Arc::new(Schema::new(vec![])), first_dropped.clone()),
                HandlerStep::Fail,
            ],
            ids,
        );
        let mut output = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        assert_eq!(output.schema(), exec.schema());
        assert!(output.next().await.unwrap().is_err());
        assert!(output.next().await.is_none());
        assert!(first_dropped.load(Ordering::SeqCst));
        assert_eq!(
            handler.seen.lock().unwrap().as_slice(),
            &[(ids[0], ids[0].as_u64()), (ids[1], ids[1].as_u64())]
        );
    }

    #[tokio::test]
    async fn late_region_stream_error_is_not_silently_treated_as_eof() {
        let upstream_dropped = Arc::new(AtomicBool::new(false));
        let other_dropped = Arc::new(AtomicBool::new(false));
        let ids = [RegionId::new(4, 1), RegionId::new(4, 2)];
        let (exec, _) = lifecycle_exec(
            vec![
                HandlerStep::LateError(Arc::new(Schema::new(vec![])), upstream_dropped.clone()),
                HandlerStep::Empty(Arc::new(Schema::new(vec![])), other_dropped.clone()),
            ],
            ids,
        );
        let mut output = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        assert!(output.next().await.unwrap().is_err());
        assert!(output.next().await.is_none());
        assert!(upstream_dropped.load(Ordering::SeqCst));
        assert!(other_dropped.load(Ordering::SeqCst));
    }

    /// A merge-level error is terminal, exactly like an EOF: the fan-in releases what it reserved for
    /// the merged stream and reports nothing afterwards, instead of holding that reservation until
    /// the consumer happens to drop the stream.
    ///
    /// The failing region's stream stalls one poll before it fails, so the fan-in has already handed
    /// the region to the merge — and the merge has already emitted a chunk of the sibling region —
    /// when the error arrives from inside the merge rather than from the drain of a live region.
    #[test]
    fn merge_error_after_hand_off_is_terminal_and_releases_the_reservation() {
        fn region(
            schema: Arc<SeriesSchema>,
            decode: LabelDecodeSpec,
            chunks: Vec<RawSeriesChunk>,
            late_error: Option<BoxedError>,
        ) -> RegionState {
            RegionState::Live {
                stream: Box::pin(TestRawSeriesStream {
                    chunks: chunks.into_iter(),
                    schema,
                    decode,
                    late_error,
                    stalled: false,
                }) as SendableRawSeriesStream,
                buffered: Vec::new(),
            }
        }

        let (_, raw_schema, decode) = bind_raw_series_table(
            &table_info(vec![0]),
            "cpu_usage",
            &["host".to_string()],
            &["value".to_string()],
            RegionId::new(4, 1),
        )
        .unwrap();
        let raw_schema = Arc::new(raw_schema);
        let failure = BoxedError::new(PlainError::new(
            "merge head failed after hand-off".to_string(),
            StatusCode::Unexpected,
        ));
        let pool: Arc<dyn MemoryPool> =
            Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(1024));
        let first = test_chunk(1, 1);
        let second = test_chunk(2, 1);
        // What the one region buffer holds on top of the fan-in's own reservation, once both
        // drained chunks are charged: the merge still owns both figures while it runs.
        let buffered_bytes = raw_chunk_held_bytes(&first) + raw_chunk_held_bytes(&second);
        let held = 64;
        let reservation = MemoryConsumer::new("test raw fan-in").register(&pool);
        reservation.try_grow(held).unwrap();
        let mut stream = FanInRawStream {
            expected_schema: raw_schema.clone(),
            expected_decode: decode.clone(),
            regions: vec![],
            states: vec![
                // Their keys order the merge: the first region's chunk is decided and emitted
                // before the second region has to answer, which is what lets its failure arrive
                // one poll later.
                region(raw_schema.clone(), decode.clone(), vec![first], None),
                region(raw_schema.clone(), decode.clone(), vec![second], Some(failure)),
            ],
            opening: FuturesUnordered::new(),
            merged: None,
            reservation: Some(reservation),
            open_params: None,
            finished: false,
        };
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);

        assert!(
            matches!(
                std::pin::Pin::new(&mut stream).poll_next(&mut context),
                Poll::Ready(Some(Ok(_)))
            ),
            "the merge must emit the first region's chunk before the second region fails"
        );
        assert!(
            stream.merged.is_some(),
            "both regions must be handed to the merge before the failure"
        );
        assert_eq!(
            pool.reserved(),
            held + buffered_bytes,
            "starting the merge released a reservation the merge still owns"
        );

        let error = match std::pin::Pin::new(&mut stream).poll_next(&mut context) {
            Poll::Ready(Some(Err(error))) => error,
            other => panic!("the merge error must be reported, got {other:?}"),
        };
        // The merge is dropped when it fails, but the error itself is passed through unchanged.
        assert!(
            format!("{error:?}").contains("merge head failed after hand-off"),
            "unexpected error: {error:?}"
        );
        assert!(stream.finished, "a merge error left the fan-in unfinished");
        assert!(
            stream.merged.is_none(),
            "the failed merge was kept after its error"
        );
        assert!(
            stream.reservation.is_none(),
            "the failed merge kept its reservation"
        );
        assert_eq!(
            pool.reserved(),
            0,
            "the released reservation is still charged to the pool"
        );
        assert!(
            matches!(
                std::pin::Pin::new(&mut stream).poll_next(&mut context),
                Poll::Ready(None)
            ),
            "a failed fan-in must stay terminal"
        );
    }

    /// `max_concurrent_queries` bounds in-flight region read streams per datanode, not per query,
    /// so when other work holds every permit this query's region read is refused at admission: the
    /// query fails explicitly, releases everything it holds, and emits no partial result.
    #[tokio::test]
    async fn exhausted_region_read_limiter_fails_and_releases_held_permit() {
        let limiter = Arc::new(tokio::sync::Semaphore::new(1));
        // Other work already holds the datanode's only read permit, so neither of this query's
        // region reads can be admitted, however many of them there are.
        let held = Arc::clone(&limiter)
            .acquire_owned()
            .await
            .expect("the test must hold the only read permit");
        let ids = [RegionId::new(4, 1), RegionId::new(4, 2)];
        let (exec, _) = lifecycle_exec(
            vec![
                HandlerStep::Limited {
                    schema: Arc::new(Schema::new(vec![])),
                    limiter: Arc::clone(&limiter),
                    dropped: Arc::new(AtomicBool::new(false)),
                },
                HandlerStep::Limited {
                    schema: Arc::new(Schema::new(vec![])),
                    limiter: Arc::clone(&limiter),
                    dropped: Arc::new(AtomicBool::new(false)),
                },
            ],
            ids,
        );
        let mut output = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let error = output
            .next()
            .await
            .expect("the fan-in must report the failure")
            .unwrap_err();
        assert!(
            format!("{error:?}").contains("limiter exhausted"),
            "unexpected error: {error:?}"
        );
        // No partial result and no silent EOF after the refusal.
        assert!(output.next().await.is_none());
        drop(output);
        // The fan-in returned no permit it did not have: the only permit is still the one held by
        // the other work, so the failed query neither stole nor fabricated one.
        assert_eq!(
            limiter.available_permits(),
            0,
            "the failed query took a read permit it never had"
        );
        drop(held);
        assert!(
            limiter.try_acquire().is_ok(),
            "the failed query left a read permit held"
        );
    }

    #[test]
    fn dropping_output_cancels_pending_request_and_releases_open_stream() {
        let first_dropped = Arc::new(AtomicBool::new(false));
        let request_dropped = Arc::new(AtomicBool::new(false));
        let ids = [RegionId::new(4, 1), RegionId::new(4, 2)];
        let (exec, _) = lifecycle_exec(
            vec![
                HandlerStep::Empty(Arc::new(Schema::new(vec![])), first_dropped.clone()),
                HandlerStep::Pending(request_dropped.clone()),
            ],
            ids,
        );
        let mut output = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(
            output.as_mut().poll_next(&mut context),
            Poll::Pending
        ));
        drop(output);
        assert!(request_dropped.load(Ordering::SeqCst));
        assert!(first_dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn rejects_invalid_column_roles_and_missing_id_map() {
        let table = table_info(vec![0]);
        assert!(bind_raw_series_table(
            &table,
            "cpu_usage",
            &["value".to_string()],
            &["value".to_string()],
            RegionId::new(4, 9)
        )
        .is_err());
        let mut missing_ids = table;
        missing_ids.meta.column_ids.clear();
        assert!(bind_raw_series_table(
            &missing_ids,
            "cpu_usage",
            &["host".to_string()],
            &["value".to_string()],
            RegionId::new(4, 9)
        )
        .is_err());
    }
}
