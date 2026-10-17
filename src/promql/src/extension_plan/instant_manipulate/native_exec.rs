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

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_query::series_frame::{raw_series_frame_ordering, unpack_raw_series};
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datatypes::series::SeriesOrdering;
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{DenseSamplesRef, Samples, SeriesFields, SeriesSchema};
use futures::{Stream, ready};
use store_api::region_engine::{RawLabelSource, SendableRawSeriesStream};

use crate::extension_plan::instant_manipulate::InstantManipulate;
use crate::extension_plan::instant_manipulate::native::NativeInstantMapping;
use crate::source::materialize_raw_labels;

#[cfg(test)]
#[path = "native_exec_tests.rs"]
mod tests;

impl InstantManipulate {
    /// Returns whether the bounded native mapping accepts this output schema, series schema, and
    /// complete logical tag set.
    ///
    /// This is the local half of [`Self::try_to_native_execution_plan`]: the same decision that
    /// method makes before it inspects its input, so a caller can decline a plan without building
    /// one. `Err` is the same error that method propagates for an invalid interval or lookback.
    pub fn is_native_mapping_supported(
        &self,
        output_schema: SchemaRef,
        series_schema: Arc<SeriesSchema>,
        logical_tags: &[String],
    ) -> DfResult<bool> {
        Ok(
            NativeInstantMapping::try_new(self, output_schema, series_schema, logical_tags)?
                .is_some(),
        )
    }

    /// Builds the bounded native execution path for a prepared raw-series frame.
    pub fn try_to_native_execution_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        label_source: RawLabelSource,
        series_schema: Arc<SeriesSchema>,
        logical_tags: &[String],
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(mapping) = NativeInstantMapping::try_new(
            self,
            output_schema,
            series_schema.clone(),
            logical_tags,
        )?
        else {
            return Ok(None);
        };
        if input.properties().partitioning.partition_count() != 1 {
            return Ok(None);
        }
        let declared_schema = input.schema();
        let ordering = raw_series_frame_ordering(declared_schema.as_ref())?;
        // Validate the declared raw-frame schema without executing the child.
        unpack_raw_series(
            Box::pin(RecordBatchStreamAdapter::new(
                declared_schema.clone(),
                futures::stream::empty(),
            )),
            series_schema.clone(),
            label_source.clone(),
            ordering,
        )?;
        Ok(Some(Arc::new(NativeInstantManipulateExec::new(
            input,
            Arc::new(mapping),
            declared_schema,
            series_schema,
            label_source,
            ordering,
        ))))
    }
}

struct NativeInstantManipulateExec {
    input: Arc<dyn ExecutionPlan>,
    mapping: Arc<NativeInstantMapping>,
    raw_schema: SchemaRef,
    series_schema: Arc<SeriesSchema>,
    label_source: RawLabelSource,
    ordering: SeriesOrdering,
    properties: Arc<PlanProperties>,
}
impl NativeInstantManipulateExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        mapping: Arc<NativeInstantMapping>,
        raw_schema: SchemaRef,
        series_schema: Arc<SeriesSchema>,
        label_source: RawLabelSource,
        ordering: SeriesOrdering,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(mapping.schema()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            input,
            mapping,
            raw_schema,
            series_schema,
            label_source,
            ordering,
            properties,
        }
    }
}
impl std::fmt::Debug for NativeInstantManipulateExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeInstantManipulateExec").finish()
    }
}
impl DisplayAs for NativeInstantManipulateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "NativeInstantManipulateExec")
    }
}
impl ExecutionPlan for NativeInstantManipulateExec {
    fn name(&self) -> &str {
        "NativeInstantManipulateExec"
    }
    fn schema(&self) -> SchemaRef {
        self.mapping.schema()
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1
            || children[0].schema() != self.raw_schema
            || children[0].properties().partitioning.partition_count() != 1
        {
            return Err(DataFusionError::Plan(
                "NativeInstantManipulateExec requires one unchanged one-partition raw child".into(),
            ));
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.mapping.clone(),
            self.raw_schema.clone(),
            self.series_schema.clone(),
            self.label_source.clone(),
            self.ordering,
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "native instant partition must be zero".into(),
            ));
        }
        let frame = self.input.execute(partition, context.clone())?;
        let source = unpack_raw_series(
            frame,
            self.series_schema.clone(),
            self.label_source.clone(),
            self.ordering,
        )?;
        let source = materialize_raw_labels(source)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let reservation =
            MemoryConsumer::new("NativeInstantStream").register(&context.runtime_env().memory_pool);
        Ok(Box::pin(NativeInstantStream {
            source: Some(source),
            mapping: self.mapping.clone(),
            schema: self.mapping.schema(),
            series_schema: self.series_schema.clone(),
            pending: Vec::new(),
            reservation,
        }))
    }
}

struct NativeInstantStream {
    source: Option<SendableRawSeriesStream>,
    mapping: Arc<NativeInstantMapping>,
    schema: SchemaRef,
    series_schema: Arc<SeriesSchema>,
    pending: Vec<RawSeriesChunk>,
    reservation: MemoryReservation,
}
impl NativeInstantStream {
    fn fail(&mut self, error: DataFusionError) -> Poll<Option<DfResult<RecordBatch>>> {
        self.source = None;
        self.pending = Vec::new();
        self.reservation.free();
        Poll::Ready(Some(Err(error)))
    }
    fn finish_series(&mut self) -> DfResult<RecordBatch> {
        let chunks = std::mem::take(&mut self.pending);
        let rows = chunks
            .iter()
            .try_fold(0usize, |n, c| n.checked_add(c.time.len()))
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted("native instant row count overflow".into())
            })?;
        let first = chunks
            .first()
            .and_then(|c| c.time.timestamp(0))
            .ok_or_else(|| {
                DataFusionError::Execution("native instant series has no timestamps".into())
            })?;
        let last = chunks
            .last()
            .and_then(|c| c.time.timestamp(c.time.len().saturating_sub(1)))
            .ok_or_else(|| {
                DataFusionError::Execution("native instant series has no timestamps".into())
            })?;
        let points = self.mapping.estimated_points(first, last)?;
        let label_bytes = chunks
            .first()
            .and_then(|c| c.metric.as_ref())
            .map(|m| {
                m.name().map_or(0, str::len)
                    + m.tags()
                        .iter()
                        .map(|l| l.name.len() + l.value.len() + 16)
                        .sum::<usize>()
            })
            .unwrap_or(0);
        // Reserve timestamp concatenation by row count, but field descriptors by their
        // actual chunk/segment counts: a dense chunk is still one field descriptor.
        let field_count = self.series_schema.fields.len();
        let chunk_count = chunks.len();
        let segment_count = chunks
            .iter()
            .flat_map(|chunk| chunk.fields.as_slice())
            .try_fold(0usize, |count, field| {
                count.checked_add(field.segments().len())
            })
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted("native instant segment count overflow".into())
            })?;
        let chunk_field_descriptors = chunk_count
            .checked_mul(field_count)
            .and_then(|n| n.checked_mul(std::mem::size_of::<Samples>()))
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "native instant descriptor estimate overflow".into(),
                )
            })?;
        // Flattening and converting to Arc can transiently retain the target descriptors and
        // two source/flattened vectors; three segment-counts conservatively cover those owners.
        let flattened_descriptors = segment_count
            .checked_mul(3)
            .and_then(|n| n.checked_mul(std::mem::size_of::<Samples>()))
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "native instant segment estimate overflow".into(),
                )
            })?;
        let scratch = rows
            .checked_mul(16)
            .and_then(|n| n.checked_add(chunk_field_descriptors))
            .and_then(|n| n.checked_add(flattened_descriptors))
            .and_then(|n| {
                n.checked_add(field_count.checked_mul(std::mem::size_of::<Vec<Samples>>())?)
            })
            .and_then(|n| {
                points
                    .checked_mul(
                        self.schema
                            .fields()
                            .len()
                            .checked_mul(24)?
                            .checked_add(label_bytes)?
                            .checked_add(48)?,
                    )?
                    .checked_add(n)
            })
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "native instant scratch estimate overflow".into(),
                )
            })?;
        self.reservation
            .try_grow(scratch)
            .map_err(|e| DataFusionError::ResourcesExhausted(e.to_string()))?;
        let metric = chunks
            .first()
            .and_then(|c| c.metric.clone())
            .ok_or_else(|| {
                DataFusionError::Execution("native instant series has no metric labels".into())
            })?;
        let mut times = Vec::new();
        times
            .try_reserve_exact(rows)
            .map_err(|e| DataFusionError::ResourcesExhausted(e.to_string()))?;
        let mut field_chunks = (0..field_count).map(|_| Vec::new()).collect::<Vec<_>>();
        for field in &mut field_chunks {
            field
                .try_reserve_exact(chunk_count)
                .map_err(|e| DataFusionError::ResourcesExhausted(e.to_string()))?;
        }
        for chunk in chunks {
            for i in 0..chunk.time.len() {
                times.push(chunk.time.timestamp(i).ok_or_else(|| {
                    DataFusionError::Execution("native instant timestamp missing".into())
                })?);
            }
            for (target, field) in field_chunks.iter_mut().zip(chunk.fields.as_slice()) {
                target.push(field.clone());
            }
        }
        let time = RawTimestamps::try_new(times.into())
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let fields = field_chunks
            .into_iter()
            .map(Samples::try_from_chunks)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let fields = SeriesFields::try_from_vec(fields)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        self.mapping.evaluate(&metric, &time, &fields)
    }
}
fn retained_chunk_bytes(chunk: &RawSeriesChunk) -> Option<usize> {
    let timestamp_bytes = chunk.time.as_buffer().inner().capacity();
    let labels = chunk
        .metric
        .as_ref()
        .map(|metric| {
            metric
                .tags()
                .iter()
                .try_fold(metric.name().map_or(0, str::len), |total, label| {
                    total
                        .checked_add(label.name.len())?
                        .checked_add(label.value.len())?
                        .checked_add(std::mem::size_of::<datatypes::series::Label>())?
                        .checked_add(2 * std::mem::size_of::<usize>())
                })
        })
        .unwrap_or(Some(0))?;
    let field_descriptors = std::mem::size_of::<RawSeriesChunk>()
        .checked_add(std::mem::size_of::<SeriesFields>())?
        .checked_add(
            chunk
                .fields
                .as_slice()
                .len()
                .checked_mul(std::mem::size_of::<Samples>())?,
        )?;
    let (segment_descriptors, sample_buffers) = chunk
        .fields
        .as_slice()
        .iter()
        .flat_map(|field| field.segments())
        .try_fold((0usize, 0usize), |(descriptors, buffers), segment| {
            let bytes = match segment.dense()? {
                DenseSamplesRef::Float(array) => array.get_buffer_memory_size(),
                _ => return None,
            };
            Some((
                descriptors.checked_add(std::mem::size_of::<Samples>())?,
                buffers.checked_add(bytes)?,
            ))
        })?;
    timestamp_bytes
        .checked_add(chunk.primary_key.capacity())?
        .checked_add(labels)?
        .checked_add(field_descriptors)?
        .checked_add(segment_descriptors)?
        .checked_add(sample_buffers)
}

impl Stream for NativeInstantStream {
    type Item = DfResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        for _ in 0..64 {
            let Some(source) = self.source.as_mut() else {
                self.pending = Vec::new();
                self.reservation.free();
                return Poll::Ready(None);
            };
            match ready!(source.as_mut().poll_next(cx)) {
                Some(Err(error)) => {
                    return self.fail(DataFusionError::External(Box::new(error)));
                }
                None if !self.pending.is_empty() => {
                    return self.fail(DataFusionError::Execution(
                        "raw series ended without final chunk".into(),
                    ));
                }
                None => {
                    self.source = None;
                    self.pending = Vec::new();
                    self.reservation.free();
                    return Poll::Ready(None);
                }
                Some(Ok(chunk)) => {
                    let Some(bytes) = retained_chunk_bytes(&chunk) else {
                        return self.fail(DataFusionError::ResourcesExhausted(
                            "native instant retained-byte estimate overflow".into(),
                        ));
                    };
                    if self.pending.len() == self.pending.capacity() {
                        let old_capacity = self.pending.capacity();
                        let new_capacity = old_capacity.saturating_mul(2).max(4);
                        let slot_growth =
                            new_capacity.checked_sub(old_capacity).and_then(|slots| {
                                slots.checked_mul(std::mem::size_of::<RawSeriesChunk>())
                            });
                        let Some(slot_growth) = slot_growth else {
                            return self.fail(DataFusionError::ResourcesExhausted(
                                "native instant pending capacity overflow".into(),
                            ));
                        };
                        let Some(growth) = bytes.checked_add(slot_growth) else {
                            return self.fail(DataFusionError::ResourcesExhausted(
                                "native instant chunk estimate overflow".into(),
                            ));
                        };
                        if let Err(e) = self.reservation.try_grow(growth) {
                            return self.fail(DataFusionError::ResourcesExhausted(e.to_string()));
                        }
                        let additional = new_capacity - self.pending.capacity();
                        if let Err(e) = self.pending.try_reserve_exact(additional) {
                            return self.fail(DataFusionError::ResourcesExhausted(e.to_string()));
                        }
                    } else if let Err(e) = self.reservation.try_grow(bytes) {
                        return self.fail(DataFusionError::ResourcesExhausted(e.to_string()));
                    }
                    let last = chunk.is_last;
                    self.pending.push(chunk);
                    if last {
                        match self.finish_series() {
                            Ok(batch) => {
                                self.pending = Vec::new();
                                self.reservation.free();
                                return Poll::Ready(Some(Ok(batch)));
                            }
                            Err(e) => return self.fail(e),
                        }
                    }
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
impl RecordBatchStream for NativeInstantStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
