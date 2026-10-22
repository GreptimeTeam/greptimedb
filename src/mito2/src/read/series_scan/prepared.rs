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

//! Scanner-wide columnar candidates and independent, lazy sample reads.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

use api::v1::SemanticType;
use common_error::ext::BoxedError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, DictionaryArray, StringArray, UInt32Array,
    UInt64Array,
};
use datatypes::arrow::buffer::{Buffer, OffsetBuffer};
use datatypes::arrow::compute::{cast, filter_record_batch};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::series::SeriesOrdering;
use datatypes::value::ValueRef;
use futures::TryStreamExt;
use mito_codec::row_converter::SparsePrimaryKeyCodec;
use snafu::{IntoError, ResultExt};
use store_api::region_engine::{
    PartitionRange, PreparedSeriesSource, RawLabelSource, SendableRawSeriesStream, SeriesInfo,
};
use store_api::storage::consts::ReservedColumnId;
use table::predicate::Predicate;
use tokio::sync::OnceCell;

use crate::error::{
    ComputeArrowSnafu, Error, InvalidRecordBatchSnafu, NewRecordBatchSnafu, Result,
};
use crate::read::BoxedRecordBatchStream;
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::PartitionMetrics;
use crate::read::seq_scan::SeqScan;
use crate::read::series_scan::{RawScanPreparation, raw};
use crate::read::stream::ScanBatch;
use crate::series_index::MetricSeriesId;
use crate::series_index::artifact::ArtifactFormat;
use crate::series_index::columnar::ColumnarSeriesFormat;
use crate::series_index::format::{
    self, SamplesFormat, SamplesFormatReader, SamplesReadRequest, SeriesIndexFormat,
    SeriesReadRequest,
};
use crate::series_index::samples::ColumnarSamplesFormat;

fn invalid(reason: impl Into<String>) -> Error {
    InvalidRecordBatchSnafu {
        reason: reason.into(),
    }
    .build()
}

/// Errors are shared too: a failed execution must not restart discovery in another partition.
#[derive(Default)]
pub(crate) struct Preparation {
    source: OnceCell<std::result::Result<Arc<ColumnarPreparedSource>, Arc<Error>>>,
}

impl Preparation {
    pub(crate) async fn prepare(
        &self,
        context: Arc<StreamContext>,
        ranges: Vec<PartitionRange>,
        labels: RawLabelSource,
        raw: RawScanPreparation,
        metrics: PartitionMetrics,
    ) -> std::result::Result<Arc<dyn PreparedSeriesSource>, BoxedError> {
        let expected = labels.clone();
        let result = self
            .source
            .get_or_init(|| async {
                ColumnarPreparedSource::prepare(context, ranges, labels, raw, metrics)
                    .await
                    .map(Arc::new)
                    .map_err(Arc::new)
            })
            .await;
        let source = result.as_ref().map_err(|error| {
            BoxedError::new(crate::error::ScanSeriesSnafu.into_error(error.clone()))
        })?;
        if !same_labels(&source.labels, &expected) {
            return Err(BoxedError::new(invalid(
                "label binding changed after series preparation",
            )));
        }
        Ok(source.clone())
    }
}

fn same_labels(left: &RawLabelSource, right: &RawLabelSource) -> bool {
    match (left, right) {
        (
            RawLabelSource::PrimaryKey {
                metric_name: a,
                physical_metadata: am,
                logical_tags: at,
            },
            RawLabelSource::PrimaryKey {
                metric_name: b,
                physical_metadata: bm,
                logical_tags: bt,
            },
        ) => {
            a == b
                && Arc::ptr_eq(am, bm)
                && at.len() == bt.len()
                && at
                    .iter()
                    .zip(bt.iter())
                    .all(|(a, b)| a.column_id == b.column_id && a.name == b.name)
        }
        _ => false,
    }
}

struct Candidate {
    id: MetricSeriesId,
    key: Buffer,
    tags: HashMap<u32, Option<String>>,
}

struct ColumnarPreparedSource {
    context: Arc<StreamContext>,
    labels: RawLabelSource,
    raw: Arc<RawScanPreparation>,
    candidates: Arc<Vec<Candidate>>,
    samples: Arc<Vec<Arc<dyn SamplesFormatReader>>>,
    metrics: PartitionMetrics,
    // Readers retain this reservation with the candidate set.
    reservation: Arc<MemoryReservation>,
}

impl ColumnarPreparedSource {
    async fn prepare(
        context: Arc<StreamContext>,
        ranges: Vec<PartitionRange>,
        labels: RawLabelSource,
        raw: RawScanPreparation,
        metrics: PartitionMetrics,
    ) -> Result<Self> {
        let input = &context.input;
        let metadata = input.region_metadata();
        let index = input
            .series_index
            .as_ref()
            .ok_or_else(|| invalid("missing columnar index context"))?;
        let mut files = BTreeMap::<usize, HashSet<i64>>::new();
        for range in &ranges {
            let range = context
                .ranges
                .get(range.identifier)
                .ok_or_else(|| invalid("invalid prepared scan range"))?;
            for row_group in &range.row_group_indices {
                if context.is_mem_range_index(*row_group) {
                    return Err(invalid("columnar prepared reads require flushed memtables"));
                }
                if !context.is_file_range_index(*row_group) {
                    return Err(invalid(
                        "columnar prepared reads do not support extension ranges",
                    ));
                }
                files
                    .entry(row_group.index - input.num_memtables())
                    .or_default()
                    .insert(row_group.row_group_index);
            }
        }
        let mut indexes = HashSet::new();
        let mut samples = Vec::<Arc<dyn SamplesFormatReader>>::new();
        for (file_idx, groups) in files {
            let file = &input.files[file_idx];
            if !groups.contains(&-1)
                && (groups.len() as u64 != file.meta_ref().num_row_groups
                    || groups
                        .iter()
                        .any(|group| *group < 0 || *group as u64 >= file.meta_ref().num_row_groups))
            {
                return Err(invalid(
                    "columnar samples require complete SST range assignments",
                ));
            }
            if file.meta_ref().region_id != metadata.region_id {
                return Err(invalid(
                    "columnar prepared reads do not support foreign SSTs",
                ));
            }
            let mut covered = false;
            for handle in index.version.series_indexes.values() {
                if handle.entry().format() == ArtifactFormat::ColumnarSeries
                    && handle
                        .entry()
                        .covers_file(file.meta_ref(), metadata.region_id)
                {
                    indexes.insert(handle.entry().index_uuid);
                    covered = true;
                }
            }
            if !covered {
                return Err(invalid(format!(
                    "SST {} has no columnar series coverage; run build_series_index()",
                    file.file_id()
                )));
            }
            let handle = index
                .version
                .samples
                .get(&file.file_id().file_id())
                .filter(|handle| handle.entry().artifact.format == ArtifactFormat::ColumnarSamples)
                .ok_or_else(|| {
                    invalid(format!(
                        "SST {} has no columnar samples; run build_series_index()",
                        file.file_id()
                    ))
                })?;
            let reader = ColumnarSamplesFormat::default()
                .reader(index.store.clone(), handle.clone())
                .await?;
            format::validate_sample_schema(&reader.schema(), metadata)?;
            samples.push(Arc::from(reader));
        }
        let reservation =
            MemoryConsumer::new("ColumnarPreparedSource").register(&input.scan_memory_pool);
        let mut candidates = BTreeMap::<MetricSeriesId, Candidate>::new();
        for id in indexes {
            let reader = ColumnarSeriesFormat::default()
                .reader(
                    index.store.clone(),
                    index.version.series_indexes[&id].clone(),
                )
                .await?;
            let schema = reader.schema();
            format::validate_series_schema(&schema, metadata)?;
            let mut tags = schema
                .fields()
                .iter()
                .skip(3)
                .enumerate()
                .map(|(i, field)| {
                    let id = format::field_id(field)?;
                    Ok((i + 3, id, field.name().clone()))
                })
                .collect::<Result<Vec<_>>>()?;
            tags.sort_unstable_by(|a, b| a.2.cmp(&b.2));
            let mut filters = Vec::new();
            if let Some(predicate) = input.predicate_group().predicate() {
                for expr in predicate.exprs() {
                    if expr
                        .column_refs()
                        .iter()
                        .all(|column| schema.index_of(&column.name).is_ok())
                    {
                        filters.push(
                            Predicate::to_physical_expr(expr, &schema)
                                .map_err(|e| invalid(e.to_string()))?,
                        );
                    }
                }
            }
            let mut batches = reader.read(SeriesReadRequest {
                projection: schema.fields().iter().map(|f| f.name().clone()).collect(),
                ..Default::default()
            })?;
            while let Some(mut batch) = batches.try_next().await? {
                for filter in &filters {
                    batch = common_recordbatch::filter::batch_filter(&batch, filter)
                        .map_err(|e| invalid(e.to_string()))?;
                }
                let table = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| invalid("invalid table ID column"))?;
                let tsid = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| invalid("invalid TSID column"))?;
                for row in 0..batch.num_rows() {
                    let id = MetricSeriesId {
                        table_id: table.value(row),
                        tsid: tsid.value(row),
                    };
                    let mut values = HashMap::new();
                    for (i, column_id, _) in &tags {
                        let strings = batch
                            .column(*i)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| invalid("invalid series tag array"))?;
                        values.insert(
                            *column_id,
                            (!strings.is_null(row)).then(|| strings.value(row).to_owned()),
                        );
                    }
                    // TODO(yingwen): Separate raw-series identity from label representation.
                    // The PoC formats assume (table_id, tsid) uniqueness; encode once at
                    // this adapter boundary until RawSeriesChunk no longer needs sparse keys.
                    let mut key = Vec::new();
                    let codec = SparsePrimaryKeyCodec::schemaless();
                    codec
                        .encode_internal(id.table_id, id.tsid, &mut key)
                        .map_err(|e| invalid(e.to_string()))?;
                    codec
                        .encode_to_vec(
                            tags.iter().map(|(_, id, _)| {
                                (
                                    *id,
                                    values[id]
                                        .as_deref()
                                        .map_or(ValueRef::Null, ValueRef::String),
                                )
                            }),
                            &mut key,
                        )
                        .map_err(|e| invalid(e.to_string()))?;
                    if let Some(previous) = candidates.get(&id) {
                        if previous.key.as_slice() != key.as_slice() {
                            return Err(invalid(
                                "conflicting labels for the same table ID and TSID",
                            ));
                        }
                        continue;
                    }
                    let bytes = key.len()
                        + values.values().flatten().map(String::len).sum::<usize>()
                        + values.capacity() * (std::mem::size_of::<(u32, Option<String>)>() + 16)
                        + std::mem::size_of::<Candidate>()
                        + 64;
                    reservation
                        .try_grow(bytes)
                        .map_err(|e| invalid(e.to_string()))?;
                    candidates.insert(
                        id,
                        Candidate {
                            id,
                            key: Buffer::from(key),
                            tags: values,
                        },
                    );
                }
            }
        }
        let mut candidates: Vec<_> = candidates.into_values().collect();
        candidates.sort_unstable_by(|a, b| a.key.as_slice().cmp(b.key.as_slice()));
        Ok(Self {
            context,
            labels,
            raw: Arc::new(raw),
            candidates: Arc::new(candidates),
            samples: Arc::new(samples),
            metrics,
            reservation: Arc::new(reservation),
        })
    }
}

impl PreparedSeriesSource for ColumnarPreparedSource {
    fn num_series(&self) -> usize {
        self.candidates.len()
    }
    fn label_source(&self) -> &RawLabelSource {
        &self.labels
    }
    fn series_info(&self, ordinal: usize) -> std::result::Result<SeriesInfo, BoxedError> {
        let candidate = self
            .candidates
            .get(ordinal)
            .ok_or_else(|| BoxedError::new(invalid("series ordinal out of bounds")))?;
        Ok(SeriesInfo {
            primary_key: candidate.key.clone(),
            metric: None,
        })
    }
    fn read_series(
        &self,
        range: Range<usize>,
    ) -> std::result::Result<SendableRawSeriesStream, BoxedError> {
        if range.start > range.end || range.end > self.candidates.len() {
            return Err(BoxedError::new(invalid("series range out of bounds")));
        }
        let context = self.context.clone();
        let candidates = self.candidates.clone();
        let samples = self.samples.clone();
        let metrics = self.metrics.clone();
        let reservation = self.reservation.clone();
        let stream = async_stream::try_stream! {
            let _reservation = reservation;
            for ordinal in range {
                let mut sources = Vec::<BoxedRecordBatchStream>::new();
                for reader in samples.iter() {
                    let batch_stream = reader.read(SamplesReadRequest {
                        projection: reader.schema().fields().iter().map(|f| f.name().clone()).collect(),
                        ids: Some(BTreeSet::from([candidates[ordinal].id])),
                        ..Default::default()
                    })?;
                    let candidates = candidates.clone();
                    let context = context.clone();
                    sources.push(Box::pin(async_stream::try_stream! {
                        let reservation = MemoryConsumer::new("ColumnarSamplesRead").register(&context.input.scan_memory_pool);
                        let mut batches = batch_stream;
                        while let Some(batch) = batches.try_next().await? {
                            let batch = sample_to_flat(batch, &candidates[ordinal], &context)?;
                            let batch = filter_visibility(batch, &context)?;
                            if batch.num_rows() == 0 { continue; }
                            let bytes = batch.get_array_memory_size();
                            reservation.try_grow(bytes).context(crate::error::MergeCandidateSeriesSnafu)?;
                            yield batch;
                            reservation.shrink(bytes);
                        }
                    }));
                }
                let mut merged = SeqScan::build_flat_reader_from_sources(&context, sources, None, Some(&metrics), false, 1).await?;
                while let Some(batch) = merged.try_next().await? {
                    yield ScanBatch::RecordBatch(batch);
                }
            }
        };
        Ok(raw::make_stream(
            Box::pin(stream),
            self.raw.schema.clone(),
            self.labels.clone(),
            self.raw.fields.clone(),
            self.metrics.clone(),
            self.raw.predicates.clone(),
            SeriesOrdering::KeyThenTime,
        ))
    }
}

fn sample_to_flat(
    batch: RecordBatch,
    candidate: &Candidate,
    context: &StreamContext,
) -> Result<RecordBatch> {
    let schema = context.input.mapper.input_arrow_schema(false);
    let metadata = context.input.region_metadata();
    let mut arrays = Vec::<ArrayRef>::new();
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "__primary_key" => Arc::new(
                DictionaryArray::<UInt32Type>::try_new(
                    UInt32Array::from(vec![0; batch.num_rows()]),
                    Arc::new(BinaryArray::new(
                        OffsetBuffer::new(
                            vec![
                                0,
                                i32::try_from(candidate.key.len()).map_err(|_| {
                                    invalid("series key exceeds Arrow offset limit")
                                })?,
                            ]
                            .into(),
                        ),
                        candidate.key.clone(),
                        None,
                    )),
                )
                .context(ComputeArrowSnafu)?,
            ),
            "__sequence" | "__op_type" => batch
                .column_by_name(field.name())
                .ok_or_else(|| invalid("missing sample internal column"))?
                .clone(),
            name => {
                let column = metadata
                    .column_by_name(name)
                    .ok_or_else(|| invalid("unknown flat column"))?;
                if column.semantic_type == SemanticType::Tag {
                    if column.column_id == ReservedColumnId::table_id() {
                        Arc::new(UInt32Array::from(vec![
                            candidate.id.table_id;
                            batch.num_rows()
                        ]))
                    } else if column.column_id == ReservedColumnId::tsid() {
                        Arc::new(UInt64Array::from(vec![candidate.id.tsid; batch.num_rows()]))
                    } else {
                        let value = candidate
                            .tags
                            .get(&column.column_id)
                            .ok_or_else(|| invalid("missing series tag"))?;
                        Arc::new(StringArray::from(vec![value.as_deref(); batch.num_rows()]))
                    }
                } else {
                    batch
                        .column_by_name(name)
                        .ok_or_else(|| invalid("missing sample field"))?
                        .clone()
                }
            }
        };
        arrays.push(if array.data_type() == field.data_type() {
            array
        } else {
            cast(&array, field.data_type()).context(ComputeArrowSnafu)?
        });
    }
    RecordBatch::try_new(schema, arrays).context(NewRecordBatchSnafu)
}

fn filter_visibility(batch: RecordBatch, context: &StreamContext) -> Result<RecordBatch> {
    let sequence = batch
        .column(batch.num_columns() - 2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("invalid sequence array"))?;
    let time = batch
        .column(batch.num_columns() - 4)
        .as_any()
        .downcast_ref::<datatypes::arrow::array::TimestampMillisecondArray>()
        .ok_or_else(|| invalid("invalid timestamp array"))?;
    let filter = BooleanArray::from(
        (0..batch.num_rows())
            .map(|i| {
                context
                    .input
                    .snapshot_sequence
                    .is_none_or(|max| sequence.value(i) <= max)
                    && context.input.time_range.as_ref().is_none_or(|range| {
                        range.contains(&common_time::Timestamp::new_millisecond(time.value(i)))
                    })
            })
            .collect::<Vec<_>>(),
    );
    let batch = filter_record_batch(&batch, &filter).context(ComputeArrowSnafu)?;
    Ok(crate::read::scan_util::filter_flat_batch_by_sequence(
        batch.clone(),
        context.input.sequence_range,
        true,
    )?
    .unwrap_or_else(|| batch.slice(0, 0)))
}
