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
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
};
use store_api::region_engine::{
    PartitionRange, PreparedSeriesSource, RawLabelSource, SendableRawSeriesStream, SeriesInfo,
};
use store_api::storage::FileId;
use store_api::storage::consts::{ReservedColumnId, SEQUENCE_COLUMN_NAME};
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
use crate::series_index::artifact::ArtifactFormat;
use crate::series_index::columnar::ColumnarSeriesFormat;
use crate::series_index::format::{
    self, SamplesFormat, SamplesFormatReader, SamplesReadRequest, SeriesIndexFormat,
    SeriesReadRequest,
};
use crate::series_index::samples::ColumnarSamplesFormat;
use crate::series_index::{MetricSeriesId, ROW_COUNT_COLUMN};

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

    /// Returns the prepared source's per-ordinal costs, or `None` before preparation succeeds.
    ///
    /// The costs are built with the source and shared by every partition, so a partition that has
    /// already awaited [`Self::prepare`] reads them here instead of rebuilding them.
    pub(crate) fn costs(&self) -> Option<Arc<[usize]>> {
        match self.source.get() {
            Some(Ok(source)) => Some(source.costs()),
            _ => None,
        }
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

/// Validates the pinned assignments and coverage without opening artifacts.
///
/// Returns whether every assigned source can be read from columnar artifacts. `false` is not an
/// error: the caller scans those sources with the ordinary reader instead.
pub(crate) fn validate_inputs(context: &StreamContext, ranges: &[PartitionRange]) -> Result<bool> {
    collect_inputs(context, ranges).map(|inputs| inputs.is_some())
}

/// Collects the assigned SSTs and their covering columnar series indexes.
///
/// `None` means the assignments are structurally valid but not ready for columnar reads: an
/// assigned memtable, an SST without columnar series coverage, or an SST without columnar
/// samples. Structural corruption is still an error, and every assignment is validated before
/// either answer is returned, so a later invalid assignment is never masked by missing artifacts.
fn collect_inputs(
    context: &StreamContext,
    ranges: &[PartitionRange],
) -> Result<Option<(Vec<usize>, HashSet<FileId>)>> {
    let input = &context.input;
    let metadata = input.region_metadata();
    let index = input
        .series_index
        .as_ref()
        .ok_or_else(|| invalid("missing columnar index context"))?;
    let mut ready = true;
    let mut files = BTreeMap::<usize, HashSet<i64>>::new();
    for range in ranges {
        let range = context
            .ranges
            .get(range.identifier)
            .ok_or_else(|| invalid("invalid prepared scan range"))?;
        for row_group in &range.row_group_indices {
            if context.is_mem_range_index(*row_group) {
                // Artifacts exist for flushed SSTs only.
                ready = false;
                continue;
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
    let file_indices = files.keys().copied().collect();
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
            ready = false;
        }
        if index
            .version
            .samples
            .get(&file.file_id().file_id())
            .is_none_or(|handle| handle.entry().artifact.format != ArtifactFormat::ColumnarSamples)
        {
            ready = false;
        }
    }
    Ok(ready.then_some((file_indices, indexes)))
}

/// The work one series of this source is estimated to cost when a partition assignment weighs it.
///
/// The assignment uses the coarse first-version model, series count times grid points, and both
/// factors weigh every series of one source equally: the grid is the scan's time range, not a
/// property of a single series, and the series count is what the source already knows. A uniform
/// weight therefore *is* that model, and it makes the split a count-based one (grid points cancel
/// out because they scale the whole source, and the split only compares costs within one source).
///
/// The series index also records a per-series row count (`__series_row_count`), which the candidate
/// batch already carries, so reading it would cost no extra I/O and would describe a long series as
/// heavier than a short one. That is the finer model this version deliberately does not use: it
/// would make the assignment depend on per-series sample counts, which the coarse model rejects.
/// Switching to it means overriding [`PreparedSeriesSource::series_cost`] per ordinal with that
/// column and nothing else.
struct Candidate {
    id: MetricSeriesId,
    key: Buffer,
    tags: HashMap<u32, Option<String>>,
    /// How many samples the series index records for this series.
    ///
    /// It is the work estimate the partition split weighs ordinals by: the column is already in the
    /// batch the candidate is built from, so it costs no extra read, and it is the one per-series
    /// size the index carries.
    cost: usize,
}

struct ColumnarPreparedSource {
    context: Arc<StreamContext>,
    labels: RawLabelSource,
    raw: Arc<RawScanPreparation>,
    candidates: Arc<Vec<Candidate>>,
    /// The work estimate of every ordinal, in ordinal order, built together with the candidates.
    ///
    /// Ordinals do not change after preparation, so one copy serves the scan: the partitions divide
    /// this slice instead of each rebuilding the whole source's costs. See
    /// [`PreparedSeriesSource::series_cost`].
    costs: Arc<[usize]>,
    samples: Arc<Vec<Arc<dyn SamplesFormatReader>>>,
    metrics: PartitionMetrics,
    // Readers retain this reservation with the candidate set.
    reservation: Arc<MemoryReservation>,
}

impl ColumnarPreparedSource {
    /// Returns the work estimate of every ordinal, in ordinal order.
    ///
    /// One copy is built while preparing, so a caller that divides the ordinals shares it instead of
    /// paying for one [`PreparedSeriesSource::series_cost`] call per ordinal and per partition.
    fn costs(&self) -> Arc<[usize]> {
        self.costs.clone()
    }

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
        let Some((files, indexes)) = collect_inputs(&context, &ranges)? else {
            // Readiness was checked before preparation, so this is a real capability change.
            return Err(invalid(
                "columnar series coverage changed after series preparation",
            ));
        };
        let mut samples = Vec::<Arc<dyn SamplesFormatReader>>::new();
        for file_idx in files {
            let file = &input.files[file_idx];
            let handle = &index.version.samples[&file.file_id().file_id()];
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
                // Found by name, not by position: the index format owns its column order, and a cost
                // read from the wrong column would misbalance every assignment without failing.
                let cost_column = schema
                    .fields()
                    .iter()
                    .position(|field| field.name() == ROW_COUNT_COLUMN)
                    .ok_or_else(|| invalid("series index has no row count column"))?;
                let costs = batch
                    .column(cost_column)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| invalid("invalid series row count column"))?;
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
                            cost: costs.value(row).min(usize::MAX as u64) as usize,
                        },
                    );
                }
            }
        }
        let mut candidates: Vec<_> = candidates.into_values().collect();
        candidates.sort_unstable_by(|a, b| a.key.as_slice().cmp(b.key.as_slice()));
        // Built once here, with the ordinals the source exposes from now on, so no partition pays
        // for the whole source again when it asks how to split them.
        let costs: Arc<[usize]> = candidates.iter().map(|candidate| candidate.cost).collect();
        Ok(Self {
            context,
            labels,
            raw: Arc::new(raw),
            candidates: Arc::new(candidates),
            costs,
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
    fn series_cost(&self, ordinal: usize) -> usize {
        self.costs.get(ordinal).copied().unwrap_or(1)
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
            if range.is_empty() { return; }
            let unit = context.input.region_metadata().time_index_type().unit();
            let bounds = context.input.time_range.as_ref();
            let start = bounds.and_then(|r| *r.start()).and_then(|t| t.convert_to_ceil(unit)).map(|t| t.value());
            let end = bounds.and_then(|r| *r.end()).and_then(|t| t.convert_to_ceil(unit)).map(|t| t.value());
            let mut sources = Vec::<BoxedRecordBatchStream>::new();
            for reader in samples.iter() {
                let candidates = candidates.clone();
                let range = range.clone();
                let context = context.clone();
                let reader = reader.clone();
                sources.push(Box::pin(async_stream::try_stream! {
                    let reservation = MemoryConsumer::new("ColumnarSamplesRead").register(&context.input.scan_memory_pool);
                    // Account for the ID selection retained by the samples reader, including
                    // B-tree node overhead, before allocating it.
                    let selection_bytes = range.len() * (std::mem::size_of::<MetricSeriesId>() + 64);
                    reservation.try_grow(selection_bytes).context(crate::error::MergeCandidateSeriesSnafu)?;
                    let ids = candidates[range.clone()].iter().map(|c| c.id).collect::<BTreeSet<_>>();
                    let mut batches = reader.read(SamplesReadRequest {
                        projection: reader.schema().fields().iter().map(|f| f.name().clone()).collect(),
                        ids: Some(ids),
                        start,
                        end,
                        ..Default::default()
                    })?;
                    while let Some(batch) = batches.try_next().await? {
                        let tables = batch.column_by_name(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
                            .and_then(|a| a.as_any().downcast_ref::<UInt32Array>())
                            .ok_or_else(|| invalid("missing or invalid sample table ID column"))?;
                        let tsids = batch.column_by_name(DATA_SCHEMA_TSID_COLUMN_NAME)
                            .and_then(|a| a.as_any().downcast_ref::<UInt64Array>())
                            .ok_or_else(|| invalid("missing or invalid sample TSID column"))?;
                        let mut offset = 0;
                        while offset < batch.num_rows() {
                            let id = MetricSeriesId { table_id: tables.value(offset), tsid: tsids.value(offset) };
                            // Sparse keys begin with the order-preserving table ID/TSID prefix.
                            let ordinal = candidates[range.clone()].binary_search_by_key(&id, |c| c.id)
                                .map_err(|_| invalid("sample contains an unselected series"))? + range.start;
                            let mut end = offset + 1;
                            while end < batch.num_rows() && tables.value(end) == id.table_id && tsids.value(end) == id.tsid {
                                end += 1;
                            }
                            let flat = sample_to_flat(batch.slice(offset, end - offset), &candidates[ordinal], &context)?;
                            let flat = filter_visibility(flat, &context)?;
                            offset = end;
                            if flat.num_rows() == 0 { continue; }
                            let bytes = flat.get_array_memory_size();
                            reservation.try_grow(bytes).context(crate::error::MergeCandidateSeriesSnafu)?;
                            yield flat;
                            reservation.shrink(bytes);
                        }
                    }
                }));
            }
            let mut merged = SeqScan::build_flat_reader_from_sources(&context, sources, None, Some(&metrics), false, 1).await?;
            while let Some(batch) = merged.try_next().await? {
                yield ScanBatch::RecordBatch(batch);
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
    let sequence_column = batch
        .column_by_name(SEQUENCE_COLUMN_NAME)
        .ok_or_else(|| invalid("missing sequence column __sequence"))?;
    let sequence = sequence_column
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("sequence column __sequence must be UInt64"))?;
    let metadata = context.input.region_metadata();
    let timestamp = &metadata.time_index_column().column_schema;
    let time_column = batch
        .column_by_name(&timestamp.name)
        .ok_or_else(|| invalid(format!("missing time index column {}", timestamp.name)))?;
    let unit = metadata.time_index_type().unit();
    let (time, actual_unit) = datatypes::timestamp::timestamp_array_to_primitive(time_column)
        .ok_or_else(|| {
            invalid(format!(
                "time index column {} must be a timestamp",
                timestamp.name
            ))
        })?;
    if common_time::timestamp::TimeUnit::from(actual_unit) != unit {
        return Err(invalid(format!(
            "time index column {} has unit {actual_unit:?}, expected {unit:?}",
            timestamp.name
        )));
    }
    let sequence_filter = context
        .input
        .sequence_range
        .map(|range| range.filter(sequence_column))
        .transpose()
        .context(ComputeArrowSnafu)?;
    let filter = BooleanArray::from(
        (0..batch.num_rows())
            .map(|i| {
                !sequence.is_null(i)
                    && !time.is_null(i)
                    && context
                        .input
                        .snapshot_sequence
                        .is_none_or(|max| sequence.value(i) <= max)
                    && sequence_filter
                        .as_ref()
                        .is_none_or(|filter| filter.value(i))
                    && context.input.time_range.as_ref().is_none_or(|range| {
                        range.contains(&common_time::Timestamp::new(time.value(i), unit))
                    })
            })
            .collect::<Vec<_>>(),
    );
    filter_record_batch(&batch, &filter).context(ComputeArrowSnafu)
}
