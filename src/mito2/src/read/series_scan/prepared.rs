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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::Range;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use api::v1::SemanticType;
use common_error::ext::BoxedError;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_time::Timestamp;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_expr::utils::split_conjunction;
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, DictionaryArray, StringArray, UInt32Array,
    UInt64Array,
};
use datatypes::arrow::buffer::{Buffer, OffsetBuffer};
use datatypes::arrow::compute::{cast, filter_record_batch, max, min, take_record_batch};
use datatypes::arrow::datatypes::{Schema, SchemaRef, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::series::{SeriesLabelBatch, SeriesLabels, SeriesOrdering};
use datatypes::value::{Value, ValueRef};
use futures::TryStreamExt;
use mito_codec::row_converter::sparse::SparsePrimaryKeyView;
use mito_codec::row_converter::{SparseOffsetsCache, SparsePrimaryKeyCodec};
use snafu::{IntoError, ResultExt};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
};
use store_api::region_engine::{
    PartitionRange, PreparedSeriesSource, RawLabelSource, SendableRawSeriesStream, SeriesInfo,
};
use store_api::storage::consts::{ReservedColumnId, SEQUENCE_COLUMN_NAME};
use store_api::storage::{FileId, SequenceRange};
use table::predicate::Predicate;
use tokio::sync::{OnceCell, Semaphore};

use crate::error::{
    ComputeArrowSnafu, Error, InvalidRecordBatchSnafu, NewRecordBatchSnafu, Result,
};
use crate::read::BoxedRecordBatchStream;
use crate::read::pruner::{Pruner, PrunerOptions};
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::PartitionMetrics;
use crate::read::seq_scan::SeqScan;
use crate::read::series_candidate::SeriesCandidateScanner;
use crate::read::series_scan::{RawScanPreparation, raw};
use crate::read::stream::ScanBatch;
use crate::series_index::artifact::{ArtifactFormat, SamplesSourceAssignments};
use crate::series_index::format::{
    self, FormatReader, SamplesFormatReader, SamplesReadRequest, SeriesReadRequest,
};
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
    plan: OnceLock<std::result::Result<Arc<SourcePlan>, Arc<Error>>>,
    source: OnceCell<std::result::Result<Arc<ColumnarPreparedSource>, Arc<Error>>>,
}

impl Preparation {
    /// Shares assignment validation and source selection with preflight.
    fn source_plan(
        &self,
        context: &StreamContext,
        ranges: &[PartitionRange],
    ) -> Result<Arc<SourcePlan>> {
        self.plan
            .get_or_init(|| {
                collect_inputs(context, ranges)
                    .map(Arc::new)
                    .map_err(Arc::new)
            })
            .clone()
            .map_err(|error| invalid(error.to_string()))
    }

    pub(crate) fn validate_inputs(
        &self,
        context: &StreamContext,
        ranges: &[PartitionRange],
    ) -> Result<bool> {
        self.source_plan(context, ranges).map(|plan| plan.supported)
    }

    pub(crate) async fn prepare(
        &self,
        context: Arc<StreamContext>,
        ranges: Vec<PartitionRange>,
        labels: RawLabelSource,
        raw: RawScanPreparation,
        metrics: PartitionMetrics,
    ) -> std::result::Result<Arc<dyn PreparedSeriesSource>, BoxedError> {
        // Per caller, including time awaiting another partition's shared preparation.
        // This overlaps prepare_wall and must not be added to its component timings.
        let acquire_timer = crate::metrics::READ_STAGE_ELAPSED
            .with_label_values(&["columnar_prepare_acquire_wall"])
            .start_timer();
        let plan = self
            .source_plan(&context, &ranges)
            .map_err(BoxedError::new)?;
        let expected = labels.clone();
        let result = self
            .source
            .get_or_init(|| async {
                ColumnarPreparedSource::prepare(context, plan, labels, raw, metrics)
                    .await
                    .map(Arc::new)
                    .map_err(Arc::new)
            })
            .await;
        drop(acquire_timer);
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

/// Source assignments derived solely from the pinned scan and artifact versions.
/// Discovery and sample replacement are deliberately independent.
#[derive(Clone, Default)]
struct SourcePlan {
    ranges: Vec<PartitionRange>,
    file_groups: BTreeMap<usize, HashSet<i64>>,
    supported: bool,
    indexes: HashSet<FileId>,
    discovery_files: HashSet<usize>,
    sample_files: HashSet<usize>,
    samples: HashSet<FileId>,
    has_memtables: bool,
    sample_sources: HashMap<FileId, HashSet<FileId>>,
}

/// Validates every assignment before deciding whether all sample sources are supported.
/// The mixed adapter consumes the same source plan when fallback readers are attached.
#[cfg(test)]
fn validate_inputs(context: &StreamContext, ranges: &[PartitionRange]) -> Result<bool> {
    collect_inputs(context, ranges).map(|plan| plan.supported)
}

fn collect_inputs(context: &StreamContext, ranges: &[PartitionRange]) -> Result<SourcePlan> {
    let input = &context.input;
    let metadata = input.region_metadata();
    let index = input
        .series_index
        .as_ref()
        .ok_or_else(|| invalid("missing columnar index context"))?;
    let mut plan = SourcePlan {
        ranges: ranges.to_vec(),
        supported: true,
        ..Default::default()
    };
    let mut files = BTreeMap::<usize, HashSet<i64>>::new();
    for range in ranges {
        let range = context
            .ranges
            .get(range.identifier)
            .ok_or_else(|| invalid("invalid prepared scan range"))?;
        for row_group in &range.row_group_indices {
            if context.is_mem_range_index(*row_group) {
                plan.has_memtables = true;
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
    let assigned: HashSet<_> = files
        .keys()
        .map(|i| input.files[*i].file_id().file_id())
        .collect();
    let complete: HashSet<_> = files
        .iter()
        .filter(|(index, groups)| {
            groups.contains(&-1)
                || groups.len() as u64 == input.files[**index].meta_ref().num_row_groups
        })
        .map(|(index, _)| input.files[*index].file_id().file_id())
        .collect();
    let trusted_sources: HashSet<_> = input
        .files
        .iter()
        .filter(|file| {
            input.sequence_range.is_none()
                || file.is_effective_target_sequence_trusted(metadata.region_id)
        })
        .map(|file| file.file_id().file_id())
        .collect();
    let allowed = complete
        .iter()
        .filter(|id| trusted_sources.contains(*id))
        .chain(&input.time_pruned_files)
        .copied()
        .collect();
    let mut visible = input
        .files
        .iter()
        .map(|file| file.file_id().file_id())
        .collect::<HashSet<_>>();
    visible.extend(&input.time_pruned_files);
    let assignments = SamplesSourceAssignments::new(
        &index.version.samples,
        &input.files,
        &visible,
        input.allow_sample_replacements && input.sequence_range.is_none(),
    );
    let mut eligible_samples: HashMap<_, _> = index
        .version
        .samples
        .iter()
        .filter(|(id, handle)| {
            handle.entry().artifact.format == ArtifactFormat::ColumnarSamples
                && assignments
                    .sources
                    .get(id)
                    .is_some_and(|sources| sources.is_subset(&allowed))
                && assigned
                    .iter()
                    .any(|source| assignments.covers(**id, *source))
        })
        .map(|(id, handle)| (*id, handle.clone()))
        .collect();
    for (file_idx, groups) in &files {
        let file = &input.files[*file_idx];
        if groups.iter().any(|group| {
            *group < -1 || (*group >= 0 && *group as u64 >= file.meta_ref().num_row_groups)
        }) {
            return Err(invalid(
                "invalid row group in complete SST range assignments",
            ));
        }
        if crate::region::parse_partition_expr(metadata.partition_expr.as_deref())?.as_ref()
            != file.meta_ref().partition_expr.as_ref()
        {
            plan.supported = false;
        }
        if file.meta_ref().region_id != metadata.region_id {
            return Err(invalid(
                "columnar prepared reads do not support foreign SSTs",
            ));
        }
        let mut covered = false;
        for handle in index.version.series_indexes.values() {
            if complete.contains(&file.file_id().file_id())
                && handle.entry().format() == ArtifactFormat::ColumnarSeries
                && handle
                    .entry()
                    .covers_file(file.meta_ref(), metadata.region_id)
            {
                plan.indexes.insert(handle.entry().index_uuid);
                covered = true;
            }
        }
        if !covered {
            plan.discovery_files.insert(*file_idx);
        }
    }
    // A whole-file artifact overlaps every daily representation. Two artifacts
    // for the same source/day also overlap, even if their generation IDs differ.
    // Prefer authoritative SSTs rather than arbitrarily choosing a generation.
    let overlapping: HashSet<_> = allowed
        .iter()
        .copied()
        .filter(|source| {
            let mut windows = HashSet::new();
            for (_, handle) in eligible_samples
                .iter()
                .filter(|(id, _)| assignments.covers(**id, *source))
            {
                let window = handle.entry().window;
                if (!windows.is_empty() && window.is_none())
                    || windows.contains(&None)
                    || !windows.insert(window)
                {
                    return true;
                }
            }
            false
        })
        .collect();
    eligible_samples.retain(|artifact, _| {
        !overlapping
            .iter()
            .any(|id| assignments.covers(*artifact, *id))
    });
    // Removing one incomplete source can invalidate another source in the same
    // grouped generation. Iterate to a fixed point before replacing any SST.
    loop {
        let uncovered: HashSet<_> = files
            .keys()
            .filter(|i| !assignments.covers_file(&eligible_samples, &input.files[**i]))
            .map(|i| input.files[*i].file_id().file_id())
            .collect();
        let before = eligible_samples.len();
        eligible_samples.retain(|artifact, _| {
            !uncovered
                .iter()
                .any(|id| assignments.covers(*artifact, *id))
        });
        if eligible_samples.len() == before {
            break;
        }
    }
    for file_idx in files.keys() {
        if !assignments.covers_file(&eligible_samples, &input.files[*file_idx]) {
            plan.sample_files.insert(*file_idx);
        }
    }
    if let Some(predicate) = input.predicate_group().predicate() {
        for expression in predicate.exprs().iter().flat_map(split_conjunction) {
            let mut has_tag = false;
            let mut has_sample = false;
            for column in expression.column_refs() {
                match metadata.column_by_name(&column.name) {
                    Some(column) if column.semantic_type == SemanticType::Tag => has_tag = true,
                    Some(_) => has_sample = true,
                    None => plan.supported = false,
                }
            }
            if has_tag && has_sample {
                plan.supported = false;
            }
        }
    }
    plan.file_groups = files;
    plan.samples = eligible_samples.into_keys().collect();
    plan.sample_sources = assignments.sources;
    Ok(plan)
}

/// Push only supported conjuncts; residual evaluation still enforces the complete predicate.
fn series_read_request(schema: &Schema, predicate: Option<&Predicate>) -> SeriesReadRequest {
    let exprs: Vec<_> = predicate
        .into_iter()
        .flat_map(|predicate| predicate.exprs())
        .flat_map(split_conjunction)
        .collect();
    let mut request = SeriesReadRequest {
        projection: schema
            .fields()
            .iter()
            .filter(|field| field.name() != ROW_COUNT_COLUMN)
            .map(|field| field.name().clone())
            .collect(),
        ..Default::default()
    };
    for expr in exprs {
        let Some(filter) = SimpleFilterEvaluator::try_new_with_column_type(expr, &|name| {
            schema
                .field_with_name(name)
                .ok()
                .map(|field| field.data_type().clone())
        }) else {
            continue;
        };
        if !request
            .projection
            .iter()
            .any(|name| name == filter.column_name())
        {
            continue;
        }
        if filter.column_name() == DATA_SCHEMA_TABLE_ID_COLUMN_NAME
            && filter.is_eq()
            && let Some(Value::UInt32(table_id)) = filter.literal_value()
        {
            // Keep all equality filters, including contradictory ones, for exact evaluation.
            request.table_id = Some(table_id);
        }
        request.filters.push(filter);
    }
    request
}

struct Candidate {
    id: MetricSeriesId,
    key: Buffer,
    merge_key: Buffer,
    tags: HashMap<u32, Option<String>>,
    metric: Option<SeriesLabels>,
    metadata_row: Option<(Arc<RecordBatch>, usize)>,
}

/// Checks every physical tag before retaining only rows that introduce new identities.
fn compact_candidate_batch(
    batch: &RecordBatch,
    candidates: &BTreeMap<MetricSeriesId, Candidate>,
    tags: &[(u32, String)],
) -> Result<RecordBatch> {
    let table = batch
        .column_by_name(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
        .and_then(|a| a.as_any().downcast_ref::<UInt32Array>())
        .ok_or_else(|| invalid("invalid table ID column"))?;
    let tsid = batch
        .column_by_name(DATA_SCHEMA_TSID_COLUMN_NAME)
        .and_then(|a| a.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| invalid("invalid TSID column"))?;
    let mut new_rows = HashMap::new();
    let mut selected = Vec::new();
    for row in 0..batch.num_rows() {
        let id = MetricSeriesId {
            table_id: table.value(row),
            tsid: tsid.value(row),
        };
        let previous = candidates
            .get(&id)
            .and_then(|c| c.metadata_row.as_ref())
            .map(|(batch, row)| (batch.as_ref(), *row))
            .or_else(|| new_rows.get(&id).map(|row| (batch, *row)));
        if let Some((previous, previous_row)) = previous {
            for (_, name) in tags {
                let current = batch
                    .column_by_name(name)
                    .and_then(|a| a.as_any().downcast_ref::<StringArray>())
                    .ok_or_else(|| invalid("invalid series tag array"))?;
                let previous = previous
                    .column_by_name(name)
                    .and_then(|a| a.as_any().downcast_ref::<StringArray>())
                    .ok_or_else(|| invalid("incompatible duplicate label metadata"))?;
                if previous.is_null(previous_row) != current.is_null(row)
                    || (!current.is_null(row) && previous.value(previous_row) != current.value(row))
                {
                    return Err(invalid("conflicting labels for the same table ID and TSID"));
                }
            }
        } else {
            new_rows.insert(id, row);
            selected.push(row as u64);
        }
    }
    if selected.len() == batch.num_rows() {
        return Ok(batch.clone());
    }
    take_record_batch(batch, &UInt64Array::from(selected)).context(ComputeArrowSnafu)
}

type ProjectedGroupEntries = VecDeque<((FileId, usize), Arc<ProjectedRowGroup>)>;
type ProjectedReaders =
    HashMap<usize, Arc<OnceLock<Vec<crate::sst::parquet::file_range::FileRange>>>>;

/// Query-local projected row groups, before selection of individual series.
struct ProjectedRowGroups {
    limit: usize,
    entries: tokio::sync::Mutex<ProjectedGroupEntries>,
    readers: tokio::sync::Mutex<ProjectedReaders>,
}

struct ProjectedRowGroup {
    batches: Vec<RecordBatch>,
    bytes: usize,
    _reservation: MemoryReservation,
}

impl Default for ProjectedRowGroups {
    fn default() -> Self {
        Self {
            limit: 64 * 1024 * 1024,
            entries: Default::default(),
            readers: Default::default(),
        }
    }
}

/// Record all fetches, including streaming overflow and cancellation.
struct ProjectedFetchMetrics(crate::sst::parquet::row_group::ParquetFetchMetrics);

impl Drop for ProjectedFetchMetrics {
    fn drop(&mut self) {
        if let Ok(fetched) = self.0.data.lock() {
            let mut metrics = crate::metrics::ColumnarReadMetrics::default();
            metrics.bytes.insert(
                "projected_group_parquet_store",
                fetched.page_size_to_fetch_store,
            );
            metrics.bytes.insert(
                "projected_group_parquet_cache",
                fetched.page_size_to_fetch_mem + fetched.page_size_to_fetch_write_cache,
            );
        }
    }
}

impl ProjectedRowGroups {
    fn read(
        self: Arc<Self>,
        range: crate::sst::parquet::file_range::FileRange,
        context: Arc<StreamContext>,
    ) -> BoxedRecordBatchStream {
        Box::pin(async_stream::try_stream! {
            let mut metrics = crate::metrics::ColumnarReadMetrics::default();
            let key = (range.file_handle().file_id().file_id(), range.row_group_index());
            let fetch = ProjectedFetchMetrics(Default::default());
            // Only completed loads are shared. A pending reader can be retained
            // without being polled, so it must never own sibling readers' progress.
            let cached = {
                let mut entries = self.entries.lock().await;
                if let Some(position) = entries.iter().position(|(cached, _)| *cached == key) {
                    let (_, group) = entries.remove(position).ok_or_else(|| invalid("missing cached group"))?;
                    entries.push_back((key, group.clone()));
                    Some(group)
                } else { None }
            };
            if let Some(group) = cached {
                metrics.counts.insert("projected_group_cache_hits", 1);
                for batch in &group.batches { yield batch.clone(); }
            } else if let Some(mut reader) = range.flat_reader(None, Some(&fetch.0)).await? {
                metrics.counts.insert("projected_group_cache_misses", 1);
                let reservation = MemoryConsumer::new("PreparedProjectedRowGroup")
                    .register(&context.input.scan_memory_pool);
                let mut batches = Vec::new();
                let mut bytes = 0;
                let mut overflow = None;
                while let Some(batch) = reader.next_batch().await? {
                    let batch = if let Some(compat) = range.compat_batch() {
                        compat.compat(batch)?
                    } else { batch };
                    let size = batch.get_array_memory_size();
                    if bytes + size > self.limit {
                        overflow = Some(batch);
                        break;
                    }
                    {
                        let mut entries = self.entries.lock().await;
                        while entries.iter().map(|(_, group)| group.bytes).sum::<usize>() + bytes + size > self.limit {
                            entries.pop_front();
                        }
                    }
                    if reservation.try_grow(size).is_err() {
                        // Active readers can still own evicted buffers. Do not
                        // fail a query just because reuse cannot reserve memory.
                        overflow = Some(batch);
                        break;
                    }
                    bytes += size;
                    batches.push(batch);
                }
                let group = Arc::new(ProjectedRowGroup { batches, bytes, _reservation: reservation });
                metrics.bytes.insert("projected_group_retained", bytes as u64);
                if overflow.is_some() { metrics.counts.insert("projected_group_streaming", 1); }
                if overflow.is_none() {
                    let mut entries = self.entries.lock().await;
                    // Concurrent misses may duplicate I/O, but retain only one result.
                    if !entries.iter().any(|(cached, _)| *cached == key) {
                        while entries.iter().map(|(_, group)| group.bytes).sum::<usize>() + bytes > self.limit {
                            entries.pop_front();
                        }
                        entries.push_back((key, group.clone()));
                    }
                }
                for batch in &group.batches { yield batch.clone(); }
                drop(group);
                if let Some(batch) = overflow {
                    yield batch;
                    while let Some(batch) = reader.next_batch().await? {
                        yield if let Some(compat) = range.compat_batch() {
                            compat.compat(batch)?
                        } else { batch };
                    }
                }
            }
        })
    }
}

struct ColumnarPreparedSource {
    projected_groups: Arc<ProjectedRowGroups>,
    plan: Arc<SourcePlan>,
    context: Arc<StreamContext>,
    labels: RawLabelSource,
    raw: Arc<RawScanPreparation>,
    sample_schema: Option<SchemaRef>,
    candidates: Arc<Vec<Candidate>>,
    samples: Arc<Vec<Arc<dyn SamplesFormatReader>>>,
    metrics: PartitionMetrics,
    // Readers retain this reservation with the candidate set.
    reservation: Arc<MemoryReservation>,
}

impl ColumnarPreparedSource {
    async fn prepare(
        context: Arc<StreamContext>,
        plan: Arc<SourcePlan>,
        labels: RawLabelSource,
        raw: RawScanPreparation,
        metrics: PartitionMetrics,
    ) -> Result<Self> {
        let _prepare_timer = crate::metrics::READ_STAGE_ELAPSED
            .with_label_values(&["columnar_prepare_wall"])
            .start_timer();
        let mut preparation_metrics = crate::metrics::ColumnarReadMetrics::default();
        preparation_metrics.counts.insert("prepare_executions", 1);
        let started = Instant::now();
        let input = &context.input;
        let metadata = input.region_metadata();
        let index = input
            .series_index
            .as_ref()
            .ok_or_else(|| invalid("missing columnar index context"))?;
        let mut plan = (*plan).clone();
        let indexes = plan.indexes.clone();
        preparation_metrics.add_elapsed("columnar_prepare_coverage", started.elapsed());
        let timer = preparation_metrics.timer("columnar_prepare_samples_open_wall");
        let cache = input
            .cache_strategy
            .columnar_metadata(index.cache_namespace);
        let mut opened_samples = Vec::<(FileId, Arc<dyn SamplesFormatReader>)>::new();
        let mut incompatible_sources = HashSet::new();
        for handle in index
            .version
            .samples
            .iter()
            .filter_map(|(id, h)| plan.samples.contains(id).then_some(h))
            .filter(|h| {
                // Coverage validation above still needs every day of each source. Only
                // opening readers can omit daily artifacts outside the query range.
                if let (Some(day), Some(range)) = (h.entry().window, input.time_range.as_ref()) {
                    let bounds = day
                        .checked_mul(86_400)
                        .zip(day.checked_add(1).and_then(|next| next.checked_mul(86_400)));
                    if let Some((start, end)) = bounds {
                        return range.intersects(
                            &(Timestamp::new_second(start)..Timestamp::new_second(end)).into(),
                        );
                    }
                    // Retain extreme days whose bounds cannot be represented in seconds.
                }
                true
            })
        {
            let reader = crate::series_index::samples::reader::SamplesReader::open_cached(
                index.store.clone(),
                handle.clone(),
                cache.clone(),
            )
            .await?
            .with_payload_cache(input.cache_strategy.columnar_payload(index.cache_namespace));
            let required = input.mapper.input_arrow_schema(false);
            let compatible = format::validate_sample_projection(
                &reader.schema(),
                metadata,
                required.fields().iter().filter_map(|field| {
                    metadata.column_by_name(field.name()).and_then(|column| {
                        (column.semantic_type != SemanticType::Tag).then_some(column.column_id)
                    })
                }),
            )
            .is_ok();
            if compatible {
                opened_samples.push((handle.file_id().file_id(), Arc::new(reader)));
            } else {
                for file in &input.files {
                    let id = file.file_id().file_id();
                    if plan.sample_sources[&handle.file_id().file_id()].contains(&id) {
                        incompatible_sources.insert(id);
                    }
                }
            }
        }
        // An incompatible day invalidates the entire replacement group, not
        // just that artifact: reading an SST alongside another day duplicates rows.
        loop {
            let before = incompatible_sources.len();
            for id in &plan.samples {
                let sources = &plan.sample_sources[id];
                if incompatible_sources
                    .iter()
                    .any(|source| sources.contains(source))
                {
                    for file in &input.files {
                        let source = file.file_id().file_id();
                        if sources.contains(&source) {
                            incompatible_sources.insert(source);
                        }
                    }
                }
            }
            if incompatible_sources.len() == before {
                break;
            }
        }
        let assigned_files: HashSet<_> = plan
            .ranges
            .iter()
            .flat_map(|range| {
                context.ranges[range.identifier]
                    .row_group_indices
                    .iter()
                    .filter(|group| context.is_file_range_index(**group))
                    .map(|group| group.index - input.num_memtables())
            })
            .collect();
        plan.sample_files.extend(
            assigned_files.into_iter().filter(|file| {
                incompatible_sources.contains(&input.files[*file].file_id().file_id())
            }),
        );
        let samples: Vec<_> = opened_samples
            .into_iter()
            .filter(|(id, _)| {
                !incompatible_sources
                    .iter()
                    .any(|source| plan.sample_sources[id].contains(source))
            })
            .map(|(_, reader)| reader)
            .collect();
        drop(timer);
        preparation_metrics
            .counts
            .insert("prepare_sample_readers", samples.len() as u64);
        preparation_metrics
            .counts
            .insert("prepare_series_indexes", indexes.len() as u64);
        let reservation =
            MemoryConsumer::new("ColumnarPreparedSource").register(&input.scan_memory_pool);
        let started = Instant::now();
        let mut raw = raw;
        let sample_schema = sample_merge_schema(&context, &mut raw)?;
        preparation_metrics.add_elapsed("columnar_prepare_schema", started.elapsed());
        preparation_metrics.bytes.insert("prepare_encoded_keys", 0);
        preparation_metrics.counts.insert("prepare_batch_series", 0);
        let direct_labels = sample_schema.is_some();
        let mut candidates = BTreeMap::<MetricSeriesId, Candidate>::new();
        let sources = indexes.iter().map(Some).chain(std::iter::once(None));
        for source in sources {
            let started = Instant::now();
            let (schema, mut batches) = if let Some(id) = source {
                let reader = crate::series_index::columnar::reader::ColumnarReader::open_cached(
                    index.store.clone(),
                    index.version.series_indexes[id].clone(),
                    cache.clone(),
                )
                .await?;
                let schema = reader.schema();
                if format::validate_series_schema(&schema, metadata).is_err() {
                    let entry = index.version.series_indexes[id].entry();
                    for assigned in &plan.ranges {
                        for group in &context.ranges[assigned.identifier].row_group_indices {
                            if context.is_file_range_index(*group) {
                                let file = group.index - input.num_memtables();
                                if entry
                                    .covers_file(input.files[file].meta_ref(), metadata.region_id)
                                {
                                    plan.discovery_files.insert(file);
                                }
                            }
                        }
                    }
                    continue;
                }
                let request = series_read_request(&schema, input.predicate_group().predicate());
                let projection = request
                    .projection
                    .iter()
                    .map(|name| schema.index_of(name).map_err(|e| invalid(e.to_string())))
                    .collect::<Result<Vec<_>>>()?;
                let projected = Arc::new(
                    schema
                        .project(&projection)
                        .map_err(|e| invalid(e.to_string()))?,
                );
                (projected, reader.read(request)?)
            } else {
                if plan.discovery_files.is_empty() && !plan.has_memtables {
                    continue;
                }
                discover_source_labels(context.clone(), &plan, metrics.clone()).await?
            };
            let mut tags = metadata
                .primary_key_columns()
                .filter(|column| !ReservedColumnId::is_reserved(column.column_id))
                .map(|column| (column.column_id, column.column_schema.name.clone()))
                .collect::<Vec<_>>();
            tags.sort_unstable_by(|a, b| a.1.cmp(&b.1));
            let projected = schema.as_ref();
            let mut fields = projected.fields().to_vec();
            let mut missing_labels = Vec::new();
            for (id, name) in &tags {
                if projected.index_of(name).is_err() {
                    fields.push(Arc::new(crate::sst::with_field_id(
                        datatypes::arrow::datatypes::Field::new(
                            name,
                            datatypes::arrow::datatypes::DataType::Utf8,
                            true,
                        ),
                        *id,
                    )));
                    missing_labels.push(name.clone());
                }
            }
            let projected_schema = Arc::new(Schema::new(fields));
            let mut filters = Vec::new();
            if let Some(predicate) = input.predicate_group().predicate() {
                for expr in predicate.exprs().iter().flat_map(split_conjunction) {
                    if expr
                        .column_refs()
                        .iter()
                        .all(|column| projected_schema.index_of(&column.name).is_ok())
                    {
                        filters.push(
                            Predicate::to_physical_expr(expr, &projected_schema)
                                .map_err(|e| invalid(e.to_string()))?,
                        );
                    }
                }
            }
            preparation_metrics.add_elapsed("columnar_prepare_compile", started.elapsed());
            loop {
                let timer = preparation_metrics.timer("columnar_prepare_discovery_wall");
                let batch = batches.try_next().await?;
                drop(timer);
                let Some(mut batch) = batch else {
                    break;
                };
                *preparation_metrics
                    .counts
                    .entry("prepare_discovered_rows")
                    .or_default() += batch.num_rows() as u64;
                *preparation_metrics
                    .counts
                    .entry("prepare_discovery_batches")
                    .or_default() += 1;
                if !missing_labels.is_empty() {
                    let mut columns = batch.columns().to_vec();
                    for _ in &missing_labels {
                        columns.push(Arc::new(StringArray::new_null(batch.num_rows())));
                    }
                    batch = RecordBatch::try_new(projected_schema.clone(), columns)
                        .context(NewRecordBatchSnafu)?;
                }
                let started = Instant::now();
                for filter in &filters {
                    batch = common_recordbatch::filter::batch_filter(&batch, filter)
                        .map_err(|e| invalid(e.to_string()))?;
                }
                preparation_metrics.add_elapsed("columnar_prepare_residual", started.elapsed());
                *preparation_metrics
                    .counts
                    .entry("prepare_selected_rows")
                    .or_default() += batch.num_rows() as u64;
                let started = Instant::now();
                let mut duplicate_rows = 0_u64;
                if direct_labels {
                    let rows = batch.num_rows();
                    batch = compact_candidate_batch(&batch, &candidates, &tags)?;
                    duplicate_rows = (rows - batch.num_rows()) as u64;
                }
                let table = batch
                    .column_by_name(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
                    .ok_or_else(|| invalid("missing table ID column"))?
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| invalid("invalid table ID column"))?;
                let tsid = batch
                    .column_by_name(DATA_SCHEMA_TSID_COLUMN_NAME)
                    .ok_or_else(|| invalid("missing TSID column"))?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| invalid("invalid TSID column"))?;
                let tag_arrays = tags
                    .iter()
                    .map(|(id, name)| {
                        let strings = batch
                            .column_by_name(name)
                            .ok_or_else(|| invalid("missing series tag column"))?
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| invalid("invalid series tag array"))?;
                        Ok((*id, strings))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let shared_batch = direct_labels.then(|| Arc::new(batch.clone()));
                let label_batch = if direct_labels {
                    let RawLabelSource::PrimaryKey { logical_tags, .. } = &labels else {
                        return Err(invalid("expected bound labels"));
                    };
                    let columns = logical_tags
                        .iter()
                        .map(|tag| {
                            let name = tags
                                .iter()
                                .find(|(id, _)| *id == tag.column_id)
                                .ok_or_else(|| invalid("missing bound tag"))?;
                            Ok((
                                tag.name.clone(),
                                batch
                                    .schema()
                                    .index_of(&name.1)
                                    .map_err(|e| invalid(e.to_string()))?,
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let owner = Arc::new(
                        MemoryConsumer::new("ColumnarSeriesLabelBatch")
                            .register(&input.scan_memory_pool),
                    );
                    let label_batch =
                        SeriesLabelBatch::try_new(batch.clone(), columns, owner.clone())
                            .map_err(|e| invalid(e.to_string()))?;
                    owner
                        .try_grow(label_batch.estimated_size())
                        .context(crate::error::ScanMemoryExhaustedSnafu)?;
                    Some(Arc::new(label_batch))
                } else {
                    None
                };
                let mut encoded_key_bytes = 0_u64;
                let mut tag_values = 0_u64;
                let mut batch_series = 0_u64;
                for row in 0..batch.num_rows() {
                    let id = MetricSeriesId {
                        table_id: table.value(row),
                        tsid: tsid.value(row),
                    };
                    // The flat merger still needs the compact sparse ID prefix.
                    // Explicit-label chunks use a region-qualified identity below.
                    let mut key = if direct_labels {
                        metadata.region_id.as_u64().to_be_bytes().to_vec()
                    } else {
                        Vec::new()
                    };
                    let identity_prefix_len = key.len();
                    let codec = SparsePrimaryKeyCodec::schemaless();
                    codec
                        .encode_internal(id.table_id, id.tsid, &mut key)
                        .map_err(|e| invalid(e.to_string()))?;
                    let merge_key_len = key.len() - identity_prefix_len;
                    if !direct_labels {
                        codec
                            .encode_to_vec(
                                tag_arrays.iter().map(|(id, strings)| {
                                    (
                                        *id,
                                        if strings.is_null(row) {
                                            ValueRef::Null
                                        } else {
                                            ValueRef::String(strings.value(row))
                                        },
                                    )
                                }),
                                &mut key,
                            )
                            .map_err(|e| invalid(e.to_string()))?;
                        encoded_key_bytes += key.len() as u64;
                    }
                    if let Some(previous) = candidates.get(&id) {
                        duplicate_rows += 1;
                        if previous.key.as_slice() != key.as_slice() {
                            return Err(invalid(
                                "conflicting labels for the same table ID and TSID",
                            ));
                        }
                        continue;
                    }
                    // Encode borrowed Arrow strings first. Duplicate identities still undergo
                    // the full label-conflict check, without allocating another owned tag map.
                    let mut values = HashMap::new();
                    if !direct_labels {
                        values.reserve(tag_arrays.len());
                        for (column_id, strings) in &tag_arrays {
                            values.insert(
                                *column_id,
                                (!strings.is_null(row)).then(|| strings.value(row).to_owned()),
                            );
                        }
                    }
                    tag_values += values.len() as u64;
                    let bytes = key.len()
                        + values.values().flatten().map(String::len).sum::<usize>()
                        + values.capacity() * (std::mem::size_of::<(u32, Option<String>)>() + 16)
                        + std::mem::size_of::<Candidate>()
                        + 64;
                    reservation
                        .try_grow(bytes)
                        .map_err(|e| invalid(e.to_string()))?;
                    let metric = if let Some(label_batch) = &label_batch {
                        batch_series += 1;
                        let RawLabelSource::PrimaryKey { metric_name, .. } = &labels else {
                            return Err(invalid("expected bound labels"));
                        };
                        Some(
                            SeriesLabels::from_batch(
                                Some(metric_name.clone()),
                                label_batch.clone(),
                                row,
                            )
                            .map_err(|e| invalid(e.to_string()))?,
                        )
                    } else {
                        None
                    };
                    let key = Buffer::from(key);
                    let merge_key = key.slice_with_length(identity_prefix_len, merge_key_len);
                    candidates.insert(
                        id,
                        Candidate {
                            id,
                            key,
                            merge_key,
                            tags: values,
                            metric,
                            metadata_row: shared_batch.as_ref().map(|batch| (batch.clone(), row)),
                        },
                    );
                }
                *preparation_metrics
                    .bytes
                    .entry("prepare_encoded_keys")
                    .or_default() += encoded_key_bytes;
                *preparation_metrics
                    .counts
                    .entry("prepare_tag_values")
                    .or_default() += tag_values;
                *preparation_metrics
                    .counts
                    .entry("prepare_duplicate_rows")
                    .or_default() += duplicate_rows;
                *preparation_metrics
                    .counts
                    .entry("prepare_batch_series")
                    .or_default() += batch_series;
                preparation_metrics.add_elapsed("columnar_prepare_candidates", started.elapsed());
            }
        }
        let started = Instant::now();
        preparation_metrics
            .counts
            .insert("prepare_unique_series", candidates.len() as u64);
        let mut candidates: Vec<_> = candidates.into_values().collect();
        candidates.sort_unstable_by(|a, b| a.key.as_slice().cmp(b.key.as_slice()));
        preparation_metrics.add_elapsed("columnar_prepare_sort", started.elapsed());
        Ok(Self {
            projected_groups: Arc::new(ProjectedRowGroups::default()),
            plan: Arc::new(plan),
            context,
            labels,
            raw: Arc::new(raw),
            sample_schema,
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
            metric: candidate.metric.clone(),
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
        let plan = self.plan.clone();
        let projected_groups = self.projected_groups.clone();
        let sample_schema = self.sample_schema.clone();
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
                let sample_schema = sample_schema.clone();
                sources.push(Box::pin(async_stream::try_stream! {
                    let reservation = MemoryConsumer::new("ColumnarSamplesRead").register(&context.input.scan_memory_pool);
                    // Account for the ID selection retained by the samples reader, including
                    // B-tree node overhead, before allocating it.
                    let selection_bytes = range.len() * (std::mem::size_of::<MetricSeriesId>() + 64);
                    reservation.try_grow(selection_bytes).context(crate::error::ScanMemoryExhaustedSnafu)?;
                    let ids = candidates[range.clone()].iter().map(|c| c.id).collect::<BTreeSet<_>>();
                    let mut stage_metrics = crate::metrics::ColumnarReadMetrics::default();
                    let mut batches = reader.read(SamplesReadRequest {
                        projection: sample_projection(&reader.schema(), &context),
                        memory_pool: Some(context.input.scan_memory_pool.clone()),
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
                            let started = std::time::Instant::now();
                            let flat = sample_to_flat(batch.slice(offset, end - offset), &candidates[ordinal], &context, sample_schema.as_ref())?;
                            let flat = filter_visibility(flat, &context)?;
                            stage_metrics.add_elapsed("columnar_sample_adapt", started.elapsed());
                            offset = end;
                            if flat.num_rows() == 0 { continue; }
                            let bytes = flat.get_array_memory_size();
                            reservation.try_grow(bytes).context(crate::error::ScanMemoryExhaustedSnafu)?;
                            yield flat;
                            reservation.shrink(bytes);
                        }
                    }
                }));
            }
            let mut seen_memtables = HashSet::new();
            for assigned in &plan.ranges {
                let assigned = &context.ranges[assigned.identifier];
                for index in &assigned.row_group_indices {
                    if !context.is_mem_range_index(*index)
                        || !seen_memtables.insert((index.index, index.row_group_index)) {
                        continue;
                    }
                    let mut batches = Box::pin(crate::read::scan_util::scan_flat_mem_ranges(
                        context.clone(), metrics.clone(), *index, assigned.time_range,
                    ));
                    let context = context.clone();
                    let candidates = candidates.clone();
                    let range = range.clone();
                    let schema = sample_schema.clone();
                    sources.push(Box::pin(async_stream::try_stream! {
                        while let Some(batch) = batches.try_next().await? {
                            for batch in adapt_encoded_samples(batch, &candidates, range.clone(), &context, schema.as_ref(), true)? {
                                yield batch;
                            }
                        }
                    }));
                }
            }
            for file_index in &plan.sample_files {
                let context = context.clone();
                let candidates = candidates.clone();
                let range = range.clone();
                let sample_schema = sample_schema.clone();
                let file_index = *file_index;
                let groups = plan.file_groups[&file_index].clone();
                let projected_groups = projected_groups.clone();
                sources.push(Box::pin(async_stream::try_stream! {
                    use crate::sst::parquet::reader::{QueryOutputMode, ReaderMetrics};
                    use crate::read::read_columns::ReadColumns;
                    let input = &context.input;
                    let reader = projected_groups.readers.lock().await
                        .entry(file_index).or_default().clone();
                    if reader.get().is_none() {
                    let file_ranges = async {
                    let required = input.mapper.input_arrow_schema(false);
                    let columns = required.fields().iter().filter_map(|field| {
                        input.region_metadata().column_by_name(field.name()).and_then(|column| {
                            (column.semantic_type != SemanticType::Tag).then_some(column.column_id)
                        })
                    });
                    let time_name = &input.region_metadata().time_index_column().column_schema.name;
                    let time_filters = input.predicate_group().predicate().into_iter()
                        .flat_map(|predicate| predicate.exprs()).flat_map(split_conjunction)
                        .filter(|expr| expr.column_refs().iter().all(|column| column.name == *time_name))
                        .cloned().collect::<Vec<_>>();
                    let predicate = (!time_filters.is_empty()).then(|| Predicate::new(time_filters));
                    let mut reader_metrics = ReaderMetrics::default();
                    let built = input.sst_reader(input.files[file_index].clone())
                        .predicate(predicate.clone())
                        .expected_metadata(Some(input.region_metadata().clone()))
                        .projection(Some(ReadColumns::new(columns)))
                        .cache(input.cache_strategy.clone())
                        .query_output_mode(QueryOutputMode::EncodedKey)
                        .enable_predicate_prefilter(false)
                        .build_reader_input(&mut reader_metrics).await?;
                    let mut file_ranges = smallvec::SmallVec::new();
                    if let Some((file_context, selection)) = built {
                        let stored = file_context.read_format().metadata();
                        let needs_compat = required.fields().iter().filter_map(|field| {
                            input.region_metadata().column_by_name(field.name())
                        }).filter(|column| column.semantic_type != SemanticType::Tag).any(|column| {
                            match stored.column_by_id(column.column_id) {
                                Some(old) => old.column_schema.name != column.column_schema.name
                                    || old.column_schema.data_type != column.column_schema.data_type,
                                None => !column.column_schema.is_nullable()
                                    || column.column_schema.default_constraint().is_some(),
                            }
                        });
                        let builder = if needs_compat {
                            // Exceptional schema transformations use the existing ordinary
                            // compatibility reader; additive labels stay on encoded output.
                            input.prune_file_after_manifest_check(
                                &input.files[file_index],
                                crate::sst::parquet::file_range::PreFilterMode::All,
                                false, predicate, &mut reader_metrics,
                            ).await?
                        } else {
                            crate::read::range::FileRangeBuilder::new(Arc::new(file_context), selection)
                        };
                        if groups.contains(&-1) {
                            builder.build_ranges(-1, &mut file_ranges);
                        } else {
                            let mut groups = groups.into_iter().collect::<Vec<_>>();
                            groups.sort_unstable();
                            for group in groups { builder.build_ranges(group, &mut file_ranges); }
                        }
                    }
                    Ok::<_, Error>(file_ranges.into_vec())
                    }.await?;
                    // Publish only completed setup; a parked initializer cannot block siblings.
                    let _ = reader.set(file_ranges);
                    }
                    let file_ranges = reader.get().ok_or_else(|| invalid("missing projected reader"))?;
                        for file_range in file_ranges {
                            let mut batches = projected_groups.clone().read(file_range.clone(), context.clone());
                            while let Some(batch) = batches.try_next().await? {
                                for batch in adapt_encoded_samples(batch, &candidates, range.clone(), &context, sample_schema.as_ref(), input.files[file_index].is_effective_target_sequence_trusted(input.region_metadata().region_id))? {
                                    yield batch;
                                }
                            }
                        }
                }));
            }
            let mut merged = if let Some(schema) = sample_schema {
                SeqScan::merge_flat_sources(&context, sources, Some(&metrics), false, schema, 0).await?
            } else {
                SeqScan::build_flat_reader_from_sources(&context, sources, None, Some(&metrics), false, 1).await?
            };
            while let Some(batch) = merged.try_next().await? {
                if batch.num_rows() > 0 {
                    yield ScanBatch::RecordBatch(batch);
                }
            }
        };
        let stream = raw::make_stream(
            Box::pin(stream),
            self.raw.schema.clone(),
            self.labels.clone(),
            self.raw.fields.clone(),
            self.metrics.clone(),
            self.raw.predicates.clone(),
            SeriesOrdering::KeyThenTime,
        );
        if self.sample_schema.is_none() {
            return Ok(stream);
        }
        let candidates = self.candidates.clone();
        let reservation = self.reservation.clone();
        Ok(raw::with_series_labels(stream, move |key| {
            // The resolver can outlive the exhausted sample stream.
            let _reservation = &reservation;
            let ordinal = candidates
                .binary_search_by(|candidate| candidate.merge_key.as_slice().cmp(key.as_slice()))
                .map_err(|_| BoxedError::new(invalid("unknown merged series identity")))?;
            Ok((
                candidates[ordinal].key.clone(),
                candidates[ordinal]
                    .metric
                    .clone()
                    .ok_or_else(|| BoxedError::new(invalid("identity merge is missing labels")))?,
            ))
        }))
    }
}

/// Selects candidate identities once per dictionary value, retaining sample buffers.
fn adapt_encoded_samples(
    batch: RecordBatch,
    candidates: &[Candidate],
    range: Range<usize>,
    context: &StreamContext,
    schema: Option<&SchemaRef>,
    sequence_trusted: bool,
) -> Result<Vec<RecordBatch>> {
    let keys = batch
        .column_by_name("__primary_key")
        .and_then(|array| array.as_any().downcast_ref::<DictionaryArray<UInt32Type>>())
        .ok_or_else(|| invalid("encoded samples require dictionary primary keys"))?;
    let values = keys
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| invalid("encoded sample dictionary must contain binary keys"))?;
    let mut metrics = crate::metrics::ColumnarReadMetrics::default();
    metrics
        .counts
        .insert("sample_dictionary_keys_decoded", values.len() as u64);
    let codec = SparsePrimaryKeyCodec::schemaless();
    let ordinals = values
        .iter()
        .map(|key| {
            let key = key.ok_or_else(|| invalid("null encoded sample key"))?;
            let (table_id, tsid) = codec.decode_ids(key).map_err(|e| invalid(e.to_string()))?;
            Ok(candidates[range.clone()]
                .binary_search_by_key(&MetricSeriesId { table_id, tsid }, |candidate| candidate.id)
                .ok()
                .map(|ordinal| range.start + ordinal))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut output = Vec::new();
    let mut start = 0;
    while start < batch.num_rows() {
        if keys.is_null(start) {
            return Err(invalid("null encoded sample key"));
        }
        let key = keys.keys().value(start);
        let mut end = start + 1;
        while end < batch.num_rows() && !keys.is_null(end) && keys.keys().value(end) == key {
            end += 1;
        }
        if let Some(ordinal) = ordinals[key as usize] {
            let flat = sample_to_flat(
                batch.slice(start, end - start),
                &candidates[ordinal],
                context,
                schema,
            )?;
            let flat = filter_source_visibility(flat, context, sequence_trusted)?;
            if flat.num_rows() != 0 {
                output.push(flat);
            }
        }
        start = end;
    }
    Ok(output)
}

/// Adapts distinct encoded source keys to the same label batches as native discovery.
async fn discover_source_labels(
    context: Arc<StreamContext>,
    plan: &SourcePlan,
    metrics: PartitionMetrics,
) -> Result<(SchemaRef, BoxedRecordBatchStream)> {
    use datatypes::arrow::datatypes::{DataType, Field};
    let pruner = Arc::new(Pruner::new_with_options(
        context.clone(),
        1,
        PrunerOptions {
            retain_builders: true,
            enable_predicate_prefilter: false,
        },
    ));
    let scanner = SeriesCandidateScanner::try_new(
        context.clone(),
        vec![plan.ranges.clone()],
        pruner,
        Arc::new(Semaphore::new(1)),
        context.input.scan_memory_pool.clone(),
        ExecutionPlanMetricsSet::new(),
        metrics,
    )?
    .with_discovery_files(&plan.discovery_files);
    let mut keys = scanner.build_key_stream().await?;
    let tags = context
        .input
        .region_metadata()
        .primary_key_columns()
        .filter(|column| !ReservedColumnId::is_reserved(column.column_id))
        .map(|column| (column.column_id, column.column_schema.name.clone()))
        .collect::<Vec<_>>();
    let mut fields = vec![
        Field::new(DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DataType::UInt32, false),
        Field::new(DATA_SCHEMA_TSID_COLUMN_NAME, DataType::UInt64, false),
    ];
    fields.extend(
        tags.iter().map(|(id, name)| {
            crate::sst::with_field_id(Field::new(name, DataType::Utf8, true), *id)
        }),
    );
    let schema = Arc::new(Schema::new(fields));
    let output_schema = schema.clone();
    let stream = async_stream::try_stream! {
        let _scanner = scanner;
        let mut offsets = SparseOffsetsCache::new();
        let mut buffer = Vec::new();
        while let Some(batch) = keys.try_next().await? {
            let keys = batch.column(0).as_any().downcast_ref::<BinaryArray>()
                .ok_or_else(|| invalid("candidate keys must be binary"))?;
            let mut tables = Vec::with_capacity(batch.num_rows());
            let mut tsids = Vec::with_capacity(batch.num_rows());
            let mut values = vec![Vec::<Option<String>>::with_capacity(batch.num_rows()); tags.len()];
            for key in keys.iter().flatten() {
                let mut view = SparsePrimaryKeyView::new(key, &mut offsets)
                    .map_err(|e| invalid(e.to_string()))?;
                tables.push(view.table_id());
                tsids.push(view.tsid());
                for (index, (id, _)) in tags.iter().enumerate() {
                    values[index].push(view.label(*id, &mut buffer)
                        .map_err(|e| invalid(e.to_string()))?.map(str::to_owned));
                }
            }
            let mut arrays: Vec<ArrayRef> = vec![Arc::new(UInt32Array::from(tables)), Arc::new(UInt64Array::from(tsids))];
            arrays.extend(values.into_iter().map(|values| Arc::new(StringArray::from(values)) as ArrayRef));
            yield RecordBatch::try_new(output_schema.clone(), arrays).context(NewRecordBatchSnafu)?;
        }
    };
    Ok((schema, Box::pin(stream)))
}

/// Omits tags only when every predicate is enforced either on the series index
/// or on sample columns. Conjunctions can be separated; mixed disjunctions
/// retain the ordinary flat adapter.
pub(crate) fn sample_merge_schema(
    context: &StreamContext,
    raw: &mut RawScanPreparation,
) -> Result<Option<SchemaRef>> {
    let metadata = context.input.region_metadata();
    let full = context.input.mapper.input_arrow_schema(false);
    let projection: Vec<_> = full
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, field)| {
            let tag = metadata
                .column_by_name(field.name())
                .is_some_and(|column| column.semantic_type == SemanticType::Tag);
            (!tag).then_some(i)
        })
        .collect();
    let schema = Arc::new(full.project(&projection).context(ComputeArrowSnafu)?);
    let mut predicates = Vec::new();
    if let Some(predicate) = context.input.predicate_group().predicate() {
        for expr in predicate.exprs().iter().flat_map(split_conjunction) {
            let columns = expr.column_refs();
            if columns.iter().all(|column| {
                metadata
                    .column_by_name(&column.name)
                    .is_some_and(|column| column.semantic_type == SemanticType::Tag)
            }) {
                // prepare() applies this complete expression to every series-index batch.
                continue;
            }
            if !columns
                .iter()
                .all(|column| schema.index_of(&column.name).is_ok())
            {
                return Ok(None);
            }
            predicates.push(
                Predicate::to_physical_expr(expr, &schema).map_err(|e| invalid(e.to_string()))?,
            );
        }
    }
    let fields = raw
        .fields
        .iter()
        .map(|(index, data_type)| {
            projection
                .iter()
                .position(|old| old == index)
                .map(|index| (index, data_type.clone()))
                .ok_or_else(|| invalid("sample merge is missing a selected field"))
        })
        .collect::<Result<Vec<_>>>()?;
    raw.fields = fields;
    raw.predicates = predicates;
    Ok(Some(schema))
}

fn sample_to_flat(
    batch: RecordBatch,
    candidate: &Candidate,
    context: &StreamContext,
    sample_schema: Option<&SchemaRef>,
) -> Result<RecordBatch> {
    let schema = sample_schema
        .cloned()
        .unwrap_or_else(|| context.input.mapper.input_arrow_schema(false));
    let key = if sample_schema.is_some() {
        &candidate.merge_key
    } else {
        &candidate.key
    };
    let mut metrics = crate::metrics::ColumnarReadMetrics::default();
    let metadata = context.input.region_metadata();
    metrics.bytes.insert(
        "sample_identity_keys_allocated",
        (batch.num_rows() * std::mem::size_of::<u32>()) as u64,
    );
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
                                i32::try_from(key.len()).map_err(|_| {
                                    invalid("series key exceeds Arrow offset limit")
                                })?,
                            ]
                            .into(),
                        ),
                        key.clone(),
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
                    *metrics
                        .counts
                        .entry("sample_tag_arrays_materialized")
                        .or_default() += 1;
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
                    match batch.column_by_name(name) {
                        Some(array) => array.clone(),
                        None if column.semantic_type == SemanticType::Field
                            && field.is_nullable() =>
                        {
                            datatypes::arrow::array::new_null_array(
                                field.data_type(),
                                batch.num_rows(),
                            )
                        }
                        None => return Err(invalid("missing sample field")),
                    }
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
    filter_source_visibility(batch, context, true)
}

fn filter_source_visibility(
    batch: RecordBatch,
    context: &StreamContext,
    sequence_trusted: bool,
) -> Result<RecordBatch> {
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
    let sequence_range = context.input.sequence_range.filter(|_| sequence_trusted);
    // Bounds are computed from the actual arrays: sample metadata does not expose
    // sequence bounds here, and format-reader timestamps need not be sorted.
    if sequence.null_count() == 0 && time.null_count() == 0 {
        let needs_sequence_max = context.input.snapshot_sequence.is_some()
            || matches!(
                sequence_range,
                Some(SequenceRange::LtEq { .. } | SequenceRange::GtLtEq { .. })
            );
        let sequence_max = needs_sequence_max.then(|| max(sequence)).flatten();
        let snapshot_visible = context
            .input
            .snapshot_sequence
            .is_none_or(|bound| sequence_max.is_some_and(|value| value <= bound));
        let range_visible = sequence_range.is_none_or(|range| match range {
            SequenceRange::Gt { min: bound } => min(sequence).is_some_and(|value| value > bound),
            SequenceRange::LtEq { max: bound } => sequence_max.is_some_and(|value| value <= bound),
            SequenceRange::GtLtEq {
                min: lower,
                max: upper,
            } => {
                sequence_max.is_some_and(|value| value <= upper)
                    && min(sequence).is_some_and(|value| value > lower)
            }
        });
        let time_visible = context.input.time_range.as_ref().is_none_or(|range| {
            min(&time).zip(max(&time)).is_some_and(|(lower, upper)| {
                range.contains(&common_time::Timestamp::new(lower, unit))
                    && range.contains(&common_time::Timestamp::new(upper, unit))
            })
        });
        if snapshot_visible && range_visible && time_visible {
            return Ok(batch);
        }
    }
    let sequence_filter = sequence_range
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

/// The mapper includes projected and residual-filter columns. Identity/time and
/// merge visibility columns are always needed by the native source adapter.
fn sample_projection(
    schema: &datatypes::arrow::datatypes::Schema,
    context: &StreamContext,
) -> Vec<String> {
    let input = context.input.mapper.input_arrow_schema(false);
    schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(index, field)| *index < 5 || input.index_of(field.name()).is_ok())
        .map(|(_, field)| field.name().clone())
        .collect()
}
