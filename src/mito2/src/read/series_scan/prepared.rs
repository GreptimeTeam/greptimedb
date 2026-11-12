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
use std::time::Instant;

use api::v1::SemanticType;
use common_error::ext::BoxedError;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_time::Timestamp;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_expr::{Expr, utils::split_conjunction};
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
use mito_codec::row_converter::SparsePrimaryKeyCodec;
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
use tokio::sync::OnceCell;

use crate::error::{
    ComputeArrowSnafu, Error, InvalidRecordBatchSnafu, NewRecordBatchSnafu, Result,
};
use crate::read::BoxedRecordBatchStream;
use crate::read::pruner::PartitionPruner;
use crate::read::range::RowGroupIndex;
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::PartitionMetrics;
use crate::read::seq_scan::SeqScan;
use crate::read::series_scan::{RawScanPreparation, raw};
use crate::read::stream::ScanBatch;
use crate::series_index::artifact::ArtifactFormat;
use crate::series_index::format::{
    self, FormatReader, SamplesBatch, SamplesFormatReader, SamplesReadRequest, SeriesReadRequest,
};
use crate::series_index::identity::ExactSeriesKey;
use crate::series_index::{MetricSeriesId, ROW_COUNT_COLUMN};
use crate::sst::file::FileTimeRange;

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
    /// Prepares the scanner-wide source for the caller's captured [`SelectedInputs`].
    ///
    /// The cache holds at most one source per scanner, so the first preparation wins. A later
    /// caller may only reuse it when its label binding and its selection are identical: the
    /// capture is the single answer, and a different capture means a different assignment.
    pub(crate) async fn prepare(
        &self,
        context: Arc<StreamContext>,
        selection: SelectedInputs,
        labels: RawLabelSource,
        raw: RawScanPreparation,
        pruner: Arc<PartitionPruner>,
        metrics: PartitionMetrics,
    ) -> std::result::Result<Arc<dyn PreparedSeriesSource>, BoxedError> {
        // Per caller, including time awaiting another partition's shared preparation.
        // This overlaps prepare_wall and must not be added to its component timings.
        let acquire_timer = crate::metrics::READ_STAGE_ELAPSED
            .with_label_values(&["columnar_prepare_acquire_wall"])
            .start_timer();
        let expected = labels.clone();
        let expected_selection = selection.clone();
        let result = self
            .source
            .get_or_init(|| async {
                ColumnarPreparedSource::prepare(context, selection, labels, raw, pruner, metrics)
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
        if source.selection != expected_selection {
            // Every entry serves its own capture, so a different capture can only come from
            // another assignment.
            return Err(BoxedError::new(invalid(
                "series preparation selection changed",
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

/// Inputs one scanner-wide preparation may read, selected from the pinned context and ranges.
///
/// Selection only decides which assigned sources and which artifacts participate. It never filters
/// rows: every live row of a selected series is returned, including rows of an eligible grouped
/// sample artifact whose own source SST has no covering series index. Readiness governs which
/// artifacts are used, never which rows are visible: whatever no selected artifact covers is read
/// as raw rows and merged with the artifact rows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SelectedInputs {
    /// Indices into the scanner's assigned SST list whose columnar artifacts are complete.
    pub(crate) ready_files: Vec<usize>,
    /// Covering columnar series index ids, restricted to ready SSTs.
    pub(crate) series_indexes: HashSet<FileId>,
    /// Eligible sample artifacts covering at least one ready SST.
    pub(crate) sample_artifacts: HashSet<FileId>,
    /// Assigned sources no selected artifact carries rows for, in assignment order.
    ///
    /// Their rows are read by the ordinary flat readers and merged with the artifact rows, so an
    /// assigned memtable or an unbuilt SST narrows which artifacts are used, never the answer.
    pub(crate) raw_sources: Vec<RawSource>,
    /// Whether every assigned source is columnar-ready, so no raw source has to be read.
    ///
    /// This is an optimisation report, never a visibility decision: a `false` capture answers
    /// exactly like a `true` one, only with more sources read as raw rows. Vacuously `true` for an
    /// empty assignment.
    pub(crate) all_ready: bool,
}

/// One assigned source whose rows no selected artifact carries.
///
/// The prepared reader reads these as raw flat batches, so they keep their own sequence and op
/// type and take part in the final merge and dedup exactly like an ordinary scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RawSource {
    /// An assigned memtable range with the inclusive time bound of its assignment.
    Mem {
        /// Index into the pinned memtable list.
        index: usize,
        /// Row group of the memtable, or a negative value for all of them.
        row_group_index: i64,
        /// Inclusive time bound of the partition range that assigned this source.
        time_range: FileTimeRange,
    },
    /// An assigned SST file. Every assignment covers all of the file's row groups.
    File {
        /// Index into the pinned SST list.
        index: usize,
    },
}

/// Validates the pinned assignments and coverage without opening artifacts.
///
/// Returns whether every assigned source can be read from columnar artifacts, which is a report
/// about artifact coverage only: an incomplete capture is not an error and not a narrower answer,
/// because its uncovered sources are read as raw rows.
///
/// Test-only: the production entries capture the whole selection with [`select_inputs`] and serve
/// it from [`ColumnarPreparedSource`], so readiness no longer decides whether a source is read.
#[cfg(test)]
pub(crate) fn validate_inputs(context: &StreamContext, ranges: &[PartitionRange]) -> Result<bool> {
    select_inputs(context, ranges).map(|inputs| inputs.all_ready)
}

/// Selects the assigned SSTs a columnar read may serve and the artifacts that cover them.
///
/// Every assignment is validated before any readiness answer, so a malformed assignment is never
/// masked by an earlier unready one. `allowed` joins every assigned SST with the sources the query
/// pruned by time only, and is computed once: a grouped artifact may carry rows of a pruned source
/// that are still live rows of a discovered series, so the ready files must never narrow it.
///
/// Selection decides how a scan reads, never what it returns: the artifacts a read may use, and
/// the assigned sources it must read itself because no selected artifact carries their rows.
pub(crate) fn select_inputs(
    context: &StreamContext,
    ranges: &[PartitionRange],
) -> Result<SelectedInputs> {
    let input = &context.input;
    let metadata = input.region_metadata();
    let index = input
        .series_index
        .as_ref()
        .ok_or_else(|| invalid("missing columnar index context"))?;
    let mut assigned_memtable = false;
    let mut mem_ranges = Vec::<(RowGroupIndex, FileTimeRange)>::new();
    let mut files = BTreeMap::<usize, HashSet<i64>>::new();
    for range in ranges {
        let range = context
            .ranges
            .get(range.identifier)
            .ok_or_else(|| invalid("invalid prepared scan range"))?;
        for row_group in &range.row_group_indices {
            if context.is_mem_range_index(*row_group) {
                // Artifacts exist for flushed SSTs only, and a memtable is never converted here.
                // The assigned range is still read: no artifact carries its rows.
                assigned_memtable = true;
                match mem_ranges.iter_mut().find(|(index, _)| index == row_group) {
                    // One memtable row group may be assigned to more than one partition range.
                    // Widening the remembered bound keeps every assigned row reachable, and the
                    // merge visibility filter still applies the scan's own time range.
                    Some((_, bound)) => {
                        bound.0 = bound.0.min(range.time_range.0);
                        bound.1 = bound.1.max(range.time_range.1);
                    }
                    None => mem_ranges.push((*row_group, range.time_range)),
                }
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
    // Structural validation of every assigned SST, before any readiness answer.
    for (file_idx, groups) in &files {
        let file = &input.files[*file_idx];
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
    }
    // Grouped artifacts may also contain sources excluded solely by time pruning.
    // Sequence-pruned, unassigned, and retired sources remain ineligible.
    let assigned: HashSet<_> = files
        .keys()
        .map(|i| input.files[*i].file_id().file_id())
        .collect();
    let allowed: HashSet<_> = assigned
        .iter()
        .chain(&input.time_pruned_files)
        .copied()
        .collect();
    let eligible: HashMap<FileId, crate::series_index::artifact::SamplesFileHandle> = index
        .version
        .samples
        .iter()
        .filter(|(_, handle)| {
            handle.entry().sources_visible(&allowed)
                && assigned.iter().any(|id| handle.entry().covers(*id))
        })
        .map(|(id, handle)| (*id, handle.clone()))
        .collect();
    let mut selected = SelectedInputs {
        all_ready: !assigned_memtable,
        ..Default::default()
    };
    for file_idx in files.keys() {
        let file = &input.files[*file_idx];
        let id = file.file_id().file_id();
        let mut covering = HashSet::new();
        for handle in index.version.series_indexes.values() {
            if handle.entry().format() == ArtifactFormat::ColumnarSeries
                && handle
                    .entry()
                    .covers_file(file.meta_ref(), metadata.region_id)
            {
                covering.insert(handle.entry().index_uuid);
            }
        }
        let ready = !covering.is_empty()
            && crate::series_index::artifact::samples_cover_file(&eligible, file)
            && !index.version.samples.values().any(|h| {
                h.entry().covers(id) && h.entry().artifact.format != ArtifactFormat::ColumnarSamples
            });
        if !ready {
            selected.all_ready = false;
            continue;
        }
        selected.ready_files.push(*file_idx);
        selected.series_indexes.extend(covering);
        for (sample_id, handle) in &eligible {
            if handle.entry().covers(id) {
                selected.sample_artifacts.insert(*sample_id);
            }
        }
    }
    // Partition the assignment by artifact *source* coverage, never by readiness: a selected
    // grouped artifact already carries the rows of every source it lists, and a source it lists
    // need not have a covering series index of its own. Reading such a source again would emit
    // each of its rows twice, which append mode cannot deduplicate. Everything no selected
    // artifact covers is read as raw rows instead.
    //
    // Known limitation: the partition is per source, so a source a windowed group artifact
    // carries for only some UTC days counts as carried. Its rows outside those days are not read
    // as raw rows and stay invisible; a per-row partition would need artifact day metadata at row
    // granularity, which is the price of never emitting a row twice.
    let carried: Vec<_> = index
        .version
        .samples
        .iter()
        .filter(|(id, _)| selected.sample_artifacts.contains(id))
        .map(|(_, handle)| handle.entry().clone())
        .collect();
    selected.raw_sources = mem_ranges
        .into_iter()
        .map(|(index, time_range)| RawSource::Mem {
            index: index.index,
            row_group_index: index.row_group_index,
            time_range,
        })
        .collect();
    for file_idx in files.keys() {
        let id = input.files[*file_idx].file_id().file_id();
        if !carried.iter().any(|entry| entry.covers(id)) {
            selected
                .raw_sources
                .push(RawSource::File { index: *file_idx });
        }
    }
    Ok(selected)
}

/// Pushes the supported conjuncts into the request.
///
/// The second element reports, for every input conjunct, whether `ColumnarReader`
/// applies it exactly, so callers must not evaluate those conjuncts again.
fn series_read_request(schema: &Schema, conjuncts: &[&Expr]) -> (SeriesReadRequest, Vec<bool>) {
    let mut request = SeriesReadRequest {
        projection: schema
            .fields()
            .iter()
            .filter(|field| field.name() != ROW_COUNT_COLUMN)
            .map(|field| field.name().clone())
            .collect(),
        ..Default::default()
    };
    let mut accepted = Vec::with_capacity(conjuncts.len());
    for expr in conjuncts {
        let Some(filter) = SimpleFilterEvaluator::try_new_with_column_type(expr, &|name| {
            schema
                .field_with_name(name)
                .ok()
                .map(|field| field.data_type().clone())
        }) else {
            accepted.push(false);
            continue;
        };
        if !request
            .projection
            .iter()
            .any(|name| name == filter.column_name())
        {
            accepted.push(false);
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
        accepted.push(true);
    }
    (request, accepted)
}

/// Flattens the scanner predicate into the conjuncts shared by pushdown and residual filters.
fn predicate_conjuncts(predicate: Option<&Predicate>) -> Vec<&Expr> {
    predicate
        .into_iter()
        .flat_map(|predicate| predicate.exprs())
        .flat_map(split_conjunction)
        .collect()
}

struct Candidate {
    /// Coarse prefix of the exact key; only a hash, never identity.
    id: MetricSeriesId,
    /// The one owned key allocation: the region-qualified exact key.
    key: Buffer,
    /// The canonical sparse key, a view of `key`'s bytes. This is the identity every
    /// comparison and sample attribution uses, so it is stored once rather than once per
    /// representation.
    merge_key: Buffer,
    tags: HashMap<u32, Option<String>>,
    metric: Option<SeriesLabels>,
}

impl Candidate {
    /// Builds a candidate from an exact key alone, without reading any sample payload.
    ///
    /// Labels come from the key itself: the sparse key encodes every non-null label, so a source
    /// whose series are known only by their keys still yields the same identity, the same
    /// region-qualified output key, and the same labels an artifact row would carry.
    fn from_key(
        exact: &ExactSeriesKey,
        metadata: &store_api::metadata::RegionMetadataRef,
        labels: &RawLabelSource,
        direct_labels: bool,
        reservation: &MemoryReservation,
    ) -> Result<Self> {
        let mut offsets = mito_codec::row_converter::SparseOffsetsCache::new();
        let mut view = mito_codec::row_converter::sparse::SparsePrimaryKeyView::new(
            exact.bytes(),
            &mut offsets,
        )
        .map_err(|e| invalid(e.to_string()))?;
        let mut buffer = Vec::new();
        let mut tags = HashMap::new();
        for column in &metadata.column_metadatas {
            if column.semantic_type != SemanticType::Tag
                || ReservedColumnId::is_reserved(column.column_id)
            {
                continue;
            }
            let value = view
                .label(column.column_id, &mut buffer)
                .map_err(|e| invalid(e.to_string()))?;
            tags.insert(column.column_id, value.map(str::to_owned));
        }
        // `merge_key` is always the exact key bytes. Explicit-label chunks keep the
        // region-qualified form as their `key`, exactly like a discovered artifact row.
        let (key, merge_key) = if direct_labels {
            let mut qualified = Vec::with_capacity(8 + exact.bytes().len());
            qualified.extend_from_slice(&metadata.region_id.as_u64().to_be_bytes());
            qualified.extend_from_slice(exact.bytes());
            let key = Buffer::from(qualified);
            let merge_key = key.slice_with_length(8, exact.bytes().len());
            (key, merge_key)
        } else {
            let key = Buffer::from(exact.bytes().to_vec());
            (key.clone(), key)
        };
        let metric = if direct_labels {
            let RawLabelSource::PrimaryKey {
                metric_name,
                logical_tags,
                ..
            } = labels
            else {
                return Err(invalid("expected bound labels"));
            };
            let mut values = Vec::with_capacity(logical_tags.len());
            for tag in logical_tags.iter() {
                if let Some(Some(value)) = tags.get(&tag.column_id) {
                    values.push(datatypes::series::Label {
                        name: tag.name.clone(),
                        value: Arc::from(value.as_str()),
                    });
                }
            }
            Some(
                SeriesLabels::try_new(Some(metric_name.clone()), values)
                    .map_err(|e| invalid(e.to_string()))?,
            )
        } else {
            None
        };
        let bytes = key.len()
            + values_size(&tags)
            + std::mem::size_of::<Candidate>()
            + metric
                .as_ref()
                .map(|metric| {
                    metric
                        .tags()
                        .iter()
                        .map(|tag| tag.name.len() + tag.value.len())
                        .sum::<usize>()
                })
                .unwrap_or(0)
            + 64;
        reservation
            .try_grow(bytes)
            .context(crate::error::ScanMemoryExhaustedSnafu)?;
        Ok(Self {
            id: exact.id(),
            key,
            merge_key,
            tags,
            metric,
        })
    }
}

/// Estimates the memory a tag map retains, matching the charge the artifact path pays.
fn values_size(values: &HashMap<u32, Option<String>>) -> usize {
    values.values().flatten().map(String::len).sum::<usize>()
        + values.capacity() * (std::mem::size_of::<(u32, Option<String>)>() + 16)
}

/// The pinned index of one raw source, with every row group of its assignment.
fn raw_source_index(context: &StreamContext, source: &RawSource) -> RowGroupIndex {
    match source {
        // Memtable indices are already indices into the pinned source list.
        RawSource::Mem {
            index,
            row_group_index,
            ..
        } => RowGroupIndex {
            index: *index,
            row_group_index: *row_group_index,
        },
        // A validated assignment always covers every row group of an SST.
        RawSource::File { index } => RowGroupIndex {
            index: context.input.num_memtables() + *index,
            row_group_index: -1,
        },
    }
}

/// Discovers the exact keys that exist only in the sources no selected artifact covers.
///
/// Discovery reads key columns only: the payload of these sources is read later, inside
/// [`ColumnarPreparedSource::read_series`], so preparation never retains sample data and every
/// ordinal read stays independent. The keys are the SST's own sparse encoding, so a series found
/// here has the same identity, ordering, and labels an artifact discovery would give it, and one
/// found by both is still a single candidate.
///
/// Known limitation: candidates come from the covering series indexes and from these uncovered
/// sources only, so a series that exists *solely* in an SST a selected artifact covers but that has
/// no series index of its own is not discovered, and its artifact rows stay invisible. Enumerating
/// the keys a samples artifact carries would close that gap.
#[allow(clippy::too_many_arguments)]
async fn discover_raw_series(
    context: &Arc<StreamContext>,
    selection: &SelectedInputs,
    pruner: &Arc<PartitionPruner>,
    metrics: &PartitionMetrics,
    labels: &RawLabelSource,
    direct_labels: bool,
    reservation: &MemoryReservation,
    candidates: &mut BTreeMap<ExactSeriesKey, Candidate>,
    preparation_metrics: &mut crate::metrics::ColumnarReadMetrics,
) -> Result<()> {
    let metadata = context.input.region_metadata();
    let mut offsets = mito_codec::row_converter::SparseOffsetsCache::new();
    for source in &selection.raw_sources {
        let index = raw_source_index(context, source);
        let keys: Option<BoxedRecordBatchStream> = match source {
            RawSource::Mem { time_range, .. } => {
                Some(crate::read::series_candidate::mem_primary_key_stream(
                    context,
                    metrics,
                    index,
                    *time_range,
                ))
            }
            RawSource::File { .. } => {
                crate::read::series_candidate::file_primary_key_stream(
                    context, metrics, index, pruner,
                )
                .await?
            }
        };
        let Some(mut keys) = keys else {
            continue;
        };
        let mut discovered = 0_u64;
        while let Some(batch) = keys.try_next().await? {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| invalid("raw series discovery is not a binary key stream"))?;
            for key in array.iter().flatten() {
                if candidates.contains_key(key) {
                    continue;
                }
                let exact = ExactSeriesKey::from_sparse_key(key, &mut offsets)?;
                let candidate =
                    Candidate::from_key(&exact, metadata, labels, direct_labels, reservation)?;
                candidates.insert(exact, candidate);
                discovered += 1;
            }
        }
        *preparation_metrics
            .counts
            .entry("prepare_raw_discovered_series")
            .or_default() += discovered;
    }
    Ok(())
}

struct ColumnarPreparedSource {
    context: Arc<StreamContext>,
    /// Inputs this source was built from; a shared cache is only reusable for an equal selection.
    selection: SelectedInputs,
    labels: RawLabelSource,
    raw: Arc<RawScanPreparation>,
    sample_schema: Option<SchemaRef>,
    candidates: Arc<Vec<Candidate>>,
    samples: Arc<Vec<Arc<dyn SamplesFormatReader>>>,
    /// Builds the file ranges of the sources no selected artifact covers.
    pruner: Arc<PartitionPruner>,
    metrics: PartitionMetrics,
    // Readers retain this reservation with the candidate set.
    reservation: Arc<MemoryReservation>,
}

impl ColumnarPreparedSource {
    /// Builds the prepared source from an already selected input set.
    ///
    /// Readiness is never recomputed here: the caller's capture is the single answer, so an
    /// artifact that merely covers a ready SST stays readable even when another of its sources
    /// has no covering series index.
    async fn prepare(
        context: Arc<StreamContext>,
        selection: SelectedInputs,
        labels: RawLabelSource,
        raw: RawScanPreparation,
        pruner: Arc<PartitionPruner>,
        metrics: PartitionMetrics,
    ) -> Result<Self> {
        let _prepare_timer = crate::metrics::READ_STAGE_ELAPSED
            .with_label_values(&["columnar_prepare_wall"])
            .start_timer();
        let mut preparation_metrics = crate::metrics::ColumnarReadMetrics::default();
        preparation_metrics.counts.insert("prepare_executions", 1);
        let input = &context.input;
        let metadata = input.region_metadata();
        let index = input
            .series_index
            .as_ref()
            .ok_or_else(|| invalid("missing columnar index context"))?;
        let timer = preparation_metrics.timer("columnar_prepare_samples_open_wall");
        let cache = input
            .cache_strategy
            .columnar_metadata(index.cache_namespace);
        let mut samples = Vec::<Arc<dyn SamplesFormatReader>>::new();
        for handle in index.version.samples.values().filter(|h| {
            if !selection.sample_artifacts.contains(&h.file_id().file_id()) {
                return false;
            }
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
        }) {
            let reader = crate::series_index::samples::reader::SamplesReader::open_cached(
                index.store.clone(),
                handle.clone(),
                cache.clone(),
            )
            .await?
            .with_payload_cache(input.cache_strategy.columnar_payload(index.cache_namespace));
            format::validate_sample_schema(&reader.schema(), metadata)?;
            samples.push(Arc::new(reader));
        }
        drop(timer);
        preparation_metrics
            .counts
            .insert("prepare_sample_readers", samples.len() as u64);
        preparation_metrics.counts.insert(
            "prepare_series_indexes",
            selection.series_indexes.len() as u64,
        );
        // Whether this preparation had full artifact coverage, so no raw source had to be read.
        preparation_metrics
            .counts
            .insert("prepare_all_ready", u64::from(selection.all_ready));
        let reservation =
            MemoryConsumer::new("ColumnarPreparedSource").register(&input.scan_memory_pool);
        let started = Instant::now();
        let mut raw = raw;
        let sample_schema = sample_merge_schema(&context, &mut raw)?;
        preparation_metrics.add_elapsed("columnar_prepare_schema", started.elapsed());
        preparation_metrics.bytes.insert("prepare_encoded_keys", 0);
        preparation_metrics.counts.insert("prepare_batch_series", 0);
        let direct_labels = sample_schema.is_some();
        let conjuncts = predicate_conjuncts(input.predicate_group().predicate());
        let mut retained_rows = 0_usize;
        let mut candidates = BTreeMap::<ExactSeriesKey, Candidate>::new();
        for id in &selection.series_indexes {
            let timer = preparation_metrics.timer("columnar_prepare_series_open_wall");
            let reader = crate::series_index::columnar::reader::ColumnarReader::open_cached(
                index.store.clone(),
                index.version.series_indexes[id].clone(),
                cache.clone(),
            )
            .await?;
            drop(timer);
            let started = Instant::now();
            let schema = reader.schema();
            format::validate_series_schema(&schema, metadata)?;
            let mut tags = schema
                .fields()
                .iter()
                .skip(3)
                .map(|field| {
                    let id = format::field_id(field)?;
                    Ok((id, field.name().clone()))
                })
                .collect::<Result<Vec<_>>>()?;
            tags.sort_unstable_by(|a, b| a.1.cmp(&b.1));
            let (request, accepted) = series_read_request(&schema, &conjuncts);
            let projection = request
                .projection
                .iter()
                .map(|name| schema.index_of(name).map_err(|e| invalid(e.to_string())))
                .collect::<Result<Vec<_>>>()?;
            let projected_schema = Arc::new(
                schema
                    .project(&projection)
                    .map_err(|e| invalid(e.to_string()))?,
            );
            let mut filters = Vec::new();
            for (expr, accepted) in conjuncts.iter().zip(&accepted) {
                // The reader already applied accepted conjuncts exactly.
                if *accepted
                    || !expr
                        .column_refs()
                        .iter()
                        .all(|column| projected_schema.index_of(&column.name).is_ok())
                {
                    continue;
                }
                filters.push(
                    Predicate::to_physical_expr(expr, &projected_schema)
                        .map_err(|e| invalid(e.to_string()))?,
                );
            }
            preparation_metrics.add_elapsed("columnar_prepare_compile", started.elapsed());
            let mut batches = reader.read(request)?;
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
                // Identity is the exact canonical sparse key. `tsid` is a hash, so rows
                // that differ only in labels are different series and must both survive:
                // re-encoding the artifact's own tags reproduces the SST key byte for byte.
                let codec = SparsePrimaryKeyCodec::schemaless();
                let mut offsets = mito_codec::row_converter::SparseOffsetsCache::new();
                let mut identity = Vec::new();
                let mut previous: Option<Vec<u8>> = None;
                let mut fresh = Vec::<(ExactSeriesKey, usize)>::new();
                let mut encoded_key_bytes = 0_u64;
                for row in 0..batch.num_rows() {
                    identity.clear();
                    codec
                        .encode_internal(table.value(row), tsid.value(row), &mut identity)
                        .map_err(|e| invalid(e.to_string()))?;
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
                            &mut identity,
                        )
                        .map_err(|e| invalid(e.to_string()))?;
                    encoded_key_bytes += identity.len() as u64;
                    // Adjacent duplicates are the common case and never allocate a key.
                    if previous.as_deref() == Some(identity.as_slice())
                        || candidates.contains_key(identity.as_slice())
                    {
                        duplicate_rows += 1;
                        continue;
                    }
                    let key = ExactSeriesKey::from_sparse_key(&identity, &mut offsets)?;
                    previous = Some(identity.clone());
                    fresh.push((key, row));
                }
                // Retain only rows that introduce a new identity; the retained batch backs
                // both explicit labels and the metadata rows of direct-label candidates.
                let retained = if fresh.len() == batch.num_rows() {
                    batch.clone()
                } else {
                    let selected: Vec<u64> = fresh.iter().map(|(_, row)| *row as u64).collect();
                    take_record_batch(&batch, &UInt64Array::from(selected))
                        .context(ComputeArrowSnafu)?
                };
                if direct_labels {
                    // Only explicit-label chunks retain a batch to resolve labels from.
                    retained_rows += retained.num_rows();
                }
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
                                retained
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
                        SeriesLabelBatch::try_new(retained.clone(), columns, owner.clone())
                            .map_err(|e| invalid(e.to_string()))?;
                    owner
                        .try_grow(label_batch.estimated_size())
                        .context(crate::error::ScanMemoryExhaustedSnafu)?;
                    Some(Arc::new(label_batch))
                } else {
                    None
                };
                let mut tag_values = 0_u64;
                let mut batch_series = 0_u64;
                for (retained_row, (exact, row)) in fresh.into_iter().enumerate() {
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
                    // `merge_key` is always the exact key bytes. Explicit-label chunks keep
                    // the region-qualified form as their `key`, so `primary_key` stays
                    // region-scoped while identity and ordering stay exact.
                    let key = if direct_labels {
                        let mut buffer = Vec::with_capacity(8 + exact.bytes().len());
                        buffer.extend_from_slice(&metadata.region_id.as_u64().to_be_bytes());
                        buffer.extend_from_slice(exact.bytes());
                        Buffer::from(buffer)
                    } else {
                        Buffer::from(exact.bytes().to_vec())
                    };
                    let merge_key = if direct_labels {
                        key.slice_with_length(8, exact.bytes().len())
                    } else {
                        key.clone()
                    };
                    // The exact key is stored once: `merge_key` is a view of `key`, so the
                    // reservation charges the single key allocation the candidate retains.
                    let bytes =
                        key.len() + values_size(&values) + std::mem::size_of::<Candidate>() + 64;
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
                                retained_row,
                            )
                            .map_err(|e| invalid(e.to_string()))?,
                        )
                    } else {
                        None
                    };
                    let id = exact.id();
                    candidates.insert(
                        exact,
                        Candidate {
                            id,
                            key,
                            merge_key,
                            tags: values,
                            metric,
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
        // Series that exist only in a source no artifact covers have no series index to be
        // discovered from, so their keys are read from the uncovered sources themselves. This is
        // key-only discovery: the payload of those sources is read inside `read_series`, so the
        // ordinal set stays complete and stable while the source retains no sample data.
        if !selection.raw_sources.is_empty() {
            let started = Instant::now();
            discover_raw_series(
                &context,
                &selection,
                &pruner,
                &metrics,
                &labels,
                direct_labels,
                &reservation,
                &mut candidates,
                &mut preparation_metrics,
            )
            .await?;
            preparation_metrics.add_elapsed("columnar_prepare_raw_series", started.elapsed());
        }
        let started = Instant::now();
        preparation_metrics
            .counts
            .insert("prepare_unique_series", candidates.len() as u64);
        preparation_metrics
            .counts
            .insert("prepare_retained_rows", retained_rows as u64);
        let mut candidates: Vec<_> = candidates.into_values().collect();
        candidates.sort_unstable_by(|a, b| a.key.as_slice().cmp(b.key.as_slice()));
        preparation_metrics.add_elapsed("columnar_prepare_sort", started.elapsed());
        Ok(Self {
            context,
            selection,
            labels,
            raw: Arc::new(raw),
            sample_schema,
            candidates: Arc::new(candidates),
            samples: Arc::new(samples),
            pruner,
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
        let sample_schema = self.sample_schema.clone();
        let candidates = self.candidates.clone();
        let samples = self.samples.clone();
        let metrics = self.metrics.clone();
        let reservation = self.reservation.clone();
        let raw_sources = self.selection.raw_sources.clone();
        let pruner = self.pruner.clone();
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
                    // Account for the selection retained by the samples reader, including
                    // B-tree node overhead, before allocating it. The exact-key selection and
                    // the coarse identifiers the reader derives from it both live for the
                    // lifetime of the sample stream.
                    let selection_bytes = candidates[range.clone()]
                        .iter()
                        .map(|candidate| {
                            std::mem::size_of::<ExactSeriesKey>()
                                + candidate.merge_key.len()
                                + std::mem::size_of::<MetricSeriesId>()
                                + 64
                        })
                        .sum::<usize>();
                    reservation.try_grow(selection_bytes).context(crate::error::ScanMemoryExhaustedSnafu)?;
                    // Exact ownership: a block is read only when its own key is selected,
                    // never because a colliding partner shares the identifier. Coarse
                    // pruning bounds are derived from these keys inside the reader.
                    // The keys are built from the one key the candidate retains, so the
                    // selection costs one owned copy per series rather than a second copy
                    // alongside every candidate.
                    let mut offsets = mito_codec::row_converter::SparseOffsetsCache::new();
                    let keys = candidates[range.clone()]
                        .iter()
                        .map(|c| ExactSeriesKey::from_sparse_key(c.merge_key.as_slice(), &mut offsets))
                        .collect::<Result<BTreeSet<_>>>()?;
                    let mut stage_metrics = crate::metrics::ColumnarReadMetrics::default();
                    let mut batches = reader.read(SamplesReadRequest {
                        projection: sample_projection(&reader.schema(), &context),
                        memory_pool: Some(context.input.scan_memory_pool.clone()),
                        keys: Some(keys),
                        start,
                        end,
                        ..Default::default()
                    })?;
                    while let Some(samples) = batches.try_next().await? {
                        // A block belongs to exactly one series, so the reader returns its
                        // exact key once and the whole batch is attributed at once.
                        let SamplesBatch { batch, key } = samples;
                        let ordinal = candidates[range.clone()]
                            .binary_search_by(|candidate| {
                                candidate.merge_key.as_slice().cmp(key.bytes())
                            })
                            .map_err(|_| invalid("sample contains an unselected series"))?
                            + range.start;
                        let started = std::time::Instant::now();
                        let flat = sample_to_flat(batch, &candidates[ordinal], &context, sample_schema.as_ref())?;
                        let flat = filter_visibility(flat, &context)?;
                        stage_metrics.add_elapsed("columnar_sample_adapt", started.elapsed());
                        if flat.num_rows() == 0 { continue; }
                        let bytes = flat.get_array_memory_size();
                        reservation.try_grow(bytes).context(crate::error::ScanMemoryExhaustedSnafu)?;
                        yield flat;
                        reservation.shrink(bytes);
                    }
                }));
            }
            // Rows of the sources no artifact covers join the same merge as the artifact samples.
            // They keep their own sequence and op type, so a memtable update or delete still
            // applies to an artifact row and the final dedup decides visibility.
            for source in raw_sources.iter() {
                let candidates = candidates.clone();
                let range = range.clone();
                let context = context.clone();
                let sample_schema = sample_schema.clone();
                let pruner = pruner.clone();
                let metrics = metrics.clone();
                let source = *source;
                sources.push(Box::pin(async_stream::try_stream! {
                    let reservation = MemoryConsumer::new("ColumnarRawSource").register(&context.input.scan_memory_pool);
                    let index = raw_source_index(&context, &source);
                    let mut raw: BoxedRecordBatchStream = match source {
                        RawSource::Mem { time_range, .. } => Box::pin(
                            crate::read::scan_util::scan_flat_mem_ranges(
                                context.clone(),
                                metrics.clone(),
                                index,
                                time_range,
                            ),
                        ),
                        RawSource::File { .. } => Box::pin(
                            crate::read::scan_util::scan_flat_file_ranges(
                                context.clone(),
                                metrics.clone(),
                                index,
                                "columnar_raw_source",
                                pruner,
                            )
                            .await?,
                        ),
                    };
                    while let Some(batch) = raw.try_next().await? {
                        // One raw batch carries several series, and only the ordinals this read
                        // serves belong to this stream.
                        let Some(batch) = retain_candidate_rows(batch, &candidates[range.clone()])? else { continue };
                        let batch = merge_shape(batch, sample_schema.as_ref())?;
                        let batch = filter_visibility(batch, &context)?;
                        if batch.num_rows() == 0 { continue; }
                        let bytes = batch.get_array_memory_size();
                        reservation.try_grow(bytes).context(crate::error::ScanMemoryExhaustedSnafu)?;
                        yield batch;
                        reservation.shrink(bytes);
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

/// Keeps only the rows whose exact key belongs to the ordinals this read serves.
///
/// A raw source carries many series in one batch, so the batch is filtered before the merge: the
/// ordinals of one `read_series` call are exactly its candidate keys, and `tsid` alone is a hash
/// that two distinct series can share. Returns `None` when no row survives.
fn retain_candidate_rows(
    batch: RecordBatch,
    candidates: &[Candidate],
) -> Result<Option<RecordBatch>> {
    let primary_key = batch
        .column(crate::sst::parquet::flat_format::primary_key_column_index(
            batch.num_columns(),
        ))
        .as_any()
        .downcast_ref::<crate::sst::parquet::format::PrimaryKeyArray>()
        .ok_or_else(|| invalid("raw source is missing a dictionary primary key"))?;
    let values = primary_key
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| invalid("raw source primary key values are not binary"))?;
    // The batch is dictionary encoded, so membership is decided once per distinct key.
    let mut retained_values = vec![false; values.len()];
    for (index, retained) in retained_values.iter_mut().enumerate() {
        *retained = candidates
            .binary_search_by(|candidate| candidate.merge_key.as_slice().cmp(values.value(index)))
            .is_ok();
    }
    if retained_values.iter().all(|retained| *retained) {
        return Ok(Some(batch));
    }
    let keys = primary_key.keys();
    let mask = BooleanArray::from(
        (0..batch.num_rows())
            .map(|row| keys.is_valid(row) && retained_values[keys.value(row) as usize])
            .collect::<Vec<_>>(),
    );
    if mask.true_count() == 0 {
        return Ok(None);
    }
    filter_record_batch(&batch, &mask)
        .map(Some)
        .context(ComputeArrowSnafu)
}

/// Reshapes a raw flat batch into the schema the merge expects.
///
/// The artifact source emits the tagless sample schema when the predicate allows labels to be
/// resolved after the merge; a raw batch keeps its own tag columns, so it is projected onto the
/// same columns in the same order. Without that schema both sides already share the mapper schema.
fn merge_shape(batch: RecordBatch, schema: Option<&SchemaRef>) -> Result<RecordBatch> {
    let Some(schema) = schema else {
        return Ok(batch);
    };
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let column = batch
            .column_by_name(field.name())
            .ok_or_else(|| invalid(format!("raw source is missing column {}", field.name())))?;
        columns.push(if column.data_type() == field.data_type() {
            column.clone()
        } else {
            cast(column, field.data_type()).context(ComputeArrowSnafu)?
        });
    }
    RecordBatch::try_new(schema.clone(), columns).context(NewRecordBatchSnafu)
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
    // Bounds are computed from the actual arrays: sample metadata does not expose
    // sequence bounds here, and format-reader timestamps need not be sorted.
    if sequence.null_count() == 0 && time.null_count() == 0 {
        let needs_sequence_max = context.input.snapshot_sequence.is_some()
            || matches!(
                context.input.sequence_range,
                Some(SequenceRange::LtEq { .. } | SequenceRange::GtLtEq { .. })
            );
        let sequence_max = needs_sequence_max.then(|| max(sequence)).flatten();
        let snapshot_visible = context
            .input
            .snapshot_sequence
            .is_none_or(|bound| sequence_max.is_some_and(|value| value <= bound));
        let range_visible = context
            .input
            .sequence_range
            .is_none_or(|range| match range {
                SequenceRange::Gt { min: bound } => {
                    min(sequence).is_some_and(|value| value > bound)
                }
                SequenceRange::LtEq { max: bound } => {
                    sequence_max.is_some_and(|value| value <= bound)
                }
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

/// The mapper includes projected and residual-filter columns. Identity/time and
/// merge visibility columns are always needed by the native source adapter. The exact
/// series key is not projected: the samples reader returns it once per batch.
fn sample_projection(
    schema: &datatypes::arrow::datatypes::Schema,
    context: &StreamContext,
) -> Vec<String> {
    let input = context.input.mapper.input_arrow_schema(false);
    schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(index, field)| {
            field.name() != store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME
                && (*index < 5 || input.index_of(field.name()).is_ok())
        })
        .map(|(_, field)| field.name().clone())
        .collect()
}
