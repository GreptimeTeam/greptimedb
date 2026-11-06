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
use datafusion_expr::utils::split_conjunction;
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, DictionaryArray, StringArray, UInt32Array,
    UInt64Array,
};
use datatypes::arrow::buffer::{Buffer, OffsetBuffer};
use datatypes::arrow::compute::{cast, filter_record_batch, take_record_batch};
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
        // Per caller, including time awaiting another partition's shared preparation.
        // This overlaps prepare_wall and must not be added to its component timings.
        let acquire_timer = crate::metrics::READ_STAGE_ELAPSED
            .with_label_values(&["columnar_prepare_acquire_wall"])
            .start_timer();
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
    // Grouped artifacts may also contain sources excluded solely by time pruning.
    // Sequence-pruned, unassigned, and retired sources remain ineligible.
    let assigned: HashSet<_> = files
        .keys()
        .map(|i| input.files[*i].file_id().file_id())
        .collect();
    let allowed = assigned
        .iter()
        .chain(&input.time_pruned_files)
        .copied()
        .collect();
    let eligible_samples = index
        .version
        .samples
        .iter()
        .filter(|(_, handle)| {
            handle.entry().sources_visible(&allowed)
                && assigned.iter().any(|id| handle.entry().covers(*id))
        })
        .map(|(id, handle)| (*id, handle.clone()))
        .collect();
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
        if !crate::series_index::artifact::samples_cover_file(&eligible_samples, file)
            || index.version.samples.values().any(|h| {
                h.entry().covers(file.file_id().file_id())
                    && h.entry().artifact.format != ArtifactFormat::ColumnarSamples
            })
        {
            ready = false;
        }
    }
    Ok(ready.then_some((file_indices, indexes)))
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

struct ColumnarPreparedSource {
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
        ranges: Vec<PartitionRange>,
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
        let Some((files, indexes)) = collect_inputs(&context, &ranges)? else {
            // Readiness was checked before preparation, so this is a real capability change.
            return Err(invalid(
                "columnar series coverage changed after series preparation",
            ));
        };
        preparation_metrics.add_elapsed("columnar_prepare_coverage", started.elapsed());
        let timer = preparation_metrics.timer("columnar_prepare_samples_open_wall");
        let cache = input
            .cache_strategy
            .columnar_metadata(index.cache_namespace);
        let mut samples = Vec::<Arc<dyn SamplesFormatReader>>::new();
        let assigned: HashSet<_> = files
            .iter()
            .map(|i| input.files[*i].file_id().file_id())
            .collect();
        let allowed = assigned
            .iter()
            .chain(&input.time_pruned_files)
            .copied()
            .collect();
        for handle in index.version.samples.values().filter(|h| {
            if !h.entry().sources_visible(&allowed)
                || !assigned.iter().any(|id| h.entry().covers(*id))
            {
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
            .await?;
            format::validate_sample_schema(&reader.schema(), metadata)?;
            samples.push(Arc::new(reader));
        }
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
        for id in indexes {
            let timer = preparation_metrics.timer("columnar_prepare_series_open_wall");
            let reader = crate::series_index::columnar::reader::ColumnarReader::open_cached(
                index.store.clone(),
                index.version.series_indexes[&id].clone(),
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
            let request = series_read_request(&schema, input.predicate_group().predicate());
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
