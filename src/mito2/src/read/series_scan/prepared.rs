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

mod narthex;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use api::v1::SemanticType;
use common_error::ext::BoxedError;
use common_recordbatch::filter::SimpleFilterEvaluator;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
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
use store_api::storage::SequenceRange;
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
use crate::series_index::format::{
    self, FormatReader, SamplesBatch, SamplesFormatReader, SamplesReadRequest, SeriesReadRequest,
};
use crate::series_index::identity::ExactSeriesKey;
use crate::series_index::{MetricSeriesId, ROW_COUNT_COLUMN};
use crate::sst::file::{FileTimeRange, WindowSequence};

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
    pub(crate) async fn prepare_batches(
        &self,
        context: Arc<StreamContext>,
        ranges: Vec<PartitionRange>,
        metrics: PartitionMetrics,
        scan_pool: Arc<dyn MemoryPool>,
    ) -> Result<Arc<ColumnarPreparedSource>> {
        let result = self
            .source
            .get_or_init(|| async {
                let raw = RawScanPreparation {
                    schema: Arc::new(datatypes::series::samples::SeriesSchema {
                        fields: Vec::new().into(),
                        eval_grid: None,
                    }),
                    fields: Vec::new(),
                    predicates: Vec::new(),
                };
                ColumnarPreparedSource::prepare(
                    context,
                    ranges,
                    RawLabelSource::Explicit,
                    raw,
                    metrics,
                    scan_pool,
                )
                .await
                .map(Arc::new)
                .map_err(Arc::new)
            })
            .await;
        let source = result
            .as_ref()
            .map_err(|error| crate::error::ScanSeriesSnafu.into_error(error.clone()))?;
        if !matches!(source.labels, RawLabelSource::Explicit) {
            return Err(invalid("cannot change prepared output representation"));
        }
        Ok(source.clone())
    }

    pub(crate) async fn prepare(
        &self,
        context: Arc<StreamContext>,
        ranges: Vec<PartitionRange>,
        labels: RawLabelSource,
        raw: RawScanPreparation,
        metrics: PartitionMetrics,
        scan_pool: Arc<dyn MemoryPool>,
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
                ColumnarPreparedSource::prepare(context, ranges, labels, raw, metrics, scan_pool)
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

pub(super) fn same_labels(left: &RawLabelSource, right: &RawLabelSource) -> bool {
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

/// Checks that the selected ranges refer to authoritative Narthex files.
pub(super) fn validate_ranges(context: &StreamContext, ranges: &[PartitionRange]) -> Result<()> {
    narthex::file_indices(context, ranges).map(|_| ())
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
    /// The canonical sparse key: the identity every comparison, sample attribution and output
    /// chunk uses. It must not be qualified by region: every region holding a fragment of one
    /// series has to report the same identity, or the fragments never meet in one owner above
    /// the regions.
    key: Buffer,
    tags: HashMap<u32, Option<String>>,
    metric: Option<SeriesLabels>,
}

pub(crate) struct ColumnarPreparedSource {
    context: Arc<StreamContext>,
    labels: RawLabelSource,
    raw: Arc<RawScanPreparation>,
    sample_schema: Option<SchemaRef>,
    merge_field_start: usize,
    // Append-mode Narthex raw reads still require one row per series/timestamp.
    dedup_append_raw: bool,
    sample_partition: Option<Arc<SamplePartitionFilter>>,
    candidates: Arc<Vec<Candidate>>,
    samples: Arc<Vec<Arc<dyn SamplesFormatReader>>>,
    metrics: PartitionMetrics,
    scan_pool: Arc<dyn MemoryPool>,
    // Readers retain this reservation with the candidate set.
    reservation: Arc<MemoryReservation>,
}

/// Only native index readers carry a coverage window. A failed index read can
/// schedule discovery from the corresponding pinned samples once per scan.
#[derive(Default)]
pub(crate) struct SeriesDiscovery {
    readers: VecDeque<(Box<dyn FormatReader>, Option<WindowSequence>)>,
    covered_samples: Vec<(FileTimeRange, usize)>,
}

impl ColumnarPreparedSource {
    async fn prepare(
        context: Arc<StreamContext>,
        ranges: Vec<PartitionRange>,
        labels: RawLabelSource,
        raw: RawScanPreparation,
        metrics: PartitionMetrics,
        scan_pool: Arc<dyn MemoryPool>,
    ) -> Result<Self> {
        let _prepare_timer = crate::metrics::READ_STAGE_ELAPSED
            .with_label_values(&["columnar_prepare_wall"])
            .start_timer();
        let mut preparation_metrics = crate::metrics::ColumnarReadMetrics::default();
        preparation_metrics.counts.insert("prepare_executions", 1);
        let started = Instant::now();
        let input = &context.input;
        let metadata = input.region_metadata();
        let (samples, mut series) = narthex::open_sources(&context, &ranges, &scan_pool).await?;
        preparation_metrics.add_elapsed("columnar_prepare_coverage", started.elapsed());
        preparation_metrics
            .counts
            .insert("prepare_sample_readers", samples.len() as u64);
        preparation_metrics
            .counts
            .insert("prepare_series_indexes", series.readers.len() as u64);
        let reservation = MemoryConsumer::new("ColumnarPreparedSource").register(&scan_pool);
        let started = Instant::now();
        let mut raw = raw;
        let batch_output = matches!(labels, RawLabelSource::Explicit);
        let dedup_append_raw = input.append_mode && !batch_output;
        let sample_schema = if batch_output {
            Some(input.mapper.input_arrow_schema(input.compaction))
        } else {
            sample_merge_schema(&context, &mut raw)?
        };
        // Sparse compaction omits tag columns, unlike the mapper's query batch.
        // Derive the offset from the schema that actually reaches the dedup reader.
        let merge_field_start = sample_schema.as_ref().map_or_else(
            || input.mapper.field_column_start(),
            |schema| {
                schema
                    .fields()
                    .iter()
                    .position(|field| {
                        metadata
                            .column_by_name(field.name())
                            .is_some_and(|column| column.semantic_type == SemanticType::Field)
                    })
                    .unwrap_or(0)
            },
        );
        preparation_metrics.add_elapsed("columnar_prepare_schema", started.elapsed());
        preparation_metrics.bytes.insert("prepare_encoded_keys", 0);
        preparation_metrics.counts.insert("prepare_batch_series", 0);
        let direct_labels = !batch_output && sample_schema.is_some();
        let conjuncts = predicate_conjuncts(input.predicate_group().predicate());
        let partition = narthex_partition_filter(metadata.partition_expr.as_deref(), None)?;
        // Series rows carry tags only, so a rule over any other column, such as the time index,
        // filters sample rows instead.
        let (series_partition, sample_partition) = match partition {
            Some(rule) if partition_on_tags_only(&rule, metadata) => (Some(rule), None),
            Some(rule) => (None, Some(SamplePartitionFilter::try_new(&rule, metadata)?)),
            None => (None, None),
        };
        let mut retained_rows = 0_usize;
        let mut candidates = BTreeMap::<ExactSeriesKey, Candidate>::new();
        while let Some((reader, window)) = series.readers.pop_front() {
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
            if let Some(partition) = &series_partition {
                filters.push(
                    partition
                        .try_as_physical_expr(&projected_schema)
                        .map_err(|error| invalid(error.to_string()))?,
                );
            }
            preparation_metrics.add_elapsed("columnar_prepare_compile", started.elapsed());
            let mut batches = reader.read(request)?;
            loop {
                let timer = preparation_metrics.timer("columnar_prepare_discovery_wall");
                let batch = batches.try_next().await;
                drop(timer);
                let batch = match (batch, window) {
                    (Ok(batch), _) => batch,
                    (Err(error), Some(window)) => {
                        common_telemetry::warn!(
                            ?error,
                            ?window,
                            "Series index read failed, discovering keys from samples"
                        );
                        series.fallback_to_samples(window, &samples, metadata, &scan_pool)?;
                        break;
                    }
                    (Err(error), None) => return Err(error),
                };
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
                        MemoryConsumer::new("ColumnarSeriesLabelBatch").register(&scan_pool),
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
                    if !direct_labels && (!batch_output || !input.compaction) {
                        values.reserve(tag_arrays.len());
                        for (column_id, strings) in &tag_arrays {
                            values.insert(
                                *column_id,
                                (!strings.is_null(row)).then(|| strings.value(row).to_owned()),
                            );
                        }
                    } else if let Some(filter) = &sample_partition {
                        for column_id in filter.tag_ids() {
                            let (_, strings) =
                                tag_arrays
                                    .iter()
                                    .find(|(id, _)| *id == column_id)
                                    .ok_or_else(|| invalid("missing partition tag column"))?;
                            values.insert(
                                column_id,
                                (!strings.is_null(row)).then(|| strings.value(row).to_owned()),
                            );
                        }
                    }
                    tag_values += values.len() as u64;
                    let key = Buffer::from(exact.bytes().to_vec());
                    let bytes = key.len()
                        + values.values().flatten().map(String::len).sum::<usize>()
                        + values.capacity() * (std::mem::size_of::<(u32, Option<String>)>() + 16)
                        + std::mem::size_of::<Candidate>()
                        + 64;
                    reservation
                        .try_grow(bytes)
                        .context(crate::error::ScanMemoryExhaustedSnafu)?;
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
            labels,
            raw: Arc::new(raw),
            sample_schema,
            merge_field_start,
            dedup_append_raw,
            sample_partition: sample_partition.map(Arc::new),
            candidates: Arc::new(candidates),
            samples: Arc::new(samples),
            metrics,
            scan_pool,
            reservation: Arc::new(reservation),
        })
    }
}

impl ColumnarPreparedSource {
    pub(crate) fn read_batches(&self, range: Range<usize>) -> Result<BoxedRecordBatchStream> {
        if range.start > range.end || range.end > self.candidates.len() {
            return Err(invalid("series range out of bounds"));
        }
        let context = self.context.clone();
        let sample_schema = self.sample_schema.clone();
        let candidates = self.candidates.clone();
        let samples = self.samples.clone();
        let metrics = self.metrics.clone();
        let reservation = self.reservation.clone();
        let scan_pool = self.scan_pool.clone();
        let field_column_start = self.merge_field_start;
        let dedup_append_raw = self.dedup_append_raw;
        let sample_partition = self.sample_partition.clone();
        // Time-only conjuncts drop every version of a (series, time) key alike, so they
        // commute with dedup and must run before the merge: a row selector inside the
        // merge would otherwise pick a row they reject. Other conjuncts may depend on
        // which version dedup keeps and stay after the merge. Volatile conjuncts never get
        // here: DataFusion's `PushDownFilter` keeps them in the `Filter` above the scan.
        let mut time_filters = Vec::new();
        let mut filters = Vec::new();
        if matches!(self.labels, RawLabelSource::Explicit) && !context.input.compaction {
            // Batch output keeps this schema before and after the merge.
            let schema = context.input.mapper.input_arrow_schema(false);
            let time_index = &context
                .input
                .region_metadata()
                .time_index_column()
                .column_schema
                .name;
            for expr in predicate_conjuncts(context.input.predicate_group().predicate()) {
                let filter = Predicate::to_physical_expr(expr, &schema)
                    .map_err(|error| invalid(error.to_string()))?;
                if expr
                    .column_refs()
                    .iter()
                    .all(|column| &column.name == time_index)
                {
                    time_filters.push(filter);
                } else {
                    filters.push(filter);
                }
            }
        }
        let time_filters = Arc::new(time_filters);
        let stream = async_stream::try_stream! {
            let _reservation = reservation;
            if range.is_empty() { return; }
            let mut sources = Vec::<BoxedRecordBatchStream>::new();
            for reader in samples.iter() {
                let candidates = candidates.clone();
                let range = range.clone();
                let context = context.clone();
                let reader = reader.clone();
                let sample_schema = sample_schema.clone();
                let scan_pool = scan_pool.clone();
                let sample_partition = sample_partition.clone();
                let time_filters = time_filters.clone();
                sources.push(Box::pin(async_stream::try_stream! {
                    let reservation = MemoryConsumer::new("ColumnarSamplesRead").register(&scan_pool);
                    // Account for the selection retained by the samples reader, including
                    // B-tree node overhead, before allocating it. The exact-key selection and
                    // the coarse identifiers the reader derives from it both live for the
                    // lifetime of the sample stream.
                    let selection_bytes = candidates[range.clone()]
                        .iter()
                        .map(|candidate| {
                            std::mem::size_of::<ExactSeriesKey>()
                                + candidate.key.len()
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
                        .map(|c| ExactSeriesKey::from_sparse_key(c.key.as_slice(), &mut offsets))
                        .collect::<Result<BTreeSet<_>>>()?;
                    let mut stage_metrics = crate::metrics::ColumnarReadMetrics::default();
                    let schema = reader.schema();
                    let metadata = context.input.region_metadata();
                    let name = &metadata.time_index_column().column_schema.name;
                    let field = schema.field_with_name(name).map_err(|err| invalid(err.to_string()))?;
                    let unit = match field.data_type() {
                        datatypes::arrow::datatypes::DataType::Timestamp(unit, _) => Ok(common_time::timestamp::TimeUnit::from(unit)),
                        _ => Err(invalid(format!("sample time index {name} must be a timestamp"))),
                    }?;
                    let bounds = context.input.time_range.as_ref();
                    let start = bounds.and_then(|r| *r.start()).and_then(|t| t.convert_to_ceil(unit)).map(|t| t.value());
                    let end = bounds.and_then(|r| *r.end()).and_then(|t| t.convert_to_ceil(unit)).map(|t| t.value());
                    let mut batches = reader.read(SamplesReadRequest {
                        projection: sample_projection(&schema, &context),
                        memory_pool: Some(scan_pool.clone()),
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
                                candidate.key.as_slice().cmp(key.bytes())
                            })
                            .map_err(|_| invalid("sample contains an unselected series"))?
                            + range.start;
                        let started = std::time::Instant::now();
                        let flat = sample_to_flat(batch, &candidates[ordinal], &context, sample_schema.as_ref())?;
                        let flat = match &sample_partition {
                            Some(filter) => filter.filter(flat, &candidates[ordinal])?,
                            None => flat,
                        };
                        let mut flat = filter_visibility(flat, &context)?;
                        for filter in time_filters.iter() {
                            flat = common_recordbatch::filter::batch_filter(&flat, filter)
                                .map_err(|error| invalid(error.to_string()))?;
                        }
                        stage_metrics.add_elapsed("columnar_sample_adapt", started.elapsed());
                        if flat.num_rows() == 0 { continue; }
                        let bytes = flat.get_array_memory_size();
                        reservation.try_grow(bytes).context(crate::error::ScanMemoryExhaustedSnafu)?;
                        yield flat;
                        reservation.shrink(bytes);
                    }
                }));
            }
            let mut merged = if let Some(schema) = sample_schema {
                SeqScan::merge_flat_sources(&context, sources, Some(&metrics), false, schema, field_column_start).await?
            } else {
                SeqScan::build_flat_reader_from_sources(&context, sources, None, Some(&metrics), false, 1).await?
            };
            // Append mode skips dedup in the merge reader, but raw series reads
            // require unique timestamps. Non-append reads use the configured merge
            // mode above; batch reads retain append semantics.
            if dedup_append_raw {
                merged = Box::pin(crate::read::flat_dedup::FlatDedupReader::new(
                    merged,
                    crate::read::flat_dedup::FlatLastRow::new(context.input.filter_deleted),
                    Some(metrics.dedup_metrics_reporter()),
                ).into_stream());
            }
            while let Some(mut batch) = merged.try_next().await? {
                for filter in &filters {
                    batch = common_recordbatch::filter::batch_filter(&batch, filter)
                        .map_err(|error| invalid(error.to_string()))?;
                }
                if batch.num_rows() > 0 {
                    yield batch;
                }
            }
        };
        Ok(Box::pin(stream))
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
        let mut batches = self.read_batches(range).map_err(BoxedError::new)?;
        let stream = async_stream::try_stream! {
            while let Some(batch) = batches.try_next().await? {
                yield ScanBatch::RecordBatch(batch);
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
                .binary_search_by(|candidate| candidate.key.as_slice().cmp(key.as_slice()))
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
    let key = &candidate.key;
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
                    match batch.column_by_name(name) {
                        Some(array) => array.clone(),
                        None if field.is_nullable() => datatypes::arrow::array::new_null_array(
                            field.data_type(),
                            batch.num_rows(),
                        ),
                        None => return Err(invalid("missing non-null sample field")),
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
/// merge visibility columns are always needed by the narthex source adapter. The exact
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

/// Returns a partition rule only when metrics rows need filtering for this owner.
fn narthex_partition_filter(
    region_partition: Option<&str>,
    file_partition: Option<&partition::expr::PartitionExpr>,
) -> Result<Option<partition::expr::PartitionExpr>> {
    Ok(region_partition
        .map(|json| {
            partition::expr::PartitionExpr::from_json_str(json)
                .map_err(|error| invalid(error.to_string()))
        })
        .transpose()?
        .flatten()
        .filter(|partition| file_partition != Some(partition)))
}

fn partition_on_tags_only(
    rule: &partition::expr::PartitionExpr,
    metadata: &store_api::metadata::RegionMetadata,
) -> bool {
    let mut columns = HashSet::new();
    rule.collect_column_names(&mut columns);
    columns.iter().all(|name| {
        metadata
            .column_by_name(name)
            .is_some_and(|column| column.semantic_type == SemanticType::Tag)
    })
}

/// A region partition rule that references a column other than a tag, applied to sample rows.
///
/// Files shared after repartition can hold rows of other partitions, so the rule has to drop them
/// row by row; a series may own rows on both sides of the rule.
struct SamplePartitionFilter {
    /// The columns the rule references, typed as the region declares them.
    schema: SchemaRef,
    /// Where each schema field's values come from.
    inputs: Vec<PartitionInput>,
    expr: Arc<dyn datafusion::physical_plan::PhysicalExpr>,
}

enum PartitionInput {
    /// Logical table ID of the series. Repartitioning a metric table may route on it, and the
    /// series index keeps it outside the tag columns.
    TableId,
    /// A tag column, by column ID, valued from the series.
    Tag(u32),
    /// A column the sample rows carry.
    Sample,
}

impl SamplePartitionFilter {
    fn try_new(
        rule: &partition::expr::PartitionExpr,
        metadata: &store_api::metadata::RegionMetadata,
    ) -> Result<Self> {
        let mut names = HashSet::new();
        rule.collect_column_names(&mut names);
        let mut names = names.into_iter().collect::<Vec<_>>();
        names.sort_unstable();
        let mut fields = Vec::with_capacity(names.len());
        let mut inputs = Vec::with_capacity(names.len());
        for name in names {
            let column = metadata
                .column_by_name(&name)
                .ok_or_else(|| invalid(format!("unknown partition column {name}")))?;
            fields.push(datatypes::arrow::datatypes::Field::new(
                name,
                datatypes::data_type::DataType::as_arrow_type(&column.column_schema.data_type),
                true,
            ));
            inputs.push(if column.column_id == ReservedColumnId::table_id() {
                PartitionInput::TableId
            } else if column.semantic_type == SemanticType::Tag {
                PartitionInput::Tag(column.column_id)
            } else {
                PartitionInput::Sample
            });
        }
        let schema = Arc::new(Schema::new(fields));
        let expr = rule
            .try_as_physical_expr(&schema)
            .map_err(|error| invalid(error.to_string()))?;
        Ok(Self {
            schema,
            inputs,
            expr,
        })
    }

    fn tag_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.inputs.iter().filter_map(|input| match input {
            PartitionInput::Tag(column_id) => Some(*column_id),
            _ => None,
        })
    }

    /// Keeps the rows of `flat`, all of `candidate`, that the rule assigns to this region.
    fn filter(&self, flat: RecordBatch, candidate: &Candidate) -> Result<RecordBatch> {
        let rows = flat.num_rows();
        let columns = self
            .schema
            .fields()
            .iter()
            .zip(&self.inputs)
            .map(|(field, input)| match input {
                PartitionInput::TableId => {
                    Ok(Arc::new(UInt32Array::from(vec![candidate.id.table_id; rows])) as ArrayRef)
                }
                PartitionInput::Tag(column_id) => {
                    let value = candidate
                        .tags
                        .get(column_id)
                        .ok_or_else(|| invalid("missing partition tag"))?;
                    Ok(Arc::new(StringArray::from(vec![value.as_deref(); rows])) as ArrayRef)
                }
                PartitionInput::Sample => {
                    let array = flat
                        .column_by_name(field.name())
                        .ok_or_else(|| invalid("missing partition column in samples"))?;
                    if array.data_type() == field.data_type() {
                        Ok(array.clone())
                    } else {
                        cast(array, field.data_type()).context(ComputeArrowSnafu)
                    }
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let columns = RecordBatch::try_new_with_options(
            self.schema.clone(),
            columns,
            &datatypes::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .context(NewRecordBatchSnafu)?;
        let mask = self
            .expr
            .evaluate(&columns)
            .and_then(|value| value.into_array(rows))
            .map_err(|error| invalid(error.to_string()))?;
        let mask = mask
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| invalid("partition rule must evaluate to booleans"))?;
        filter_record_batch(&flat, mask).context(ComputeArrowSnafu)
    }
}

/// Shares narthex discovery across partitions and assigns disjoint exact-series ranges.
pub(crate) fn scan_batches(
    preparation: Arc<Preparation>,
    context: Arc<StreamContext>,
    ranges: Vec<PartitionRange>,
    partition: usize,
    partitions: usize,
    metrics: PartitionMetrics,
    scan_pool: Arc<dyn MemoryPool>,
) -> crate::read::stream::ScanBatchStream {
    Box::pin(async_stream::try_stream! {
        let source = preparation.prepare_batches(context, ranges, metrics, scan_pool).await?;
        let n = source.candidates.len() as u128;
        let start = (n * partition as u128 / partitions as u128) as usize;
        let end = (n * (partition + 1) as u128 / partitions as u128) as usize;
        let mut stream = source.read_batches(start..end)?;
        while let Some(batch) = stream.try_next().await? {
            yield ScanBatch::RecordBatch(batch);
        }
    })
}
