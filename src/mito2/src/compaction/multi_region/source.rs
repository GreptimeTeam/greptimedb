// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use common_base::readable_size::ReadableSize;
use common_time::Timestamp;
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, DictionaryArray, GenericByteArray, GenericByteDictionaryBuilder,
    UInt32Array, UInt64Array,
};
use datatypes::arrow::compute::{concat_batches, take_record_batch};
use datatypes::arrow::datatypes::{
    BinaryType, ByteArrayType, DataType, Field, Schema, SchemaRef, UInt32Type, Utf8Type,
};
use datatypes::arrow::record_batch::RecordBatch;
use mito_codec::row_converter::{PrimaryKeyCodec, build_primary_key_codec};
use parquet::file::metadata::PageIndexPolicy;
use partition::PartitionRuleRef;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::RegionId;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use crate::cache::CacheStrategy;
use crate::compaction::json2::collect_json2_rewrite_plans_from_parquet;
use crate::compaction::multi_region::memory::{MemoryLimits, UploadBudget};
use crate::compaction::multi_region::metrics::{self, StageGuard};
use crate::compaction::multi_region::{
    MultiRegionCompactionContext, MultiRegionCompactionPlan, MultiRegionTarget, SourceCounters,
};
use crate::config::MemoryThreshold;
use crate::error::{
    ComputeArrowSnafu, ExternalSnafu, InvalidMultiRegionCompactionSnafu,
    MultiRegionCompactionResourceSnafu, NewRecordBatchSnafu, Result,
};
use crate::read::flat_merge::FlatMergeReader;
use crate::read::flat_projection::FlatProjectionMapper;
use crate::read::read_columns::ReadColumns;
use crate::read::scan_region::ScanInput;
use crate::read::{BoxedRecordBatchStream, timestamp_array_to_i64_slice};
use crate::sst::file::{FileHandle, FileMeta};
use crate::sst::file_purger::NoopFilePurger;
use crate::sst::index::IndexBuildType;
use crate::sst::index::handler::PlanContext;
use crate::sst::index::registry::{IndexBuildPlan, IndexRegistry};
use crate::sst::parquet::decoded_input_size;
use crate::sst::parquet::file_range::PreFilterMode;
use crate::sst::parquet::flat_format::{
    decode_primary_keys, primary_key_column_index, sequence_column_index, time_index_column_index,
};
use crate::sst::parquet::format::{PrimaryKeyArray, need_override_sequence};
use crate::sst::parquet::reader::{MetadataCacheMetrics, ReaderMetrics};
use crate::sst::parquet::row_group::ParquetFetchMetrics;

pub(crate) struct SourceEvent {
    pub(crate) region_id: RegionId,
    pub(crate) batch: RecordBatch,
}

/// Complete leading-key groups, ordered by target, key-group ordinal and version.
/// Only the next bounded fragment is materialized, not one array per destination.
struct RoutedBatch {
    batch: RecordBatch,
    rows: Vec<(RegionId, usize, Reverse<u64>, usize)>,
    offset: usize,
}

impl RoutedBatch {
    fn next_fragment(&mut self, source: &SharedSource) -> Result<Option<SourceEvent>> {
        source.check_cancelled()?;
        let Some(&(region_id, _, _, _)) = self.rows.get(self.offset) else {
            return Ok(None);
        };
        let rows = &self.rows[self.offset..];
        let len = rows
            .partition_point(|row| row.0 == region_id)
            .min(source.limits.batch_rows);
        let indices = UInt64Array::from_iter_values(rows[..len].iter().map(|row| row.3 as u64));
        let batch = take_record_batch(&self.batch, &indices).context(ComputeArrowSnafu)?;
        let batch = compact_dictionaries(batch)?;
        ensure!(
            batch.get_array_memory_size() <= source.limits.arrow_batch_bytes,
            MultiRegionCompactionResourceSnafu {
                reason: "Routed batch exceeds the task buffer budget"
            }
        );
        self.offset += len;
        Ok(Some(SourceEvent { region_id, batch }))
    }
}

pub(crate) struct SharedSource {
    sources: Vec<FileMeta>,
    legacy_overrides: Vec<bool>,
    expected_rows: Vec<i64>,
    targets: BTreeMap<RegionId, MultiRegionTarget>,
    metadata: RegionMetadataRef,
    scan_input: ScanInput,
    primary_key_codec: Arc<dyn PrimaryKeyCodec>,
    schema: SchemaRef,
    merge_schema: SchemaRef,
    partition_rule: PartitionRuleRef,
    window: (Timestamp, Timestamp),
    pub(crate) limits: MemoryLimits,
    pub(crate) index_plans: BTreeMap<RegionId, Arc<IndexBuildPlan>>,
    /// Rows and decoded bytes of the sources, which size output row groups.
    pub(crate) input_size: (u64, u64),
    peak_group_bytes: AtomicU64,
    cancellation: CancellationToken,
    stop: CancellationToken,
    counters: Arc<SourceCounters>,
    memory_admission: Option<Arc<OwnedSemaphorePermit>>,
}

impl SharedSource {
    pub(crate) async fn build(
        plan: &MultiRegionCompactionPlan,
        context: &MultiRegionCompactionContext,
        metadata: RegionMetadataRef,
        counters: Arc<SourceCounters>,
    ) -> Result<Arc<Self>> {
        let upload = UploadBudget::new(plan, context.access_layer.object_store())?;
        let mut common = (*metadata).clone();
        common.set_partition_expr(None);
        let metadata = Arc::new(common);
        let cache = CacheStrategy::Compaction(context.cache.clone());
        let mut footers = Vec::with_capacity(plan.sources.len());
        let mut footer_bytes = 0usize;
        for file in &plan.sources {
            ensure!(
                !context.cancellation.is_cancelled(),
                crate::error::CompactionCancelledSnafu
            );
            let handle = FileHandle::new(file.clone(), Arc::new(NoopFilePurger));
            let path = handle.file_path(
                context.access_layer.table_dir(),
                context.access_layer.path_type(),
            );
            let (sst, _) = context
                .access_layer
                .read_sst(handle)
                .cache(cache.clone())
                .read_parquet_metadata(
                    &path,
                    file.file_size,
                    &mut MetadataCacheMetrics::default(),
                    PageIndexPolicy::Skip,
                )
                .await?;
            let footer = sst.parquet_metadata();
            footer_bytes = footer_bytes.saturating_add(footer.memory_size());
            ensure!(
                footer_bytes <= upload.compute_bytes / 8,
                MultiRegionCompactionResourceSnafu {
                    reason: format!(
                        "Source footer metadata needs {footer_bytes} bytes, budget after reserving uploads is {}",
                        upload.compute_bytes / 8,
                    ),
                }
            );
            footers.push(footer);
        }
        let legacy_overrides = footers
            .iter()
            .map(|footer| need_override_sequence(footer))
            .collect();
        let expected_rows = footers
            .iter()
            .map(|footer| footer.file_metadata().num_rows())
            .collect();
        let input_size = decoded_input_size(footers.iter().map(Arc::as_ref));
        let layouts = collect_json2_rewrite_plans_from_parquet(&metadata, &footers)?;
        let json2_targets = crate::compaction::reader::json2_rewrite_targets(&metadata, &layouts)?;
        let mapper = FlatProjectionMapper::new_with_json2_rewrite_targets(
            &metadata,
            (0..metadata.column_metadatas.len()).collect(),
            ReadColumns::new(
                metadata
                    .column_metadatas
                    .iter()
                    .map(|column| column.column_id),
            ),
            &json2_targets,
        )?;
        let schema = mapper.input_arrow_schema(true);
        let scan_input = ScanInput::builder(context.access_layer.clone(), mapper)
            .with_compaction(true)
            .with_page_index_policy(PageIndexPolicy::Skip)
            .with_batch_size(super::tuning().batch_rows)
            .with_cache(cache.clone())
            .with_json2_rewrite_targets(json2_targets.clone())
            .build();
        let mut limits = MemoryLimits::new(plan, &schema, upload)?;
        let mut inverted = context.config.inverted_index.clone();
        inverted.mem_threshold_on_create =
            admitted_index_threshold(inverted.mem_threshold_on_create(), limits.index);
        let mut fulltext = context.config.fulltext_index.clone();
        fulltext.mem_threshold_on_create =
            admitted_index_threshold(Some(fulltext.mem_threshold_on_create()), limits.index);
        let mut bloom = context.config.bloom_filter_index.clone();
        bloom.mem_threshold_on_create =
            admitted_index_threshold(bloom.mem_threshold_on_create(), limits.index);
        let registry = IndexRegistry::new(inverted, fulltext, bloom)
            .with_provider(context.index_kind_provider.clone())
            .with_context(|_| ExternalSnafu {
                context: "registering index handlers".to_string(),
            })?;
        let mut index_plans = plan
            .targets
            .iter()
            .map(|(id, target)| {
                let work = registry.plan(&PlanContext {
                    metadata: &target.metadata,
                    index_options: &target.options.index_options,
                    build_type: IndexBuildType::Compact,
                    existing_indexes: &[],
                });
                (*id, Arc::new(work))
            })
            .collect::<BTreeMap<_, _>>();
        limits.admit(
            plan,
            &context.config,
            &footers,
            index_plans
                .values()
                .filter(|work| work.entries().any(|entry| !entry.kind.is_builtin()))
                .count(),
        )?;
        if limits.extension_index_memory_limit.is_none() {
            for (region_id, work) in &mut index_plans {
                Arc::make_mut(work).omit_extensions_for_budget(*region_id);
            }
        }
        // Compute routing/version annotations on bounded reader batches, but
        // keep physical __sequence as the merge key. The annotations are not
        // persisted and fixed-position time/PK/sequence/op columns stay last.
        let fields = ["__compaction_region", "__compaction_sequence"]
            .into_iter()
            .map(|name| Arc::new(Field::new(name, DataType::UInt64, false)))
            .chain(schema.fields().iter().cloned())
            .collect::<Vec<_>>();
        let merge_schema = Arc::new(Schema::new(fields));
        Ok(Arc::new(Self {
            memory_admission: context.memory_admission.clone(),
            sources: plan.sources.clone(),
            legacy_overrides,
            expected_rows,
            targets: plan.targets.clone(),
            primary_key_codec: build_primary_key_codec(&metadata),
            metadata,
            scan_input,
            schema,
            merge_schema,
            partition_rule: plan.partition_rule.clone(),
            window: (
                Timestamp::new_second(plan.window_start),
                Timestamp::new_second(plan.window_start + plan.window_seconds),
            ),
            limits,
            index_plans,
            input_size,
            peak_group_bytes: AtomicU64::new(0),
            cancellation: context.cancellation.clone(),
            stop: CancellationToken::new(),
            counters,
        }))
    }

    pub(crate) fn cancel_on_drop(&self) -> tokio_util::sync::DropGuard {
        self.stop.clone().drop_guard()
    }

    pub(crate) fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    pub(crate) fn peak_group_bytes(&self) -> u64 {
        self.peak_group_bytes.load(Ordering::Relaxed)
    }

    /// One global merge closes leading (PK,time) groups. Only that group's
    /// versions need reordering after applying target-local admission sequences.
    /// This also supports field partitions and reversed installation orders.
    pub(crate) fn batches(
        self: Arc<Self>,
    ) -> std::pin::Pin<Box<dyn futures::Stream<Item = Result<SourceEvent>> + Send>> {
        use futures::TryStreamExt;
        Box::pin(async_stream::try_stream! {
            let parallelism = self.limits.source_parallelism;
            let permits = Arc::new(Semaphore::new(parallelism));
            let streams = (0..self.sources.len())
                .map(|index| {
                    let stream = self.clone().source_batches(index);
                    if parallelism > 1 {
                        prefetch_source(stream, permits.clone())
                    } else {
                        stream
                    }
                })
                .collect();
            let mut merged =
                FlatMergeReader::new(self.merge_schema.clone(), streams, self.limits.batch_rows, None).await?;
            let mut groups = KeyGroups::new(self.limits.group);
            while let Some(batch) = merged.next_batch().await? {
                self.check_cancelled()?;
                let complete = groups.push(batch)?;
                self.peak_group_bytes.fetch_max(groups.peak as u64, Ordering::Relaxed);
                if let Some(complete) = complete {
                    let mut fragments = self.clone().route_batches(complete);
                    while let Some(fragment) = fragments.try_next().await? { yield fragment; }
                }
            }
            if !groups.batches.is_empty() {
                let mut fragments = self.clone().route_batches(groups.batches);
                while let Some(fragment) = fragments.try_next().await? { yield fragment; }
            }
        })
    }

    fn route_batches(
        self: Arc<Self>,
        batches: Vec<RecordBatch>,
    ) -> std::pin::Pin<Box<dyn futures::Stream<Item = Result<SourceEvent>> + Send>> {
        Box::pin(async_stream::try_stream! {
            let router = self.clone();
            let mut routed = spawn_preparation(self.memory_admission.clone(), move || router.route(batches))
                .await
                .context(crate::error::JoinSnafu)??;
            loop {
                let router = self.clone();
                let (event, next) = spawn_preparation(self.memory_admission.clone(), move || {
                    let event = routed.next_fragment(&router)?;
                    Ok::<_, crate::error::Error>((event, routed))
                }).await.context(crate::error::JoinSnafu)??;
                routed = next;
                let Some(event) = event else { break; };
                yield event;
            }
        })
    }

    fn source_batches(self: Arc<Self>, index: usize) -> BoxedRecordBatchStream {
        Box::pin(async_stream::try_stream! {
            self.check_cancelled()?;
            let file = &self.sources[index];
            let mut raw_meta = file.clone();
            raw_meta.sequence = None;
            raw_meta.preserve_row_sequence = true;
            let handle = FileHandle::new(raw_meta, Arc::new(NoopFilePurger));
            let input = self.scan_input.prune_file(
                &handle, PreFilterMode::All, &mut ReaderMetrics::default(),
            ).await?;
            let mut ranges = smallvec::SmallVec::new();
            input.build_ranges(-1, &mut ranges);
            let fetch_metrics = ParquetFetchMetrics::default();
            let mut rows = 0u64;
            let mut last_key = None;
            let mut read_batches = 0usize;
            let mut reported_bytes = 0;
            for range in ranges {
                let Some(mut reader) = range.flat_reader(None, Some(&fetch_metrics)).await? else {
                    continue;
                };
                while let Some(batch) = {
                    let _stage = StageGuard::start("read_batch");
                    crate::read::scan_util::next_aligned_file_batch(
                        &range, &mut reader, None, false,
                    ).await?
                } {
                    self.check_cancelled()?;
                    read_batches += 1;
                    if read_batches.is_multiple_of(64) {
                        self.report_store_bytes(&fetch_metrics, &mut reported_bytes)?;
                    }
                    let preparing = self.clone();
                    let (batch, previous) = spawn_preparation(self.memory_admission.clone(), move || {
                        let _stage = StageGuard::start("prepare_batch");
                        preparing.check_cancelled()?;
                        let batch = compact_dictionaries(batch)?;
                        let batch_bytes = batch.get_array_memory_size();
                        ensure!(
                            batch_bytes <= preparing.limits.arrow_batch_bytes,
                            MultiRegionCompactionResourceSnafu {
                                reason: format!(
                                    "Decoded source batch uses {} bytes, budget is {}",
                                    batch_bytes,
                                    preparing.limits.arrow_batch_bytes,
                                ),
                            }
                        );
                        preparing.validate_batch(&batch, &mut last_key)?;
                        let batch = preparing.annotate(batch, index)?;
                        Ok::<_, crate::error::Error>((batch, last_key))
                    }).await.context(crate::error::JoinSnafu)??;
                    last_key = previous;
                    rows += batch.num_rows() as u64;
                    self.counters.rows.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                    metrics::progress("input_rows", batch.num_rows() as u64);
                    yield batch;
                }
            }
            self.report_store_bytes(&fetch_metrics, &mut reported_bytes)?;
            let expected = self.expected_rows[index];
            if expected < 0 || rows != expected as u64 {
                Err(InvalidMultiRegionCompactionSnafu {
                    reason: "Shared source scan did not cover its complete footer row count",
                }
                .build())?;
            }
            self.counters.files.fetch_add(1, Ordering::Relaxed);
            metrics::progress("input_files", 1);
        })
    }

    fn report_store_bytes(
        &self,
        fetch_metrics: &ParquetFetchMetrics,
        reported: &mut u64,
    ) -> Result<()> {
        let total = fetch_metrics
            .data
            .lock()
            .map_err(|_| {
                crate::error::UnexpectedSnafu {
                    reason: "Source fetch metrics lock poisoned",
                }
                .build()
            })?
            .page_size_to_fetch_store;
        let delta = total.saturating_sub(*reported);
        self.counters
            .store_page_bytes
            .fetch_add(delta, Ordering::Relaxed);
        metrics::progress("store_page_bytes", delta);
        *reported = total;
        Ok(())
    }

    fn validate_batch(
        &self,
        batch: &RecordBatch,
        previous: &mut Option<(Vec<u8>, i64, Reverse<u64>)>,
    ) -> Result<()> {
        let time = batch.column(time_index_column_index(batch.num_columns()));
        let sequences = batch
            .column(sequence_column_index(batch.num_columns()))
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context(InvalidMultiRegionCompactionSnafu {
                reason: "Invalid sequence column",
            })?;
        ensure!(
            time.null_count() == 0 && sequences.null_count() == 0,
            InvalidMultiRegionCompactionSnafu {
                reason: "Null timestamp or sequence in source"
            }
        );
        let unit = self
            .metadata
            .time_index_column()
            .column_schema
            .data_type
            .as_timestamp()
            .context(InvalidMultiRegionCompactionSnafu {
                reason: "Time index is not a timestamp",
            })?
            .unit();
        for (row, &value) in timestamp_array_to_i64_slice(time).iter().enumerate() {
            let timestamp = Timestamp::new(value, unit);
            ensure!(
                timestamp >= self.window.0 && timestamp < self.window.1,
                InvalidMultiRegionCompactionSnafu {
                    reason: "Source row is outside the task window"
                }
            );
            let current = (
                key_at(batch, row)?.to_vec(),
                value,
                Reverse(sequences.value(row)),
            );
            ensure!(
                previous
                    .as_ref()
                    .is_none_or(|previous| *previous <= current),
                InvalidMultiRegionCompactionSnafu {
                    reason: "Source is not ordered after schema alignment"
                }
            );
            *previous = Some(current);
        }
        Ok(())
    }

    fn annotate(&self, batch: RecordBatch, index: usize) -> Result<RecordBatch> {
        // Evaluate masks before adding private columns, avoiding name collisions
        // and N-target masks over a potentially much larger equal-key group.
        let owners = self.row_owners(&batch)?;
        let physical = batch
            .column(sequence_column_index(batch.num_columns()))
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context(InvalidMultiRegionCompactionSnafu {
                reason: "Invalid sequence column",
            })?;
        let source = &self.sources[index];
        let sequences = owners.iter().enumerate().map(|(row, region)| {
            let input = &self.targets[region].inputs[&source.file_id()];
            if input.region_id != *region
                || (!input.preserve_row_sequence && self.legacy_overrides[index])
            {
                input
                    .sequence
                    .map(|value| value.get())
                    .unwrap_or_else(|| physical.value(row))
            } else {
                physical.value(row)
            }
        });
        let columns = [
            Arc::new(UInt64Array::from_iter_values(
                owners.iter().map(RegionId::as_u64),
            )) as ArrayRef,
            Arc::new(UInt64Array::from_iter_values(sequences)) as ArrayRef,
        ]
        .into_iter()
        .chain(batch.columns().iter().cloned())
        .collect();
        RecordBatch::try_new(self.merge_schema.clone(), columns).context(NewRecordBatchSnafu)
    }

    fn route(&self, batches: Vec<RecordBatch>) -> Result<RoutedBatch> {
        self.check_cancelled()?;
        let batch = concat_batches(&self.merge_schema, &batches).context(ComputeArrowSnafu)?;
        let owners = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context(InvalidMultiRegionCompactionSnafu {
                reason: "Missing routing annotation",
            })?;
        let sequences = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context(InvalidMultiRegionCompactionSnafu {
                reason: "Missing sequence annotation",
            })?;
        let mut group = 0usize;
        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if row > 0 && !same_key(&batch, row - 1, &batch, row)? {
                group += 1;
            }
            rows.push((
                RegionId::from_u64(owners.value(row)),
                group,
                Reverse(sequences.value(row)),
                row,
            ));
        }
        rows.sort_unstable();
        let mut columns = batch.columns()[2..].to_vec();
        let sequence_index = sequence_column_index(columns.len());
        columns[sequence_index] = batch.column(1).clone();
        let batch =
            RecordBatch::try_new(self.schema.clone(), columns).context(NewRecordBatchSnafu)?;
        Ok(RoutedBatch {
            batch,
            rows,
            offset: 0,
        })
    }

    pub(super) fn row_owners(&self, batch: &RecordBatch) -> Result<Vec<RegionId>> {
        let routing = routing_batch(
            &self.metadata,
            self.primary_key_codec.as_ref(),
            self.partition_rule.partition_columns(),
            batch,
            self.limits.arrow_batch_bytes,
        )?;
        let batch = &routing;
        let rows = batch.num_rows();
        let masks = self
            .partition_rule
            .split_record_batch(batch)
            .context(crate::error::RouteMultiRegionSnafu)?;
        let mut owners = vec![None; rows];
        for (number, mask) in masks {
            ensure!(
                mask.array().len() == rows,
                InvalidMultiRegionCompactionSnafu {
                    reason: "Partition rule returned a mask with the wrong row count"
                }
            );
            let id = RegionId::new(self.metadata.region_id.table_id(), number);
            for (row, selected) in mask.array().iter().enumerate() {
                if selected == Some(true) {
                    ensure!(
                        owners[row].replace(id).is_none(),
                        InvalidMultiRegionCompactionSnafu {
                            reason: "Source row matches multiple target partitions"
                        }
                    );
                }
            }
        }
        self.resolve_unassigned(batch, &mut owners)?;
        owners
            .into_iter()
            .map(|owner| {
                owner.filter(|id| self.targets.contains_key(id)).context(
                    InvalidMultiRegionCompactionSnafu {
                        reason: "Partition rule selected a region outside the captured targets",
                    },
                )
            })
            .collect()
    }

    fn resolve_unassigned(
        &self,
        batch: &RecordBatch,
        owners: &mut [Option<RegionId>],
    ) -> Result<()> {
        if owners.iter().all(Option::is_some) {
            return Ok(());
        }
        let columns = self
            .partition_rule
            .partition_columns()
            .iter()
            .map(|name| {
                let array =
                    batch
                        .column_by_name(name)
                        .context(InvalidMultiRegionCompactionSnafu {
                            reason: "Partition column missing from the source batch",
                        })?;
                datatypes::vectors::Helper::try_into_vector(array.clone())
                    .context(crate::error::ConvertVectorSnafu)
            })
            .collect::<Result<Vec<_>>>()?;
        for (row, owner) in owners
            .iter_mut()
            .enumerate()
            .filter(|(_, owner)| owner.is_none())
        {
            let values = columns
                .iter()
                .map(|column| column.get(row))
                .collect::<Vec<_>>();
            let number = self
                .partition_rule
                .find_region(&values)
                .context(crate::error::RouteMultiRegionSnafu)?;
            *owner = Some(RegionId::new(self.metadata.region_id.table_id(), number));
        }
        Ok(())
    }

    fn check_cancelled(&self) -> Result<()> {
        ensure!(
            !self.stop.is_cancelled() && !self.cancellation.is_cancelled(),
            crate::error::CompactionCancelledSnafu
        );
        Ok(())
    }
}

/// Decodes only partition dependencies into a temporary batch. The merge
/// stream and writers retain the compact SST layout.
fn routing_batch(
    metadata: &RegionMetadata,
    codec: &dyn PrimaryKeyCodec,
    names: &[String],
    batch: &RecordBatch,
    memory_limit: usize,
) -> Result<RecordBatch> {
    let mut retained_bytes = batch.get_array_memory_size();
    ensure!(
        retained_bytes <= memory_limit,
        MultiRegionCompactionResourceSnafu {
            reason: "Partition routing batch exceeds the task buffer budget",
        }
    );
    if names.is_empty() {
        return Ok(batch.clone());
    }
    let mut decoded = None;
    let mut columns = Vec::with_capacity(names.len());
    let mut fields = Vec::with_capacity(names.len());
    for name in names {
        let array = if let Some(array) = batch.column_by_name(name) {
            array.clone()
        } else {
            let column =
                metadata
                    .column_by_name(name)
                    .context(InvalidMultiRegionCompactionSnafu {
                        reason: "Partition column missing from the source metadata",
                    })?;
            ensure!(
                metadata.primary_key_index(column.column_id).is_some(),
                InvalidMultiRegionCompactionSnafu {
                    reason: "Partition column missing from the source batch",
                }
            );
            let decoded = match &mut decoded {
                Some(decoded) => decoded,
                None => decoded.insert(decode_primary_keys(codec, batch)?),
            };
            let array = decoded.get_tag_column(
                column.column_id,
                metadata.primary_key_index(column.column_id),
                &column.column_schema.data_type,
            )?;
            // The compact source and decoded routing dependencies coexist. Stop
            // before decoding the next dependency if their buffers exceed the
            // same per-source allowance used by the expanded representation.
            retained_bytes = retained_bytes.saturating_add(array.get_array_memory_size());
            ensure!(
                retained_bytes <= memory_limit,
                MultiRegionCompactionResourceSnafu {
                    reason: "Partition routing batch exceeds the task buffer budget",
                }
            );
            array
        };
        fields.push(Field::new(name, array.data_type().clone(), true));
        columns.push(array);
    }
    let routing = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .context(NewRecordBatchSnafu)?;
    Ok(routing)
}

/// Carries only the trailing equal (PK,time) group. Complete groups can be
/// routed immediately; EOF closes the remaining group without another scan.
struct KeyGroups {
    batches: Vec<RecordBatch>,
    bytes: usize,
    peak: usize,
    limit: usize,
}

impl KeyGroups {
    fn new(limit: usize) -> Self {
        Self {
            batches: Vec::new(),
            bytes: 0,
            peak: 0,
            limit,
        }
    }

    fn push(&mut self, batch: RecordBatch) -> Result<Option<Vec<RecordBatch>>> {
        let tail = trailing_group_start(&batch)?;
        let same_tail = self
            .batches
            .last()
            .map(|previous| {
                same_key(
                    previous,
                    previous.num_rows() - 1,
                    &batch,
                    batch.num_rows() - 1,
                )
            })
            .transpose()?
            .unwrap_or(false);
        let complete = if (!same_tail && !self.batches.is_empty()) || tail > 0 {
            if tail > 0 {
                self.retain(batch.slice(0, tail))?;
            }
            self.bytes = 0;
            Some(std::mem::take(&mut self.batches))
        } else {
            None
        };
        self.retain(batch.slice(tail, batch.num_rows() - tail))?;
        Ok(complete)
    }

    fn retain(&mut self, batch: RecordBatch) -> Result<()> {
        self.bytes = self
            .bytes
            .saturating_add(batch.get_array_memory_size() + std::mem::size_of::<RecordBatch>());
        ensure!(
            self.bytes <= self.limit,
            MultiRegionCompactionResourceSnafu {
                reason: "Equal-key version group exceeds the task buffer budget"
            }
        );
        self.peak = self.peak.max(self.bytes);
        self.batches.push(batch);
        Ok(())
    }
}

/// Tokio cannot abort an already-running blocking closure. Retain its admission
/// independently of the awaiting future so cancellation cannot admit another job
/// against memory that this closure is still using.
fn spawn_preparation<F, T>(
    admission: Option<Arc<OwnedSemaphorePermit>>,
    prepare: F,
) -> tokio::task::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    common_runtime::spawn_blocking_compact(move || {
        let _admission = admission;
        prepare()
    })
}

/// Keep one prepared batch per source ahead of the merge. The shared permit
/// bounds active I/O/preparation, and is released before waiting on the channel
/// so a source that is not the current merge winner cannot block other sources.
fn prefetch_source(
    mut source: BoxedRecordBatchStream,
    permits: Arc<Semaphore>,
) -> BoxedRecordBatchStream {
    use futures::TryStreamExt;

    let (sender, mut receiver) = mpsc::channel(1);
    let task = AbortOnDropHandle::new(common_runtime::spawn_compact(async move {
        loop {
            let next = {
                let _permit = permits.acquire().await.map_err(|_| {
                    InvalidMultiRegionCompactionSnafu {
                        reason: "Source preparation semaphore closed",
                    }
                    .build()
                })?;
                source.try_next().await?
            };
            let Some(batch) = next else { break };
            let _stage = StageGuard::start("source_channel_wait");
            if sender.send(batch).await.is_err() {
                break;
            }
        }
        Ok::<_, crate::error::Error>(())
    }));
    Box::pin(async_stream::try_stream! {
        while let Some(batch) = receiver.recv().await {
            yield batch;
        }
        // A closed channel alone cannot distinguish EOF from a failed/panicked
        // producer. Keep the task owned by this stream, including on cancellation.
        task.await.context(crate::error::JoinSnafu)??;
    })
}

fn trailing_group_start(batch: &RecordBatch) -> Result<usize> {
    let last = batch.num_rows() - 1;
    let mut start = last;
    while start > 0 && same_key(batch, start - 1, batch, last)? {
        start -= 1;
    }
    Ok(start)
}

fn same_key(
    left: &RecordBatch,
    left_row: usize,
    right: &RecordBatch,
    right_row: usize,
) -> Result<bool> {
    let left_time =
        timestamp_array_to_i64_slice(left.column(time_index_column_index(left.num_columns())))
            [left_row];
    let right_time =
        timestamp_array_to_i64_slice(right.column(time_index_column_index(right.num_columns())))
            [right_row];
    Ok(left_time == right_time && key_at(left, left_row)? == key_at(right, right_row)?)
}

/// A selected batch must not pin dictionaries for a whole row group or version
/// group. Copy only referenced dictionary values without expanding them per row.
fn compact_dictionaries(batch: RecordBatch) -> Result<RecordBatch> {
    let _stage = StageGuard::start("dictionary_remap");
    let columns = batch
        .columns()
        .iter()
        .map(|array| match array.data_type() {
            DataType::Dictionary(key, values) if **key == DataType::UInt32 => match **values {
                DataType::Utf8 => compact_dictionary::<Utf8Type>(array),
                DataType::Binary => compact_dictionary::<BinaryType>(array),
                _ => Ok(array.clone()),
            },
            _ => Ok(array.clone()),
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(batch.schema(), columns).context(NewRecordBatchSnafu)
}

fn compact_dictionary<T: ByteArrayType>(array: &ArrayRef) -> Result<ArrayRef> {
    let dictionary = array
        .as_any()
        .downcast_ref::<DictionaryArray<UInt32Type>>()
        .context(InvalidMultiRegionCompactionSnafu {
            reason: "Invalid UInt32 dictionary",
        })?;
    let values = dictionary
        .values()
        .as_any()
        .downcast_ref::<GenericByteArray<T>>()
        .context(InvalidMultiRegionCompactionSnafu {
            reason: "Invalid byte dictionary values",
        })?;
    // Bound remapping work by the selected batch, not the original row group's
    // dictionary. Arrow's dictionary garbage collector sizes its map by that
    // entire dictionary, which can be much larger than a routed fragment.
    let mut selected: Vec<_> = dictionary.keys().iter().flatten().collect();
    selected.sort_unstable();
    selected.dedup();
    // Different dictionary keys can contain equal bytes. Intern each referenced
    // key once, preserving the old memory compactness without hashing every row
    // or first expanding all rows into a temporary byte array.
    let mut builder = GenericByteDictionaryBuilder::<UInt32Type, T>::with_capacity(
        selected.len(),
        selected.len(),
        0,
    );
    for &key in &selected {
        if values.is_null(key as usize) {
            builder.append_null();
        } else {
            builder
                .append(values.value(key as usize))
                .context(ComputeArrowSnafu)?;
        }
    }
    let referenced = builder.finish();
    let keys = UInt32Array::from_iter(dictionary.keys().iter().map(|key| {
        key.and_then(|key| {
            // Every non-null input key is in selected. The referenced dictionary
            // key is null when the old key addressed a null dictionary value.
            let index = selected.partition_point(|old| *old < key);
            (!referenced.keys().is_null(index)).then(|| referenced.keys().value(index))
        })
    }));
    Ok(Arc::new(
        DictionaryArray::<UInt32Type>::try_new(keys, referenced.values().clone())
            .context(ComputeArrowSnafu)?,
    ))
}

fn key_at(batch: &RecordBatch, row: usize) -> Result<&[u8]> {
    let keys = batch
        .column(primary_key_column_index(batch.num_columns()))
        .as_any()
        .downcast_ref::<PrimaryKeyArray>()
        .context(InvalidMultiRegionCompactionSnafu {
            reason: "Invalid primary-key column",
        })?;
    let values = keys
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context(InvalidMultiRegionCompactionSnafu {
            reason: "Invalid primary-key dictionary",
        })?;
    let index = keys.key(row).context(InvalidMultiRegionCompactionSnafu {
        reason: "Null internal primary key",
    })?;
    ensure!(
        !values.is_null(index),
        InvalidMultiRegionCompactionSnafu {
            reason: "Null internal primary-key value"
        }
    );
    Ok(values.value(index))
}

fn admitted_index_threshold(configured: Option<usize>, limit: usize) -> MemoryThreshold {
    MemoryThreshold::Size(ReadableSize(configured.unwrap_or(limit).min(limit) as u64))
}

#[cfg(test)]
mod tests {
    use std::hint::black_box;
    use std::time::Instant;

    use datatypes::arrow::array::StringArray;
    use datatypes::arrow::compute::cast;

    use super::*;

    // Keep the previous implementation as an independent semantic oracle and
    // benchmark baseline: decode rows, then intern their values again.
    fn rebuild_dictionary<T: ByteArrayType>(array: &ArrayRef) -> ArrayRef {
        let decoded = cast(array.as_ref(), &T::DATA_TYPE).unwrap();
        let values = decoded
            .as_any()
            .downcast_ref::<GenericByteArray<T>>()
            .unwrap();
        let mut builder = GenericByteDictionaryBuilder::<UInt32Type, T>::with_capacity(
            values.len(),
            values.len(),
            values.value_data().len(),
        );
        for value in values.iter() {
            if let Some(value) = value {
                builder.append(value).unwrap();
            } else {
                builder.append_null();
            }
        }
        Arc::new(builder.finish())
    }

    fn assert_dictionary_values(array: ArrayRef) -> ArrayRef {
        let DataType::Dictionary(_, value_type) = array.data_type() else {
            unreachable!();
        };
        let output = match value_type.as_ref() {
            DataType::Utf8 => compact_dictionary::<Utf8Type>(&array).unwrap(),
            DataType::Binary => compact_dictionary::<BinaryType>(&array).unwrap(),
            _ => unreachable!(),
        };
        assert_eq!(output.data_type(), array.data_type());
        assert_eq!(output.len(), array.len());
        assert_eq!(
            cast(array.as_ref(), value_type).unwrap().to_data(),
            cast(output.as_ref(), value_type).unwrap().to_data(),
        );
        output
    }

    #[test]
    fn test_compact_dictionary_preserves_values_and_nulls() {
        let strings = StringArray::from(vec![
            Some("unused"),
            Some("same"),
            None,
            Some("same"),
            Some(""),
        ]);
        let binary = BinaryArray::from(vec![
            Some(b"unused".as_slice()),
            Some(b"same".as_slice()),
            None,
            Some(b"same".as_slice()),
            Some(b"".as_slice()),
        ]);
        for values in [Arc::new(strings) as ArrayRef, Arc::new(binary) as ArrayRef] {
            for keys in [
                vec![Some(0), Some(3), None, Some(2), Some(1), Some(4), Some(3)],
                vec![None, None],
                vec![],
            ] {
                let array: ArrayRef = Arc::new(
                    DictionaryArray::<UInt32Type>::try_new(UInt32Array::from(keys), values.clone())
                        .unwrap(),
                );
                let output = assert_dictionary_values(array.clone());
                let baseline = if matches!(values.data_type(), DataType::Utf8) {
                    rebuild_dictionary::<Utf8Type>(&array)
                } else {
                    rebuild_dictionary::<BinaryType>(&array)
                };
                let actual = output
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt32Type>>()
                    .unwrap();
                let expected = baseline
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt32Type>>()
                    .unwrap();
                assert_eq!(actual.values().len(), expected.values().len());
                if array.len() > 2 {
                    assert_dictionary_values(array.slice(1, array.len() - 2));
                }
            }
        }
    }

    #[test]
    fn test_compact_dictionary_releases_unused_and_sliced_value_buffers() {
        let values = Arc::new(StringArray::from_iter_values(
            (0..4096).map(|i| format!("{i}:{}", "x".repeat(1024))),
        )) as ArrayRef;
        // Sparse selections must not allocate proportional to the old dictionary,
        // or retain its multi-megabyte string buffer.
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<UInt32Type>::try_new(
                UInt32Array::from(vec![Some(4095), None, Some(4095)]),
                values.clone(),
            )
            .unwrap(),
        );
        let output = assert_dictionary_values(dictionary);
        assert!(output.get_array_memory_size() < 8192);
        let compacted = output
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()
            .unwrap();
        assert_eq!(compacted.values().len(), 1);
        // Even when all values are referenced, a slice can retain a large buffer.
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<UInt32Type>::try_new(
                UInt32Array::from(vec![0, 1, 0]),
                values.slice(4094, 2),
            )
            .unwrap(),
        );
        let output = assert_dictionary_values(dictionary);
        assert!(output.get_array_memory_size() < 8192);
    }

    #[tokio::test]
    async fn test_cancelled_preparation_retains_memory_admission() {
        let permits = Arc::new(Semaphore::new(1));
        let admission = Arc::new(permits.clone().acquire_owned().await.unwrap());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let task = spawn_preparation(Some(admission.clone()), move || {
            let _ = started_tx.send(());
            release_rx.recv().unwrap();
        });
        started_rx.await.unwrap();
        task.abort();
        drop(task);
        drop(admission);
        // The detached blocking closure still owns its memory, even after both
        // the awaiting task and the original executor's permit have been dropped.
        assert!(permits.try_acquire().is_err());
        release_tx.send(()).unwrap();
        let permit = tokio::time::timeout(std::time::Duration::from_secs(10), permits.acquire())
            .await
            .unwrap()
            .unwrap();
        drop(permit);
        assert_eq!(permits.available_permits(), 1);
    }

    #[tokio::test]
    async fn test_prefetch_sources_parallel_bounded_and_ordered() {
        use futures::TryStreamExt;
        use std::sync::atomic::AtomicUsize;
        use tokio::sync::Barrier;

        let permits = Arc::new(Semaphore::new(2));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(2));
        let mut streams = Vec::new();
        for source_id in 0..4 {
            let active = active.clone();
            let peak = peak.clone();
            let barrier = barrier.clone();
            let source: BoxedRecordBatchStream = Box::pin(async_stream::try_stream! {
                let count = active.fetch_add(1, Ordering::Relaxed) + 1;
                peak.fetch_max(count, Ordering::Relaxed);
                // Both admitted sources must reach this point before either
                // proceeds. This proves actual overlap without timing sleeps.
                barrier.wait().await;
                active.fetch_sub(1, Ordering::Relaxed);
                for row in 0..3 {
                    yield RecordBatch::try_from_iter([
                        ("value", Arc::new(UInt64Array::from(vec![source_id * 10 + row])) as ArrayRef),
                    ]).unwrap();
                }
            });
            streams.push(prefetch_source(source, permits.clone()));
        }
        let reads = streams
            .into_iter()
            .map(|stream| stream.try_collect::<Vec<_>>());
        let outputs = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            futures::future::try_join_all(reads),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(peak.load(Ordering::Relaxed), 2);
        for (source_id, output) in outputs.into_iter().enumerate() {
            let values: Vec<_> = output
                .iter()
                .map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .value(0)
                })
                .collect();
            assert_eq!(
                values,
                vec![
                    source_id as u64 * 10,
                    source_id as u64 * 10 + 1,
                    source_id as u64 * 10 + 2
                ]
            );
        }
    }

    #[tokio::test]
    async fn test_prefetch_source_preserves_error_and_releases_on_drop() {
        use futures::TryStreamExt;
        use tokio::sync::oneshot;

        let source: BoxedRecordBatchStream = Box::pin(futures::stream::once(async {
            Err(InvalidMultiRegionCompactionSnafu {
                reason: "source test failure",
            }
            .build())
        }));
        let mut stream = prefetch_source(source, Arc::new(Semaphore::new(1)));
        assert!(
            stream
                .try_next()
                .await
                .unwrap_err()
                .to_string()
                .contains("source test failure")
        );

        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        struct NotifyOnDrop(Option<oneshot::Sender<()>>);
        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }
        let source: BoxedRecordBatchStream = Box::pin(async_stream::try_stream! {
            let _guard = NotifyOnDrop(Some(dropped_tx));
            let _ = started_tx.send(());
            futures::future::pending::<()>().await;
            yield RecordBatch::new_empty(Arc::new(Schema::empty()));
        });
        let permits = Arc::new(Semaphore::new(1));
        let stream = prefetch_source(source, permits.clone());
        started_rx.await.unwrap();
        drop(stream);
        tokio::time::timeout(std::time::Duration::from_secs(10), dropped_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(permits.available_permits(), 1);
    }

    #[test]
    #[ignore = "manual dictionary compaction performance comparison"]
    fn bench_compact_dictionary_remap() {
        common_telemetry::init_default_ut_logging();
        let values: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..4096).map(|i| format!("{i}:{}", "x".repeat(1024))),
        ));
        for rows in [64, 256, 1024] {
            for distinct in [1, 8, rows] {
                let input: ArrayRef = Arc::new(
                    DictionaryArray::<UInt32Type>::try_new(
                        UInt32Array::from_iter_values(
                            (0..rows).map(|row| (4095 - row % distinct) as u32),
                        ),
                        values.clone(),
                    )
                    .unwrap(),
                );
                let baseline = rebuild_dictionary::<Utf8Type>(&input);
                let actual = assert_dictionary_values(input.clone());
                assert_eq!(
                    cast(baseline.as_ref(), &DataType::Utf8).unwrap().to_data(),
                    cast(actual.as_ref(), &DataType::Utf8).unwrap().to_data()
                );
                for repetition in 0..5 {
                    // Alternate execution order to reduce warmup/order bias.
                    for remap in if repetition % 2 == 0 {
                        [false, true]
                    } else {
                        [true, false]
                    } {
                        let start = Instant::now();
                        for _ in 0..1000 {
                            let output = if remap {
                                compact_dictionary::<Utf8Type>(black_box(&input)).unwrap()
                            } else {
                                rebuild_dictionary::<Utf8Type>(black_box(&input))
                            };
                            black_box(output);
                        }
                        common_telemetry::info!(
                            rows,
                            distinct,
                            repetition,
                            remap,
                            elapsed_ns = start.elapsed().as_nanos() as u64,
                            retained_bytes = actual.get_array_memory_size(),
                            "Dictionary compaction benchmark"
                        );
                    }
                }
            }
        }
    }
    #[test]
    fn test_sparse_routing_accounts_decoded_dependencies() {
        use crate::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
        use crate::test_util::sst_util::{
            new_sparse_primary_key, sst_region_metadata_with_encoding,
        };
        use datatypes::arrow::array::{
            BinaryDictionaryBuilder, TimestampMillisecondArray, UInt8Array,
        };
        use store_api::codec::PrimaryKeyEncoding;

        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let codec = build_primary_key_codec(&metadata);
        let label = "x".repeat(4096);
        let mut keys = BinaryDictionaryBuilder::<UInt32Type>::new();
        keys.append(new_sparse_primary_key(&[&label, "series"], &metadata, 1, 1))
            .unwrap();
        let batch = RecordBatch::try_new(
            to_flat_sst_arrow_schema(
                &metadata,
                &FlatSchemaOptions::from_encoding(PrimaryKeyEncoding::Sparse),
            ),
            vec![
                Arc::new(UInt64Array::from(vec![1])),
                Arc::new(TimestampMillisecondArray::from(vec![0])),
                Arc::new(keys.finish()),
                Arc::new(UInt64Array::from(vec![1])),
                Arc::new(UInt8Array::from(vec![0])),
            ],
        )
        .unwrap();
        let names = ["tag_0".into(), "field_0".into()];
        let routing = routing_batch(&metadata, codec.as_ref(), &names, &batch, usize::MAX).unwrap();
        assert_eq!(2, routing.num_columns());
        let retained = batch.get_array_memory_size() + routing.column(0).get_array_memory_size();
        routing_batch(&metadata, codec.as_ref(), &names, &batch, retained).unwrap();
        let error =
            routing_batch(&metadata, codec.as_ref(), &names, &batch, retained - 1).unwrap_err();
        assert!(matches!(
            error,
            crate::error::Error::MultiRegionCompactionResource { .. }
        ));
        assert!(Arc::ptr_eq(
            batch.column_by_name("field_0").unwrap(),
            routing.column(1)
        ));
    }

    /// Isolates tag materialization and dictionary work from object-store I/O.
    #[test]
    #[ignore = "manual wide sparse routing ablation benchmark"]
    fn bench_sparse_routing_materialization() {
        use crate::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
        use crate::test_util::sst_util::{
            new_sparse_primary_key, sst_region_metadata_with_encoding,
        };
        use api::v1::SemanticType;
        use datatypes::arrow::array::{
            BinaryDictionaryBuilder, TimestampMillisecondArray, UInt8Array,
        };
        use datatypes::data_type::ConcreteDataType;
        use datatypes::schema::ColumnSchema;
        use datatypes::value::Value;
        use partition::expr::col;
        use partition::multi_dim::MultiDimPartitionRule;
        use store_api::codec::PrimaryKeyEncoding;
        use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};

        common_telemetry::init_default_ut_logging();
        let metadata = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
        let mut builder = RegionMetadataBuilder::from_existing(metadata.clone());
        let mut primary_key = metadata.primary_key.clone();
        for id in 10..3610 {
            builder.push_column_metadata(ColumnMetadata {
                column_id: id,
                semantic_type: SemanticType::Tag,
                column_schema: ColumnSchema::new(
                    format!("unused_tag_{id}"),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
            });
            primary_key.push(id);
        }
        builder.primary_key(primary_key);
        let metadata = Arc::new(builder.build().unwrap());
        let codec = build_primary_key_codec(&metadata);
        let all_tags: Vec<_> = metadata
            .primary_key_columns()
            .map(|column| (column.column_id, column.column_schema.data_type.clone()))
            .collect();
        let rule = MultiDimPartitionRule::try_new(
            vec!["tag_0".into()],
            vec![0, 1],
            vec![
                col("tag_0").lt(Value::from("032")),
                col("tag_0").gt_eq(Value::from("032")),
            ],
            false,
        )
        .unwrap();
        for distinct in [1, 8, 64] {
            let mut keys = BinaryDictionaryBuilder::<UInt32Type>::new();
            for row in 0..64 {
                keys.append(new_sparse_primary_key(
                    &[&format!("{:03}", row % distinct), "series"],
                    &metadata,
                    1,
                    (row % distinct) as u64,
                ))
                .unwrap();
            }
            let batch = RecordBatch::try_new(
                to_flat_sst_arrow_schema(
                    &metadata,
                    &FlatSchemaOptions::from_encoding(PrimaryKeyEncoding::Sparse),
                ),
                vec![
                    Arc::new(UInt64Array::from(vec![1; 64])),
                    Arc::new(TimestampMillisecondArray::from(vec![0; 64])),
                    Arc::new(keys.finish()),
                    Arc::new(UInt64Array::from(vec![1; 64])),
                    Arc::new(UInt8Array::from(vec![0; 64])),
                ],
            )
            .unwrap();
            let materialize = |full: bool| {
                if !full {
                    let routing = routing_batch(
                        &metadata,
                        codec.as_ref(),
                        &["tag_0".into()],
                        &batch,
                        usize::MAX,
                    )
                    .unwrap();
                    return (
                        rule.split_record_batch(&routing).unwrap(),
                        routing.get_array_memory_size(),
                    );
                }
                let mut decoded = decode_primary_keys(codec.as_ref(), &batch).unwrap();
                let arrays = decoded.get_sparse_tag_columns(&all_tags).unwrap();
                let fields: Vec<_> = arrays
                    .iter()
                    .enumerate()
                    .map(|(i, array)| {
                        let column = metadata.column_by_id(all_tags[i].0).unwrap();
                        Field::new(&column.column_schema.name, array.data_type().clone(), true)
                    })
                    .collect();
                let routing = compact_dictionaries(
                    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap(),
                )
                .unwrap();
                let masks = rule.split_record_batch(&routing).unwrap();
                (masks, routing.get_array_memory_size())
            };
            let (baseline, full_bytes) = materialize(true);
            let (projected, projected_bytes) = materialize(false);
            assert_eq!(baseline.len(), projected.len());
            for (region, mask) in baseline {
                assert_eq!(mask.array(), projected[&region].array());
            }
            for repetition in 0..5 {
                for full in if repetition % 2 == 0 {
                    [true, false]
                } else {
                    [false, true]
                } {
                    let start = Instant::now();
                    for _ in 0..20 {
                        black_box(materialize(black_box(full)));
                    }
                    common_telemetry::info!(
                        distinct,
                        repetition,
                        full,
                        elapsed_ns = start.elapsed().as_nanos() as u64,
                        full_bytes,
                        projected_bytes,
                        "Sparse routing materialization benchmark"
                    );
                }
            }
        }
    }
}
