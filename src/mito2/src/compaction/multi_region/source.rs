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

use arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY;
use common_base::readable_size::ReadableSize;
use common_time::Timestamp;
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, GenericByteArray, GenericByteDictionaryBuilder, UInt64Array,
};
use datatypes::arrow::compute::{cast, concat_batches, take_record_batch};
use datatypes::arrow::datatypes::{
    BinaryType, ByteArrayType, DataType, Field, Schema, SchemaRef, UInt32Type, Utf8Type,
};
use datatypes::arrow::record_batch::RecordBatch;
use parquet::file::metadata::PageIndexPolicy;
use partition::PartitionRuleRef;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;
use tokio_util::sync::CancellationToken;

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheStrategy;
use crate::compaction::json2::collect_json2_rewrite_plans_from_parquet;
use crate::compaction::multi_region::memory::{BATCH_SIZE, MemoryLimits};
use crate::compaction::multi_region::{
    MultiRegionCompactionContext, MultiRegionCompactionPlan, MultiRegionTarget, SourceCounters,
};
use crate::config::MemoryThreshold;
use crate::error::{
    ComputeArrowSnafu, ExternalSnafu, InvalidMultiRegionCompactionSnafu,
    MultiRegionCompactionResourceSnafu, NewRecordBatchSnafu, Result,
};
use crate::read::compat::FlatCompatBatch;
use crate::read::flat_merge::FlatMergeReader;
use crate::read::flat_projection::FlatProjectionMapper;
use crate::read::read_columns::ReadColumns;
use crate::read::{BoxedRecordBatchStream, timestamp_array_to_i64_slice};
use crate::sst::file::{FileHandle, FileMeta};
use crate::sst::file_purger::NoopFilePurger;
use crate::sst::index::IndexBuildType;
use crate::sst::index::handler::PlanContext;
use crate::sst::index::registry::{IndexBuildPlan, IndexRegistry};
use crate::sst::parquet::flat_format::{
    primary_key_column_index, sequence_column_index, time_index_column_index,
};
use crate::sst::parquet::format::{PrimaryKeyArray, need_override_sequence};
use crate::sst::parquet::reader::{MetadataCacheMetrics, ParquetReader, ReaderMetrics};
use crate::sst::parquet::{Json2RewriteTargets, Json2TargetLayout};

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
            .min(BATCH_SIZE);
        let indices = UInt64Array::from_iter_values(rows[..len].iter().map(|row| row.3 as u64));
        let batch = take_record_batch(&self.batch, &indices).context(ComputeArrowSnafu)?;
        let batch = compact_dictionaries(batch)?;
        ensure!(
            batch.get_array_memory_size() <= source.limits.batch,
            MultiRegionCompactionResourceSnafu {
                reason: "Routed batch exceeds the task buffer budget"
            }
        );
        self.offset += len;
        Ok(Some(SourceEvent { region_id, batch }))
    }
}

pub(crate) struct SharedSource {
    layer: AccessLayerRef,
    cache: CacheStrategy,
    sources: Vec<FileMeta>,
    legacy_overrides: Vec<bool>,
    expected_rows: Vec<i64>,
    targets: BTreeMap<RegionId, MultiRegionTarget>,
    metadata: RegionMetadataRef,
    mapper: FlatProjectionMapper,
    json2_targets: Json2RewriteTargets,
    schema: SchemaRef,
    merge_schema: SchemaRef,
    partition_rule: PartitionRuleRef,
    window: (Timestamp, Timestamp),
    pub(crate) limits: MemoryLimits,
    pub(crate) index_plans: BTreeMap<RegionId, Arc<IndexBuildPlan>>,
    peak_group_bytes: AtomicU64,
    cancellation: Arc<common_base::cancellation::CancellationHandle>,
    stop: CancellationToken,
    counters: Arc<SourceCounters>,
}

impl SharedSource {
    pub(crate) async fn build(
        plan: &MultiRegionCompactionPlan,
        context: &MultiRegionCompactionContext,
        metadata: RegionMetadataRef,
        counters: Arc<SourceCounters>,
    ) -> Result<Arc<Self>> {
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
                footer_bytes <= plan.memory_limit / 8,
                MultiRegionCompactionResourceSnafu {
                    reason: "Source footer metadata exceeds the task budget",
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
        let layouts = collect_json2_rewrite_plans_from_parquet(&metadata, &footers)?;
        let mut json2 = BTreeMap::new();
        for (name, layout) in layouts {
            let column =
                metadata
                    .column_by_name(&name)
                    .context(InvalidMultiRegionCompactionSnafu {
                        reason: "JSON2 layout names an unknown column",
                    })?;
            let extension_metadata = column
                .column_schema
                .metadata()
                .get(EXTENSION_TYPE_METADATA_KEY)
                .cloned()
                .context(InvalidMultiRegionCompactionSnafu {
                    reason: "JSON2 extension metadata missing",
                })?;
            json2.insert(
                column.column_id,
                Json2TargetLayout {
                    extension_metadata,
                    target_layout: layout.target_layout,
                },
            );
        }
        let json2_targets = Arc::new(json2);
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
        let schema = mapper.input_arrow_schema(false);
        let mut limits = MemoryLimits::new(plan, &schema)?;
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
            context,
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
            layer: context.access_layer.clone(),
            cache,
            sources: plan.sources.clone(),
            legacy_overrides,
            expected_rows,
            targets: plan.targets.clone(),
            metadata,
            mapper,
            json2_targets,
            schema,
            merge_schema,
            partition_rule: plan.partition_rule.clone(),
            window: (
                Timestamp::new_second(plan.window_start),
                Timestamp::new_second(plan.window_start + plan.window_seconds),
            ),
            limits,
            index_plans,
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
            let streams = (0..self.sources.len())
                .map(|index| self.clone().source_batches(index))
                .collect();
            let mut merged =
                FlatMergeReader::new(self.merge_schema.clone(), streams, BATCH_SIZE, None).await?;
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
            let mut routed = common_runtime::spawn_blocking_compact(move || router.route(batches))
                .await
                .context(crate::error::JoinSnafu)??;
            loop {
                let router = self.clone();
                let (event, next) = common_runtime::spawn_blocking_compact(move || {
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
            let builder = self
                .layer
                .read_sst(FileHandle::new(raw_meta, Arc::new(NoopFilePurger)))
                .cache(self.cache.clone())
                .batch_size(BATCH_SIZE)
                .expected_metadata(Some(self.metadata.clone()))
                .json2_rewrite_targets(self.json2_targets.clone())
                .compaction(false)
                .page_index_policy(PageIndexPolicy::Skip);
            let mut rows = 0u64;
            let input = builder
                .build_reader_input(&mut ReaderMetrics::default())
                .await?;
            if let Some((input, selection)) = input {
                let compat = FlatCompatBatch::try_new(&self.mapper, input.read_format(), false)?
                    .map(Arc::new);
                let mut reader = ParquetReader::new(Arc::new(input), selection).await?;
                let mut last_key = None;
                while let Some(batch) = reader.next_record_batch().await? {
                    self.check_cancelled()?;
                    let preparing = self.clone();
                    let compat = compat.clone();
                    let (batch, previous) = common_runtime::spawn_blocking_compact(move || {
                        preparing.check_cancelled()?;
                        let batch = match compat {
                            Some(compat) => compat.compat(batch)?,
                            None => batch,
                        };
                        let batch = compact_dictionaries(batch)?;
                        let batch_bytes = batch.get_array_memory_size();
                        ensure!(
                            batch_bytes <= preparing.limits.batch,
                            MultiRegionCompactionResourceSnafu {
                                reason: format!(
                                    "Decoded source batch uses {} bytes, budget is {}",
                                    batch_bytes,
                                    preparing.limits.batch,
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
                    yield batch;
                }
                self.counters
                    .store_page_bytes
                    .fetch_add(reader.fetched_store_page_bytes()?, Ordering::Relaxed);
            }
            let expected = self.expected_rows[index];
            if expected < 0 || rows != expected as u64 {
                Err(InvalidMultiRegionCompactionSnafu {
                    reason: "Shared source scan did not cover its complete footer row count",
                }
                .build())?;
            }
            self.counters.files.fetch_add(1, Ordering::Relaxed);
        })
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

    fn row_owners(&self, batch: &RecordBatch) -> Result<Vec<RegionId>> {
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
/// group. Casting through the value type compacts both tag and PK dictionaries.
fn compact_dictionaries(batch: RecordBatch) -> Result<RecordBatch> {
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
    let decoded = cast(array, &T::DATA_TYPE).context(ComputeArrowSnafu)?;
    let values = decoded
        .as_any()
        .downcast_ref::<GenericByteArray<T>>()
        .context(InvalidMultiRegionCompactionSnafu {
            reason: "Invalid decoded dictionary",
        })?;
    let mut builder = GenericByteDictionaryBuilder::<UInt32Type, T>::with_capacity(
        values.len(),
        values.len(),
        values.value_data().len(),
    );
    for value in values.iter() {
        if let Some(value) = value {
            builder.append(value).context(ComputeArrowSnafu)?;
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
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
