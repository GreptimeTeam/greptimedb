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

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY;
use common_time::{Timestamp, timestamp::TimeUnit};
use datatypes::arrow::array::{Array, UInt64Array};
use datatypes::arrow::compute::take_record_batch;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use futures::Stream;
use parquet::file::metadata::PageIndexPolicy;
use partition::PartitionRuleRef;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheStrategy;
use crate::compaction::json2::collect_json2_rewrite_plans_from_parquet;
use crate::compaction::multi_region::{
    MultiRegionCompactionContext, MultiRegionCompactionPlan, MultiRegionTarget, SourceCounters,
};
use crate::error::{
    ComputeArrowSnafu, InvalidMultiRegionCompactionSnafu, MultiRegionCompactionResourceSnafu,
    NewRecordBatchSnafu, Result,
};
use crate::read::compat::FlatCompatBatch;
use crate::read::flat_projection::FlatProjectionMapper;
use crate::read::read_columns::ReadColumns;
use crate::read::timestamp_array_to_i64_slice;
use crate::sst::file::{FileHandle, FileMeta};
use crate::sst::file_purger::NoopFilePurger;
use crate::sst::parquet::flat_format::{sequence_column_index, time_index_column_index};
use crate::sst::parquet::format::need_override_sequence;
use crate::sst::parquet::reader::{MetadataCacheMetrics, ParquetReader, ReaderMetrics};
use crate::sst::parquet::{Json2RewriteTargets, Json2TargetLayout};

struct SourceBatch {
    batch: RecordBatch,
    source: FileMeta,
    legacy_override: bool,
}

/// Retains only the input batch and row destinations, never future output arrays.
/// Row indices break destination ties so each fragment preserves source order.
struct RoutedBatch {
    input: SourceBatch,
    rows: Vec<(RegionId, i64, usize)>,
    offset: usize,
}

impl RoutedBatch {
    /// Materializes one fragment on the blocking pool, only when dispatch pulls it.
    fn next_fragment(&mut self, router: &SharedSource) -> Result<Option<SourceEvent>> {
        router.check_cancelled()?;
        let Some(&(region_id, window, _)) = self.rows.get(self.offset) else {
            return Ok(None);
        };
        let remaining = &self.rows[self.offset..];
        let len = remaining
            .partition_point(|(id, time_window, _)| (*id, *time_window) == (region_id, window));
        let indices =
            UInt64Array::from_iter_values(remaining[..len].iter().map(|(_, _, row)| *row as u64));
        let batch = router.target_batch(&self.input, region_id, &indices)?;
        self.offset += len;
        Ok(Some(SourceEvent::Batch {
            region_id,
            window,
            batch,
        }))
    }
}

/// Source boundaries are ordered after all routed fragments of that source.
pub(crate) enum SourceEvent {
    Batch {
        region_id: RegionId,
        window: i64,
        batch: RecordBatch,
    },
    SourceEnd,
}

pub(crate) struct SharedSource {
    layer: AccessLayerRef,
    cache: CacheStrategy,
    sources: Vec<FileMeta>,
    targets: BTreeMap<RegionId, MultiRegionTarget>,
    metadata: RegionMetadataRef,
    mapper: FlatProjectionMapper,
    json2_targets: Json2RewriteTargets,
    schema: SchemaRef,
    partition_rule: PartitionRuleRef,
    window_seconds: i64,
    batch_memory_limit: usize,
    cancellation: Arc<common_base::cancellation::CancellationHandle>,
    counters: Arc<SourceCounters>,
}

impl fmt::Debug for SharedSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedTableSource")
            .field("source_files", &self.sources.len())
            .finish()
    }
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
        for file in &plan.sources {
            if context.cancellation.is_cancelled() {
                return crate::error::CompactionCancelledSnafu.fail();
            }
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
            footers.push(sst.parquet_metadata());
        }
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
        Ok(Arc::new(Self {
            layer: context.access_layer.clone(),
            cache,
            sources: plan.sources.clone(),
            targets: plan.targets.clone(),
            metadata,
            mapper,
            json2_targets,
            schema,
            partition_rule: plan.partition_rule.clone(),
            window_seconds: plan.window_seconds,
            batch_memory_limit: plan.memory_limit / 8,
            cancellation: context.cancellation.clone(),
            counters,
        }))
    }

    pub(crate) fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub(crate) fn batches(
        self: Arc<Self>,
    ) -> std::pin::Pin<Box<dyn Stream<Item = Result<SourceEvent>> + Send>> {
        Box::pin(async_stream::try_stream! {
            for file in &self.sources {
                self.check_cancelled()?;
                // Preserve physical source sequences. Target-domain overrides are applied only after routing.
                let mut raw_meta = file.clone(); raw_meta.sequence = None; raw_meta.preserve_row_sequence = true;
                let handle = FileHandle::new(raw_meta, Arc::new(NoopFilePurger));
                let builder = self.layer.read_sst(handle).cache(self.cache.clone()).batch_size(1024)
                    .expected_metadata(Some(self.metadata.clone())).json2_rewrite_targets(self.json2_targets.clone())
                    .compaction(false).page_index_policy(PageIndexPolicy::Skip);
                let Some((context, selection)) = builder.build_reader_input(&mut ReaderMetrics::default()).await? else {
                    self.counters.files.fetch_add(1, Ordering::Relaxed);
                    yield SourceEvent::SourceEnd;
                    continue;
                };
                let compat = FlatCompatBatch::try_new(&self.mapper, context.read_format(), false)?;
                let mut reader = ParquetReader::new(Arc::new(context), selection).await?;
                let expected_rows = u64::try_from(reader.parquet_metadata().file_metadata().num_rows())
                    .map_err(|_| InvalidMultiRegionCompactionSnafu { reason: "Negative source footer row count" }.build())?;
                let mut read_rows = 0_u64;
                let legacy_override = need_override_sequence(&reader.parquet_metadata());
                while let Some(batch) = reader.next_record_batch().await? {
                    self.check_cancelled()?;
                    let batch = if let Some(compat) = &compat { compat.compat(batch)? } else { batch };
                    if batch.get_array_memory_size() > self.batch_memory_limit {
                        Err(MultiRegionCompactionResourceSnafu { reason: "Decoded source batch exceeds the shared reader budget" }.build())?;
                    }
                    self.counters.rows.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                    read_rows += batch.num_rows() as u64;
                    let router = self.clone();
                    let source = file.clone();
                    let mut routed = common_runtime::spawn_blocking_compact(move || {
                        router.route(SourceBatch { batch, source, legacy_override })
                    }).await.context(crate::error::JoinSnafu)??;
                    loop {
                        let router = self.clone();
                        let (event, remaining) = common_runtime::spawn_blocking_compact(move || {
                            let event = routed.next_fragment(&router)?;
                            Ok::<_, crate::error::Error>((event, routed))
                        }).await.context(crate::error::JoinSnafu)??;
                        routed = remaining;
                        let Some(event) = event else { break; };
                        // Dispatch reserves/spills and sends this fragment before
                        // polling again. No work slot is held across the yield.
                        yield event;
                    }
                }
                if read_rows != expected_rows {
                    Err(InvalidMultiRegionCompactionSnafu { reason: "Shared source scan did not cover its complete footer row count" }.build())?;
                }
                let page_bytes = reader.fetched_store_page_bytes()?;
                self.counters.store_page_bytes.fetch_add(page_bytes, Ordering::Relaxed);
                self.counters.files.fetch_add(1, Ordering::Relaxed);
                yield SourceEvent::SourceEnd;
            }
        })
    }

    fn route(&self, input: SourceBatch) -> Result<RoutedBatch> {
        self.check_cancelled()?;
        let owners = self.row_owners(&input.batch)?;
        let windows = self.time_windows(&input.batch)?;
        let mut rows: Vec<_> = owners
            .into_iter()
            .zip(windows)
            .enumerate()
            .map(|(row, (region_id, window))| (region_id, window, row))
            .collect();
        rows.sort_unstable();
        Ok(RoutedBatch {
            input,
            rows,
            offset: 0,
        })
    }

    fn target_batch(
        &self,
        source: &SourceBatch,
        region_id: RegionId,
        indices: &UInt64Array,
    ) -> Result<RecordBatch> {
        let target = self
            .targets
            .get(&region_id)
            .context(InvalidMultiRegionCompactionSnafu {
                reason: "Routed fragment names an unknown region",
            })?;
        let input = target.inputs.get(&source.source.file_id()).context(
            InvalidMultiRegionCompactionSnafu {
                reason: "Target has no effective metadata for the selected source",
            },
        )?;
        let filtered = take_record_batch(&source.batch, indices).context(ComputeArrowSnafu)?;
        let override_sequence = if input.region_id != region_id
            || (!input.preserve_row_sequence && source.legacy_override)
        {
            input.sequence
        } else {
            None
        };
        let mut columns = filtered.columns().to_vec();
        if let Some(sequence) = override_sequence {
            columns[sequence_column_index(filtered.num_columns())] =
                Arc::new(UInt64Array::from_value(sequence.get(), filtered.num_rows()));
        }
        RecordBatch::try_new(self.schema.clone(), columns).context(NewRecordBatchSnafu)
    }

    fn time_windows(&self, filtered: &RecordBatch) -> Result<Vec<i64>> {
        let time_index = filtered.column(time_index_column_index(filtered.num_columns()));
        ensure!(
            time_index.null_count() == 0,
            InvalidMultiRegionCompactionSnafu {
                reason: "Null timestamp in shared source"
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
        timestamp_array_to_i64_slice(time_index)
            .iter()
            .map(|value| {
                Timestamp::new(*value, unit)
                    .convert_to(TimeUnit::Second)
                    .map(|value| value.value().div_euclid(self.window_seconds))
                    .context(InvalidMultiRegionCompactionSnafu {
                        reason: "Timestamp window overflow",
                    })
            })
            .collect::<Result<Vec<_>>>()
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
        // Nullable vector predicates can leave unknown masks. Resolve only those
        // rows through the same rule's scalar API, including its default region.
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
        if self.cancellation.is_cancelled() {
            return crate::error::CompactionCancelledSnafu.fail();
        }
        Ok(())
    }
}
