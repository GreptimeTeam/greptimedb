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

//! Memtable implementation for bulk load

#[allow(unused)]
pub mod context;
#[allow(unused)]
pub mod part;
pub mod part_reader;
mod row_group_reader;

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::Instant;

/// Reads an environment variable as usize, returning default if not set or invalid.
fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

use datatypes::arrow::datatypes::SchemaRef;
use mito_codec::key_values::KeyValue;
use rayon::prelude::*;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, FileId, RegionId, SequenceRange};
use tokio::sync::Semaphore;

use crate::error::{Result, UnsupportedOperationSnafu};
use crate::flush::WriteBufferManagerRef;
use crate::memtable::bulk::context::BulkIterContext;
use crate::memtable::bulk::part::{
    BulkPart, BulkPartEncodeMetrics, BulkPartEncoder, MultiBulkPart, UnorderedPart,
};
use crate::memtable::bulk::part_reader::BulkPartBatchIter;
use crate::memtable::stats::WriteMetrics;
use crate::memtable::{
    AllocTracker, BoxedBatchIterator, BoxedRecordBatchIterator, EncodedBulkPart, EncodedRange,
    IterBuilder, KeyValues, MemScanMetrics, Memtable, MemtableBuilder, MemtableId, MemtableRange,
    MemtableRangeContext, MemtableRanges, MemtableRef, MemtableStats, RangesOptions,
};
use crate::read::flat_dedup::{FlatDedupIterator, FlatLastNonNull, FlatLastRow};
use crate::read::flat_merge::FlatMergeIterator;
use crate::region::options::MergeMode;
use crate::sst::parquet::format::FIXED_POS_COLUMN_NUM;
use crate::sst::parquet::{DEFAULT_READ_BATCH_SIZE, DEFAULT_ROW_GROUP_SIZE};
use crate::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};

/// Default merge threshold for triggering compaction.
const DEFAULT_MERGE_THRESHOLD: usize = 16;

/// Threshold for triggering merge of parts. Configurable via `GREPTIME_BULK_MERGE_THRESHOLD`.
static MERGE_THRESHOLD: LazyLock<usize> =
    LazyLock::new(|| env_usize("GREPTIME_BULK_MERGE_THRESHOLD", DEFAULT_MERGE_THRESHOLD));

/// Default maximum number of groups for parallel merging.
const DEFAULT_MAX_MERGE_GROUPS: usize = 16;

/// Maximum merge groups. Configurable via `GREPTIME_BULK_MAX_MERGE_GROUPS`.
static MAX_MERGE_GROUPS: LazyLock<usize> =
    LazyLock::new(|| env_usize("GREPTIME_BULK_MAX_MERGE_GROUPS", DEFAULT_MAX_MERGE_GROUPS));

/// Row threshold for encoding parts. Configurable via `GREPTIME_BULK_ENCODE_ROW_THRESHOLD`.
/// When estimated rows exceed this threshold, parts are encoded as EncodedBulkPart.
pub(crate) static ENCODE_ROW_THRESHOLD: LazyLock<usize> = LazyLock::new(|| {
    env_usize(
        "GREPTIME_BULK_ENCODE_ROW_THRESHOLD",
        8 * DEFAULT_ROW_GROUP_SIZE,
    )
});

/// Default bytes threshold for encoding.
const DEFAULT_ENCODE_BYTES_THRESHOLD: usize = 64 * 1024 * 1024;

/// Bytes threshold for encoding parts. Configurable via `GREPTIME_BULK_ENCODE_BYTES_THRESHOLD`.
/// When estimated bytes exceed this threshold, parts are encoded as EncodedBulkPart.
static ENCODE_BYTES_THRESHOLD: LazyLock<usize> = LazyLock::new(|| {
    env_usize(
        "GREPTIME_BULK_ENCODE_BYTES_THRESHOLD",
        DEFAULT_ENCODE_BYTES_THRESHOLD,
    )
});

/// Configuration for bulk memtable.
#[derive(Debug, Clone)]
pub struct BulkMemtableConfig {
    /// Threshold for triggering merge of parts.
    pub merge_threshold: usize,
    /// Row threshold for encoding parts.
    pub encode_row_threshold: usize,
    /// Bytes threshold for encoding parts.
    pub encode_bytes_threshold: usize,
    /// Maximum number of groups for parallel merging.
    pub max_merge_groups: usize,
}

impl Default for BulkMemtableConfig {
    fn default() -> Self {
        Self {
            merge_threshold: *MERGE_THRESHOLD,
            encode_row_threshold: *ENCODE_ROW_THRESHOLD,
            encode_bytes_threshold: *ENCODE_BYTES_THRESHOLD,
            max_merge_groups: *MAX_MERGE_GROUPS,
        }
    }
}

/// Result of merging parts - either a MultiBulkPart or an EncodedBulkPart
enum MergedPart {
    /// Merged part stored as MultiBulkPart (when rows < DEFAULT_ROW_GROUP_SIZE)
    Multi(MultiBulkPart),
    /// Merged part stored as EncodedBulkPart (when rows >= DEFAULT_ROW_GROUP_SIZE)
    Encoded(EncodedBulkPart),
}

/// Result of collecting parts to merge
struct CollectedParts {
    /// Groups of parts ready for merging (each group has up to 16 parts)
    groups: Vec<Vec<PartToMerge>>,
}

/// All parts in a bulk memtable.
#[derive(Default)]
struct BulkParts {
    /// Unordered small parts (< 1024 rows).
    unordered_part: UnorderedPart,
    /// All parts (raw and encoded).
    parts: Vec<BulkPartWrapper>,
}

impl BulkParts {
    /// Total number of parts (including unordered).
    fn num_parts(&self) -> usize {
        let unordered_count = if self.unordered_part.is_empty() { 0 } else { 1 };
        self.parts.len() + unordered_count
    }

    /// Returns true if there is no part.
    fn is_empty(&self) -> bool {
        self.unordered_part.is_empty() && self.parts.is_empty()
    }

    /// Returns true if bulk parts or encoded parts should be merged.
    /// Uses short-circuit counting to stop early once threshold is reached.
    fn should_merge_parts(&self, merge_threshold: usize) -> bool {
        let mut bulk_count = 0;
        let mut encoded_count = 0;

        for wrapper in &self.parts {
            if wrapper.merging {
                continue;
            }

            if wrapper.part.is_encoded() {
                encoded_count += 1;
            } else {
                bulk_count += 1;
            }

            // Short-circuit: stop counting if either threshold is reached
            if bulk_count >= merge_threshold || encoded_count >= merge_threshold {
                return true;
            }
        }

        false
    }

    /// Returns true if the unordered_part should be compacted into a BulkPart.
    fn should_compact_unordered_part(&self) -> bool {
        self.unordered_part.should_compact()
    }

    /// Collects unmerged parts and marks them as being merged.
    /// Only collects parts of types that meet the threshold.
    /// Parts are pre-grouped into chunks for parallel processing.
    fn collect_parts_to_merge(
        &mut self,
        merge_threshold: usize,
        max_merge_groups: usize,
    ) -> CollectedParts {
        // First pass: collect indices and row counts for each type
        let mut bulk_indices: Vec<(usize, usize)> = Vec::new();
        let mut encoded_indices: Vec<(usize, usize)> = Vec::new();

        for (idx, wrapper) in self.parts.iter().enumerate() {
            if wrapper.merging {
                continue;
            }
            let num_rows = wrapper.part.num_rows();
            if wrapper.part.is_encoded() {
                encoded_indices.push((idx, num_rows));
            } else {
                bulk_indices.push((idx, num_rows));
            }
        }

        let mut groups = Vec::new();

        // Process bulk parts if threshold met
        if bulk_indices.len() >= merge_threshold {
            groups.extend(self.collect_and_group_parts(
                bulk_indices,
                merge_threshold,
                max_merge_groups,
            ));
        }

        // Process encoded parts if threshold met
        if encoded_indices.len() >= merge_threshold {
            groups.extend(self.collect_and_group_parts(
                encoded_indices,
                merge_threshold,
                max_merge_groups,
            ));
        }

        CollectedParts { groups }
    }

    /// Sorts indices by row count, groups into chunks, marks as merging, and returns groups.
    fn collect_and_group_parts(
        &mut self,
        mut indices: Vec<(usize, usize)>,
        merge_threshold: usize,
        max_merge_groups: usize,
    ) -> Vec<Vec<PartToMerge>> {
        if indices.is_empty() {
            return Vec::new();
        }

        // Sort by row count for better grouping
        indices.sort_unstable_by_key(|(_, num_rows)| *num_rows);

        // Group into chunks of merge_threshold size, limit to max_merge_groups
        indices
            .chunks(merge_threshold)
            .take(max_merge_groups)
            .map(|chunk| {
                chunk
                    .iter()
                    .map(|(idx, _)| {
                        let wrapper = &mut self.parts[*idx];
                        wrapper.merging = true;
                        wrapper.part.clone()
                    })
                    .collect()
            })
            .collect()
    }

    /// Installs merged parts and removes the original parts by file ids.
    /// Returns the total number of rows in the merged parts.
    fn install_merged_parts<I>(
        &mut self,
        merged_parts: I,
        merged_file_ids: &HashSet<FileId>,
    ) -> usize
    where
        I: IntoIterator<Item = MergedPart>,
    {
        let mut total_output_rows = 0;

        for merged_part in merged_parts {
            match merged_part {
                MergedPart::Encoded(encoded_part) => {
                    total_output_rows += encoded_part.metadata().num_rows;
                    self.parts.push(BulkPartWrapper {
                        part: PartToMerge::Encoded {
                            part: encoded_part,
                            file_id: FileId::random(),
                        },
                        merging: false,
                    });
                }
                MergedPart::Multi(multi_part) => {
                    total_output_rows += multi_part.num_rows();
                    self.parts.push(BulkPartWrapper {
                        part: PartToMerge::Multi {
                            part: multi_part,
                            file_id: FileId::random(),
                        },
                        merging: false,
                    });
                }
            }
        }

        self.parts
            .retain(|wrapper| !merged_file_ids.contains(&wrapper.file_id()));

        total_output_rows
    }

    /// Resets merging flag for parts with the given file ids.
    /// Used when merging fails or is cancelled.
    fn reset_merging_flags(&mut self, file_ids: &HashSet<FileId>) {
        for wrapper in &mut self.parts {
            if file_ids.contains(&wrapper.file_id()) {
                wrapper.merging = false;
            }
        }
    }
}

/// RAII guard for managing merging flags.
/// Automatically resets merging flags when dropped if the merge operation wasn't successful.
struct MergingFlagsGuard<'a> {
    bulk_parts: &'a RwLock<BulkParts>,
    file_ids: &'a HashSet<FileId>,
    success: bool,
}

impl<'a> MergingFlagsGuard<'a> {
    /// Creates a new guard for the given file ids.
    fn new(bulk_parts: &'a RwLock<BulkParts>, file_ids: &'a HashSet<FileId>) -> Self {
        Self {
            bulk_parts,
            file_ids,
            success: false,
        }
    }

    /// Marks the merge operation as successful.
    /// When this is called, the guard will not reset the flags on drop.
    fn mark_success(&mut self) {
        self.success = true;
    }
}

impl<'a> Drop for MergingFlagsGuard<'a> {
    fn drop(&mut self) {
        if !self.success
            && let Ok(mut parts) = self.bulk_parts.write()
        {
            parts.reset_merging_flags(self.file_ids);
        }
    }
}

/// Memtable that ingests and scans parts directly.
pub struct BulkMemtable {
    id: MemtableId,
    /// Configuration for the bulk memtable.
    config: BulkMemtableConfig,
    parts: Arc<RwLock<BulkParts>>,
    metadata: RegionMetadataRef,
    alloc_tracker: AllocTracker,
    max_timestamp: AtomicI64,
    min_timestamp: AtomicI64,
    max_sequence: AtomicU64,
    num_rows: AtomicUsize,
    /// Cached flat SST arrow schema for memtable compaction.
    flat_arrow_schema: SchemaRef,
    /// Compactor for merging bulk parts
    compactor: Arc<Mutex<MemtableCompactor>>,
    /// Dispatcher for scheduling compaction tasks
    compact_dispatcher: Option<Arc<CompactDispatcher>>,
    /// Whether the append mode is enabled
    append_mode: bool,
    /// Mode to handle duplicate rows while merging
    merge_mode: MergeMode,
}

impl std::fmt::Debug for BulkMemtable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BulkMemtable")
            .field("id", &self.id)
            .field("num_rows", &self.num_rows.load(Ordering::Relaxed))
            .field("min_timestamp", &self.min_timestamp.load(Ordering::Relaxed))
            .field("max_timestamp", &self.max_timestamp.load(Ordering::Relaxed))
            .field("max_sequence", &self.max_sequence.load(Ordering::Relaxed))
            .finish()
    }
}

impl Memtable for BulkMemtable {
    fn id(&self) -> MemtableId {
        self.id
    }

    fn write(&self, _kvs: &KeyValues) -> Result<()> {
        UnsupportedOperationSnafu {
            err_msg: "write() is not supported for bulk memtable",
        }
        .fail()
    }

    fn write_one(&self, _key_value: KeyValue) -> Result<()> {
        UnsupportedOperationSnafu {
            err_msg: "write_one() is not supported for bulk memtable",
        }
        .fail()
    }

    fn write_bulk(&self, fragment: BulkPart) -> Result<()> {
        let local_metrics = WriteMetrics {
            key_bytes: 0,
            value_bytes: fragment.estimated_size(),
            min_ts: fragment.min_timestamp,
            max_ts: fragment.max_timestamp,
            num_rows: fragment.num_rows(),
            max_sequence: fragment.sequence,
        };

        {
            let mut bulk_parts = self.parts.write().unwrap();

            // Routes small parts to unordered_part based on threshold
            if bulk_parts.unordered_part.should_accept(fragment.num_rows()) {
                bulk_parts.unordered_part.push(fragment);

                // Compacts unordered_part if threshold is reached
                if bulk_parts.should_compact_unordered_part()
                    && let Some(bulk_part) = bulk_parts.unordered_part.to_bulk_part()?
                {
                    bulk_parts.parts.push(BulkPartWrapper {
                        part: PartToMerge::Bulk {
                            part: bulk_part,
                            file_id: FileId::random(),
                        },
                        merging: false,
                    });
                    bulk_parts.unordered_part.clear();
                }
            } else {
                bulk_parts.parts.push(BulkPartWrapper {
                    part: PartToMerge::Bulk {
                        part: fragment,
                        file_id: FileId::random(),
                    },
                    merging: false,
                });
            }

            // Since this operation should be fast, we do it in parts lock scope.
            // This ensure the statistics in `ranges()` are correct. What's more,
            // it guarantees no rows are out of the time range so we don't need to
            // prune rows by time range again in the iterator of the MemtableRange.
            self.update_stats(local_metrics);
        }

        if self.should_compact() {
            self.schedule_compact();
        }

        Ok(())
    }

    #[cfg(any(test, feature = "test"))]
    fn iter(
        &self,
        _projection: Option<&[ColumnId]>,
        _predicate: Option<table::predicate::Predicate>,
        _sequence: Option<SequenceRange>,
    ) -> Result<crate::memtable::BoxedBatchIterator> {
        todo!()
    }

    fn ranges(
        &self,
        projection: Option<&[ColumnId]>,
        options: RangesOptions,
    ) -> Result<MemtableRanges> {
        let predicate = options.predicate;
        let sequence = options.sequence;
        let mut ranges = BTreeMap::new();
        let mut range_id = 0;

        // TODO(yingwen): Filter ranges by sequence.
        let context = Arc::new(BulkIterContext::new_with_pre_filter_mode(
            self.metadata.clone(),
            projection,
            predicate.predicate().cloned(),
            options.for_flush,
            options.pre_filter_mode,
        )?);

        // Adds ranges for regular parts and encoded parts
        {
            let bulk_parts = self.parts.read().unwrap();

            // Adds range for unordered part if not empty
            if !bulk_parts.unordered_part.is_empty()
                && let Some(unordered_bulk_part) = bulk_parts.unordered_part.to_bulk_part()?
            {
                let part_stats = unordered_bulk_part.to_memtable_stats(&self.metadata);
                let range = MemtableRange::new(
                    Arc::new(MemtableRangeContext::new(
                        self.id,
                        Box::new(BulkRangeIterBuilder {
                            part: unordered_bulk_part,
                            context: context.clone(),
                            sequence,
                        }),
                        predicate.clone(),
                    )),
                    part_stats,
                );
                ranges.insert(range_id, range);
                range_id += 1;
            }

            // Adds ranges for all parts
            for part_wrapper in bulk_parts.parts.iter() {
                // Skips empty parts
                if part_wrapper.part.num_rows() == 0 {
                    continue;
                }

                let part_stats = part_wrapper.part.to_memtable_stats(&self.metadata);
                let iter_builder: Box<dyn IterBuilder> = match &part_wrapper.part {
                    PartToMerge::Bulk { part, .. } => Box::new(BulkRangeIterBuilder {
                        part: part.clone(),
                        context: context.clone(),
                        sequence,
                    }),
                    PartToMerge::Multi { part, .. } => Box::new(MultiBulkRangeIterBuilder {
                        part: part.clone(),
                        context: context.clone(),
                        sequence,
                    }),
                    PartToMerge::Encoded { part, file_id } => {
                        Box::new(EncodedBulkRangeIterBuilder {
                            file_id: *file_id,
                            part: part.clone(),
                            context: context.clone(),
                            sequence,
                        })
                    }
                };

                let range = MemtableRange::new(
                    Arc::new(MemtableRangeContext::new(
                        self.id,
                        iter_builder,
                        predicate.clone(),
                    )),
                    part_stats,
                );
                ranges.insert(range_id, range);
                range_id += 1;
            }
        }

        Ok(MemtableRanges { ranges })
    }

    fn is_empty(&self) -> bool {
        let bulk_parts = self.parts.read().unwrap();
        bulk_parts.is_empty()
    }

    fn freeze(&self) -> Result<()> {
        self.alloc_tracker.done_allocating();
        Ok(())
    }

    fn stats(&self) -> MemtableStats {
        let estimated_bytes = self.alloc_tracker.bytes_allocated();

        if estimated_bytes == 0 || self.num_rows.load(Ordering::Relaxed) == 0 {
            return MemtableStats {
                estimated_bytes,
                time_range: None,
                num_rows: 0,
                num_ranges: 0,
                max_sequence: 0,
                series_count: 0,
            };
        }

        let ts_type = self
            .metadata
            .time_index_column()
            .column_schema
            .data_type
            .clone()
            .as_timestamp()
            .expect("Timestamp column must have timestamp type");
        let max_timestamp = ts_type.create_timestamp(self.max_timestamp.load(Ordering::Relaxed));
        let min_timestamp = ts_type.create_timestamp(self.min_timestamp.load(Ordering::Relaxed));

        let num_ranges = self.parts.read().unwrap().num_parts();

        MemtableStats {
            estimated_bytes,
            time_range: Some((min_timestamp, max_timestamp)),
            num_rows: self.num_rows.load(Ordering::Relaxed),
            num_ranges,
            max_sequence: self.max_sequence.load(Ordering::Relaxed),
            series_count: self.estimated_series_count(),
        }
    }

    fn fork(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef {
        // Computes the new flat schema based on the new metadata.
        let flat_arrow_schema = to_flat_sst_arrow_schema(
            metadata,
            &FlatSchemaOptions::from_encoding(metadata.primary_key_encoding),
        );

        Arc::new(Self {
            id,
            config: self.config.clone(),
            parts: Arc::new(RwLock::new(BulkParts::default())),
            metadata: metadata.clone(),
            alloc_tracker: AllocTracker::new(self.alloc_tracker.write_buffer_manager()),
            max_timestamp: AtomicI64::new(i64::MIN),
            min_timestamp: AtomicI64::new(i64::MAX),
            max_sequence: AtomicU64::new(0),
            num_rows: AtomicUsize::new(0),
            flat_arrow_schema,
            compactor: Arc::new(Mutex::new(MemtableCompactor::new(
                metadata.region_id,
                id,
                self.config.clone(),
            ))),
            compact_dispatcher: self.compact_dispatcher.clone(),
            append_mode: self.append_mode,
            merge_mode: self.merge_mode,
        })
    }

    fn compact(&self, for_flush: bool) -> Result<()> {
        let mut compactor = self.compactor.lock().unwrap();

        if for_flush {
            return Ok(());
        }

        // Unified merge for all parts
        let should_merge = self
            .parts
            .read()
            .unwrap()
            .should_merge_parts(self.config.merge_threshold);
        if should_merge {
            compactor.merge_parts(
                &self.flat_arrow_schema,
                &self.parts,
                &self.metadata,
                !self.append_mode,
                self.merge_mode,
            )?;
        }

        Ok(())
    }
}

impl BulkMemtable {
    /// Creates a new BulkMemtable
    pub fn new(
        id: MemtableId,
        config: BulkMemtableConfig,
        metadata: RegionMetadataRef,
        write_buffer_manager: Option<WriteBufferManagerRef>,
        compact_dispatcher: Option<Arc<CompactDispatcher>>,
        append_mode: bool,
        merge_mode: MergeMode,
    ) -> Self {
        let flat_arrow_schema = to_flat_sst_arrow_schema(
            &metadata,
            &FlatSchemaOptions::from_encoding(metadata.primary_key_encoding),
        );

        let region_id = metadata.region_id;
        Self {
            id,
            config: config.clone(),
            parts: Arc::new(RwLock::new(BulkParts::default())),
            metadata,
            alloc_tracker: AllocTracker::new(write_buffer_manager),
            max_timestamp: AtomicI64::new(i64::MIN),
            min_timestamp: AtomicI64::new(i64::MAX),
            max_sequence: AtomicU64::new(0),
            num_rows: AtomicUsize::new(0),
            flat_arrow_schema,
            compactor: Arc::new(Mutex::new(MemtableCompactor::new(region_id, id, config))),
            compact_dispatcher,
            append_mode,
            merge_mode,
        }
    }

    /// Sets the unordered part threshold (for testing).
    #[cfg(test)]
    pub fn set_unordered_part_threshold(&self, threshold: usize) {
        self.parts
            .write()
            .unwrap()
            .unordered_part
            .set_threshold(threshold);
    }

    /// Sets the unordered part compact threshold (for testing).
    #[cfg(test)]
    pub fn set_unordered_part_compact_threshold(&self, compact_threshold: usize) {
        self.parts
            .write()
            .unwrap()
            .unordered_part
            .set_compact_threshold(compact_threshold);
    }

    /// Updates memtable stats.
    ///
    /// Please update this inside the write lock scope.
    fn update_stats(&self, stats: WriteMetrics) {
        self.alloc_tracker
            .on_allocation(stats.key_bytes + stats.value_bytes);

        self.max_timestamp
            .fetch_max(stats.max_ts, Ordering::Relaxed);
        self.min_timestamp
            .fetch_min(stats.min_ts, Ordering::Relaxed);
        self.max_sequence
            .fetch_max(stats.max_sequence, Ordering::Relaxed);
        self.num_rows.fetch_add(stats.num_rows, Ordering::Relaxed);
    }

    /// Returns the estimated time series count.
    fn estimated_series_count(&self) -> usize {
        let bulk_parts = self.parts.read().unwrap();
        bulk_parts
            .parts
            .iter()
            .map(|part_wrapper| part_wrapper.part.series_count())
            .sum()
    }

    /// Returns whether the memtable should be compacted.
    fn should_compact(&self) -> bool {
        let parts = self.parts.read().unwrap();
        parts.should_merge_parts(self.config.merge_threshold)
    }

    /// Schedules a compaction task using the CompactDispatcher.
    fn schedule_compact(&self) {
        if let Some(dispatcher) = &self.compact_dispatcher {
            let task = MemCompactTask {
                metadata: self.metadata.clone(),
                parts: self.parts.clone(),
                config: self.config.clone(),
                flat_arrow_schema: self.flat_arrow_schema.clone(),
                compactor: self.compactor.clone(),
                append_mode: self.append_mode,
                merge_mode: self.merge_mode,
            };

            dispatcher.dispatch_compact(task);
        } else {
            // Uses synchronous compaction if no dispatcher is available.
            if let Err(e) = self.compact(false) {
                common_telemetry::error!(e; "Failed to compact table");
            }
        }
    }
}

/// Iterator builder for bulk range
pub struct BulkRangeIterBuilder {
    pub part: BulkPart,
    pub context: Arc<BulkIterContext>,
    pub sequence: Option<SequenceRange>,
}

/// Iterator builder for multi bulk range
struct MultiBulkRangeIterBuilder {
    part: MultiBulkPart,
    context: Arc<BulkIterContext>,
    sequence: Option<SequenceRange>,
}

impl IterBuilder for BulkRangeIterBuilder {
    fn build(&self, _metrics: Option<MemScanMetrics>) -> Result<BoxedBatchIterator> {
        UnsupportedOperationSnafu {
            err_msg: "BatchIterator is not supported for bulk memtable",
        }
        .fail()
    }

    fn is_record_batch(&self) -> bool {
        true
    }

    fn build_record_batch(
        &self,
        metrics: Option<MemScanMetrics>,
    ) -> Result<BoxedRecordBatchIterator> {
        let series_count = self.part.estimated_series_count();
        let iter = BulkPartBatchIter::from_single(
            self.part.batch.clone(),
            self.context.clone(),
            self.sequence,
            series_count,
            metrics,
        );

        Ok(Box::new(iter))
    }

    fn encoded_range(&self) -> Option<EncodedRange> {
        None
    }
}

impl IterBuilder for MultiBulkRangeIterBuilder {
    fn build(&self, _metrics: Option<MemScanMetrics>) -> Result<BoxedBatchIterator> {
        UnsupportedOperationSnafu {
            err_msg: "BatchIterator is not supported for multi bulk memtable",
        }
        .fail()
    }

    fn is_record_batch(&self) -> bool {
        true
    }

    fn build_record_batch(
        &self,
        metrics: Option<MemScanMetrics>,
    ) -> Result<BoxedRecordBatchIterator> {
        self.part
            .read(self.context.clone(), self.sequence, metrics)?
            .ok_or_else(|| {
                UnsupportedOperationSnafu {
                    err_msg: "Failed to create iterator for multi bulk part",
                }
                .build()
            })
    }

    fn encoded_range(&self) -> Option<EncodedRange> {
        None
    }
}

/// Iterator builder for encoded bulk range
struct EncodedBulkRangeIterBuilder {
    file_id: FileId,
    part: EncodedBulkPart,
    context: Arc<BulkIterContext>,
    sequence: Option<SequenceRange>,
}

impl IterBuilder for EncodedBulkRangeIterBuilder {
    fn build(&self, _metrics: Option<MemScanMetrics>) -> Result<BoxedBatchIterator> {
        UnsupportedOperationSnafu {
            err_msg: "BatchIterator is not supported for encoded bulk memtable",
        }
        .fail()
    }

    fn is_record_batch(&self) -> bool {
        true
    }

    fn build_record_batch(
        &self,
        metrics: Option<MemScanMetrics>,
    ) -> Result<BoxedRecordBatchIterator> {
        if let Some(iter) = self
            .part
            .read(self.context.clone(), self.sequence, metrics)?
        {
            Ok(iter)
        } else {
            // Return an empty iterator if no data to read
            Ok(Box::new(std::iter::empty()))
        }
    }

    fn encoded_range(&self) -> Option<EncodedRange> {
        Some(EncodedRange {
            data: self.part.data().clone(),
            sst_info: self.part.to_sst_info(self.file_id),
        })
    }
}

struct BulkPartWrapper {
    /// The part to store. It already contains the file id.
    part: PartToMerge,
    /// Whether this part is currently being merged.
    merging: bool,
}

impl BulkPartWrapper {
    /// Returns the file id of this part.
    fn file_id(&self) -> FileId {
        self.part.file_id()
    }
}

/// Enum to wrap different types of parts for unified merging.
#[derive(Clone)]
enum PartToMerge {
    /// Raw bulk part.
    Bulk { part: BulkPart, file_id: FileId },
    /// Multiple bulk parts.
    Multi {
        part: MultiBulkPart,
        file_id: FileId,
    },
    /// Encoded bulk part.
    Encoded {
        part: EncodedBulkPart,
        file_id: FileId,
    },
}

impl PartToMerge {
    /// Gets the file ID of this part.
    fn file_id(&self) -> FileId {
        match self {
            PartToMerge::Bulk { file_id, .. } => *file_id,
            PartToMerge::Multi { file_id, .. } => *file_id,
            PartToMerge::Encoded { file_id, .. } => *file_id,
        }
    }

    /// Gets the minimum timestamp of this part.
    fn min_timestamp(&self) -> i64 {
        match self {
            PartToMerge::Bulk { part, .. } => part.min_timestamp,
            PartToMerge::Multi { part, .. } => part.min_timestamp(),
            PartToMerge::Encoded { part, .. } => part.metadata().min_timestamp,
        }
    }

    /// Gets the maximum timestamp of this part.
    fn max_timestamp(&self) -> i64 {
        match self {
            PartToMerge::Bulk { part, .. } => part.max_timestamp,
            PartToMerge::Multi { part, .. } => part.max_timestamp(),
            PartToMerge::Encoded { part, .. } => part.metadata().max_timestamp,
        }
    }

    /// Gets the number of rows in this part.
    fn num_rows(&self) -> usize {
        match self {
            PartToMerge::Bulk { part, .. } => part.num_rows(),
            PartToMerge::Multi { part, .. } => part.num_rows(),
            PartToMerge::Encoded { part, .. } => part.metadata().num_rows,
        }
    }

    /// Gets the maximum sequence number of this part.
    fn max_sequence(&self) -> u64 {
        match self {
            PartToMerge::Bulk { part, .. } => part.sequence,
            PartToMerge::Multi { part, .. } => part.max_sequence(),
            PartToMerge::Encoded { part, .. } => part.metadata().max_sequence,
        }
    }

    /// Gets the estimated series count in this part.
    fn series_count(&self) -> usize {
        match self {
            PartToMerge::Bulk { part, .. } => part.estimated_series_count(),
            PartToMerge::Multi { part, .. } => part.series_count(),
            PartToMerge::Encoded { part, .. } => part.metadata().num_series as usize,
        }
    }

    /// Returns true if this is an encoded part.
    fn is_encoded(&self) -> bool {
        matches!(self, PartToMerge::Encoded { .. })
    }

    /// Gets the estimated size in bytes of this part.
    fn estimated_size(&self) -> usize {
        match self {
            PartToMerge::Bulk { part, .. } => part.estimated_size(),
            PartToMerge::Multi { part, .. } => part.estimated_size(),
            PartToMerge::Encoded { part, .. } => part.size_bytes(),
        }
    }

    /// Converts this part to `MemtableStats`.
    fn to_memtable_stats(&self, region_metadata: &RegionMetadataRef) -> MemtableStats {
        match self {
            PartToMerge::Bulk { part, .. } => part.to_memtable_stats(region_metadata),
            PartToMerge::Multi { part, .. } => part.to_memtable_stats(region_metadata),
            PartToMerge::Encoded { part, .. } => part.to_memtable_stats(),
        }
    }

    /// Creates a record batch iterator for this part.
    fn create_iterator(
        self,
        context: Arc<BulkIterContext>,
    ) -> Result<Option<BoxedRecordBatchIterator>> {
        match self {
            PartToMerge::Bulk { part, .. } => {
                let series_count = part.estimated_series_count();
                let iter = BulkPartBatchIter::from_single(
                    part.batch,
                    context,
                    None, // No sequence filter for merging
                    series_count,
                    None, // No metrics for merging
                );
                Ok(Some(Box::new(iter) as BoxedRecordBatchIterator))
            }
            PartToMerge::Multi { part, .. } => part.read(context, None, None),
            PartToMerge::Encoded { part, .. } => part.read(context, None, None),
        }
    }
}

struct MemtableCompactor {
    region_id: RegionId,
    memtable_id: MemtableId,
    /// Configuration for the bulk memtable.
    config: BulkMemtableConfig,
}

impl MemtableCompactor {
    /// Creates a new MemtableCompactor.
    fn new(region_id: RegionId, memtable_id: MemtableId, config: BulkMemtableConfig) -> Self {
        Self {
            region_id,
            memtable_id,
            config,
        }
    }

    /// Merges parts (bulk and encoded) and then encodes the result.
    fn merge_parts(
        &mut self,
        arrow_schema: &SchemaRef,
        bulk_parts: &RwLock<BulkParts>,
        metadata: &RegionMetadataRef,
        dedup: bool,
        merge_mode: MergeMode,
    ) -> Result<()> {
        let start = Instant::now();

        // Collect pre-grouped parts
        let collected = bulk_parts
            .write()
            .unwrap()
            .collect_parts_to_merge(self.config.merge_threshold, self.config.max_merge_groups);

        if collected.groups.is_empty() {
            return Ok(());
        }

        // Collect all file IDs for tracking
        let merged_file_ids: HashSet<FileId> = collected
            .groups
            .iter()
            .flatten()
            .map(|part| part.file_id())
            .collect();
        let mut guard = MergingFlagsGuard::new(bulk_parts, &merged_file_ids);

        let num_groups = collected.groups.len();
        let num_parts: usize = collected.groups.iter().map(|g| g.len()).sum();

        let encode_row_threshold = self.config.encode_row_threshold;
        let encode_bytes_threshold = self.config.encode_bytes_threshold;

        // Merge all groups in parallel
        let merged_parts = collected
            .groups
            .into_par_iter()
            .map(|group| {
                Self::merge_parts_group(
                    group,
                    arrow_schema,
                    metadata,
                    dedup,
                    merge_mode,
                    encode_row_threshold,
                    encode_bytes_threshold,
                )
            })
            .collect::<Result<Vec<Option<MergedPart>>>>()?;

        // Install all merged parts
        let total_output_rows = {
            let mut parts = bulk_parts.write().unwrap();
            parts.install_merged_parts(merged_parts.into_iter().flatten(), &merged_file_ids)
        };

        guard.mark_success();

        common_telemetry::debug!(
            "BulkMemtable {} {} concurrent compact {} groups, {} parts, {} rows, cost: {:?}",
            self.region_id,
            self.memtable_id,
            num_groups,
            num_parts,
            total_output_rows,
            start.elapsed()
        );

        Ok(())
    }

    /// Merges a group of parts into a single part (either MultiBulkPart or EncodedBulkPart).
    fn merge_parts_group(
        parts_to_merge: Vec<PartToMerge>,
        arrow_schema: &SchemaRef,
        metadata: &RegionMetadataRef,
        dedup: bool,
        merge_mode: MergeMode,
        encode_row_threshold: usize,
        encode_bytes_threshold: usize,
    ) -> Result<Option<MergedPart>> {
        if parts_to_merge.is_empty() {
            return Ok(None);
        }

        // Calculates timestamp bounds and statistics for merged data
        let min_timestamp = parts_to_merge
            .iter()
            .map(|p| p.min_timestamp())
            .min()
            .unwrap_or(i64::MAX);
        let max_timestamp = parts_to_merge
            .iter()
            .map(|p| p.max_timestamp())
            .max()
            .unwrap_or(i64::MIN);
        let max_sequence = parts_to_merge
            .iter()
            .map(|p| p.max_sequence())
            .max()
            .unwrap_or(0);

        // Collects statistics from parts before creating iterators
        let estimated_total_rows: usize = parts_to_merge.iter().map(|p| p.num_rows()).sum();
        let estimated_total_bytes: usize = parts_to_merge.iter().map(|p| p.estimated_size()).sum();
        let estimated_series_count = parts_to_merge
            .iter()
            .map(|p| p.series_count())
            .max()
            .unwrap_or(0);

        let context = Arc::new(BulkIterContext::new(
            metadata.clone(),
            None, // No column projection for merging
            None, // No predicate for merging
            true,
        )?);

        // Creates iterators for all parts to merge.
        let iterators: Vec<BoxedRecordBatchIterator> = parts_to_merge
            .into_iter()
            .filter_map(|part| part.create_iterator(context.clone()).ok().flatten())
            .collect();

        if iterators.is_empty() {
            return Ok(None);
        }

        let merged_iter =
            FlatMergeIterator::new(arrow_schema.clone(), iterators, DEFAULT_READ_BATCH_SIZE)?;

        let boxed_iter: BoxedRecordBatchIterator = if dedup {
            // Applies deduplication based on merge mode
            match merge_mode {
                MergeMode::LastRow => {
                    let dedup_iter = FlatDedupIterator::new(merged_iter, FlatLastRow::new(false));
                    Box::new(dedup_iter)
                }
                MergeMode::LastNonNull => {
                    // Calculates field column start: total columns - fixed columns - field columns
                    // Field column count = total metadata columns - time index column - primary key columns
                    let field_column_count =
                        metadata.column_metadatas.len() - 1 - metadata.primary_key.len();
                    let total_columns = arrow_schema.fields().len();
                    let field_column_start =
                        total_columns - FIXED_POS_COLUMN_NUM - field_column_count;

                    let dedup_iter = FlatDedupIterator::new(
                        merged_iter,
                        FlatLastNonNull::new(field_column_start, false),
                    );
                    Box::new(dedup_iter)
                }
            }
        } else {
            Box::new(merged_iter)
        };

        // Encode as EncodedBulkPart if rows exceed row threshold OR bytes exceed bytes threshold
        if estimated_total_rows > encode_row_threshold
            || estimated_total_bytes > encode_bytes_threshold
        {
            let encoder = BulkPartEncoder::new(metadata.clone(), DEFAULT_ROW_GROUP_SIZE)?;
            let mut metrics = BulkPartEncodeMetrics::default();
            let encoded_part = encoder.encode_record_batch_iter(
                boxed_iter,
                arrow_schema.clone(),
                min_timestamp,
                max_timestamp,
                max_sequence,
                &mut metrics,
            )?;

            common_telemetry::trace!("merge_parts_group metrics: {:?}", metrics);

            Ok(encoded_part.map(MergedPart::Encoded))
        } else {
            // Otherwise, collect into MultiBulkPart
            let mut batches = Vec::new();
            let mut actual_total_rows = 0;

            for batch_result in boxed_iter {
                let batch = batch_result?;
                actual_total_rows += batch.num_rows();
                batches.push(batch);
            }

            if actual_total_rows == 0 {
                return Ok(None);
            }

            let multi_part = MultiBulkPart::new(
                batches,
                min_timestamp,
                max_timestamp,
                max_sequence,
                estimated_series_count,
            );

            common_telemetry::trace!(
                "merge_parts_group created MultiBulkPart: rows={}, batches={}",
                actual_total_rows,
                multi_part.num_batches()
            );

            Ok(Some(MergedPart::Multi(multi_part)))
        }
    }
}

/// A memtable compact task to run in background.
struct MemCompactTask {
    metadata: RegionMetadataRef,
    parts: Arc<RwLock<BulkParts>>,
    /// Configuration for the bulk memtable.
    config: BulkMemtableConfig,
    /// Cached flat SST arrow schema
    flat_arrow_schema: SchemaRef,
    /// Compactor for merging bulk parts
    compactor: Arc<Mutex<MemtableCompactor>>,
    /// Whether the append mode is enabled
    append_mode: bool,
    /// Mode to handle duplicate rows while merging
    merge_mode: MergeMode,
}

impl MemCompactTask {
    fn compact(&self) -> Result<()> {
        let mut compactor = self.compactor.lock().unwrap();

        let should_merge = self
            .parts
            .read()
            .unwrap()
            .should_merge_parts(self.config.merge_threshold);
        if should_merge {
            compactor.merge_parts(
                &self.flat_arrow_schema,
                &self.parts,
                &self.metadata,
                !self.append_mode,
                self.merge_mode,
            )?;
        }

        Ok(())
    }
}

/// Scheduler to run compact tasks in background.
#[derive(Debug)]
pub struct CompactDispatcher {
    semaphore: Arc<Semaphore>,
}

impl CompactDispatcher {
    /// Creates a new dispatcher with the given number of max concurrent tasks.
    pub fn new(permits: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(permits)),
        }
    }

    /// Dispatches a compact task to run in background.
    fn dispatch_compact(&self, task: MemCompactTask) {
        let semaphore = self.semaphore.clone();
        common_runtime::spawn_global(async move {
            let Ok(_permit) = semaphore.acquire().await else {
                return;
            };

            common_runtime::spawn_blocking_global(move || {
                if let Err(e) = task.compact() {
                    common_telemetry::error!(e; "Failed to compact memtable, region: {}", task.metadata.region_id);
                }
            });
        });
    }
}

/// Builder to build a [BulkMemtable].
#[derive(Debug, Default)]
pub struct BulkMemtableBuilder {
    /// Configuration for the bulk memtable.
    config: BulkMemtableConfig,
    write_buffer_manager: Option<WriteBufferManagerRef>,
    compact_dispatcher: Option<Arc<CompactDispatcher>>,
    append_mode: bool,
    merge_mode: MergeMode,
}

impl BulkMemtableBuilder {
    /// Creates a new builder with specific `write_buffer_manager`.
    pub fn new(
        write_buffer_manager: Option<WriteBufferManagerRef>,
        append_mode: bool,
        merge_mode: MergeMode,
    ) -> Self {
        Self {
            config: BulkMemtableConfig::default(),
            write_buffer_manager,
            compact_dispatcher: None,
            append_mode,
            merge_mode,
        }
    }

    /// Sets the compact dispatcher.
    pub fn with_compact_dispatcher(mut self, compact_dispatcher: Arc<CompactDispatcher>) -> Self {
        self.compact_dispatcher = Some(compact_dispatcher);
        self
    }
}

impl MemtableBuilder for BulkMemtableBuilder {
    fn build(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef {
        Arc::new(BulkMemtable::new(
            id,
            self.config.clone(),
            metadata.clone(),
            self.write_buffer_manager.clone(),
            self.compact_dispatcher.clone(),
            self.append_mode,
            self.merge_mode,
        ))
    }

    fn use_bulk_insert(&self, _metadata: &RegionMetadataRef) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use mito_codec::row_converter::build_primary_key_codec;

    use super::*;
    use crate::memtable::bulk::part::BulkPartConverter;
    use crate::read::scan_region::PredicateGroup;
    use crate::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
    use crate::test_util::memtable_util::{build_key_values_with_ts_seq_values, metadata_for_test};

    fn create_bulk_part_with_converter(
        k0: &str,
        k1: u32,
        timestamps: Vec<i64>,
        values: Vec<Option<f64>>,
        sequence: u64,
    ) -> Result<BulkPart> {
        let metadata = metadata_for_test();
        let capacity = 100;
        let primary_key_codec = build_primary_key_codec(&metadata);
        let schema = to_flat_sst_arrow_schema(
            &metadata,
            &FlatSchemaOptions::from_encoding(metadata.primary_key_encoding),
        );

        let mut converter =
            BulkPartConverter::new(&metadata, schema, capacity, primary_key_codec, true);

        let key_values = build_key_values_with_ts_seq_values(
            &metadata,
            k0.to_string(),
            k1,
            timestamps.into_iter(),
            values.into_iter(),
            sequence,
        );

        converter.append_key_values(&key_values)?;
        converter.convert()
    }

    #[test]
    fn test_bulk_memtable_write_read() {
        let metadata = metadata_for_test();
        let memtable = BulkMemtable::new(
            999,
            BulkMemtableConfig::default(),
            metadata.clone(),
            None,
            None,
            false,
            MergeMode::LastRow,
        );
        // Disable unordered_part for this test
        memtable.set_unordered_part_threshold(0);

        let test_data = [
            (
                "key_a",
                1u32,
                vec![1000i64, 2000i64],
                vec![Some(10.5), Some(20.5)],
                100u64,
            ),
            (
                "key_b",
                2u32,
                vec![1500i64, 2500i64],
                vec![Some(15.5), Some(25.5)],
                200u64,
            ),
            ("key_c", 3u32, vec![3000i64], vec![Some(30.5)], 300u64),
        ];

        for (k0, k1, timestamps, values, seq) in test_data.iter() {
            let part =
                create_bulk_part_with_converter(k0, *k1, timestamps.clone(), values.clone(), *seq)
                    .unwrap();
            memtable.write_bulk(part).unwrap();
        }

        let stats = memtable.stats();
        assert_eq!(5, stats.num_rows);
        assert_eq!(3, stats.num_ranges);
        assert_eq!(300, stats.max_sequence);

        let (min_ts, max_ts) = stats.time_range.unwrap();
        assert_eq!(1000, min_ts.value());
        assert_eq!(3000, max_ts.value());

        let predicate_group = PredicateGroup::new(&metadata, &[]).unwrap();
        let ranges = memtable
            .ranges(
                None,
                RangesOptions::default().with_predicate(predicate_group),
            )
            .unwrap();

        assert_eq!(3, ranges.ranges.len());
        let total_rows: usize = ranges.ranges.values().map(|r| r.stats().num_rows()).sum();
        assert_eq!(5, total_rows);

        for (_range_id, range) in ranges.ranges.iter() {
            assert!(range.num_rows() > 0);
            assert!(range.is_record_batch());

            let record_batch_iter = range.build_record_batch_iter(None).unwrap();

            let mut total_rows = 0;
            for batch_result in record_batch_iter {
                let batch = batch_result.unwrap();
                total_rows += batch.num_rows();
                assert!(batch.num_rows() > 0);
                assert_eq!(8, batch.num_columns());
            }
            assert_eq!(total_rows, range.num_rows());
        }
    }

    #[test]
    fn test_bulk_memtable_ranges_with_projection() {
        let metadata = metadata_for_test();
        let memtable = BulkMemtable::new(
            111,
            BulkMemtableConfig::default(),
            metadata.clone(),
            None,
            None,
            false,
            MergeMode::LastRow,
        );

        let bulk_part = create_bulk_part_with_converter(
            "projection_test",
            5,
            vec![5000, 6000, 7000],
            vec![Some(50.0), Some(60.0), Some(70.0)],
            500,
        )
        .unwrap();

        memtable.write_bulk(bulk_part).unwrap();

        let projection = vec![4u32];
        let predicate_group = PredicateGroup::new(&metadata, &[]).unwrap();
        let ranges = memtable
            .ranges(
                Some(&projection),
                RangesOptions::default().with_predicate(predicate_group),
            )
            .unwrap();

        assert_eq!(1, ranges.ranges.len());
        let range = ranges.ranges.get(&0).unwrap();

        assert!(range.is_record_batch());
        let record_batch_iter = range.build_record_batch_iter(None).unwrap();

        let mut total_rows = 0;
        for batch_result in record_batch_iter {
            let batch = batch_result.unwrap();
            assert!(batch.num_rows() > 0);
            assert_eq!(5, batch.num_columns());
            total_rows += batch.num_rows();
        }
        assert_eq!(3, total_rows);
    }

    #[test]
    fn test_bulk_memtable_unsupported_operations() {
        let metadata = metadata_for_test();
        let memtable = BulkMemtable::new(
            111,
            BulkMemtableConfig::default(),
            metadata.clone(),
            None,
            None,
            false,
            MergeMode::LastRow,
        );

        let key_values = build_key_values_with_ts_seq_values(
            &metadata,
            "test".to_string(),
            1,
            vec![1000].into_iter(),
            vec![Some(1.0)].into_iter(),
            1,
        );

        let err = memtable.write(&key_values).unwrap_err();
        assert!(err.to_string().contains("not supported"));

        let kv = key_values.iter().next().unwrap();
        let err = memtable.write_one(kv).unwrap_err();
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn test_bulk_memtable_freeze() {
        let metadata = metadata_for_test();
        let memtable = BulkMemtable::new(
            222,
            BulkMemtableConfig::default(),
            metadata.clone(),
            None,
            None,
            false,
            MergeMode::LastRow,
        );

        let bulk_part = create_bulk_part_with_converter(
            "freeze_test",
            10,
            vec![10000],
            vec![Some(100.0)],
            1000,
        )
        .unwrap();

        memtable.write_bulk(bulk_part).unwrap();
        memtable.freeze().unwrap();

        let stats_after_freeze = memtable.stats();
        assert_eq!(1, stats_after_freeze.num_rows);
    }

    #[test]
    fn test_bulk_memtable_fork() {
        let metadata = metadata_for_test();
        let original_memtable = BulkMemtable::new(
            333,
            BulkMemtableConfig::default(),
            metadata.clone(),
            None,
            None,
            false,
            MergeMode::LastRow,
        );

        let bulk_part =
            create_bulk_part_with_converter("fork_test", 15, vec![15000], vec![Some(150.0)], 1500)
                .unwrap();

        original_memtable.write_bulk(bulk_part).unwrap();

        let forked_memtable = original_memtable.fork(444, &metadata);

        assert_eq!(forked_memtable.id(), 444);
        assert!(forked_memtable.is_empty());
        assert_eq!(0, forked_memtable.stats().num_rows);

        assert!(!original_memtable.is_empty());
        assert_eq!(1, original_memtable.stats().num_rows);
    }

    #[test]
    fn test_bulk_memtable_ranges_multiple_parts() {
        let metadata = metadata_for_test();
        let memtable = BulkMemtable::new(
            777,
            BulkMemtableConfig::default(),
            metadata.clone(),
            None,
            None,
            false,
            MergeMode::LastRow,
        );
        // Disable unordered_part for this test
        memtable.set_unordered_part_threshold(0);

        let parts_data = vec![
            (
                "part1",
                1u32,
                vec![1000i64, 1100i64],
                vec![Some(10.0), Some(11.0)],
                100u64,
            ),
            (
                "part2",
                2u32,
                vec![2000i64, 2100i64],
                vec![Some(20.0), Some(21.0)],
                200u64,
            ),
            ("part3", 3u32, vec![3000i64], vec![Some(30.0)], 300u64),
        ];

        for (k0, k1, timestamps, values, seq) in parts_data {
            let part = create_bulk_part_with_converter(k0, k1, timestamps, values, seq).unwrap();
            memtable.write_bulk(part).unwrap();
        }

        let predicate_group = PredicateGroup::new(&metadata, &[]).unwrap();
        let ranges = memtable
            .ranges(
                None,
                RangesOptions::default().with_predicate(predicate_group),
            )
            .unwrap();

        assert_eq!(3, ranges.ranges.len());
        let total_rows: usize = ranges.ranges.values().map(|r| r.stats().num_rows()).sum();
        assert_eq!(5, total_rows);
        assert_eq!(3, ranges.ranges.len());

        for (range_id, range) in ranges.ranges.iter() {
            assert!(*range_id < 3);
            assert!(range.num_rows() > 0);
            assert!(range.is_record_batch());
        }
    }

    #[test]
    fn test_bulk_memtable_ranges_with_sequence_filter() {
        let metadata = metadata_for_test();
        let memtable = BulkMemtable::new(
            888,
            BulkMemtableConfig::default(),
            metadata.clone(),
            None,
            None,
            false,
            MergeMode::LastRow,
        );

        let part = create_bulk_part_with_converter(
            "seq_test",
            1,
            vec![1000, 2000, 3000],
            vec![Some(10.0), Some(20.0), Some(30.0)],
            500,
        )
        .unwrap();

        memtable.write_bulk(part).unwrap();

        let predicate_group = PredicateGroup::new(&metadata, &[]).unwrap();
        let sequence_filter = Some(SequenceRange::LtEq { max: 400 }); // Filters out rows with sequence > 400
        let ranges = memtable
            .ranges(
                None,
                RangesOptions::default()
                    .with_predicate(predicate_group)
                    .with_sequence(sequence_filter),
            )
            .unwrap();

        assert_eq!(1, ranges.ranges.len());
        let range = ranges.ranges.get(&0).unwrap();

        let mut record_batch_iter = range.build_record_batch_iter(None).unwrap();
        assert!(record_batch_iter.next().is_none());
    }

    #[test]
    fn test_bulk_memtable_ranges_with_encoded_parts() {
        let metadata = metadata_for_test();
        let memtable = BulkMemtable::new(
            999,
            BulkMemtableConfig::default(),
            metadata.clone(),
            None,
            None,
            false,
            MergeMode::LastRow,
        );
        // Disable unordered_part for this test
        memtable.set_unordered_part_threshold(0);

        // Adds enough bulk parts to trigger encoding
        for i in 0..10 {
            let part = create_bulk_part_with_converter(
                &format!("key_{}", i),
                i,
                vec![1000 + i as i64 * 100],
                vec![Some(i as f64 * 10.0)],
                100 + i as u64,
            )
            .unwrap();
            memtable.write_bulk(part).unwrap();
        }

        memtable.compact(false).unwrap();

        let predicate_group = PredicateGroup::new(&metadata, &[]).unwrap();
        let ranges = memtable
            .ranges(
                None,
                RangesOptions::default().with_predicate(predicate_group),
            )
            .unwrap();

        // Should have ranges for both bulk parts and encoded parts
        assert_eq!(3, ranges.ranges.len());
        let total_rows: usize = ranges.ranges.values().map(|r| r.stats().num_rows()).sum();
        assert_eq!(10, total_rows);

        for (_range_id, range) in ranges.ranges.iter() {
            assert!(range.num_rows() > 0);
            assert!(range.is_record_batch());

            let record_batch_iter = range.build_record_batch_iter(None).unwrap();
            let mut total_rows = 0;
            for batch_result in record_batch_iter {
                let batch = batch_result.unwrap();
                total_rows += batch.num_rows();
                assert!(batch.num_rows() > 0);
            }
            assert_eq!(total_rows, range.num_rows());
        }
    }

    #[test]
    fn test_bulk_memtable_unordered_part() {
        let metadata = metadata_for_test();
        let memtable = BulkMemtable::new(
            1001,
            BulkMemtableConfig::default(),
            metadata.clone(),
            None,
            None,
            false,
            MergeMode::LastRow,
        );

        // Set smaller thresholds for testing with smaller inputs
        // Accept parts with < 5 rows into unordered_part
        memtable.set_unordered_part_threshold(5);
        // Compact when total rows >= 10
        memtable.set_unordered_part_compact_threshold(10);

        // Write 3 small parts (each has 2 rows), should be collected in unordered_part
        for i in 0..3 {
            let part = create_bulk_part_with_converter(
                &format!("key_{}", i),
                i,
                vec![1000 + i as i64 * 100, 1100 + i as i64 * 100],
                vec![Some(i as f64 * 10.0), Some(i as f64 * 10.0 + 1.0)],
                100 + i as u64,
            )
            .unwrap();
            assert_eq!(2, part.num_rows());
            memtable.write_bulk(part).unwrap();
        }

        // Total rows = 6, not yet reaching compact threshold
        let stats = memtable.stats();
        assert_eq!(6, stats.num_rows);

        // Write 2 more small parts (each has 2 rows)
        // This should trigger compaction when total >= 10
        for i in 3..5 {
            let part = create_bulk_part_with_converter(
                &format!("key_{}", i),
                i,
                vec![1000 + i as i64 * 100, 1100 + i as i64 * 100],
                vec![Some(i as f64 * 10.0), Some(i as f64 * 10.0 + 1.0)],
                100 + i as u64,
            )
            .unwrap();
            memtable.write_bulk(part).unwrap();
        }

        // Total rows = 10, should have compacted unordered_part into a regular part
        let stats = memtable.stats();
        assert_eq!(10, stats.num_rows);

        // Verify we can read all data correctly
        let predicate_group = PredicateGroup::new(&metadata, &[]).unwrap();
        let ranges = memtable
            .ranges(
                None,
                RangesOptions::default().with_predicate(predicate_group),
            )
            .unwrap();

        // Should have at least 1 range (the compacted part)
        assert!(!ranges.ranges.is_empty());
        let total_rows: usize = ranges.ranges.values().map(|r| r.stats().num_rows()).sum();
        assert_eq!(10, total_rows);

        // Read all data and verify
        let mut total_rows_read = 0;
        for (_range_id, range) in ranges.ranges.iter() {
            assert!(range.is_record_batch());
            let record_batch_iter = range.build_record_batch_iter(None).unwrap();

            for batch_result in record_batch_iter {
                let batch = batch_result.unwrap();
                total_rows_read += batch.num_rows();
            }
        }
        assert_eq!(10, total_rows_read);
    }

    #[test]
    fn test_bulk_memtable_unordered_part_mixed_sizes() {
        let metadata = metadata_for_test();
        let memtable = BulkMemtable::new(
            1002,
            BulkMemtableConfig::default(),
            metadata.clone(),
            None,
            None,
            false,
            MergeMode::LastRow,
        );

        // Set threshold to 4 rows - parts with < 4 rows go to unordered_part
        memtable.set_unordered_part_threshold(4);
        memtable.set_unordered_part_compact_threshold(8);

        // Write small parts (3 rows each) - should go to unordered_part
        for i in 0..2 {
            let part = create_bulk_part_with_converter(
                &format!("small_{}", i),
                i,
                vec![1000 + i as i64, 2000 + i as i64, 3000 + i as i64],
                vec![Some(i as f64), Some(i as f64 + 1.0), Some(i as f64 + 2.0)],
                10 + i as u64,
            )
            .unwrap();
            assert_eq!(3, part.num_rows());
            memtable.write_bulk(part).unwrap();
        }

        // Write a large part (5 rows) - should go directly to regular parts
        let large_part = create_bulk_part_with_converter(
            "large_key",
            100,
            vec![5000, 6000, 7000, 8000, 9000],
            vec![
                Some(100.0),
                Some(101.0),
                Some(102.0),
                Some(103.0),
                Some(104.0),
            ],
            50,
        )
        .unwrap();
        assert_eq!(5, large_part.num_rows());
        memtable.write_bulk(large_part).unwrap();

        // Write another small part (2 rows) - should trigger compaction of unordered_part
        let part = create_bulk_part_with_converter(
            "small_2",
            2,
            vec![4000, 4100],
            vec![Some(20.0), Some(21.0)],
            30,
        )
        .unwrap();
        memtable.write_bulk(part).unwrap();

        let stats = memtable.stats();
        assert_eq!(13, stats.num_rows); // 3 + 3 + 5 + 2 = 13

        // Verify all data can be read
        let predicate_group = PredicateGroup::new(&metadata, &[]).unwrap();
        let ranges = memtable
            .ranges(
                None,
                RangesOptions::default().with_predicate(predicate_group),
            )
            .unwrap();

        let total_rows: usize = ranges.ranges.values().map(|r| r.stats().num_rows()).sum();
        assert_eq!(13, total_rows);

        let mut total_rows_read = 0;
        for (_range_id, range) in ranges.ranges.iter() {
            let record_batch_iter = range.build_record_batch_iter(None).unwrap();
            for batch_result in record_batch_iter {
                let batch = batch_result.unwrap();
                total_rows_read += batch.num_rows();
            }
        }
        assert_eq!(13, total_rows_read);
    }

    #[test]
    fn test_bulk_memtable_unordered_part_with_ranges() {
        let metadata = metadata_for_test();
        let memtable = BulkMemtable::new(
            1003,
            BulkMemtableConfig::default(),
            metadata.clone(),
            None,
            None,
            false,
            MergeMode::LastRow,
        );

        // Set small thresholds
        memtable.set_unordered_part_threshold(3);
        memtable.set_unordered_part_compact_threshold(100); // High threshold to prevent auto-compaction

        // Write several small parts that stay in unordered_part
        for i in 0..3 {
            let part = create_bulk_part_with_converter(
                &format!("key_{}", i),
                i,
                vec![1000 + i as i64 * 100],
                vec![Some(i as f64 * 10.0)],
                100 + i as u64,
            )
            .unwrap();
            assert_eq!(1, part.num_rows());
            memtable.write_bulk(part).unwrap();
        }

        let stats = memtable.stats();
        assert_eq!(3, stats.num_rows);

        // Test that ranges() can correctly read from unordered_part
        let predicate_group = PredicateGroup::new(&metadata, &[]).unwrap();
        let ranges = memtable
            .ranges(
                None,
                RangesOptions::default().with_predicate(predicate_group),
            )
            .unwrap();

        // Should have 1 range for the unordered_part
        assert_eq!(1, ranges.ranges.len());
        let total_rows: usize = ranges.ranges.values().map(|r| r.stats().num_rows()).sum();
        assert_eq!(3, total_rows);

        // Verify data is sorted correctly in the range
        let range = ranges.ranges.get(&0).unwrap();
        let record_batch_iter = range.build_record_batch_iter(None).unwrap();

        let mut total_rows = 0;
        for batch_result in record_batch_iter {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
            // Verify data is properly sorted by primary key
            assert!(batch.num_rows() > 0);
        }
        assert_eq!(3, total_rows);
    }

    /// Helper to create a BulkPartWrapper from a BulkPart.
    fn create_bulk_part_wrapper(part: BulkPart) -> BulkPartWrapper {
        BulkPartWrapper {
            part: PartToMerge::Bulk {
                part,
                file_id: FileId::random(),
            },
            merging: false,
        }
    }

    #[test]
    fn test_should_merge_parts_below_threshold() {
        let mut bulk_parts = BulkParts::default();

        // Add 7 bulk parts (below DEFAULT_MERGE_THRESHOLD of 8)
        for i in 0..DEFAULT_MERGE_THRESHOLD - 1 {
            let part = create_bulk_part_with_converter(
                &format!("key_{}", i),
                i as u32,
                vec![1000 + i as i64 * 100],
                vec![Some(i as f64 * 10.0)],
                100 + i as u64,
            )
            .unwrap();
            bulk_parts.parts.push(create_bulk_part_wrapper(part));
        }

        // Should not trigger merge since we have only 7 parts
        assert!(!bulk_parts.should_merge_parts(DEFAULT_MERGE_THRESHOLD));
    }

    #[test]
    fn test_should_merge_parts_at_threshold() {
        let mut bulk_parts = BulkParts::default();

        // Add 8 bulk parts (at DEFAULT_MERGE_THRESHOLD)
        for i in 0..8 {
            let part = create_bulk_part_with_converter(
                &format!("key_{}", i),
                i,
                vec![1000 + i as i64 * 100],
                vec![Some(i as f64 * 10.0)],
                100 + i as u64,
            )
            .unwrap();
            bulk_parts.parts.push(create_bulk_part_wrapper(part));
        }

        // Should trigger merge since we have 8 parts
        assert!(bulk_parts.should_merge_parts(DEFAULT_MERGE_THRESHOLD));
    }

    #[test]
    fn test_should_merge_parts_with_merging_flag() {
        let mut bulk_parts = BulkParts::default();

        // Add 10 bulk parts
        for i in 0..10 {
            let part = create_bulk_part_with_converter(
                &format!("key_{}", i),
                i,
                vec![1000 + i as i64 * 100],
                vec![Some(i as f64 * 10.0)],
                100 + i as u64,
            )
            .unwrap();
            bulk_parts.parts.push(create_bulk_part_wrapper(part));
        }

        // Should trigger merge since we have 10 parts
        assert!(bulk_parts.should_merge_parts(DEFAULT_MERGE_THRESHOLD));

        // Mark first 3 parts as merging
        for wrapper in bulk_parts.parts.iter_mut().take(3) {
            wrapper.merging = true;
        }

        // Now only 7 parts are available for merging, should not trigger
        assert!(!bulk_parts.should_merge_parts(DEFAULT_MERGE_THRESHOLD));
    }

    #[test]
    fn test_collect_parts_to_merge_grouping() {
        let mut bulk_parts = BulkParts::default();

        // Add 16 bulk parts with different row counts
        for i in 0..16 {
            let num_rows = (i % 4) + 1; // 1 to 4 rows
            let timestamps: Vec<i64> = (0..num_rows)
                .map(|j| 1000 + i as i64 * 100 + j as i64)
                .collect();
            let values: Vec<Option<f64>> =
                (0..num_rows).map(|j| Some((i * 10 + j) as f64)).collect();
            let part = create_bulk_part_with_converter(
                &format!("key_{}", i),
                i as u32,
                timestamps,
                values,
                100 + i as u64,
            )
            .unwrap();
            bulk_parts.parts.push(create_bulk_part_wrapper(part));
        }

        // Should trigger merge since we have 16 parts
        assert!(bulk_parts.should_merge_parts(DEFAULT_MERGE_THRESHOLD));

        // Collect parts to merge
        let collected =
            bulk_parts.collect_parts_to_merge(DEFAULT_MERGE_THRESHOLD, DEFAULT_MAX_MERGE_GROUPS);

        // Should have groups
        assert!(!collected.groups.is_empty());

        // All groups should have parts
        for group in &collected.groups {
            assert!(!group.is_empty());
        }

        // Total parts collected should be 16
        let total_parts: usize = collected.groups.iter().map(|g| g.len()).sum();
        assert_eq!(16, total_parts);
    }

    #[test]
    fn test_bulk_memtable_ranges_with_multi_bulk_part() {
        let metadata = metadata_for_test();
        let memtable = BulkMemtable::new(
            2005,
            BulkMemtableConfig::default(),
            metadata.clone(),
            None,
            None,
            false,
            MergeMode::LastRow,
        );
        // Disable unordered_part for this test
        memtable.set_unordered_part_threshold(0);

        // Write enough bulk parts to trigger merge (DEFAULT_MERGE_THRESHOLD = 8)
        // Each part has small number of rows so total < DEFAULT_ROW_GROUP_SIZE
        // This will result in MultiBulkPart after compaction
        for i in 0..DEFAULT_MERGE_THRESHOLD {
            let part = create_bulk_part_with_converter(
                &format!("key_{}", i),
                i as u32,
                vec![1000 + i as i64 * 100, 2000 + i as i64 * 100],
                vec![Some(i as f64 * 10.0), Some(i as f64 * 10.0 + 1.0)],
                100 + i as u64,
            )
            .unwrap();
            memtable.write_bulk(part).unwrap();
        }

        // Compact to trigger MultiBulkPart creation (since total rows < DEFAULT_ROW_GROUP_SIZE)
        memtable.compact(false).unwrap();

        // Verify we can read from the memtable
        let predicate_group = PredicateGroup::new(&metadata, &[]).unwrap();
        let ranges = memtable
            .ranges(
                None,
                RangesOptions::default().with_predicate(predicate_group),
            )
            .unwrap();

        assert_eq!(1, ranges.ranges.len());
        let total_rows: usize = ranges.ranges.values().map(|r| r.stats().num_rows()).sum();
        assert_eq!(16, total_rows);

        // Read all data
        let mut total_rows_read = 0;
        for (_range_id, range) in ranges.ranges.iter() {
            assert!(range.is_record_batch());
            let record_batch_iter = range.build_record_batch_iter(None).unwrap();

            for batch_result in record_batch_iter {
                let batch = batch_result.unwrap();
                total_rows_read += batch.num_rows();
            }
        }
        assert_eq!(16, total_rows_read);
    }
}
