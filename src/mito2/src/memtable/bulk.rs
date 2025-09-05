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
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use datatypes::arrow::datatypes::SchemaRef;
use mito_codec::key_values::KeyValue;
use rayon::prelude::*;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, SequenceNumber};
use tokio::sync::Semaphore;

use crate::error::{Result, UnsupportedOperationSnafu};
use crate::flush::WriteBufferManagerRef;
use crate::memtable::bulk::context::BulkIterContext;
use crate::memtable::bulk::part::{BulkPart, BulkPartEncoder};
use crate::memtable::bulk::part_reader::BulkPartRecordBatchIter;
use crate::memtable::stats::WriteMetrics;
use crate::memtable::{
    AllocTracker, BoxedBatchIterator, BoxedRecordBatchIterator, EncodedBulkPart, EncodedRange,
    IterBuilder, KeyValues, MemScanMetrics, Memtable, MemtableBuilder, MemtableId, MemtableRange,
    MemtableRangeContext, MemtableRanges, MemtableRef, MemtableStats, PredicateGroup,
};
use crate::read::flat_merge::FlatMergeIterator;
use crate::sst::file::FileId;
use crate::sst::parquet::{DEFAULT_READ_BATCH_SIZE, DEFAULT_ROW_GROUP_SIZE};
use crate::sst::{to_flat_sst_arrow_schema, FlatSchemaOptions};

/// All parts in a bulk memtable.
#[derive(Default)]
struct BulkParts {
    /// Raw parts.
    parts: Vec<BulkPartWrapper>,
    /// Parts encoded as parquets.
    encoded_parts: Vec<EncodedPartWrapper>,
}

impl BulkParts {
    /// Total number of parts (raw + encoded).
    fn num_parts(&self) -> usize {
        self.parts.len() + self.encoded_parts.len()
    }

    /// Returns true if there is no part.
    fn is_empty(&self) -> bool {
        self.parts.is_empty() && self.encoded_parts.is_empty()
    }
}

/// Memtable that ingests and scans parts directly.
pub struct BulkMemtable {
    id: MemtableId,
    parts: Arc<RwLock<BulkParts>>,
    metadata: RegionMetadataRef,
    alloc_tracker: AllocTracker,
    max_timestamp: AtomicI64,
    min_timestamp: AtomicI64,
    max_sequence: AtomicU64,
    num_rows: AtomicUsize,
    /// Cached flat SST arrow schema for memtable compaction.
    #[allow(dead_code)]
    flat_arrow_schema: SchemaRef,
    /// Compactor for merging bulk parts
    compactor: Arc<Mutex<MemtableCompactor>>,
    /// Dispatcher for scheduling compaction tasks
    compact_dispatcher: Option<Arc<CompactDispatcher>>,
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
            min_ts: fragment.min_ts,
            max_ts: fragment.max_ts,
            num_rows: fragment.num_rows(),
            max_sequence: fragment.sequence,
        };

        {
            let mut bulk_parts = self.parts.write().unwrap();
            bulk_parts.parts.push(BulkPartWrapper {
                part: fragment,
                file_id: FileId::random(),
                merging: false,
            });

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
        _sequence: Option<SequenceNumber>,
    ) -> Result<crate::memtable::BoxedBatchIterator> {
        todo!()
    }

    fn ranges(
        &self,
        projection: Option<&[ColumnId]>,
        predicate: PredicateGroup,
        sequence: Option<SequenceNumber>,
    ) -> Result<MemtableRanges> {
        let mut ranges = BTreeMap::new();
        let mut range_id = 0;

        let context = Arc::new(BulkIterContext::new(
            self.metadata.clone(),
            &projection,
            predicate.predicate().cloned(),
        ));

        // Adds ranges for regular parts and encoded parts
        {
            let bulk_parts = self.parts.read().unwrap();

            // Adds ranges for regular parts
            for part_wrapper in bulk_parts.parts.iter() {
                // Skips empty parts
                if part_wrapper.part.num_rows() == 0 {
                    continue;
                }

                let range = MemtableRange::new(
                    Arc::new(MemtableRangeContext::new(
                        self.id,
                        Box::new(BulkRangeIterBuilder {
                            part: part_wrapper.part.clone(),
                            context: context.clone(),
                            sequence,
                        }),
                        predicate.clone(),
                    )),
                    part_wrapper.part.num_rows(),
                );
                ranges.insert(range_id, range);
                range_id += 1;
            }

            // Adds ranges for encoded parts
            for encoded_part_wrapper in bulk_parts.encoded_parts.iter() {
                // Skips empty parts
                if encoded_part_wrapper.part.metadata().num_rows == 0 {
                    continue;
                }

                let range = MemtableRange::new(
                    Arc::new(MemtableRangeContext::new(
                        self.id,
                        Box::new(EncodedBulkRangeIterBuilder {
                            file_id: encoded_part_wrapper.file_id,
                            part: encoded_part_wrapper.part.clone(),
                            context: context.clone(),
                            sequence,
                        }),
                        predicate.clone(),
                    )),
                    encoded_part_wrapper.part.metadata().num_rows,
                );
                ranges.insert(range_id, range);
                range_id += 1;
            }
        }

        let mut stats = self.stats();
        stats.num_ranges = ranges.len();

        // TODO(yingwen): Supports per range stats.
        Ok(MemtableRanges { ranges, stats })
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
            parts: Arc::new(RwLock::new(BulkParts::default())),
            metadata: metadata.clone(),
            alloc_tracker: AllocTracker::new(self.alloc_tracker.write_buffer_manager()),
            max_timestamp: AtomicI64::new(i64::MIN),
            min_timestamp: AtomicI64::new(i64::MAX),
            max_sequence: AtomicU64::new(0),
            num_rows: AtomicUsize::new(0),
            flat_arrow_schema,
            compactor: Arc::new(Mutex::new(MemtableCompactor::new(id))),
            compact_dispatcher: self.compact_dispatcher.clone(),
        })
    }

    fn compact(&self, for_flush: bool) -> Result<()> {
        let mut compactor = self.compactor.lock().unwrap();

        if for_flush {
            return Ok(());
        }

        // Try to merge regular parts first
        if MemtableCompactor::should_merge_parts(&self.parts) {
            compactor.merge_parts(&self.flat_arrow_schema, &self.parts, &self.metadata)?;
        }

        // Then try to merge encoded parts
        if MemtableCompactor::should_merge_encoded_parts(&self.parts) {
            compactor.merge_encoded_parts(&self.flat_arrow_schema, &self.parts, &self.metadata)?;
        }

        Ok(())
    }
}

impl BulkMemtable {
    /// Creates a new BulkMemtable
    pub fn new(
        id: MemtableId,
        metadata: RegionMetadataRef,
        write_buffer_manager: Option<WriteBufferManagerRef>,
        compact_dispatcher: Option<Arc<CompactDispatcher>>,
    ) -> Self {
        let flat_arrow_schema = to_flat_sst_arrow_schema(
            &metadata,
            &FlatSchemaOptions::from_encoding(metadata.primary_key_encoding),
        );

        Self {
            id,
            parts: Arc::new(RwLock::new(BulkParts::default())),
            metadata,
            alloc_tracker: AllocTracker::new(write_buffer_manager),
            max_timestamp: AtomicI64::new(i64::MIN),
            min_timestamp: AtomicI64::new(i64::MAX),
            max_sequence: AtomicU64::new(0),
            num_rows: AtomicUsize::new(0),
            flat_arrow_schema,
            compactor: Arc::new(Mutex::new(MemtableCompactor::new(id))),
            compact_dispatcher,
        }
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
            .map(|part_wrapper| part_wrapper.part.estimated_series_count())
            .sum()
    }

    /// Returns whether the memtable should be compacted.
    fn should_compact(&self) -> bool {
        MemtableCompactor::should_merge_parts(&self.parts)
            || MemtableCompactor::should_merge_encoded_parts(&self.parts)
    }

    /// Schedules a compaction task using the CompactDispatcher.
    fn schedule_compact(&self) {
        if let Some(dispatcher) = &self.compact_dispatcher {
            let task = MemCompactTask {
                metadata: self.metadata.clone(),
                parts: self.parts.clone(),
                flat_arrow_schema: self.flat_arrow_schema.clone(),
                compactor: self.compactor.clone(),
            };

            dispatcher.dispatch_compact(task);
        } else {
            // Fallback to synchronous compaction if no dispatcher is available
            if let Err(e) = self.compact(false) {
                common_telemetry::error!(e; "Failed to compact table");
            }
        }
    }
}

/// Iterator builder for bulk range
struct BulkRangeIterBuilder {
    part: BulkPart,
    context: Arc<BulkIterContext>,
    sequence: Option<SequenceNumber>,
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
        _metrics: Option<MemScanMetrics>,
    ) -> Result<BoxedRecordBatchIterator> {
        let iter = BulkPartRecordBatchIter::new(
            self.part.batch.clone(),
            self.context.clone(),
            self.sequence,
        );

        Ok(Box::new(iter))
    }

    fn encoded_range(&self) -> Option<EncodedRange> {
        None
    }
}

/// Iterator builder for encoded bulk range
struct EncodedBulkRangeIterBuilder {
    #[allow(dead_code)]
    file_id: FileId,
    part: EncodedBulkPart,
    context: Arc<BulkIterContext>,
    sequence: Option<SequenceNumber>,
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
        _metrics: Option<MemScanMetrics>,
    ) -> Result<BoxedRecordBatchIterator> {
        if let Some(iter) = self.part.read(self.context.clone(), self.sequence)? {
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
    part: BulkPart,
    /// The unique file id for this part in memtable.
    #[allow(dead_code)]
    file_id: FileId,
    /// Whether this part is currently being merged.
    merging: bool,
}

struct EncodedPartWrapper {
    part: EncodedBulkPart,
    /// The unique file id for this part in memtable.
    #[allow(dead_code)]
    file_id: FileId,
    /// Whether this part is currently being merged.
    merging: bool,
}

struct MemtableCompactor {
    memtable_id: MemtableId,
}

impl MemtableCompactor {
    /// Creates a new MemtableCompactor.
    fn new(memtable_id: MemtableId) -> Self {
        Self { memtable_id }
    }

    /// Returns true if the bulk parts should be merged.
    fn should_merge_parts(bulk_parts: &RwLock<BulkParts>) -> bool {
        let parts = bulk_parts.read().unwrap();
        let unmerged_count = parts
            .parts
            .iter()
            .filter(|wrapper| !wrapper.merging)
            .count();
        // If the total number of unmerged parts is >= 8, start a merge task.
        unmerged_count >= 8
    }

    /// Returns true if the encoded parts should be merged.
    fn should_merge_encoded_parts(bulk_parts: &RwLock<BulkParts>) -> bool {
        let parts = bulk_parts.read().unwrap();
        let unmerged_count = parts
            .encoded_parts
            .iter()
            .filter(|wrapper| !wrapper.merging)
            .count();
        // If the total number of unmerged encoded parts is >= 8, start a merge task.
        unmerged_count >= 8
    }

    /// Merges bulk parts and then encodes the result to an [EncodedBulkPart].
    fn merge_parts(
        &mut self,
        arrow_schema: &SchemaRef,
        bulk_parts: &RwLock<BulkParts>,
        metadata: &RegionMetadataRef,
    ) -> Result<()> {
        let start = Instant::now();

        // Collect unmerged parts and mark them as being merged
        let (parts_to_merge, merged_file_ids): (Vec<BulkPart>, HashSet<FileId>) = {
            let mut parts = bulk_parts.write().unwrap();
            let mut collected_parts = Vec::new();
            let mut collected_file_ids = HashSet::new();

            for wrapper in &mut parts.parts {
                if !wrapper.merging {
                    wrapper.merging = true;
                    collected_parts.push(wrapper.part.clone());
                    collected_file_ids.insert(wrapper.file_id);
                }
            }
            (collected_parts, collected_file_ids)
        };

        if parts_to_merge.is_empty() {
            return Ok(());
        }

        // Sort parts by row count (ascending) using unstable sort for better performance
        let mut sorted_parts = parts_to_merge;
        sorted_parts.sort_unstable_by_key(|part| part.num_rows());

        // Group parts into chunks of 16 for concurrent processing
        let part_groups: Vec<Vec<BulkPart>> = sorted_parts
            .chunks(16)
            .map(|chunk| chunk.to_vec())
            .collect();

        let total_groups = part_groups.len();
        let total_parts_to_merge: usize = part_groups.iter().map(|group| group.len()).sum();

        // Process each group concurrently using rayon - fail if any task fails
        let merged_parts = part_groups
            .into_par_iter()
            .map(|group| Self::merge_bulk_parts_group(group, arrow_schema, metadata))
            .collect::<Result<Vec<Option<EncodedBulkPart>>>>()?;

        // Collect merged parts and remove original parts
        let mut total_output_rows = 0;
        {
            let mut parts = bulk_parts.write().unwrap();
            for encoded_part_opt in merged_parts {
                if let Some(encoded_part) = encoded_part_opt {
                    total_output_rows += encoded_part.metadata().num_rows;
                    parts.encoded_parts.push(EncodedPartWrapper {
                        part: encoded_part,
                        file_id: FileId::random(),
                        merging: false,
                    });
                }
            }
            // Remove parts that were merged by checking file IDs
            parts
                .parts
                .retain(|wrapper| !merged_file_ids.contains(&wrapper.file_id));
        }

        common_telemetry::info!(
            "BulkMemtable {} concurrent compact {} groups, {} parts -> {} rows, cost: {:?}",
            self.memtable_id,
            total_groups,
            total_parts_to_merge,
            total_output_rows,
            start.elapsed()
        );

        Ok(())
    }

    /// Merges encoded parts and then encodes the result to an [EncodedBulkPart].
    fn merge_encoded_parts(
        &mut self,
        arrow_schema: &SchemaRef,
        bulk_parts: &RwLock<BulkParts>,
        metadata: &RegionMetadataRef,
    ) -> Result<()> {
        let start = Instant::now();

        // Find min size and collect unmerged encoded parts within size threshold
        let (parts_to_merge, merged_file_ids): (Vec<EncodedBulkPart>, HashSet<FileId>) = {
            let mut parts = bulk_parts.write().unwrap();

            // Find minimum size among unmerged parts
            let min_size = parts
                .encoded_parts
                .iter()
                .filter(|wrapper| !wrapper.merging)
                .map(|wrapper| wrapper.part.size_bytes())
                .min();

            let Some(min_size) = min_size else {
                return Ok(());
            };

            let max_allowed_size = min_size.saturating_mul(16).min(4 * 1024 * 1024);
            let mut collected_parts = Vec::new();
            let mut collected_file_ids = HashSet::new();

            for wrapper in &mut parts.encoded_parts {
                if !wrapper.merging {
                    let size = wrapper.part.size_bytes();
                    if size <= max_allowed_size {
                        wrapper.merging = true;
                        collected_parts.push(wrapper.part.clone());
                        collected_file_ids.insert(wrapper.file_id);
                    }
                }
            }
            (collected_parts, collected_file_ids)
        };

        if parts_to_merge.is_empty() {
            return Ok(());
        }

        if parts_to_merge.len() == 1 {
            // Only 1 part, don't have to merge.
            let mut parts = bulk_parts.write().unwrap();
            for wrapper in &mut parts.encoded_parts {
                if merged_file_ids.contains(&wrapper.file_id) {
                    wrapper.merging = false;
                }
            }
            return Ok(());
        }

        // Group parts into chunks of 16 for concurrent processing
        let part_groups: Vec<Vec<_>> = parts_to_merge
            .chunks(16)
            .map(|chunk| chunk.to_vec())
            .collect();

        let total_groups = part_groups.len();
        let total_parts_to_merge: usize = part_groups.iter().map(|group| group.len()).sum();

        // Process each group concurrently using rayon - fail if any task fails
        let merged_parts = part_groups
            .into_par_iter()
            .map(|group| Self::merge_parts_group(group, arrow_schema, metadata))
            .collect::<Result<Vec<Option<EncodedBulkPart>>>>()?;

        // Collect merged parts and remove original parts
        let mut total_output_rows = 0;
        {
            let mut parts = bulk_parts.write().unwrap();
            for encoded_part_opt in merged_parts {
                if let Some(encoded_part) = encoded_part_opt {
                    total_output_rows += encoded_part.metadata().num_rows;
                    parts.encoded_parts.push(EncodedPartWrapper {
                        part: encoded_part,
                        file_id: FileId::random(),
                        merging: false,
                    });
                }
            }
            // Remove parts that were merged by checking file IDs
            parts
                .encoded_parts
                .retain(|wrapper| !merged_file_ids.contains(&wrapper.file_id));
        }

        common_telemetry::info!(
            "BulkMemtable {} concurrent compact {} groups, {} encoded parts -> {} rows, cost: {:?}",
            self.memtable_id,
            total_groups,
            total_parts_to_merge,
            total_output_rows,
            start.elapsed()
        );

        Ok(())
    }

    /// Merges a group of encoded parts into a single encoded part
    fn merge_parts_group(
        parts_to_merge: Vec<EncodedBulkPart>,
        arrow_schema: &SchemaRef,
        metadata: &RegionMetadataRef,
    ) -> Result<Option<EncodedBulkPart>> {
        if parts_to_merge.is_empty() {
            return Ok(None);
        }

        // Calculate timestamp bounds for merged data
        let min_timestamp = parts_to_merge
            .iter()
            .map(|p| p.metadata().min_timestamp)
            .min()
            .unwrap_or(i64::MAX);
        let max_timestamp = parts_to_merge
            .iter()
            .map(|p| p.metadata().max_timestamp)
            .max()
            .unwrap_or(i64::MIN);

        // Create context for reading parts
        let context = Arc::new(BulkIterContext::new(
            metadata.clone(),
            &None, // No column projection for merging
            None,  // No predicate for merging
        ));

        // Create iterators for all parts to merge
        let iterators: Vec<BoxedRecordBatchIterator> = parts_to_merge
            .into_iter()
            .filter_map(|part| part.read(context.clone(), None).ok().flatten())
            .collect();

        if iterators.is_empty() {
            return Ok(None);
        }

        // Merge the iterators
        // TODO(yingwen): Dedup.
        let merged_iter =
            FlatMergeIterator::new(arrow_schema.clone(), iterators, DEFAULT_READ_BATCH_SIZE)?;
        let boxed_iter: BoxedRecordBatchIterator = Box::new(merged_iter);

        // Encode the merged iterator
        let encoder = BulkPartEncoder::new(metadata.clone(), DEFAULT_ROW_GROUP_SIZE)?;
        let encoded_part = encoder.encode_record_batch_iter(
            boxed_iter,
            arrow_schema.clone(),
            min_timestamp,
            max_timestamp,
        )?;

        Ok(encoded_part)
    }

    /// Merges a group of bulk parts into a single encoded part
    fn merge_bulk_parts_group(
        parts_to_merge: Vec<BulkPart>,
        arrow_schema: &SchemaRef,
        metadata: &RegionMetadataRef,
    ) -> Result<Option<EncodedBulkPart>> {
        if parts_to_merge.is_empty() {
            return Ok(None);
        }

        // Calculate timestamp bounds for merged data
        let min_timestamp = parts_to_merge
            .iter()
            .map(|p| p.min_ts)
            .min()
            .unwrap_or(i64::MAX);
        let max_timestamp = parts_to_merge
            .iter()
            .map(|p| p.max_ts)
            .max()
            .unwrap_or(i64::MIN);

        // Create context for reading parts
        let context = Arc::new(BulkIterContext::new(
            metadata.clone(),
            &None, // No column projection for merging
            None,  // No predicate for merging
        ));

        // Create iterators for all parts to merge
        let iterators: Vec<BoxedRecordBatchIterator> = parts_to_merge
            .into_iter()
            .map(|part| {
                let iter = BulkPartRecordBatchIter::new(
                    part.batch,
                    context.clone(),
                    None, // No sequence filter for merging
                );
                Box::new(iter) as BoxedRecordBatchIterator
            })
            .collect();

        if iterators.is_empty() {
            return Ok(None);
        }

        // Merge the iterators
        let merged_iter =
            FlatMergeIterator::new(arrow_schema.clone(), iterators, DEFAULT_READ_BATCH_SIZE)?;
        let boxed_iter: BoxedRecordBatchIterator = Box::new(merged_iter);

        // Encode the merged iterator
        let encoder = BulkPartEncoder::new(metadata.clone(), DEFAULT_ROW_GROUP_SIZE)?;
        let encoded_part = encoder.encode_record_batch_iter(
            boxed_iter,
            arrow_schema.clone(),
            min_timestamp,
            max_timestamp,
        )?;

        Ok(encoded_part)
    }
}

struct MemCompactTask {
    metadata: RegionMetadataRef,
    parts: Arc<RwLock<BulkParts>>,

    /// Cached flat SST arrow schema
    flat_arrow_schema: SchemaRef,
    /// Compactor for merging bulk parts
    compactor: Arc<Mutex<MemtableCompactor>>,
}

impl MemCompactTask {
    fn compact(&self) -> Result<()> {
        let mut compactor = self.compactor.lock().unwrap();

        // Try to merge regular parts first
        if MemtableCompactor::should_merge_parts(&self.parts) {
            compactor.merge_parts(&self.flat_arrow_schema, &self.parts, &self.metadata)?;
        }

        // Then try to merge encoded parts
        if MemtableCompactor::should_merge_encoded_parts(&self.parts) {
            compactor.merge_encoded_parts(&self.flat_arrow_schema, &self.parts, &self.metadata)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct CompactDispatcher {
    semaphore: Arc<Semaphore>,
}

impl CompactDispatcher {
    pub fn new(permits: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(permits)),
        }
    }

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
    write_buffer_manager: Option<WriteBufferManagerRef>,
    compact_dispatcher: Option<Arc<CompactDispatcher>>,
}

impl BulkMemtableBuilder {
    /// Creates a new builder with specific `write_buffer_manager`.
    pub fn new(write_buffer_manager: Option<WriteBufferManagerRef>) -> Self {
        Self {
            write_buffer_manager,
            compact_dispatcher: None,
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
            metadata.clone(),
            self.write_buffer_manager.clone(),
            self.compact_dispatcher.clone(),
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
        let memtable = BulkMemtable::new(999, metadata.clone(), None, None);

        let test_data = vec![
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

        let predicate_group = PredicateGroup::new(&metadata, &[]);
        let ranges = memtable.ranges(None, predicate_group, None).unwrap();

        assert_eq!(3, ranges.ranges.len());
        assert_eq!(5, ranges.stats.num_rows);

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
        let memtable = BulkMemtable::new(111, metadata.clone(), None, None);

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
        let predicate_group = PredicateGroup::new(&metadata, &[]);
        let ranges = memtable
            .ranges(Some(&projection), predicate_group, None)
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
        let memtable = BulkMemtable::new(111, metadata.clone(), None, None);

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
        let memtable = BulkMemtable::new(222, metadata.clone(), None, None);

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
        let original_memtable = BulkMemtable::new(333, metadata.clone(), None, None);

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
}
