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

//! Memtables are write buffers for regions.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub use bulk::part::EncodedBulkPart;
use bytes::Bytes;
use common_time::Timestamp;
use datatypes::arrow::record_batch::RecordBatch;
use mito_codec::key_values::KeyValue;
pub use mito_codec::key_values::KeyValues;
use mito_codec::row_converter::{PrimaryKeyCodec, build_primary_key_codec};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, SequenceNumber, SequenceRange};

use crate::config::MitoConfig;
use crate::error::{Result, UnsupportedOperationSnafu};
use crate::flush::WriteBufferManagerRef;
use crate::memtable::bulk::{BulkMemtableBuilder, CompactDispatcher};
use crate::memtable::partition_tree::{PartitionTreeConfig, PartitionTreeMemtableBuilder};
use crate::memtable::time_series::TimeSeriesMemtableBuilder;
use crate::metrics::WRITE_BUFFER_BYTES;
use crate::read::Batch;
use crate::read::batch_adapter::BatchToRecordBatchAdapter;
use crate::read::prune::PruneTimeIterator;
use crate::read::scan_region::PredicateGroup;
use crate::region::options::{MemtableOptions, MergeMode, RegionOptions};
use crate::sst::FormatType;
use crate::sst::file::FileTimeRange;
use crate::sst::parquet::SstInfo;
use crate::sst::parquet::file_range::PreFilterMode;

mod builder;
pub mod bulk;
pub mod partition_tree;
pub mod simple_bulk_memtable;
mod stats;
pub mod time_partition;
pub mod time_series;
pub(crate) mod version;

pub use bulk::part::{
    BulkPart, BulkPartEncoder, BulkPartMeta, UnorderedPart, record_batch_estimated_size,
    sort_primary_key_record_batch,
};
#[cfg(any(test, feature = "test"))]
pub use time_partition::filter_record_batch;

/// Id for memtables.
///
/// Should be unique under the same region.
pub type MemtableId = u32;

/// Config for memtables.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MemtableConfig {
    PartitionTree(PartitionTreeConfig),
    #[default]
    TimeSeries,
}

/// Options for querying ranges from a memtable.
#[derive(Clone)]
pub struct RangesOptions {
    /// Whether the ranges are being queried for flush.
    pub for_flush: bool,
    /// Mode to pre-filter columns in ranges.
    pub pre_filter_mode: PreFilterMode,
    /// Predicate to filter the data.
    pub predicate: PredicateGroup,
    /// Sequence range to filter the data.
    pub sequence: Option<SequenceRange>,
}

impl Default for RangesOptions {
    fn default() -> Self {
        Self {
            for_flush: false,
            pre_filter_mode: PreFilterMode::All,
            predicate: PredicateGroup::default(),
            sequence: None,
        }
    }
}

impl RangesOptions {
    /// Creates a new [RangesOptions] for flushing.
    pub fn for_flush() -> Self {
        Self {
            for_flush: true,
            pre_filter_mode: PreFilterMode::All,
            predicate: PredicateGroup::default(),
            sequence: None,
        }
    }

    /// Sets the pre-filter mode.
    #[must_use]
    pub fn with_pre_filter_mode(mut self, pre_filter_mode: PreFilterMode) -> Self {
        self.pre_filter_mode = pre_filter_mode;
        self
    }

    /// Sets the predicate.
    #[must_use]
    pub fn with_predicate(mut self, predicate: PredicateGroup) -> Self {
        self.predicate = predicate;
        self
    }

    /// Sets the sequence range.
    #[must_use]
    pub fn with_sequence(mut self, sequence: Option<SequenceRange>) -> Self {
        self.sequence = sequence;
        self
    }
}

#[derive(Debug, Default, Clone)]
pub struct MemtableStats {
    /// The estimated bytes allocated by this memtable from heap.
    pub estimated_bytes: usize,
    /// The inclusive time range that this memtable contains. It is None if
    /// and only if the memtable is empty.
    pub time_range: Option<(Timestamp, Timestamp)>,
    /// Total rows in memtable
    pub num_rows: usize,
    /// Total number of ranges in the memtable.
    pub num_ranges: usize,
    /// The maximum sequence number in the memtable.
    pub max_sequence: SequenceNumber,
    /// Number of estimated timeseries in memtable.
    pub series_count: usize,
}

impl MemtableStats {
    /// Attaches the time range to the stats.
    #[cfg(any(test, feature = "test"))]
    pub fn with_time_range(mut self, time_range: Option<(Timestamp, Timestamp)>) -> Self {
        self.time_range = time_range;
        self
    }

    #[cfg(feature = "test")]
    pub fn with_max_sequence(mut self, max_sequence: SequenceNumber) -> Self {
        self.max_sequence = max_sequence;
        self
    }

    /// Returns the estimated bytes allocated by this memtable.
    pub fn bytes_allocated(&self) -> usize {
        self.estimated_bytes
    }

    /// Returns the time range of the memtable.
    pub fn time_range(&self) -> Option<(Timestamp, Timestamp)> {
        self.time_range
    }

    /// Returns the num of total rows in memtable.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Returns the number of ranges in the memtable.
    pub fn num_ranges(&self) -> usize {
        self.num_ranges
    }

    /// Returns the maximum sequence number in the memtable.
    pub fn max_sequence(&self) -> SequenceNumber {
        self.max_sequence
    }

    /// Series count in memtable.
    pub fn series_count(&self) -> usize {
        self.series_count
    }
}

pub type BoxedBatchIterator = Box<dyn Iterator<Item = Result<Batch>> + Send>;

pub type BoxedRecordBatchIterator = Box<dyn Iterator<Item = Result<RecordBatch>> + Send>;

/// Ranges in a memtable.
#[derive(Default)]
pub struct MemtableRanges {
    /// Range IDs and ranges.
    pub ranges: BTreeMap<usize, MemtableRange>,
}

impl MemtableRanges {
    /// Returns the total number of rows across all ranges.
    pub fn num_rows(&self) -> usize {
        self.ranges.values().map(|r| r.stats().num_rows()).sum()
    }

    /// Returns the total series count across all ranges.
    pub fn series_count(&self) -> usize {
        self.ranges.values().map(|r| r.stats().series_count()).sum()
    }

    /// Returns the maximum sequence number across all ranges.
    pub fn max_sequence(&self) -> SequenceNumber {
        self.ranges
            .values()
            .map(|r| r.stats().max_sequence())
            .max()
            .unwrap_or(0)
    }
}

impl IterBuilder for MemtableRanges {
    fn build(&self, _metrics: Option<MemScanMetrics>) -> Result<BoxedBatchIterator> {
        ensure!(
            self.ranges.len() == 1,
            UnsupportedOperationSnafu {
                err_msg: format!(
                    "Building an iterator from MemtableRanges expects 1 range, but got {}",
                    self.ranges.len()
                ),
            }
        );

        self.ranges.values().next().unwrap().build_iter()
    }

    fn is_record_batch(&self) -> bool {
        self.ranges.values().all(|range| range.is_record_batch())
    }
}

/// In memory write buffer.
pub trait Memtable: Send + Sync + fmt::Debug {
    /// Returns the id of this memtable.
    fn id(&self) -> MemtableId;

    /// Writes key values into the memtable.
    fn write(&self, kvs: &KeyValues) -> Result<()>;

    /// Writes one key value pair into the memtable.
    fn write_one(&self, key_value: KeyValue) -> Result<()>;

    /// Writes an encoded batch of into memtable.
    fn write_bulk(&self, part: crate::memtable::bulk::part::BulkPart) -> Result<()>;

    /// Returns the ranges in the memtable.
    ///
    /// The returned map contains the range id and the range after applying the predicate.
    fn ranges(
        &self,
        projection: Option<&[ColumnId]>,
        options: RangesOptions,
    ) -> Result<MemtableRanges>;

    /// Returns true if the memtable is empty.
    fn is_empty(&self) -> bool;

    /// Turns a mutable memtable into an immutable memtable.
    fn freeze(&self) -> Result<()>;

    /// Returns the [MemtableStats] info of Memtable.
    fn stats(&self) -> MemtableStats;

    /// Forks this (immutable) memtable and returns a new mutable memtable with specific memtable `id`.
    ///
    /// A region must freeze the memtable before invoking this method.
    fn fork(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef;

    /// Compacts the memtable.
    ///
    /// The `for_flush` is true when the flush job calls this method.
    fn compact(&self, for_flush: bool) -> Result<()> {
        let _ = for_flush;
        Ok(())
    }
}

pub type MemtableRef = Arc<dyn Memtable>;

/// Builder to build a new [Memtable].
pub trait MemtableBuilder: Send + Sync + fmt::Debug {
    /// Builds a new memtable instance.
    fn build(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef;

    /// Returns true if the memtable supports bulk insert and benefits from it.
    fn use_bulk_insert(&self, metadata: &RegionMetadataRef) -> bool {
        let _metadata = metadata;
        false
    }
}

pub type MemtableBuilderRef = Arc<dyn MemtableBuilder>;

/// Memtable memory allocation tracker.
#[derive(Default)]
pub struct AllocTracker {
    write_buffer_manager: Option<WriteBufferManagerRef>,
    /// Bytes allocated by the tracker.
    bytes_allocated: AtomicUsize,
    /// Whether allocating is done.
    is_done_allocating: AtomicBool,
}

impl fmt::Debug for AllocTracker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AllocTracker")
            .field("bytes_allocated", &self.bytes_allocated)
            .field("is_done_allocating", &self.is_done_allocating)
            .finish()
    }
}

impl AllocTracker {
    /// Returns a new [AllocTracker].
    pub fn new(write_buffer_manager: Option<WriteBufferManagerRef>) -> AllocTracker {
        AllocTracker {
            write_buffer_manager,
            bytes_allocated: AtomicUsize::new(0),
            is_done_allocating: AtomicBool::new(false),
        }
    }

    /// Tracks `bytes` memory is allocated.
    pub(crate) fn on_allocation(&self, bytes: usize) {
        self.bytes_allocated.fetch_add(bytes, Ordering::Relaxed);
        WRITE_BUFFER_BYTES.add(bytes as i64);
        if let Some(write_buffer_manager) = &self.write_buffer_manager {
            write_buffer_manager.reserve_mem(bytes);
        }
    }

    /// Marks we have finished allocating memory so we can free it from
    /// the write buffer's limit.
    ///
    /// The region MUST ensure that it calls this method inside the region writer's write lock.
    pub(crate) fn done_allocating(&self) {
        if let Some(write_buffer_manager) = &self.write_buffer_manager
            && self
                .is_done_allocating
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            write_buffer_manager.schedule_free_mem(self.bytes_allocated.load(Ordering::Relaxed));
        }
    }

    /// Returns bytes allocated.
    pub(crate) fn bytes_allocated(&self) -> usize {
        self.bytes_allocated.load(Ordering::Relaxed)
    }

    /// Returns the write buffer manager.
    pub(crate) fn write_buffer_manager(&self) -> Option<WriteBufferManagerRef> {
        self.write_buffer_manager.clone()
    }
}

impl Drop for AllocTracker {
    fn drop(&mut self) {
        if !self.is_done_allocating.load(Ordering::Relaxed) {
            self.done_allocating();
        }

        let bytes_allocated = self.bytes_allocated.load(Ordering::Relaxed);
        WRITE_BUFFER_BYTES.sub(bytes_allocated as i64);

        // Memory tracked by this tracker is freed.
        if let Some(write_buffer_manager) = &self.write_buffer_manager {
            write_buffer_manager.free_mem(bytes_allocated);
        }
    }
}

/// Provider of memtable builders for regions.
#[derive(Clone)]
pub(crate) struct MemtableBuilderProvider {
    write_buffer_manager: Option<WriteBufferManagerRef>,
    config: Arc<MitoConfig>,
    compact_dispatcher: Arc<CompactDispatcher>,
}

impl MemtableBuilderProvider {
    pub(crate) fn new(
        write_buffer_manager: Option<WriteBufferManagerRef>,
        config: Arc<MitoConfig>,
    ) -> Self {
        let compact_dispatcher =
            Arc::new(CompactDispatcher::new(config.max_background_compactions));

        Self {
            write_buffer_manager,
            config,
            compact_dispatcher,
        }
    }

    pub(crate) fn builder_for_options(&self, options: &RegionOptions) -> MemtableBuilderRef {
        let dedup = options.need_dedup();
        let merge_mode = options.merge_mode();
        let flat_format = options
            .sst_format
            .map(|format| format == FormatType::Flat)
            .unwrap_or(self.config.default_experimental_flat_format);
        if flat_format {
            if options.memtable.is_some() {
                common_telemetry::info!(
                    "Overriding memtable config, use BulkMemtable under flat format"
                );
            }

            return Arc::new(
                BulkMemtableBuilder::new(
                    self.write_buffer_manager.clone(),
                    !dedup, // append_mode: true if not dedup, false if dedup
                    merge_mode,
                )
                .with_compact_dispatcher(self.compact_dispatcher.clone()),
            );
        }

        // The format is not flat.
        match &options.memtable {
            Some(MemtableOptions::TimeSeries) => Arc::new(TimeSeriesMemtableBuilder::new(
                self.write_buffer_manager.clone(),
                dedup,
                merge_mode,
            )),
            Some(MemtableOptions::PartitionTree(opts)) => {
                Arc::new(PartitionTreeMemtableBuilder::new(
                    PartitionTreeConfig {
                        index_max_keys_per_shard: opts.index_max_keys_per_shard,
                        data_freeze_threshold: opts.data_freeze_threshold,
                        fork_dictionary_bytes: opts.fork_dictionary_bytes,
                        dedup,
                        merge_mode,
                    },
                    self.write_buffer_manager.clone(),
                ))
            }
            None => self.default_primary_key_memtable_builder(dedup, merge_mode),
        }
    }

    fn default_primary_key_memtable_builder(
        &self,
        dedup: bool,
        merge_mode: MergeMode,
    ) -> MemtableBuilderRef {
        match &self.config.memtable {
            MemtableConfig::PartitionTree(config) => {
                let mut config = config.clone();
                config.dedup = dedup;
                Arc::new(PartitionTreeMemtableBuilder::new(
                    config,
                    self.write_buffer_manager.clone(),
                ))
            }
            MemtableConfig::TimeSeries => Arc::new(TimeSeriesMemtableBuilder::new(
                self.write_buffer_manager.clone(),
                dedup,
                merge_mode,
            )),
        }
    }
}

/// Metrics for scanning a memtable.
#[derive(Clone, Default)]
pub struct MemScanMetrics(Arc<Mutex<MemScanMetricsData>>);

impl MemScanMetrics {
    /// Merges the metrics.
    pub(crate) fn merge_inner(&self, inner: &MemScanMetricsData) {
        let mut metrics = self.0.lock().unwrap();
        metrics.total_series += inner.total_series;
        metrics.num_rows += inner.num_rows;
        metrics.num_batches += inner.num_batches;
        metrics.scan_cost += inner.scan_cost;
    }

    /// Gets the metrics data.
    pub(crate) fn data(&self) -> MemScanMetricsData {
        self.0.lock().unwrap().clone()
    }
}

#[derive(Clone, Default)]
pub(crate) struct MemScanMetricsData {
    /// Total series in the memtable.
    pub(crate) total_series: usize,
    /// Number of rows read.
    pub(crate) num_rows: usize,
    /// Number of batch read.
    pub(crate) num_batches: usize,
    /// Duration to scan the memtable.
    pub(crate) scan_cost: Duration,
}

/// Encoded range in the memtable.
pub struct EncodedRange {
    /// Encoded file data.
    pub data: Bytes,
    /// Metadata of the encoded range.
    pub sst_info: SstInfo,
}

/// Builder to build an iterator to read the range.
/// The builder should know the projection and the predicate to build the iterator.
pub trait IterBuilder: Send + Sync {
    /// Returns the iterator to read the range.
    fn build(&self, metrics: Option<MemScanMetrics>) -> Result<BoxedBatchIterator>;

    /// Returns whether the iterator is a record batch iterator.
    fn is_record_batch(&self) -> bool {
        false
    }

    /// Returns the record batch iterator to read the range.
    /// ## Note
    /// Implementations should ensure the iterator yields data within given time range.
    fn build_record_batch(
        &self,
        time_range: Option<(Timestamp, Timestamp)>,
        metrics: Option<MemScanMetrics>,
    ) -> Result<BoxedRecordBatchIterator> {
        let _metrics = metrics;
        let _ = time_range;
        UnsupportedOperationSnafu {
            err_msg: "Record batch iterator is not supported by this memtable",
        }
        .fail()
    }

    /// Returns the [EncodedRange] if the range is already encoded into SST.
    fn encoded_range(&self) -> Option<EncodedRange> {
        None
    }
}

pub type BoxedIterBuilder = Box<dyn IterBuilder>;

/// Computes the column IDs to read based on the projection.
///
/// If `projection` is `Some`, returns those column IDs. If `None`, returns all column IDs
/// from the metadata.
pub fn read_column_ids_from_projection(
    metadata: &RegionMetadataRef,
    projection: Option<&[ColumnId]>,
) -> Vec<ColumnId> {
    if let Some(projection) = projection {
        projection.to_vec()
    } else {
        metadata
            .column_metadatas
            .iter()
            .map(|c| c.column_id)
            .collect()
    }
}

/// Context to adapt batch iterators to record batch iterators for flat scan.
pub struct BatchToRecordBatchContext {
    metadata: RegionMetadataRef,
    codec: Arc<dyn PrimaryKeyCodec>,
    read_column_ids: Vec<ColumnId>,
}

impl BatchToRecordBatchContext {
    /// Creates a new context for adapting batch iterators.
    pub fn new(metadata: RegionMetadataRef, mut read_column_ids: Vec<ColumnId>) -> Self {
        if read_column_ids.is_empty() {
            read_column_ids.push(metadata.time_index_column().column_id);
        }

        let codec = build_primary_key_codec(&metadata);
        Self {
            metadata,
            codec,
            read_column_ids,
        }
    }

    fn adapt_iter(&self, iter: BoxedBatchIterator) -> BoxedRecordBatchIterator {
        Box::new(BatchToRecordBatchAdapter::new(
            iter,
            self.metadata.clone(),
            self.codec.clone(),
            &self.read_column_ids,
        ))
    }
}

/// Context shared by ranges of the same memtable.
pub struct MemtableRangeContext {
    /// Id of the memtable.
    id: MemtableId,
    /// Iterator builder.
    builder: BoxedIterBuilder,
    /// All filters.
    predicate: PredicateGroup,
    /// Optional context to adapt batch iterators for flat scans.
    batch_to_record_batch: Option<Arc<BatchToRecordBatchContext>>,
}

pub type MemtableRangeContextRef = Arc<MemtableRangeContext>;

impl MemtableRangeContext {
    /// Creates a new [MemtableRangeContext].
    pub fn new(id: MemtableId, builder: BoxedIterBuilder, predicate: PredicateGroup) -> Self {
        Self::new_with_batch_to_record_batch(id, builder, predicate, None)
    }

    /// Creates a new [MemtableRangeContext] with optional adapter context.
    pub fn new_with_batch_to_record_batch(
        id: MemtableId,
        builder: BoxedIterBuilder,
        predicate: PredicateGroup,
        batch_to_record_batch: Option<Arc<BatchToRecordBatchContext>>,
    ) -> Self {
        Self {
            id,
            builder,
            predicate,
            batch_to_record_batch,
        }
    }
}

/// A range in the memtable.
#[derive(Clone)]
pub struct MemtableRange {
    /// Shared context.
    context: MemtableRangeContextRef,
    /// Statistics for this memtable range.
    stats: MemtableStats,
}

impl MemtableRange {
    /// Creates a new range from context and stats.
    pub fn new(context: MemtableRangeContextRef, stats: MemtableStats) -> Self {
        Self { context, stats }
    }

    /// Returns the statistics for this range.
    pub fn stats(&self) -> &MemtableStats {
        &self.stats
    }

    /// Returns the id of the memtable to read.
    pub fn id(&self) -> MemtableId {
        self.context.id
    }

    /// Builds an iterator to read the range.
    /// Filters the result by the specific time range, this ensures memtable won't return
    /// rows out of the time range when new rows are inserted.
    pub fn build_prune_iter(
        &self,
        time_range: FileTimeRange,
        metrics: Option<MemScanMetrics>,
    ) -> Result<BoxedBatchIterator> {
        let iter = self.context.builder.build(metrics)?;
        let time_filters = self.context.predicate.time_filters();
        Ok(Box::new(PruneTimeIterator::new(
            iter,
            time_range,
            time_filters,
        )))
    }

    /// Builds an iterator to read all rows in range.
    pub fn build_iter(&self) -> Result<BoxedBatchIterator> {
        self.context.builder.build(None)
    }

    /// Builds a record batch iterator to read rows in range.
    ///
    /// For mutable memtables (adapter path), applies time-range pruning to ensure rows
    /// outside the time range are filtered, matching the behavior of `build_prune_iter`.
    pub fn build_record_batch_iter(
        &self,
        time_range: Option<FileTimeRange>,
        metrics: Option<MemScanMetrics>,
    ) -> Result<BoxedRecordBatchIterator> {
        if self.context.builder.is_record_batch() {
            return self.context.builder.build_record_batch(time_range, metrics);
        }

        if let Some(context) = self.context.batch_to_record_batch.as_ref() {
            let iter = self.context.builder.build(metrics)?;
            let iter: BoxedBatchIterator = if let Some(time_range) = time_range {
                let time_filters = self.context.predicate.time_filters();
                Box::new(PruneTimeIterator::new(iter, time_range, time_filters))
            } else {
                iter
            };
            return Ok(context.adapt_iter(iter));
        }

        UnsupportedOperationSnafu {
            err_msg: "Record batch iterator is not supported by this memtable",
        }
        .fail()
    }

    /// Returns whether the iterator is a record batch iterator.
    pub fn is_record_batch(&self) -> bool {
        self.context.builder.is_record_batch()
    }

    pub fn num_rows(&self) -> usize {
        self.stats.num_rows
    }

    /// Returns the encoded range if available.
    pub fn encoded(&self) -> Option<EncodedRange> {
        self.context.builder.encoded_range()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_base::readable_size::ReadableSize;

    use super::*;
    use crate::flush::{WriteBufferManager, WriteBufferManagerImpl};

    #[test]
    fn test_deserialize_memtable_config() {
        let s = r#"
type = "partition_tree"
index_max_keys_per_shard = 8192
data_freeze_threshold = 1024
dedup = true
fork_dictionary_bytes = "512MiB"
"#;
        let config: MemtableConfig = toml::from_str(s).unwrap();
        let MemtableConfig::PartitionTree(memtable_config) = config else {
            unreachable!()
        };
        assert!(memtable_config.dedup);
        assert_eq!(8192, memtable_config.index_max_keys_per_shard);
        assert_eq!(1024, memtable_config.data_freeze_threshold);
        assert_eq!(ReadableSize::mb(512), memtable_config.fork_dictionary_bytes);
    }

    #[test]
    fn test_alloc_tracker_without_manager() {
        let tracker = AllocTracker::new(None);
        assert_eq!(0, tracker.bytes_allocated());
        tracker.on_allocation(100);
        assert_eq!(100, tracker.bytes_allocated());
        tracker.on_allocation(200);
        assert_eq!(300, tracker.bytes_allocated());

        tracker.done_allocating();
        assert_eq!(300, tracker.bytes_allocated());
    }

    #[test]
    fn test_alloc_tracker_with_manager() {
        let manager = Arc::new(WriteBufferManagerImpl::new(1000));
        {
            let tracker = AllocTracker::new(Some(manager.clone() as WriteBufferManagerRef));

            tracker.on_allocation(100);
            assert_eq!(100, tracker.bytes_allocated());
            assert_eq!(100, manager.memory_usage());
            assert_eq!(100, manager.mutable_usage());

            for _ in 0..2 {
                // Done allocating won't free the same memory multiple times.
                tracker.done_allocating();
                assert_eq!(100, manager.memory_usage());
                assert_eq!(0, manager.mutable_usage());
            }
        }

        assert_eq!(0, manager.memory_usage());
        assert_eq!(0, manager.mutable_usage());
    }

    #[test]
    fn test_alloc_tracker_without_done_allocating() {
        let manager = Arc::new(WriteBufferManagerImpl::new(1000));
        {
            let tracker = AllocTracker::new(Some(manager.clone() as WriteBufferManagerRef));

            tracker.on_allocation(100);
            assert_eq!(100, tracker.bytes_allocated());
            assert_eq!(100, manager.memory_usage());
            assert_eq!(100, manager.mutable_usage());
        }

        assert_eq!(0, manager.memory_usage());
        assert_eq!(0, manager.mutable_usage());
    }
}
