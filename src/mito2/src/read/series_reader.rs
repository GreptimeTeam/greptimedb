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

//! Reads selected metric series from partition ranges.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_stream::try_stream;
use datatypes::arrow::array::{ArrayRef, DictionaryArray, StringArray, UInt32Array};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::data_type::ConcreteDataType;
use datatypes::value::Value;
use futures::TryStreamExt;
use mito_codec::row_converter::{PrimaryKeyFilter, SparsePrimaryKeyCodec};
use snafu::ResultExt;
use store_api::region_engine::{PartitionRange, ScanProcessor};
use store_api::storage::{SequenceRange, TimeSeriesRowSelector};
use tokio::sync::Semaphore;

use crate::cache::CacheStrategy;
#[cfg(feature = "enterprise")]
use crate::error::InvalidRequestSnafu;
use crate::error::{ExternalSnafu, JoinSnafu, Result, UnexpectedSnafu};
use crate::read::BoxedRecordBatchStream;
use crate::read::projection::{MAX_VECTOR_LENGTH_TO_CACHE, repeated_vector_with_cache};
use crate::read::pruner::PartitionPruner;
use crate::read::range_cache::{
    build_series_range_cache_key, cache_flat_range_stream, cached_flat_range_stream,
};
use crate::read::research_series_native::{NativeRecord, SelectedNativeBlock};
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::{
    PartitionMetrics, SplitRecordBatchStream, compute_average_batch_size,
    compute_parallel_channel_size, filter_flat_batch_by_sequence, new_filter_metrics,
    scan_flat_mem_ranges, should_split_flat_batches_for_merge,
};
use crate::read::seq_scan::SeqScan;
use crate::read::series_candidate::validate_metric_metadata;
use crate::series_index::MetricSeriesId;
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::parquet::flat_format::primary_key_column_index;
use crate::sst::parquet::prefilter::prefilter_flat_batch_by_primary_key;
use crate::sst::parquet::reader::ReaderMetrics;
use crate::sst::parquet::research_sidecar::{PreparedRangeMaxRequest, SelectedSeriesBlock};
use crate::sst::parquet::row_group::ParquetFetchMetrics;

const TSID_DOMAIN_END: u128 = 1u128 << u64::BITS;

pub(crate) type SharedScanProcessor = Arc<Mutex<Box<dyn ScanProcessor>>>;

enum ResearchBlock {
    Page(SelectedSeriesBlock),
    Native(SelectedNativeBlock),
}

impl ResearchBlock {
    fn num_rows(&self) -> usize {
        match self {
            Self::Page(block) => block.num_rows(),
            Self::Native(block) => block.block().count,
        }
    }
    fn decode_bytes(&self) -> usize {
        match self {
            Self::Page(block) => block.decode_bytes(),
            Self::Native(block) => block.block().values.decode_bytes(),
        }
    }
    fn record(&self) -> Option<&Arc<NativeRecord>> {
        match self {
            Self::Page(_) => None,
            Self::Native(block) => Some(&block.record),
        }
    }
    fn read_visible_values(
        &self,
        native: bool,
    ) -> Result<store_api::region_engine::VisibleSeriesValues> {
        match self {
            Self::Page(block) => block.read_visible_values(native),
            Self::Native(block) => block.read_values(native),
        }
    }
    fn read_value_bits(&self) -> Result<datatypes::arrow::buffer::ScalarBuffer<u64>> {
        use datatypes::arrow::array::Array;
        match self {
            Self::Page(block) => block.read_value_bits(),
            Self::Native(_) => {
                let values = self.read_visible_values(false)?.materialize();
                Ok(datatypes::arrow::buffer::ScalarBuffer::new(
                    values.values().inner().clone(),
                    0,
                    values.len(),
                ))
            }
        }
    }
    fn read_range_max(
        &self,
        request: &PreparedRangeMaxRequest<'_>,
        rows: std::ops::Range<usize>,
    ) -> Result<Option<Vec<store_api::region_engine::VisibleSeriesBlock>>> {
        match self {
            Self::Page(block) => block.read_range_max(request, rows),
            Self::Native(_) => Ok(None),
        }
    }
}

/// A stable partition of the TSID integer domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SeriesRange {
    start: u128,
    end: u128,
}

impl SeriesRange {
    pub(crate) fn new(partition: usize, partitions: usize) -> Option<Self> {
        if partitions == 0 || partition >= partitions {
            return None;
        }

        let partitions = partitions as u128;
        let partition = partition as u128;
        let boundary = |partition: u128| {
            let numerator = partition * TSID_DOMAIN_END;
            numerator.div_ceil(partitions)
        };
        Some(Self {
            start: boundary(partition),
            end: boundary(partition + 1),
        })
    }

    fn partition_for(tsid: u64, partitions: usize) -> usize {
        ((tsid as u128 * partitions as u128) >> u64::BITS) as usize
    }
}

/// All series assigned to one data-reader partition.
#[derive(Debug)]
pub(crate) struct AssignedSeriesBatch {
    range: SeriesRange,
    series: Vec<MetricSeriesId>,
    enable_range_cache: bool,
}

impl AssignedSeriesBatch {
    fn new(range: SeriesRange, series: Vec<MetricSeriesId>, enable_range_cache: bool) -> Self {
        Self {
            range,
            series,
            enable_range_cache,
        }
    }

    pub(crate) fn range(&self) -> SeriesRange {
        self.range
    }

    pub(crate) fn series(&self) -> &[MetricSeriesId] {
        &self.series
    }

    pub(crate) fn enable_range_cache(&self) -> bool {
        self.enable_range_cache
    }
}

/// Collects candidate batches and assigns every TSID by its integer range.
pub(crate) struct SeriesBatchCollector {
    assignments: Vec<Vec<MetricSeriesId>>,
    num_series: usize,
}

impl SeriesBatchCollector {
    pub(crate) fn new(partitions: usize) -> Option<Self> {
        (partitions > 0).then(|| Self {
            assignments: (0..partitions).map(|_| Vec::new()).collect(),
            num_series: 0,
        })
    }

    pub(crate) fn push(&mut self, batch: Vec<MetricSeriesId>) {
        self.num_series += batch.len();
        let partitions = self.assignments.len();
        for series in batch {
            let partition = SeriesRange::partition_for(series.tsid, partitions);
            self.assignments[partition].push(series);
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.num_series
    }

    pub(crate) fn finish(self, enable_range_cache: bool) -> Vec<AssignedSeriesBatch> {
        let partitions = self.assignments.len();
        self.assignments
            .into_iter()
            .enumerate()
            .map(|(partition, series)| {
                AssignedSeriesBatch::new(
                    SeriesRange::new(partition, partitions).unwrap(),
                    series,
                    enable_range_cache,
                )
            })
            .collect()
    }
}

/// Immutable allow-list for one partition's metric series.
#[derive(Clone, Debug)]
struct MetricSeriesFilter {
    range: SeriesRange,
    series: Arc<HashSet<MetricSeriesId>>,
    sorted_series: Arc<Vec<MetricSeriesId>>,
    enable_range_cache: bool,
}

impl MetricSeriesFilter {
    fn new(assigned: &AssignedSeriesBatch) -> Self {
        let mut sorted_series = assigned.series().to_vec();
        sorted_series.sort_unstable();
        sorted_series.dedup();
        let series = sorted_series.iter().copied().collect();
        Self {
            range: assigned.range(),
            series: Arc::new(series),
            sorted_series: Arc::new(sorted_series),
            enable_range_cache: assigned.enable_range_cache(),
        }
    }

    fn primary_key_filter(&self, codec: SparsePrimaryKeyCodec) -> Box<dyn PrimaryKeyFilter> {
        Box::new(MetricSeriesPrimaryKeyFilter {
            codec,
            series: self.series.clone(),
            last_primary_key: Vec::new(),
            last_match: None,
        })
    }

    fn overlaps_encoded_bounds(
        &self,
        codec: &SparsePrimaryKeyCodec,
        encoded_min: &[u8],
        encoded_max: &[u8],
    ) -> Option<bool> {
        let (min_table_id, min_tsid) = codec.decode_ids(encoded_min).ok()?;
        let (max_table_id, max_tsid) = codec.decode_ids(encoded_max).ok()?;
        let min = MetricSeriesId {
            table_id: min_table_id,
            tsid: min_tsid,
        };
        let max = MetricSeriesId {
            table_id: max_table_id,
            tsid: max_tsid,
        };
        if min > max {
            return None;
        }
        if min_table_id != max_table_id {
            return Some(true);
        }

        Some(u128::from(min_tsid) < self.range.end && u128::from(max_tsid) >= self.range.start)
    }
}

struct MetricSeriesPrimaryKeyFilter {
    codec: SparsePrimaryKeyCodec,
    series: Arc<HashSet<MetricSeriesId>>,
    last_primary_key: Vec<u8>,
    last_match: Option<bool>,
}

impl PrimaryKeyFilter for MetricSeriesPrimaryKeyFilter {
    fn matches(&mut self, primary_key: &[u8]) -> mito_codec::error::Result<bool> {
        if let Some(last_match) = self.last_match
            && self.last_primary_key == primary_key
        {
            return Ok(last_match);
        }

        let (table_id, tsid) = self.codec.decode_ids(primary_key)?;
        let matched = self.series.contains(&MetricSeriesId { table_id, tsid });
        self.last_primary_key.clear();
        self.last_primary_key.extend_from_slice(primary_key);
        self.last_match = Some(matched);
        Ok(matched)
    }
}

fn filter_flat_stream_by_series(
    mut input: BoxedRecordBatchStream,
    codec: SparsePrimaryKeyCodec,
    filter: MetricSeriesFilter,
) -> BoxedRecordBatchStream {
    Box::pin(try_stream! {
        let mut primary_key_filter = filter.primary_key_filter(codec);
        while let Some(batch) = input.try_next().await? {
            let pk_idx = primary_key_column_index(batch.num_columns());
            if let Some(batch) = prefilter_flat_batch_by_primary_key(
                batch,
                pk_idx,
                primary_key_filter.as_mut(),
            )? {
                yield batch;
            }
        }
    })
}

/// Reads all collected metric series assigned to one partition.
pub(crate) struct SeriesReader {
    stream_ctx: Arc<StreamContext>,
    partition_ranges: Vec<PartitionRange>,
    filter: MetricSeriesFilter,
    codec: SparsePrimaryKeyCodec,
    partition_pruner: Arc<PartitionPruner>,
    range_semaphore: Arc<Semaphore>,
    part_metrics: PartitionMetrics,
}

impl SeriesReader {
    pub(crate) async fn try_build_projected_block_stream(
        &self,
        processor: Option<SharedScanProcessor>,
    ) -> Result<Option<futures::stream::BoxStream<'static, Result<common_recordbatch::RecordBatch>>>>
    {
        use crate::sst::parquet::file_range::SeriesBlockContext;
        use datafusion::execution::memory_pool::MemoryConsumer;
        use datatypes::arrow::array::{Array, ArrayRef, Float64Array, TimestampMillisecondArray};
        use mito_codec::row_converter::{CompositeValues, PrimaryKeyCodec};
        use std::collections::BTreeMap;

        let input = &self.stream_ctx.input;
        if std::env::var_os("GT_RESEARCH_BLOCK_SCAN").is_none()
            || !input.memtables.is_empty()
            || input.compaction
            || input.append_mode
            || input.sequence_range.is_some()
            || input.snapshot_sequence.is_some()
            || matches!(
                input.series_row_selector,
                Some(TimeSeriesRowSelector::LastRow { .. })
            )
        {
            return Ok(None);
        }
        let grid = input.series_row_selector;
        let direct_series =
            if grid.is_none() && std::env::var_os("GT_RESEARCH_DIRECT_SERIES_INPUT").is_some() {
                match &processor {
                    Some(processor) => processor
                        .lock()
                        .map_err(|_| block_scan_error("Scan processor lock poisoned"))?
                        .series_blocks()
                        .is_some(),
                    None => false,
                }
            } else {
                false
            };
        if let Some(TimeSeriesRowSelector::InstantGrid {
            start,
            end,
            interval,
            lookback,
            ..
        }) = grid
            && (start > end || interval <= 0 || lookback < 0)
        {
            return Ok(None);
        }
        let reservation = Arc::new(
            MemoryConsumer::new("research-series-blocks").register(&input.scan_memory_pool),
        );
        let mut retained_pages = HashSet::new();
        // Each context pins its SST while the lazy payload read is outstanding.
        type Block = (
            ResearchBlock,
            SeriesBlockContext,
            i64,
            i64,
            i128,
            std::ops::Range<usize>,
        );
        let mut series: BTreeMap<Vec<u8>, Vec<Block>> = BTreeMap::new();
        let mut native_series = None;
        let mut primary_key_filter = self.filter.primary_key_filter(self.codec.clone());
        let native_scan = self.stream_ctx.native_series_scan().await?;
        if direct_series
            && std::env::var_os("GT_RESEARCH_MAX_SUMMARY").is_none()
            && let Some(scan) = native_scan.as_ref()
            && scan.supports_coalesced(&self.filter.sorted_series)
        {
            let scan = scan.clone();
            let ids = self.filter.sorted_series.clone();
            let processor =
                processor.ok_or_else(|| block_scan_error("Missing coalesced processor"))?;
            for part in &self.partition_ranges {
                for index in &self.stream_ctx.ranges[part.identifier].row_group_indices {
                    self.partition_pruner
                        .finish_file_series_lookup(*index, &self.part_metrics);
                }
            }
            let work = async move {
                common_runtime::spawn_blocking_query(move || -> Result<()> {
                    let mut locked = processor
                        .lock()
                        .map_err(|_| block_scan_error("Scan processor lock poisoned"))?;
                    let processor = locked
                        .series_blocks()
                        .ok_or_else(|| block_scan_error("Coalesced processor disappeared"))?;
                    // The scan keeps every source FileHandle alive until processing finishes.
                    let cache_chunks = scan.caches_selection(&ids);
                    scan.process_coalesced(&ids, &reservation, processor, cache_chunks)
                })
                .await
                .context(JoinSnafu)??;
                Ok::<Option<common_recordbatch::RecordBatch>, crate::error::Error>(None)
            };
            return Ok(Some(Box::pin(
                futures::stream::once(work)
                    .try_filter_map(|batch| futures::future::ready(Ok(batch))),
            )));
        }
        let directory_series = if native_scan.is_none()
            && std::env::var_os("GT_RESEARCH_SERIES_DIRECTORY").is_some()
        {
            let mut keys = Vec::with_capacity(self.filter.sorted_series.len());
            let mut encoded = Vec::with_capacity(22);
            for id in self.filter.sorted_series.iter() {
                encoded.clear();
                self.codec
                    .encode_internal(id.table_id, id.tsid, &mut encoded)
                    .map_err(block_scan_error)?;
                keys.push(
                    encoded
                        .as_slice()
                        .try_into()
                        .map_err(|_| block_scan_error("Invalid encoded series prefix"))?,
                );
            }
            keys.sort_unstable();
            keys.dedup();
            Some(Arc::new(keys))
        } else {
            None
        };
        let file_lookup = std::env::var_os("GT_RESEARCH_FILE_SERIES_LOOKUP").is_some()
            && directory_series.is_some();
        let mut native_indices = Vec::new();
        let mut candidate_blocks = 0usize;
        let mut retained_blocks = 0usize;
        let mut candidate_rows = 0usize;
        let mut retained_rows = 0usize;
        let selection_batch_size = if std::env::var_os("GT_RESEARCH_BATCH_BLOCK_SELECT").is_some() {
            16
        } else {
            1
        };
        let mut collect_blocks =
            |selected: Vec<(SeriesBlockContext, Vec<SelectedSeriesBlock>)>| -> Result<bool> {
                for (range, blocks) in selected {
                    for block in blocks {
                        candidate_blocks += 1;
                        candidate_rows += block.num_rows();
                        if !primary_key_filter
                            .matches(block.primary_key())
                            .map_err(block_scan_error)?
                        {
                            continue;
                        }
                        retained_blocks += 1;
                        retained_rows += block.num_rows();
                        let Some((first, last, step)) = block.regular_put_range()? else {
                            return Ok(false);
                        };
                        let rows = if grid.is_some() || direct_series {
                            let Some(rows) = range.grid_row_bounds(first, step, block.num_rows())
                            else {
                                return Ok(false);
                            };
                            rows
                        } else {
                            0..block.num_rows()
                        };
                        if let Some(TimeSeriesRowSelector::InstantGrid { offset, .. }) = grid
                            && (first.checked_add(offset).is_none()
                                || last.checked_add(offset).is_none())
                        {
                            return Ok(false);
                        }
                        let (page, bytes) = block.retained_page();
                        let page_bytes = if retained_pages.insert(page) {
                            bytes
                        } else {
                            0
                        };
                        reservation
                            .try_grow(page_bytes + block.primary_key().len() + 256)
                            .map_err(block_scan_error)?;
                        series
                            .entry(block.primary_key().to_vec())
                            .or_default()
                            .push((
                                ResearchBlock::Page(block),
                                range.clone(),
                                first,
                                last,
                                step,
                                rows,
                            ));
                    }
                }
                Ok(true)
            };
        if let Some(scan) = native_scan {
            let ids = self.filter.sorted_series.clone();
            if scan.has_repeated_ids() {
                return Ok(None);
            }
            if scan.uses_records() {
                let memory = reservation.clone();
                let selected =
                    common_runtime::spawn_blocking_query(move || scan.records(&ids, &memory))
                        .await
                        .map_err(block_scan_error)??;
                let mut grouped = Vec::with_capacity(selected.len());
                for (key, blocks) in selected {
                    let mut selected = Vec::with_capacity(blocks.len());
                    for (range, block) in blocks {
                        let descriptor = block.block();
                        let (first, last, step) =
                            (descriptor.first, descriptor.last, descriptor.step);
                        let rows = if grid.is_some() || direct_series {
                            let Some(rows) = range.grid_row_bounds(first, step, descriptor.count)
                            else {
                                return Ok(None);
                            };
                            rows
                        } else {
                            0..descriptor.count
                        };
                        if let Some(TimeSeriesRowSelector::InstantGrid { offset, .. }) = grid
                            && (first.checked_add(offset).is_none()
                                || last.checked_add(offset).is_none())
                        {
                            return Ok(None);
                        }
                        selected.push((
                            ResearchBlock::Native(block),
                            range,
                            first,
                            last,
                            step,
                            rows,
                        ));
                    }
                    grouped.push((key, selected));
                }
                native_series = Some(grouped);
            } else {
                let allocation = reservation.new_empty();
                allocation
                    .try_grow(
                        scan.block_count(&ids)? * std::mem::size_of::<SelectedSeriesBlock>() * 2,
                    )
                    .map_err(block_scan_error)?;
                let (selected, _allocation) = common_runtime::spawn_blocking_query(move || {
                    Ok::<_, crate::error::Error>((scan.blocks(&ids)?, allocation))
                })
                .await
                .map_err(block_scan_error)??;
                if !collect_blocks(selected)? {
                    return Ok(None);
                }
            }
            for part in &self.partition_ranges {
                native_indices.extend(
                    self.stream_ctx.ranges[part.identifier]
                        .row_group_indices
                        .iter()
                        .copied(),
                );
            }
            common_telemetry::debug!(
                assigned_series = self.filter.sorted_series.len(),
                "Research series-major blocks activated"
            );
        } else {
            for part in &self.partition_ranges {
                for index in self.stream_ctx.ranges[part.identifier]
                    .row_group_indices
                    .iter()
                    .copied()
                {
                    if !self.stream_ctx.is_file_range_index(index) {
                        return Ok(None);
                    }
                    let file = input.file_from_index(index);
                    if file.region_id() != input.region_metadata().region_id {
                        return Ok(None);
                    }
                    let mut batches = Vec::new();
                    if file_lookup {
                        let Some((context, selection)) = self
                            .partition_pruner
                            .prepare_file_series_blocks(
                                index,
                                directory_series.as_ref().unwrap().clone(),
                            )
                            .await?
                        else {
                            return Ok(None);
                        };
                        batches.push(vec![(context, Some(selection))]);
                        native_indices.push(index);
                    } else {
                        if self
                            .partition_pruner
                            .try_skip_manifest_pruned_file_range(index, &self.part_metrics)
                        {
                            continue;
                        }
                        let mut reader_metrics = ReaderMetrics::default();
                        let mut ranges = self
                            .partition_pruner
                            .build_file_ranges(index, &self.part_metrics, &mut reader_metrics)
                            .await?;
                        // FileRangeBuilder shares one immutable context across these row groups.
                        if let Some(range) = ranges.first()
                            && (!range.supports_projected_series_blocks()
                                || range.region_metadata().as_ref()
                                    != input.region_metadata().as_ref())
                        {
                            return Ok(None);
                        }
                        ranges.retain(|range| {
                            !matches!(
                                range.primary_key_range().and_then(|(min, max)| {
                                    self.filter.overlaps_encoded_bounds(&self.codec, min, max)
                                }),
                                Some(false)
                            )
                        });
                        for chunk in ranges.chunks(selection_batch_size) {
                            let mut prepared = Vec::with_capacity(chunk.len());
                            for range in chunk {
                                let selected = if let Some(searcher) =
                                    range.range_index_searcher().await?
                                {
                                    let group = u32::try_from(range.row_group_index())
                                        .map_err(block_scan_error)?;
                                    Some(searcher.search(group, &self.filter.sorted_series).await?)
                                } else {
                                    None
                                };
                                let Some(selection) = range
                                    .prepare_series_blocks(selected, directory_series.clone())
                                    .await?
                                else {
                                    return Ok(None);
                                };
                                prepared
                                    .push((SeriesBlockContext::Range(range.clone()), selection));
                            }

                            batches.push(prepared);
                        }
                    }
                    for prepared in batches {
                        let batch_reservation = reservation.new_empty();
                        let (selected, _batch_reservation) =
                            common_runtime::spawn_blocking_query(move || {
                                let mut selected = Vec::with_capacity(prepared.len());
                                for (range, selection) in prepared {
                                    let blocks = if let Some(selection) = selection {
                                        let Some(blocks) = selection.select()? else {
                                            return Ok::<_, crate::error::Error>((
                                                None,
                                                batch_reservation,
                                            ));
                                        };
                                        blocks
                                    } else {
                                        Vec::new()
                                    };
                                    batch_reservation
                                        .try_grow(
                                            blocks.capacity()
                                                * std::mem::size_of::<SelectedSeriesBlock>(),
                                        )
                                        .map_err(block_scan_error)?;
                                    selected.push((range, blocks));
                                }
                                Ok((Some(selected), batch_reservation))
                            })
                            .await
                            .map_err(block_scan_error)??;
                        let Some(selected) = selected else {
                            return Ok(None);
                        };
                        if !collect_blocks(selected)? {
                            return Ok(None);
                        }
                    }
                }
            }
        }
        let native_records = native_series.is_some();
        let mut series = if let Some(series) = native_series {
            candidate_blocks = series.iter().map(|(_, blocks)| blocks.len()).sum();
            candidate_rows = series
                .iter()
                .flat_map(|(_, blocks)| blocks)
                .map(|block| block.0.num_rows())
                .sum();
            retained_blocks = candidate_blocks;
            retained_rows = candidate_rows;
            series
        } else {
            series.into_iter().collect::<Vec<_>>()
        };
        for (_, blocks) in &mut series {
            blocks.sort_unstable_by_key(|block| block.2);
            if blocks.windows(2).any(|pair| pair[0].3 >= pair[1].2) {
                return Ok(None);
            }
        }
        for index in native_indices {
            self.partition_pruner
                .finish_file_series_lookup(index, &self.part_metrics);
        }
        if native_records {
            common_telemetry::debug!(
                series = series.len(),
                blocks = candidate_blocks,
                "Research native series records activated"
            );
        }
        // All relevant sources were inspected before any output. Disjoint, strictly
        // increasing Put-only blocks need no sequence comparison or tombstone handling.
        let mapper = input.mapper.clone();
        let cache = input.cache_strategy.clone();
        let codec = self.codec.clone();
        common_telemetry::debug!(
            series = series.len(),
            assigned_series = self.filter.sorted_series.len(),
            candidate_blocks,
            retained_blocks,
            candidate_rows,
            retained_rows,
            selection_batch_size,
            file_lookup,
            direct_series,
            ?grid,
            processing = processor.is_some(),
            "Research projected block scan activated"
        );
        if direct_series {
            let processor =
                processor.ok_or_else(|| block_scan_error("Missing direct series processor"))?;
            let work = async move {
                common_runtime::spawn_blocking_query(move || -> Result<()> {
                    let mut processor = processor
                        .lock()
                        .map_err(|_| block_scan_error("Scan processor lock poisoned"))?;
                    let processor = processor
                        .series_blocks()
                        .ok_or_else(|| block_scan_error("Direct series processor disappeared"))?;
                    let native_values = std::env::var_os("GT_RESEARCH_NATIVE_VALUES").is_some();
                    let max_summary_root = std::env::var_os("GT_RESEARCH_MAX_SUMMARY");
                    let max_request = if max_summary_root.is_some() {
                        processor.range_max_request()
                    } else {
                        None
                    };
                    let request_memory = reservation.new_empty();
                    if let Some(request) = &max_request {
                        request_memory.try_grow(request.instants.capacity() * 8).map_err(block_scan_error)?;
                    }
                    let prepared_max = max_request.as_ref().zip(max_summary_root)
                        .map(|(request, root)| PreparedRangeMaxRequest::new(request, root.into()))
                        .transpose()?;
                    let mut summary_blocks = 0usize;
                    let mut summary_input_rows = 0usize;
                    let mut summary_output_rows = 0usize;
                    let mut raw_blocks = 0usize;
                    let mut native_value_blocks = 0usize;
                    let mut native_value_rows = 0usize;
                    for (key, blocks) in series {
                        let record = blocks.first().and_then(|block| block.0.record()).cloned();
                        let tags = if let Some(record) = &record {
                            std::borrow::Cow::Borrowed(record.tags.as_slice())
                        } else {
                            let CompositeValues::Sparse(decoded) = codec.decode(&key).map_err(block_scan_error)? else {
                                return Err(block_scan_error("Expected sparse primary key"));
                            };
                            std::borrow::Cow::Owned(mapper.metadata().primary_key_columns().map(|column|
                                (column.column_schema.name.clone(), decoded.get_or_null(column.column_id).clone())).collect::<Vec<_>>())
                        };
                        let scratch = blocks
                            .iter()
                            .map(|block| if max_request.is_some() { block.0.decode_bytes() * 4 + 4096 } else { block.0.decode_bytes() })
                            .max()
                            .unwrap_or(0);
                        let temporary = reservation.new_empty();
                        temporary.try_grow(scratch).map_err(block_scan_error)?;
                        let mut source = blocks
                            .into_iter()
                            .filter(|block| !block.5.is_empty())
                            .flat_map(|(block, _range, first, _, step, rows)| {
                                let result = (|| -> std::result::Result<Vec<_>, common_error::ext::BoxedError> {
                                if let Some(request) = &prepared_max
                                    && let Some(values) = block.read_range_max(request, rows.clone()).map_err(common_error::ext::BoxedError::new)? {
                                    summary_blocks += 1;
                                    summary_input_rows += rows.len();
                                    summary_output_rows += values.iter().map(|block| block.values.len()).sum::<usize>();
                                    return Ok(values);
                                }
                                raw_blocks += 1;
                                let values = block
                                    .read_visible_values(native_values)
                                    .map_err(common_error::ext::BoxedError::new)?
                                    .slice(rows.start, rows.len());
                                if matches!(values, store_api::region_engine::VisibleSeriesValues::Integer(_)) {
                                    native_value_blocks += 1;
                                    native_value_rows += values.len();
                                }
                                Ok(vec![store_api::region_engine::VisibleSeriesBlock {
                                    first_timestamp: (first as i128 + step * rows.start as i128)
                                        as i64,
                                    timestamp_step: step,
                                    values,
                                }])
                                })();
                                match result {
                                    Ok(blocks) => blocks.into_iter().map(Ok).collect::<Vec<_>>(),
                                    Err(error) => vec![Err(error)],
                                }
                            });
                        processor
                            .process_series(&tags, &mut source)
                            .context(ExternalSnafu {
                                context: "Process visible series blocks",
                            })?;
                        // A consumer must not silently discard unread blocks or their errors.
                        if source.next().is_some() {
                            return Err(block_scan_error(
                                "Direct series processor left unread blocks",
                            ));
                        }
                    }
                    common_telemetry::debug!(raw_blocks, native_value_blocks, native_value_rows,
                        "Research native values consumed");
                    if summary_blocks > 0 {
                        common_telemetry::debug!(summary_blocks, raw_blocks, summary_input_rows,
                            summary_output_rows, "Research range max summary consumed");
                    }
                    Ok(())
                })
                .await
                .context(JoinSnafu)??;
                Ok::<Option<common_recordbatch::RecordBatch>, crate::error::Error>(None)
            };
            return Ok(Some(Box::pin(
                futures::stream::once(work)
                    .try_filter_map(|batch| futures::future::ready(Ok(batch))),
            )));
        }
        let build_batch = Arc::new(move |key: Vec<u8>, blocks: Vec<Block>| -> Result<_> {
            let rows = blocks.iter().map(|block| block.0.num_rows()).sum::<usize>();
            let output_rows = if let Some(TimeSeriesRowSelector::InstantGrid {
                start,
                end,
                interval,
                ..
            }) = grid
            {
                ((end as i128 - start as i128) / interval as i128 + 1 + blocks.len() as i128)
                    .min(rows as i128) as usize
            } else {
                rows
            };
            let scratch = blocks
                .iter()
                .map(|block| block.0.decode_bytes())
                .max()
                .unwrap_or(0);
            let bytes = output_rows
                .checked_mul(32 + key.len())
                .and_then(|size| size.checked_add(scratch))
                .ok_or_else(|| block_scan_error("Series allocation size overflow"))?;
            let sample_reservation = reservation.new_empty();
            sample_reservation
                .try_grow(bytes)
                .map_err(block_scan_error)?;
            let mut times = Vec::with_capacity(output_rows);
            let mut values = Vec::with_capacity(output_rows);
            for (block, range, first, _, step, row_bounds) in blocks {
                if let Some(grid) = grid {
                    let selected = block_grid_rows(first, step, row_bounds, grid);
                    if selected.is_empty() {
                        continue;
                    }
                    let bits = block.read_value_bits()?;
                    for row in selected {
                        times.push((first as i128 + step * row as i128) as i64);
                        values.push(f64::from_bits(bits[row]));
                    }
                    continue;
                }
                let timestamps: ArrayRef = Arc::new(TimestampMillisecondArray::from_iter_values(
                    (0..block.num_rows()).map(|i| (first as i128 + step * i as i128) as i64),
                ));
                let bits = block.read_value_bits()?;
                let count = bits.len();
                let data: ArrayRef = Arc::new(Float64Array::new(
                    datatypes::arrow::buffer::ScalarBuffer::new(bits.into_inner(), 0, count),
                    None,
                ));
                let mask = range.filter_series_values(&timestamps, &data)?;
                let timestamps = timestamps
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| block_scan_error("Invalid time array"))?;
                let data = data
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| block_scan_error("Invalid value array"))?;
                for row in mask.set_indices() {
                    times.push(timestamps.value(row));
                    values.push(data.value(row));
                }
            }
            let count = times.len();
            let timestamps: ArrayRef = Arc::new(TimestampMillisecondArray::from(times));
            let values: ArrayRef = Arc::new(Float64Array::from(values));
            let CompositeValues::Sparse(tags) = codec.decode(&key).map_err(block_scan_error)?
            else {
                return Err(block_scan_error("Expected sparse primary key"));
            };
            let schema = mapper.output_schema();
            let mut arrays = Vec::with_capacity(schema.num_columns());
            let mut tag_dictionary = None;
            for column in schema.column_schemas() {
                let metadata = mapper
                    .metadata()
                    .column_by_name(&column.name)
                    .ok_or_else(|| block_scan_error("Missing output column"))?;
                let array = match metadata.semantic_type {
                    api::v1::SemanticType::Timestamp => timestamps.clone(),
                    api::v1::SemanticType::Field => values.clone(),
                    api::v1::SemanticType::Tag => constant_series_tag(
                        &column.data_type,
                        tags.get_or_null(metadata.column_id),
                        count,
                        &cache,
                        &mut tag_dictionary,
                    )?,
                };
                arrays.push(array);
            }
            if arrays.is_empty() {
                let batch = common_recordbatch::RecordBatch::new_with_count(schema, count)
                    .map_err(block_scan_error)?;
                return Ok((batch, sample_reservation));
            }
            let batch = datatypes::arrow::record_batch::RecordBatch::try_new(
                schema.arrow_schema().clone(),
                arrays,
            )
            .map_err(block_scan_error)?;
            Ok((
                common_recordbatch::RecordBatch::from_df_record_batch(schema, batch),
                sample_reservation,
            ))
        });
        if let Some(processor) = processor {
            let work = async move {
                common_runtime::spawn_blocking_query(move || -> Result<()> {
                    let mut processor = processor
                        .lock()
                        .map_err(|_| block_scan_error("Scan processor lock poisoned"))?;
                    for (key, blocks) in series {
                        let (batch, _reservation) = build_batch(key, blocks)?;
                        if batch.num_rows() != 0 {
                            processor.process(&batch).context(ExternalSnafu {
                                context: "Process auxiliary scan batch",
                            })?;
                        }
                    }
                    Ok(())
                })
                .await
                .context(JoinSnafu)??;
                Ok::<Option<common_recordbatch::RecordBatch>, crate::error::Error>(None)
            };
            return Ok(Some(Box::pin(
                futures::stream::once(work)
                    .try_filter_map(|batch| futures::future::ready(Ok(batch))),
            )));
        }
        Ok(Some(projected_series_stream(
            series
                .into_iter()
                .map(move |(key, blocks)| build_batch(key, blocks)),
        )))
    }

    /// Creates a reader for the series assigned to one data partition.
    ///
    /// `partition_pruner` must come from the candidate scanner's pruner, which
    /// has predicate prefiltering disabled. The file read path applies precise
    /// filters with tag filtering skipped because candidate discovery has already
    /// enforced tag predicates through the exact assigned-series set.
    ///
    /// A single `range_semaphore` covers both the range-build phase and the final
    /// merge.
    pub(crate) fn try_new(
        stream_ctx: Arc<StreamContext>,
        partition_ranges: Vec<PartitionRange>,
        assigned_series: AssignedSeriesBatch,
        partition_pruner: Arc<PartitionPruner>,
        range_semaphore: Arc<Semaphore>,
        part_metrics: PartitionMetrics,
    ) -> Result<Self> {
        validate_metric_metadata(&stream_ctx)?;
        #[cfg(feature = "enterprise")]
        snafu::ensure!(
            stream_ctx.input.extension_ranges().is_empty(),
            InvalidRequestSnafu {
                region_id: stream_ctx.input.region_metadata().region_id,
                reason: "series reader does not support extension ranges",
            }
        );

        let filter = MetricSeriesFilter::new(&assigned_series);
        let codec = SparsePrimaryKeyCodec::new(stream_ctx.input.region_metadata());
        Ok(Self {
            stream_ctx,
            partition_ranges,
            filter,
            codec,
            partition_pruner,
            range_semaphore,
            part_metrics,
        })
    }

    pub(crate) async fn build_stream(&self) -> Result<BoxedRecordBatchStream> {
        if self.partition_ranges.is_empty() || self.filter.series.is_empty() {
            return Ok(Box::pin(futures::stream::empty()));
        }

        let mut tasks = Vec::with_capacity(self.partition_ranges.len());
        for part_range in self.partition_ranges.iter().copied() {
            let stream_ctx = self.stream_ctx.clone();
            let filter = self.filter.clone();
            let codec = self.codec.clone();
            let partition_pruner = self.partition_pruner.clone();
            let range_semaphore = self.range_semaphore.clone();
            let part_metrics = self.part_metrics.clone();
            tasks.push(common_runtime::spawn_query(async move {
                let _permit = range_semaphore.acquire().await.map_err(|error| {
                    UnexpectedSnafu {
                        reason: format!("failed to acquire series range permit: {error}"),
                    }
                    .build()
                })?;
                build_series_partition_range(
                    stream_ctx,
                    part_range,
                    filter,
                    codec,
                    partition_pruner,
                    part_metrics,
                )
                .await
            }));
        }

        let mut range_streams = Vec::with_capacity(tasks.len());
        let mut estimated_batch_sizes = Vec::with_capacity(tasks.len());
        for task in tasks {
            let (stream, estimated_batch_size) = task.await.context(JoinSnafu)??;
            range_streams.push(stream);
            estimated_batch_sizes.push(estimated_batch_size);
        }

        // Every range task above has finished, so all build permits are released
        // and the final merge can reuse the same semaphore.
        let estimated_batch_size = compute_average_batch_size(estimated_batch_sizes);
        SeqScan::build_flat_reader_from_sources(
            &self.stream_ctx,
            range_streams,
            Some(self.range_semaphore.clone()),
            Some(&self.part_metrics),
            true,
            compute_parallel_channel_size(estimated_batch_size),
        )
        .await
    }
}

fn projected_series_stream(
    mut batches: impl Iterator<
        Item = Result<(
            common_recordbatch::RecordBatch,
            datafusion::execution::memory_pool::MemoryReservation,
        )>,
    > + Send
    + 'static,
) -> futures::stream::BoxStream<'static, Result<common_recordbatch::RecordBatch>> {
    Box::pin(try_stream! {
        loop {
            // Finite jobs amortize scheduling without holding blocking threads on backpressure.
            let (remaining, output, exhausted) = common_runtime::spawn_blocking_query(move || {
                let mut output = Vec::with_capacity(16);
                let mut exhausted = false;
                for _ in 0..16 {
                    let Some(batch) = batches.next() else {
                        exhausted = true;
                        break;
                    };
                    let failed = batch.is_err();
                    output.push(batch);
                    if failed {
                        break;
                    }
                }
                (batches, output, exhausted)
            }).await.context(JoinSnafu)?;
            batches = remaining;
            for output in output {
                let (batch, reservation) = output?;
                if batch.num_rows() != 0 {
                    yield batch;
                }
                drop(reservation);
            }
            if exhausted {
                break;
            }
        }
    })
}

// Keep a block's last candidate even when a later block can supersede it. The
// normal instant evaluator resolves these candidates without changing stale bits.
fn block_grid_rows(
    first: i64,
    step: i128,
    rows: std::ops::Range<usize>,
    grid: TimeSeriesRowSelector,
) -> Vec<usize> {
    let TimeSeriesRowSelector::InstantGrid {
        start,
        end,
        interval,
        lookback,
        offset,
    } = grid
    else {
        return rows.collect();
    };
    if rows.is_empty() {
        return Vec::new();
    }
    let interval = interval as i128;
    let first = first as i128 + offset as i128;
    let sample = first + step * rows.start as i128;
    let delta = (sample - start as i128).max(0);
    let mut evaluation = start as i128 + ((delta + interval - 1) / interval) * interval;
    let mut selected = Vec::new();
    while evaluation <= end as i128 {
        let row = if step == 0 {
            rows.start
        } else {
            ((evaluation - first) / step).min((rows.end - 1) as i128) as usize
        };
        let age = evaluation - (first + step * row as i128);
        if (age == 0 || age < lookback as i128) && selected.last() != Some(&row) {
            selected.push(row);
        }
        if row == rows.end - 1 {
            break;
        }
        evaluation += interval;
    }
    selected
}

fn block_scan_error(reason: impl std::fmt::Display) -> crate::error::Error {
    UnexpectedSnafu {
        reason: format!("Research block scan: {reason}"),
    }
    .build()
}

fn constant_series_tag(
    data_type: &ConcreteDataType,
    value: &Value,
    rows: usize,
    cache: &CacheStrategy,
    dictionary: &mut Option<DictionaryArray<UInt32Type>>,
) -> Result<ArrayRef> {
    if rows > MAX_VECTOR_LENGTH_TO_CACHE
        && let Value::String(value) = value
        && *data_type
            == ConcreteDataType::dictionary_datatype(
                ConcreteDataType::uint32_datatype(),
                ConcreteDataType::string_datatype(),
            )
    {
        let values: ArrayRef = Arc::new(StringArray::from(vec![value.as_utf8()]));
        if let Some(dictionary) = dictionary {
            return Ok(Arc::new(dictionary.with_values(values)));
        }
        let result = DictionaryArray::try_new(UInt32Array::from(vec![0; rows]), values)
            .map_err(block_scan_error)?;
        // All non-null string tags in this series share the validated zero keys.
        *dictionary = Some(result.clone());
        common_telemetry::debug!(rows, "Research shared series tags activated");
        return Ok(Arc::new(result));
    }
    repeated_vector_with_cache(data_type, value, rows, cache)
        .map(|vector| vector.to_arrow_array())
        .map_err(block_scan_error)
}

async fn build_series_partition_range(
    stream_ctx: Arc<StreamContext>,
    part_range: PartitionRange,
    filter: MetricSeriesFilter,
    codec: SparsePrimaryKeyCodec,
    partition_pruner: Arc<PartitionPruner>,
    part_metrics: PartitionMetrics,
) -> Result<(BoxedRecordBatchStream, usize)> {
    let range = filter.range;
    let cache_key = filter
        .enable_range_cache
        .then(|| build_series_range_cache_key(&stream_ctx, &part_range, range))
        .flatten();
    if let Some(key) = cache_key.as_ref() {
        if let Some(value) = stream_ctx.input.cache_strategy.get_range_result(key) {
            part_metrics.inc_range_cache_hit();
            return Ok((cached_flat_range_stream(value), DEFAULT_READ_BATCH_SIZE));
        }
        part_metrics.inc_range_cache_miss();
    }

    let range_meta = &stream_ctx.ranges[part_range.identifier];
    let split_batch_size = should_split_flat_batches_for_merge(&stream_ctx, range_meta);
    let mut sources = Vec::with_capacity(range_meta.row_group_indices.len());

    for index in range_meta.row_group_indices.iter().copied() {
        if stream_ctx.is_mem_range_index(index) {
            let stream = Box::pin(scan_flat_mem_ranges(
                stream_ctx.clone(),
                part_metrics.clone(),
                index,
                range_meta.time_range,
            ));
            sources.push(filter_flat_stream_by_series(
                stream,
                codec.clone(),
                filter.clone(),
            ));
            continue;
        }

        if stream_ctx.is_file_range_index(index) {
            let file = stream_ctx.input.file_from_index(index);
            if matches!(
                file.primary_key_range(stream_ctx.input.primary_key_mapper())
                    .and_then(|(min, max)| { filter.overlaps_encoded_bounds(&codec, &min, &max) }),
                Some(false)
            ) {
                continue;
            }

            if partition_pruner.try_skip_manifest_pruned_file_range(index, &part_metrics) {
                continue;
            }
            let mut reader_metrics = ReaderMetrics {
                filter_metrics: new_filter_metrics(part_metrics.explain_verbose()),
                ..Default::default()
            };
            let file_ranges = partition_pruner
                .build_file_ranges(index, &part_metrics, &mut reader_metrics)
                .await?;
            part_metrics.inc_num_file_ranges(file_ranges.len());
            part_metrics.merge_reader_metrics(&reader_metrics, None);
            let ranges = file_ranges
                .iter()
                .filter(|file_range| {
                    !matches!(
                        file_range.primary_key_range().and_then(|(min, max)| {
                            filter.overlaps_encoded_bounds(&codec, min, max)
                        }),
                        Some(false)
                    )
                })
                .cloned()
                .collect::<smallvec::SmallVec<[_; 2]>>();
            if ranges.is_empty() {
                continue;
            }

            let stream = scan_series_file_ranges(
                part_metrics.clone(),
                ranges,
                filter.clone(),
                codec.clone(),
                stream_ctx.input.sequence_range,
                stream_ctx.input.region_metadata().region_id,
            );
            sources.push(Box::pin(stream) as BoxedRecordBatchStream);
            continue;
        }

        return UnexpectedSnafu {
            reason: format!(
                "series reader received unsupported range index {}",
                index.index
            ),
        }
        .fail();
    }

    if split_batch_size.is_some() {
        sources = sources
            .into_iter()
            .map(|stream| Box::pin(SplitRecordBatchStream::new(stream)) as BoxedRecordBatchStream)
            .collect();
    }
    let estimated_batch_size = split_batch_size.unwrap_or(DEFAULT_READ_BATCH_SIZE);
    let stream = SeqScan::build_flat_reader_from_sources(
        &stream_ctx,
        sources,
        None,
        Some(&part_metrics),
        false,
        compute_parallel_channel_size(estimated_batch_size),
    )
    .await?;
    let stream = match cache_key {
        Some(key) => cache_flat_range_stream(
            stream,
            stream_ctx.input.cache_strategy.clone(),
            key,
            part_metrics,
        ),
        None => stream,
    };
    Ok((stream, estimated_batch_size))
}

fn scan_series_file_ranges(
    part_metrics: PartitionMetrics,
    ranges: smallvec::SmallVec<[crate::sst::parquet::file_range::FileRange; 2]>,
    filter: MetricSeriesFilter,
    codec: SparsePrimaryKeyCodec,
    sequence_range: Option<SequenceRange>,
    region_id: store_api::storage::RegionId,
) -> impl futures::Stream<Item = Result<datatypes::arrow::record_batch::RecordBatch>> {
    try_stream! {
        let fetch_metrics = part_metrics
            .explain_verbose()
            .then(|| Arc::new(ParquetFetchMetrics::default()));
        let mut reader_metrics = ReaderMetrics {
            fetch_metrics: fetch_metrics.clone(),
            ..Default::default()
        };
        let mut primary_key_filter = filter.primary_key_filter(codec);

        for range in ranges {
            let build_start = Instant::now();
            let searcher = range.range_index_searcher().await?;
            let reader = if let Some(searcher) = searcher {
                let row_group_id = u32::try_from(range.row_group_index()).map_err(|_| UnexpectedSnafu {
                    reason: format!("row group index exceeds u32: {}", range.row_group_index()),
                }.build())?;
                let selected = searcher.search(row_group_id, &filter.sorted_series).await?;
                range.reader_by_row_ranges(selected, fetch_metrics.as_deref()).await?
            } else {
                range.reader_by_primary_key(primary_key_filter.as_mut(), fetch_metrics.as_deref()).await?
            };
            let build_cost = build_start.elapsed();
            reader_metrics.build_cost += build_cost;
            part_metrics.inc_build_reader_cost(build_cost);
            let Some(mut reader) = reader else {
                continue;
            };

            let scan_start = Instant::now();
            let file_sequence_trusted = range
                .file_handle()
                .is_effective_target_sequence_trusted(region_id);
            while let Some(record_batch) = reader.next_batch().await? {
                reader_metrics.num_record_batches += 1;
                reader_metrics.num_batches += 1;
                reader_metrics.num_rows += record_batch.num_rows();

                let Some(record_batch) = filter_flat_batch_by_sequence(
                    record_batch,
                    sequence_range,
                    file_sequence_trusted,
                )? else {
                    continue;
                };

                let num_rows_before_filter = record_batch.num_rows();
                let Some(record_batch) = range.precise_filter_flat(
                    record_batch,
                    range.pre_filter_mode().skip_fields(),
                    true,
                )? else {
                    reader_metrics.filter_metrics.rows_precise_filtered +=
                        num_rows_before_filter;
                    continue;
                };
                reader_metrics.filter_metrics.rows_precise_filtered +=
                    num_rows_before_filter - record_batch.num_rows();

                let record_batch = if let Some(mapper) = range.compaction_projection_mapper() {
                    mapper.project(record_batch)?
                } else {
                    record_batch
                };
                if let Some(compat) = range.compat_batch() {
                    yield compat.compat(record_batch)?;
                } else {
                    yield record_batch;
                }
            }
            reader_metrics.scan_cost += scan_start.elapsed();
        }

        reader_metrics.observe_rows("series_data");
        reader_metrics.filter_metrics.observe();
        part_metrics.merge_reader_metrics(&reader_metrics, None);
    }
}

#[cfg(test)]
mod tests {
    use store_api::codec::PrimaryKeyEncoding;

    use super::*;
    use crate::error::DecodeSnafu;
    use crate::test_util::sst_util::sst_region_metadata_with_encoding;

    #[tokio::test]
    async fn projected_series_worker_is_bounded_and_releases_on_cancel() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
        use tokio::sync::oneshot;

        struct Completion(Option<oneshot::Sender<()>>);
        impl Drop for Completion {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
            }
        }

        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
        let memory = MemoryConsumer::new("bounded-series-test").register(&pool);
        let built = Arc::new(AtomicUsize::new(0));
        let producer_built = built.clone();
        let (done, completed) = oneshot::channel();
        let completion = Completion(Some(done));
        let batches = (0..64).map(move |index| {
            let _completion = &completion;
            let reservation = memory.new_empty();
            reservation.try_grow(64).unwrap();
            let batch = common_recordbatch::RecordBatch::new_with_count(
                Arc::new(datatypes::schema::Schema::new(vec![])),
                index + 1,
            )
            .unwrap();
            producer_built.fetch_add(1, Ordering::SeqCst);
            Ok((batch, reservation))
        });
        let mut stream = projected_series_stream(batches);
        assert_eq!(stream.try_next().await.unwrap().unwrap().num_rows(), 1);
        assert_eq!(built.load(Ordering::SeqCst), 16);
        assert_eq!(pool.reserved(), 1024);
        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), completed)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(built.load(Ordering::SeqCst), 16);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn projected_series_worker_propagates_read_failure_and_panic() {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};

        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
        let memory = MemoryConsumer::new("series-error-test").register(&pool);
        let batches = (0..18).map(move |index| {
            if index == 17 {
                return Err(block_scan_error("injected read failure"));
            }
            let reservation = memory.new_empty();
            reservation.try_grow(64).unwrap();
            let batch = common_recordbatch::RecordBatch::new_with_count(
                Arc::new(datatypes::schema::Schema::new(vec![])),
                index + 1,
            )
            .unwrap();
            Ok((batch, reservation))
        });
        let mut stream = projected_series_stream(batches);
        for index in 0..17 {
            assert_eq!(
                stream.try_next().await.unwrap().unwrap().num_rows(),
                index + 1
            );
        }
        assert!(
            stream
                .try_next()
                .await
                .unwrap_err()
                .to_string()
                .contains("injected read failure")
        );
        assert_eq!(pool.reserved(), 0);

        let batches = std::iter::once_with(|| panic!("injected worker panic"));
        let mut stream = projected_series_stream(batches);
        assert!(matches!(
            stream.try_next().await,
            Err(crate::error::Error::Join { .. })
        ));
    }

    #[test]
    fn block_grid_preserves_filtered_winners() {
        let blocks = [(-30, 3, 2..8), (2, 4, 0..7), (40, 0, 0..1), (50, 2, 0..0)];
        let original: Vec<i64> = blocks
            .iter()
            .flat_map(|(first, step, rows)| {
                rows.clone()
                    .map(|row| (first + step * row as i128) as i64)
                    .collect::<Vec<_>>()
            })
            .collect();
        for interval in [1, 3, 10, 30] {
            for lookback in [0, 1, 7, 100] {
                for offset in [-17, 0, 23] {
                    let grid = TimeSeriesRowSelector::InstantGrid {
                        start: -50,
                        end: 100,
                        interval,
                        lookback,
                        offset,
                    };
                    let selected: Vec<i64> = blocks
                        .iter()
                        .flat_map(|(first, step, rows)| {
                            block_grid_rows(*first as i64, *step, rows.clone(), grid)
                                .into_iter()
                                .map(|row| (first + step * row as i128) as i64)
                                .collect::<Vec<_>>()
                        })
                        .collect();
                    for evaluation in (-50..=100).step_by(interval as usize) {
                        let winner = |samples: &[i64]| {
                            samples.iter().copied().rev().find(|t| {
                                let age = evaluation - (*t + offset);
                                age >= 0 && (age == 0 || age < lookback)
                            })
                        };
                        assert_eq!(
                            winner(&original),
                            winner(&selected),
                            "{grid:?} at {evaluation}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn block_scan_constant_tags_preserve_values_and_share_keys() {
        let data_type = ConcreteDataType::dictionary_datatype(
            ConcreteDataType::uint32_datatype(),
            ConcreteDataType::string_datatype(),
        );
        for rows in [0, 3, 20160] {
            let mut dictionary = None;
            let mut arrays = Vec::new();
            for value in [
                Value::from("node-a"),
                Value::Null,
                Value::from(""),
                Value::from("node-b"),
            ] {
                let actual = constant_series_tag(
                    &data_type,
                    &value,
                    rows,
                    &CacheStrategy::Disabled,
                    &mut dictionary,
                )
                .unwrap();
                let expected =
                    repeated_vector_with_cache(&data_type, &value, rows, &CacheStrategy::Disabled)
                        .unwrap()
                        .to_arrow_array();
                assert_eq!(actual.to_data(), expected.to_data());
                arrays.push(actual);
            }
            let first = arrays[0]
                .as_any()
                .downcast_ref::<DictionaryArray<UInt32Type>>()
                .unwrap();
            let last = arrays[3]
                .as_any()
                .downcast_ref::<DictionaryArray<UInt32Type>>()
                .unwrap();
            if rows > MAX_VECTOR_LENGTH_TO_CACHE {
                assert_eq!(
                    first.keys().values().as_ptr(),
                    last.keys().values().as_ptr()
                );
            }
            assert_eq!(arrays[1].null_count(), rows);
            assert_eq!(arrays[2].null_count(), 0);
        }
    }

    fn series(table_id: u32, tsid: u64) -> MetricSeriesId {
        MetricSeriesId { table_id, tsid }
    }

    fn assigned_batch(
        partitions: usize,
        partition: usize,
        series: Vec<MetricSeriesId>,
    ) -> AssignedSeriesBatch {
        let mut collector = SeriesBatchCollector::new(partitions).unwrap();
        collector.push(series);
        collector.finish(true).remove(partition)
    }

    #[test]
    fn series_range_assignment_is_stable_across_batch_boundaries() {
        let input = vec![
            series(1, 0),
            series(2, 1u64 << 62),
            series(1, 1u64 << 63),
            series(2, 3u64 << 62),
            series(1, u64::MAX),
            series(3, 0),
        ];
        let mut first = SeriesBatchCollector::new(4).unwrap();
        first.push(input.clone());
        let first = first.finish(true);

        let mut second = SeriesBatchCollector::new(4).unwrap();
        for chunk in input.chunks(2) {
            second.push(chunk.to_vec());
        }
        let second = second.finish(true);

        assert_eq!(
            first
                .iter()
                .map(AssignedSeriesBatch::series)
                .collect::<Vec<_>>(),
            second
                .iter()
                .map(AssignedSeriesBatch::series)
                .collect::<Vec<_>>()
        );
        assert_eq!(&[series(1, 0), series(3, 0)], first[0].series());
        assert_eq!(&[series(2, 1u64 << 62)], first[1].series());
        assert_eq!(
            input.len(),
            first
                .iter()
                .map(|batch| batch.series().len())
                .sum::<usize>()
        );
    }

    #[test]
    fn collector_tracks_size_and_cache_policy() {
        let mut collector = SeriesBatchCollector::new(2).unwrap();
        collector.push(vec![series(1, 1), series(1, u64::MAX)]);
        collector.push(vec![series(2, 2)]);
        assert_eq!(3, collector.len());

        let assignments = collector.finish(false);
        assert_eq!(
            3,
            assignments
                .iter()
                .map(|batch| batch.series().len())
                .sum::<usize>()
        );
        assert!(assignments.iter().all(|batch| !batch.enable_range_cache()));
    }

    #[test]
    fn series_ranges_cover_the_tsid_domain() {
        let ranges = (0..3)
            .map(|partition| SeriesRange::new(partition, 3).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(0, ranges[0].start);
        assert_eq!(TSID_DOMAIN_END, ranges[2].end);
        assert_eq!(ranges[0].end, ranges[1].start);
        assert_eq!(ranges[1].end, ranges[2].start);
        assert_eq!(0, SeriesRange::partition_for(0, 3));
        assert_eq!(2, SeriesRange::partition_for(u64::MAX, 3));

        for (partition, range) in ranges.iter().enumerate() {
            assert_eq!(partition, SeriesRange::partition_for(range.start as u64, 3));
            if range.end < TSID_DOMAIN_END {
                assert_eq!(
                    partition + 1,
                    SeriesRange::partition_for(range.end as u64, 3)
                );
            }
        }
    }

    #[test]
    fn metric_series_filter_matches_encoded_primary_key() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let assigned = assigned_batch(1, 0, vec![series(1, 10), series(2, 20)]);
        let filter = MetricSeriesFilter::new(&assigned);
        let codec = SparsePrimaryKeyCodec::new(&metadata);
        let mut primary_key_filter = filter.primary_key_filter(codec);

        for selected in [series(1, 10), series(2, 20)] {
            assert!(
                primary_key_filter
                    .matches(&encode_series(&metadata, selected.table_id, selected.tsid))
                    .context(DecodeSnafu)
                    .unwrap()
            );
        }
        for unselected in [series(2, 10), series(1, 20)] {
            assert!(
                !primary_key_filter
                    .matches(&encode_series(
                        &metadata,
                        unselected.table_id,
                        unselected.tsid,
                    ))
                    .context(DecodeSnafu)
                    .unwrap()
            );
        }
    }

    fn encode_series(
        metadata: &store_api::metadata::RegionMetadataRef,
        table_id: u32,
        tsid: u64,
    ) -> Vec<u8> {
        let codec = SparsePrimaryKeyCodec::new(metadata);
        let mut primary_key = Vec::new();
        codec
            .encode_internal(table_id, tsid, &mut primary_key)
            .unwrap();
        primary_key
    }

    #[test]
    fn series_range_overlaps_single_table_primary_key_bounds() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let assigned = assigned_batch(2, 0, vec![series(1, 10), series(2, 20)]);
        let filter = MetricSeriesFilter::new(&assigned);
        let codec = SparsePrimaryKeyCodec::new(&metadata);

        // The full assigned range overlaps even though the actual candidate bounds do not.
        assert_eq!(
            Some(true),
            filter.overlaps_encoded_bounds(
                &codec,
                &encode_series(&metadata, 1, 100),
                &encode_series(&metadata, 1, 200),
            )
        );
        assert_eq!(
            Some(false),
            filter.overlaps_encoded_bounds(
                &codec,
                &encode_series(&metadata, 1, 1u64 << 63),
                &encode_series(&metadata, 1, u64::MAX),
            )
        );
        assert_eq!(
            Some(true),
            filter.overlaps_encoded_bounds(
                &codec,
                &encode_series(&metadata, 3, 10),
                &encode_series(&metadata, 3, 20),
            )
        );
    }

    #[test]
    fn series_range_keeps_multiple_table_primary_key_bounds() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let assigned = assigned_batch(2, 0, vec![series(1, 10), series(2, 20)]);
        let filter = MetricSeriesFilter::new(&assigned);
        let codec = SparsePrimaryKeyCodec::new(&metadata);

        assert_eq!(
            Some(true),
            filter.overlaps_encoded_bounds(
                &codec,
                &encode_series(&metadata, 1, 1u64 << 63),
                &encode_series(&metadata, 2, u64::MAX),
            )
        );
    }

    #[test]
    fn series_statistics_keep_invalid_or_inverted_bounds() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let assigned = assigned_batch(2, 0, vec![series(1, 10)]);
        let filter = MetricSeriesFilter::new(&assigned);
        let codec = SparsePrimaryKeyCodec::new(&metadata);

        assert_eq!(
            None,
            filter.overlaps_encoded_bounds(&codec, b"invalid", b"bounds")
        );
        assert_eq!(
            None,
            filter.overlaps_encoded_bounds(
                &codec,
                &encode_series(&metadata, 2, 0),
                &encode_series(&metadata, 1, 0),
            )
        );
    }
}
