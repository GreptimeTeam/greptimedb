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

//! Sequential scan.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_stream::try_stream;
use common_error::ext::BoxedError;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::util::ChainedRecordBatchStream;
use common_recordbatch::{RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_telemetry::{debug, tracing};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datatypes::schema::SchemaRef;
use smallvec::smallvec;
use snafu::ResultExt;
use store_api::region_engine::{PartitionRange, RegionScanner, ScannerProperties};
use store_api::storage::{ColumnId, TimeSeriesRowSelector};
use table::predicate::Predicate;
use tokio::sync::Semaphore;

use crate::error::{PartitionOutOfRangeSnafu, Result};
use crate::memtable::MemtableRef;
use crate::read::dedup::{DedupReader, LastNonNull, LastRow};
use crate::read::last_row::LastRowReader;
use crate::read::merge::MergeReaderBuilder;
use crate::read::scan_region::{
    FileRangeCollector, ScanInput, ScanPart, ScanPartList, StreamContext,
};
use crate::read::{BatchReader, BoxedBatchReader, ScannerMetrics, Source};
use crate::region::options::MergeMode;
use crate::sst::file::FileMeta;
use crate::sst::parquet::file_range::FileRange;
use crate::sst::parquet::reader::ReaderMetrics;

/// Scans a region and returns rows in a sorted sequence.
///
/// The output order is always `order by primary keys, time index` inside every
/// [`PartitionRange`]. Each "partition" may contains many [`PartitionRange`]s.
pub struct SeqScan {
    /// Properties of the scanner.
    properties: ScannerProperties,
    /// Context of streams.
    stream_ctx: Arc<StreamContext>,
    /// Semaphore to control scan parallelism of files.
    /// Streams created by the scanner share the same semaphore.
    semaphore: Arc<Semaphore>,
    /// The scanner is used for compaction.
    compaction: bool,
}

impl SeqScan {
    /// Creates a new [SeqScan].
    pub(crate) fn new(input: ScanInput) -> Self {
        let parallelism = input.parallelism.parallelism.max(1);
        let mut properties = ScannerProperties::default()
            .with_parallelism(parallelism)
            .with_append_mode(input.append_mode)
            .with_total_rows(input.total_rows());
        properties.partitions = vec![input.partition_ranges()];
        let stream_ctx = Arc::new(StreamContext::new(input));

        Self {
            properties,
            stream_ctx,
            semaphore: Arc::new(Semaphore::new(parallelism)),
            compaction: false,
        }
    }

    /// Sets the scanner to be used for compaction.
    pub(crate) fn with_compaction(mut self) -> Self {
        self.compaction = true;
        self
    }

    /// Builds a stream for the query.
    ///
    /// The returned stream is not partitioned and will contains all the data. If want
    /// partitioned scan, use [`RegionScanner::scan_partition`].
    pub fn build_stream(&self) -> Result<SendableRecordBatchStream, BoxedError> {
        let streams = (0..self.properties.partitions.len())
            .map(|partition: usize| self.scan_partition(partition))
            .collect::<Result<Vec<_>, _>>()?;

        let aggr_stream = ChainedRecordBatchStream::new(streams).map_err(BoxedError::new)?;
        Ok(Box::pin(aggr_stream))
    }

    /// Builds a [BoxedBatchReader] from sequential scan for compaction.
    pub async fn build_reader(&self) -> Result<BoxedBatchReader> {
        let mut metrics = ScannerMetrics {
            prepare_scan_cost: self.stream_ctx.query_start.elapsed(),
            ..Default::default()
        };
        let maybe_reader = Self::build_all_merge_reader(
            &self.stream_ctx,
            self.semaphore.clone(),
            &mut metrics,
            self.compaction,
            self.properties.num_partitions(),
        )
        .await?;
        // Safety: `build_merge_reader()` always returns a reader if partition is None.
        let reader = maybe_reader.unwrap();
        Ok(Box::new(reader))
    }

    /// Builds sources from a [ScanPart].
    fn build_part_sources(
        part: &ScanPart,
        sources: &mut Vec<Source>,
        row_selector: Option<TimeSeriesRowSelector>,
        compaction: bool,
    ) -> Result<()> {
        sources.reserve(part.memtable_ranges.len() + part.file_ranges.len());
        // Read memtables.
        for mem in &part.memtable_ranges {
            let iter = mem.build_iter()?;
            sources.push(Source::Iter(iter));
        }
        let read_type = if compaction {
            "compaction"
        } else {
            "seq_scan_files"
        };
        // Read files.
        for file in &part.file_ranges {
            if file.is_empty() {
                continue;
            }

            // Creates a stream to read the file.
            let ranges = file.clone();
            let stream = try_stream! {
                let mut reader_metrics = ReaderMetrics::default();
                // Safety: We checked whether it is empty before.
                let file_id = ranges[0].file_handle().file_id();
                let region_id = ranges[0].file_handle().region_id();
                let range_num = ranges.len();
                for range in ranges {
                    let mut reader = range.reader(row_selector).await?;
                    let compat_batch = range.compat_batch();
                    while let Some(mut batch) = reader.next_batch().await? {
                        if let Some(compat) = compat_batch {
                            batch = compat
                                .compat_batch(batch)?;
                        }

                        yield batch;
                    }
                    reader_metrics.merge_from(reader.metrics());
                }
                debug!(
                    "Seq scan region {}, file {}, {} ranges finished, metrics: {:?}, compaction: {}",
                    region_id, file_id, range_num, reader_metrics, compaction
                );
                // Reports metrics.
                reader_metrics.observe_rows(read_type);
            };
            let stream = Box::pin(stream);
            sources.push(Source::Stream(stream));
        }

        Ok(())
    }

    /// Builds a merge reader that reads all data.
    async fn build_all_merge_reader(
        stream_ctx: &StreamContext,
        semaphore: Arc<Semaphore>,
        metrics: &mut ScannerMetrics,
        compaction: bool,
        parallelism: usize,
    ) -> Result<Option<BoxedBatchReader>> {
        // initialize parts list
        let mut parts = stream_ctx.parts.lock().await;
        Self::maybe_init_parts(&stream_ctx.input, &mut parts, metrics, parallelism).await?;
        let parts_len = parts.0.len();

        let mut sources = Vec::with_capacity(parts_len);
        for id in 0..parts_len {
            let Some(part) = parts.0.get_part(id) else {
                return Ok(None);
            };

            Self::build_part_sources(part, &mut sources, None, compaction)?;
        }

        Self::build_reader_from_sources(stream_ctx, sources, semaphore).await
    }

    /// Builds a merge reader that reads data from one [`PartitionRange`].
    ///
    /// If the `range_id` is out of bound, returns None.
    async fn build_merge_reader(
        stream_ctx: &StreamContext,
        range_id: usize,
        semaphore: Arc<Semaphore>,
        metrics: &mut ScannerMetrics,
        compaction: bool,
        parallelism: usize,
    ) -> Result<Option<BoxedBatchReader>> {
        let mut sources = Vec::new();
        let build_start = {
            let mut parts = stream_ctx.parts.lock().await;
            Self::maybe_init_parts(&stream_ctx.input, &mut parts, metrics, parallelism).await?;

            let Some(part) = parts.0.get_part(range_id) else {
                return Ok(None);
            };

            let build_start = Instant::now();
            Self::build_part_sources(
                part,
                &mut sources,
                stream_ctx.input.series_row_selector,
                compaction,
            )?;

            build_start
        };

        let maybe_reader = Self::build_reader_from_sources(stream_ctx, sources, semaphore).await;
        let build_reader_cost = build_start.elapsed();
        metrics.build_reader_cost += build_reader_cost;
        debug!(
            "Build reader region: {}, range_id: {}, from sources, build_reader_cost: {:?}, compaction: {}",
            stream_ctx.input.mapper.metadata().region_id,
            range_id,
            build_reader_cost,
            compaction,
        );

        maybe_reader
    }

    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    async fn build_reader_from_sources(
        stream_ctx: &StreamContext,
        mut sources: Vec<Source>,
        semaphore: Arc<Semaphore>,
    ) -> Result<Option<BoxedBatchReader>> {
        if stream_ctx.input.parallelism.parallelism > 1 {
            // Read sources in parallel. We always spawn a task so we can control the parallelism
            // by the semaphore.
            sources = stream_ctx
                .input
                .create_parallel_sources(sources, semaphore.clone())?;
        }

        let mut builder = MergeReaderBuilder::from_sources(sources);
        let reader = builder.build().await?;

        let dedup = !stream_ctx.input.append_mode;
        let reader = if dedup {
            match stream_ctx.input.merge_mode {
                MergeMode::LastRow => Box::new(DedupReader::new(
                    reader,
                    LastRow::new(stream_ctx.input.filter_deleted),
                )) as _,
                MergeMode::LastNonNull => Box::new(DedupReader::new(
                    reader,
                    LastNonNull::new(stream_ctx.input.filter_deleted),
                )) as _,
            }
        } else {
            Box::new(reader) as _
        };

        let reader = match &stream_ctx.input.series_row_selector {
            Some(TimeSeriesRowSelector::LastRow) => Box::new(LastRowReader::new(reader)) as _,
            None => reader,
        };

        Ok(Some(reader))
    }

    /// Scans the given partition when the part list is set properly.
    /// Otherwise the returned stream might not contains any data.
    // TODO: refactor out `uncached_scan_part_impl`.
    #[allow(dead_code)]
    fn scan_partition_impl(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, BoxedError> {
        if partition >= self.properties.partitions.len() {
            return Err(BoxedError::new(
                PartitionOutOfRangeSnafu {
                    given: partition,
                    all: self.properties.partitions.len(),
                }
                .build(),
            ));
        }

        let mut metrics = ScannerMetrics {
            prepare_scan_cost: self.stream_ctx.query_start.elapsed(),
            ..Default::default()
        };
        let stream_ctx = self.stream_ctx.clone();
        let semaphore = self.semaphore.clone();
        let partition_ranges = self.properties.partitions[partition].clone();
        let compaction = self.compaction;
        let parallelism = self.properties.num_partitions();
        let stream = try_stream! {
            let first_poll = stream_ctx.query_start.elapsed();

            for partition_range in partition_ranges {
                let maybe_reader =
                    Self::build_merge_reader(&stream_ctx, partition_range.identifier, semaphore.clone(), &mut metrics, compaction, parallelism)
                        .await
                        .map_err(BoxedError::new)
                        .context(ExternalSnafu)?;
                let Some(mut reader) = maybe_reader else {
                    return;
                };
                let cache = stream_ctx.input.cache_manager.as_deref();
                let mut fetch_start = Instant::now();
                while let Some(batch) = reader
                    .next_batch()
                    .await
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?
                {
                    metrics.scan_cost += fetch_start.elapsed();
                    metrics.num_batches += 1;
                    metrics.num_rows += batch.num_rows();

                    let convert_start = Instant::now();
                    let record_batch = stream_ctx.input.mapper.convert(&batch, cache)?;
                    metrics.convert_cost += convert_start.elapsed();
                    let yield_start = Instant::now();
                    yield record_batch;
                    metrics.yield_cost += yield_start.elapsed();

                    fetch_start = Instant::now();
                }
                metrics.scan_cost += fetch_start.elapsed();
                metrics.total_cost = stream_ctx.query_start.elapsed();
                metrics.observe_metrics_on_finish();

                debug!(
                    "Seq scan finished, region_id: {:?}, partition: {}, metrics: {:?}, first_poll: {:?}, compaction: {}",
                    stream_ctx.input.mapper.metadata().region_id,
                    partition,
                    metrics,
                    first_poll,
                    compaction,
                );
            }
        };

        let stream = Box::pin(RecordBatchStreamWrapper::new(
            self.stream_ctx.input.mapper.output_schema(),
            Box::pin(stream),
        ));

        Ok(stream)
    }

    /// Scans the given partition when the part list is not set.
    /// This method will do a lazy initialize of part list and
    /// ignores the partition settings in `properties`.
    fn uncached_scan_part_impl(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, BoxedError> {
        let num_partitions = self.properties.partitions.len();
        if partition >= num_partitions {
            return Err(BoxedError::new(
                PartitionOutOfRangeSnafu {
                    given: partition,
                    all: self.properties.partitions.len(),
                }
                .build(),
            ));
        }
        let mut metrics = ScannerMetrics {
            prepare_scan_cost: self.stream_ctx.query_start.elapsed(),
            ..Default::default()
        };
        let stream_ctx = self.stream_ctx.clone();
        let semaphore = self.semaphore.clone();
        let compaction = self.compaction;
        let parallelism = self.properties.num_partitions();

        // build stream
        let stream = try_stream! {
            let first_poll = stream_ctx.query_start.elapsed();

            // init parts
            let parts_len = {
                let mut parts = stream_ctx.parts.lock().await;
                Self::maybe_init_parts(&stream_ctx.input, &mut parts, &mut metrics, parallelism).await
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?;
                parts.0.len()
            };

            for id in (0..parts_len).skip(partition).step_by(num_partitions) {
                let maybe_reader = Self::build_merge_reader(
                    &stream_ctx,
                    id,
                    semaphore.clone(),
                    &mut metrics,
                    compaction,
                    parallelism
                )
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
                let Some(mut reader) = maybe_reader else {
                    return;
                };
                let cache = stream_ctx.input.cache_manager.as_deref();
                let mut fetch_start = Instant::now();
                while let Some(batch) = reader
                    .next_batch()
                    .await
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?
                {
                    metrics.scan_cost += fetch_start.elapsed();
                    metrics.num_batches += 1;
                    metrics.num_rows += batch.num_rows();

                    let convert_start = Instant::now();
                    let record_batch = stream_ctx.input.mapper.convert(&batch, cache)?;
                    metrics.convert_cost += convert_start.elapsed();
                    let yield_start = Instant::now();
                    yield record_batch;
                    metrics.yield_cost += yield_start.elapsed();

                    fetch_start = Instant::now();
                }
                metrics.scan_cost += fetch_start.elapsed();
                metrics.total_cost = stream_ctx.query_start.elapsed();
                metrics.observe_metrics_on_finish();

                debug!(
                    "Seq scan finished, region_id: {}, partition: {}, id: {}, metrics: {:?}, first_poll: {:?}, compaction: {}",
                    stream_ctx.input.mapper.metadata().region_id,
                    partition,
                    id,
                    metrics,
                    first_poll,
                    compaction,
                );
            }
        };

        let stream = Box::pin(RecordBatchStreamWrapper::new(
            self.stream_ctx.input.mapper.output_schema(),
            Box::pin(stream),
        ));

        Ok(stream)
    }

    /// Initializes parts if they are not built yet.
    async fn maybe_init_parts(
        input: &ScanInput,
        part_list: &mut (ScanPartList, Duration),
        metrics: &mut ScannerMetrics,
        parallelism: usize,
    ) -> Result<()> {
        if part_list.0.is_none() {
            let now = Instant::now();
            let mut distributor = SeqDistributor::default();
            let reader_metrics = input.prune_file_ranges(&mut distributor).await?;
            distributor.append_mem_ranges(
                &input.memtables,
                Some(input.mapper.column_ids()),
                input.predicate.clone(),
            );
            part_list.0.set_parts(distributor.build_parts(parallelism));
            let build_part_cost = now.elapsed();
            part_list.1 = build_part_cost;

            metrics.observe_init_part(build_part_cost, &reader_metrics);
        } else {
            // Updates the cost of building parts.
            metrics.build_parts_cost = part_list.1;
        }
        Ok(())
    }
}

impl RegionScanner for SeqScan {
    fn properties(&self) -> &ScannerProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.stream_ctx.input.mapper.output_schema()
    }

    fn scan_partition(&self, partition: usize) -> Result<SendableRecordBatchStream, BoxedError> {
        self.uncached_scan_part_impl(partition)
    }

    fn prepare(&mut self, ranges: Vec<Vec<PartitionRange>>) -> Result<(), BoxedError> {
        self.properties.partitions = ranges;
        Ok(())
    }

    fn has_predicate(&self) -> bool {
        let predicate = self.stream_ctx.input.predicate();
        predicate.map(|p| !p.exprs().is_empty()).unwrap_or(false)
    }
}

impl DisplayAs for SeqScan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SeqScan: region={}, ",
            self.stream_ctx.input.mapper.metadata().region_id
        )?;
        self.stream_ctx.format_for_explain(t, f)
    }
}

impl fmt::Debug for SeqScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeqScan")
            .field("parts", &self.stream_ctx.parts)
            .finish()
    }
}

#[cfg(test)]
impl SeqScan {
    /// Returns the input.
    pub(crate) fn input(&self) -> &ScanInput {
        &self.stream_ctx.input
    }
}

/// Builds [ScanPart]s that preserves order.
#[derive(Default)]
pub(crate) struct SeqDistributor {
    parts: Vec<ScanPart>,
}

impl FileRangeCollector for SeqDistributor {
    fn append_file_ranges(
        &mut self,
        file_meta: &FileMeta,
        file_ranges: impl Iterator<Item = FileRange>,
    ) {
        // Creates a [ScanPart] for each file.
        let ranges: Vec<_> = file_ranges.collect();
        if ranges.is_empty() {
            // No ranges to read.
            return;
        }
        let part = ScanPart {
            memtable_ranges: Vec::new(),
            file_ranges: smallvec![ranges],
            time_range: Some(file_meta.time_range),
        };
        self.parts.push(part);
    }
}

impl SeqDistributor {
    /// Appends memtable ranges to the distributor.
    fn append_mem_ranges(
        &mut self,
        memtables: &[MemtableRef],
        projection: Option<&[ColumnId]>,
        predicate: Option<Predicate>,
    ) {
        for mem in memtables {
            let stats = mem.stats();
            let mem_ranges = mem.ranges(projection, predicate.clone());
            if mem_ranges.is_empty() {
                continue;
            }
            let part = ScanPart {
                memtable_ranges: mem_ranges,
                file_ranges: smallvec![],
                time_range: stats.time_range(),
            };
            self.parts.push(part);
        }
    }

    /// Groups file ranges and memtable ranges by time ranges.
    /// The output number of parts may be `<= parallelism`. If `parallelism` is 0, it will be set to 1.
    ///
    /// Output parts have non-overlapping time ranges.
    fn build_parts(self, parallelism: usize) -> Vec<ScanPart> {
        let parallelism = parallelism.max(1);
        let parts = group_parts_by_range(self.parts);
        let parts = maybe_split_parts(parts, parallelism);
        // Ensures it doesn't returns parts more than `parallelism`.
        maybe_merge_parts(parts, parallelism)
    }
}

/// Groups parts by time range. It may generate parts more than parallelism.
/// All time ranges are not None.
fn group_parts_by_range(mut parts: Vec<ScanPart>) -> Vec<ScanPart> {
    if parts.is_empty() {
        return Vec::new();
    }

    // Sorts parts by time range.
    parts.sort_unstable_by(|a, b| {
        // Safety: time ranges of parts from [SeqPartBuilder] are not None.
        let a = a.time_range.unwrap();
        let b = b.time_range.unwrap();
        a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1))
    });
    let mut part_in_range = None;
    // Parts with exclusive time ranges.
    let mut part_groups = Vec::new();
    for part in parts {
        let Some(mut prev_part) = part_in_range.take() else {
            part_in_range = Some(part);
            continue;
        };

        if prev_part.overlaps(&part) {
            prev_part.merge(part);
            part_in_range = Some(prev_part);
        } else {
            // A new group.
            part_groups.push(prev_part);
            part_in_range = Some(part);
        }
    }
    if let Some(part) = part_in_range {
        part_groups.push(part);
    }

    part_groups
}

/// Merges parts by parallelism.
/// It merges parts if the number of parts is greater than `parallelism`.
fn maybe_merge_parts(mut parts: Vec<ScanPart>, parallelism: usize) -> Vec<ScanPart> {
    assert!(parallelism > 0);
    if parts.len() <= parallelism {
        // No need to merge parts.
        return parts;
    }

    // Sort parts by number of memtables and ranges in reverse order.
    parts.sort_unstable_by(|a, b| {
        a.memtable_ranges
            .len()
            .cmp(&b.memtable_ranges.len())
            .then_with(|| {
                let a_ranges_len = a
                    .file_ranges
                    .iter()
                    .map(|ranges| ranges.len())
                    .sum::<usize>();
                let b_ranges_len = b
                    .file_ranges
                    .iter()
                    .map(|ranges| ranges.len())
                    .sum::<usize>();
                a_ranges_len.cmp(&b_ranges_len)
            })
            .reverse()
    });

    let parts_to_reduce = parts.len() - parallelism;
    for _ in 0..parts_to_reduce {
        // Safety: We ensure `parts.len() > parallelism`.
        let part = parts.pop().unwrap();
        parts.last_mut().unwrap().merge(part);
    }

    parts
}

/// Splits parts by parallelism.
/// It splits a part if it only scans one file and doesn't scan any memtable.
fn maybe_split_parts(mut parts: Vec<ScanPart>, parallelism: usize) -> Vec<ScanPart> {
    assert!(parallelism > 0);
    if parts.len() >= parallelism {
        // No need to split parts.
        return parts;
    }

    let has_part_to_split = parts.iter().any(|part| part.can_split_preserve_order());
    if !has_part_to_split {
        // No proper parts to scan.
        return parts;
    }

    // Sorts parts by the number of ranges in the first file.
    parts.sort_unstable_by(|a, b| {
        let a_len = a.file_ranges.first().map(|file| file.len()).unwrap_or(0);
        let b_len = b.file_ranges.first().map(|file| file.len()).unwrap_or(0);
        a_len.cmp(&b_len).reverse()
    });
    let num_parts_to_split = parallelism - parts.len();
    let mut output_parts = Vec::with_capacity(parallelism);
    // Split parts up to num_parts_to_split.
    for part in parts.iter_mut() {
        if !part.can_split_preserve_order() {
            continue;
        }
        // Safety: `can_split_preserve_order()` ensures file_ranges.len() == 1.
        // Splits part into `num_parts_to_split + 1` new parts if possible.
        let target_part_num = num_parts_to_split + 1;
        let ranges_per_part = (part.file_ranges[0].len() + target_part_num - 1) / target_part_num;
        // `can_split_preserve_order()` ensures part.file_ranges[0].len() > 1.
        assert!(ranges_per_part > 0);
        for ranges in part.file_ranges[0].chunks(ranges_per_part) {
            let new_part = ScanPart {
                memtable_ranges: Vec::new(),
                file_ranges: smallvec![ranges.to_vec()],
                time_range: part.time_range,
            };
            output_parts.push(new_part);
        }
        // Replace the current part with the last output part as we will put the current part
        // into the output parts later.
        *part = output_parts.pop().unwrap();
        if output_parts.len() >= num_parts_to_split {
            // We already split enough parts.
            break;
        }
    }
    // Put the remaining parts into the output parts.
    output_parts.append(&mut parts);

    output_parts
}

#[cfg(test)]
mod tests {
    use common_time::timestamp::TimeUnit;
    use common_time::Timestamp;

    use super::*;
    use crate::memtable::MemtableId;
    use crate::test_util::memtable_util::mem_range_for_test;

    type Output = (Vec<MemtableId>, i64, i64);

    fn run_group_parts_test(input: &[(MemtableId, i64, i64)], expect: &[Output]) {
        let parts = input
            .iter()
            .map(|(id, start, end)| {
                let range = (
                    Timestamp::new(*start, TimeUnit::Second),
                    Timestamp::new(*end, TimeUnit::Second),
                );
                ScanPart {
                    memtable_ranges: vec![mem_range_for_test(*id)],
                    file_ranges: smallvec![],
                    time_range: Some(range),
                }
            })
            .collect();
        let output = group_parts_by_range(parts);
        let actual: Vec<_> = output
            .iter()
            .map(|part| {
                let ids: Vec<_> = part.memtable_ranges.iter().map(|mem| mem.id()).collect();
                let range = part.time_range.unwrap();
                (ids, range.0.value(), range.1.value())
            })
            .collect();
        assert_eq!(expect, actual);
    }

    #[test]
    fn test_group_parts() {
        // Group 1 part.
        run_group_parts_test(&[(1, 0, 2000)], &[(vec![1], 0, 2000)]);

        // 1, 2, 3, 4 => [3, 1, 4], [2]
        run_group_parts_test(
            &[
                (1, 1000, 2000),
                (2, 6000, 7000),
                (3, 0, 1500),
                (4, 1500, 3000),
            ],
            &[(vec![3, 1, 4], 0, 3000), (vec![2], 6000, 7000)],
        );

        // 1, 2, 3 => [3], [1], [2],
        run_group_parts_test(
            &[(1, 3000, 4000), (2, 4001, 6000), (3, 0, 1000)],
            &[
                (vec![3], 0, 1000),
                (vec![1], 3000, 4000),
                (vec![2], 4001, 6000),
            ],
        );
    }
}
