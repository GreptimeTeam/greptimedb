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
use std::sync::Arc;
use std::time::Instant;

use async_stream::try_stream;
use futures::TryStreamExt;
use mito_codec::row_converter::{PrimaryKeyFilter, SparsePrimaryKeyCodec};
use snafu::ResultExt;
use store_api::region_engine::PartitionRange;
use tokio::sync::Semaphore;

#[cfg(feature = "enterprise")]
use crate::error::InvalidRequestSnafu;
use crate::error::{JoinSnafu, Result, UnexpectedSnafu};
use crate::read::BoxedRecordBatchStream;
use crate::read::pruner::PartitionPruner;
use crate::read::range_cache::{
    build_series_range_cache_key, cache_flat_range_stream, cached_flat_range_stream,
};
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::{
    PartitionMetrics, SplitRecordBatchStream, compute_average_batch_size,
    compute_parallel_channel_size, new_filter_metrics, scan_flat_mem_ranges,
    should_split_flat_batches_for_merge,
};
use crate::read::seq_scan::SeqScan;
use crate::read::series_candidate::{MetricSeriesId, validate_metric_metadata};
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::parquet::flat_format::primary_key_column_index;
use crate::sst::parquet::prefilter::prefilter_flat_batch_by_primary_key;
use crate::sst::parquet::reader::ReaderMetrics;
use crate::sst::parquet::row_group::ParquetFetchMetrics;

const TSID_DOMAIN_END: u128 = 1u128 << u64::BITS;

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
}

#[allow(dead_code)]
impl AssignedSeriesBatch {
    fn new(range: SeriesRange, series: Vec<MetricSeriesId>) -> Self {
        Self { range, series }
    }

    pub(crate) fn range(&self) -> SeriesRange {
        self.range
    }

    pub(crate) fn series(&self) -> &[MetricSeriesId] {
        &self.series
    }
}

/// Collects candidate batches and assigns every TSID by its integer range.
#[allow(dead_code)]
pub(crate) struct SeriesBatchCollector {
    assignments: Vec<Vec<MetricSeriesId>>,
}

#[allow(dead_code)]
impl SeriesBatchCollector {
    pub(crate) fn new(partitions: usize) -> Option<Self> {
        (partitions > 0).then(|| Self {
            assignments: (0..partitions).map(|_| Vec::new()).collect(),
        })
    }

    pub(crate) fn push(&mut self, batch: Vec<MetricSeriesId>) {
        let partitions = self.assignments.len();
        for series in batch {
            let partition = SeriesRange::partition_for(series.tsid, partitions);
            self.assignments[partition].push(series);
        }
    }

    pub(crate) fn finish(self) -> Vec<AssignedSeriesBatch> {
        let partitions = self.assignments.len();
        self.assignments
            .into_iter()
            .enumerate()
            .map(|(partition, series)| {
                AssignedSeriesBatch::new(SeriesRange::new(partition, partitions).unwrap(), series)
            })
            .collect()
    }
}

/// Immutable allow-list for one partition's metric series.
#[derive(Clone, Debug)]
struct MetricSeriesFilter {
    range: SeriesRange,
    series: Arc<HashSet<MetricSeriesId>>,
}

impl MetricSeriesFilter {
    fn new(assigned: &AssignedSeriesBatch) -> Self {
        let series = assigned.series.iter().copied().collect();
        Self {
            range: assigned.range,
            series: Arc::new(series),
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
#[allow(dead_code)]
pub(crate) struct SeriesReader {
    stream_ctx: Arc<StreamContext>,
    partition_ranges: Vec<PartitionRange>,
    range: SeriesRange,
    filter: MetricSeriesFilter,
    codec: SparsePrimaryKeyCodec,
    partition_pruner: Arc<PartitionPruner>,
    file_scan_semaphore: Arc<Semaphore>,
    final_merge_semaphore: Arc<Semaphore>,
    part_metrics: PartitionMetrics,
}

#[allow(dead_code)]
impl SeriesReader {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        stream_ctx: Arc<StreamContext>,
        partition_ranges: Vec<PartitionRange>,
        assigned_series: AssignedSeriesBatch,
        partition_pruner: Arc<PartitionPruner>,
        file_scan_semaphore: Arc<Semaphore>,
        final_merge_semaphore: Arc<Semaphore>,
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

        let range = assigned_series.range();
        let filter = MetricSeriesFilter::new(&assigned_series);
        let codec = SparsePrimaryKeyCodec::new(stream_ctx.input.region_metadata());
        Ok(Self {
            stream_ctx,
            partition_ranges,
            range,
            filter,
            codec,
            partition_pruner,
            file_scan_semaphore,
            final_merge_semaphore,
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
            let file_scan_semaphore = self.file_scan_semaphore.clone();
            let part_metrics = self.part_metrics.clone();
            let range = self.range;
            tasks.push(common_runtime::spawn_query(async move {
                build_series_partition_range(
                    stream_ctx,
                    part_range,
                    range,
                    filter,
                    codec,
                    partition_pruner,
                    file_scan_semaphore,
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

        let estimated_batch_size = compute_average_batch_size(estimated_batch_sizes);
        SeqScan::build_flat_reader_from_sources(
            &self.stream_ctx,
            range_streams,
            Some(self.final_merge_semaphore.clone()),
            Some(&self.part_metrics),
            true,
            compute_parallel_channel_size(estimated_batch_size),
        )
        .await
    }
}

#[allow(clippy::too_many_arguments)]
async fn build_series_partition_range(
    stream_ctx: Arc<StreamContext>,
    part_range: PartitionRange,
    range: SeriesRange,
    filter: MetricSeriesFilter,
    codec: SparsePrimaryKeyCodec,
    partition_pruner: Arc<PartitionPruner>,
    file_scan_semaphore: Arc<Semaphore>,
    part_metrics: PartitionMetrics,
) -> Result<(BoxedRecordBatchStream, usize)> {
    let cache_key = build_series_range_cache_key(&stream_ctx, &part_range, range);
    if let Some(key) = cache_key.as_ref() {
        if let Some(value) = stream_ctx.input.cache_strategy.get_range_result(key) {
            part_metrics.inc_range_cache_hit();
            return Ok((cached_flat_range_stream(value), DEFAULT_READ_BATCH_SIZE));
        }
        part_metrics.inc_range_cache_miss();
    }

    let range_meta = &stream_ctx.ranges[part_range.identifier];
    let split_batch_size = should_split_flat_batches_for_merge(&stream_ctx, range_meta);
    let mut ordered_sources = Vec::with_capacity(range_meta.row_group_indices.len());
    ordered_sources.resize_with(range_meta.row_group_indices.len(), || None);
    let mut file_tasks = Vec::new();

    for (position, index) in range_meta.row_group_indices.iter().copied().enumerate() {
        if stream_ctx.is_mem_range_index(index) {
            let stream = Box::pin(scan_flat_mem_ranges(
                stream_ctx.clone(),
                part_metrics.clone(),
                index,
                range_meta.time_range,
            ));
            ordered_sources[position] = Some(filter_flat_stream_by_series(
                stream,
                codec.clone(),
                filter.clone(),
            ));
            continue;
        }

        if stream_ctx.is_file_range_index(index) {
            let file = stream_ctx.input.file_from_index(index);
            if matches!(
                file.meta_ref()
                    .primary_key_range()
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

            let part_metrics = part_metrics.clone();
            let filter = filter.clone();
            let codec = codec.clone();
            let semaphore = file_scan_semaphore.clone();
            file_tasks.push(async move {
                let _permit = semaphore.acquire().await.map_err(|error| {
                    UnexpectedSnafu {
                        reason: format!("failed to acquire series file permit: {error}"),
                    }
                    .build()
                })?;
                let stream = scan_series_file_ranges(part_metrics, ranges, filter, codec);
                Ok::<_, crate::error::Error>((position, Box::pin(stream) as BoxedRecordBatchStream))
            });
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

    for (position, stream) in futures::future::try_join_all(file_tasks).await? {
        ordered_sources[position] = Some(stream);
    }

    let mut sources = ordered_sources.into_iter().flatten().collect::<Vec<_>>();
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
            let Some(mut reader) = range
                .reader_by_primary_key(
                    primary_key_filter.as_mut(),
                    fetch_metrics.as_deref(),
                )
                .await?
            else {
                continue;
            };
            let build_cost = build_start.elapsed();
            reader_metrics.build_cost += build_cost;
            part_metrics.inc_build_reader_cost(build_cost);

            let scan_start = Instant::now();
            while let Some(record_batch) = reader.next_batch().await? {
                reader_metrics.num_record_batches += 1;
                reader_metrics.num_batches += 1;
                reader_metrics.num_rows += record_batch.num_rows();

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
        collector.finish().remove(partition)
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
        let first = first.finish();

        let mut second = SeriesBatchCollector::new(4).unwrap();
        for chunk in input.chunks(2) {
            second.push(chunk.to_vec());
        }
        let second = second.finish();

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
