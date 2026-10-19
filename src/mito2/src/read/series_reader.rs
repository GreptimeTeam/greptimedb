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
use store_api::storage::SequenceRange;
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
use crate::sst::parquet::row_group::ParquetFetchMetrics;

/// Immutable allow-list for one chunk of candidate metric series.
#[derive(Clone, Debug)]
struct MetricSeriesFilter {
    series: Arc<HashSet<MetricSeriesId>>,
    sorted_series: Arc<Vec<MetricSeriesId>>,
    enable_range_cache: bool,
}

impl MetricSeriesFilter {
    fn new(mut sorted_series: Vec<MetricSeriesId>, enable_range_cache: bool) -> Self {
        sorted_series.sort_unstable();
        sorted_series.dedup();
        let series = sorted_series.iter().copied().collect();
        Self {
            series: Arc::new(series),
            sorted_series: Arc::new(sorted_series),
            enable_range_cache,
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
        let first = self.sorted_series.partition_point(|series| *series < min);
        Some(
            self.sorted_series
                .get(first)
                .is_some_and(|series| *series <= max),
        )
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

/// Separate budgets for range work and buffered file-source reads.
///
/// Range builders and final merge tasks can wait for file-source output while
/// holding a range permit, so file sources must use an independent pool.
#[derive(Clone)]
pub(crate) struct SeriesReadSemaphores {
    pub(crate) range: Arc<Semaphore>,
    pub(crate) file: Arc<Semaphore>,
}

/// Reads one chunk of candidate metric series across all partition ranges.
pub(crate) struct SeriesReader {
    stream_ctx: Arc<StreamContext>,
    partition_ranges: Vec<PartitionRange>,
    filter: MetricSeriesFilter,
    codec: SparsePrimaryKeyCodec,
    partition_pruner: Arc<PartitionPruner>,
    semaphores: SeriesReadSemaphores,
    part_metrics: PartitionMetrics,
}

impl SeriesReader {
    /// Creates a reader for one chunk of candidate series.
    ///
    /// `partition_pruner` must come from the candidate scanner's pruner, which
    /// has predicate prefiltering disabled. The file read path applies precise
    /// filters with tag filtering skipped because candidate discovery has already
    /// enforced tag predicates through the exact candidate-series set.
    ///
    /// Range building and final merging share a budget; file sources use a
    /// separate budget so they can produce batches while a merge holds a permit.
    pub(crate) fn try_new(
        stream_ctx: Arc<StreamContext>,
        partition_ranges: Vec<PartitionRange>,
        series: Vec<MetricSeriesId>,
        enable_range_cache: bool,
        partition_pruner: Arc<PartitionPruner>,
        semaphores: SeriesReadSemaphores,
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

        let filter = MetricSeriesFilter::new(series, enable_range_cache);
        let codec = SparsePrimaryKeyCodec::new(stream_ctx.input.region_metadata());
        Ok(Self {
            stream_ctx,
            partition_ranges,
            filter,
            codec,
            partition_pruner,
            semaphores,
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
            let range_semaphore = self.semaphores.range.clone();
            let file_semaphore = self.semaphores.file.clone();
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
                    file_semaphore,
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
            Some(self.semaphores.range.clone()),
            Some(&self.part_metrics),
            true,
            compute_parallel_channel_size(estimated_batch_size),
        )
        .await
    }
}

async fn build_series_partition_range(
    stream_ctx: Arc<StreamContext>,
    part_range: PartitionRange,
    filter: MetricSeriesFilter,
    codec: SparsePrimaryKeyCodec,
    partition_pruner: Arc<PartitionPruner>,
    part_metrics: PartitionMetrics,
    file_semaphore: Arc<Semaphore>,
) -> Result<(BoxedRecordBatchStream, usize)> {
    let cache_key = filter
        .enable_range_cache
        .then(|| build_series_range_cache_key(&stream_ctx, &part_range))
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
        Some(file_semaphore),
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

    fn series(table_id: u32, tsid: u64) -> MetricSeriesId {
        MetricSeriesId { table_id, tsid }
    }

    #[test]
    fn metric_series_filter_matches_encoded_primary_key() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let filter = MetricSeriesFilter::new(vec![series(1, 10), series(2, 20)], true);
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
    fn series_filter_overlaps_single_table_primary_key_bounds() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let filter = MetricSeriesFilter::new(vec![series(1, 10), series(2, 20)], true);
        let codec = SparsePrimaryKeyCodec::new(&metadata);

        assert_eq!(
            Some(true),
            filter.overlaps_encoded_bounds(
                &codec,
                &encode_series(&metadata, 1, 0),
                &encode_series(&metadata, 1, 10),
            )
        );
        assert_eq!(
            Some(false),
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
            Some(false),
            filter.overlaps_encoded_bounds(
                &codec,
                &encode_series(&metadata, 3, 10),
                &encode_series(&metadata, 3, 20),
            )
        );
    }

    #[test]
    fn series_filter_overlaps_multiple_table_primary_key_bounds() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let filter = MetricSeriesFilter::new(vec![series(1, 10), series(2, 20)], true);
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
        let filter = MetricSeriesFilter::new(vec![series(1, 10)], true);
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
