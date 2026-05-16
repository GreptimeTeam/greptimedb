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

//! Push decoder stream implementation for SST parquet files.

use std::ops::Range;

use bytes::{Bytes, BytesMut};
use datatypes::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::ObjectStore;
use parquet::DecodeResult;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, RowSelection};
use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
use snafu::{ResultExt, ensure};

use crate::cache::file_cache::{FileType, IndexKey};
use crate::cache::{CacheStrategy, PageRangePart};
use crate::error::{OpenDalSnafu, ReadParquetSnafu, Result, UnexpectedSnafu};
use crate::metrics::{READ_STAGE_ELAPSED, READ_STAGE_FETCH_PAGES};
use crate::sst::file::RegionFileId;
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::parquet::helper::fetch_byte_ranges;
use crate::sst::parquet::row_group::{ParquetFetchMetrics, compute_total_range_size};

/// Fetches parquet byte ranges through Greptime's cache hierarchy.
///
/// The push decoder decides which ranges are required for decoding, while this
/// fetcher keeps cache lookup, local write-cache reads, and remote I/O explicit
/// in Greptime code.
pub(crate) struct SstParquetRangeFetcher {
    /// Region file ID for cache key.
    region_file_id: RegionFileId,
    /// Path to the parquet file in object storage.
    file_path: String,
    /// Object store for reading data.
    object_store: ObjectStore,
    /// Cache strategy for reading pages.
    cache_strategy: CacheStrategy,
    /// Row group index for cache key.
    row_group_idx: usize,
    /// Optional metrics for tracking fetch operations.
    fetch_metrics: Option<ParquetFetchMetrics>,
}

impl SstParquetRangeFetcher {
    /// Creates a new [SstParquetRangeFetcher].
    pub(crate) fn new(
        region_file_id: RegionFileId,
        file_path: String,
        object_store: ObjectStore,
        cache_strategy: CacheStrategy,
        row_group_idx: usize,
        fetch_metrics: Option<ParquetFetchMetrics>,
    ) -> Self {
        Self {
            region_file_id,
            file_path,
            object_store,
            cache_strategy,
            row_group_idx,
            fetch_metrics,
        }
    }

    /// Fetches byte ranges from page cache, write cache, or object store.
    async fn fetch_bytes_with_cache(&self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        let fetch_start = self
            .fetch_metrics
            .as_ref()
            .map(|_| std::time::Instant::now());
        let _timer = READ_STAGE_FETCH_PAGES.start_timer();

        let mut page_lookup = self.cache_strategy.get_page_ranges(
            self.region_file_id.file_id(),
            self.row_group_idx,
            &ranges,
        );
        if let Some(lookup) = &page_lookup
            && lookup.cached_bytes > 0
            && let Some(metrics) = &self.fetch_metrics
        {
            let mut metrics_data = metrics.data.lock().unwrap();
            metrics_data.page_cache_hit += 1;
            metrics_data.pages_to_fetch_mem += lookup.cached_range_count;
            metrics_data.page_size_to_fetch_mem += lookup.cached_bytes;
            metrics_data.page_size_needed += lookup.cached_bytes;
        }

        // Fast path: all requested ranges can be assembled from cached fragments.
        if page_lookup
            .as_ref()
            .map(|lookup| lookup.is_fully_cached())
            .unwrap_or(false)
        {
            let lookup = page_lookup.take().unwrap();
            if let Some(metrics) = &self.fetch_metrics
                && let Some(start) = fetch_start
            {
                metrics.data.lock().unwrap().total_fetch_elapsed += start.elapsed();
            }
            return assemble_ranges(&ranges, lookup.cached_parts, &[]);
        }

        let missing_ranges = page_lookup
            .as_ref()
            .map(|lookup| lookup.missing_ranges.clone())
            .unwrap_or_else(|| ranges.clone());

        // Calculate total range size for metrics.
        let (_, unaligned_size) = compute_total_range_size(&missing_ranges);

        // Check write cache.
        let key = IndexKey::new(
            self.region_file_id.region_id(),
            self.region_file_id.file_id(),
            FileType::Parquet,
        );
        let fetch_write_cache_start = self
            .fetch_metrics
            .as_ref()
            .map(|_| std::time::Instant::now());
        let write_cache_result = match self.cache_strategy.write_cache() {
            Some(cache) => cache.file_cache().read_ranges(key, &missing_ranges).await,
            None => None,
        };

        let fetched_pages = match write_cache_result {
            Some(data) => {
                if let Some(metrics) = &self.fetch_metrics {
                    let elapsed = fetch_write_cache_start
                        .map(|start| start.elapsed())
                        .unwrap_or_default();
                    let range_size_needed: u64 =
                        missing_ranges.iter().map(|r| r.end - r.start).sum();
                    let mut metrics_data = metrics.data.lock().unwrap();
                    metrics_data.write_cache_fetch_elapsed += elapsed;
                    metrics_data.write_cache_hit += 1;
                    metrics_data.pages_to_fetch_write_cache += missing_ranges.len();
                    metrics_data.page_size_to_fetch_write_cache += unaligned_size;
                    metrics_data.page_size_needed += range_size_needed;
                }
                data
            }
            None => {
                // Fetch data from object store.
                let _timer = READ_STAGE_ELAPSED
                    .with_label_values(&["cache_miss_read"])
                    .start_timer();

                let start = self
                    .fetch_metrics
                    .as_ref()
                    .map(|_| std::time::Instant::now());
                let data =
                    fetch_byte_ranges(&self.file_path, self.object_store.clone(), &missing_ranges)
                        .await
                        .context(OpenDalSnafu)?;

                if let Some(metrics) = &self.fetch_metrics {
                    let elapsed = start.map(|start| start.elapsed()).unwrap_or_default();
                    let range_size_needed: u64 =
                        missing_ranges.iter().map(|r| r.end - r.start).sum();
                    let mut metrics_data = metrics.data.lock().unwrap();
                    metrics_data.store_fetch_elapsed += elapsed;
                    metrics_data.cache_miss += 1;
                    metrics_data.pages_to_fetch_store += missing_ranges.len();
                    metrics_data.page_size_to_fetch_store += unaligned_size;
                    metrics_data.page_size_needed += range_size_needed;
                }
                data
            }
        };

        self.cache_strategy.put_page_ranges(
            self.region_file_id.file_id(),
            self.row_group_idx,
            &missing_ranges,
            &fetched_pages,
        );

        if let (Some(metrics), Some(start)) = (&self.fetch_metrics, fetch_start) {
            metrics.data.lock().unwrap().total_fetch_elapsed += start.elapsed();
        }

        if let Some(lookup) = page_lookup {
            let fetched_parts = missing_ranges
                .into_iter()
                .zip(fetched_pages)
                .map(|(range, bytes)| PageRangePart { range, bytes })
                .collect::<Vec<_>>();
            return assemble_ranges(&ranges, lookup.cached_parts, &fetched_parts);
        }

        Ok(fetched_pages)
    }
}

fn assemble_ranges(
    ranges: &[Range<u64>],
    cached_parts: Vec<Vec<PageRangePart>>,
    fetched_parts: &[PageRangePart],
) -> Result<Vec<Bytes>> {
    ensure!(
        ranges.len() == cached_parts.len(),
        UnexpectedSnafu {
            reason: format!(
                "Invalid parquet range assembly: {} requested ranges but {} cached part groups",
                ranges.len(),
                cached_parts.len()
            ),
        }
    );

    ranges
        .iter()
        .zip(cached_parts)
        .map(|(range, mut parts)| {
            parts.extend(
                fetched_parts
                    .iter()
                    .filter_map(|part| overlapping_part(range, part)),
            );
            assemble_range(range, parts)
        })
        .collect()
}

fn overlapping_part(range: &Range<u64>, part: &PageRangePart) -> Option<PageRangePart> {
    let start = range.start.max(part.range.start);
    let end = range.end.min(part.range.end);
    if start >= end {
        return None;
    }

    let slice_start = (start - part.range.start) as usize;
    let slice_end = (end - part.range.start) as usize;
    Some(PageRangePart {
        range: start..end,
        bytes: part.bytes.slice(slice_start..slice_end),
    })
}

fn assemble_range(range: &Range<u64>, mut parts: Vec<PageRangePart>) -> Result<Bytes> {
    if range.start >= range.end {
        return Ok(Bytes::new());
    }

    parts.sort_unstable_by_key(|part| part.range.start);
    if parts.len() == 1 && parts[0].range == *range {
        return Ok(parts.pop().unwrap().bytes);
    }

    let mut cursor = range.start;
    let mut output = BytesMut::with_capacity((range.end - range.start) as usize);
    for part in parts {
        ensure!(
            part.range.start <= cursor,
            UnexpectedSnafu {
                reason: format!(
                    "Missing cached parquet bytes for range {}..{}, next part starts at {}",
                    range.start, range.end, part.range.start
                ),
            }
        );
        if part.range.end <= cursor {
            continue;
        }

        let slice_start = (cursor - part.range.start) as usize;
        output.extend_from_slice(&part.bytes.slice(slice_start..));
        cursor = part.range.end.min(range.end);
        if cursor >= range.end {
            break;
        }
    }

    ensure!(
        cursor == range.end,
        UnexpectedSnafu {
            reason: format!(
                "Missing cached parquet bytes for range {}..{}, assembled through {}",
                range.start, range.end, cursor
            ),
        }
    );

    Ok(output.freeze())
}

/// Builds a parquet record batch stream driven directly by [ParquetPushDecoderBuilder].
pub(crate) fn build_sst_parquet_record_batch_stream(
    arrow_metadata: ArrowReaderMetadata,
    row_group_idx: usize,
    row_selection: Option<RowSelection>,
    projection: ProjectionMask,
    fetcher: SstParquetRangeFetcher,
    file_path: String,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let mut builder = ParquetPushDecoderBuilder::new_with_metadata(arrow_metadata)
        .with_row_groups(vec![row_group_idx])
        .with_projection(projection)
        .with_batch_size(DEFAULT_READ_BATCH_SIZE);

    if let Some(selection) = row_selection {
        builder = builder.with_row_selection(selection);
    }

    let mut decoder = builder
        .build()
        .context(ReadParquetSnafu { path: &file_path })?;

    Ok(async_stream::try_stream! {
        loop {
            match decoder.try_decode().context(ReadParquetSnafu { path: &file_path })? {
                DecodeResult::NeedsData(ranges) => {
                    let data = fetcher.fetch_bytes_with_cache(ranges.clone()).await?;
                    decoder
                        .push_ranges(ranges, data)
                        .context(ReadParquetSnafu { path: &file_path })?;
                }
                DecodeResult::Data(batch) => yield batch,
                DecodeResult::Finished => break,
            }
        }
    }
    .boxed())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assemble_range_from_cached_subrange_and_fetched_tail() {
        let cached_parts = vec![vec![PageRangePart {
            range: 400..500,
            bytes: Bytes::from(vec![1; 100]),
        }]];
        let fetched_parts = vec![PageRangePart {
            range: 500..600,
            bytes: Bytes::from(vec![2; 100]),
        }];

        let requested = 400..600;
        let output = assemble_ranges(
            std::slice::from_ref(&requested),
            cached_parts,
            &fetched_parts,
        )
        .unwrap();
        assert_eq!(1, output.len());
        assert_eq!(vec![1; 100].as_slice(), &output[0][..100]);
        assert_eq!(vec![2; 100].as_slice(), &output[0][100..]);
    }

    #[test]
    fn test_assemble_range_returns_single_covering_part_without_copy() {
        let bytes = Bytes::from_static(b"abcdef");
        let cached_parts = vec![vec![PageRangePart {
            range: 10..16,
            bytes: bytes.clone(),
        }]];

        let requested = 10..16;
        let output = assemble_ranges(std::slice::from_ref(&requested), cached_parts, &[]).unwrap();
        assert_eq!(bytes, output[0]);
    }
}
