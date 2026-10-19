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
use crate::cache::load::{CacheLoad, CacheWork};
use crate::cache::{CacheStrategy, PageRangePart};
use crate::error::{OpenDalSnafu, ReadParquetSnafu, Result, UnexpectedSnafu};
use crate::metrics::{
    CACHE_LOAD_BYTES, CACHE_LOAD_EVENTS, PARQUET_DECODE_OUTPUT, READ_STAGE_ELAPSED,
    READ_STAGE_FETCH_PAGES,
};
use crate::sst::file::RegionFileId;
use crate::sst::parquet::helper::fetch_byte_ranges;
use crate::sst::parquet::row_group::{ParquetFetchMetrics, compute_total_range_size};

/// Fetches parquet byte ranges through Greptime's cache hierarchy.
///
/// The push decoder decides which ranges are required for decoding, while this
/// fetcher keeps cache lookup, local write-cache reads, and remote I/O explicit
/// in Greptime code.
pub struct SstParquetRangeFetcher {
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
    pub fn new(
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

        let fetched_pages = if page_lookup.is_some() {
            self.fetch_missing_ranges(&missing_ranges).await?
        } else {
            // Cache-disabled reads and compaction bypass the sharing bookkeeping.
            self.fetch_storage_ranges(&missing_ranges).await?
        };

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
    /// Loads owned ranges before awaiting shared ones, preserving batched storage I/O.
    async fn fetch_missing_ranges(&self, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let mut pages = vec![Bytes::new(); ranges.len()];
        let mut owned = Vec::new();
        let mut waiting = Vec::new();
        for (index, range) in ranges.iter().enumerate() {
            match self.cache_strategy.start_page_load(
                self.region_file_id.file_id(),
                self.row_group_idx,
                range,
            ) {
                Some(CacheLoad::Load(guard)) => {
                    // Recheck after registration to close a concurrent publication race.
                    if let Some(page) = self.cache_strategy.get_cached_page_range(
                        self.region_file_id.file_id(),
                        self.row_group_idx,
                        range,
                    ) {
                        guard.finish(page.clone());
                        if let Some(metrics) = &self.fetch_metrics {
                            metrics.data.lock().unwrap().page_size_needed += page.len() as u64;
                        }
                        pages[index] = page;
                    } else {
                        owned.push((index, Some(guard)));
                    }
                }
                Some(CacheLoad::Wait(waiter)) => waiting.push((index, waiter)),
                None => owned.push((index, None)),
            }
        }
        if !owned.is_empty() {
            let requested = owned
                .iter()
                .map(|(index, _)| ranges[*index].clone())
                .collect::<Vec<_>>();
            let loaded = self.fetch_storage_ranges(&requested).await?;
            for ((index, guard), page) in owned.into_iter().zip(loaded) {
                if let Some(guard) = guard {
                    // Share the detached cache allocation rather than copy it again.
                    // If admission failed, detach here to avoid pinning a merged I/O buffer.
                    let shared = self
                        .cache_strategy
                        .get_cached_page_range(
                            self.region_file_id.file_id(),
                            self.row_group_idx,
                            &ranges[index],
                        )
                        .unwrap_or_else(|| Bytes::copy_from_slice(&page));
                    guard.finish(shared);
                }
                pages[index] = page;
            }
        }
        let mut retry = Vec::new();
        for (index, waiter) in waiting {
            if let Some(page) = waiter.wait().await {
                if let Some(metrics) = &self.fetch_metrics {
                    metrics.data.lock().unwrap().page_size_needed += page.len() as u64;
                }
                pages[index] = page;
            } else {
                retry.push(index);
            }
        }
        if !retry.is_empty() {
            let requested = retry
                .iter()
                .map(|index| ranges[*index].clone())
                .collect::<Vec<_>>();
            let loaded = self.fetch_storage_ranges(&requested).await?;
            for (index, page) in retry.into_iter().zip(loaded) {
                pages[index] = page;
            }
        }
        Ok(pages)
    }

    /// Reads a batch from local file cache or object storage and populates page cache.
    async fn fetch_storage_ranges(&self, missing_ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        CACHE_LOAD_EVENTS
            .with_label_values(&["page", "read_batch"])
            .inc();
        CACHE_LOAD_EVENTS
            .with_label_values(&["page", "read_range"])
            .inc_by(missing_ranges.len() as u64);
        // Calculate total range size for metrics.
        let (_, unaligned_size) = compute_total_range_size(missing_ranges);

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
            Some(cache) => cache.file_cache().read_ranges(key, missing_ranges).await,
            None => None,
        };

        let fetched_pages = match write_cache_result {
            Some(data) => {
                CACHE_LOAD_EVENTS
                    .with_label_values(&["page", "read_local_batch"])
                    .inc();
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
                CACHE_LOAD_EVENTS
                    .with_label_values(&["page", "read_remote_batch"])
                    .inc();
                // Fetch data from object store.
                let _timer = READ_STAGE_ELAPSED
                    .with_label_values(&["cache_miss_read"])
                    .start_timer();

                let start = self
                    .fetch_metrics
                    .as_ref()
                    .map(|_| std::time::Instant::now());
                let data =
                    fetch_byte_ranges(&self.file_path, self.object_store.clone(), missing_ranges)
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
        ensure!(
            fetched_pages.len() == missing_ranges.len(),
            UnexpectedSnafu {
                reason: format!(
                    "Invalid parquet range fetch: {} missing ranges but {} fetched byte ranges",
                    missing_ranges.len(),
                    fetched_pages.len()
                ),
            }
        );

        self.cache_strategy.put_page_ranges(
            self.region_file_id.file_id(),
            self.row_group_idx,
            missing_ranges,
            &fetched_pages,
        );

        CACHE_LOAD_BYTES
            .with_label_values(&["page"])
            .inc_by(fetched_pages.iter().map(|page| page.len() as u64).sum());
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
        let slice_end = (part.range.end.min(range.end) - part.range.start) as usize;
        output.extend_from_slice(&part.bytes.slice(slice_start..slice_end));
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
pub fn build_sst_parquet_record_batch_stream(
    arrow_metadata: ArrowReaderMetadata,
    row_group_idx: usize,
    row_selection: Option<RowSelection>,
    projection: ProjectionMask,
    fetcher: SstParquetRangeFetcher,
    file_path: String,
    batch_size: usize,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let mut builder = ParquetPushDecoderBuilder::new_with_metadata(arrow_metadata)
        .with_row_groups(vec![row_group_idx])
        .with_projection(projection)
        .with_batch_size(batch_size);

    if let Some(selection) = row_selection {
        builder = builder.with_row_selection(selection);
    }

    let mut decoder = builder
        .build()
        .context(ReadParquetSnafu { path: &file_path })?;

    Ok(async_stream::try_stream! {
        let _work = CacheWork::new("parquet", "decoder");
        loop {
            let decoded = {
                let _timer = crate::metrics::CACHE_WORK_ELAPSED
                    .with_label_values(&["parquet", "decode"])
                    .start_timer();
                decoder.try_decode().context(ReadParquetSnafu { path: &file_path })?
            };
            match decoded {
                DecodeResult::NeedsData(ranges) => {
                    let data = fetcher.fetch_bytes_with_cache(ranges.clone()).await?;
                    decoder
                        .push_ranges(ranges, data)
                        .context(ReadParquetSnafu { path: &file_path })?;
                }
                DecodeResult::Data(batch) => {
                    PARQUET_DECODE_OUTPUT.with_label_values(&["batches"]).inc();
                    PARQUET_DECODE_OUTPUT.with_label_values(&["rows"]).inc_by(batch.num_rows() as u64);
                    PARQUET_DECODE_OUTPUT.with_label_values(&["bytes"]).inc_by(batch.get_array_memory_size() as u64);
                    yield batch;
                },
                DecodeResult::Finished => break,
            }
        }
    }
    .boxed())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use object_store::layers::mock::MockLayerBuilder;
    use object_store::services::Memory;
    use store_api::storage::{FileId, RegionId};

    use super::*;
    use crate::cache::CacheManager;
    use crate::cache::test_util::GatedStorageReader;

    async fn test_fetcher() -> SstParquetRangeFetcher {
        let store = ObjectStore::new(Memory::default()).unwrap();
        store
            .write("test.parquet", Bytes::from_static(b"0123456789abcdef"))
            .await
            .unwrap();
        SstParquetRangeFetcher::new(
            RegionFileId::new(RegionId::new(1, 1), FileId::random()),
            "test.parquet".into(),
            store,
            CacheStrategy::EnableAll(Arc::new(
                CacheManager::builder().page_cache_size(1024).build(),
            )),
            0,
            None,
        )
    }

    #[tokio::test]
    async fn concurrent_identical_ranges_read_storage_once() {
        const CONCURRENCY: usize = 8;
        for coalesce in [false, true] {
            let reads = Arc::new(AtomicUsize::new(0));
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let layer = MockLayerBuilder::default()
                .reader_factory(Arc::new({
                    let reads = reads.clone();
                    let gate = gate.clone();
                    move |_, _, inner| {
                        Box::new(GatedStorageReader {
                            inner,
                            reads: reads.clone(),
                            gate: gate.clone(),
                        })
                    }
                }))
                .build()
                .unwrap();
            let store = ObjectStore::new(Memory::default()).unwrap().layer(layer);
            store
                .write("test.parquet", Bytes::from_static(b"0123456789abcdef"))
                .await
                .unwrap();
            let fetcher = SstParquetRangeFetcher::new(
                RegionFileId::new(RegionId::new(1, 1), FileId::random()),
                "test.parquet".into(),
                store,
                CacheStrategy::EnableAll(Arc::new(
                    CacheManager::builder().page_cache_size(1024).build(),
                )),
                0,
                None,
            );
            let mut requests = (0..CONCURRENCY)
                .map(|_| {
                    Box::pin(async {
                        let range = 0..4;
                        if coalesce {
                            fetcher
                                .fetch_bytes_with_cache(std::iter::once(range).collect())
                                .await
                        } else {
                            fetcher
                                .fetch_storage_ranges(std::slice::from_ref(&range))
                                .await
                        }
                    })
                })
                .collect::<Vec<_>>();
            let expected_reads = if coalesce { 1 } else { CONCURRENCY };
            // The object-store reader can yield before entering the gated read.
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    for request in &mut requests {
                        assert!(futures::poll!(request).is_pending());
                    }
                    if reads.load(Ordering::Relaxed) >= expected_reads {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(reads.load(Ordering::Relaxed), expected_reads);
            gate.add_permits(CONCURRENCY * 4);
            let results =
                tokio::time::timeout(Duration::from_secs(5), futures::future::join_all(requests))
                    .await
                    .unwrap();
            for result in results {
                assert_eq!(result.unwrap(), vec![Bytes::from_static(b"0123")]);
            }
            assert_eq!(reads.load(Ordering::Relaxed), expected_reads);
        }
    }

    #[tokio::test]
    async fn shared_range_loads_owned_ranges_before_waiting_and_recovers_cancellation() {
        for cancel in [false, true] {
            let fetcher = test_fetcher().await;
            let file_id = fetcher.region_file_id.file_id();
            let Some(CacheLoad::Load(producer)) =
                fetcher.cache_strategy.start_page_load(file_id, 0, &(0..4))
            else {
                panic!("expected producer");
            };
            let mut consumer = Box::pin(fetcher.fetch_bytes_with_cache(vec![0..4, 8..12]));
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    assert!(futures::poll!(&mut consumer).is_pending());
                    let own_range = 8..12;
                    if fetcher
                        .cache_strategy
                        .get_page_ranges(file_id, 0, std::slice::from_ref(&own_range))
                        .unwrap()
                        .is_fully_cached()
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            if cancel {
                drop(producer);
            } else {
                // A distinct payload proves this result came from the shared producer.
                producer.finish(Bytes::from_static(b"abcd"));
            }
            let data = tokio::time::timeout(Duration::from_secs(5), consumer)
                .await
                .unwrap()
                .unwrap();
            let first = if cancel { b"0123" } else { b"abcd" };
            assert_eq!(
                data,
                vec![Bytes::copy_from_slice(first), Bytes::from_static(b"89ab")]
            );
        }
    }

    #[tokio::test]
    async fn overlapping_but_different_range_does_not_wait() {
        let fetcher = test_fetcher().await;
        let Some(CacheLoad::Load(_producer)) =
            fetcher
                .cache_strategy
                .start_page_load(fetcher.region_file_id.file_id(), 0, &(0..4))
        else {
            panic!("expected producer");
        };
        let requested = std::iter::once(2..6).collect();
        let data = tokio::time::timeout(
            Duration::from_secs(5),
            fetcher.fetch_bytes_with_cache(requested),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(data, vec![Bytes::from_static(b"2345")]);
    }

    #[tokio::test]
    async fn failed_storage_read_releases_registration() {
        let mut fetcher = test_fetcher().await;
        fetcher.file_path = "missing.parquet".into();
        let range = 0..4;
        assert!(
            fetcher
                .fetch_bytes_with_cache(std::iter::once(range.clone()).collect())
                .await
                .is_err()
        );
        assert!(matches!(
            fetcher
                .cache_strategy
                .start_page_load(fetcher.region_file_id.file_id(), 0, &range,),
            Some(CacheLoad::Load(_))
        ));
    }

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

    #[test]
    fn test_assemble_range_clamps_overlapping_part_to_requested_end() {
        let parts = vec![PageRangePart {
            range: 0..10,
            bytes: Bytes::from_static(b"0123456789"),
        }];

        let output = assemble_range(&(2..5), parts).unwrap();
        assert_eq!(Bytes::from_static(b"234"), output);
    }
}
