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

//! Fixed byte blocks shared across row groups of an immutable Parquet file.

use std::io;
use std::mem;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use common_base::range_read::RangeReader;
use common_cache::paged::{PageFetchStats, PagedCache};

use crate::metrics;
use crate::sst::file::RegionFileId;

/// Default byte blocks, independent of the Parquet format's encoded data-page boundaries.
pub(crate) const PAGE_SIZE: u64 = 64 * 1024;

/// Largest block whose bytes and entry overhead fit the cache's u32 weight.
pub(crate) const MAX_PAGE_SIZE: u64 = u32::MAX as u64
    - (mem::size_of::<(RegionFileId, u64)>() + mem::size_of::<Bytes>() + mem::size_of::<u64>())
        as u64;

/// Caches immutable Parquet file bytes in fixed-size blocks shared across row groups.
pub(crate) struct ParquetPageCache {
    page_size: u64,
    cache: PagedCache<RegionFileId>,
}

impl ParquetPageCache {
    /// Creates a cache with a total byte capacity and a byte-block size.
    pub(crate) fn new(capacity: u64, page_size: u64) -> Arc<Self> {
        Arc::new(Self {
            page_size,
            cache: PagedCache::new(
                "parquet_pages",
                capacity,
                page_size,
                |_: &(RegionFileId, u64), bytes: &Bytes| {
                    (mem::size_of::<(RegionFileId, u64)>()
                        + mem::size_of::<Bytes>()
                        + mem::size_of::<u64>()
                        + bytes.len()) as u32
                },
                metrics::cache_page_observer("page", "page", |hits, misses| {
                    (usize::from(hits > 0), usize::from(misses > 0))
                }),
            ),
        })
    }

    /// Recovers the immutable file size from a requested block already in memory.
    pub(crate) async fn known_file_size(
        &self,
        file: RegionFileId,
        ranges: &[Range<u64>],
    ) -> Option<u64> {
        self.cache.known_file_size(&file, ranges).await
    }

    /// Fetches ranges in input order, sharing missing blocks and returning [`PageFetchStats`].
    /// The reader must address the immutable file identified by `file`.
    pub(crate) async fn fetch<R: RangeReader + ?Sized>(
        &self,
        file: RegionFileId,
        file_size: u64,
        ranges: &[Range<u64>],
        reader: &R,
    ) -> Result<(Vec<Bytes>, PageFetchStats), Arc<io::Error>> {
        // Also protect direct builder callers that do not sanitize MitoConfig.
        if self.page_size == 0 || self.page_size > MAX_PAGE_SIZE {
            return Err(Arc::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Parquet cache page size {} is outside 1..={MAX_PAGE_SIZE} bytes",
                    self.page_size
                ),
            )));
        }
        let result = self.cache.fetch(file, file_size, ranges, reader).await?;
        metrics::cache_page_fetch_metrics("page", &result.1);
        Ok(result)
    }

    /// Invalidates this immutable file's completed pages; in-flight readers keep
    /// their own file handles and may finish their existing loads.
    pub(crate) fn invalidate_file(&self, file: RegionFileId) {
        if let Err(error) = self.cache.invalidate_if(move |key| *key == file) {
            common_telemetry::warn!(error; "Failed to invalidate Parquet pages for {file:?}");
        }
    }

    /// Checks whether a completed block is available without recording a lookup event.
    #[cfg(test)]
    pub(crate) async fn contains(&self, file: RegionFileId, page_id: u64) -> bool {
        self.cache.contains(&file, page_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use common_base::range_read::Metadata;
    use std::sync::Mutex;
    use store_api::storage::{FileId, RegionId};

    #[derive(Default)]
    struct Reader(Mutex<Vec<Range<u64>>>);

    #[async_trait]
    impl RangeReader for Reader {
        async fn metadata(&self) -> io::Result<Metadata> {
            Ok(Metadata {
                content_length: PAGE_SIZE * 2 + 10,
            })
        }
        async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
            self.0.lock().unwrap().push(range.clone());
            tokio::task::yield_now().await;
            Ok(Bytes::from(range.map(|n| n as u8).collect::<Vec<_>>()))
        }
    }

    #[tokio::test]
    async fn test_aligns_and_reuses_partial_overlaps_without_merging_reads() {
        let cache = ParquetPageCache::new(1024 * 1024, PAGE_SIZE);
        let file = RegionFileId::new(RegionId::new(1, 1), FileId::random());
        let size = PAGE_SIZE * 2 + 10;
        let reader = Reader::default();
        let (result, stats) = cache
            .fetch(
                file,
                size,
                &[1..4, PAGE_SIZE - 2..PAGE_SIZE + 2, size - 3..size],
                &reader,
            )
            .await
            .unwrap();
        assert_eq!(&result[0][..], &[1, 2, 3]);
        assert_eq!(&result[1][..], &[254, 255, 0, 1]);
        assert_eq!(&result[2][..], &[7, 8, 9]);
        assert_eq!(
            (
                stats.requested_bytes,
                stats.unique_requested_bytes,
                stats.aligned_bytes
            ),
            (10, 10, size)
        );
        assert_eq!(
            reader.0.lock().unwrap().as_slice(),
            &[0..PAGE_SIZE, PAGE_SIZE..PAGE_SIZE * 2, PAGE_SIZE * 2..size]
        );
        let (_, stats) = cache
            .fetch(file, size, &[2..6, 4..8], &reader)
            .await
            .unwrap();
        assert_eq!(
            (stats.cache_hit, stats.cache_miss, stats.cached_bytes),
            (1, 0, 8)
        );
        assert_eq!(reader.0.lock().unwrap().len(), 3);
        cache.invalidate_file(file);
        assert!(!cache.contains(file, 0).await);
        cache
            .fetch(file, size, &[2..6, 4..8], &reader)
            .await
            .unwrap();
        assert_eq!(reader.0.lock().unwrap().len(), 4);
    }

    #[tokio::test]
    async fn file_identity_includes_region_and_concurrent_reads_share() {
        let cache = ParquetPageCache::new(1024 * 1024, PAGE_SIZE);
        let file_id = FileId::random();
        let first = RegionFileId::new(RegionId::new(1, 1), file_id);
        let second = RegionFileId::new(RegionId::new(2, 1), file_id);
        let reader = Reader::default();
        let ranges = [0..4, 2..6];
        let (a, b) = tokio::join!(
            cache.fetch(first, 100, &ranges, &reader),
            cache.fetch(first, 100, &ranges, &reader)
        );
        assert_eq!(a.unwrap().0, b.unwrap().0);
        assert_eq!(reader.0.lock().unwrap().len(), 1);
        cache.fetch(second, 100, &ranges, &reader).await.unwrap();
        assert_eq!(reader.0.lock().unwrap().len(), 2);
        cache.invalidate_file(first);
        assert!(!cache.contains(first, 0).await);
        assert!(cache.contains(second, 0).await);
    }

    #[tokio::test]
    async fn configurable_blocks_preserve_cross_block_and_tail_reads() {
        let file = RegionFileId::new(RegionId::new(1, 1), FileId::random());
        let size = PAGE_SIZE * 2 + 10;
        // Include a non-power-of-two size: the layout only requires a nonzero divisor.
        for page_size in [1, 37, 4096, PAGE_SIZE, PAGE_SIZE * 4] {
            let cache = ParquetPageCache::new(16 * 1024 * 1024, page_size);
            let reader = Reader::default();
            let boundary = page_size.min(size - 1);
            let ranges = [
                0..3,
                boundary - 1..boundary + 1,
                PAGE_SIZE - 2..PAGE_SIZE + 2,
                size - 3..size,
                size..size,
            ];
            let expected: Vec<_> = ranges
                .iter()
                .map(|r| Bytes::from(r.clone().map(|n| n as u8).collect::<Vec<_>>()))
                .collect();
            let (actual, _) = cache.fetch(file, size, &ranges, &reader).await.unwrap();
            assert_eq!(actual, expected);
            let reads = reader.0.lock().unwrap().len();
            assert!(reader.0.lock().unwrap().iter().all(|r| r.end <= size));
            let (cached, stats) = cache.fetch(file, size, &ranges, &reader).await.unwrap();
            assert_eq!(cached, expected);
            assert_eq!(stats.cache_miss, 0);
            assert_eq!(reader.0.lock().unwrap().len(), reads);
        }
    }

    #[tokio::test]
    async fn builder_uses_fixed_block_size() {
        use crate::cache::{CacheManager, CacheStrategy};

        let strategy = CacheStrategy::EnableAll(Arc::new(
            CacheManager::builder().page_cache_size(1024 * 1024).build(),
        ));
        let cache = strategy.parquet_page_cache().unwrap();
        let reader = Reader::default();
        let file = RegionFileId::new(RegionId::new(1, 1), FileId::random());
        cache
            .fetch(file, PAGE_SIZE * 2, std::slice::from_ref(&(0..1)), &reader)
            .await
            .unwrap();
        assert_eq!(
            reader.0.lock().unwrap().as_slice(),
            std::slice::from_ref(&(0..PAGE_SIZE))
        );
        let disabled = CacheStrategy::EnableAll(Arc::new(CacheManager::builder().build()));
        assert!(disabled.parquet_page_cache().is_none());
    }

    #[tokio::test]
    async fn invalid_block_size_is_rejected_before_io() {
        let file = RegionFileId::new(RegionId::new(1, 1), FileId::random());
        for page_size in [0, MAX_PAGE_SIZE + 1, u64::MAX] {
            let cache = ParquetPageCache::new(1024, page_size);
            let reader = Reader::default();
            let error = cache
                .fetch(file, 100, std::slice::from_ref(&(0..4)), &reader)
                .await
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert!(reader.0.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn oversized_page_is_returned_without_retention() {
        let cache = ParquetPageCache::new(1, PAGE_SIZE);
        let file = RegionFileId::new(RegionId::new(1, 1), FileId::random());
        let reader = Reader::default();
        let result = cache
            .fetch(file, 100, &[0..4, 4..8], &reader)
            .await
            .unwrap();
        assert_eq!(&result.0[0][..], &[0, 1, 2, 3]);
        cache.cache.run_pending_tasks().await;
        assert!(!cache.contains(file, 0).await);
    }
}
