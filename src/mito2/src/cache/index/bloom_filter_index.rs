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

use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use api::v1::index::{BloomFilterLoc, BloomFilterMeta};
use async_trait::async_trait;
use bytes::Bytes;
use index::bloom_filter::error::{Error, IoSnafu, Result, SharedReadSnafu};
use index::bloom_filter::reader::{BloomFilterReadMetrics, BloomFilterReader};
use index::target::IndexTarget;
use snafu::IntoError;
use store_api::storage::{FileId, IndexVersion};

use crate::cache::index::{INDEX_METADATA_TYPE, IndexCache};
use crate::metrics::{CACHE_HIT, CACHE_MISS};

const INDEX_TYPE_BLOOM_FILTER_INDEX: &str = "bloom_filter_index";

/// Tag for bloom filter index cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tag {
    Skipping,
    Fulltext,
}

pub type BloomFilterIndexKey = (FileId, IndexVersion, Arc<IndexTarget>, Tag);

/// Cache for bloom filter index.
pub type BloomFilterIndexCache = IndexCache<BloomFilterIndexKey, BloomFilterMeta>;
pub type BloomFilterIndexCacheRef = Arc<BloomFilterIndexCache>;

impl BloomFilterIndexCache {
    /// Creates a new bloom filter index cache.
    pub fn new(index_metadata_cap: u64, index_content_cap: u64, page_size: u64) -> Self {
        Self::new_with_weighter(
            index_metadata_cap,
            index_content_cap,
            page_size,
            INDEX_TYPE_BLOOM_FILTER_INDEX,
            "bloom_filter_index_metadata",
            bloom_filter_index_metadata_weight,
            bloom_filter_index_content_weight,
        )
    }

    /// Removes all cached entries for the given `file_id`.
    pub fn invalidate_file(&self, file_id: FileId) {
        self.invalidate_if(move |key| key.0 == file_id);
    }
}

/// Calculates weight for bloom filter index metadata.
fn bloom_filter_index_metadata_weight(k: &BloomFilterIndexKey, meta: &Arc<BloomFilterMeta>) -> u32 {
    let base = k.0.as_bytes().len()
        + std::mem::size_of::<IndexVersion>()
        + k.2.approx_mem_size()
        + std::mem::size_of::<Tag>()
        + std::mem::size_of::<BloomFilterMeta>();

    let vec_estimated = meta.segment_loc_indices.len() * std::mem::size_of::<u64>()
        + meta.bloom_filter_locs.len() * std::mem::size_of::<BloomFilterLoc>();

    (base + vec_estimated) as u32
}

/// Calculates weight for bloom filter index content.
fn bloom_filter_index_content_weight((k, _): &(BloomFilterIndexKey, u64), v: &Bytes) -> u32 {
    (k.0.as_bytes().len() + k.2.approx_mem_size() + v.len()) as u32
}

/// Bloom filter index blob reader with cache.
pub struct CachedBloomFilterIndexBlobReader<R> {
    file_id: FileId,
    index_version: IndexVersion,
    target: Arc<IndexTarget>,
    tag: Tag,
    blob_size: u64,
    inner: R,
    cache: BloomFilterIndexCacheRef,
}

impl<R> CachedBloomFilterIndexBlobReader<R> {
    /// Creates a new bloom filter index blob reader with cache.
    pub fn new(
        file_id: FileId,
        index_version: IndexVersion,
        target: IndexTarget,
        tag: Tag,
        blob_size: u64,
        inner: R,
        cache: BloomFilterIndexCacheRef,
    ) -> Self {
        Self {
            file_id,
            index_version,
            target: Arc::new(target),
            tag,
            blob_size,
            inner,
            cache,
        }
    }
}

#[async_trait]
impl<R: BloomFilterReader + Send> BloomFilterReader for CachedBloomFilterIndexBlobReader<R> {
    async fn range_read(
        &self,
        offset: u64,
        size: u32,
        metrics: Option<&mut BloomFilterReadMetrics>,
    ) -> Result<Bytes> {
        let start = metrics.as_ref().map(|_| Instant::now());
        let inner = &self.inner;
        let (result, cache_metrics) = self
            .cache
            .get_or_load(
                (
                    self.file_id,
                    self.index_version,
                    self.target.clone(),
                    self.tag,
                ),
                self.blob_size,
                offset,
                size,
                move |ranges| async move { inner.read_vec(&ranges, None).await },
            )
            .await
            .map_err(map_page_error)?;

        if let Some(m) = metrics {
            m.total_ranges += cache_metrics.num_pages;
            m.total_bytes += cache_metrics.page_bytes;
            m.cache_hit += cache_metrics.cache_hit;
            m.cache_miss += cache_metrics.cache_miss;
            if let Some(start) = start {
                m.fetch_elapsed += start.elapsed();
            }
        }

        Ok(result)
    }

    async fn read_vec(
        &self,
        ranges: &[Range<u64>],
        metrics: Option<&mut BloomFilterReadMetrics>,
    ) -> Result<Vec<Bytes>> {
        let start = metrics.as_ref().map(|_| Instant::now());

        let inner = &self.inner;
        let (pages, total_cache_metrics) = self
            .cache
            .get_or_load_vec(
                (
                    self.file_id,
                    self.index_version,
                    self.target.clone(),
                    self.tag,
                ),
                self.blob_size,
                ranges,
                move |ranges| async move { inner.read_vec(&ranges, None).await },
            )
            .await
            .map_err(map_page_error)?;
        if let Some(m) = metrics {
            m.total_ranges += total_cache_metrics.num_pages;
            m.total_bytes += total_cache_metrics.page_bytes;
            m.cache_hit += total_cache_metrics.cache_hit;
            m.cache_miss += total_cache_metrics.cache_miss;
            if let Some(start) = start {
                m.fetch_elapsed += start.elapsed();
            }
        }

        Ok(pages)
    }

    /// Reads the meta information of the bloom filter.
    async fn metadata(
        &self,
        metrics: Option<&mut BloomFilterReadMetrics>,
    ) -> Result<Arc<BloomFilterMeta>> {
        let key = (
            self.file_id,
            self.index_version,
            self.target.clone(),
            self.tag,
        );
        if let Some(cached) = self.cache.get_metadata(key.clone()).await {
            CACHE_HIT.with_label_values(&[INDEX_METADATA_TYPE]).inc();
            if let Some(m) = metrics {
                m.cache_hit += 1;
            }
            return Ok(cached);
        }

        let start = metrics.as_ref().map(|_| Instant::now());
        let collect_metrics = metrics.is_some();
        let mut loaded = BloomFilterReadMetrics::default();
        let result = self
            .cache
            .get_or_load_metadata(key, async {
                self.inner
                    .metadata(collect_metrics.then_some(&mut loaded))
                    .await
            })
            .await;
        if let Some(m) = metrics {
            // Only the selected loader performs I/O. Waiters retain zero I/O
            // counts. fetch_elapsed is per-caller elapsed (including wait and error),
            // not physical I/O time summed across callers.
            m.total_bytes += loaded.total_bytes;
            m.total_ranges += loaded.total_ranges;
            m.cache_hit += loaded.cache_hit;
            m.cache_miss += loaded.cache_miss;
            if let Some(start) = start {
                m.fetch_elapsed += start.elapsed();
            }
        }
        let meta = result.map_err(|source| SharedReadSnafu.into_error(source))?;
        CACHE_MISS.with_label_values(&[INDEX_METADATA_TYPE]).inc();
        Ok(meta)
    }
}

/// Recovers the typed source retained by the range-reader adapter.
fn map_page_error(error: Arc<std::io::Error>) -> Error {
    if let Some(source) = crate::cache::find_shared_page_source::<Error>(&error) {
        SharedReadSnafu.into_error(source)
    } else {
        IoSnafu.into_error(std::io::Error::other(error))
    }
}

#[cfg(test)]
mod test {
    use rand::{Rng, RngCore};
    use store_api::storage::ColumnId;

    use super::*;

    const FUZZ_REPEAT_TIMES: usize = 100;

    struct MetadataReader {
        release: tokio::sync::Notify,
        fail: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl BloomFilterReader for MetadataReader {
        async fn range_read(
            &self,
            _: u64,
            _: u32,
            _: Option<&mut BloomFilterReadMetrics>,
        ) -> Result<Bytes> {
            unreachable!("metadata test does not read content pages")
        }

        async fn read_vec(
            &self,
            _: &[Range<u64>],
            _: Option<&mut BloomFilterReadMetrics>,
        ) -> Result<Vec<Bytes>> {
            unreachable!("metadata test does not read content pages")
        }

        async fn metadata(
            &self,
            metrics: Option<&mut BloomFilterReadMetrics>,
        ) -> Result<Arc<BloomFilterMeta>> {
            self.release.notified().await;
            if let Some(metrics) = metrics {
                metrics.total_bytes += 8192;
                metrics.total_ranges += 1;
                // The wrapper must replace, not add, this physical-fetch duration.
                metrics.fetch_elapsed += std::time::Duration::from_secs(3600);
            }
            if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(IoSnafu.into_error(std::io::Error::other("metadata failed")));
            }
            Ok(Arc::new(BloomFilterMeta::default()))
        }
    }

    #[tokio::test]
    async fn metadata_waiters_do_not_duplicate_io_metrics() {
        for fail in [false, true] {
            let cache = Arc::new(BloomFilterIndexCache::new(4096, 4096, 1024));
            let reader = CachedBloomFilterIndexBlobReader::new(
                FileId::random(),
                0,
                IndexTarget::ColumnId(42),
                Tag::Skipping,
                8192,
                MetadataReader {
                    release: tokio::sync::Notify::new(),
                    fail: std::sync::atomic::AtomicBool::new(fail),
                },
                cache,
            );
            let mut owner_metrics = BloomFilterReadMetrics::default();
            let mut waiter_metrics = BloomFilterReadMetrics::default();
            let mut owner = Box::pin(reader.metadata(Some(&mut owner_metrics)));
            assert!(futures::poll!(&mut owner).is_pending());
            let mut waiter = Box::pin(reader.metadata(Some(&mut waiter_metrics)));
            assert!(futures::poll!(&mut waiter).is_pending());
            reader.inner.release.notify_one();
            let owner_result = owner.await;
            let waiter_result = waiter.await;
            assert_eq!(owner_result.is_err(), fail);
            assert_eq!(waiter_result.is_err(), fail);
            assert_eq!(
                (owner_metrics.total_bytes, owner_metrics.total_ranges),
                (8192, 1)
            );
            assert_eq!(
                (waiter_metrics.total_bytes, waiter_metrics.total_ranges),
                (0, 0)
            );
            assert_eq!((owner_metrics.cache_hit, waiter_metrics.cache_hit), (0, 0));
            for metrics in [&owner_metrics, &waiter_metrics] {
                // Per-caller elapsed includes waiting, including on error; it is
                // not a sum of physical I/O durations across concurrent callers.
                assert!(metrics.fetch_elapsed > std::time::Duration::ZERO);
                assert!(metrics.fetch_elapsed < std::time::Duration::from_secs(3600));
            }
            if fail {
                let Error::SharedRead { source: owner, .. } = owner_result.unwrap_err() else {
                    panic!("owner must preserve the typed shared read error");
                };
                let Error::SharedRead { source: waiter, .. } = waiter_result.unwrap_err() else {
                    panic!("waiter must preserve the typed shared read error");
                };
                assert!(Arc::ptr_eq(&owner, &waiter));
            } else {
                let mut hot_metrics = BloomFilterReadMetrics::default();
                reader.metadata(Some(&mut hot_metrics)).await.unwrap();
                assert_eq!(hot_metrics.cache_hit, 1);
                assert_eq!(hot_metrics.total_bytes, 0);
            }
        }
    }

    #[tokio::test]
    async fn json_paths_have_independent_cache_entries() {
        let cache = BloomFilterIndexCache::new(1024 * 1024, 1024 * 1024, 1024);
        let file_id = FileId::random();
        let first = (
            file_id,
            0,
            Arc::new(IndexTarget::new_json_path(1, vec!["a".into(), "b".into()]).unwrap()),
            Tag::Skipping,
        );
        let second = (
            file_id,
            0,
            Arc::new(IndexTarget::new_json_path(1, vec!["a.b".into()]).unwrap()),
            Tag::Skipping,
        );
        cache
            .put_metadata(
                first.clone(),
                Arc::new(BloomFilterMeta {
                    row_count: 1,
                    ..Default::default()
                }),
            )
            .await;
        cache
            .put_metadata(
                second.clone(),
                Arc::new(BloomFilterMeta {
                    row_count: 2,
                    ..Default::default()
                }),
            )
            .await;
        assert_eq!(
            cache.get_metadata(first.clone()).await.unwrap().row_count,
            1
        );
        assert_eq!(
            cache.get_metadata(second.clone()).await.unwrap().row_count,
            2
        );
        cache.invalidate_file(file_id);
        assert!(cache.get_metadata(first).await.is_none());
        assert!(cache.get_metadata(second).await.is_none());
    }

    #[test]
    fn bloom_filter_metadata_weight_counts_vec_contents() {
        let file_id = FileId::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let version = 0;
        let column_id: ColumnId = 42;
        let tag = Tag::Skipping;

        let meta = BloomFilterMeta {
            rows_per_segment: 128,
            segment_count: 2,
            row_count: 256,
            bloom_filter_size: 1024,
            segment_loc_indices: vec![0, 64, 128, 192],
            bloom_filter_locs: vec![
                BloomFilterLoc {
                    offset: 0,
                    size: 512,
                    element_count: 1000,
                },
                BloomFilterLoc {
                    offset: 512,
                    size: 512,
                    element_count: 1000,
                },
            ],
        };

        let weight = bloom_filter_index_metadata_weight(
            &(
                file_id,
                version,
                Arc::new(IndexTarget::ColumnId(column_id)),
                tag,
            ),
            &Arc::new(meta.clone()),
        );

        let base = file_id.as_bytes().len()
            + std::mem::size_of::<IndexVersion>()
            + std::mem::size_of::<IndexTarget>()
            + std::mem::size_of::<Tag>()
            + std::mem::size_of::<BloomFilterMeta>();
        let expected_dynamic = meta.segment_loc_indices.len() * std::mem::size_of::<u64>()
            + meta.bloom_filter_locs.len() * std::mem::size_of::<BloomFilterLoc>();

        assert_eq!(weight as usize, base + expected_dynamic);
    }

    #[test]
    fn fuzz_index_calculation() {
        use common_cache::page::PageLayout;
        use std::num::NonZeroU64;

        let mut rng = rand::rng();
        let mut data = vec![0u8; 1024 * 1024];
        rng.fill_bytes(&mut data);
        for _ in 0..FUZZ_REPEAT_TIMES {
            let offset = rng.random_range(0..data.len() as u64);
            let size = rng.random_range(0..data.len() as u64 - offset);
            let page_size = NonZeroU64::new(rng.random_range(1_u64..1024)).unwrap();
            let layout = PageLayout::new(page_size, data.len() as u64);
            let requested = offset..offset + size;
            let mut read = Vec::with_capacity(size as usize);
            for id in layout.page_ids(requested.clone()).unwrap() {
                let range = layout.page_range(id).unwrap();
                let page = &data[range.start as usize..range.end as usize];
                let slice = layout
                    .request_slice(id, requested.clone())
                    .unwrap()
                    .unwrap();
                read.extend_from_slice(&page[slice]);
            }
            assert_eq!(read, data[offset as usize..(offset + size) as usize]);
        }
    }
}
