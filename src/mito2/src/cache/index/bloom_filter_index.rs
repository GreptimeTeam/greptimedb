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
use index::bloom_filter::error::Result;
use index::bloom_filter::reader::{BloomFilterReadMetrics, BloomFilterReader};
use store_api::storage::{ColumnId, FileId};

use crate::cache::index::{INDEX_METADATA_TYPE, IndexCache, PageKey};
use crate::metrics::{CACHE_HIT, CACHE_MISS};

const INDEX_TYPE_BLOOM_FILTER_INDEX: &str = "bloom_filter_index";

/// Tag for bloom filter index cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tag {
    Skipping,
    Fulltext,
}

/// Cache for bloom filter index.
pub type BloomFilterIndexCache = IndexCache<(FileId, ColumnId, Tag), BloomFilterMeta>;
pub type BloomFilterIndexCacheRef = Arc<BloomFilterIndexCache>;

impl BloomFilterIndexCache {
    /// Creates a new bloom filter index cache.
    pub fn new(index_metadata_cap: u64, index_content_cap: u64, page_size: u64) -> Self {
        Self::new_with_weighter(
            index_metadata_cap,
            index_content_cap,
            page_size,
            INDEX_TYPE_BLOOM_FILTER_INDEX,
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
fn bloom_filter_index_metadata_weight(
    k: &(FileId, ColumnId, Tag),
    meta: &Arc<BloomFilterMeta>,
) -> u32 {
    let base = k.0.as_bytes().len()
        + std::mem::size_of::<ColumnId>()
        + std::mem::size_of::<Tag>()
        + std::mem::size_of::<BloomFilterMeta>();

    let vec_estimated = meta.segment_loc_indices.len() * std::mem::size_of::<u64>()
        + meta.bloom_filter_locs.len() * std::mem::size_of::<BloomFilterLoc>();

    (base + vec_estimated) as u32
}

/// Calculates weight for bloom filter index content.
fn bloom_filter_index_content_weight(
    (k, _): &((FileId, ColumnId, Tag), PageKey),
    v: &Bytes,
) -> u32 {
    (k.0.as_bytes().len() + std::mem::size_of::<ColumnId>() + v.len()) as u32
}

/// Bloom filter index blob reader with cache.
pub struct CachedBloomFilterIndexBlobReader<R> {
    file_id: FileId,
    column_id: ColumnId,
    tag: Tag,
    blob_size: u64,
    inner: R,
    cache: BloomFilterIndexCacheRef,
}

impl<R> CachedBloomFilterIndexBlobReader<R> {
    /// Creates a new bloom filter index blob reader with cache.
    pub fn new(
        file_id: FileId,
        column_id: ColumnId,
        tag: Tag,
        blob_size: u64,
        inner: R,
        cache: BloomFilterIndexCacheRef,
    ) -> Self {
        Self {
            file_id,
            column_id,
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
        let result = self
            .cache
            .get_or_load(
                (self.file_id, self.column_id, self.tag),
                self.blob_size,
                offset,
                size,
                move |ranges| async move { inner.read_vec(&ranges, None).await },
            )
            .await
            .map(|b| b.into())?;

        if let Some(m) = metrics {
            m.total_ranges += 1;
            m.total_bytes += size as u64;
            if let Some(start) = start {
                m.elapsed += start.elapsed();
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

        let mut pages = Vec::with_capacity(ranges.len());
        for range in ranges {
            let inner = &self.inner;
            let page = self
                .cache
                .get_or_load(
                    (self.file_id, self.column_id, self.tag),
                    self.blob_size,
                    range.start,
                    (range.end - range.start) as u32,
                    move |ranges| async move { inner.read_vec(&ranges, None).await },
                )
                .await?;

            pages.push(Bytes::from(page));
        }

        if let Some(m) = metrics {
            m.total_ranges += ranges.len();
            m.total_bytes += ranges.iter().map(|r| r.end - r.start).sum::<u64>();
            if let Some(start) = start {
                m.elapsed += start.elapsed();
            }
        }

        Ok(pages)
    }

    /// Reads the meta information of the bloom filter.
    async fn metadata(
        &self,
        metrics: Option<&mut BloomFilterReadMetrics>,
    ) -> Result<BloomFilterMeta> {
        if let Some(cached) = self
            .cache
            .get_metadata((self.file_id, self.column_id, self.tag))
        {
            CACHE_HIT.with_label_values(&[INDEX_METADATA_TYPE]).inc();
            Ok((*cached).clone())
        } else {
            let meta = self.inner.metadata(metrics).await?;
            self.cache.put_metadata(
                (self.file_id, self.column_id, self.tag),
                Arc::new(meta.clone()),
            );
            CACHE_MISS.with_label_values(&[INDEX_METADATA_TYPE]).inc();
            Ok(meta)
        }
    }
}

#[cfg(test)]
mod test {
    use rand::{Rng, RngCore};

    use super::*;

    const FUZZ_REPEAT_TIMES: usize = 100;

    #[test]
    fn bloom_filter_metadata_weight_counts_vec_contents() {
        let file_id = FileId::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
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

        let weight =
            bloom_filter_index_metadata_weight(&(file_id, column_id, tag), &Arc::new(meta.clone()));

        let base = file_id.as_bytes().len()
            + std::mem::size_of::<ColumnId>()
            + std::mem::size_of::<Tag>()
            + std::mem::size_of::<BloomFilterMeta>();
        let expected_dynamic = meta.segment_loc_indices.len() * std::mem::size_of::<u64>()
            + meta.bloom_filter_locs.len() * std::mem::size_of::<BloomFilterLoc>();

        assert_eq!(weight as usize, base + expected_dynamic);
    }

    #[test]
    fn fuzz_index_calculation() {
        let mut rng = rand::rng();
        let mut data = vec![0u8; 1024 * 1024];
        rng.fill_bytes(&mut data);

        for _ in 0..FUZZ_REPEAT_TIMES {
            let offset = rng.random_range(0..data.len() as u64);
            let size = rng.random_range(0..data.len() as u32 - offset as u32);
            let page_size: usize = rng.random_range(1..1024);

            let indexes =
                PageKey::generate_page_keys(offset, size, page_size as u64).collect::<Vec<_>>();
            let page_num = indexes.len();
            let mut read = Vec::with_capacity(size as usize);
            for key in indexes.into_iter() {
                let start = key.page_id as usize * page_size;
                let page = if start + page_size < data.len() {
                    &data[start..start + page_size]
                } else {
                    &data[start..]
                };
                read.extend_from_slice(page);
            }
            let expected_range = offset as usize..(offset + size as u64 as u64) as usize;
            let read = read[PageKey::calculate_range(offset, size, page_size as u64)].to_vec();
            assert_eq!(
                read,
                data.get(expected_range).unwrap(),
                "fuzz_read_index failed, offset: {}, size: {}, page_size: {}\nread len: {}, expected len: {}\nrange: {:?}, page num: {}",
                offset,
                size,
                page_size,
                read.len(),
                size as usize,
                PageKey::calculate_range(offset, size, page_size as u64),
                page_num
            );
        }
    }
}
