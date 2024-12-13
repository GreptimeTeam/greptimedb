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

use api::v1::index::InvertedIndexMetas;
use async_trait::async_trait;
use bytes::Bytes;
use common_base::buffer::Buffer;
use common_base::BitVec;
use index::inverted_index::error::DecodeFstSnafu;
use index::inverted_index::format::reader::InvertedIndexReader;
use index::inverted_index::FstMap;
use prost::Message;
use snafu::ResultExt;

use crate::metrics::{CACHE_BYTES, CACHE_HIT, CACHE_MISS};
use crate::sst::file::FileId;

/// Metrics for index metadata.
const INDEX_METADATA_TYPE: &str = "index_metadata";
/// Metrics for index content.
const INDEX_CONTENT_TYPE: &str = "index_content";

/// Inverted index blob reader with cache.
pub struct CachedInvertedIndexBlobReader<R> {
    file_id: FileId,
    file_size: u64,
    inner: R,
    cache: InvertedIndexCacheRef,
}

impl<R> CachedInvertedIndexBlobReader<R> {
    pub fn new(file_id: FileId, file_size: u64, inner: R, cache: InvertedIndexCacheRef) -> Self {
        Self {
            file_id,
            file_size,
            inner,
            cache,
        }
    }
}

impl<R> CachedInvertedIndexBlobReader<R>
where
    R: InvertedIndexReader,
{
    /// Gets given range of index data from cache, and loads from source if the file
    /// is not already cached.
    async fn get_or_load(
        &mut self,
        offset: u64,
        size: u32,
    ) -> index::inverted_index::error::Result<Vec<u8>> {
        let keys =
            IndexDataPageKey::generate_page_keys(self.file_id, offset, size, self.cache.page_size);
        // Size is 0, return empty data.
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        // TODO: Can be replaced by an uncontinuous structure like opendal::Buffer.
        let mut data = Vec::with_capacity(keys.len());
        data.resize(keys.len(), Bytes::new());
        let mut cache_miss_range = vec![];
        let mut cache_miss_idx = vec![];
        let last_index = keys.len() - 1;
        // TODO: Avoid copy as much as possible.
        for (i, index) in keys.clone().into_iter().enumerate() {
            match self.cache.get_index(&index) {
                Some(page) => {
                    CACHE_HIT.with_label_values(&[INDEX_CONTENT_TYPE]).inc();
                    data[i] = page;
                }
                None => {
                    CACHE_MISS.with_label_values(&[INDEX_CONTENT_TYPE]).inc();
                    let base_offset = index.page_id * self.cache.page_size;
                    let pruned_size = if i == last_index {
                        prune_size(&keys, self.file_size, self.cache.page_size)
                    } else {
                        self.cache.page_size
                    };
                    cache_miss_range.push(base_offset..base_offset + pruned_size);
                    cache_miss_idx.push(i);
                }
            }
        }
        if !cache_miss_range.is_empty() {
            let pages = self.inner.read_vec(&cache_miss_range).await?;
            for (i, page) in cache_miss_idx.into_iter().zip(pages.into_iter()) {
                let page = page;
                let key = keys[i].clone();
                data[i] = page.clone();
                self.cache.put_index(key, page.clone());
            }
        }
        let buffer = Buffer::from_iter(data.into_iter());
        Ok(buffer
            .slice(IndexDataPageKey::calculate_range(
                offset,
                size,
                self.cache.page_size,
            ))
            .to_vec())
    }
}

#[async_trait]
impl<R: InvertedIndexReader> InvertedIndexReader for CachedInvertedIndexBlobReader<R> {
    async fn range_read(
        &mut self,
        offset: u64,
        size: u32,
    ) -> index::inverted_index::error::Result<Vec<u8>> {
        self.inner.range_read(offset, size).await
    }

    async fn read_vec(
        &mut self,
        ranges: &[Range<u64>],
    ) -> index::inverted_index::error::Result<Vec<Bytes>> {
        self.inner.read_vec(ranges).await
    }

    async fn metadata(&mut self) -> index::inverted_index::error::Result<Arc<InvertedIndexMetas>> {
        if let Some(cached) = self.cache.get_index_metadata(self.file_id) {
            CACHE_HIT.with_label_values(&[INDEX_METADATA_TYPE]).inc();
            Ok(cached)
        } else {
            let meta = self.inner.metadata().await?;
            self.cache.put_index_metadata(self.file_id, meta.clone());
            CACHE_MISS.with_label_values(&[INDEX_METADATA_TYPE]).inc();
            Ok(meta)
        }
    }

    async fn fst(
        &mut self,
        offset: u64,
        size: u32,
    ) -> index::inverted_index::error::Result<FstMap> {
        self.get_or_load(offset, size)
            .await
            .and_then(|r| FstMap::new(r).context(DecodeFstSnafu))
    }

    async fn bitmap(
        &mut self,
        offset: u64,
        size: u32,
    ) -> index::inverted_index::error::Result<BitVec> {
        self.get_or_load(offset, size).await.map(BitVec::from_vec)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexMetadataKey {
    file_id: FileId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexDataPageKey {
    file_id: FileId,
    page_id: u64,
}

impl IndexDataPageKey {
    /// Converts an offset to a page ID based on the page size.
    fn calculate_page_id(offset: u64, page_size: u64) -> u64 {
        offset / page_size
    }

    /// Calculates the total number of pages that a given size spans, starting from a specific offset.
    fn calculate_page_count(offset: u64, size: u32, page_size: u64) -> u32 {
        let start_page = Self::calculate_page_id(offset, page_size);
        let end_page = Self::calculate_page_id(offset + (size as u64) - 1, page_size);
        (end_page + 1 - start_page) as u32
    }

    /// Computes the byte range based on the offset and size.
    /// For example, if offset is 5000 and size is 5000 with PAGE_SIZE of 4096, the range is 904..5904.
    fn calculate_range(offset: u64, size: u32, page_size: u64) -> Range<usize> {
        let start = (offset % page_size) as usize;
        let end = start + size as usize;
        start..end
    }

    /// Generates a vector of IndexKey instances for the pages that a given offset and size span.
    fn generate_page_keys(file_id: FileId, offset: u64, size: u32, page_size: u64) -> Vec<Self> {
        let start_page = Self::calculate_page_id(offset, page_size);
        let total_pages = Self::calculate_page_count(offset, size, page_size);
        (0..total_pages)
            .map(|i| Self {
                file_id,
                page_id: start_page + i as u64,
            })
            .collect()
    }
}

pub type InvertedIndexCacheRef = Arc<InvertedIndexCache>;

pub struct InvertedIndexCache {
    /// Cache for inverted index metadata
    index_metadata: moka::sync::Cache<IndexMetadataKey, Arc<InvertedIndexMetas>>,
    /// Cache for inverted index content.
    index: moka::sync::Cache<IndexDataPageKey, Bytes>,
    // Page size for index content.
    page_size: u64,
}

impl InvertedIndexCache {
    /// Creates `InvertedIndexCache` with provided `index_metadata_cap` and `index_content_cap`.
    pub fn new(index_metadata_cap: u64, index_content_cap: u64, page_size: u64) -> Self {
        common_telemetry::debug!("Building InvertedIndexCache with metadata size: {index_metadata_cap}, content size: {index_content_cap}");
        let index_metadata = moka::sync::CacheBuilder::new(index_metadata_cap)
            .name("inverted_index_metadata")
            .weigher(index_metadata_weight)
            .eviction_listener(|k, v, _cause| {
                let size = index_metadata_weight(&k, &v);
                CACHE_BYTES
                    .with_label_values(&[INDEX_METADATA_TYPE])
                    .sub(size.into());
            })
            .build();
        let index_cache = moka::sync::CacheBuilder::new(index_content_cap)
            .name("inverted_index_content")
            .weigher(index_content_weight)
            .eviction_listener(|k, v, _cause| {
                let size = index_content_weight(&k, &v);
                CACHE_BYTES
                    .with_label_values(&[INDEX_CONTENT_TYPE])
                    .sub(size.into());
            })
            .build();
        Self {
            index_metadata,
            index: index_cache,
            page_size,
        }
    }
}

impl InvertedIndexCache {
    pub fn get_index_metadata(&self, file_id: FileId) -> Option<Arc<InvertedIndexMetas>> {
        self.index_metadata.get(&IndexMetadataKey { file_id })
    }

    pub fn put_index_metadata(&self, file_id: FileId, metadata: Arc<InvertedIndexMetas>) {
        let key = IndexMetadataKey { file_id };
        CACHE_BYTES
            .with_label_values(&[INDEX_METADATA_TYPE])
            .add(index_metadata_weight(&key, &metadata).into());
        self.index_metadata.insert(key, metadata)
    }

    pub fn get_index(&self, key: &IndexDataPageKey) -> Option<Bytes> {
        self.index.get(key)
    }

    pub fn put_index(&self, key: IndexDataPageKey, value: Bytes) {
        CACHE_BYTES
            .with_label_values(&[INDEX_CONTENT_TYPE])
            .add(index_content_weight(&key, &value).into());
        self.index.insert(key, value);
    }
}

/// Calculates weight for index metadata.
fn index_metadata_weight(k: &IndexMetadataKey, v: &Arc<InvertedIndexMetas>) -> u32 {
    (k.file_id.as_bytes().len() + v.encoded_len()) as u32
}

/// Calculates weight for index content.
fn index_content_weight(k: &IndexDataPageKey, v: &Bytes) -> u32 {
    (k.file_id.as_bytes().len() + v.len()) as u32
}

/// Prunes the size of the last page based on the indexes.
/// We have following cases:
/// 1. The rest file size is less than the page size, read to the end of the file.
/// 2. Otherwise, read the page size.
fn prune_size(indexes: &[IndexDataPageKey], file_size: u64, page_size: u64) -> u64 {
    let last_page_start = indexes.last().map(|i| i.page_id * page_size).unwrap_or(0);
    page_size.min(file_size - last_page_start)
}

#[cfg(test)]
mod test {
    use std::num::NonZeroUsize;

    use common_base::BitVec;
    use futures::stream;
    use index::inverted_index::format::reader::{InvertedIndexBlobReader, InvertedIndexReader};
    use index::inverted_index::format::writer::{InvertedIndexBlobWriter, InvertedIndexWriter};
    use index::inverted_index::Bytes;
    use prometheus::register_int_counter_vec;
    use rand::{Rng, RngCore};

    use super::*;
    use crate::sst::index::store::InstrumentedStore;
    use crate::test_util::TestEnv;

    // Repeat times for following little fuzz tests.
    const FUZZ_REPEAT_TIMES: usize = 100;

    // Fuzz test for index data page key
    #[test]
    fn fuzz_index_calculation() {
        // randomly generate a large u8 array
        let mut rng = rand::thread_rng();
        let mut data = vec![0u8; 1024 * 1024];
        rng.fill_bytes(&mut data);
        let file_id = FileId::random();

        for _ in 0..FUZZ_REPEAT_TIMES {
            let offset = rng.gen_range(0..data.len() as u64);
            let size = rng.gen_range(0..data.len() as u32 - offset as u32);
            let page_size: usize = rng.gen_range(1..1024);

            let indexes =
                IndexDataPageKey::generate_page_keys(file_id, offset, size, page_size as u64);
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
            let read =
                read[IndexDataPageKey::calculate_range(offset, size, page_size as u64)].to_vec();
            if read != data.get(expected_range).unwrap() {
                panic!(
                    "fuzz_read_index failed, offset: {}, size: {}, page_size: {}\nread len: {}, expected len: {}\nrange: {:?}, page num: {}",
                    offset, size, page_size, read.len(), size as usize,
                    IndexDataPageKey::calculate_range(offset, size, page_size as u64),
                    page_num
                );
            }
        }
    }

    fn unpack(fst_value: u64) -> [u32; 2] {
        bytemuck::cast::<u64, [u32; 2]>(fst_value)
    }

    async fn create_inverted_index_blob() -> Vec<u8> {
        let mut blob = Vec::new();
        let mut writer = InvertedIndexBlobWriter::new(&mut blob);
        writer
            .add_index(
                "tag0".to_string(),
                BitVec::from_slice(&[0b0000_0001, 0b0000_0000]),
                Box::new(stream::iter(vec![
                    Ok((Bytes::from("a"), BitVec::from_slice(&[0b0000_0001]))),
                    Ok((Bytes::from("b"), BitVec::from_slice(&[0b0010_0000]))),
                    Ok((Bytes::from("c"), BitVec::from_slice(&[0b0000_0001]))),
                ])),
            )
            .await
            .unwrap();
        writer
            .add_index(
                "tag1".to_string(),
                BitVec::from_slice(&[0b0000_0001, 0b0000_0000]),
                Box::new(stream::iter(vec![
                    Ok((Bytes::from("x"), BitVec::from_slice(&[0b0000_0001]))),
                    Ok((Bytes::from("y"), BitVec::from_slice(&[0b0010_0000]))),
                    Ok((Bytes::from("z"), BitVec::from_slice(&[0b0000_0001]))),
                ])),
            )
            .await
            .unwrap();
        writer
            .finish(8, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap();

        blob
    }

    #[tokio::test]
    async fn test_inverted_index_cache() {
        let blob = create_inverted_index_blob().await;

        // Init a test range reader in local fs.
        let mut env = TestEnv::new();
        let file_size = blob.len() as u64;
        let store = env.init_object_store_manager();
        let temp_path = "data";
        store.write(temp_path, blob).await.unwrap();
        let store = InstrumentedStore::new(store);
        let metric =
            register_int_counter_vec!("test_bytes", "a counter for test", &["test"]).unwrap();
        let counter = metric.with_label_values(&["test"]);
        let range_reader = store
            .range_reader("data", &counter, &counter)
            .await
            .unwrap();

        let reader = InvertedIndexBlobReader::new(range_reader);
        let mut cached_reader = CachedInvertedIndexBlobReader::new(
            FileId::random(),
            file_size,
            reader,
            Arc::new(InvertedIndexCache::new(8192, 8192, 50)),
        );
        let metadata = cached_reader.metadata().await.unwrap();
        assert_eq!(metadata.total_row_count, 8);
        assert_eq!(metadata.segment_row_count, 1);
        assert_eq!(metadata.metas.len(), 2);
        // tag0
        let tag0 = metadata.metas.get("tag0").unwrap();
        let stats0 = tag0.stats.as_ref().unwrap();
        assert_eq!(stats0.distinct_count, 3);
        assert_eq!(stats0.null_count, 1);
        assert_eq!(stats0.min_value, Bytes::from("a"));
        assert_eq!(stats0.max_value, Bytes::from("c"));
        let fst0 = cached_reader
            .fst(
                tag0.base_offset + tag0.relative_fst_offset as u64,
                tag0.fst_size,
            )
            .await
            .unwrap();
        assert_eq!(fst0.len(), 3);
        let [offset, size] = unpack(fst0.get(b"a").unwrap());
        let bitmap = cached_reader
            .bitmap(tag0.base_offset + offset as u64, size)
            .await
            .unwrap();
        assert_eq!(bitmap, BitVec::from_slice(&[0b0000_0001]));
        let [offset, size] = unpack(fst0.get(b"b").unwrap());
        let bitmap = cached_reader
            .bitmap(tag0.base_offset + offset as u64, size)
            .await
            .unwrap();
        assert_eq!(bitmap, BitVec::from_slice(&[0b0010_0000]));
        let [offset, size] = unpack(fst0.get(b"c").unwrap());
        let bitmap = cached_reader
            .bitmap(tag0.base_offset + offset as u64, size)
            .await
            .unwrap();
        assert_eq!(bitmap, BitVec::from_slice(&[0b0000_0001]));

        // tag1
        let tag1 = metadata.metas.get("tag1").unwrap();
        let stats1 = tag1.stats.as_ref().unwrap();
        assert_eq!(stats1.distinct_count, 3);
        assert_eq!(stats1.null_count, 1);
        assert_eq!(stats1.min_value, Bytes::from("x"));
        assert_eq!(stats1.max_value, Bytes::from("z"));
        let fst1 = cached_reader
            .fst(
                tag1.base_offset + tag1.relative_fst_offset as u64,
                tag1.fst_size,
            )
            .await
            .unwrap();
        assert_eq!(fst1.len(), 3);
        let [offset, size] = unpack(fst1.get(b"x").unwrap());
        let bitmap = cached_reader
            .bitmap(tag1.base_offset + offset as u64, size)
            .await
            .unwrap();
        assert_eq!(bitmap, BitVec::from_slice(&[0b0000_0001]));
        let [offset, size] = unpack(fst1.get(b"y").unwrap());
        let bitmap = cached_reader
            .bitmap(tag1.base_offset + offset as u64, size)
            .await
            .unwrap();
        assert_eq!(bitmap, BitVec::from_slice(&[0b0010_0000]));
        let [offset, size] = unpack(fst1.get(b"z").unwrap());
        let bitmap = cached_reader
            .bitmap(tag1.base_offset + offset as u64, size)
            .await
            .unwrap();
        assert_eq!(bitmap, BitVec::from_slice(&[0b0000_0001]));

        // fuzz test
        let mut rng = rand::thread_rng();
        for _ in 0..FUZZ_REPEAT_TIMES {
            let offset = rng.gen_range(0..file_size);
            let size = rng.gen_range(0..file_size as u32 - offset as u32);
            let expected = cached_reader.range_read(offset, size).await.unwrap();
            let read = cached_reader.get_or_load(offset, size).await.unwrap();
            assert_eq!(read, expected);
        }
    }
}
