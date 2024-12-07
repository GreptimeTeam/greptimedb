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
use common_base::BitVec;
use index::inverted_index::error::DecodeFstSnafu;
use index::inverted_index::format::reader::InvertedIndexReader;
use index::inverted_index::FstMap;
use prost::Message;
use snafu::ResultExt;

use super::PAGE_SIZE;
use crate::metrics::{CACHE_BYTES, CACHE_HIT, CACHE_MISS};
use crate::sst::file::FileId;

/// Metrics for index metadata.
const INDEX_METADATA_TYPE: &str = "index_metadata";
/// Metrics for index content.
const INDEX_CONTENT_TYPE: &str = "index_content";

/// Inverted index blob reader with cache.
pub struct CachedInvertedIndexBlobReader<R> {
    file_id: FileId,
    inner: R,
    cache: InvertedIndexCacheRef,
}

impl<R> CachedInvertedIndexBlobReader<R> {
    pub fn new(file_id: FileId, inner: R, cache: InvertedIndexCacheRef) -> Self {
        Self {
            file_id,
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
        let indexes = IndexKey::index(self.file_id, offset, size);
        // Size is 0, return empty data.
        if indexes.is_empty() {
            return Ok(Vec::new());
        }
        let mut data = Vec::with_capacity(size as usize);
        // Safety: indexes is not empty.
        let first_page_id = indexes[0].page_id;
        let last_page_id = indexes[indexes.len() - 1].page_id;
        for index in indexes {
            let page = match self.cache.get_index(&index) {
                Some(page) => {
                    CACHE_HIT.with_label_values(&[INDEX_CONTENT_TYPE]).inc();
                    page
                }
                None => {
                    let page = self
                        .inner
                        .seek_read(index.page_id * (PAGE_SIZE as u64), PAGE_SIZE as u32)
                        .await?;
                    self.cache.put_index(index.clone(), Arc::new(page.clone()));
                    CACHE_MISS.with_label_values(&[INDEX_CONTENT_TYPE]).inc();
                    Arc::new(page)
                }
            };
            if index.page_id == first_page_id {
                data.extend_from_slice(&page[IndexKey::offset_to_first_range(offset, size)]);
            } else if index.page_id == last_page_id {
                data.extend_from_slice(&page[IndexKey::offset_to_last_range(offset, size)]);
            } else {
                data.extend_from_slice(&page);
            }
        }
        Ok(data)
    }
}

#[async_trait]
impl<R: InvertedIndexReader> InvertedIndexReader for CachedInvertedIndexBlobReader<R> {
    async fn read_all(
        &mut self,
        dest: &mut Vec<u8>,
    ) -> index::inverted_index::error::Result<usize> {
        self.inner.read_all(dest).await
    }

    async fn seek_read(
        &mut self,
        offset: u64,
        size: u32,
    ) -> index::inverted_index::error::Result<Vec<u8>> {
        self.inner.seek_read(offset, size).await
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
pub struct IndexKey {
    file_id: FileId,
    page_id: u64,
}

impl IndexKey {
    fn offset_to_page_id(offset: u64) -> u64 {
        offset / (PAGE_SIZE as u64)
    }

    fn size_to_page_num(size: u32) -> u32 {
        size / (PAGE_SIZE as u32) + if size % (PAGE_SIZE as u32) == 0 { 0 } else { 1 }
    }

    /// Ranges of first page.
    /// For example, if offset is 1000 and size is 5000 and PAGE_SIZE is 4096, then the first page is 1000..4096.
    fn offset_to_first_range(offset: u64, size: u32) -> Range<usize> {
        (offset % (PAGE_SIZE as u64)) as usize..if size > PAGE_SIZE as u32 {
            PAGE_SIZE
        } else {
            (offset % (PAGE_SIZE as u64) + size as u64) as usize
        }
    }

    /// Ranges of last page.
    /// For example, if offset is 1000 and size is 2000, then the last page is 0..904.
    fn offset_to_last_range(_offset: u64, size: u32) -> Range<usize> {
        0..(size % (PAGE_SIZE as u32)) as usize
    }

    pub fn index(file_id: FileId, offset: u64, size: u32) -> Vec<Self> {
        let page_id = Self::offset_to_page_id(offset);
        let page_num = Self::size_to_page_num(size);
        (0..page_num)
            .map(|i| Self {
                file_id,
                page_id: page_id + i as u64,
            })
            .collect()
    }
}

pub type InvertedIndexCacheRef = Arc<InvertedIndexCache>;

pub struct InvertedIndexCache {
    /// Cache for inverted index metadata
    index_metadata: moka::sync::Cache<IndexKey, Arc<InvertedIndexMetas>>,
    /// Cache for inverted index content.
    index: moka::sync::Cache<IndexKey, Arc<Vec<u8>>>,
}

impl InvertedIndexCache {
    /// Creates `InvertedIndexCache` with provided `index_metadata_cap` and `index_content_cap`.
    pub fn new(index_metadata_cap: u64, index_content_cap: u64) -> Self {
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
        }
    }
}

impl InvertedIndexCache {
    pub fn get_index_metadata(&self, file_id: FileId) -> Option<Arc<InvertedIndexMetas>> {
        self.index_metadata.get(&IndexKey {
            file_id,
            page_id: 0,
        })
    }

    pub fn put_index_metadata(&self, file_id: FileId, metadata: Arc<InvertedIndexMetas>) {
        let key = IndexKey {
            file_id,
            page_id: 0,
        };
        CACHE_BYTES
            .with_label_values(&[INDEX_METADATA_TYPE])
            .add(index_metadata_weight(&key, &metadata).into());
        self.index_metadata.insert(key, metadata)
    }

    pub fn get_index(&self, key: &IndexKey) -> Option<Arc<Vec<u8>>> {
        self.index.get(key)
    }

    pub fn put_index(&self, key: IndexKey, value: Arc<Vec<u8>>) {
        CACHE_BYTES
            .with_label_values(&[INDEX_CONTENT_TYPE])
            .add(index_content_weight(&key, &value).into());
        self.index.insert(key, value);
    }
}

/// Calculates weight for index metadata.
fn index_metadata_weight(k: &IndexKey, v: &Arc<InvertedIndexMetas>) -> u32 {
    (k.file_id.as_bytes().len() + v.encoded_len()) as u32
}

/// Calculates weight for index content.
fn index_content_weight(k: &IndexKey, v: &Arc<Vec<u8>>) -> u32 {
    (k.file_id.as_bytes().len() + v.len()) as u32
}

#[cfg(test)]
mod test {
    use std::num::NonZeroUsize;

    use common_base::BitVec;
    use futures::stream;
    use index::inverted_index::format::reader::{InvertedIndexBlobReader, InvertedIndexReader};
    use index::inverted_index::format::writer::{InvertedIndexBlobWriter, InvertedIndexWriter};
    use index::inverted_index::Bytes;

    use super::*;

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
        let reader = InvertedIndexBlobReader::new(blob);
        let mut cached_reader = CachedInvertedIndexBlobReader::new(
            FileId::random(),
            reader,
            Arc::new(InvertedIndexCache::new(8192, 8192)),
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
    }
}
