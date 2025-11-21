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

use core::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use api::v1::index::InvertedIndexMetas;
use async_trait::async_trait;
use bytes::Bytes;
use index::inverted_index::error::Result;
use index::inverted_index::format::reader::{InvertedIndexReadMetrics, InvertedIndexReader};
use prost::Message;
use store_api::storage::FileId;

use crate::cache::index::{INDEX_METADATA_TYPE, IndexCache, PageKey};
use crate::metrics::{CACHE_HIT, CACHE_MISS};

const INDEX_TYPE_INVERTED_INDEX: &str = "inverted_index";

/// Cache for inverted index.
pub type InvertedIndexCache = IndexCache<FileId, InvertedIndexMetas>;
pub type InvertedIndexCacheRef = Arc<InvertedIndexCache>;

impl InvertedIndexCache {
    /// Creates a new inverted index cache.
    pub fn new(index_metadata_cap: u64, index_content_cap: u64, page_size: u64) -> Self {
        Self::new_with_weighter(
            index_metadata_cap,
            index_content_cap,
            page_size,
            INDEX_TYPE_INVERTED_INDEX,
            inverted_index_metadata_weight,
            inverted_index_content_weight,
        )
    }

    /// Removes all cached entries for the given `file_id`.
    pub fn invalidate_file(&self, file_id: FileId) {
        self.invalidate_if(move |key| *key == file_id);
    }
}

/// Calculates weight for inverted index metadata.
fn inverted_index_metadata_weight(k: &FileId, v: &Arc<InvertedIndexMetas>) -> u32 {
    (k.as_bytes().len() + v.encoded_len()) as u32
}

/// Calculates weight for inverted index content.
fn inverted_index_content_weight((k, _): &(FileId, PageKey), v: &Bytes) -> u32 {
    (k.as_bytes().len() + v.len()) as u32
}

/// Inverted index blob reader with cache.
pub struct CachedInvertedIndexBlobReader<R> {
    file_id: FileId,
    blob_size: u64,
    inner: R,
    cache: InvertedIndexCacheRef,
}

impl<R> CachedInvertedIndexBlobReader<R> {
    /// Creates a new inverted index blob reader with cache.
    pub fn new(file_id: FileId, blob_size: u64, inner: R, cache: InvertedIndexCacheRef) -> Self {
        Self {
            file_id,
            blob_size,
            inner,
            cache,
        }
    }
}

#[async_trait]
impl<R: InvertedIndexReader> InvertedIndexReader for CachedInvertedIndexBlobReader<R> {
    async fn range_read<'a>(
        &self,
        offset: u64,
        size: u32,
        metrics: Option<&'a mut InvertedIndexReadMetrics>,
    ) -> Result<Vec<u8>> {
        let start = metrics.as_ref().map(|_| Instant::now());

        let inner = &self.inner;
        let result = self
            .cache
            .get_or_load(
                self.file_id,
                self.blob_size,
                offset,
                size,
                move |ranges| async move { inner.read_vec(&ranges, None).await },
            )
            .await?;

        if let Some(m) = metrics {
            m.total_bytes += size as u64;
            m.total_ranges += 1;
            m.fetch_elapsed += start.unwrap().elapsed();
        }

        Ok(result)
    }

    async fn read_vec<'a>(
        &self,
        ranges: &[Range<u64>],
        metrics: Option<&'a mut InvertedIndexReadMetrics>,
    ) -> Result<Vec<Bytes>> {
        let start = metrics.as_ref().map(|_| Instant::now());

        let mut pages = Vec::with_capacity(ranges.len());
        for range in ranges {
            let inner = &self.inner;
            let page = self
                .cache
                .get_or_load(
                    self.file_id,
                    self.blob_size,
                    range.start,
                    (range.end - range.start) as u32,
                    move |ranges| async move { inner.read_vec(&ranges, None).await },
                )
                .await?;

            pages.push(Bytes::from(page));
        }

        if let Some(m) = metrics {
            m.total_bytes += ranges.iter().map(|r| r.end - r.start).sum::<u64>();
            m.total_ranges += ranges.len();
            m.fetch_elapsed += start.unwrap().elapsed();
        }

        Ok(pages)
    }

    async fn metadata(&self) -> Result<Arc<InvertedIndexMetas>> {
        if let Some(cached) = self.cache.get_metadata(self.file_id) {
            CACHE_HIT.with_label_values(&[INDEX_METADATA_TYPE]).inc();
            Ok(cached)
        } else {
            let meta = self.inner.metadata().await?;
            self.cache.put_metadata(self.file_id, meta.clone());
            CACHE_MISS.with_label_values(&[INDEX_METADATA_TYPE]).inc();
            Ok(meta)
        }
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroUsize;

    use futures::stream;
    use index::Bytes;
    use index::bitmap::{Bitmap, BitmapType};
    use index::inverted_index::format::reader::{InvertedIndexBlobReader, InvertedIndexReader};
    use index::inverted_index::format::writer::{InvertedIndexBlobWriter, InvertedIndexWriter};
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
            if read != data.get(expected_range).unwrap() {
                panic!(
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

    fn unpack(fst_value: u64) -> [u32; 2] {
        bytemuck::cast::<u64, [u32; 2]>(fst_value)
    }

    async fn create_inverted_index_blob() -> Vec<u8> {
        let mut blob = Vec::new();
        let mut writer = InvertedIndexBlobWriter::new(&mut blob);
        writer
            .add_index(
                "tag0".to_string(),
                Bitmap::from_lsb0_bytes(&[0b0000_0001, 0b0000_0000], BitmapType::Roaring),
                Box::new(stream::iter(vec![
                    Ok((
                        Bytes::from("a"),
                        Bitmap::from_lsb0_bytes(&[0b0000_0001], BitmapType::Roaring),
                    )),
                    Ok((
                        Bytes::from("b"),
                        Bitmap::from_lsb0_bytes(&[0b0010_0000], BitmapType::Roaring),
                    )),
                    Ok((
                        Bytes::from("c"),
                        Bitmap::from_lsb0_bytes(&[0b0000_0001], BitmapType::Roaring),
                    )),
                ])),
                index::bitmap::BitmapType::Roaring,
            )
            .await
            .unwrap();
        writer
            .add_index(
                "tag1".to_string(),
                Bitmap::from_lsb0_bytes(&[0b0000_0001, 0b0000_0000], BitmapType::Roaring),
                Box::new(stream::iter(vec![
                    Ok((
                        Bytes::from("x"),
                        Bitmap::from_lsb0_bytes(&[0b0000_0001], BitmapType::Roaring),
                    )),
                    Ok((
                        Bytes::from("y"),
                        Bitmap::from_lsb0_bytes(&[0b0010_0000], BitmapType::Roaring),
                    )),
                    Ok((
                        Bytes::from("z"),
                        Bitmap::from_lsb0_bytes(&[0b0000_0001], BitmapType::Roaring),
                    )),
                ])),
                index::bitmap::BitmapType::Roaring,
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
        let mut env = TestEnv::new().await;
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
        let cached_reader = CachedInvertedIndexBlobReader::new(
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
                None,
            )
            .await
            .unwrap();
        assert_eq!(fst0.len(), 3);
        let [offset, size] = unpack(fst0.get(b"a").unwrap());
        let bitmap = cached_reader
            .bitmap(
                tag0.base_offset + offset as u64,
                size,
                BitmapType::Roaring,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            bitmap,
            Bitmap::from_lsb0_bytes(&[0b0000_0001], BitmapType::Roaring)
        );
        let [offset, size] = unpack(fst0.get(b"b").unwrap());
        let bitmap = cached_reader
            .bitmap(
                tag0.base_offset + offset as u64,
                size,
                BitmapType::Roaring,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            bitmap,
            Bitmap::from_lsb0_bytes(&[0b0010_0000], BitmapType::Roaring)
        );
        let [offset, size] = unpack(fst0.get(b"c").unwrap());
        let bitmap = cached_reader
            .bitmap(
                tag0.base_offset + offset as u64,
                size,
                BitmapType::Roaring,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            bitmap,
            Bitmap::from_lsb0_bytes(&[0b0000_0001], BitmapType::Roaring)
        );

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
                None,
            )
            .await
            .unwrap();
        assert_eq!(fst1.len(), 3);
        let [offset, size] = unpack(fst1.get(b"x").unwrap());
        let bitmap = cached_reader
            .bitmap(
                tag1.base_offset + offset as u64,
                size,
                BitmapType::Roaring,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            bitmap,
            Bitmap::from_lsb0_bytes(&[0b0000_0001], BitmapType::Roaring)
        );
        let [offset, size] = unpack(fst1.get(b"y").unwrap());
        let bitmap = cached_reader
            .bitmap(
                tag1.base_offset + offset as u64,
                size,
                BitmapType::Roaring,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            bitmap,
            Bitmap::from_lsb0_bytes(&[0b0010_0000], BitmapType::Roaring)
        );
        let [offset, size] = unpack(fst1.get(b"z").unwrap());
        let bitmap = cached_reader
            .bitmap(
                tag1.base_offset + offset as u64,
                size,
                BitmapType::Roaring,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            bitmap,
            Bitmap::from_lsb0_bytes(&[0b0000_0001], BitmapType::Roaring)
        );

        // fuzz test
        let mut rng = rand::rng();
        for _ in 0..FUZZ_REPEAT_TIMES {
            let offset = rng.random_range(0..file_size);
            let size = rng.random_range(0..file_size as u32 - offset as u32);
            let expected = cached_reader.range_read(offset, size, None).await.unwrap();
            let inner = &cached_reader.inner;
            let read = cached_reader
                .cache
                .get_or_load(
                    cached_reader.file_id,
                    file_size,
                    offset,
                    size,
                    |ranges| async move { inner.read_vec(&ranges, None).await },
                )
                .await
                .unwrap();
            assert_eq!(read, expected);
        }
    }
}
