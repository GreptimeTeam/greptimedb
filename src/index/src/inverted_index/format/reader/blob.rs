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

use async_trait::async_trait;
use bytes::Bytes;
use common_base::range_read::RangeReader;
use greptime_proto::v1::index::InvertedIndexMetas;
use snafu::{ResultExt, ensure};

use crate::inverted_index::error::{CommonIoSnafu, Result, UnexpectedBlobSizeSnafu};
use crate::inverted_index::format::MIN_BLOB_SIZE;
use crate::inverted_index::format::reader::footer::{
    DEFAULT_PREFETCH_SIZE, InvertedIndexFooterReader,
};
use crate::inverted_index::format::reader::{InvertedIndexReadMetrics, InvertedIndexReader};

/// Inverted index blob reader, implements [`InvertedIndexReader`]
pub struct InvertedIndexBlobReader<R> {
    /// The blob
    source: R,
}

impl<R> InvertedIndexBlobReader<R> {
    pub fn new(source: R) -> Self {
        Self { source }
    }

    fn validate_blob_size(blob_size: u64) -> Result<()> {
        ensure!(
            blob_size >= MIN_BLOB_SIZE,
            UnexpectedBlobSizeSnafu {
                min_blob_size: MIN_BLOB_SIZE,
                actual_blob_size: blob_size,
            }
        );
        Ok(())
    }
}

#[async_trait]
impl<R: RangeReader + Sync> InvertedIndexReader for InvertedIndexBlobReader<R> {
    async fn range_read<'a>(
        &self,
        offset: u64,
        size: u32,
        metrics: Option<&'a mut InvertedIndexReadMetrics>,
    ) -> Result<Vec<u8>> {
        let start = metrics.as_ref().map(|_| Instant::now());

        let buf = self
            .source
            .read(offset..offset + size as u64)
            .await
            .context(CommonIoSnafu)?;

        if let Some(m) = metrics {
            m.total_bytes += size as u64;
            m.total_ranges += 1;
            m.elapsed += start.unwrap().elapsed();
        }

        Ok(buf.into())
    }

    async fn read_vec<'a>(
        &self,
        ranges: &[Range<u64>],
        metrics: Option<&'a mut InvertedIndexReadMetrics>,
    ) -> Result<Vec<Bytes>> {
        let start = metrics.as_ref().map(|_| Instant::now());

        let result = self.source.read_vec(ranges).await.context(CommonIoSnafu)?;

        if let Some(m) = metrics {
            m.total_bytes += ranges.iter().map(|r| r.end - r.start).sum::<u64>();
            m.total_ranges += ranges.len();
            m.elapsed += start.unwrap().elapsed();
        }

        Ok(result)
    }

    async fn metadata(&self) -> Result<Arc<InvertedIndexMetas>> {
        let metadata = self.source.metadata().await.context(CommonIoSnafu)?;
        let blob_size = metadata.content_length;
        Self::validate_blob_size(blob_size)?;

        let mut footer_reader = InvertedIndexFooterReader::new(&self.source, blob_size)
            .with_prefetch_size(DEFAULT_PREFETCH_SIZE);
        footer_reader.metadata().await.map(Arc::new)
    }
}

#[cfg(test)]
mod tests {
    use fst::MapBuilder;
    use greptime_proto::v1::index::{BitmapType, InvertedIndexMeta, InvertedIndexMetas};
    use prost::Message;

    use super::*;
    use crate::bitmap::Bitmap;

    fn mock_fst() -> Vec<u8> {
        let mut fst_buf = Vec::new();
        let mut build = MapBuilder::new(&mut fst_buf).unwrap();
        build.insert("key1".as_bytes(), 1).unwrap();
        build.insert("key2".as_bytes(), 2).unwrap();
        build.finish().unwrap();
        fst_buf
    }

    fn mock_bitmap() -> Bitmap {
        Bitmap::from_lsb0_bytes(&[0b10101010, 0b10000000], BitmapType::Roaring)
    }

    fn mock_bitmap_bytes() -> Vec<u8> {
        let mut buf = Vec::new();
        mock_bitmap()
            .serialize_into(BitmapType::Roaring, &mut buf)
            .unwrap();
        buf
    }

    fn create_inverted_index_blob() -> Vec<u8> {
        let bitmap_size = mock_bitmap_bytes().len();
        let fst_size = mock_fst().len();

        // first index
        let mut inverted_index = Vec::new();
        inverted_index.extend_from_slice(&mock_bitmap_bytes()); // value bitmap
        inverted_index.extend_from_slice(&mock_bitmap_bytes()); // null bitmap
        inverted_index.extend_from_slice(&mock_fst()); // fst

        let meta = InvertedIndexMeta {
            name: "tag0".to_string(),
            base_offset: 0,
            inverted_index_size: inverted_index.len() as _,
            relative_null_bitmap_offset: bitmap_size as _,
            null_bitmap_size: bitmap_size as _,
            relative_fst_offset: (bitmap_size * 2) as _,
            fst_size: fst_size as _,
            bitmap_type: BitmapType::Roaring as _,
            ..Default::default()
        };

        // second index
        let meta1 = InvertedIndexMeta {
            name: "tag1".to_string(),
            base_offset: meta.inverted_index_size,
            inverted_index_size: inverted_index.len() as _,
            relative_null_bitmap_offset: bitmap_size as _,
            null_bitmap_size: bitmap_size as _,
            relative_fst_offset: (bitmap_size * 2) as _,
            fst_size: fst_size as _,
            bitmap_type: BitmapType::Roaring as _,
            ..Default::default()
        };

        // metas
        let mut metas = InvertedIndexMetas {
            total_row_count: 10,
            segment_row_count: 1,
            ..Default::default()
        };
        metas.metas.insert(meta.name.clone(), meta);
        metas.metas.insert(meta1.name.clone(), meta1);
        let mut meta_buf = Vec::new();
        metas.encode(&mut meta_buf).unwrap();

        let mut blob = vec![];

        // first index
        blob.extend_from_slice(&inverted_index);

        // second index
        blob.extend_from_slice(&inverted_index);

        // footer
        blob.extend_from_slice(&meta_buf);
        blob.extend_from_slice(&(meta_buf.len() as u32).to_le_bytes());

        blob
    }

    #[tokio::test]
    async fn test_inverted_index_blob_reader_metadata() {
        let blob = create_inverted_index_blob();
        let blob_reader = InvertedIndexBlobReader::new(blob);

        let metas = blob_reader.metadata().await.unwrap();
        assert_eq!(metas.metas.len(), 2);

        let meta0 = metas.metas.get("tag0").unwrap();
        assert_eq!(meta0.name, "tag0");
        assert_eq!(meta0.base_offset, 0);
        assert_eq!(meta0.inverted_index_size, 102);
        assert_eq!(meta0.relative_null_bitmap_offset, 26);
        assert_eq!(meta0.null_bitmap_size, 26);
        assert_eq!(meta0.relative_fst_offset, 52);
        assert_eq!(meta0.fst_size, 50);

        let meta1 = metas.metas.get("tag1").unwrap();
        assert_eq!(meta1.name, "tag1");
        assert_eq!(meta1.base_offset, 102);
        assert_eq!(meta1.inverted_index_size, 102);
        assert_eq!(meta1.relative_null_bitmap_offset, 26);
        assert_eq!(meta1.null_bitmap_size, 26);
        assert_eq!(meta1.relative_fst_offset, 52);
        assert_eq!(meta1.fst_size, 50);
    }

    #[tokio::test]
    async fn test_inverted_index_blob_reader_fst() {
        let blob = create_inverted_index_blob();
        let blob_reader = InvertedIndexBlobReader::new(blob);

        let metas = blob_reader.metadata().await.unwrap();
        let meta = metas.metas.get("tag0").unwrap();

        let fst_map = blob_reader
            .fst(
                meta.base_offset + meta.relative_fst_offset as u64,
                meta.fst_size,
                None,
            )
            .await
            .unwrap();
        assert_eq!(fst_map.len(), 2);
        assert_eq!(fst_map.get("key1".as_bytes()), Some(1));
        assert_eq!(fst_map.get("key2".as_bytes()), Some(2));

        let meta = metas.metas.get("tag1").unwrap();
        let fst_map = blob_reader
            .fst(
                meta.base_offset + meta.relative_fst_offset as u64,
                meta.fst_size,
                None,
            )
            .await
            .unwrap();
        assert_eq!(fst_map.len(), 2);
        assert_eq!(fst_map.get("key1".as_bytes()), Some(1));
        assert_eq!(fst_map.get("key2".as_bytes()), Some(2));
    }

    #[tokio::test]
    async fn test_inverted_index_blob_reader_bitmap() {
        let blob = create_inverted_index_blob();
        let blob_reader = InvertedIndexBlobReader::new(blob);

        let metas = blob_reader.metadata().await.unwrap();
        let meta = metas.metas.get("tag0").unwrap();

        let bitmap = blob_reader
            .bitmap(meta.base_offset, 26, BitmapType::Roaring, None)
            .await
            .unwrap();
        assert_eq!(bitmap, mock_bitmap());
        let bitmap = blob_reader
            .bitmap(meta.base_offset + 26, 26, BitmapType::Roaring, None)
            .await
            .unwrap();
        assert_eq!(bitmap, mock_bitmap());

        let metas = blob_reader.metadata().await.unwrap();
        let meta = metas.metas.get("tag1").unwrap();

        let bitmap = blob_reader
            .bitmap(meta.base_offset, 26, BitmapType::Roaring, None)
            .await
            .unwrap();
        assert_eq!(bitmap, mock_bitmap());
        let bitmap = blob_reader
            .bitmap(meta.base_offset + 26, 26, BitmapType::Roaring, None)
            .await
            .unwrap();
        assert_eq!(bitmap, mock_bitmap());
    }
}
