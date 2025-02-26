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

use fst::MapBuilder;
use futures::{AsyncWrite, AsyncWriteExt, Stream, StreamExt};
use greptime_proto::v1::index::{InvertedIndexMeta, InvertedIndexStats};
use snafu::ResultExt;

use crate::bitmap::{Bitmap, BitmapType};
use crate::inverted_index::error::{FstCompileSnafu, FstInsertSnafu, Result, WriteSnafu};
use crate::Bytes;

/// `SingleIndexWriter` writes values to the blob storage for an individual inverted index
pub struct SingleIndexWriter<W, S> {
    /// The underlying blob storage
    blob_writer: W,

    /// The null bitmap to be written
    null_bitmap: Bitmap,

    /// The stream of values to be written, yielded lexicographically
    values: S,

    /// Builder for constructing the FST
    fst: MapBuilder<Vec<u8>>,

    /// Metadata about the index
    meta: InvertedIndexMeta,

    /// The type of bitmap to use
    bitmap_type: BitmapType,

    /// Buffer for writing the blob
    buf: Vec<u8>,
}

impl<W, S> SingleIndexWriter<W, S>
where
    W: AsyncWrite + Send + Unpin,
    S: Stream<Item = Result<(Bytes, Bitmap)>> + Send + Unpin,
{
    /// Constructs a new `SingleIndexWriter`
    pub fn new(
        name: String,
        base_offset: u64,
        null_bitmap: Bitmap,
        values: S,
        blob_writer: W,
        bitmap_type: BitmapType,
    ) -> SingleIndexWriter<W, S> {
        SingleIndexWriter {
            blob_writer,
            null_bitmap,
            values,
            fst: MapBuilder::memory(),
            bitmap_type,
            buf: Vec::new(),
            meta: InvertedIndexMeta {
                name,
                base_offset,
                stats: Some(InvertedIndexStats::default()),
                bitmap_type: bitmap_type.into(),
                ..Default::default()
            },
        }
    }

    /// Writes the null bitmap, values with their bitmaps, and constructs the FST map.
    pub async fn write(mut self) -> Result<InvertedIndexMeta> {
        self.write_null_bitmap().await?;

        while let Some(result) = self.values.next().await {
            let (bytes, bitmap) = result?;
            self.append_value(bytes, bitmap).await?;
        }

        self.finish_fst_construction().await
    }

    /// Writes the null bitmap to the blob and updates the metadata accordingly
    async fn write_null_bitmap(&mut self) -> Result<()> {
        self.buf.clear();
        self.null_bitmap
            .serialize_into(self.bitmap_type, &mut self.buf);
        self.blob_writer
            .write_all(&self.buf)
            .await
            .context(WriteSnafu)?;

        self.meta.relative_null_bitmap_offset = self.meta.inverted_index_size as _;
        self.meta.null_bitmap_size = self.buf.len() as _;
        self.meta.inverted_index_size += self.meta.null_bitmap_size as u64;

        // update stats
        if let Some(stats) = self.meta.stats.as_mut() {
            let null_count = self.null_bitmap.count_ones();
            stats.null_count = null_count as u64;
        }

        Ok(())
    }

    /// Appends a value and its bitmap to the blob, updates the FST, and the metadata
    async fn append_value(&mut self, value: Bytes, bitmap: Bitmap) -> Result<()> {
        self.buf.clear();
        bitmap.serialize_into(self.bitmap_type, &mut self.buf);
        self.blob_writer
            .write_all(&self.buf)
            .await
            .context(WriteSnafu)?;

        let offset = self.meta.inverted_index_size as u32;
        let size = self.buf.len() as u32;
        self.meta.inverted_index_size += size as u64;

        let packed = bytemuck::cast::<[u32; 2], u64>([offset, size]);
        self.fst.insert(&value, packed).context(FstInsertSnafu)?;

        // update stats
        if let Some(stats) = self.meta.stats.as_mut() {
            stats.distinct_count += 1;

            // update min/max, assume values are appended in lexicographic order
            if stats.distinct_count == 1 {
                stats.min_value.clone_from(&value);
            }
            stats.max_value = value;
        }

        Ok(())
    }

    /// Writes the compiled FST to the blob and finalizes the metadata
    async fn finish_fst_construction(mut self) -> Result<InvertedIndexMeta> {
        let fst_bytes = self.fst.into_inner().context(FstCompileSnafu)?;
        self.blob_writer
            .write_all(&fst_bytes)
            .await
            .context(WriteSnafu)?;

        self.meta.relative_fst_offset = self.meta.inverted_index_size as _;
        self.meta.fst_size = fst_bytes.len() as _;
        self.meta.inverted_index_size += self.meta.fst_size as u64;
        Ok(self.meta)
    }
}

#[cfg(test)]
mod tests {
    use futures::stream;

    use super::*;
    use crate::inverted_index::error::Error;
    use crate::Bytes;

    #[tokio::test]
    async fn test_single_index_writer_write_empty() {
        let mut blob = Vec::new();
        let writer = SingleIndexWriter::new(
            "test".to_string(),
            0,
            Bitmap::new_roaring(),
            stream::empty(),
            &mut blob,
            BitmapType::Roaring,
        );

        let meta = writer.write().await.unwrap();
        assert_eq!(meta.name, "test");
        assert_eq!(meta.base_offset, 0);
        assert_eq!(meta.stats, Some(InvertedIndexStats::default()));
    }

    #[tokio::test]
    async fn test_single_index_writer_write_basic() {
        let mut blob = Vec::new();
        let writer = SingleIndexWriter::new(
            "test".to_string(),
            0,
            Bitmap::from_lsb0_bytes(&[0b0000_0001, 0b0000_0000], BitmapType::Roaring),
            stream::iter(vec![
                Ok((
                    Bytes::from("a"),
                    Bitmap::from_lsb0_bytes(&[0b0000_0001], BitmapType::Roaring),
                )),
                Ok((
                    Bytes::from("b"),
                    Bitmap::from_lsb0_bytes(&[0b0000_0000], BitmapType::Roaring),
                )),
                Ok((
                    Bytes::from("c"),
                    Bitmap::from_lsb0_bytes(&[0b0000_0001], BitmapType::Roaring),
                )),
            ]),
            &mut blob,
            BitmapType::Roaring,
        );
        let meta = writer.write().await.unwrap();

        assert_eq!(meta.name, "test");
        assert_eq!(meta.base_offset, 0);
        let stats = meta.stats.as_ref().unwrap();
        assert_eq!(stats.distinct_count, 3);
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.min_value, Bytes::from("a"));
        assert_eq!(stats.max_value, Bytes::from("c"));
    }

    #[tokio::test]
    async fn test_single_index_writer_write_out_of_order() {
        let mut blob = Vec::new();
        let writer = SingleIndexWriter::new(
            "test".to_string(),
            0,
            Bitmap::from_lsb0_bytes(&[0b0000_0001, 0b0000_0000], BitmapType::Roaring),
            stream::iter(vec![
                Ok((
                    Bytes::from("b"),
                    Bitmap::from_lsb0_bytes(&[0b0000_0000], BitmapType::Roaring),
                )),
                Ok((
                    Bytes::from("a"),
                    Bitmap::from_lsb0_bytes(&[0b0000_0001], BitmapType::Roaring),
                )),
                Ok((
                    Bytes::from("c"),
                    Bitmap::from_lsb0_bytes(&[0b0000_0001], BitmapType::Roaring),
                )),
            ]),
            &mut blob,
            BitmapType::Roaring,
        );
        let res = writer.write().await;
        assert!(matches!(res, Err(Error::FstInsert { .. })));
    }
}
