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

use std::num::NonZeroUsize;

use async_trait::async_trait;
use common_base::BitVec;
use futures::{AsyncWrite, AsyncWriteExt};
use greptime_proto::v1::index::InvertedIndexMetas;
use prost::Message;
use snafu::ResultExt;

use crate::inverted_index::error::{CloseSnafu, FlushSnafu, Result, WriteSnafu};
use crate::inverted_index::format::writer::single::SingleIndexWriter;
use crate::inverted_index::format::writer::{InvertedIndexWriter, ValueStream};

/// `InvertedIndexBlobWriter`, implemented [`InvertedIndexWriter`], manages
/// writing of an inverted index to a blob storage.
pub struct InvertedIndexBlobWriter<W> {
    /// The underlying blob storage
    blob_writer: W,

    /// Tracks the total number of bytes written to the storage so far
    written_size: u64,

    /// Metadata about each index that has been written  
    metas: InvertedIndexMetas,
}

#[async_trait]
impl<W: AsyncWrite + Send + Unpin> InvertedIndexWriter for InvertedIndexBlobWriter<W> {
    async fn add_index(
        &mut self,
        name: String,
        null_bitmap: BitVec,
        values: ValueStream,
    ) -> Result<()> {
        let single_writer = SingleIndexWriter::new(
            name.clone(),
            self.written_size,
            null_bitmap,
            values,
            &mut self.blob_writer,
        );
        let metadata = single_writer.write().await?;

        self.written_size += metadata.inverted_index_size;
        self.metas.metas.insert(name, metadata);

        Ok(())
    }

    async fn finish(
        &mut self,
        total_row_count: u64,
        segment_row_count: NonZeroUsize,
    ) -> Result<()> {
        self.metas.segment_row_count = segment_row_count.get() as _;
        self.metas.total_row_count = total_row_count;

        let metas_bytes = self.metas.encode_to_vec();
        self.blob_writer
            .write_all(&metas_bytes)
            .await
            .context(WriteSnafu)?;

        let footer_size = metas_bytes.len() as u32;
        self.blob_writer
            .write_all(&footer_size.to_le_bytes())
            .await
            .context(WriteSnafu)?;

        self.blob_writer.flush().await.context(FlushSnafu)?;
        self.blob_writer.close().await.context(CloseSnafu)?;
        Ok(())
    }
}

impl<W: AsyncWrite + Send + Unpin> InvertedIndexBlobWriter<W> {
    pub fn new(blob_writer: W) -> InvertedIndexBlobWriter<W> {
        InvertedIndexBlobWriter {
            blob_writer,
            written_size: 0,
            metas: InvertedIndexMetas::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use common_base::range_read::RangeReaderAdapter;
    use futures::io::Cursor;
    use futures::stream;

    use super::*;
    use crate::inverted_index::format::reader::{InvertedIndexBlobReader, InvertedIndexReader};
    use crate::inverted_index::Bytes;

    fn unpack(fst_value: u64) -> [u32; 2] {
        bytemuck::cast::<u64, [u32; 2]>(fst_value)
    }

    #[tokio::test]
    async fn test_inverted_index_blob_writer_write_empty() {
        let mut blob = Vec::new();
        let mut writer = InvertedIndexBlobWriter::new(&mut blob);
        writer
            .finish(8, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap();

        let cursor = RangeReaderAdapter(Cursor::new(blob));
        let mut reader = InvertedIndexBlobReader::new(cursor);
        let metadata = reader.metadata().await.unwrap();
        assert_eq!(metadata.total_row_count, 8);
        assert_eq!(metadata.segment_row_count, 1);
        assert_eq!(metadata.metas.len(), 0);
    }

    #[tokio::test]
    async fn test_inverted_index_blob_writer_write_basic() {
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

        let cursor = RangeReaderAdapter(Cursor::new(blob));
        let mut reader = InvertedIndexBlobReader::new(cursor);
        let metadata = reader.metadata().await.unwrap();
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
        let fst0 = reader
            .fst(
                tag0.base_offset + tag0.relative_fst_offset as u64,
                tag0.fst_size,
            )
            .await
            .unwrap();
        assert_eq!(fst0.len(), 3);
        let [offset, size] = unpack(fst0.get(b"a").unwrap());
        let bitmap = reader
            .bitmap(tag0.base_offset + offset as u64, size)
            .await
            .unwrap();
        assert_eq!(bitmap, BitVec::from_slice(&[0b0000_0001]));
        let [offset, size] = unpack(fst0.get(b"b").unwrap());
        let bitmap = reader
            .bitmap(tag0.base_offset + offset as u64, size)
            .await
            .unwrap();
        assert_eq!(bitmap, BitVec::from_slice(&[0b0010_0000]));
        let [offset, size] = unpack(fst0.get(b"c").unwrap());
        let bitmap = reader
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
        let fst1 = reader
            .fst(
                tag1.base_offset + tag1.relative_fst_offset as u64,
                tag1.fst_size,
            )
            .await
            .unwrap();
        assert_eq!(fst1.len(), 3);
        let [offset, size] = unpack(fst1.get(b"x").unwrap());
        let bitmap = reader
            .bitmap(tag1.base_offset + offset as u64, size)
            .await
            .unwrap();
        assert_eq!(bitmap, BitVec::from_slice(&[0b0000_0001]));
        let [offset, size] = unpack(fst1.get(b"y").unwrap());
        let bitmap = reader
            .bitmap(tag1.base_offset + offset as u64, size)
            .await
            .unwrap();
        assert_eq!(bitmap, BitVec::from_slice(&[0b0010_0000]));
        let [offset, size] = unpack(fst1.get(b"z").unwrap());
        let bitmap = reader
            .bitmap(tag1.base_offset + offset as u64, size)
            .await
            .unwrap();
        assert_eq!(bitmap, BitVec::from_slice(&[0b0000_0001]));
    }
}
