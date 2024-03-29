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

use std::io::SeekFrom;

use futures::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};
use greptime_proto::v1::index::{InvertedIndexMeta, InvertedIndexMetas};
use prost::Message;
use snafu::{ensure, ResultExt};

use crate::inverted_index::error::{
    DecodeProtoSnafu, ReadSnafu, Result, SeekSnafu, UnexpectedFooterPayloadSizeSnafu,
    UnexpectedOffsetSizeSnafu, UnexpectedZeroSegmentRowCountSnafu,
};
use crate::inverted_index::format::FOOTER_PAYLOAD_SIZE_SIZE;

/// InvertedIndeFooterReader is for reading the footer section of the blob.
pub struct InvertedIndeFooterReader<R> {
    source: R,
    blob_size: u64,
}

impl<R> InvertedIndeFooterReader<R> {
    pub fn new(source: R, blob_size: u64) -> Self {
        Self { source, blob_size }
    }
}

impl<R: AsyncRead + AsyncSeek + Unpin> InvertedIndeFooterReader<R> {
    pub async fn metadata(&mut self) -> Result<InvertedIndexMetas> {
        let payload_size = self.read_payload_size().await?;
        let metas = self.read_payload(payload_size).await?;
        Ok(metas)
    }

    async fn read_payload_size(&mut self) -> Result<u64> {
        let size_offset = SeekFrom::Start(self.blob_size - FOOTER_PAYLOAD_SIZE_SIZE);
        self.source.seek(size_offset).await.context(SeekSnafu)?;
        let size_buf = &mut [0u8; FOOTER_PAYLOAD_SIZE_SIZE as usize];
        self.source.read_exact(size_buf).await.context(ReadSnafu)?;

        let payload_size = u32::from_le_bytes(*size_buf) as u64;
        self.validate_payload_size(payload_size)?;

        Ok(payload_size)
    }

    async fn read_payload(&mut self, payload_size: u64) -> Result<InvertedIndexMetas> {
        let payload_offset =
            SeekFrom::Start(self.blob_size - FOOTER_PAYLOAD_SIZE_SIZE - payload_size);
        self.source.seek(payload_offset).await.context(SeekSnafu)?;

        let payload = &mut vec![0u8; payload_size as usize];
        self.source.read_exact(payload).await.context(ReadSnafu)?;

        let metas = InvertedIndexMetas::decode(&payload[..]).context(DecodeProtoSnafu)?;
        self.validate_metas(&metas, payload_size)?;

        Ok(metas)
    }

    fn validate_payload_size(&self, payload_size: u64) -> Result<()> {
        let max_payload_size = self.blob_size - FOOTER_PAYLOAD_SIZE_SIZE;
        ensure!(
            payload_size <= max_payload_size,
            UnexpectedFooterPayloadSizeSnafu {
                max_payload_size,
                actual_payload_size: payload_size,
            }
        );

        Ok(())
    }

    /// Check if the read metadata is consistent with expected sizes and offsets.
    fn validate_metas(&self, metas: &InvertedIndexMetas, payload_size: u64) -> Result<()> {
        ensure!(
            metas.segment_row_count > 0,
            UnexpectedZeroSegmentRowCountSnafu
        );

        for meta in metas.metas.values() {
            let InvertedIndexMeta {
                base_offset,
                inverted_index_size,
                ..
            } = meta;

            let limit = self.blob_size - FOOTER_PAYLOAD_SIZE_SIZE - payload_size;
            ensure!(
                *base_offset + *inverted_index_size <= limit,
                UnexpectedOffsetSizeSnafu {
                    offset: *base_offset,
                    size: *inverted_index_size,
                    blob_size: self.blob_size,
                    payload_size,
                }
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use futures::io::Cursor;
    use prost::Message;

    use super::*;

    fn create_test_payload(meta: InvertedIndexMeta) -> Vec<u8> {
        let mut metas = InvertedIndexMetas {
            segment_row_count: 1,
            ..Default::default()
        };
        metas.metas.insert("test".to_string(), meta);

        let mut payload_buf = vec![];
        metas.encode(&mut payload_buf).unwrap();

        let footer_payload_size = (payload_buf.len() as u32).to_le_bytes().to_vec();
        payload_buf.extend_from_slice(&footer_payload_size);
        payload_buf
    }

    #[tokio::test]
    async fn test_read_payload() {
        let meta = InvertedIndexMeta {
            name: "test".to_string(),
            ..Default::default()
        };

        let payload_buf = create_test_payload(meta);
        let blob_size = payload_buf.len() as u64;
        let mut reader = InvertedIndeFooterReader::new(Cursor::new(payload_buf), blob_size);

        let payload_size = reader.read_payload_size().await.unwrap();
        let metas = reader.read_payload(payload_size).await.unwrap();

        assert_eq!(metas.metas.len(), 1);
        let index_meta = &metas.metas.get("test").unwrap();
        assert_eq!(index_meta.name, "test");
    }

    #[tokio::test]
    async fn test_invalid_footer_payload_size() {
        let meta = InvertedIndexMeta {
            name: "test".to_string(),
            ..Default::default()
        };

        let mut payload_buf = create_test_payload(meta);
        payload_buf.push(0xff); // Add an extra byte to corrupt the footer
        let blob_size = payload_buf.len() as u64;
        let mut reader = InvertedIndeFooterReader::new(Cursor::new(payload_buf), blob_size);

        let payload_size_result = reader.read_payload_size().await;
        assert!(payload_size_result.is_err());
    }

    #[tokio::test]
    async fn test_invalid_offset_size() {
        let meta = InvertedIndexMeta {
            name: "test".to_string(),
            base_offset: 0,
            inverted_index_size: 1, // Set size to 1 to make ecceed the blob size
            ..Default::default()
        };

        let payload_buf = create_test_payload(meta);
        let blob_size = payload_buf.len() as u64;
        let mut reader = InvertedIndeFooterReader::new(Cursor::new(payload_buf), blob_size);

        let payload_size = reader.read_payload_size().await.unwrap();
        let payload_result = reader.read_payload(payload_size).await;
        assert!(payload_result.is_err());
    }
}
