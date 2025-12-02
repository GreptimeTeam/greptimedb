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

use std::time::Instant;

use common_base::range_read::RangeReader;
use greptime_proto::v1::index::{InvertedIndexMeta, InvertedIndexMetas};
use prost::Message;
use snafu::{ResultExt, ensure};

use crate::inverted_index::error::{
    BlobSizeTooSmallSnafu, CommonIoSnafu, DecodeProtoSnafu, InvalidFooterPayloadSizeSnafu, Result,
    UnexpectedFooterPayloadSizeSnafu, UnexpectedOffsetSizeSnafu,
    UnexpectedZeroSegmentRowCountSnafu,
};
use crate::inverted_index::format::FOOTER_PAYLOAD_SIZE_SIZE;
use crate::inverted_index::format::reader::InvertedIndexReadMetrics;

pub const DEFAULT_PREFETCH_SIZE: u64 = 8192; // 8KiB

/// InvertedIndexFooterReader is for reading the footer section of the blob.
pub struct InvertedIndexFooterReader<R> {
    source: R,
    blob_size: u64,
    prefetch_size: Option<u64>,
}

impl<R> InvertedIndexFooterReader<R> {
    pub fn new(source: R, blob_size: u64) -> Self {
        Self {
            source,
            blob_size,
            prefetch_size: None,
        }
    }

    /// Set the prefetch size for the footer reader.
    pub fn with_prefetch_size(mut self, prefetch_size: u64) -> Self {
        self.prefetch_size = Some(prefetch_size.max(FOOTER_PAYLOAD_SIZE_SIZE));
        self
    }

    pub fn prefetch_size(&self) -> u64 {
        self.prefetch_size.unwrap_or(FOOTER_PAYLOAD_SIZE_SIZE)
    }
}

impl<R: RangeReader> InvertedIndexFooterReader<R> {
    pub async fn metadata(
        &mut self,
        mut metrics: Option<&mut InvertedIndexReadMetrics>,
    ) -> Result<InvertedIndexMetas> {
        ensure!(
            self.blob_size >= FOOTER_PAYLOAD_SIZE_SIZE,
            BlobSizeTooSmallSnafu
        );

        let start = metrics.as_ref().map(|_| Instant::now());

        let footer_start = self.blob_size.saturating_sub(self.prefetch_size());
        let suffix = self
            .source
            .read(footer_start..self.blob_size)
            .await
            .context(CommonIoSnafu)?;
        let suffix_len = suffix.len();
        let length = u32::from_le_bytes(Self::read_tailing_four_bytes(&suffix)?) as u64;
        self.validate_payload_size(length)?;

        let footer_size = FOOTER_PAYLOAD_SIZE_SIZE;

        // Did not fetch the entire file metadata in the initial read, need to make a second request.
        let result = if length > suffix_len as u64 - footer_size {
            let metadata_start = self.blob_size - length - footer_size;
            let meta = self
                .source
                .read(metadata_start..self.blob_size - footer_size)
                .await
                .context(CommonIoSnafu)?;

            if let Some(m) = metrics.as_deref_mut() {
                m.total_bytes += self.blob_size.min(self.prefetch_size()) + length;
                m.total_ranges += 2;
            }

            self.parse_payload(&meta, length)
        } else {
            if let Some(m) = metrics.as_deref_mut() {
                m.total_bytes += self.blob_size.min(self.prefetch_size());
                m.total_ranges += 1;
            }

            let metadata_start = self.blob_size - length - footer_size - footer_start;
            let meta = &suffix[metadata_start as usize..suffix_len - footer_size as usize];
            self.parse_payload(meta, length)
        };

        if let Some(m) = metrics {
            m.fetch_elapsed += start.unwrap().elapsed();
        }

        result
    }

    fn read_tailing_four_bytes(suffix: &[u8]) -> Result<[u8; 4]> {
        let suffix_len = suffix.len();
        ensure!(suffix_len >= 4, InvalidFooterPayloadSizeSnafu);
        let mut bytes = [0; 4];
        bytes.copy_from_slice(&suffix[suffix_len - 4..suffix_len]);

        Ok(bytes)
    }

    fn parse_payload(&mut self, bytes: &[u8], payload_size: u64) -> Result<InvertedIndexMetas> {
        let metas = InvertedIndexMetas::decode(bytes).context(DecodeProtoSnafu)?;
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
    use std::assert_matches::assert_matches;

    use prost::Message;

    use super::*;
    use crate::inverted_index::error::Error;

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

        for prefetch in [0, blob_size / 2, blob_size, blob_size + 10] {
            let mut reader = InvertedIndexFooterReader::new(&payload_buf, blob_size);
            if prefetch > 0 {
                reader = reader.with_prefetch_size(prefetch);
            }

            let metas = reader.metadata(None).await.unwrap();
            assert_eq!(metas.metas.len(), 1);
            let index_meta = &metas.metas.get("test").unwrap();
            assert_eq!(index_meta.name, "test");
        }
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

        for prefetch in [0, blob_size / 2, blob_size, blob_size + 10] {
            let blob_size = payload_buf.len() as u64;
            let mut reader = InvertedIndexFooterReader::new(&payload_buf, blob_size);
            if prefetch > 0 {
                reader = reader.with_prefetch_size(prefetch);
            }

            let result = reader.metadata(None).await;
            assert_matches!(result, Err(Error::UnexpectedFooterPayloadSize { .. }));
        }
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

        for prefetch in [0, blob_size / 2, blob_size, blob_size + 10] {
            let mut reader = InvertedIndexFooterReader::new(&payload_buf, blob_size);
            if prefetch > 0 {
                reader = reader.with_prefetch_size(prefetch);
            }

            let result = reader.metadata(None).await;
            assert_matches!(result, Err(Error::UnexpectedOffsetSize { .. }));
        }
    }
}
