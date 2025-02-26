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

use std::io::Cursor;

use common_base::range_read::RangeReader;
use snafu::{ensure, ResultExt};

use crate::error::{
    DeserializeJsonSnafu, InvalidPuffinFooterSnafu, Lz4DecompressionSnafu, MagicNotMatchedSnafu,
    ReadSnafu, Result, UnexpectedFooterPayloadSizeSnafu,
};
use crate::file_format::{Flags, FLAGS_SIZE, MAGIC, MAGIC_SIZE, MIN_FILE_SIZE, PAYLOAD_SIZE_SIZE};
use crate::file_metadata::FileMetadata;

/// The default prefetch size for the footer reader.
pub const DEFAULT_PREFETCH_SIZE: u64 = 8192; // 8KiB

/// Reader for the footer of a Puffin data file
///
/// The footer has a specific layout that needs to be read and parsed to
/// extract metadata about the file, which is encapsulated in the [`FileMetadata`] type.
///
/// This reader supports prefetching, allowing for more efficient reading
/// of the footer by fetching additional data ahead of time.
///
/// ```text
/// Footer layout: HeadMagic Payload PayloadSize Flags FootMagic
///                [4]       [?]     [4]         [4]   [4]
/// ```
pub struct PuffinFileFooterReader<R> {
    /// The source of the puffin file
    source: R,
    /// The content length of the puffin file
    file_size: u64,
    /// The prefetch footer size
    prefetch_size: Option<u64>,
}

impl<'a, R: RangeReader + 'a> PuffinFileFooterReader<R> {
    pub fn new(source: R, content_len: u64) -> Self {
        Self {
            source,
            file_size: content_len,
            prefetch_size: None,
        }
    }

    fn prefetch_size(&self) -> u64 {
        self.prefetch_size.unwrap_or(MIN_FILE_SIZE)
    }

    pub fn with_prefetch_size(mut self, prefetch_size: u64) -> Self {
        self.prefetch_size = Some(prefetch_size.max(MIN_FILE_SIZE));
        self
    }

    pub async fn metadata(&'a mut self) -> Result<FileMetadata> {
        // Note: prefetch > content_len is allowed, since we're using saturating_sub.
        let footer_start = self.file_size.saturating_sub(self.prefetch_size());
        let suffix = self
            .source
            .read(footer_start..self.file_size)
            .await
            .context(ReadSnafu)?;
        let suffix_len = suffix.len();

        // check the magic
        let magic = Self::read_tailing_four_bytes(&suffix)?;
        ensure!(magic == MAGIC, MagicNotMatchedSnafu);

        let flags = self.decode_flags(&suffix[..suffix_len - MAGIC_SIZE as usize])?;
        let length = self.decode_payload_size(
            &suffix[..suffix_len - MAGIC_SIZE as usize - FLAGS_SIZE as usize],
        )?;
        let footer_size = PAYLOAD_SIZE_SIZE + FLAGS_SIZE + MAGIC_SIZE;

        // Did not fetch the entire file metadata in the initial read, need to make a second request.
        if length > suffix_len as u64 - footer_size {
            let metadata_start = self.file_size - length - footer_size;
            let meta = self
                .source
                .read(metadata_start..self.file_size - footer_size)
                .await
                .context(ReadSnafu)?;
            self.parse_payload(&flags, &meta)
        } else {
            let metadata_start = self.file_size - length - footer_size - footer_start;
            let meta = &suffix[metadata_start as usize..suffix_len - footer_size as usize];
            self.parse_payload(&flags, meta)
        }
    }

    fn parse_payload(&self, flags: &Flags, bytes: &[u8]) -> Result<FileMetadata> {
        if flags.contains(Flags::FOOTER_PAYLOAD_COMPRESSED_LZ4) {
            let decoder = lz4_flex::frame::FrameDecoder::new(Cursor::new(bytes));
            let res = serde_json::from_reader(decoder).context(Lz4DecompressionSnafu)?;
            Ok(res)
        } else {
            serde_json::from_slice(bytes).context(DeserializeJsonSnafu)
        }
    }

    fn read_tailing_four_bytes(suffix: &[u8]) -> Result<[u8; 4]> {
        let suffix_len = suffix.len();
        ensure!(suffix_len >= 4, InvalidPuffinFooterSnafu);
        let mut bytes = [0; 4];
        bytes.copy_from_slice(&suffix[suffix_len - 4..suffix_len]);

        Ok(bytes)
    }

    fn decode_flags(&self, suffix: &[u8]) -> Result<Flags> {
        let flags = u32::from_le_bytes(Self::read_tailing_four_bytes(suffix)?);
        Ok(Flags::from_bits_truncate(flags))
    }

    fn decode_payload_size(&self, suffix: &[u8]) -> Result<u64> {
        let payload_size = i32::from_le_bytes(Self::read_tailing_four_bytes(suffix)?);

        ensure!(
            payload_size >= 0,
            UnexpectedFooterPayloadSizeSnafu { size: payload_size }
        );
        let payload_size = payload_size as u64;
        ensure!(
            payload_size <= self.file_size - MIN_FILE_SIZE,
            UnexpectedFooterPayloadSizeSnafu {
                size: self.file_size as i32
            }
        );

        Ok(payload_size)
    }
}
