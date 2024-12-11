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
    BytesToIntegerSnafu, DeserializeJsonSnafu, InvalidBlobAreaEndSnafu, InvalidBlobOffsetSnafu,
    InvalidPuffinFooterSnafu, Lz4DecompressionSnafu, MagicNotMatchedSnafu, ParseStageNotMatchSnafu,
    ReadSnafu, Result, UnexpectedFooterPayloadSizeSnafu,
};
use crate::file_format::{Flags, FLAGS_SIZE, MAGIC, MAGIC_SIZE, MIN_FILE_SIZE, PAYLOAD_SIZE_SIZE};
use crate::file_metadata::FileMetadata;

/// Parser for the footer of a Puffin data file
///
/// The footer has a specific layout that needs to be read and parsed to
/// extract metadata about the file, which is encapsulated in the [`FileMetadata`] type.
///
/// ```text
/// Footer layout: HeadMagic Payload PayloadSize Flags FootMagic
///                [4]       [?]     [4]         [4]   [4]
/// ```
pub struct FooterParser<R> {
    // The underlying IO source
    source: R,

    // The size of the file, used for calculating offsets to read from
    file_size: u64,
}

impl<R> FooterParser<R> {
    pub fn new(source: R, file_size: u64) -> Self {
        Self { source, file_size }
    }
}

impl<R: RangeReader> FooterParser<R> {
    /// Parses the footer from the IO source in a asynchronous manner.
    pub async fn parse_async(&mut self) -> Result<FileMetadata> {
        let mut parser = StageParser::new(self.file_size);

        let mut buf = vec![];
        while let Some(byte_to_read) = parser.next_to_read() {
            buf.clear();
            let range = byte_to_read.offset..byte_to_read.offset + byte_to_read.size;
            self.source
                .read_into(range, &mut buf)
                .await
                .context(ReadSnafu)?;
            parser.consume_bytes(&buf)?;
        }

        parser.finish()
    }
}

/// The internal stages of parsing the footer.
/// This enum allows the StageParser to keep track of which part
/// of the footer needs to be parsed next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParseStage {
    FootMagic,
    Flags,
    PayloadSize,
    Payload,
    HeadMagic,
    Done,
}

/// Manages the parsing process of the file's footer.
struct StageParser {
    /// Current stage in the parsing sequence of the footer.
    stage: ParseStage,

    /// Total file size; used for calculating offsets to read from.
    file_size: u64,

    /// Flags from the footer, set when the `Flags` field is parsed.
    flags: Flags,

    /// Size of the footer's payload, set when the `PayloadSize` is parsed.
    payload_size: u64,

    /// Metadata from the footer's payload, set when the `Payload` is parsed.
    metadata: Option<FileMetadata>,
}

/// Represents a read operation that needs to be performed, including the
/// offset from the start of the file and the number of bytes to read.
struct BytesToRead {
    offset: u64,
    size: u64,
}

impl StageParser {
    fn new(file_size: u64) -> Self {
        Self {
            stage: ParseStage::FootMagic,
            file_size,
            payload_size: 0,
            flags: Flags::empty(),
            metadata: None,
        }
    }

    /// Determines the next segment of bytes to read based on the current parsing stage.
    /// This method returns information like the offset and size of the next read,
    /// or None if parsing is complete.
    fn next_to_read(&self) -> Option<BytesToRead> {
        if self.stage == ParseStage::Done {
            return None;
        }

        let btr = match self.stage {
            ParseStage::FootMagic => BytesToRead {
                offset: self.foot_magic_offset(),
                size: MAGIC_SIZE,
            },
            ParseStage::Flags => BytesToRead {
                offset: self.flags_offset(),
                size: FLAGS_SIZE,
            },
            ParseStage::PayloadSize => BytesToRead {
                offset: self.payload_size_offset(),
                size: PAYLOAD_SIZE_SIZE,
            },
            ParseStage::Payload => BytesToRead {
                offset: self.payload_offset(),
                size: self.payload_size,
            },
            ParseStage::HeadMagic => BytesToRead {
                offset: self.head_magic_offset(),
                size: MAGIC_SIZE,
            },
            ParseStage::Done => unreachable!(),
        };

        Some(btr)
    }

    /// Processes the bytes that have been read according to the current parsing stage
    /// and advances the parsing stage. It ensures the correct sequence of bytes is
    /// encountered and stores the necessary information in the `StageParser`.
    fn consume_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        match self.stage {
            ParseStage::FootMagic => {
                ensure!(bytes == MAGIC, MagicNotMatchedSnafu);
                self.stage = ParseStage::Flags;
            }
            ParseStage::Flags => {
                self.flags = Self::parse_flags(bytes)?;
                self.stage = ParseStage::PayloadSize;
            }
            ParseStage::PayloadSize => {
                self.payload_size = Self::parse_payload_size(bytes)?;
                self.validate_payload_size()?;
                self.stage = ParseStage::Payload;
            }
            ParseStage::Payload => {
                self.metadata = Some(self.parse_payload(bytes)?);
                self.validate_metadata()?;
                self.stage = ParseStage::HeadMagic;
            }
            ParseStage::HeadMagic => {
                ensure!(bytes == MAGIC, MagicNotMatchedSnafu);
                self.stage = ParseStage::Done;
            }
            ParseStage::Done => unreachable!(),
        }

        Ok(())
    }

    /// Finalizes the parsing process, ensuring all stages are complete, and returns
    /// the parsed `FileMetadata`. It converts the raw footer payload into structured data.
    fn finish(self) -> Result<FileMetadata> {
        ensure!(
            self.stage == ParseStage::Done,
            ParseStageNotMatchSnafu {
                expected: format!("{:?}", ParseStage::Done),
                actual: format!("{:?}", self.stage),
            }
        );

        Ok(self.metadata.unwrap())
    }

    fn parse_flags(bytes: &[u8]) -> Result<Flags> {
        let n = u32::from_le_bytes(bytes.try_into().context(BytesToIntegerSnafu)?);
        Ok(Flags::from_bits_truncate(n))
    }

    fn parse_payload_size(bytes: &[u8]) -> Result<u64> {
        let n = i32::from_le_bytes(bytes.try_into().context(BytesToIntegerSnafu)?);
        ensure!(n >= 0, UnexpectedFooterPayloadSizeSnafu { size: n });
        Ok(n as u64)
    }

    fn validate_payload_size(&self) -> Result<()> {
        ensure!(
            self.payload_size <= self.file_size - MIN_FILE_SIZE,
            UnexpectedFooterPayloadSizeSnafu {
                size: self.payload_size as i32
            }
        );
        Ok(())
    }

    fn parse_payload(&self, bytes: &[u8]) -> Result<FileMetadata> {
        if self.flags.contains(Flags::FOOTER_PAYLOAD_COMPRESSED_LZ4) {
            let decoder = lz4_flex::frame::FrameDecoder::new(Cursor::new(bytes));
            let res = serde_json::from_reader(decoder).context(Lz4DecompressionSnafu)?;
            Ok(res)
        } else {
            serde_json::from_slice(bytes).context(DeserializeJsonSnafu)
        }
    }

    fn validate_metadata(&self) -> Result<()> {
        let metadata = self.metadata.as_ref().expect("metadata is not set");

        let mut next_blob_offset = MAGIC_SIZE;
        // check blob offsets
        for blob in &metadata.blobs {
            ensure!(
                blob.offset as u64 == next_blob_offset,
                InvalidBlobOffsetSnafu {
                    offset: blob.offset
                }
            );
            next_blob_offset += blob.length as u64;
        }

        let blob_area_end = metadata
            .blobs
            .last()
            .map_or(MAGIC_SIZE, |b| (b.offset + b.length) as u64);
        ensure!(
            blob_area_end == self.head_magic_offset(),
            InvalidBlobAreaEndSnafu {
                offset: blob_area_end
            }
        );

        Ok(())
    }

    fn foot_magic_offset(&self) -> u64 {
        self.file_size - MAGIC_SIZE
    }

    fn flags_offset(&self) -> u64 {
        self.file_size - MAGIC_SIZE - FLAGS_SIZE
    }

    fn payload_size_offset(&self) -> u64 {
        self.file_size - MAGIC_SIZE - FLAGS_SIZE - PAYLOAD_SIZE_SIZE
    }

    fn payload_offset(&self) -> u64 {
        // `validate_payload_size` ensures that this subtraction will not overflow
        self.file_size - MAGIC_SIZE - FLAGS_SIZE - PAYLOAD_SIZE_SIZE - self.payload_size
    }

    fn head_magic_offset(&self) -> u64 {
        // `validate_payload_size` ensures that this subtraction will not overflow
        self.file_size - MAGIC_SIZE * 2 - FLAGS_SIZE - PAYLOAD_SIZE_SIZE - self.payload_size
    }
}

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
