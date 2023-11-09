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

use std::io::{self, Read, SeekFrom};

use futures::{AsyncRead, AsyncReadExt as _, AsyncSeek, AsyncSeekExt as _};
use snafu::{ensure, ResultExt};

use crate::error::{
    DeserializeJsonSnafu, MagicNotMatchedSnafu, ReadSnafu, Result, SeekSnafu,
    UnsupportedDecompressionSnafu,
};
use crate::file_format::{Flags, MAGIC};
use crate::file_metadata::FileMetadata;
use crate::partial_reader::PartialReader;

/// Parser of the footer in Puffin
///
/// Footer structure: Magic FooterPayload FooterPayloadSize Flags Magic
///                   [4]   [?]           [4]               [4]   [4]
pub struct FooterParser<R> {
    source: R,
}

impl<R> FooterParser<R> {
    pub fn new(source: R) -> Self {
        Self { source }
    }
}

impl<R: io::Read + io::Seek> FooterParser<R> {
    pub fn parse_sync(&mut self) -> Result<FileMetadata> {
        // seek to the end of the file
        let file_size = self.source.seek(SeekFrom::End(0)).context(SeekSnafu)?;

        // check the magic
        self.check_magic_sync(last_magic_offset(file_size))?;

        // read the flags
        let flags = self.read_flags_sync(flags_offset(file_size))?;

        // read the footer payload size
        let payload_size = self.read_payload_size_sync(payload_size_offset(file_size))? as u64;

        // check the magic
        self.check_magic_sync(first_magic_offset(file_size, payload_size))?;

        // read the footer payload
        let payload =
            self.read_payload_sync(payload_offset(file_size, payload_size), payload_size, flags)?;

        // deserialize the footer payload
        let metadata: FileMetadata =
            serde_json::from_slice(payload.as_slice()).context(DeserializeJsonSnafu)?;

        Ok(metadata)
    }

    fn check_magic_sync(&mut self, offset: u64) -> Result<()> {
        let mut magic = [0; 4];
        self.source
            .seek(SeekFrom::Start(offset))
            .context(SeekSnafu)?;
        self.source.read_exact(&mut magic).context(ReadSnafu)?;

        ensure!(magic == MAGIC, MagicNotMatchedSnafu);
        Ok(())
    }

    fn read_flags_sync(&mut self, offset: u64) -> Result<Flags> {
        let mut flags = [0; 4];
        self.source
            .seek(SeekFrom::Start(offset))
            .context(SeekSnafu)?;
        self.source.read_exact(&mut flags).context(ReadSnafu)?;
        let flags = Flags::from_bits_truncate(u32::from_le_bytes(flags));

        Ok(flags)
    }

    fn read_payload_size_sync(&mut self, offset: u64) -> Result<i32> {
        let mut payload_size = [0; 4];
        self.source
            .seek(SeekFrom::Start(offset))
            .context(SeekSnafu)?;
        self.source
            .read_exact(&mut payload_size)
            .context(ReadSnafu)?;
        let payload_size = i32::from_le_bytes(payload_size);

        Ok(payload_size)
    }

    fn read_payload_sync(&mut self, offset: u64, size: u64, flags: Flags) -> Result<Vec<u8>> {
        // TODO(zhongzc): support lz4
        ensure!(
            !flags.contains(Flags::FOOTER_PAYLOAD_COMPRESSED_LZ4),
            UnsupportedDecompressionSnafu {
                decompression: "lz4"
            }
        );

        let mut reader = PartialReader::new(&mut self.source, offset, size);
        let mut payload = vec![];
        reader.read_to_end(&mut payload).context(ReadSnafu)?;

        Ok(payload)
    }
}

impl<R: AsyncRead + AsyncSeek + Unpin> FooterParser<R> {
    pub async fn parse_async(&mut self) -> Result<FileMetadata> {
        // seek to the end of the file
        let file_size = self
            .source
            .seek(SeekFrom::End(0))
            .await
            .context(SeekSnafu)?;

        // check the magic
        self.check_magic_async(last_magic_offset(file_size)).await?;

        // read the flags
        let flags = self.read_flags_async(flags_offset(file_size)).await?;

        // read the footer payload size
        let payload_size = self
            .read_payload_size_async(payload_size_offset(file_size))
            .await? as u64;

        // check the magic
        self.check_magic_async(first_magic_offset(file_size, payload_size))
            .await?;

        // read the footer payload
        let payload = self
            .read_payload_async(payload_offset(file_size, payload_size), payload_size, flags)
            .await?;

        // deserialize the footer payload
        let metadata: FileMetadata =
            serde_json::from_slice(payload.as_slice()).context(DeserializeJsonSnafu)?;

        Ok(metadata)
    }

    pub async fn check_magic_async(&mut self, offset: u64) -> Result<()> {
        let mut magic = [0; 4];
        self.source
            .seek(SeekFrom::Start(offset))
            .await
            .context(SeekSnafu)?;
        self.source
            .read_exact(&mut magic)
            .await
            .context(ReadSnafu)?;

        ensure!(magic == MAGIC, MagicNotMatchedSnafu);
        Ok(())
    }

    pub async fn read_flags_async(&mut self, offset: u64) -> Result<Flags> {
        let mut flags = [0; 4];
        self.source
            .seek(SeekFrom::Start(offset))
            .await
            .context(SeekSnafu)?;
        self.source
            .read_exact(&mut flags)
            .await
            .context(ReadSnafu)?;
        let flags = Flags::from_bits_truncate(u32::from_le_bytes(flags));

        Ok(flags)
    }

    pub async fn read_payload_size_async(&mut self, offset: u64) -> Result<i32> {
        let mut payload_size = [0; 4];
        self.source
            .seek(SeekFrom::Start(offset))
            .await
            .context(SeekSnafu)?;
        self.source
            .read_exact(&mut payload_size)
            .await
            .context(ReadSnafu)?;
        let payload_size = i32::from_le_bytes(payload_size);

        Ok(payload_size)
    }

    pub async fn read_payload_async(
        &mut self,
        offset: u64,
        size: u64,
        flags: Flags,
    ) -> Result<Vec<u8>> {
        // TODO(zhongzc): support lz4
        ensure!(
            !flags.contains(Flags::FOOTER_PAYLOAD_COMPRESSED_LZ4),
            UnsupportedDecompressionSnafu {
                decompression: "lz4"
            }
        );

        let mut reader = PartialReader::new(&mut self.source, offset, size);
        let mut payload = vec![];
        reader.read_to_end(&mut payload).await.context(ReadSnafu)?;

        Ok(payload)
    }
}

fn last_magic_offset(file_size: u64) -> u64 {
    file_size - 4
}

fn flags_offset(file_size: u64) -> u64 {
    file_size - 8
}

fn payload_size_offset(file_size: u64) -> u64 {
    file_size - 12
}

fn payload_offset(file_size: u64, payload_size: u64) -> u64 {
    file_size - 12 - payload_size
}

fn first_magic_offset(file_size: u64, payload_size: u64) -> u64 {
    file_size - 12 - payload_size - 4
}
