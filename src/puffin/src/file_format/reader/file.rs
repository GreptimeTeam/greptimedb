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

use async_trait::async_trait;
use common_base::range_read::RangeReader;
use snafu::{ensure, ResultExt};

use crate::blob_metadata::BlobMetadata;
use crate::error::{MagicNotMatchedSnafu, ReadSnafu, Result, UnexpectedPuffinFileSizeSnafu};
use crate::file_format::reader::footer::FooterParser;
use crate::file_format::reader::AsyncReader;
use crate::file_format::{MAGIC, MAGIC_SIZE, MIN_FILE_SIZE};
use crate::file_metadata::FileMetadata;
use crate::partial_reader::PartialReader;

/// Puffin file reader, implemented [`PuffinSyncReader`] and [`PuffinAsyncReader`]
///
/// ```text
/// File layout: Magic Blob₁ Blob₂ ... Blobₙ Footer
///              [4]   [?]   [?]       [?]   [?]
/// ```
pub struct PuffinFileReader<R> {
    /// The source of the puffin file
    source: R,

    /// The metadata of the puffin file, which is parsed from the footer
    metadata: Option<FileMetadata>,
}

impl<R> PuffinFileReader<R> {
    pub fn new(source: R) -> Self {
        Self {
            source,
            metadata: None,
        }
    }

    fn validate_file_size(file_size: u64) -> Result<()> {
        ensure!(
            file_size >= MIN_FILE_SIZE,
            UnexpectedPuffinFileSizeSnafu {
                min_file_size: MIN_FILE_SIZE,
                actual_file_size: file_size
            }
        );
        Ok(())
    }

    /// Converts the reader into an owned blob reader.
    pub fn into_blob_reader(self, blob_metadata: &BlobMetadata) -> PartialReader<R> {
        PartialReader::new(
            self.source,
            blob_metadata.offset as _,
            blob_metadata.length as _,
        )
    }
}

#[async_trait]
impl<'a, R: RangeReader + 'a> AsyncReader<'a> for PuffinFileReader<R> {
    type Reader = PartialReader<&'a mut R>;

    async fn metadata(&'a mut self) -> Result<FileMetadata> {
        if let Some(metadata) = &self.metadata {
            return Ok(metadata.clone());
        }

        // check the magic
        let magic = self.source.read(0..MAGIC_SIZE).await.context(ReadSnafu)?;
        ensure!(*magic == MAGIC, MagicNotMatchedSnafu);

        let file_size = self.get_file_size_async().await?;

        // parse the footer
        let metadata = FooterParser::new(&mut self.source, file_size)
            .parse_async()
            .await?;
        self.metadata = Some(metadata.clone());
        Ok(metadata)
    }

    fn blob_reader(&'a mut self, blob_metadata: &BlobMetadata) -> Result<Self::Reader> {
        Ok(PartialReader::new(
            &mut self.source,
            blob_metadata.offset as _,
            blob_metadata.length as _,
        ))
    }
}

impl<R: RangeReader> PuffinFileReader<R> {
    async fn get_file_size_async(&mut self) -> Result<u64> {
        let file_size = self
            .source
            .metadata()
            .await
            .context(ReadSnafu)?
            .content_length;
        Self::validate_file_size(file_size)?;
        Ok(file_size)
    }
}
