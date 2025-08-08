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

use object_store::ObjectStore;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::FOOTER_SIZE;
use snafu::ResultExt;

use crate::error::{self, Result};

/// The estimated size of the footer and metadata need to read from the end of parquet file.
const DEFAULT_PREFETCH_SIZE: u64 = 64 * 1024;

/// Load the metadata of parquet file in an async way.
pub(crate) struct MetadataLoader<'a> {
    // An object store that supports async read
    object_store: ObjectStore,
    // The path of parquet file
    file_path: &'a str,
    // The size of parquet file
    file_size: u64,
}

impl<'a> MetadataLoader<'a> {
    /// Create a new parquet metadata loader.
    pub fn new(
        object_store: ObjectStore,
        file_path: &'a str,
        file_size: u64,
    ) -> MetadataLoader<'a> {
        Self {
            object_store,
            file_path,
            file_size,
        }
    }

    /// Async load the metadata of parquet file.
    ///
    /// Read [DEFAULT_PREFETCH_SIZE] from the end of parquet file at first, if File Metadata is in the
    /// read range, decode it and return [ParquetMetaData], otherwise, read again to get the rest of the metadata.
    ///
    /// Parquet File Format:
    /// ```text
    /// ┌───────────────────────────────────┐
    /// |4-byte magic number "PAR1"         |
    /// |───────────────────────────────────|
    /// |Column 1 Chunk 1 + Column Metadata |
    /// |Column 2 Chunk 1 + Column Metadata |
    /// |...                                |
    /// |Column N Chunk M + Column Metadata |
    /// |───────────────────────────────────|
    /// |File Metadata                      |
    /// |───────────────────────────────────|
    /// |4-byte length of file metadata     |
    /// |4-byte magic number "PAR1"         |
    /// └───────────────────────────────────┘
    /// ```
    ///
    /// Refer to https://github.com/apache/arrow-rs/blob/093a10e46203be1a0e94ae117854701bf58d4c79/parquet/src/arrow/async_reader/metadata.rs#L55-L106
    pub async fn load(&self) -> Result<ParquetMetaData> {
        let object_store = &self.object_store;
        let path = self.file_path;
        let file_size = self.get_file_size().await?;

        if file_size < FOOTER_SIZE as u64 {
            return error::InvalidParquetSnafu {
                file: path,
                reason: "file size is smaller than footer size",
            }
            .fail();
        }

        // Prefetch bytes for metadata from the end and process the footer
        let buffer_start = file_size.saturating_sub(DEFAULT_PREFETCH_SIZE);
        let buffer = object_store
            .read_with(path)
            .range(buffer_start..file_size)
            .await
            .context(error::OpenDalSnafu)?
            .to_vec();
        let buffer_len = buffer.len();

        let mut footer = [0; 8];
        footer.copy_from_slice(&buffer[buffer_len - FOOTER_SIZE..]);

        let footer_tail = ParquetMetaDataReader::decode_footer_tail(&footer).map_err(|e| {
            error::InvalidParquetSnafu {
                file: path,
                reason: format!("failed to decode footer, {e}"),
            }
            .build()
        })?;
        let metadata_len = footer_tail.metadata_length() as u64;

        if file_size - (FOOTER_SIZE as u64) < metadata_len {
            return error::InvalidParquetSnafu {
                file: path,
                reason: format!(
                    "the sum of Metadata length {} and Footer size {} is larger than file size {}",
                    metadata_len, FOOTER_SIZE, file_size
                ),
            }
            .fail();
        }

        if (metadata_len as usize) <= buffer_len - FOOTER_SIZE {
            // The whole metadata is in the first read
            let metadata_start = buffer_len - metadata_len as usize - FOOTER_SIZE;
            let metadata = ParquetMetaDataReader::decode_metadata(
                &buffer[metadata_start..buffer_len - FOOTER_SIZE],
            )
            .map_err(|e| {
                error::InvalidParquetSnafu {
                    file: path,
                    reason: format!("failed to decode metadata, {e}"),
                }
                .build()
            })?;
            Ok(metadata)
        } else {
            // The metadata is out of buffer, need to make a second read
            let metadata_start = file_size - metadata_len - FOOTER_SIZE as u64;
            let data = object_store
                .read_with(path)
                .range(metadata_start..(file_size - FOOTER_SIZE as u64))
                .await
                .context(error::OpenDalSnafu)?
                .to_vec();

            let metadata = ParquetMetaDataReader::decode_metadata(&data).map_err(|e| {
                error::InvalidParquetSnafu {
                    file: path,
                    reason: format!("failed to decode metadata, {e}"),
                }
                .build()
            })?;
            Ok(metadata)
        }
    }

    /// Get the size of parquet file.
    async fn get_file_size(&self) -> Result<u64> {
        let file_size = match self.file_size {
            0 => self
                .object_store
                .stat(self.file_path)
                .await
                .context(error::OpenDalSnafu)?
                .content_length(),
            other => other,
        };
        Ok(file_size)
    }
}
