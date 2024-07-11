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

use std::collections::HashMap;
use std::{io, mem};

use async_trait::async_trait;
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use snafu::ResultExt;

use crate::blob_metadata::{BlobMetadata, BlobMetadataBuilder, CompressionCodec};
use crate::error::{CloseSnafu, FlushSnafu, Result, WriteSnafu};
use crate::file_format::writer::footer::FooterWriter;
use crate::file_format::writer::{AsyncWriter, Blob, SyncWriter};
use crate::file_format::MAGIC;

/// Puffin file writer, implements both [`PuffinSyncWriter`] and [`PuffinAsyncWriter`]
pub struct PuffinFileWriter<W> {
    /// The writer to write to.
    writer: W,

    /// The properties of the file.
    properties: HashMap<String, String>,

    /// The metadata of the blobs.
    blob_metadata: Vec<BlobMetadata>,

    /// The number of bytes written.
    written_bytes: u64,

    /// Whether the footer payload should be LZ4 compressed.
    footer_lz4_compressed: bool,
}

impl<W> PuffinFileWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            properties: HashMap::new(),
            blob_metadata: Vec::new(),
            written_bytes: 0,
            footer_lz4_compressed: false,
        }
    }

    fn create_blob_metadata(
        &self,
        typ: String,
        compression_codec: Option<CompressionCodec>,
        properties: HashMap<String, String>,
        size: u64,
    ) -> BlobMetadata {
        BlobMetadataBuilder::default()
            .blob_type(typ)
            .properties(properties)
            .compression_codec(compression_codec)
            .offset(self.written_bytes as _)
            .length(size as _)
            .build()
            .expect("Required fields are not set")
    }
}

impl<W: io::Write> SyncWriter for PuffinFileWriter<W> {
    fn set_properties(&mut self, properties: HashMap<String, String>) {
        self.properties = properties;
    }

    fn add_blob<R: io::Read>(&mut self, mut blob: Blob<R>) -> Result<u64> {
        self.write_header_if_needed_sync()?;

        let size = io::copy(&mut blob.compressed_data, &mut self.writer).context(WriteSnafu)?;

        let blob_metadata = self.create_blob_metadata(
            blob.blob_type,
            blob.compression_codec,
            blob.properties,
            size,
        );
        self.blob_metadata.push(blob_metadata);

        self.written_bytes += size;
        Ok(size)
    }

    fn set_footer_lz4_compressed(&mut self, lz4_compressed: bool) {
        self.footer_lz4_compressed = lz4_compressed;
    }

    fn finish(&mut self) -> Result<u64> {
        self.write_header_if_needed_sync()?;
        self.write_footer_sync()?;
        self.writer.flush().context(FlushSnafu)?;

        Ok(self.written_bytes)
    }
}

#[async_trait]
impl<W: AsyncWrite + Unpin + Send> AsyncWriter for PuffinFileWriter<W> {
    fn set_properties(&mut self, properties: HashMap<String, String>) {
        self.properties = properties;
    }

    async fn add_blob<R: AsyncRead + Send>(&mut self, blob: Blob<R>) -> Result<u64> {
        self.write_header_if_needed_async().await?;

        let size = futures::io::copy(blob.compressed_data, &mut self.writer)
            .await
            .context(WriteSnafu)?;

        let blob_metadata = self.create_blob_metadata(
            blob.blob_type,
            blob.compression_codec,
            blob.properties,
            size,
        );
        self.blob_metadata.push(blob_metadata);

        self.written_bytes += size;
        Ok(size)
    }

    fn set_footer_lz4_compressed(&mut self, lz4_compressed: bool) {
        self.footer_lz4_compressed = lz4_compressed;
    }

    async fn finish(&mut self) -> Result<u64> {
        self.write_header_if_needed_async().await?;
        self.write_footer_async().await?;
        self.writer.flush().await.context(FlushSnafu)?;
        self.writer.close().await.context(CloseSnafu)?;

        Ok(self.written_bytes)
    }
}

impl<W: io::Write> PuffinFileWriter<W> {
    fn write_header_if_needed_sync(&mut self) -> Result<()> {
        if self.written_bytes == 0 {
            self.writer.write_all(&MAGIC).context(WriteSnafu)?;
            self.written_bytes += MAGIC.len() as u64;
        }
        Ok(())
    }

    fn write_footer_sync(&mut self) -> Result<()> {
        let bytes = FooterWriter::new(
            mem::take(&mut self.blob_metadata),
            mem::take(&mut self.properties),
            self.footer_lz4_compressed,
        )
        .into_footer_bytes()?;

        self.writer.write_all(&bytes).context(WriteSnafu)?;
        self.written_bytes += bytes.len() as u64;
        Ok(())
    }
}

impl<W: AsyncWrite + Unpin> PuffinFileWriter<W> {
    async fn write_header_if_needed_async(&mut self) -> Result<()> {
        if self.written_bytes == 0 {
            self.writer.write_all(&MAGIC).await.context(WriteSnafu)?;
            self.written_bytes += MAGIC.len() as u64;
        }
        Ok(())
    }

    async fn write_footer_async(&mut self) -> Result<()> {
        let bytes = FooterWriter::new(
            mem::take(&mut self.blob_metadata),
            mem::take(&mut self.properties),
            self.footer_lz4_compressed,
        )
        .into_footer_bytes()?;

        self.writer.write_all(&bytes).await.context(WriteSnafu)?;
        self.written_bytes += bytes.len() as u64;
        Ok(())
    }
}
