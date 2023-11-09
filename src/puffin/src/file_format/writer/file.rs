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

use crate::blob_metadata::{BlobMetadata, BlobMetadataBuilder};
use crate::error::{Result, SerializeJsonSnafu, WriteSnafu};
use crate::file_format::writer::{Blob, PuffinAsyncWriter, PuffinSyncWriter};
use crate::file_format::MAGIC;
use crate::file_metadata::FileMetadataBuilder;

pub struct PuffinWriter<W> {
    /// The writer to write to
    writer: W,

    /// The properties of the file
    properties: HashMap<String, String>,

    /// The metadata of the blobs
    blob_metadata: Vec<BlobMetadata>,

    /// The offset of the next blob
    next_blob_offset: u64,
}

impl<W> PuffinWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            properties: HashMap::new(),
            blob_metadata: Vec::new(),
            next_blob_offset: 0,
        }
    }
}

impl<W: io::Write> PuffinSyncWriter for PuffinWriter<W> {
    fn set_properties(&mut self, properties: HashMap<String, String>) {
        self.properties = properties;
    }

    fn add_blob<R: io::Read>(&mut self, blob: Blob<R>) -> Result<()> {
        self.write_header_magic_if_needed_sync()?;
        let size = self.write_blob_sync(blob.data)?;
        let blob_metadata = BlobMetadataBuilder::default()
            .blob_type(blob.blob_type)
            .properties(blob.properties)
            .offset(self.next_blob_offset as _)
            .length(size as _)
            .build()
            .expect("Required fields are not set");

        self.next_blob_offset += size;
        self.blob_metadata.push(blob_metadata);
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.write_header_magic_if_needed_sync()?;
        self.write_footer_sync()
    }
}

#[async_trait]
impl<W: AsyncWrite + Unpin + Send> PuffinAsyncWriter for PuffinWriter<W> {
    fn set_properties(&mut self, properties: HashMap<String, String>) {
        self.properties = properties;
    }

    async fn add_blob<R: AsyncRead + Send>(&mut self, blob: Blob<R>) -> Result<()> {
        self.write_header_magic_if_needed_async().await?;
        let size = self.write_blob_async(blob.data).await?;
        let blob_metadata = BlobMetadataBuilder::default()
            .blob_type(blob.blob_type)
            .properties(blob.properties)
            .offset(self.next_blob_offset as _)
            .length(size as _)
            .build()
            .expect("Required fields are not set");

        self.next_blob_offset += size;
        self.blob_metadata.push(blob_metadata);
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        self.write_header_magic_if_needed_async().await?;
        self.write_footer_async().await
    }
}

impl<W: io::Write> PuffinWriter<W> {
    fn write_header_magic_if_needed_sync(&mut self) -> Result<()> {
        if self.next_blob_offset == 0 {
            self.writer.write_all(&MAGIC).context(WriteSnafu)?;
            self.next_blob_offset += MAGIC.len() as u64;
        }
        Ok(())
    }

    fn write_magic_sync(&mut self) -> Result<()> {
        self.writer.write_all(&MAGIC).context(WriteSnafu)
    }

    fn write_blob_sync<R: io::Read>(&mut self, mut blob_data: R) -> Result<u64> {
        io::copy(&mut blob_data, &mut self.writer).context(WriteSnafu)
    }

    // Magic FooterPayload FooterPayloadSize Flags Magic
    fn write_footer_sync(&mut self) -> Result<()> {
        // Magic
        self.write_magic_sync()?;

        // FooterPayload
        let size = self.write_footer_payload_sync()?;

        // FooterPayloadSize
        self.write_footer_payload_size_sync(size as _)?;

        // Flags
        self.write_flags_sync()?;

        // Magic
        self.write_magic_sync()?;

        Ok(())
    }

    fn write_footer_payload_sync(&mut self) -> Result<u64> {
        let file_metadata = FileMetadataBuilder::default()
            .blob_metadata(mem::take(&mut self.blob_metadata))
            .properties(mem::take(&mut self.properties))
            .build()
            .expect("Required fields are not set");

        let json_data = serde_json::to_vec(&file_metadata).context(SerializeJsonSnafu)?;
        self.writer.write_all(&json_data).context(WriteSnafu)?;
        Ok(json_data.len() as u64)
    }

    fn write_footer_payload_size_sync(&mut self, size: i32) -> Result<()> {
        self.writer
            .write_all(&size.to_le_bytes())
            .context(WriteSnafu)
    }

    fn write_flags_sync(&mut self) -> Result<()> {
        self.writer.write_all(&[0; 4]).context(WriteSnafu)
    }
}

impl<W: AsyncWrite + Unpin> PuffinWriter<W> {
    async fn write_header_magic_if_needed_async(&mut self) -> Result<()> {
        if self.next_blob_offset == 0 {
            self.writer.write_all(&MAGIC).await.context(WriteSnafu)?;
            self.next_blob_offset += MAGIC.len() as u64;
        }
        Ok(())
    }

    async fn write_magic_async(&mut self) -> Result<()> {
        self.writer.write_all(&MAGIC).await.context(WriteSnafu)
    }

    async fn write_blob_async<R: AsyncRead>(&mut self, blob_data: R) -> Result<u64> {
        futures::io::copy(blob_data, &mut self.writer)
            .await
            .context(WriteSnafu)
    }

    // Magic FooterPayload FooterPayloadSize Flags Magic
    async fn write_footer_async(&mut self) -> Result<()> {
        // Magic
        self.write_magic_async().await?;

        // FooterPayload
        let size = self.write_footer_payload_async().await?;

        // FooterPayloadSize
        self.write_footer_payload_size_async(size as _).await?;

        // Flags
        self.write_flags_async().await?;

        // Magic
        self.write_magic_async().await?;

        Ok(())
    }

    async fn write_footer_payload_async(&mut self) -> Result<u64> {
        let file_metadata = FileMetadataBuilder::default()
            .blob_metadata(mem::take(&mut self.blob_metadata))
            .properties(mem::take(&mut self.properties))
            .build()
            .expect("Required fields are not set");

        let json_data = serde_json::to_vec(&file_metadata).context(SerializeJsonSnafu)?;
        self.writer
            .write_all(&json_data)
            .await
            .context(WriteSnafu)?;
        Ok(json_data.len() as u64)
    }

    async fn write_footer_payload_size_async(&mut self, size: i32) -> Result<()> {
        self.writer
            .write_all(&size.to_le_bytes())
            .await
            .context(WriteSnafu)
    }

    async fn write_flags_async(&mut self) -> Result<()> {
        self.writer.write_all(&[0; 4]).await.context(WriteSnafu)
    }
}
