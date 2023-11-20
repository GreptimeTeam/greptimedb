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
use std::mem;

use snafu::ResultExt;

use crate::blob_metadata::BlobMetadata;
use crate::error::{Result, SerializeJsonSnafu};
use crate::file_format::MAGIC;
use crate::file_metadata::FileMetadataBuilder;

/// Writer for the footer of a Puffin file.
///
/// ```text
/// Footer layout: HeadMagic Payload PayloadSize Flags FootMagic
///                [4]       [?]     [4]         [4]   [4]
/// ```
pub struct FooterWriter {
    blob_metadata: Vec<BlobMetadata>,
    file_properties: HashMap<String, String>,
}

impl FooterWriter {
    pub fn new(blob_metadata: Vec<BlobMetadata>, file_properties: HashMap<String, String>) -> Self {
        Self {
            blob_metadata,
            file_properties,
        }
    }

    /// Serializes the footer to bytes
    pub fn into_footer_bytes(mut self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.write_magic(&mut buf); // HeadMagic
        let payload_size = self.write_footer_payload(&mut buf)?; // Payload
        self.write_footer_payload_size(payload_size, &mut buf); // PayloadSize
        self.write_flags(&mut buf); // Flags
        self.write_magic(&mut buf); // FootMagic
        Ok(buf)
    }

    fn write_magic(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&MAGIC);
    }

    fn write_footer_payload(&mut self, buf: &mut Vec<u8>) -> Result<i32> {
        let file_metadata = FileMetadataBuilder::default()
            .blobs(mem::take(&mut self.blob_metadata))
            .properties(mem::take(&mut self.file_properties))
            .build()
            .expect("Required fields are not set");

        let json_data = serde_json::to_vec(&file_metadata).context(SerializeJsonSnafu)?;
        buf.extend_from_slice(&json_data);
        Ok(json_data.len() as i32)
    }

    fn write_footer_payload_size(&self, payload_size: i32, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&payload_size.to_le_bytes());
    }

    /// Appends reserved flags (currently zero-initialized) to the given buffer.
    ///
    /// TODO(zhongzc): support compression
    fn write_flags(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&[0; 4]);
    }
}
