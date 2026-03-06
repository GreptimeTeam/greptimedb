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

use common_time::util::current_time_millis;
use flexbuffers::{FlexbufferSerializer, Reader};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{
    DeserializeFlexbuffersSnafu, ReadFlexbuffersSnafu, Result, SerializeFlexbuffersSnafu,
};
use crate::rpc::KeyValue;
use crate::snapshot::FileFormat;

/// The layout of the backup file.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Document {
    metadata: Metadata,
    content: Content,
}

impl Document {
    /// Creates a new document.
    pub fn new(metadata: Metadata, content: Content) -> Self {
        Self { metadata, content }
    }

    fn serialize_to_flexbuffer(&self) -> Result<Vec<u8>> {
        let mut builder = FlexbufferSerializer::new();
        self.serialize(&mut builder)
            .context(SerializeFlexbuffersSnafu)?;
        Ok(builder.take_buffer())
    }

    /// Converts the [`Document`] to a bytes.
    pub(crate) fn to_bytes(&self, format: &FileFormat) -> Result<Vec<u8>> {
        match format {
            FileFormat::FlexBuffers => self.serialize_to_flexbuffer(),
        }
    }

    fn deserialize_from_flexbuffer(data: &[u8]) -> Result<Self> {
        let reader = Reader::get_root(data).context(ReadFlexbuffersSnafu)?;
        Document::deserialize(reader).context(DeserializeFlexbuffersSnafu)
    }

    /// Deserializes the [`Document`] from a bytes.
    pub(crate) fn from_slice(format: &FileFormat, data: &[u8]) -> Result<Self> {
        match format {
            FileFormat::FlexBuffers => Self::deserialize_from_flexbuffer(data),
        }
    }

    /// Converts the [`Document`] to a [`MetadataContent`].
    pub(crate) fn into_metadata_content(self) -> Result<MetadataContent> {
        match self.content {
            Content::Metadata(metadata) => Ok(metadata),
        }
    }
}

/// The metadata of the backup file.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Metadata {
    // UNIX_EPOCH in milliseconds.
    created_timestamp_mills: i64,
}

impl Metadata {
    /// Create a new metadata.
    ///
    /// The `created_timestamp_mills` will be the current time in milliseconds.
    pub fn new() -> Self {
        Self {
            created_timestamp_mills: current_time_millis(),
        }
    }
}

/// The content of the backup file.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum Content {
    Metadata(MetadataContent),
}

/// The content of the backup file.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct MetadataContent {
    values: Vec<KeyValue>,
}

impl MetadataContent {
    /// Create a new metadata content.
    pub fn new(values: impl IntoIterator<Item = KeyValue>) -> Self {
        Self {
            values: values.into_iter().collect(),
        }
    }

    /// Returns an iterator over the key-value pairs.
    pub fn into_iter(self) -> impl Iterator<Item = KeyValue> {
        self.values.into_iter()
    }

    /// Returns the key-value pairs as a vector.
    pub fn values(self) -> Vec<KeyValue> {
        self.values
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_document() {
        let document = Document::new(
            Metadata::new(),
            Content::Metadata(MetadataContent::new(vec![KeyValue {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            }])),
        );

        let bytes = document.to_bytes(&FileFormat::FlexBuffers).unwrap();
        let document_deserialized = Document::from_slice(&FileFormat::FlexBuffers, &bytes).unwrap();
        assert_eq!(
            document.metadata.created_timestamp_mills,
            document_deserialized.metadata.created_timestamp_mills
        );
        assert_eq!(document.content, document_deserialized.content);
    }
}
