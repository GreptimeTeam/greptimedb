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

use core::fmt;
use std::collections::HashMap;

use derive_builder::Builder;
use serde::{Deserialize, Serialize};

/// Blob metadata of Puffin
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Builder)]
#[serde(rename_all = "kebab-case")]
pub struct BlobMetadata {
    /// Blob type
    #[serde(rename = "type")]
    pub blob_type: String,

    /// For Iceberg, it' list of field IDs the blob was computed for;
    /// the order of items is used to compute sketches stored in the blob.
    ///
    /// For usage outside the context of Iceberg, it can be ignored.
    #[builder(default)]
    #[serde(default)]
    #[serde(rename = "fields")]
    pub input_fields: Vec<i32>,

    /// For Iceberg, it's ID of the Iceberg table’s snapshot the blob was computed from.
    ///
    /// For usage outside the context of Iceberg, it can be ignored.
    #[builder(default)]
    #[serde(default)]
    pub snapshot_id: i64,

    /// For Iceberg, it's sequence number of the Iceberg table’s snapshot the blob was computed from.
    ///
    /// For usage outside the context of Iceberg, it can be ignored.
    #[builder(default)]
    #[serde(default)]
    pub sequence_number: i64,

    /// The offset in the file where the blob contents start
    pub offset: i64,

    /// The length of the blob stored in the file (after compression, if compressed)
    pub length: i64,

    /// See [`CompressionCodec`]. If omitted, the data is assumed to be uncompressed.
    #[builder(default, setter(strip_option))]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_codec: Option<CompressionCodec>,

    /// Storage for arbitrary meta-information about the blob
    #[builder(default)]
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
}

/// Compression codec used to compress the blob
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    /// Single [LZ4 compression frame](https://github.com/lz4/lz4/blob/77d1b93f72628af7bbde0243b4bba9205c3138d9/doc/lz4_Frame_format.md),
    /// with content size present
    Lz4,

    /// Single [Zstandard compression frame](https://github.com/facebook/zstd/blob/8af64f41161f6c2e0ba842006fe238c664a6a437/doc/zstd_compression_format.md#zstandard-frames),
    /// with content size present
    Zstd,
}

impl fmt::Display for CompressionCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionCodec::Lz4 => write!(f, "lz4"),
            CompressionCodec::Zstd => write!(f, "zstd"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_metadata_builder() {
        let mut properties = HashMap::new();
        properties.insert("property1".to_string(), "value1".to_string());
        properties.insert("property2".to_string(), "value2".to_string());

        let blob_metadata = BlobMetadataBuilder::default()
            .blob_type("type1".to_string())
            .input_fields(vec![1, 2, 3])
            .snapshot_id(100)
            .sequence_number(200)
            .offset(300)
            .length(400)
            .compression_codec(CompressionCodec::Lz4)
            .properties(properties)
            .build()
            .unwrap();

        assert_eq!("type1", blob_metadata.blob_type);
        assert_eq!(vec![1, 2, 3], blob_metadata.input_fields);
        assert_eq!(100, blob_metadata.snapshot_id);
        assert_eq!(200, blob_metadata.sequence_number);
        assert_eq!(300, blob_metadata.offset);
        assert_eq!(400, blob_metadata.length);
        assert_eq!(Some(CompressionCodec::Lz4), blob_metadata.compression_codec);
        assert_eq!(
            "value1",
            blob_metadata.properties.get("property1").unwrap().as_str()
        );
        assert_eq!(
            "value2",
            blob_metadata.properties.get("property2").unwrap().as_str()
        );
    }

    #[test]
    fn test_blob_metadata_minimal_builder() {
        let blob_metadata = BlobMetadataBuilder::default()
            .blob_type("type1".to_string())
            .offset(300)
            .length(400)
            .build()
            .unwrap();

        assert_eq!("type1", blob_metadata.blob_type);
        assert_eq!(300, blob_metadata.offset);
        assert_eq!(400, blob_metadata.length);
        assert_eq!(None, blob_metadata.compression_codec);
        assert_eq!(0, blob_metadata.properties.len());
    }

    #[test]
    fn test_blob_metadata_missing_field() {
        let blob_metadata = BlobMetadataBuilder::default()
            .blob_type("type1".to_string())
            .offset(300)
            .build();
        assert_eq!(
            blob_metadata.unwrap_err().to_string(),
            "`length` must be initialized"
        );

        let blob_metadata = BlobMetadataBuilder::default()
            .blob_type("type1".to_string())
            .length(400)
            .build();
        assert_eq!(
            blob_metadata.unwrap_err().to_string(),
            "`offset` must be initialized"
        );

        let blob_metadata = BlobMetadataBuilder::default()
            .offset(300)
            .length(400)
            .build();
        assert_eq!(
            blob_metadata.unwrap_err().to_string(),
            "`blob_type` must be initialized"
        );
    }

    #[test]
    fn test_serialize_deserialize_blob_metadata_with_properties() {
        let mut properties = HashMap::new();
        properties.insert(String::from("key1"), String::from("value1"));
        properties.insert(String::from("key2"), String::from("value2"));

        let metadata = BlobMetadata {
            blob_type: String::from("test"),
            input_fields: vec![1, 2, 3],
            snapshot_id: 12345,
            sequence_number: 67890,
            offset: 100,
            length: 200,
            compression_codec: Some(CompressionCodec::Lz4),
            properties: properties.clone(),
        };

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: BlobMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(metadata, deserialized);
        assert_eq!(properties, deserialized.properties);
    }

    #[test]
    fn test_serialize_deserialize_blob_metadata_without_compression_codec() {
        let metadata = BlobMetadata {
            blob_type: String::from("test"),
            input_fields: vec![1, 2, 3],
            snapshot_id: 12345,
            sequence_number: 67890,
            offset: 100,
            length: 200,
            compression_codec: None,
            properties: HashMap::new(),
        };

        let expected_json = r#"{"type":"test","fields":[1,2,3],"snapshot-id":12345,"sequence-number":67890,"offset":100,"length":200}"#;

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: BlobMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(expected_json, json);
        assert_eq!(metadata, deserialized);
    }

    #[test]
    fn test_deserialize_blob_metadata_with_properties() {
        let json = r#"{
            "type": "test",
            "fields": [1, 2, 3],
            "snapshot-id": 12345,
            "sequence-number": 67890,
            "offset": 100,
            "length": 200,
            "compression-codec": "lz4",
            "properties": {
                "key1": "value1",
                "key2": "value2"
            }
        }"#;

        let mut expected_properties = HashMap::new();
        expected_properties.insert(String::from("key1"), String::from("value1"));
        expected_properties.insert(String::from("key2"), String::from("value2"));

        let expected = BlobMetadata {
            blob_type: String::from("test"),
            input_fields: vec![1, 2, 3],
            snapshot_id: 12345,
            sequence_number: 67890,
            offset: 100,
            length: 200,
            compression_codec: Some(CompressionCodec::Lz4),
            properties: expected_properties.clone(),
        };

        let deserialized: BlobMetadata = serde_json::from_str(json).unwrap();

        assert_eq!(expected, deserialized);
        assert_eq!(expected_properties, deserialized.properties);
    }

    #[test]
    fn test_deserialize_blob_metadata_without_properties() {
        let json = r#"{
            "type": "test",
            "fields": [1, 2, 3],
            "snapshot-id": 12345,
            "sequence-number": 67890,
            "offset": 100,
            "length": 200,
            "compression-codec": "lz4"
        }"#;

        let expected = BlobMetadata {
            blob_type: String::from("test"),
            input_fields: vec![1, 2, 3],
            snapshot_id: 12345,
            sequence_number: 67890,
            offset: 100,
            length: 200,
            compression_codec: Some(CompressionCodec::Lz4),
            properties: HashMap::new(),
        };

        let deserialized: BlobMetadata = serde_json::from_str(json).unwrap();

        assert_eq!(expected, deserialized);
    }

    #[test]
    fn test_deserialize_blob_metadata_with_empty_properties() {
        let json = r#"{
            "type": "test",
            "fields": [1, 2, 3],
            "snapshot-id": 12345,
            "sequence-number": 67890,
            "offset": 100,
            "length": 200,
            "compression-codec": "lz4",
            "properties": {}
        }"#;

        let expected_properties = HashMap::new();
        let expected = BlobMetadata {
            blob_type: String::from("test"),
            input_fields: vec![1, 2, 3],
            snapshot_id: 12345,
            sequence_number: 67890,
            offset: 100,
            length: 200,
            compression_codec: Some(CompressionCodec::Lz4),
            properties: expected_properties.clone(),
        };

        let deserialized: BlobMetadata = serde_json::from_str(json).unwrap();

        assert_eq!(expected, deserialized);
        assert_eq!(expected_properties, deserialized.properties);
    }

    #[test]
    fn test_deserialize_invalid_blob_metadata() {
        let invalid_json = r#"{
            "type": "test",
            "input-fields": [1, 2, 3],
            "snapshot-id": "12345",
            "sequence-number": 67890,
            "offset": 100,
            "length": 200,
            "compression-codec": "Invalid",
            "properties": {}
        }"#;

        assert!(serde_json::from_str::<BlobMetadata>(invalid_json).is_err());
    }
}
