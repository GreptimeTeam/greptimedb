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

use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use crate::blob_metadata::BlobMetadata;

/// Metadata of a Puffin file
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Builder)]
pub struct FileMetadata {
    /// Metadata for each blob in the file
    #[builder(default)]
    pub blobs: Vec<BlobMetadata>,

    /// Storage for arbitrary meta-information, like writer identification/version
    #[builder(default)]
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::blob_metadata::BlobMetadataBuilder;

    #[test]
    fn test_file_metadata_builder() {
        let mut properties = HashMap::new();
        properties.insert(String::from("key1"), String::from("value1"));

        let blob_metadata = BlobMetadataBuilder::default()
            .blob_type("type1".to_string())
            .offset(10)
            .length(30)
            .build()
            .unwrap();

        let metadata = FileMetadataBuilder::default()
            .blobs(vec![blob_metadata.clone()])
            .properties(properties.clone())
            .build()
            .unwrap();

        assert_eq!(properties, metadata.properties);
        assert_eq!(vec![blob_metadata], metadata.blobs);
    }

    #[test]
    fn test_file_metadata_serialization() {
        let mut properties = HashMap::new();
        properties.insert(String::from("key1"), String::from("value1"));

        let blob_metadata = BlobMetadataBuilder::default()
            .blob_type("type1".to_string())
            .offset(10)
            .length(30)
            .build()
            .unwrap();

        let metadata = FileMetadataBuilder::default()
            .blobs(vec![blob_metadata.clone()])
            .properties(properties.clone())
            .build()
            .unwrap();

        let serialized = serde_json::to_string(&metadata).unwrap();
        assert_eq!(
            serialized,
            r#"{"blobs":[{"type":"type1","fields":[],"snapshot-id":0,"sequence-number":0,"offset":10,"length":30}],"properties":{"key1":"value1"}}"#
        );
    }

    #[test]
    fn test_file_metadata_deserialization() {
        let data = r#"{"blobs":[{"type":"type1","fields":[],"snapshot-id":0,"sequence-number":0,"offset":10,"length":30}],"properties":{"key1":"value1"}}"#;
        let deserialized: FileMetadata = serde_json::from_str(data).unwrap();

        assert_eq!(deserialized.blobs[0].blob_type, "type1");
        assert_eq!(deserialized.blobs[0].offset, 10);
        assert_eq!(deserialized.blobs[0].length, 30);
        assert_eq!(deserialized.properties.get("key1").unwrap(), "value1");
    }

    #[test]
    fn test_empty_properties_not_serialized() {
        let blob_metadata = BlobMetadataBuilder::default()
            .blob_type("type1".to_string())
            .offset(10)
            .length(30)
            .build()
            .unwrap();

        let metadata = FileMetadataBuilder::default()
            .blobs(vec![blob_metadata.clone()])
            .build()
            .unwrap();

        let serialized = serde_json::to_string(&metadata).unwrap();
        assert_eq!(
            serialized,
            r#"{"blobs":[{"type":"type1","fields":[],"snapshot-id":0,"sequence-number":0,"offset":10,"length":30}]}"#
        );
    }

    #[test]
    fn test_empty_blobs_serialization() {
        let metadata = FileMetadataBuilder::default()
            .blobs(vec![])
            .build()
            .unwrap();

        let serialized = serde_json::to_string(&metadata).unwrap();
        assert_eq!(serialized, r#"{"blobs":[]}"#);
    }

    #[test]
    fn test_missing_blobs_deserialization() {
        let data = r#"{"properties":{"key1":"value1"}}"#;
        let deserialized = serde_json::from_str::<FileMetadata>(data);

        assert!(deserialized
            .unwrap_err()
            .to_string()
            .contains("missing field `blobs`"));
    }
}
