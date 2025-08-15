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

//! Defines [RegionMetaAction] related structs and [RegionCheckpoint].

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{RegionId, SequenceNumber};
use store_api::ManifestVersion;
use strum::Display;

use crate::error::{RegionMetadataNotFoundSnafu, Result, SerdeJsonSnafu, Utf8Snafu};
use crate::sst::file::{FileId, FileMeta};
use crate::wal::EntryId;

/// Actions that can be applied to region manifest.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Display)]
pub enum RegionMetaAction {
    /// Change region's metadata for request like ALTER
    Change(RegionChange),
    /// Edit region's state for changing options or file list.
    Edit(RegionEdit),
    /// Remove the region.
    Remove(RegionRemove),
    /// Truncate the region.
    Truncate(RegionTruncate),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionChange {
    /// The metadata after changed.
    pub metadata: RegionMetadataRef,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionEdit {
    pub files_to_add: Vec<FileMeta>,
    pub files_to_remove: Vec<FileMeta>,
    #[serde(with = "humantime_serde")]
    pub compaction_time_window: Option<Duration>,
    pub flushed_entry_id: Option<EntryId>,
    pub flushed_sequence: Option<SequenceNumber>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionRemove {
    pub region_id: RegionId,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionTruncate {
    pub region_id: RegionId,
    #[serde(flatten)]
    pub kind: TruncateKind,
}

/// The kind of truncate operation.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum TruncateKind {
    /// Truncate all data in the region, marked by all data before the given entry id&sequence.
    All {
        /// Last WAL entry id of truncated data.
        truncated_entry_id: EntryId,
        // Last sequence number of truncated data.
        truncated_sequence: SequenceNumber,
    },
    /// Only remove certain files in the region.
    Partial { files_to_remove: Vec<FileMeta> },
}

/// The region manifest data.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionManifest {
    /// Metadata of the region.
    pub metadata: RegionMetadataRef,
    /// SST files.
    pub files: HashMap<FileId, FileMeta>,
    /// Last WAL entry id of flushed data.
    pub flushed_entry_id: EntryId,
    /// Last sequence of flushed data.
    pub flushed_sequence: SequenceNumber,
    /// Current manifest version.
    pub manifest_version: ManifestVersion,
    /// Last WAL entry id of truncated data.
    pub truncated_entry_id: Option<EntryId>,
    /// Inferred compaction time window.
    #[serde(with = "humantime_serde")]
    pub compaction_time_window: Option<Duration>,
}

#[derive(Debug, Default)]
pub struct RegionManifestBuilder {
    metadata: Option<RegionMetadataRef>,
    files: HashMap<FileId, FileMeta>,
    flushed_entry_id: EntryId,
    flushed_sequence: SequenceNumber,
    manifest_version: ManifestVersion,
    truncated_entry_id: Option<EntryId>,
    compaction_time_window: Option<Duration>,
}

impl RegionManifestBuilder {
    /// Start with a checkpoint.
    pub fn with_checkpoint(checkpoint: Option<RegionManifest>) -> Self {
        if let Some(s) = checkpoint {
            Self {
                metadata: Some(s.metadata),
                files: s.files,
                flushed_entry_id: s.flushed_entry_id,
                manifest_version: s.manifest_version,
                flushed_sequence: s.flushed_sequence,
                truncated_entry_id: s.truncated_entry_id,
                compaction_time_window: s.compaction_time_window,
            }
        } else {
            Default::default()
        }
    }

    pub fn apply_change(&mut self, manifest_version: ManifestVersion, change: RegionChange) {
        self.metadata = Some(change.metadata);
        self.manifest_version = manifest_version;
    }

    pub fn apply_edit(&mut self, manifest_version: ManifestVersion, edit: RegionEdit) {
        self.manifest_version = manifest_version;
        for file in edit.files_to_add {
            self.files.insert(file.file_id, file);
        }
        for file in edit.files_to_remove {
            self.files.remove(&file.file_id);
        }
        if let Some(flushed_entry_id) = edit.flushed_entry_id {
            self.flushed_entry_id = self.flushed_entry_id.max(flushed_entry_id);
        }
        if let Some(flushed_sequence) = edit.flushed_sequence {
            self.flushed_sequence = self.flushed_sequence.max(flushed_sequence);
        }
        if let Some(window) = edit.compaction_time_window {
            self.compaction_time_window = Some(window);
        }
    }

    pub fn apply_truncate(&mut self, manifest_version: ManifestVersion, truncate: RegionTruncate) {
        self.manifest_version = manifest_version;
        match truncate.kind {
            TruncateKind::All {
                truncated_entry_id,
                truncated_sequence,
            } => {
                self.flushed_entry_id = truncated_entry_id;
                self.flushed_sequence = truncated_sequence;
                self.truncated_entry_id = Some(truncated_entry_id);
                self.files.clear();
            }
            TruncateKind::Partial { files_to_remove } => {
                for file in files_to_remove {
                    self.files.remove(&file.file_id);
                }
            }
        }
    }

    /// Check if the builder keeps a [RegionMetadata](store_api::metadata::RegionMetadata).
    pub fn contains_metadata(&self) -> bool {
        self.metadata.is_some()
    }

    pub fn try_build(self) -> Result<RegionManifest> {
        let metadata = self.metadata.context(RegionMetadataNotFoundSnafu)?;
        Ok(RegionManifest {
            metadata,
            files: self.files,
            flushed_entry_id: self.flushed_entry_id,
            flushed_sequence: self.flushed_sequence,
            manifest_version: self.manifest_version,
            truncated_entry_id: self.truncated_entry_id,
            compaction_time_window: self.compaction_time_window,
        })
    }
}

// The checkpoint of region manifest, generated by checkpointer.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct RegionCheckpoint {
    /// The last manifest version that this checkpoint compacts(inclusive).
    pub last_version: ManifestVersion,
    // The number of manifest actions that this checkpoint compacts.
    pub compacted_actions: usize,
    // The checkpoint data
    pub checkpoint: Option<RegionManifest>,
}

impl RegionCheckpoint {
    pub fn last_version(&self) -> ManifestVersion {
        self.last_version
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let json = serde_json::to_string(&self).context(SerdeJsonSnafu)?;

        Ok(json.into_bytes())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let data = std::str::from_utf8(bytes).context(Utf8Snafu)?;

        serde_json::from_str(data).context(SerdeJsonSnafu)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionMetaActionList {
    pub actions: Vec<RegionMetaAction>,
}

impl RegionMetaActionList {
    pub fn with_action(action: RegionMetaAction) -> Self {
        Self {
            actions: vec![action],
        }
    }

    pub fn new(actions: Vec<RegionMetaAction>) -> Self {
        Self { actions }
    }
}

impl RegionMetaActionList {
    /// Encode self into json in the form of string lines.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let json = serde_json::to_string(&self).context(SerdeJsonSnafu)?;

        Ok(json.into_bytes())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let data = std::str::from_utf8(bytes).context(Utf8Snafu)?;

        serde_json::from_str(data).context(SerdeJsonSnafu)
    }
}

#[cfg(test)]
mod tests {

    use common_time::Timestamp;

    use super::*;

    // These tests are used to ensure backward compatibility of manifest files.
    // DO NOT modify the serialized string when they fail, check if your
    // modification to manifest-related structs is compatible with older manifests.
    #[test]
    fn test_region_action_compatibility() {
        let region_edit = r#"{
            "flushed_entry_id":null,
            "compaction_time_window":null,
            "files_to_add":[
            {"region_id":4402341478400,"file_id":"4b220a70-2b03-4641-9687-b65d94641208","time_range":[{"value":1451609210000,"unit":"Millisecond"},{"value":1451609520000,"unit":"Millisecond"}],"level":1,"file_size":100}
            ],
            "files_to_remove":[
            {"region_id":4402341478400,"file_id":"34b6ebb9-b8a5-4a4b-b744-56f67defad02","time_range":[{"value":1451609210000,"unit":"Millisecond"},{"value":1451609520000,"unit":"Millisecond"}],"level":0,"file_size":100}
            ]
        }"#;
        let _ = serde_json::from_str::<RegionEdit>(region_edit).unwrap();

        let region_edit = r#"{
            "flushed_entry_id":10,
            "flushed_sequence":10,
            "compaction_time_window":null,
            "files_to_add":[
            {"region_id":4402341478400,"file_id":"4b220a70-2b03-4641-9687-b65d94641208","time_range":[{"value":1451609210000,"unit":"Millisecond"},{"value":1451609520000,"unit":"Millisecond"}],"level":1,"file_size":100}
            ],
            "files_to_remove":[
            {"region_id":4402341478400,"file_id":"34b6ebb9-b8a5-4a4b-b744-56f67defad02","time_range":[{"value":1451609210000,"unit":"Millisecond"},{"value":1451609520000,"unit":"Millisecond"}],"level":0,"file_size":100}
            ]
        }"#;
        let _ = serde_json::from_str::<RegionEdit>(region_edit).unwrap();

        let region_change = r#" {
            "metadata":{
                "column_metadatas":[
                {"column_schema":{"name":"a","data_type":{"Int64":{}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"metadata":{}},"semantic_type":"Tag","column_id":1},{"column_schema":{"name":"b","data_type":{"Float64":{}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"metadata":{}},"semantic_type":"Field","column_id":2},{"column_schema":{"name":"c","data_type":{"Timestamp":{"Millisecond":null}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"metadata":{}},"semantic_type":"Timestamp","column_id":3}
                ],
                "primary_key":[1],
                "region_id":5299989648942,
                "schema_version":0
            }
            }"#;
        let _ = serde_json::from_str::<RegionChange>(region_change).unwrap();

        let region_remove = r#"{"region_id":42}"#;
        let _ = serde_json::from_str::<RegionRemove>(region_remove).unwrap();
    }

    #[test]
    fn test_region_manifest_builder() {
        // TODO(ruihang): port this test case
    }

    #[test]
    fn test_encode_decode_region_checkpoint() {
        // TODO(ruihang): port this test case
    }

    #[test]
    fn test_region_manifest_compatibility() {
        // Test deserializing RegionManifest from old schema where FileId is a UUID string
        let region_manifest_json = r#"{
            "metadata": {
                "column_metadatas": [
                    {
                        "column_schema": {
                            "name": "a",
                            "data_type": {"Int64": {}},
                            "is_nullable": false,
                            "is_time_index": false,
                            "default_constraint": null,
                            "metadata": {}
                        },
                        "semantic_type": "Tag",
                        "column_id": 1
                    },
                    {
                        "column_schema": {
                            "name": "b",
                            "data_type": {"Float64": {}},
                            "is_nullable": false,
                            "is_time_index": false,
                            "default_constraint": null,
                            "metadata": {}
                        },
                        "semantic_type": "Field",
                        "column_id": 2
                    },
                    {
                        "column_schema": {
                            "name": "c",
                            "data_type": {"Timestamp": {"Millisecond": null}},
                            "is_nullable": false,
                            "is_time_index": false,
                            "default_constraint": null,
                            "metadata": {}
                        },
                        "semantic_type": "Timestamp",
                        "column_id": 3
                    }
                ],
                "primary_key": [1],
                "region_id": 4402341478400,
                "schema_version": 0
            },
            "files": {
                "4b220a70-2b03-4641-9687-b65d94641208": {
                    "region_id": 4402341478400,
                    "file_id": "4b220a70-2b03-4641-9687-b65d94641208",
                    "time_range": [
                        {"value": 1451609210000, "unit": "Millisecond"},
                        {"value": 1451609520000, "unit": "Millisecond"}
                    ],
                    "level": 1,
                    "file_size": 100
                },
                "34b6ebb9-b8a5-4a4b-b744-56f67defad02": {
                    "region_id": 4402341478400,
                    "file_id": "34b6ebb9-b8a5-4a4b-b744-56f67defad02",
                    "time_range": [
                        {"value": 1451609210000, "unit": "Millisecond"},
                        {"value": 1451609520000, "unit": "Millisecond"}
                    ],
                    "level": 0,
                    "file_size": 100
                }
            },
            "flushed_entry_id": 10,
            "flushed_sequence": 20,
            "manifest_version": 1,
            "truncated_entry_id": null,
            "compaction_time_window": null
        }"#;

        let manifest = serde_json::from_str::<RegionManifest>(region_manifest_json).unwrap();

        // Verify that the files were correctly deserialized
        assert_eq!(manifest.files.len(), 2);
        assert_eq!(manifest.flushed_entry_id, 10);
        assert_eq!(manifest.flushed_sequence, 20);
        assert_eq!(manifest.manifest_version, 1);

        // Verify that FileIds were correctly parsed from UUID strings
        let mut file_ids: Vec<String> = manifest.files.keys().map(|id| id.to_string()).collect();
        file_ids.sort_unstable();
        assert_eq!(
            file_ids,
            vec![
                "34b6ebb9-b8a5-4a4b-b744-56f67defad02",
                "4b220a70-2b03-4641-9687-b65d94641208",
            ]
        );

        // Roundtrip test with current FileId format
        let serialized_manifest = serde_json::to_string(&manifest).unwrap();
        let deserialized_manifest: RegionManifest =
            serde_json::from_str(&serialized_manifest).unwrap();
        assert_eq!(manifest, deserialized_manifest);
        assert_ne!(serialized_manifest, region_manifest_json);
    }

    #[test]
    fn test_region_truncate_compat() {
        // Test deserializing RegionTruncate from old schema
        let region_truncate_json = r#"{
            "region_id": 4402341478400,
            "truncated_entry_id": 10,
            "truncated_sequence": 20
        }"#;

        let truncate_v1: RegionTruncate = serde_json::from_str(region_truncate_json).unwrap();
        assert_eq!(truncate_v1.region_id, 4402341478400);
        assert_eq!(
            truncate_v1.kind,
            TruncateKind::All {
                truncated_entry_id: 10,
                truncated_sequence: 20,
            }
        );

        // Test deserializing RegionTruncate from new schema
        let region_truncate_v2_json = r#"{
    "region_id": 4402341478400,
    "files_to_remove": [
        {
            "region_id": 4402341478400,
            "file_id": "4b220a70-2b03-4641-9687-b65d94641208",
            "time_range": [
                {
                    "value": 1451609210000,
                    "unit": "Millisecond"
                },
                {
                    "value": 1451609520000,
                    "unit": "Millisecond"
                }
            ],
            "level": 1,
            "file_size": 100
        }
    ]
}"#;

        let truncate_v2: RegionTruncate = serde_json::from_str(region_truncate_v2_json).unwrap();
        assert_eq!(truncate_v2.region_id, 4402341478400);
        assert_eq!(
            truncate_v2.kind,
            TruncateKind::Partial {
                files_to_remove: vec![FileMeta {
                    region_id: RegionId::from_u64(4402341478400),
                    file_id: FileId::parse_str("4b220a70-2b03-4641-9687-b65d94641208").unwrap(),
                    time_range: (
                        Timestamp::new_millisecond(1451609210000),
                        Timestamp::new_millisecond(1451609520000)
                    ),
                    level: 1,
                    file_size: 100,
                    ..Default::default()
                }]
            }
        );
    }
}
