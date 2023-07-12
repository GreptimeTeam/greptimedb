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

use serde::{Deserialize, Serialize};
use storage::metadata::{ColumnFamilyMetadata, ColumnMetadata, VersionNumber};
use storage::sst::{FileId, FileMeta};
use store_api::manifest::action::{ProtocolAction, ProtocolVersion};
use store_api::manifest::ManifestVersion;
use store_api::storage::{RegionId, SequenceNumber};

use crate::error::Result;
use crate::manifest::helper;

/// Minimal data that could be used to persist and recover [RegionMetadata](crate::metadata::RegionMetadata).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RawRegionMetadata {
    pub id: RegionId,
    pub name: String,
    pub columns: RawColumnsMetadata,
    pub column_families: RawColumnFamiliesMetadata,
    pub version: VersionNumber,
}

/// Minimal data that could be used to persist and recover [ColumnsMetadata](crate::metadata::ColumnsMetadata).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RawColumnsMetadata {
    pub columns: Vec<ColumnMetadata>,
    pub row_key_end: usize,
    pub timestamp_key_index: usize,
    pub user_column_end: usize,
}

/// Minimal data that could be used to persist and recover [ColumnFamiliesMetadata](crate::metadata::ColumnFamiliesMetadata).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RawColumnFamiliesMetadata {
    pub column_families: Vec<ColumnFamilyMetadata>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionChange {
    /// The committed sequence of the region when this change happens. So the
    /// data with sequence **greater than** this sequence would use the new
    /// metadata.
    pub committed_sequence: SequenceNumber,
    /// The metadata after changed.
    pub metadata: RawRegionMetadata,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionRemove {
    pub region_id: RegionId,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionEdit {
    pub region_version: VersionNumber,
    pub flushed_sequence: Option<SequenceNumber>,
    pub files_to_add: Vec<FileMeta>,
    pub files_to_remove: Vec<FileMeta>,
    pub compaction_time_window: Option<i64>,
}

/// The region version checkpoint
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionVersion {
    pub manifest_version: ManifestVersion,
    pub flushed_sequence: Option<SequenceNumber>,
    pub files: HashMap<FileId, FileMeta>,
}

/// The region manifest data checkpoint
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct RegionManifestData {
    pub committed_sequence: SequenceNumber,
    pub metadata: RawRegionMetadata,
    pub version: Option<RegionVersion>,
}

#[derive(Debug, Default)]
pub struct RegionManifestDataBuilder {
    committed_sequence: SequenceNumber,
    metadata: RawRegionMetadata,
    version: Option<RegionVersion>,
}

impl RegionManifestDataBuilder {
    pub fn with_checkpoint(checkpoint: Option<RegionManifestData>) -> Self {
        if let Some(s) = checkpoint {
            Self {
                metadata: s.metadata,
                version: s.version,
                committed_sequence: s.committed_sequence,
            }
        } else {
            Default::default()
        }
    }

    pub fn apply_change(&mut self, change: RegionChange) {
        self.metadata = change.metadata;
        self.committed_sequence = change.committed_sequence;
    }

    pub fn apply_edit(&mut self, manifest_version: ManifestVersion, edit: RegionEdit) {
        if let Some(version) = &mut self.version {
            version.manifest_version = manifest_version;
            version.flushed_sequence = edit.flushed_sequence;
            for file in edit.files_to_add {
                let _ = version.files.insert(file.file_id, file);
            }
            for file in edit.files_to_remove {
                let _ = version.files.remove(&file.file_id);
            }
        } else {
            self.version = Some(RegionVersion {
                manifest_version,
                flushed_sequence: edit.flushed_sequence,
                files: edit
                    .files_to_add
                    .into_iter()
                    .map(|f| (f.file_id, f))
                    .collect(),
            });
        }
    }
    pub fn build(self) -> RegionManifestData {
        RegionManifestData {
            metadata: self.metadata,
            version: self.version,
            committed_sequence: self.committed_sequence,
        }
    }
}

// The checkpoint of region manifest, generated by checkpoint.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct RegionCheckpoint {
    /// The snasphot protocol
    pub protocol: ProtocolAction,
    /// The last manifest version that this checkpoint compacts(inclusive).
    pub last_version: ManifestVersion,
    // The number of manifest actions that this checkpoint compacts.
    pub compacted_actions: usize,
    // The checkpoint data
    pub checkpoint: Option<RegionManifestData>,
}

impl RegionCheckpoint {
    fn set_protocol(&mut self, action: ProtocolAction) {
        self.protocol = action;
    }

    fn last_version(&self) -> ManifestVersion {
        self.last_version
    }

    fn encode(&self) -> Result<Vec<u8>> {
        todo!()
    }

    fn decode(bs: &[u8], reader_version: ProtocolVersion) -> Result<Self> {
        helper::decode_checkpoint(bs, reader_version)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum RegionMetaAction {
    Protocol(ProtocolAction),
    Change(RegionChange),
    Remove(RegionRemove),
    Edit(RegionEdit),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionMetaActionList {
    pub actions: Vec<RegionMetaAction>,
    pub prev_version: ManifestVersion,
}

impl RegionMetaActionList {
    pub fn with_action(action: RegionMetaAction) -> Self {
        Self {
            actions: vec![action],
            prev_version: 0,
        }
    }

    pub fn new(actions: Vec<RegionMetaAction>) -> Self {
        Self {
            actions,
            prev_version: 0,
        }
    }
}

impl RegionMetaActionList {
    fn set_protocol(&mut self, action: ProtocolAction) {
        // The protocol action should be the first action in action list by convention.
        self.actions.insert(0, RegionMetaAction::Protocol(action));
    }

    fn set_prev_version(&mut self, version: ManifestVersion) {
        self.prev_version = version;
    }

    /// Encode self into json in the form of string lines, starts with prev_version and then action json list.
    fn encode(&self) -> Result<Vec<u8>> {
        helper::encode_actions(self.prev_version, &self.actions)
    }

    fn decode(
        _bs: &[u8],
        _reader_version: ProtocolVersion,
    ) -> Result<(Self, Option<ProtocolAction>)> {
        todo!()
    }
}

pub struct MetaActionIteratorImpl {
    // log_iter: ObjectStoreLogIterator,
    reader_version: ProtocolVersion,
    last_protocol: Option<ProtocolAction>,
}

impl MetaActionIteratorImpl {
    pub fn last_protocol(&self) -> Option<ProtocolAction> {
        self.last_protocol.clone()
    }

    pub async fn next_action(&mut self) -> Result<Option<(ManifestVersion, RegionMetaActionList)>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use storage::sst::FileId;

    use super::*;

    #[test]
    fn test_encode_decode_action_list() {
        // TODO(ruihang): port this test case
    }

    // These tests are used to ensure backward compatibility of manifest files.
    // DO NOT modify the serialized string when they fail, check if your
    // modification to manifest-related structs is compatible with older manifests.
    #[test]
    fn test_region_manifest_compatibility() {
        let region_edit = r#"{"region_version":0,"flushed_sequence":null,"files_to_add":[{"region_id":4402341478400,"file_name":"4b220a70-2b03-4641-9687-b65d94641208.parquet","time_range":[{"value":1451609210000,"unit":"Millisecond"},{"value":1451609520000,"unit":"Millisecond"}],"level":1}],"files_to_remove":[{"region_id":4402341478400,"file_name":"34b6ebb9-b8a5-4a4b-b744-56f67defad02.parquet","time_range":[{"value":1451609210000,"unit":"Millisecond"},{"value":1451609520000,"unit":"Millisecond"}],"level":0}]}"#;
        let _ = serde_json::from_str::<RegionEdit>(region_edit).unwrap();

        let region_change = r#" {"committed_sequence":42,"metadata":{"id":0,"name":"region-0","columns":{"columns":[{"cf_id":0,"desc":{"id":2,"name":"k1","data_type":{"Int32":{}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"comment":""}},{"cf_id":0,"desc":{"id":1,"name":"timestamp","data_type":{"Timestamp":{"Millisecond":null}},"is_nullable":false,"is_time_index":true,"default_constraint":null,"comment":""}},{"cf_id":1,"desc":{"id":3,"name":"v1","data_type":{"Float32":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"comment":""}},{"cf_id":1,"desc":{"id":2147483649,"name":"__sequence","data_type":{"UInt64":{}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"comment":""}},{"cf_id":1,"desc":{"id":2147483650,"name":"__op_type","data_type":{"UInt8":{}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"comment":""}}],"row_key_end":2,"timestamp_key_index":1,"enable_version_column":false,"user_column_end":3},"column_families":{"column_families":[{"name":"default","cf_id":1,"column_index_start":2,"column_index_end":3}]},"version":0}}"#;
        let _ = serde_json::from_str::<RegionChange>(region_change).unwrap();

        let region_remove = r#"{"region_id":42}"#;
        let _ = serde_json::from_str::<RegionRemove>(region_remove).unwrap();

        let protocol_action = r#"{"min_reader_version":1,"min_writer_version":2}"#;
        let _ = serde_json::from_str::<ProtocolAction>(protocol_action).unwrap();
    }

    fn mock_file_meta() -> FileMeta {
        FileMeta {
            region_id: 0.into(),
            file_id: FileId::random(),
            time_range: None,
            level: 0,
            file_size: 1024,
        }
    }

    #[test]
    fn test_region_manifest_builder() {
        // TODO(ruihang): port this test case
    }

    #[test]
    fn test_encode_decode_region_checkpoint() {
        // TODO(ruihang): port this test case
    }
}
