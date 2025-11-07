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

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::Utc;
use common_telemetry::warn;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::ManifestVersion;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{FileId, RegionId, SequenceNumber};
use strum::Display;

use crate::error::{RegionMetadataNotFoundSnafu, Result, SerdeJsonSnafu, Utf8Snafu};
use crate::manifest::manager::RemoveFileOptions;
use crate::region::ManifestStats;
use crate::sst::FormatType;
use crate::sst::file::FileMeta;
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
    /// Format of the SST.
    #[serde(default)]
    pub sst_format: FormatType,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionEdit {
    pub files_to_add: Vec<FileMeta>,
    pub files_to_remove: Vec<FileMeta>,
    /// event unix timestamp in milliseconds, help to determine file deletion time.
    #[serde(default)]
    pub timestamp_ms: Option<i64>,
    #[serde(with = "humantime_serde")]
    pub compaction_time_window: Option<Duration>,
    pub flushed_entry_id: Option<EntryId>,
    pub flushed_sequence: Option<SequenceNumber>,
    pub committed_sequence: Option<SequenceNumber>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionRemove {
    pub region_id: RegionId,
}

/// Last data truncated in the region.
///
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RegionTruncate {
    pub region_id: RegionId,
    #[serde(flatten)]
    pub kind: TruncateKind,
    /// event unix timestamp in milliseconds, help to determine file deletion time.
    #[serde(default)]
    pub timestamp_ms: Option<i64>,
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
#[derive(Serialize, Deserialize, Clone, Debug)]
#[cfg_attr(test, derive(Eq))]
pub struct RegionManifest {
    /// Metadata of the region.
    pub metadata: RegionMetadataRef,
    /// SST files.
    pub files: HashMap<FileId, FileMeta>,
    /// Removed files, which are not in the current manifest but may still be kept for a while.
    /// This is a list of (set of files, timestamp) pairs, where the timestamp is the time when
    /// the files are removed from manifest. The timestamp is in milliseconds since unix epoch.
    ///
    /// Using same checkpoint files and action files, the recovered manifest may differ in this
    /// `removed_files` field, because the checkpointer may evict some removed files using
    /// current machine time. This is acceptable because the removed files are not used in normal
    /// read/write path.
    ///
    #[serde(default)]
    pub removed_files: RemovedFilesRecord,
    /// Last WAL entry id of flushed data.
    pub flushed_entry_id: EntryId,
    /// Last sequence of flushed data.
    pub flushed_sequence: SequenceNumber,
    pub committed_sequence: Option<SequenceNumber>,
    /// Current manifest version.
    pub manifest_version: ManifestVersion,
    /// Last WAL entry id of truncated data.
    pub truncated_entry_id: Option<EntryId>,
    /// Inferred compaction time window.
    #[serde(with = "humantime_serde")]
    pub compaction_time_window: Option<Duration>,
    /// Format of the SST file.
    #[serde(default)]
    pub sst_format: FormatType,
}

#[cfg(test)]
impl PartialEq for RegionManifest {
    fn eq(&self, other: &Self) -> bool {
        self.metadata == other.metadata
            && self.files == other.files
            && self.flushed_entry_id == other.flushed_entry_id
            && self.flushed_sequence == other.flushed_sequence
            && self.manifest_version == other.manifest_version
            && self.truncated_entry_id == other.truncated_entry_id
            && self.compaction_time_window == other.compaction_time_window
            && self.committed_sequence == other.committed_sequence
    }
}

#[derive(Debug, Default)]
pub struct RegionManifestBuilder {
    metadata: Option<RegionMetadataRef>,
    files: HashMap<FileId, FileMeta>,
    pub removed_files: RemovedFilesRecord,
    flushed_entry_id: EntryId,
    flushed_sequence: SequenceNumber,
    manifest_version: ManifestVersion,
    truncated_entry_id: Option<EntryId>,
    compaction_time_window: Option<Duration>,
    committed_sequence: Option<SequenceNumber>,
    sst_format: FormatType,
}

impl RegionManifestBuilder {
    /// Start with a checkpoint.
    pub fn with_checkpoint(checkpoint: Option<RegionManifest>) -> Self {
        if let Some(s) = checkpoint {
            Self {
                metadata: Some(s.metadata),
                files: s.files,
                removed_files: s.removed_files,
                flushed_entry_id: s.flushed_entry_id,
                manifest_version: s.manifest_version,
                flushed_sequence: s.flushed_sequence,
                truncated_entry_id: s.truncated_entry_id,
                compaction_time_window: s.compaction_time_window,
                committed_sequence: s.committed_sequence,
                sst_format: s.sst_format,
            }
        } else {
            Default::default()
        }
    }

    pub fn apply_change(&mut self, manifest_version: ManifestVersion, change: RegionChange) {
        self.metadata = Some(change.metadata);
        self.manifest_version = manifest_version;
        self.sst_format = change.sst_format;
    }

    pub fn apply_edit(&mut self, manifest_version: ManifestVersion, edit: RegionEdit) {
        self.manifest_version = manifest_version;
        for file in edit.files_to_add {
            self.files.insert(file.file_id, file);
        }
        self.removed_files.add_removed_files(
            edit.files_to_remove
                .iter()
                .map(|meta| meta.file_id)
                .collect(),
            edit.timestamp_ms
                .unwrap_or_else(|| Utc::now().timestamp_millis()),
        );
        for file in edit.files_to_remove {
            self.files.remove(&file.file_id);
        }
        if let Some(flushed_entry_id) = edit.flushed_entry_id {
            self.flushed_entry_id = self.flushed_entry_id.max(flushed_entry_id);
        }
        if let Some(flushed_sequence) = edit.flushed_sequence {
            self.flushed_sequence = self.flushed_sequence.max(flushed_sequence);
        }

        if let Some(committed_sequence) = edit.committed_sequence {
            self.committed_sequence = Some(
                self.committed_sequence
                    .map_or(committed_sequence, |exist| exist.max(committed_sequence)),
            );
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
                self.removed_files.add_removed_files(
                    self.files.values().map(|meta| meta.file_id).collect(),
                    truncate
                        .timestamp_ms
                        .unwrap_or_else(|| Utc::now().timestamp_millis()),
                );
                self.files.clear();
            }
            TruncateKind::Partial { files_to_remove } => {
                self.removed_files.add_removed_files(
                    files_to_remove.iter().map(|meta| meta.file_id).collect(),
                    truncate
                        .timestamp_ms
                        .unwrap_or_else(|| Utc::now().timestamp_millis()),
                );
                for file in files_to_remove {
                    self.files.remove(&file.file_id);
                }
            }
        }
    }

    pub fn files(&self) -> &HashMap<FileId, FileMeta> {
        &self.files
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
            removed_files: self.removed_files,
            flushed_entry_id: self.flushed_entry_id,
            flushed_sequence: self.flushed_sequence,
            committed_sequence: self.committed_sequence,
            manifest_version: self.manifest_version,
            truncated_entry_id: self.truncated_entry_id,
            compaction_time_window: self.compaction_time_window,
            sst_format: self.sst_format,
        })
    }
}

/// A record of removed files in the region manifest.
/// This is used to keep track of files that have been removed from the manifest but may still
/// be kept for a while
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct RemovedFilesRecord {
    /// a list of `(FileIds, timestamp)` pairs, where the timestamp is the time when
    /// the files are removed from manifest. The timestamp is in milliseconds since unix epoch.
    pub removed_files: Vec<RemovedFiles>,
}

impl RemovedFilesRecord {
    /// Clear the actually deleted files from the list of removed files
    pub fn clear_deleted_files(&mut self, deleted_files: Vec<FileId>) {
        let deleted_file_set: HashSet<_> = HashSet::from_iter(deleted_files);
        for files in self.removed_files.iter_mut() {
            files.file_ids.retain(|fid| !deleted_file_set.contains(fid));
        }

        self.removed_files.retain(|fs| !fs.file_ids.is_empty());
    }

    pub fn update_file_removed_cnt_to_stats(&self, stats: &ManifestStats) {
        let cnt = self
            .removed_files
            .iter()
            .map(|r| r.file_ids.len() as u64)
            .sum();
        stats
            .file_removed_cnt
            .store(cnt, std::sync::atomic::Ordering::Relaxed);
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct RemovedFiles {
    /// The timestamp is the time when
    /// the files are removed from manifest. The timestamp is in milliseconds since unix epoch.
    pub removed_at: i64,
    /// The set of file ids that are removed.
    pub file_ids: HashSet<FileId>,
}

impl RemovedFilesRecord {
    /// Add a record of removed files with the current timestamp.
    pub fn add_removed_files(&mut self, file_ids: HashSet<FileId>, at: i64) {
        if file_ids.is_empty() {
            return;
        }
        self.removed_files.push(RemovedFiles {
            removed_at: at,
            file_ids,
        });
    }

    pub fn evict_old_removed_files(&mut self, opt: &RemoveFileOptions) -> Result<()> {
        if !opt.enable_gc {
            // If GC is not enabled, always keep removed files empty.
            self.removed_files.clear();
            return Ok(());
        }

        // if GC is enabled, rely on gc worker to delete files, and evict removed files based on options.

        Ok(())
    }
}

// The checkpoint of region manifest, generated by checkpointer.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
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

    pub fn into_region_edit(self) -> RegionEdit {
        let mut edit = RegionEdit {
            files_to_add: Vec::new(),
            files_to_remove: Vec::new(),
            timestamp_ms: None,
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
            committed_sequence: None,
        };

        for action in self.actions {
            if let RegionMetaAction::Edit(region_edit) = action {
                // Merge file adds/removes
                edit.files_to_add.extend(region_edit.files_to_add);
                edit.files_to_remove.extend(region_edit.files_to_remove);
                // Max of flushed entry id / sequence
                if let Some(eid) = region_edit.flushed_entry_id {
                    edit.flushed_entry_id = Some(edit.flushed_entry_id.map_or(eid, |v| v.max(eid)));
                }
                if let Some(seq) = region_edit.flushed_sequence {
                    edit.flushed_sequence = Some(edit.flushed_sequence.map_or(seq, |v| v.max(seq)));
                }
                if let Some(seq) = region_edit.committed_sequence {
                    edit.committed_sequence =
                        Some(edit.committed_sequence.map_or(seq, |v| v.max(seq)));
                }
                // Prefer the latest non-none time window
                if region_edit.compaction_time_window.is_some() {
                    edit.compaction_time_window = region_edit.compaction_time_window;
                }
            }
        }

        edit
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

        // Note: For backward compatibility, the test accepts a RegionChange without sst_format
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

    #[test]
    fn test_region_manifest_removed_files() {
        let region_metadata = r#"{
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
            }"#;

        let metadata: RegionMetadataRef =
            serde_json::from_str(region_metadata).expect("Failed to parse region metadata");
        let manifest = RegionManifest {
            metadata: metadata.clone(),
            files: HashMap::new(),
            flushed_entry_id: 0,
            flushed_sequence: 0,
            committed_sequence: None,
            manifest_version: 0,
            truncated_entry_id: None,
            compaction_time_window: None,
            removed_files: RemovedFilesRecord {
                removed_files: vec![RemovedFiles {
                    removed_at: 0,
                    file_ids: HashSet::from([FileId::parse_str(
                        "4b220a70-2b03-4641-9687-b65d94641208",
                    )
                    .unwrap()]),
                }],
            },
            sst_format: FormatType::PrimaryKey,
        };

        let json = serde_json::to_string(&manifest).unwrap();
        let new: RegionManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(manifest, new);
    }

    /// Test if old version can still be deserialized then serialized to the new version.
    #[test]
    fn test_old_region_manifest_compat() {
        #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
        pub struct RegionManifestV1 {
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

        let region_metadata = r#"{
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
            }"#;

        let metadata: RegionMetadataRef =
            serde_json::from_str(region_metadata).expect("Failed to parse region metadata");

        // first test v1 empty to new
        let v1 = RegionManifestV1 {
            metadata: metadata.clone(),
            files: HashMap::new(),
            flushed_entry_id: 0,
            flushed_sequence: 0,
            manifest_version: 0,
            truncated_entry_id: None,
            compaction_time_window: None,
        };
        let json = serde_json::to_string(&v1).unwrap();
        let new_from_old: RegionManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(
            new_from_old,
            RegionManifest {
                metadata: metadata.clone(),
                files: HashMap::new(),
                removed_files: Default::default(),
                flushed_entry_id: 0,
                flushed_sequence: 0,
                committed_sequence: None,
                manifest_version: 0,
                truncated_entry_id: None,
                compaction_time_window: None,
                sst_format: FormatType::PrimaryKey,
            }
        );

        let new_manifest = RegionManifest {
            metadata: metadata.clone(),
            files: HashMap::new(),
            removed_files: Default::default(),
            flushed_entry_id: 0,
            flushed_sequence: 0,
            committed_sequence: None,
            manifest_version: 0,
            truncated_entry_id: None,
            compaction_time_window: None,
            sst_format: FormatType::PrimaryKey,
        };
        let json = serde_json::to_string(&new_manifest).unwrap();
        let old_from_new: RegionManifestV1 = serde_json::from_str(&json).unwrap();
        assert_eq!(
            old_from_new,
            RegionManifestV1 {
                metadata: metadata.clone(),
                files: HashMap::new(),
                flushed_entry_id: 0,
                flushed_sequence: 0,
                manifest_version: 0,
                truncated_entry_id: None,
                compaction_time_window: None,
            }
        );

        #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
        pub struct RegionEditV1 {
            pub files_to_add: Vec<FileMeta>,
            pub files_to_remove: Vec<FileMeta>,
            #[serde(with = "humantime_serde")]
            pub compaction_time_window: Option<Duration>,
            pub flushed_entry_id: Option<EntryId>,
            pub flushed_sequence: Option<SequenceNumber>,
        }

        let json = serde_json::to_string(&RegionEditV1 {
            files_to_add: vec![],
            files_to_remove: vec![],
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
        })
        .unwrap();
        let new_from_old: RegionEdit = serde_json::from_str(&json).unwrap();
        assert_eq!(
            RegionEdit {
                files_to_add: vec![],
                files_to_remove: vec![],
                timestamp_ms: None,
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
            },
            new_from_old
        );

        // test new version with timestamp_ms set can deserialize to old version
        let new = RegionEdit {
            files_to_add: vec![],
            files_to_remove: vec![],
            timestamp_ms: Some(42),
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
            committed_sequence: None,
        };

        let new_json = serde_json::to_string(&new).unwrap();

        let old_from_new: RegionEditV1 = serde_json::from_str(&new_json).unwrap();
        assert_eq!(
            RegionEditV1 {
                files_to_add: vec![],
                files_to_remove: vec![],
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
            },
            old_from_new
        );
    }

    #[test]
    fn test_region_change_backward_compatibility() {
        // Test that we can deserialize a RegionChange without sst_format
        let region_change_json = r#"{
            "metadata": {
                "column_metadatas": [
                    {"column_schema":{"name":"a","data_type":{"Int64":{}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"metadata":{}},"semantic_type":"Tag","column_id":1},
                    {"column_schema":{"name":"b","data_type":{"Int64":{}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"metadata":{}},"semantic_type":"Field","column_id":2},
                    {"column_schema":{"name":"c","data_type":{"Timestamp":{"Millisecond":null}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"metadata":{}},"semantic_type":"Timestamp","column_id":3}
                ],
                "primary_key": [
                    1
                ],
                "region_id": 42,
                "schema_version": 0
            }
        }"#;

        let region_change: RegionChange = serde_json::from_str(region_change_json).unwrap();
        assert_eq!(region_change.sst_format, FormatType::PrimaryKey);

        // Test serialization and deserialization with sst_format
        let region_change = RegionChange {
            metadata: region_change.metadata.clone(),
            sst_format: FormatType::Flat,
        };

        let serialized = serde_json::to_string(&region_change).unwrap();
        let deserialized: RegionChange = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.sst_format, FormatType::Flat);
    }
}
