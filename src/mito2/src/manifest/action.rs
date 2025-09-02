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
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use partition::expr::PartitionExpr;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{RegionId, SequenceNumber};
use store_api::ManifestVersion;
use strum::Display;

use crate::error::{
    DurationOutOfRangeSnafu, RegionMetadataNotFoundSnafu, Result, SerdeJsonSnafu, Utf8Snafu,
};
use crate::manifest::manager::RemoveFileOptions;
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
    /// event unix timestamp in milliseconds, help to determine file deletion time.
    #[serde(default)]
    pub timestamp_ms: Option<i64>,
    #[serde(with = "humantime_serde")]
    pub compaction_time_window: Option<Duration>,
    pub flushed_entry_id: Option<EntryId>,
    pub flushed_sequence: Option<SequenceNumber>,
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

/// Serialized representation of RegionManifest with deduplicated partition expressions.
#[derive(Serialize, Deserialize)]
struct SerializedRegionManifest {
    metadata: RegionMetadataRef,
    files: HashMap<FileId, SerializedFileMeta>,
    #[serde(default)]
    removed_files: RemovedFilesRecord,
    flushed_entry_id: EntryId,
    flushed_sequence: SequenceNumber,
    manifest_version: ManifestVersion,
    truncated_entry_id: Option<EntryId>,
    #[serde(with = "humantime_serde")]
    compaction_time_window: Option<Duration>,
    #[serde(default)]
    partition_expressions: HashMap<u32, String>,
}

/// Serialized representation of FileMeta that uses partition expression keys.
#[derive(Serialize, Deserialize)]
struct SerializedFileMeta {
    region_id: RegionId,
    file_id: FileId,
    time_range: (common_time::Timestamp, common_time::Timestamp),
    level: u8,
    file_size: u64,
    available_indexes: smallvec::SmallVec<[crate::sst::file::IndexType; 4]>,
    index_file_size: u64,
    num_rows: u64,
    num_row_groups: u64,
    sequence: Option<std::num::NonZeroU64>,
    #[serde(default)]
    partition_expr_key: Option<u32>,
}

/// The region manifest data.
#[derive(Clone, Debug)]
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
    pub removed_files: RemovedFilesRecord,
    /// Last WAL entry id of flushed data.
    pub flushed_entry_id: EntryId,
    /// Last sequence of flushed data.
    pub flushed_sequence: SequenceNumber,
    /// Current manifest version.
    pub manifest_version: ManifestVersion,
    /// Last WAL entry id of truncated data.
    pub truncated_entry_id: Option<EntryId>,
    /// Inferred compaction time window.
    pub compaction_time_window: Option<Duration>,
    /// Deduplicated partition expressions shared by files.
    /// Key is assigned during serialization for deduplication.
    pub partition_expressions: HashMap<u32, Arc<PartitionExpr>>,
}

impl Serialize for RegionManifest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;

        // Build expression deduplication map
        let mut expr_to_key: HashMap<String, u32> = HashMap::new();
        let mut partition_expressions: HashMap<u32, String> = HashMap::new();
        let mut next_key = 1u32;

        // Collect all unique partition expressions from files
        for file_meta in self.files.values() {
            if let Some(expr) = &file_meta.partition_expr {
                let expr_json = expr.as_json_str().map_err(S::Error::custom)?;
                if !expr_to_key.contains_key(&expr_json) {
                    expr_to_key.insert(expr_json.clone(), next_key);
                    partition_expressions.insert(next_key, expr_json);
                    next_key += 1;
                }
            }
        }

        // Convert files to serialized format with expression keys
        let serialized_files: HashMap<FileId, SerializedFileMeta> = self
            .files
            .iter()
            .map(|(file_id, file_meta)| {
                let partition_expr_key = file_meta.partition_expr.as_ref().and_then(|expr| {
                    expr.as_json_str()
                        .ok()
                        .and_then(|json| expr_to_key.get(&json).copied())
                });

                let serialized_meta = SerializedFileMeta {
                    region_id: file_meta.region_id,
                    file_id: file_meta.file_id,
                    time_range: file_meta.time_range,
                    level: file_meta.level,
                    file_size: file_meta.file_size,
                    available_indexes: file_meta.available_indexes.clone(),
                    index_file_size: file_meta.index_file_size,
                    num_rows: file_meta.num_rows,
                    num_row_groups: file_meta.num_row_groups,
                    sequence: file_meta.sequence,
                    partition_expr_key,
                };

                (*file_id, serialized_meta)
            })
            .collect();

        let serialized = SerializedRegionManifest {
            metadata: self.metadata.clone(),
            files: serialized_files,
            removed_files: self.removed_files.clone(),
            flushed_entry_id: self.flushed_entry_id,
            flushed_sequence: self.flushed_sequence,
            manifest_version: self.manifest_version,
            truncated_entry_id: self.truncated_entry_id,
            compaction_time_window: self.compaction_time_window,
            partition_expressions,
        };

        serialized.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RegionManifest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        use serde_json::Value;

        // First deserialize as a generic JSON value to check the format
        let value = Value::deserialize(deserializer)?;

        // Check if this is the new format (has partition_expressions field and files contain partition_expr_key)
        let is_new_format = value.get("partition_expressions").is_some();

        if is_new_format {
            // New format: use the deduplicated serialization
            let serialized: SerializedRegionManifest =
                serde_json::from_value(value).map_err(D::Error::custom)?;

            // Deserialize partition expressions into Arc for sharing
            let mut partition_expressions: HashMap<u32, Arc<PartitionExpr>> = HashMap::new();
            for (key, expr_json) in serialized.partition_expressions {
                let expr = PartitionExpr::from_json_str(&expr_json)
                    .map_err(D::Error::custom)?
                    .ok_or_else(|| D::Error::custom("Failed to parse partition expression"))?;
                partition_expressions.insert(key, Arc::new(expr));
            }

            // Convert serialized files back to FileMeta with Arc-shared expressions
            let mut files: HashMap<FileId, FileMeta> = HashMap::new();
            for (file_id, serialized_meta) in serialized.files {
                let partition_expr = serialized_meta
                    .partition_expr_key
                    .and_then(|key| partition_expressions.get(&key))
                    .map(|arc_expr| (**arc_expr).clone());

                let file_meta = FileMeta {
                    region_id: serialized_meta.region_id,
                    file_id: serialized_meta.file_id,
                    time_range: serialized_meta.time_range,
                    level: serialized_meta.level,
                    file_size: serialized_meta.file_size,
                    available_indexes: serialized_meta.available_indexes,
                    index_file_size: serialized_meta.index_file_size,
                    num_rows: serialized_meta.num_rows,
                    num_row_groups: serialized_meta.num_row_groups,
                    sequence: serialized_meta.sequence,
                    partition_expr,
                };

                files.insert(file_id, file_meta);
            }

            Ok(RegionManifest {
                metadata: serialized.metadata,
                files,
                removed_files: serialized.removed_files,
                flushed_entry_id: serialized.flushed_entry_id,
                flushed_sequence: serialized.flushed_sequence,
                manifest_version: serialized.manifest_version,
                truncated_entry_id: serialized.truncated_entry_id,
                compaction_time_window: serialized.compaction_time_window,
                partition_expressions,
            })
        } else {
            // Old format: deserialize directly with FileMeta containing inline partition expressions
            #[derive(serde::Deserialize)]
            struct OldRegionManifest {
                metadata: RegionMetadataRef,
                files: HashMap<FileId, FileMeta>,
                #[serde(default)]
                removed_files: RemovedFilesRecord,
                flushed_entry_id: EntryId,
                flushed_sequence: SequenceNumber,
                manifest_version: ManifestVersion,
                truncated_entry_id: Option<EntryId>,
                #[serde(with = "humantime_serde")]
                compaction_time_window: Option<Duration>,
            }

            let old_manifest: OldRegionManifest =
                serde_json::from_value(value).map_err(D::Error::custom)?;

            Ok(RegionManifest {
                metadata: old_manifest.metadata,
                files: old_manifest.files,
                removed_files: old_manifest.removed_files,
                flushed_entry_id: old_manifest.flushed_entry_id,
                flushed_sequence: old_manifest.flushed_sequence,
                manifest_version: old_manifest.manifest_version,
                truncated_entry_id: old_manifest.truncated_entry_id,
                compaction_time_window: old_manifest.compaction_time_window,
                partition_expressions: HashMap::new(),
            })
        }
    }
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
            && self.partition_expressions == other.partition_expressions
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
    partition_expressions: HashMap<u32, Arc<PartitionExpr>>,
    next_partition_expr_key: u32,
}

impl RegionManifestBuilder {
    /// Start with a checkpoint.
    pub fn with_checkpoint(checkpoint: Option<RegionManifest>) -> Self {
        if let Some(s) = checkpoint {
            let next_key = s.partition_expressions.keys().max().map_or(1, |k| k + 1);
            Self {
                metadata: Some(s.metadata),
                files: s.files,
                removed_files: s.removed_files,
                flushed_entry_id: s.flushed_entry_id,
                manifest_version: s.manifest_version,
                flushed_sequence: s.flushed_sequence,
                truncated_entry_id: s.truncated_entry_id,
                compaction_time_window: s.compaction_time_window,
                partition_expressions: s.partition_expressions,
                next_partition_expr_key: next_key,
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
            self.add_file_with_deduplication(file);
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
                self.removed_files.add_removed_files(
                    self.files.values().map(|meta| meta.file_id).collect(),
                    truncate
                        .timestamp_ms
                        .unwrap_or_else(|| Utc::now().timestamp_millis()),
                );
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

    /// Add a file with partition expression deduplication.
    fn add_file_with_deduplication(&mut self, file: FileMeta) {
        if let Some(partition_expr) = &file.partition_expr {
            // Check if this expression already exists
            let existing_key = self
                .partition_expressions
                .iter()
                .find(|(_, expr)| expr.as_ref() == partition_expr)
                .map(|(key, _)| *key);

            match existing_key {
                Some(_) => {
                    // Expression exists, use the file as-is (expression will be deduplicated during serialization)
                    self.files.insert(file.file_id, file);
                }
                None => {
                    // New expression, store it in the map
                    let key = self.next_partition_expr_key;
                    self.next_partition_expr_key += 1;
                    self.partition_expressions
                        .insert(key, Arc::new(partition_expr.clone()));
                    self.files.insert(file.file_id, file);
                }
            }
        } else {
            self.files.insert(file.file_id, file);
        }
    }

    pub fn try_build(self) -> Result<RegionManifest> {
        let metadata = self.metadata.context(RegionMetadataNotFoundSnafu)?;
        Ok(RegionManifest {
            metadata,
            files: self.files,
            removed_files: self.removed_files,
            flushed_entry_id: self.flushed_entry_id,
            flushed_sequence: self.flushed_sequence,
            manifest_version: self.manifest_version,
            truncated_entry_id: self.truncated_entry_id,
            compaction_time_window: self.compaction_time_window,
            partition_expressions: self.partition_expressions,
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
        self.removed_files.push(RemovedFiles {
            removed_at: at,
            file_ids,
        });
    }

    pub fn evict_old_removed_files(&mut self, opt: &RemoveFileOptions) -> Result<()> {
        let total_removed_files: usize = self.removed_files.iter().map(|s| s.file_ids.len()).sum();
        if opt.keep_count > 0 && total_removed_files <= opt.keep_count {
            return Ok(());
        }

        let mut cur_file_cnt = total_removed_files;

        let can_evict_until = chrono::Utc::now()
            - chrono::Duration::from_std(opt.keep_ttl).context(DurationOutOfRangeSnafu {
                input: opt.keep_ttl,
            })?;

        self.removed_files.sort_unstable_by_key(|f| f.removed_at);
        let updated = std::mem::take(&mut self.removed_files)
            .into_iter()
            .filter_map(|f| {
                if f.removed_at < can_evict_until.timestamp_millis()
                    && (opt.keep_count == 0 || cur_file_cnt >= opt.keep_count)
                {
                    // can evict all files
                    // TODO(discord9): maybe only evict to below keep_count? Maybe not, or the update might be too frequent.
                    cur_file_cnt -= f.file_ids.len();
                    None
                } else {
                    Some(f)
                }
            })
            .collect();
        self.removed_files = updated;

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
    use datatypes::value::Value;
    use partition::expr::{col, PartitionExpr};

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
            partition_expressions: HashMap::new(),
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
                manifest_version: 0,
                truncated_entry_id: None,
                compaction_time_window: None,
                partition_expressions: HashMap::new(),
            }
        );

        let new_manifest = RegionManifest {
            metadata: metadata.clone(),
            files: HashMap::new(),
            removed_files: Default::default(),
            flushed_entry_id: 0,
            flushed_sequence: 0,
            manifest_version: 0,
            truncated_entry_id: None,
            compaction_time_window: None,
            partition_expressions: HashMap::new(),
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

        /// Last data truncated in the region.
        #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
        pub struct RegionTruncateV1 {
            pub region_id: RegionId,
            pub kind: TruncateKind,
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
    fn test_partition_expression_optimization() {
        let metadata: RegionMetadataRef = serde_json::from_str(
            r#"{
                "column_metadatas": [
                    {"column_schema": {"name": "a", "data_type": {"Int64": {}}, "is_nullable": false, "is_time_index": false, "default_constraint": null, "metadata": {}}, "semantic_type": "Tag", "column_id": 1},
                    {"column_schema": {"name": "ts", "data_type": {"Timestamp": {"Millisecond": null}}, "is_nullable": false, "is_time_index": true, "default_constraint": null, "metadata": {}}, "semantic_type": "Timestamp", "column_id": 2}
                ],
                "primary_key": [1], "region_id": 4402341478400, "schema_version": 0
            }"#,
        ).unwrap();

        // Test comprehensive partition expression optimization: deduplication, Arc sharing, serialization
        let expr1 = PartitionExpr::new(
            col("a"),
            partition::expr::RestrictedOp::GtEq,
            Value::UInt32(10).into(),
        );
        let expr2 = PartitionExpr::new(
            col("a"),
            partition::expr::RestrictedOp::Lt,
            Value::UInt32(20).into(),
        );

        let base_file = FileMeta {
            region_id: RegionId::from_u64(4402341478400),
            file_id: FileId::random(),
            time_range: (
                common_time::Timestamp::new_millisecond(1451609210000),
                common_time::Timestamp::new_millisecond(1451609520000),
            ),
            level: 1,
            file_size: 100,
            available_indexes: Default::default(),
            index_file_size: 0,
            num_rows: 10,
            num_row_groups: 1,
            sequence: None,
            partition_expr: None,
        };

        let mut files = HashMap::new();
        // Test deduplication: 2 files with expr1, 1 with expr2, 1 with None
        files.insert(
            FileId::random(),
            FileMeta {
                partition_expr: Some(expr1.clone()),
                ..base_file.clone()
            },
        );
        files.insert(
            FileId::random(),
            FileMeta {
                partition_expr: Some(expr1.clone()),
                ..base_file.clone()
            },
        );
        files.insert(
            FileId::random(),
            FileMeta {
                partition_expr: Some(expr2.clone()),
                ..base_file.clone()
            },
        );
        files.insert(
            FileId::random(),
            FileMeta {
                partition_expr: None,
                ..base_file.clone()
            },
        );

        let manifest = RegionManifest {
            metadata,
            files,
            removed_files: Default::default(),
            flushed_entry_id: 0,
            flushed_sequence: 0,
            manifest_version: 0,
            truncated_entry_id: None,
            compaction_time_window: None,
            partition_expressions: Default::default(),
        };

        // Test serialization and deduplication
        let json = serde_json::to_string(&manifest).unwrap();
        let serialized_value: serde_json::Value = serde_json::from_str(&json).unwrap();

        // Verify deduplication: exactly 2 unique expressions stored
        assert_eq!(
            serialized_value["partition_expressions"]
                .as_object()
                .unwrap()
                .len(),
            2
        );

        // Verify files use keys instead of inline expressions
        let mut expr_key_count = 0;
        let mut null_key_count = 0;
        for file in serialized_value["files"].as_object().unwrap().values() {
            if file["partition_expr_key"].is_null() {
                null_key_count += 1;
            } else {
                expr_key_count += 1;
            }
        }
        assert_eq!(expr_key_count, 3); // 3 files with expressions
        assert_eq!(null_key_count, 1); // 1 file without

        // Test deserialization and Arc sharing
        let deserialized: RegionManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.files.len(), 4);
        assert_eq!(deserialized.partition_expressions.len(), 2);

        // Verify expressions restored correctly
        let mut expr1_count = 0;
        let mut expr2_count = 0;
        let mut none_count = 0;
        for file_meta in deserialized.files.values() {
            match &file_meta.partition_expr {
                Some(expr) => match format!("{}", expr).as_str() {
                    "a >= 10" => expr1_count += 1,
                    "a < 20" => expr2_count += 1,
                    _ => panic!("Unexpected expression"),
                },
                None => none_count += 1,
            }
        }
        assert_eq!(expr1_count, 2);
        assert_eq!(expr2_count, 1);
        assert_eq!(none_count, 1);
    }
}
