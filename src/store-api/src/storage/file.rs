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

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Debug;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use uuid::Uuid;

use crate::ManifestVersion;
use crate::storage::RegionId;

/// Index version, incremented when the index file is rebuilt.
pub type IndexVersion = u64;

#[derive(Debug, Snafu, PartialEq)]
pub struct ParseIdError {
    source: uuid::Error,
}

/// Unique id for [SST File].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct FileId(Uuid);

impl FileId {
    /// Returns a new unique [FileId] randomly.
    pub fn random() -> FileId {
        FileId(Uuid::new_v4())
    }

    /// Parses id from string.
    pub fn parse_str(input: &str) -> std::result::Result<FileId, ParseIdError> {
        Uuid::parse_str(input).map(FileId).context(ParseIdSnafu)
    }

    /// Converts [FileId] as byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl From<FileId> for Uuid {
    fn from(value: FileId) -> Self {
        value.0
    }
}

impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for FileId {
    type Err = ParseIdError;

    fn from_str(s: &str) -> std::result::Result<FileId, ParseIdError> {
        FileId::parse_str(s)
    }
}

/// Indicating holding a `FileHandle` reference for a specific file&index in a region.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileRef {
    pub region_id: RegionId,
    pub file_id: FileId,
    pub index_version: IndexVersion,
}

impl FileRef {
    pub fn new(region_id: RegionId, file_id: FileId, index_version: u64) -> Self {
        Self {
            region_id,
            file_id,
            index_version,
        }
    }
}

/// The tmp file manifest which record a table's file references.
/// Also record the manifest version when these tmp files are read.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRefsManifest {
    pub file_refs: HashMap<RegionId, HashSet<FileId>>,
    /// Manifest version when this manifest is read for it's files
    pub manifest_version: HashMap<RegionId, ManifestVersion>,
}

#[derive(Clone, Default, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcReport {
    /// deleted files per region
    pub deleted_files: HashMap<RegionId, Vec<FileId>>,
    /// Regions that need retry in next gc round, usually because their tmp ref files are outdated
    pub need_retry_regions: HashSet<RegionId>,
}

impl GcReport {
    pub fn new(
        deleted_files: HashMap<RegionId, Vec<FileId>>,
        need_retry_regions: HashSet<RegionId>,
    ) -> Self {
        Self {
            deleted_files,
            need_retry_regions,
        }
    }

    pub fn merge(&mut self, other: GcReport) {
        for (region, files) in other.deleted_files {
            let self_files = self.deleted_files.entry(region).or_default();
            let dedup: HashSet<FileId> = HashSet::from_iter(
                std::mem::take(self_files)
                    .into_iter()
                    .chain(files.iter().cloned()),
            );
            *self_files = dedup.into_iter().collect();
        }
        self.need_retry_regions.extend(other.need_retry_regions);
        // Remove regions that have succeeded from need_retry_regions
        self.need_retry_regions
            .retain(|region| !self.deleted_files.contains_key(region));
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_file_id() {
        let id = FileId::random();
        let uuid_str = id.to_string();
        assert_eq!(id.0.to_string(), uuid_str);

        let parsed = FileId::parse_str(&uuid_str).unwrap();
        assert_eq!(id, parsed);
        let parsed = uuid_str.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_file_id_serialization() {
        let id = FileId::random();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(format!("\"{id}\""), json);

        let parsed = serde_json::from_str(&json).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_file_refs_manifest_serialization() {
        let mut manifest = FileRefsManifest::default();
        let r0 = RegionId::new(1024, 1);
        let r1 = RegionId::new(1024, 2);
        manifest.file_refs.insert(r0, [FileId::random()].into());
        manifest.file_refs.insert(r1, [FileId::random()].into());
        manifest.manifest_version.insert(r0, 10);
        manifest.manifest_version.insert(r1, 20);

        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: FileRefsManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(manifest, parsed);
    }
}
