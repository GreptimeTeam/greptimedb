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

//! SST version.
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use common_time::{TimeToLive, Timestamp};

use crate::sst::file::{FileHandle, FileId, FileMeta, Level, MAX_LEVEL};
use crate::sst::file_purger::FilePurgerRef;

/// A version of all SSTs in a region.
#[derive(Debug, Clone)]
pub(crate) struct SstVersion {
    /// SST metadata organized by levels.
    levels: LevelMetaArray,
}

pub(crate) type SstVersionRef = Arc<SstVersion>;

impl SstVersion {
    /// Returns a new [SstVersion].
    pub(crate) fn new() -> SstVersion {
        SstVersion {
            levels: new_level_meta_vec(),
        }
    }

    /// Returns a slice to metadatas of all levels.
    pub(crate) fn levels(&self) -> &[LevelMeta] {
        &self.levels
    }

    /// Add files to the version. If a file with the same `file_id` already exists,
    /// it will be overwritten with the new file.
    ///
    /// # Panics
    /// Panics if level of [FileMeta] is greater than [MAX_LEVEL].
    pub(crate) fn add_files(
        &mut self,
        file_purger: FilePurgerRef,
        files_to_add: impl Iterator<Item = FileMeta>,
    ) {
        for file in files_to_add {
            let level = file.level;
            self.levels[level as usize]
                .files
                .insert(file.file_id, FileHandle::new(file, file_purger.clone()));
        }
    }

    /// Remove files from the version.
    ///
    /// # Panics
    /// Panics if level of [FileMeta] is greater than [MAX_LEVEL].
    pub(crate) fn remove_files(&mut self, files_to_remove: impl Iterator<Item = FileMeta>) {
        for file in files_to_remove {
            let level = file.level;
            if let Some(handle) = self.levels[level as usize].files.remove(&file.file_id) {
                handle.mark_deleted();
            }
        }
    }

    /// Marks all SSTs in this version as deleted.
    pub(crate) fn mark_all_deleted(&self) {
        for level_meta in &self.levels {
            for file_handle in level_meta.files.values() {
                file_handle.mark_deleted();
            }
        }
    }

    /// Returns the number of rows in SST files.
    /// For historical reasons, the result is not precise for old SST files.
    pub(crate) fn num_rows(&self) -> u64 {
        self.levels
            .iter()
            .map(|level_meta| {
                level_meta
                    .files
                    .values()
                    .map(|file_handle| {
                        let meta = file_handle.meta_ref();
                        meta.num_rows
                    })
                    .sum::<u64>()
            })
            .sum()
    }

    /// Returns the number of SST files.
    pub(crate) fn num_files(&self) -> u64 {
        self.levels
            .iter()
            .map(|level_meta| level_meta.files.len() as u64)
            .sum()
    }

    /// Returns SST data files'space occupied in current version.
    pub(crate) fn sst_usage(&self) -> u64 {
        self.levels
            .iter()
            .map(|level_meta| {
                level_meta
                    .files
                    .values()
                    .map(|file_handle| {
                        let meta = file_handle.meta_ref();
                        meta.file_size
                    })
                    .sum::<u64>()
            })
            .sum()
    }

    /// Returns SST index files'space occupied in current version.
    pub(crate) fn index_usage(&self) -> u64 {
        self.levels
            .iter()
            .map(|level_meta| {
                level_meta
                    .files
                    .values()
                    .map(|file_handle| {
                        let meta = file_handle.meta_ref();
                        meta.index_file_size
                    })
                    .sum::<u64>()
            })
            .sum()
    }
}

// We only has fixed number of level, so we use array to hold elements. This implementation
// detail of LevelMetaArray should not be exposed to users of [LevelMetas].
type LevelMetaArray = [LevelMeta; MAX_LEVEL as usize];

/// Metadata of files in the same SST level.
#[derive(Clone)]
pub struct LevelMeta {
    /// Level number.
    pub level: Level,
    /// Handles of SSTs in this level.
    pub files: HashMap<FileId, FileHandle>,
}

impl LevelMeta {
    /// Returns an empty meta of specific `level`.
    pub(crate) fn new(level: Level) -> LevelMeta {
        LevelMeta {
            level,
            files: HashMap::new(),
        }
    }

    /// Returns expired SSTs from current level.
    pub fn get_expired_files(&self, now: &Timestamp, ttl: &TimeToLive) -> Vec<FileHandle> {
        self.files
            .values()
            .filter(|v| {
                let (_, end) = v.time_range();

                match ttl.is_expired(&end, now) {
                    Ok(expired) => expired,
                    Err(e) => {
                        common_telemetry::error!(e; "Failed to calculate region TTL expire time");
                        false
                    }
                }
            })
            .cloned()
            .collect()
    }

    pub fn files(&self) -> impl Iterator<Item = &FileHandle> {
        self.files.values()
    }
}

impl fmt::Debug for LevelMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LevelMeta")
            .field("level", &self.level)
            .field("files", &self.files.keys())
            .finish()
    }
}

fn new_level_meta_vec() -> LevelMetaArray {
    (0u8..MAX_LEVEL)
        .map(LevelMeta::new)
        .collect::<Vec<_>>()
        .try_into()
        .unwrap() // safety: LevelMetaArray is a fixed length array with length MAX_LEVEL
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::new_noop_file_purger;

    #[test]
    fn test_add_files() {
        let purger = new_noop_file_purger();

        let files = (1..=3)
            .map(|_| FileMeta {
                file_id: FileId::random(),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let mut version = SstVersion::new();
        // files[1] is added multiple times, and that's ok.
        version.add_files(purger.clone(), files[..=1].iter().cloned());
        version.add_files(purger, files[1..].iter().cloned());

        let added_files = &version.levels()[0].files;
        assert_eq!(added_files.len(), 3);
        files.iter().for_each(|f| {
            assert!(added_files.contains_key(&f.file_id));
        });
    }
}
