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

use bytes::Bytes;
use common_time::{TimeToLive, Timestamp};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::FileId;

use crate::cache::CacheManagerRef;
use crate::sst::file::{FileHandle, FileMeta, Level, MAX_LEVEL};
use crate::sst::file_purger::FilePurgerRef;
use crate::sst::parquet::metadata::extract_primary_key_range;

fn build_file_handle(
    file: FileMeta,
    file_purger: FilePurgerRef,
    region_metadata: &RegionMetadataRef,
    cache_manager: Option<&CacheManagerRef>,
    primary_key_ranges: Option<&HashMap<FileId, (Bytes, Bytes)>>,
) -> FileHandle {
    let handle = FileHandle::new(file, file_purger);
    if let Some(primary_key_range) =
        primary_key_ranges.and_then(|ranges| ranges.get(&handle.file_id().file_id()).cloned())
    {
        handle.set_primary_key_range(primary_key_range);
    } else if let Some(cache_manager) = cache_manager
        && let Some(parquet_meta) =
            cache_manager.get_parquet_meta_data_from_mem_cache(handle.file_id())
        && let Some(primary_key_range) = extract_primary_key_range(&parquet_meta, region_metadata)
    {
        handle.set_primary_key_range(primary_key_range);
    }

    handle
}

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

    pub(crate) fn add_files_with_cache_manager(
        &mut self,
        file_purger: FilePurgerRef,
        files_to_add: impl Iterator<Item = FileMeta>,
        region_metadata: Option<&RegionMetadataRef>,
        cache_manager: Option<&CacheManagerRef>,
        primary_key_ranges: Option<&HashMap<FileId, (Bytes, Bytes)>>,
    ) {
        for file in files_to_add {
            let level = file.level;
            let new_index_version = file.index_version;
            let build_handle = || {
                if let Some(region_metadata) = region_metadata {
                    build_file_handle(
                        file.clone(),
                        file_purger.clone(),
                        region_metadata,
                        cache_manager,
                        primary_key_ranges,
                    )
                } else {
                    FileHandle::new(file.clone(), file_purger.clone())
                }
            };
            // If the file already exists, then we should only replace the handle when the index is outdated.
            self.levels[level as usize]
                .files
                .entry(file.file_id)
                .and_modify(|f| {
                    if *f.meta_ref() == file || f.meta_ref().is_index_up_to_date(&file) {
                        // same file meta or current file handle's index is up-to-date, skip adding
                        if f.index_id().version > new_index_version {
                            // what does it mean for us to see older index version?
                            common_telemetry::warn!(
                                "Adding file with older index version, existing: {:?}, new: {:?}, ignoring new file",
                                f.meta_ref(),
                                file
                            );
                        }
                    } else {
                        // include case like old file have no index or index is outdated
                        *f = build_handle();
                    }
                })
                .or_insert_with(build_handle);
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
        version.add_files_with_cache_manager(
            purger.clone(),
            files[..=1].iter().cloned(),
            None,
            None,
            None,
        );
        version.add_files_with_cache_manager(purger, files[1..].iter().cloned(), None, None, None);

        let added_files = &version.levels()[0].files;
        assert_eq!(added_files.len(), 3);
        files.iter().for_each(|f| {
            assert!(added_files.contains_key(&f.file_id));
        });
    }
}
