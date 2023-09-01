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

    /// Add files to the version.
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
            let handle = FileHandle::new(file, file_purger.clone());
            let file_id = handle.file_id();
            let old = self.levels[level as usize].files.insert(file_id, handle);
            assert!(old.is_none(), "Adds an existing file: {file_id}");
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
