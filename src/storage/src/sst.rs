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

pub(crate) mod parquet;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use common_time::range::TimestampRange;
use common_time::Timestamp;
use object_store::{util, ObjectStore};
use serde::{Deserialize, Serialize};
use table::predicate::Predicate;

use crate::error::Result;
use crate::memtable::BoxedBatchIterator;
use crate::read::BoxedBatchReader;
use crate::schema::ProjectedSchemaRef;
use crate::sst::parquet::{ParquetReader, ParquetWriter, Source};

/// Maximum level of SSTs.
pub const MAX_LEVEL: u8 = 2;

pub type Level = u8;

// We only has fixed number of level, so we use array to hold elements. This implementation
// detail of LevelMetaVec should not be exposed to the user of [LevelMetas].
type LevelMetaVec = [LevelMeta; MAX_LEVEL as usize];

/// Visitor to access file in each level.
pub trait Visitor {
    /// Visit all `files` in `level`.
    ///
    /// Now the input `files` are unordered.
    fn visit<'a>(
        &mut self,
        level: usize,
        files: impl Iterator<Item = &'a FileHandle>,
    ) -> Result<()>;
}

/// Metadata of all SSTs under a region.
///
/// Files are organized into multiple level, though there may be only one level.
#[derive(Debug, Clone)]
pub struct LevelMetas {
    levels: LevelMetaVec,
}

impl LevelMetas {
    /// Create a new LevelMetas and initialized each level.
    pub fn new() -> LevelMetas {
        LevelMetas {
            levels: new_level_meta_vec(),
        }
    }

    /// Returns total level number.
    #[inline]
    pub fn level_num(&self) -> usize {
        self.levels.len()
    }

    #[inline]
    pub fn level(&self, level: Level) -> &LevelMeta {
        &self.levels[level as usize]
    }

    /// Merge `self` with files to add/remove to create a new [LevelMetas].
    ///
    /// # Panics
    /// Panics if level of [FileHandle] is greater than [MAX_LEVEL].
    pub fn merge(&self, files_to_add: impl Iterator<Item = FileHandle>) -> LevelMetas {
        let mut merged = self.clone();
        for file in files_to_add {
            let level = file.level();
            merged.levels[level as usize].add_file(file);
        }
        merged
    }

    /// Removes files with given file meta and builds a new [LevelMetas].
    ///
    /// # Panics
    /// Panics if level of [FileHandle] is greater than [MAX_LEVEL].
    pub fn remove(&self, files_to_remove: impl Iterator<Item = FileMeta>) -> LevelMetas {
        let mut merged = self.clone();
        for file in files_to_remove {
            let level = file.level;
            merged.levels[level as usize].remove_file(file);
        }
        merged
    }

    /// Visit all SST files.
    ///
    /// Stop visiting remaining files if the visitor returns `Err`, and the `Err`
    /// will be returned to caller.
    pub fn visit_levels<V: Visitor>(&self, visitor: &mut V) -> Result<()> {
        for level in &self.levels {
            level.visit_level(visitor)?;
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn levels(&self) -> &[LevelMeta] {
        &self.levels
    }
}

impl Default for LevelMetas {
    fn default() -> LevelMetas {
        LevelMetas::new()
    }
}

/// Metadata of files in same SST level.
#[derive(Debug, Default, Clone)]
pub struct LevelMeta {
    level: Level,
    /// Handles to the files in this level.
    // TODO(yingwen): Now for simplicity, files are unordered, maybe sort the files by time range
    // or use another structure to hold them.
    files: HashMap<String, FileHandle>,
}

impl LevelMeta {
    pub fn new_empty(level: Level) -> Self {
        Self {
            level,
            files: HashMap::new(),
        }
    }

    fn add_file(&mut self, file: FileHandle) {
        self.files.insert(file.file_name().to_string(), file);
    }

    fn remove_file(&mut self, file_to_remove: FileMeta) {
        self.files.remove(&file_to_remove.file_name);
    }

    pub fn visit_level<V: Visitor>(&self, visitor: &mut V) -> Result<()> {
        visitor.visit(self.level.into(), self.files.values())
    }

    /// Returns the level of level meta.
    #[inline]
    pub fn level(&self) -> Level {
        self.level
    }

    pub fn files(&self) -> impl Iterator<Item = &FileHandle> {
        self.files.values()
    }
}

fn new_level_meta_vec() -> LevelMetaVec {
    (0u8..MAX_LEVEL)
        .into_iter()
        .map(LevelMeta::new_empty)
        .collect::<Vec<_>>()
        .try_into()
        .unwrap() // safety: LevelMetaVec is a fixed length array with length MAX_LEVEL
}

/// In-memory handle to a file.
#[derive(Debug, Clone)]
pub struct FileHandle {
    inner: Arc<FileHandleInner>,
}

impl FileHandle {
    pub fn new(meta: FileMeta) -> FileHandle {
        FileHandle {
            inner: Arc::new(FileHandleInner::new(meta)),
        }
    }

    /// Returns level as usize so it can be used as index.
    #[inline]
    pub fn level(&self) -> Level {
        self.inner.meta.level
    }

    #[inline]
    pub fn file_name(&self) -> &str {
        &self.inner.meta.file_name
    }

    #[inline]
    pub fn time_range(&self) -> &Option<(Timestamp, Timestamp)> {
        &self.inner.meta.time_range
    }

    /// Returns true if current file is under compaction.
    #[inline]
    pub fn compacting(&self) -> bool {
        self.inner.compacting.load(Ordering::Relaxed)
    }

    /// Sets the compacting flag.
    #[inline]
    pub fn set_compacting(&self, compacting: bool) {
        self.inner.compacting.store(compacting, Ordering::Relaxed);
    }
}

/// Actually data of [FileHandle].
///
/// Contains meta of the file, and other mutable info like metrics.
#[derive(Debug)]
struct FileHandleInner {
    meta: FileMeta,
    compacting: AtomicBool,
}

impl FileHandleInner {
    fn new(meta: FileMeta) -> FileHandleInner {
        FileHandleInner {
            meta,
            compacting: AtomicBool::new(false),
        }
    }
}

/// Immutable metadata of a sst file.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileMeta {
    pub file_name: String,
    pub time_range: Option<(Timestamp, Timestamp)>,
    /// SST level of the file.
    pub level: Level,
}

#[derive(Debug, Default)]
pub struct WriteOptions {
    // TODO(yingwen): [flush] row group size.
}

pub struct ReadOptions {
    /// Suggested size of each batch.
    pub batch_size: usize,
    /// The schema that user expected to read, might not the same as the
    /// schema of the SST file.
    pub projected_schema: ProjectedSchemaRef,

    pub predicate: Predicate,
    pub time_range: TimestampRange,
}

#[derive(Debug, PartialEq)]
pub struct SstInfo {
    pub time_range: Option<(Timestamp, Timestamp)>,
}

/// SST access layer.
#[async_trait]
pub trait AccessLayer: Send + Sync + std::fmt::Debug {
    /// Writes SST file with given `file_name`.
    async fn write_sst(
        &self,
        file_name: &str,
        iter: BoxedBatchIterator,
        opts: &WriteOptions,
    ) -> Result<SstInfo>;

    /// Read SST file with given `file_name` and schema.
    async fn read_sst(&self, file_name: &str, opts: &ReadOptions) -> Result<BoxedBatchReader>;

    /// Returns backend object store.
    fn object_store(&self) -> ObjectStore;
}

pub type AccessLayerRef = Arc<dyn AccessLayer>;

/// Sst access layer based on local file system.
#[derive(Debug)]
pub struct FsAccessLayer {
    sst_dir: String,
    object_store: ObjectStore,
}

impl FsAccessLayer {
    pub fn new(sst_dir: &str, object_store: ObjectStore) -> FsAccessLayer {
        FsAccessLayer {
            sst_dir: util::normalize_dir(sst_dir),
            object_store,
        }
    }

    #[inline]
    fn sst_file_path(&self, file_name: &str) -> String {
        format!("{}{}", self.sst_dir, file_name)
    }
}

#[async_trait]
impl AccessLayer for FsAccessLayer {
    async fn write_sst(
        &self,
        file_name: &str,
        iter: BoxedBatchIterator,
        opts: &WriteOptions,
    ) -> Result<SstInfo> {
        // Now we only supports parquet format. We may allow caller to specific SST format in
        // WriteOptions in the future.
        let file_path = self.sst_file_path(file_name);
        let writer = ParquetWriter::new(&file_path, Source::Iter(iter), self.object_store.clone());
        writer.write_sst(opts).await
    }

    async fn read_sst(&self, file_name: &str, opts: &ReadOptions) -> Result<BoxedBatchReader> {
        let file_path = self.sst_file_path(file_name);
        let reader = ParquetReader::new(
            &file_path,
            self.object_store.clone(),
            opts.projected_schema.clone(),
            opts.predicate.clone(),
            opts.time_range.clone(),
        );

        let stream = reader.chunk_stream().await?;
        Ok(Box::new(stream))
    }

    fn object_store(&self) -> ObjectStore {
        self.object_store.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn create_meta(name: &str, level: Level) -> FileMeta {
        FileMeta {
            file_name: name.to_string(),
            time_range: None,
            level,
        }
    }

    fn create_handle(name: &str, level: Level) -> FileHandle {
        FileHandle::new(create_meta(name, level))
    }

    #[test]
    fn test_level_metas_add_and_remove() {
        let metas = LevelMetas::new();
        let merged = metas.merge(vec![create_handle("a", 0), create_handle("b", 0)].into_iter());

        assert_eq!(
            HashSet::from(["a".to_string(), "b".to_string()]),
            merged
                .level(0)
                .files()
                .map(|f| f.file_name().to_string())
                .collect()
        );

        let merged1 = merged.merge(vec![create_handle("c", 1), create_handle("d", 1)].into_iter());
        assert_eq!(
            HashSet::from(["a".to_string(), "b".to_string()]),
            merged1
                .level(0)
                .files()
                .map(|f| f.file_name().to_string())
                .collect()
        );

        assert_eq!(
            HashSet::from(["c".to_string(), "d".to_string()]),
            merged1
                .level(1)
                .files()
                .map(|f| f.file_name().to_string())
                .collect()
        );

        let removed1 = merged1.remove(vec![create_meta("a", 0), create_meta("c", 0)].into_iter());
        assert_eq!(
            HashSet::from(["b".to_string()]),
            removed1
                .level(0)
                .files()
                .map(|f| f.file_name().to_string())
                .collect()
        );

        assert_eq!(
            HashSet::from(["c".to_string(), "d".to_string()]),
            removed1
                .level(1)
                .files()
                .map(|f| f.file_name().to_string())
                .collect()
        );

        let removed2 = removed1.remove(vec![create_meta("c", 1), create_meta("d", 1)].into_iter());
        assert_eq!(
            HashSet::from(["b".to_string()]),
            removed2
                .level(0)
                .files()
                .map(|f| f.file_name().to_string())
                .collect()
        );

        assert_eq!(
            HashSet::new(),
            removed2
                .level(1)
                .files()
                .map(|f| f.file_name().to_string())
                .collect()
        );
    }
}
