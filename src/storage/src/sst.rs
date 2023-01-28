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

mod parquet;

use std::sync::Arc;

use async_trait::async_trait;
use common_time::Timestamp;
use object_store::{util, ObjectStore};
use serde::{Deserialize, Serialize};
use table::predicate::Predicate;

use crate::error::Result;
use crate::memtable::BoxedBatchIterator;
use crate::read::BoxedBatchReader;
use crate::schema::ProjectedSchemaRef;
use crate::sst::parquet::{ParquetReader, ParquetWriter};

/// Maximum level of SSTs.
pub const MAX_LEVEL: usize = 1;

// We only has fixed number of level, so we array to hold elements. This implement
// detail of LevelMetaVec should not be exposed to the user of [LevelMetas].
type LevelMetaVec = [LevelMeta; MAX_LEVEL];

/// Visitor to access file in each level.
pub trait Visitor {
    /// Visit all `files` in `level`.
    ///
    /// Now the input `files` are unordered.
    fn visit(&mut self, level: usize, files: &[FileHandle]) -> Result<()>;
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

    /// Merge `self` with files to add/remove to create a new [LevelMetas].
    ///
    /// # Panics
    /// Panics if level of [FileHandle] is greater than [MAX_LEVEL].
    pub fn merge(&self, files_to_add: impl Iterator<Item = FileHandle>) -> LevelMetas {
        let mut merged = self.clone();
        for file in files_to_add {
            let level = file.level_index();

            merged.levels[level].add_file(file);
        }

        // TODO(yingwen): Support file removal.

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
    level: u8,
    /// Handles to the files in this level.
    // TODO(yingwen): Now for simplicity, files are unordered, maybe sort the files by time range
    // or use another structure to hold them.
    files: Vec<FileHandle>,
}

impl LevelMeta {
    fn add_file(&mut self, file: FileHandle) {
        self.files.push(file);
    }

    fn visit_level<V: Visitor>(&self, visitor: &mut V) -> Result<()> {
        visitor.visit(self.level.into(), &self.files)
    }

    #[cfg(test)]
    pub fn files(&self) -> &[FileHandle] {
        &self.files
    }
}

fn new_level_meta_vec() -> LevelMetaVec {
    let mut levels = [LevelMeta::default(); MAX_LEVEL];
    for (i, level) in levels.iter_mut().enumerate() {
        level.level = i as u8;
    }

    levels
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
    pub fn level_index(&self) -> usize {
        self.inner.meta.level.into()
    }

    #[inline]
    pub fn file_name(&self) -> &str {
        &self.inner.meta.file_name
    }

    /// Return the start timestamp of current SST file.
    #[inline]
    pub fn start_timestamp(&self) -> Option<Timestamp> {
        self.inner.meta.start_timestamp
    }

    /// Return the end timestamp of current SST file.
    #[inline]
    pub fn end_timestamp(&self) -> Option<Timestamp> {
        self.inner.meta.end_timestamp
    }
}

/// Actually data of [FileHandle].
///
/// Contains meta of the file, and other mutable info like metrics.
#[derive(Debug)]
struct FileHandleInner {
    meta: FileMeta,
}

impl FileHandleInner {
    fn new(meta: FileMeta) -> FileHandleInner {
        FileHandleInner { meta }
    }
}

/// Immutable metadata of a sst file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileMeta {
    pub file_name: String,
    pub start_timestamp: Option<Timestamp>,
    pub end_timestamp: Option<Timestamp>,
    /// SST level of the file.
    pub level: u8,
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
}

#[derive(Debug)]
pub struct SstInfo {
    pub start_timestamp: Option<Timestamp>,
    pub end_timestamp: Option<Timestamp>,
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
        let writer = ParquetWriter::new(&file_path, iter, self.object_store.clone());
        writer.write_sst(opts).await
    }

    async fn read_sst(&self, file_name: &str, opts: &ReadOptions) -> Result<BoxedBatchReader> {
        let file_path = self.sst_file_path(file_name);
        let reader = ParquetReader::new(
            &file_path,
            self.object_store.clone(),
            opts.projected_schema.clone(),
            opts.predicate.clone(),
        );

        let stream = reader.chunk_stream().await?;
        Ok(Box::new(stream))
    }
}
